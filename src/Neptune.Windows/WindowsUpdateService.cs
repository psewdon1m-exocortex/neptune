using System.Diagnostics;
using System.IO.Compression;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json.Serialization;
using Neptune.Core;

namespace Neptune.Windows;

public sealed record WindowsRelease(Version Version, string VersionText, Uri DownloadUri, string ArtifactName, string Sha256);

public sealed class WindowsUpdateService(WindowsProfileContext profile)
{
    private readonly HttpClient _http = CreateHttpClient();

    public Version CurrentVersion => Assembly.GetEntryAssembly()?.GetName().Version ?? new Version(0, 0, 0, 0);

    public async Task<WindowsRelease?> CheckAsync(WindowsConnection connection, CancellationToken cancellationToken = default)
    {
        var register = new KernelRegisterClient(_http, Path.Combine(profile.StateDirectory, "register-update-lkg.json"));
        using var snapshot = await register.GetSnapshotAsync(connection.KernelOrigin, connection.KernelToken, cancellationToken);
        var repository = RegisterValues.RequiredString(snapshot.RootElement.GetProperty("values"), "repositories.neptune.url");
        var (owner, name) = ParseRepository(repository);
        var releases = await _http.GetFromJsonAsync<GitHubRelease[]>(
            $"https://api.github.com/repos/{Uri.EscapeDataString(owner)}/{Uri.EscapeDataString(name)}/releases?per_page=30", cancellationToken) ?? [];

        foreach (var release in releases.Where(value => !value.Draft && !value.Prerelease && value.TagName.StartsWith("neptune-windows-v", StringComparison.Ordinal))
                     .Select(value => (Release: value, VersionText: value.TagName["neptune-windows-v".Length..], Version: ParseVersion(value.TagName["neptune-windows-v".Length..])))
                     .Where(value => value.Version is not null)
                     .OrderByDescending(value => value.Version))
        {
            if (release.Version! <= CurrentVersion) return null;
            var manifestAsset = release.Release.Assets.SingleOrDefault(value => value.Name == "neptune-windows-release.json")
                ?? throw new InvalidDataException("Windows release manifest is missing.");
            var manifest = await _http.GetFromJsonAsync<ReleaseManifest>(manifestAsset.DownloadUrl, cancellationToken)
                ?? throw new InvalidDataException("Windows release manifest is empty.");
            if (manifest.Schema != "exocortex.neptune.release.v1" || manifest.Product != "neptune-windows" || manifest.Runtime != "win-x64"
                || manifest.Packaging != "portable-zip" || manifest.Version != release.VersionText || !IsSha256(manifest.Sha256))
                throw new InvalidDataException("Windows release manifest is invalid.");
            var artifact = release.Release.Assets.SingleOrDefault(value => value.Name == manifest.Artifact)
                ?? throw new InvalidDataException("Windows release artifact is missing.");
            return new WindowsRelease(release.Version!, release.VersionText, artifact.DownloadUrl, manifest.Artifact, manifest.Sha256.ToLowerInvariant());
        }
        return null;
    }

    public async Task<string> DownloadAsync(WindowsRelease release, IProgress<int>? progress = null, CancellationToken cancellationToken = default)
    {
        var updateRoot = Path.Combine(profile.StateDirectory, "updates", $"{release.VersionText}-{Guid.NewGuid():N}");
        Directory.CreateDirectory(updateRoot);
        var archivePath = Path.Combine(updateRoot, release.ArtifactName);
        using (var response = await _http.GetAsync(release.DownloadUri, HttpCompletionOption.ResponseHeadersRead, cancellationToken))
        {
            response.EnsureSuccessStatusCode();
            var total = response.Content.Headers.ContentLength;
            await using var input = await response.Content.ReadAsStreamAsync(cancellationToken);
            await using var output = new FileStream(archivePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
            var buffer = new byte[128 * 1024];
            long received = 0;
            while (true)
            {
                var count = await input.ReadAsync(buffer, cancellationToken);
                if (count == 0) break;
                await output.WriteAsync(buffer.AsMemory(0, count), cancellationToken);
                received += count;
                if (total is > 0) progress?.Report((int)Math.Min(100, received * 100 / total.Value));
            }
        }
        await using (var archive = new FileStream(archivePath, FileMode.Open, FileAccess.Read, FileShare.Read, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan))
        {
            var hash = Convert.ToHexStringLower(await SHA256.HashDataAsync(archive, cancellationToken));
            if (!string.Equals(hash, release.Sha256, StringComparison.Ordinal)) throw new InvalidDataException("Downloaded update SHA-256 does not match its manifest.");
        }

        var staging = Path.Combine(updateRoot, "staging");
        Directory.CreateDirectory(staging);
        ExtractSafely(archivePath, staging);
        if (!File.Exists(Path.Combine(staging, "Neptune.Windows.exe"))) throw new InvalidDataException("Downloaded update has no Neptune executable.");
        return staging;
    }

    public void BeginApply(string stagingDirectory)
    {
        var executable = Environment.ProcessPath ?? throw new InvalidOperationException("Neptune executable path is unavailable.");
        var target = Path.GetDirectoryName(executable) ?? throw new InvalidOperationException("Neptune application directory is unavailable.");
        var updater = Path.Combine(stagingDirectory, "Neptune.Windows.exe");
        var start = new ProcessStartInfo(updater) { UseShellExecute = false, WorkingDirectory = stagingDirectory };
        start.ArgumentList.Add("--apply-update");
        start.ArgumentList.Add("--source"); start.ArgumentList.Add(stagingDirectory);
        start.ArgumentList.Add("--target"); start.ArgumentList.Add(target);
        start.ArgumentList.Add("--wait-pid"); start.ArgumentList.Add(Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture));
        start.ArgumentList.Add("--profile"); start.ArgumentList.Add(profile.ProfileId);
        _ = Process.Start(start) ?? throw new InvalidOperationException("Could not start the Neptune update helper.");
    }

    public static bool HasOtherInstances()
    {
        if (Environment.ProcessPath is not { Length: > 0 } processPath) return false;
        var current = Path.GetFullPath(processPath);
        foreach (var process in Process.GetProcessesByName(Path.GetFileNameWithoutExtension(current)))
        {
            using (process)
            {
                if (process.Id == Environment.ProcessId) continue;
                try
                {
                    if (string.Equals(Path.GetFullPath(process.MainModule?.FileName ?? ""), current, StringComparison.OrdinalIgnoreCase)) return true;
                }
                catch (Exception error) when (error is InvalidOperationException or System.ComponentModel.Win32Exception) { }
            }
        }
        return false;
    }

    private static HttpClient CreateHttpClient()
    {
        var client = new HttpClient { Timeout = TimeSpan.FromMinutes(30) };
        client.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("Neptune-Windows", "1"));
        return client;
    }

    private static (string Owner, string Name) ParseRepository(string value)
    {
        if (!Uri.TryCreate(value, UriKind.Absolute, out var uri) || uri.Scheme != Uri.UriSchemeHttps || uri.Host != "github.com")
            throw new InvalidDataException("Kernel Register Neptune repository URL is invalid.");
        var parts = uri.AbsolutePath.Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 2) throw new InvalidDataException("Kernel Register Neptune repository URL is invalid.");
        return (parts[0], parts[1].EndsWith(".git", StringComparison.OrdinalIgnoreCase) ? parts[1][..^4] : parts[1]);
    }

    private static Version? ParseVersion(string value) => Version.TryParse(value, out var version) ? version : null;
    private static bool IsSha256(string value) => value.Length == 64 && value.All(Uri.IsHexDigit);

    private static void ExtractSafely(string archivePath, string destination)
    {
        var root = Path.GetFullPath(destination) + Path.DirectorySeparatorChar;
        using var archive = ZipFile.OpenRead(archivePath);
        foreach (var entry in archive.Entries)
        {
            var target = Path.GetFullPath(Path.Combine(destination, entry.FullName));
            if (!target.StartsWith(root, StringComparison.OrdinalIgnoreCase)) throw new InvalidDataException("Update archive contains an unsafe path.");
            if (string.IsNullOrEmpty(entry.Name)) { Directory.CreateDirectory(target); continue; }
            Directory.CreateDirectory(Path.GetDirectoryName(target)!);
            entry.ExtractToFile(target, overwrite: false);
        }
    }

    private sealed record GitHubRelease(
        [property: JsonPropertyName("tag_name")] string TagName,
        [property: JsonPropertyName("draft")] bool Draft,
        [property: JsonPropertyName("prerelease")] bool Prerelease,
        [property: JsonPropertyName("assets")] GitHubAsset[] Assets);
    private sealed record GitHubAsset(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("browser_download_url")] Uri DownloadUrl);
    private sealed record ReleaseManifest(
        [property: JsonPropertyName("schema")] string Schema,
        [property: JsonPropertyName("product")] string Product,
        [property: JsonPropertyName("version")] string Version,
        [property: JsonPropertyName("runtime")] string Runtime,
        [property: JsonPropertyName("packaging")] string Packaging,
        [property: JsonPropertyName("artifact")] string Artifact,
        [property: JsonPropertyName("sha256")] string Sha256);
}
