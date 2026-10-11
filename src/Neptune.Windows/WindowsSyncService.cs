using System.Text.Json;
using System.Net.Http;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Net.Http.Headers;
using System.Reflection;
using Neptune.Core;

namespace Neptune.Windows;

public sealed record WindowsSyncResult(int UploadedFiles, SaturnPalette Palette, IReadOnlyList<SyncMappingMirrorState> Mirrors);
public sealed record WindowsConnectionResult(SaturnPalette Palette, WindowsConnection Connection);

public sealed class WindowsSyncService(WindowsProfileContext profile, HttpClient? httpClient = null)
{
    private readonly HttpClient _http = httpClient ?? new() { Timeout = TimeSpan.FromHours(2) };

    public async Task<WindowsConnectionResult> ConnectAsync(string clientInstanceId, WindowsConnection connection, CancellationToken cancellationToken = default)
    {
        var prepared = await PrepareAsync(clientInstanceId, connection, cancellationToken);
        return new WindowsConnectionResult(prepared.Palette, connection with { SaturnToken = prepared.SaturnToken, RemoteFolder = prepared.RemoteFolder });
    }

    public async Task<WindowsSyncResult> SyncAsync(
        string clientInstanceId,
        IReadOnlyList<SyncMapping> mappings,
        WindowsConnection connection,
        IProgress<string>? progress = null,
        CancellationToken cancellationToken = default)
    {
        var prepared = await PrepareAsync(clientInstanceId, connection, cancellationToken);
        {
            var state = new NeptuneStateStore(Path.Combine(profile.StateDirectory, "neptune.db"));
            await state.InitializeAsync(cancellationToken);
            var client = new WebDavSyncClient(_http, state);
            var uploaded = 0;
            var failures = 0;
            foreach (var mapping in mappings.Where(item => item.Enabled))
            {
                var mappingName = Path.GetFileName(mapping.LocalPath.TrimEnd(Path.DirectorySeparatorChar));
                if (string.IsNullOrWhiteSpace(mappingName)) mappingName = "root";
                var mappingKey = WebDavSyncClient.CanonicalPathKey(mappingName);
                if (mappings.Any(other => other.MappingId != mapping.MappingId
                    && WebDavSyncClient.CanonicalPathKey(Path.GetFileName(other.LocalPath.TrimEnd(Path.DirectorySeparatorChar))) == mappingKey))
                    throw new InvalidOperationException($"Two selected directories cannot use the same Saturn folder name '{mappingName}'.");
                var entries = SafeEntries(mapping.LocalPath).ToArray();
                EnsureCanonicalPathsAreUnique(entries);
                await client.EnsureDirectoryAsync(prepared.NamespaceRoot, prepared.SaturnToken, mappingName, cancellationToken);
                var canonicalMapping = WebDavSyncClient.RelativeTargetUri(prepared.NamespaceRoot, mappingName);
                var mappingRoot = new Uri(canonicalMapping.AbsoluteUri.TrimEnd('/') + "/");
                var localFiles = entries.Where(value => !value.IsDirectory).Select(value => value.RelativePath).ToHashSet(StringComparer.Ordinal);
                var localDirectories = entries.Where(value => value.IsDirectory).Select(value => value.RelativePath).ToHashSet(StringComparer.Ordinal);
                var mirroredBytes = entries.Where(value => !value.IsDirectory)
                    .Aggregate(0L, (total, value) => checked(total + new FileInfo(value.FullPath).Length));
                var failuresBeforeMapping = failures;
                await client.CancelMissingUploadsAsync(mappingRoot, prepared.SaturnToken, localFiles, cancellationToken);
                foreach (var directory in localDirectories.OrderBy(value => value.Count(character => character == '/')))
                    await client.EnsureDirectoryAsync(mappingRoot, prepared.SaturnToken, directory, cancellationToken);
                foreach (var entry in entries.Where(value => !value.IsDirectory))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var file = entry.FullPath;
                    var relative = entry.RelativePath;
                    var info = new FileInfo(file);
                    var modifiedAt = new DateTimeOffset(info.LastWriteTimeUtc, TimeSpan.Zero);
                    var previous = await state.GetSyncFileAsync(mapping.MappingId, relative, cancellationToken);
                    if (previous is not null && previous.State == "synced" && previous.LocalSize == info.Length && previous.LocalModifiedAt == modifiedAt)
                    {
                        var remoteEtag = await client.ReadEtagAsync(WebDavSyncClient.RelativeTargetUri(mappingRoot, relative), prepared.SaturnToken, cancellationToken);
                        if (remoteEtag is not null && remoteEtag == previous.RemoteEtag) continue;
                    }
                    progress?.Report($"{Path.GetFileName(mapping.LocalPath)} · {relative}");
                    string hash;
                    await using (var source = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan))
                        hash = Convert.ToHexStringLower(await SHA256.HashDataAsync(source, cancellationToken));
                    var afterHash = new FileInfo(file);
                    if (afterHash.Length != info.Length || afterHash.LastWriteTimeUtc != info.LastWriteTimeUtc)
                    {
                        failures++;
                        await state.UpsertSyncFileAsync(new SyncFileRecord(mapping.MappingId, relative, info.Length, modifiedAt, hash, previous?.RemoteEtag, "retry-wait", DateTimeOffset.UtcNow, "File changed while being read."), cancellationToken);
                        continue;
                    }
                    try
                    {
                        var etag = await client.UploadAsync(mappingRoot, prepared.SaturnToken, relative, file, cancellationToken);
                        if (!string.Equals(etag, $"\"sha256-{hash}\"", StringComparison.Ordinal))
                            throw new InvalidDataException("The source changed between scanning and upload; retrying a fresh snapshot.");
                        var afterUpload = new FileInfo(file);
                        if (afterUpload.Length != info.Length || afterUpload.LastWriteTimeUtc != info.LastWriteTimeUtc)
                        {
                            failures++;
                            await state.UpsertSyncFileAsync(new SyncFileRecord(mapping.MappingId, relative, afterUpload.Length, new DateTimeOffset(afterUpload.LastWriteTimeUtc, TimeSpan.Zero), hash, etag, "retry-wait", DateTimeOffset.UtcNow, "File changed while being uploaded."), cancellationToken);
                            continue;
                        }
                        await state.UpsertSyncFileAsync(new SyncFileRecord(mapping.MappingId, relative, info.Length, modifiedAt, hash, etag, "synced", DateTimeOffset.UtcNow, null), cancellationToken);
                        uploaded++;
                    }
                    catch (Exception error) when (error is not OperationCanceledException)
                    {
                        failures++;
                        await state.UpsertSyncFileAsync(new SyncFileRecord(mapping.MappingId, relative, info.Length, modifiedAt, hash, previous?.RemoteEtag, "retry-wait", DateTimeOffset.UtcNow, error.Message), cancellationToken);
                    }
                }
                try
                {
                    await client.MirrorAsync(mappingRoot, prepared.SaturnToken, localFiles, localDirectories, protectMassDeletion: true, cancellationToken: cancellationToken);
                    if (failures == failuresBeforeMapping)
                        await state.UpsertSyncMappingStatsAsync(new SyncMappingMirrorState(mapping.MappingId, mirroredBytes, localFiles.Count, DateTimeOffset.UtcNow), cancellationToken);
                }
                catch (Exception error) when (error is not OperationCanceledException)
                {
                    failures++;
                    progress?.Report($"{mappingName} · remote cleanup failed: {error.Message}");
                }
            }
            if (failures > 0) throw new IOException($"{failures} file(s) could not be synchronized and remain queued for retry.");
            return new WindowsSyncResult(uploaded, prepared.Palette, await state.ListSyncMappingStatsAsync(clientInstanceId, cancellationToken));
        }
    }

    private async Task<PreparedConnection> PrepareAsync(string clientInstanceId, WindowsConnection connection, CancellationToken cancellationToken)
    {
        var register = new KernelRegisterClient(_http, Path.Combine(profile.StateDirectory, "register-lkg.json"));
        string[] keys = ["services.saturn.sni", "services.saturn.port", "services.saturn.paths.sync", "services.saturn.paths.sync_preferences"];
        var snapshot = connection.KernelCredentialKind == WindowsKernelCredentialKind.NeptuneOutdoorToken
            ? await register.ResolveOutdoorAsync(connection.KernelOrigin, connection.KernelToken, keys, cancellationToken, allowCachedMetadata: connection.SaturnToken.Trim().Length == 43)
            : await register.GetSnapshotAsync(connection.KernelOrigin, connection.KernelToken, cancellationToken, keys, allowCachedMetadata: connection.SaturnToken.Trim().Length == 43);
        using (snapshot)
        {
            var values = snapshot.RootElement.GetProperty("values");
            var saturnOrigin = RegisterValues.HttpsOrigin(values, "saturn");
            var syncPath = RegisterValues.RequiredString(values, "services.saturn.paths.sync");
            string preferencesPath;
            try { preferencesPath = RegisterValues.RequiredString(values, "services.saturn.paths.sync_preferences"); }
            catch (InvalidDataException) { preferencesPath = "/api/v1/sync/preferences"; }
            var client = new WebDavSyncClient(_http);
            var syncRoot = new Uri(saturnOrigin, syncPath.TrimEnd('/') + "/");
            var saturnToken = connection.SaturnToken.Trim();
            if (saturnToken.Length == 32)
                saturnToken = await RedeemSetupCodeAsync(saturnOrigin, saturnToken, cancellationToken);
            if (saturnToken.Length != 43)
                throw new InvalidDataException("Saturn setup code or stored device credential is invalid.");
            var remoteFolder = await SendHeartbeatAsync(saturnOrigin, saturnToken, cancellationToken);
            var namespaceRoot = new Uri(syncRoot, Uri.EscapeDataString(remoteFolder) + "/");
            var palette = SaturnPalette.Default;
            try { palette = await client.ReadPaletteAsync(new Uri(saturnOrigin, preferencesPath), saturnToken, cancellationToken); }
            catch (HttpRequestException error) when (error.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                // Older Saturn deployments have no preferences endpoint. The next
                // successful connection after Saturn is upgraded replaces this fallback.
            }
            return new PreparedConnection(namespaceRoot, palette, saturnToken, remoteFolder);
        }
    }

    private async Task<string> RedeemSetupCodeAsync(Uri saturnOrigin, string code, CancellationToken cancellationToken)
    {
        var version = typeof(WindowsSyncService).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0] ?? "unknown";
        using var response = await _http.PostAsJsonAsync(new Uri(saturnOrigin, "/api/v1/device-enrollments/redeem"), new { code, version }, cancellationToken);
        response.EnsureSuccessStatusCode();
        var redeemed = await response.Content.ReadFromJsonAsync<DeviceEnrollmentResponse>(cancellationToken: cancellationToken)
            ?? throw new InvalidDataException("Saturn returned an empty device enrollment response.");
        if (redeemed.Token is not { Length: 43 }) throw new InvalidDataException("Saturn returned an invalid device credential.");
        return redeemed.Token;
    }

    private async Task<string> SendHeartbeatAsync(Uri saturnOrigin, string token, CancellationToken cancellationToken)
    {
        var version = typeof(WindowsSyncService).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0] ?? "unknown";
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(saturnOrigin, "/api/v1/device-session/heartbeat"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        request.Content = JsonContent.Create(new { platform = "windows", version });
        using var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        var device = await response.Content.ReadFromJsonAsync<DeviceAssignment>(cancellationToken: cancellationToken)
            ?? throw new InvalidDataException("Saturn returned no folder assignment.");
        var folder = device.SyncFolderName;
        if (device.SyncRootId is null || !Guid.TryParse(device.SyncRootId, out _) || string.IsNullOrWhiteSpace(folder)
            || folder.Length > 80 || folder is "." or ".." || folder.Any(value => char.IsControl(value) || value is '/' or '\\'))
            throw new InvalidDataException("This Windows connection has no isolated Saturn folder. Create a new setup code in Saturn.");
        return folder;
    }

    private static IEnumerable<LocalEntry> SafeEntries(string root)
    {
        var pending = new Stack<string>();
        pending.Push(root);
        while (pending.Count > 0)
        {
            var directory = pending.Pop();
            // An incomplete local scan must never be interpreted as remote deletions.
            // Let the scheduler retry a missing, inaccessible or concurrently changed tree.
            var entries = Directory.EnumerateFileSystemEntries(directory).ToArray();
            foreach (var entry in entries)
            {
                var attributes = File.GetAttributes(entry);
                if ((attributes & FileAttributes.ReparsePoint) != 0) continue;
                var relative = Path.GetRelativePath(root, entry).Replace('\\', '/');
                if ((attributes & FileAttributes.Directory) != 0)
                {
                    yield return new LocalEntry(entry, relative, true);
                    pending.Push(entry);
                }
                else yield return new LocalEntry(entry, relative, false);
            }
        }
    }

    private static void EnsureCanonicalPathsAreUnique(IEnumerable<LocalEntry> entries)
    {
        var paths = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var entry in entries)
        {
            var key = WebDavSyncClient.CanonicalPathKey(entry.RelativePath);
            if (paths.TryGetValue(key, out var existing) && !string.Equals(existing, entry.RelativePath, StringComparison.Ordinal))
                throw new InvalidDataException($"Local paths '{existing}' and '{entry.RelativePath}' resolve to the same Saturn path.");
            paths[key] = entry.RelativePath;
        }
    }

    private sealed record PreparedConnection(Uri NamespaceRoot, SaturnPalette Palette, string SaturnToken, string RemoteFolder);
    private sealed record DeviceAssignment(string? SyncRootId, string? SyncFolderName);
    private sealed record DeviceEnrollmentResponse(string Token);
    private sealed record LocalEntry(string FullPath, string RelativePath, bool IsDirectory);
}
