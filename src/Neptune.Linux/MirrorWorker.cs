using System.Collections.Concurrent;
using System.IO.Compression;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using Neptune.Core;

namespace Neptune.Linux;

public sealed class MirrorWorker(
    LinuxOptions options,
    ProjectRegistry registry,
    IHttpClientFactory clients,
    ILogger<MirrorWorker> logger) : BackgroundService
{
    private const long MaximumArchiveBytes = 8L * 1024 * 1024 * 1024;
    private const long MaximumExtractedBytes = 32L * 1024 * 1024 * 1024;
    private const int MaximumEntries = 100_000;
    private readonly ConcurrentDictionary<string, Task> _active = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, MirrorRunStatus> _status = new(StringComparer.Ordinal);
    private CancellationToken _stoppingToken;

    public bool IsActive(string projectId) => _active.ContainsKey(projectId);
    public MirrorRunStatus Status(string projectId) => _status.GetValueOrDefault(projectId)
        ?? new MirrorRunStatus("idle", null, null, null, 0, 0);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stoppingToken = stoppingToken;
        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTimeOffset.UtcNow;
            foreach (var project in await registry.ReadAsync(stoppingToken))
                if (project.Mirror is { Enabled: true } mirror && (mirror.NextRunAt is null || mirror.NextRunAt <= now))
                    _ = Start(project);
            await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken);
        }
    }

    public bool Start(ProjectRegistration project)
    {
        if (project.Mirror is null) return false;
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var task = RunAfterGateAsync(project, gate.Task, _stoppingToken);
        if (!_active.TryAdd(project.ProjectId, task))
        {
            gate.SetResult(false);
            return false;
        }
        gate.SetResult(true);
        return true;
    }

    private async Task RunAfterGateAsync(ProjectRegistration project, Task<bool> gate, CancellationToken cancellationToken)
    {
        if (!await gate) return;
        var started = DateTimeOffset.UtcNow;
        _status[project.ProjectId] = Status(project.ProjectId) with { State = "running", LastAttemptAt = started, Error = null, UploadedFiles = 0, DeletedEntries = 0 };
        try
        {
            var result = await MirrorOnceAsync(project, cancellationToken);
            _status[project.ProjectId] = new MirrorRunStatus("complete", started, DateTimeOffset.UtcNow, null, result.UploadedFiles, result.DeletedEntries);
            await registry.UpdateMirrorNextRunAsync(project.ProjectId, DateTimeOffset.UtcNow.AddMinutes(project.Mirror!.IntervalMinutes), cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _status[project.ProjectId] = Status(project.ProjectId) with { State = "stopped", Error = "Neptune is stopping." };
        }
        catch (Exception error)
        {
            logger.LogError(error, "Mirror failed for project {ProjectId}", project.ProjectId);
            _status[project.ProjectId] = Status(project.ProjectId) with { State = "retry-wait", Error = error.Message };
            await registry.UpdateMirrorNextRunAsync(project.ProjectId, DateTimeOffset.UtcNow.AddMinutes(5), CancellationToken.None);
        }
        finally { _active.TryRemove(project.ProjectId, out _); }
    }

    private async Task<(int UploadedFiles, int DeletedEntries)> MirrorOnceAsync(ProjectRegistration project, CancellationToken cancellationToken)
    {
        var mirror = project.Mirror ?? throw new InvalidOperationException("Mirror is not configured.");
        var runDirectory = Path.Combine(options.StateDirectory, "mirror-spool", project.ProjectId + "-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(runDirectory);
        try
        {
            var exportToken = (await File.ReadAllTextAsync(project.ExportTokenFile, cancellationToken)).Trim();
            var source = Path.Combine(runDirectory, mirror.Mode == "single-file" ? mirror.TargetFilename! : "dataset.zip");
            await DownloadExportAsync(mirror.ExportUri, exportToken, source, cancellationToken);
            var contentRoot = runDirectory;
            if (mirror.Mode == "zip-tree")
            {
                contentRoot = Path.Combine(runDirectory, "tree");
                Directory.CreateDirectory(contentRoot);
                ExtractArchive(source, contentRoot);
                File.Delete(source);
            }

            var http = clients.CreateClient("neptune");
            using var snapshot = await ReadRegisterAsync(http, cancellationToken);
            var values = snapshot.RootElement.GetProperty("values");
            var saturnOrigin = RegisterValues.HttpsOrigin(values, "saturn");
            var token = (await File.ReadAllTextAsync(mirror.SaturnTokenFile, cancellationToken)).Trim();
            var targetRoot = new Uri(new Uri(saturnOrigin, "/dav/"), Uri.EscapeDataString(mirror.SaturnRoot) + "/");
            return await UploadTreeAsync(new WebDavSyncClient(http), targetRoot, token, contentRoot, cancellationToken);
        }
        finally
        {
            try { Directory.Delete(runDirectory, true); }
            catch (Exception error) { logger.LogWarning(error, "Could not remove mirror spool {Directory}", runDirectory); }
        }
    }

    private async Task DownloadExportAsync(Uri source, string token, string destination, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, source);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        using var response = await clients.CreateClient("neptune").SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        if (response.Content.Headers.ContentLength is > MaximumArchiveBytes)
            throw new InvalidDataException("Mirror export exceeds the maximum transfer size.");
        await using var input = await response.Content.ReadAsStreamAsync(cancellationToken);
        await using var output = new FileStream(destination, FileMode.CreateNew, FileAccess.Write, FileShare.None, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
        var buffer = new byte[128 * 1024];
        long total = 0;
        while (true)
        {
            var read = await input.ReadAsync(buffer, cancellationToken);
            if (read == 0) break;
            total += read;
            if (total > MaximumArchiveBytes) throw new InvalidDataException("Mirror export exceeds the maximum transfer size.");
            await output.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
        }
        await output.FlushAsync(cancellationToken);
    }

    private static void ExtractArchive(string archivePath, string destination)
    {
        using var archive = ZipFile.OpenRead(archivePath);
        if (archive.Entries.Count > MaximumEntries) throw new InvalidDataException("Mirror archive contains too many entries.");
        long total = 0;
        var root = Path.GetFullPath(destination) + Path.DirectorySeparatorChar;
        foreach (var entry in archive.Entries)
        {
            total += entry.Length;
            if (total > MaximumExtractedBytes) throw new InvalidDataException("Mirror archive expands beyond the allowed size.");
            var target = Path.GetFullPath(Path.Combine(destination, entry.FullName.Replace('/', Path.DirectorySeparatorChar)));
            if (!target.StartsWith(root, StringComparison.Ordinal)) throw new InvalidDataException("Mirror archive contains an unsafe path.");
            if (string.IsNullOrEmpty(entry.Name)) { Directory.CreateDirectory(target); continue; }
            Directory.CreateDirectory(Path.GetDirectoryName(target)!);
            entry.ExtractToFile(target, false);
        }
    }

    private static async Task<(int UploadedFiles, int DeletedEntries)> UploadTreeAsync(WebDavSyncClient client, Uri target, string token, string root, CancellationToken cancellationToken)
    {
        var (directories, files) = EnumerateTree(root);
        foreach (var directory in directories.OrderBy(value => value.Count(character => character == '/')))
            await client.EnsureDirectoryAsync(target, token, directory, cancellationToken);
        var uploaded = 0;
        foreach (var file in files)
        {
            await using var stream = new FileStream(file.Path, FileMode.Open, FileAccess.Read, FileShare.Read, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
            var hash = Convert.ToHexStringLower(await SHA256.HashDataAsync(stream, cancellationToken));
            var destination = WebDavSyncClient.RelativeTargetUri(target, file.Relative);
            if (string.Equals(await client.ReadEtagAsync(destination, token, cancellationToken), $"\"sha256-{hash}\"", StringComparison.OrdinalIgnoreCase)) continue;
            await client.UploadAsync(target, token, file.Relative, file.Path, cancellationToken);
            uploaded++;
        }
        var deleted = await client.MirrorAsync(target, token, files.Select(value => value.Relative).ToHashSet(StringComparer.Ordinal), directories, true, cancellationToken);
        return (uploaded, deleted);
    }

    private static (HashSet<string> Directories, (string Path, string Relative)[] Files) EnumerateTree(string root)
    {
        var directories = new HashSet<string>(StringComparer.Ordinal);
        var files = new List<(string Path, string Relative)>();
        var pending = new Stack<string>();
        pending.Push(root);
        while (pending.TryPop(out var current))
        {
            foreach (var path in Directory.EnumerateFileSystemEntries(current))
            {
                var attributes = File.GetAttributes(path);
                if ((attributes & FileAttributes.ReparsePoint) != 0)
                    throw new InvalidDataException("Mirror export contains a symbolic link or reparse point.");
                var relative = Path.GetRelativePath(root, path).Replace('\\', '/');
                if ((attributes & FileAttributes.Directory) != 0)
                {
                    directories.Add(relative);
                    pending.Push(path);
                }
                else files.Add((path, relative));
                if (directories.Count + files.Count > MaximumEntries)
                    throw new InvalidDataException("Mirror export contains too many entries.");
            }
        }
        return (directories, files.ToArray());
    }

    private async Task<System.Text.Json.JsonDocument> ReadRegisterAsync(HttpClient http, CancellationToken cancellationToken)
    {
        var token = (await File.ReadAllTextAsync(options.KernelTokenFile, cancellationToken)).Trim();
        return await new KernelRegisterClient(http, Path.Combine(options.StateDirectory, "register-lkg.json"))
            .GetSnapshotAsync(options.KernelOrigin, token, cancellationToken);
    }
}
