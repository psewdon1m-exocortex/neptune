using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Neptune.Core;

namespace Neptune.Linux;

public sealed class MirrorWorker(
    LinuxOptions options,
    ProjectRegistry registry,
    IHttpClientFactory clients,
    ILogger<MirrorWorker> logger) : BackgroundService
{
    private const long MaximumArchiveBytes = 8L * 1024 * 1024 * 1024;
    private const int MaximumEntries = 100_000;
    private readonly ConcurrentDictionary<string, Task<bool>> _active = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, MirrorRunStatus> _status = new(StringComparer.Ordinal);
    private Uri? _lastSaturnOrigin;
    private CancellationToken _stoppingToken;

    public bool IsActive(string projectId) => _active.ContainsKey(projectId);
    public MirrorRunStatus Status(string projectId) => _status.GetOrAdd(projectId, ReadStatus);

    private string StatusPath(string projectId) => Path.Combine(options.StateDirectory, "mirror-status", Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(projectId))) + ".json");
    private MirrorRunStatus ReadStatus(string projectId)
    {
        var filename = StatusPath(projectId);
        try
        {
            if (File.Exists(filename) && new FileInfo(filename).Length <= 8192)
                return JsonSerializer.Deserialize<MirrorRunStatus>(File.ReadAllText(filename)) ?? new("idle", null, null, null, 0, 0);
        }
        catch (IOException) { }
        catch (JsonException) { }
        return new("idle", null, null, null, 0, 0);
    }
    private async Task PersistStatusAsync(string projectId)
    {
        var filename = StatusPath(projectId);
        Directory.CreateDirectory(Path.GetDirectoryName(filename)!);
        var current = Status(projectId);
        if (current.Error?.Length > 1024) current = current with { Error = current.Error[..1024] };
        await File.WriteAllTextAsync(filename + ".tmp", JsonSerializer.Serialize(current));
        if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(filename + ".tmp", UnixFileMode.UserRead | UnixFileMode.UserWrite);
        File.Move(filename + ".tmp", filename, overwrite: true);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stoppingToken = stoppingToken;
        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTimeOffset.UtcNow;
            foreach (var project in await registry.ReadAsync(stoppingToken))
            {
                var previous = Status(project.ProjectId);
                if (previous.State == "retry-wait" && previous.LastAttemptAt?.AddMinutes(5) <= now)
                    _ = TryStart(project, previous.Manual, previous.CommandId);
                if (!project.PolicyPaused && project.Mirror is { Enabled: true } mirror && (mirror.NextRunAt is null || mirror.NextRunAt <= now))
                    _ = Start(project);
            }
            await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken);
        }
    }

    public bool Start(ProjectRegistration project)
        => TryStart(project, false) is not null;

    public async Task RunCommandAsync(ProjectRegistration project, CancellationToken cancellationToken, string? commandId = null)
    {
        if (project.Mirror is null) throw new InvalidOperationException("This project has no mirror pipeline");
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var previous = Status(project.ProjectId);
            if (commandId is not null && previous.CommandId == commandId && previous.State == "complete") return;
            if (_active.TryGetValue(project.ProjectId, out var active)) { await active.WaitAsync(cancellationToken); continue; }
            var task = TryStart(project, true, commandId);
            if (task is null) continue;
            if (await task.WaitAsync(cancellationToken)) return;
            await Task.Delay(TimeSpan.FromMinutes(5), cancellationToken);
        }
    }

    private Task<bool>? TryStart(ProjectRegistration project, bool manual, string? commandId = null)
    {
        if (project.Mirror is null) return null;
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var task = RunAfterGateAsync(project, gate.Task, _stoppingToken, manual, commandId);
        if (!_active.TryAdd(project.ProjectId, task))
        {
            gate.SetResult(false);
            return null;
        }
        gate.SetResult(true);
        return task;
    }

    private async Task<bool> RunAfterGateAsync(ProjectRegistration project, Task<bool> gate, CancellationToken cancellationToken, bool manual, string? commandId)
    {
        if (!await gate) return false;
        var started = DateTimeOffset.UtcNow;
        _status[project.ProjectId] = Status(project.ProjectId) with { State = "running", LastAttemptAt = started, Error = null, UploadedFiles = 0, DeletedEntries = 0, Manual = manual, CommandId = commandId };
        try
        {
            var result = await MirrorOnceAsync(project, cancellationToken);
            _status[project.ProjectId] = new MirrorRunStatus("complete", started, DateTimeOffset.UtcNow, null, result.UploadedFiles, result.DeletedEntries, manual, commandId);
            if (!manual)
                await registry.UpdateMirrorNextRunAsync(project.ProjectId, DateTimeOffset.UtcNow.AddMinutes(project.Mirror!.IntervalMinutes), cancellationToken, project.ControlRevision);
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _status[project.ProjectId] = Status(project.ProjectId) with { State = "stopped", Error = "Neptune is stopping." };
            throw;
        }
        catch (Exception error)
        {
            logger.LogError(error, "Mirror failed for project {ProjectId}", project.ProjectId);
            _status[project.ProjectId] = Status(project.ProjectId) with { State = "retry-wait", Error = error.Message };
            if (!manual)
                await registry.UpdateMirrorNextRunAsync(project.ProjectId, DateTimeOffset.UtcNow.AddMinutes(5), CancellationToken.None, project.ControlRevision);
            return false;
        }
        finally
        {
            try { await PersistStatusAsync(project.ProjectId); }
            finally { _active.TryRemove(project.ProjectId, out _); }
        }
    }

    private async Task<(int UploadedFiles, int DeletedEntries)> MirrorOnceAsync(ProjectRegistration project, CancellationToken cancellationToken)
    {
        var mirror = project.Mirror ?? throw new InvalidOperationException("Mirror is not configured.");
        var runDirectory = Path.Combine(options.StateDirectory, "mirror-spool", project.ProjectId + "-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(runDirectory);
        try
        {
            var exportToken = await CredentialFile.ReadAsync(project.ExportTokenFile, cancellationToken);
            var source = Path.Combine(runDirectory, mirror.Mode == "single-file" ? mirror.TargetFilename! : "dataset.zip");
            var exporter = new ProjectBackupExporter(clients.CreateClient("neptune"));
            ExportArtifactReceipt? exported = null;
            if (project.ProjectId == "mastermind")
                exported = await exporter.ExportAsync(mirror.ExportUri, exportToken, source, cancellationToken, "mirror");
            else
                await DownloadExportAsync(mirror.ExportUri, exportToken, source, cancellationToken);
            var contentRoot = runDirectory;
            if (mirror.Mode == "zip-tree")
            {
                contentRoot = Path.Combine(runDirectory, "tree");
                Directory.CreateDirectory(contentRoot);
                MirrorArchive.Extract(source, contentRoot, cancellationToken);
                File.Delete(source);
            }

            var http = clients.CreateClient("neptune");
            var saturnOrigin = await ResolveSaturnOriginAsync(http, cancellationToken);
            var token = await CredentialFile.ReadAsync(mirror.SaturnTokenFile, cancellationToken);
            var targetRoot = new Uri(new Uri(saturnOrigin, "/dav/"), Uri.EscapeDataString(mirror.SaturnRoot) + "/");
            var result = await UploadTreeAsync(new WebDavSyncClient(http), targetRoot, token, contentRoot, cancellationToken);
            if (exported is not null)
                await exporter.AcknowledgeAsync(mirror.ExportUri, exportToken, "mirror", exported, cancellationToken);
            return result;
        }
        finally
        {
            try { Directory.Delete(runDirectory, true); }
            catch (Exception error) { logger.LogWarning(error, "Could not remove mirror spool {Directory}", runDirectory); }
        }
    }

    private async Task<Uri> ResolveSaturnOriginAsync(HttpClient http, CancellationToken cancellationToken)
    {
        try
        {
            using var snapshot = await ReadRegisterAsync(http, cancellationToken);
            var resolved = RegisterValues.HttpsOrigin(snapshot.RootElement.GetProperty("values"), "saturn");
            _lastSaturnOrigin = resolved;
            return resolved;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
        catch (Exception error) when (_lastSaturnOrigin is not null)
        {
            logger.LogWarning(error, "Kernel Register refresh failed; mirror upload is using the last resolved Saturn origin held in memory");
            return _lastSaturnOrigin;
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
            if (!string.Equals(await client.ReadEtagAsync(destination, token, cancellationToken), $"\"sha256-{hash}\"", StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException("Mirror upload did not retain the exact file SHA-256.");
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
        var token = await CredentialFile.ReadAsync(options.KernelTokenFile, cancellationToken);
        return await new KernelRegisterClient(http, Path.Combine(options.StateDirectory, "register-lkg.json"))
            .GetSnapshotAsync(options.KernelOrigin, token, cancellationToken, ["services.saturn.sni", "services.saturn.port"]);
    }
}
