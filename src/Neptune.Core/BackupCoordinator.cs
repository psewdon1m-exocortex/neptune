using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace Neptune.Core;

public sealed class BackupCoordinator(
    NeptuneStateStore stateStore,
    string clientInstanceId,
    string spoolDirectory,
    int maxParallelProjects = 4)
{
    private readonly SemaphoreSlim _global = new(Math.Max(1, maxParallelProjects));
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _projectLocks = new(StringComparer.Ordinal);

    public async Task<BackupRun> ExportAsync(
        ProjectRegistration registration,
        Func<string, CancellationToken, Task> exportToFile,
        CancellationToken cancellationToken = default,
        string? requestedRunId = null)
    {
        registration.Validate();
        var projectLock = _projectLocks.GetOrAdd(registration.ProjectId, static _ => new SemaphoreSlim(1, 1));
        if (!await projectLock.WaitAsync(0, cancellationToken))
            throw new InvalidOperationException($"A backup for project '{registration.ProjectId}' is already active on this client.");

        var enteredGlobal = false;
        try
        {
            await _global.WaitAsync(cancellationToken);
            enteredGlobal = true;
            Directory.CreateDirectory(spoolDirectory);
            var now = DateTimeOffset.UtcNow;
            var runId = requestedRunId ?? Guid.NewGuid().ToString("N");
            if (runId.Length > 64 || runId.Any(character => !char.IsAsciiLetterOrDigit(character)))
                throw new InvalidDataException("Invalid backup run identifier.");
            var spoolPath = Path.Combine(spoolDirectory, $"{registration.ProjectId}-{runId}.zip");
            try
            {
                await exportToFile(spoolPath, cancellationToken);
                await using var stream = File.OpenRead(spoolPath);
                var hash = Convert.ToHexStringLower(await SHA256.HashDataAsync(stream, cancellationToken));
                var run = new BackupRun(
                    runId, registration.ProjectId, clientInstanceId, "spooled", spoolPath,
                    stream.Length, hash, null, 0, 0, now, DateTimeOffset.UtcNow, null);
                await stateStore.AddRunAsync(run, cancellationToken);
                return run;
            }
            catch
            {
                try { File.Delete(spoolPath); }
                catch (IOException) { }
                throw;
            }
        }
        finally
        {
            if (enteredGlobal) _global.Release();
            projectLock.Release();
        }
    }

    public string CreateIdempotencyKey(BackupRun run)
    {
        var identity = $"{run.ClientInstanceId}\n{run.ProjectId}\n{run.RunId}";
        return "neptune-" + Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(identity)));
    }
}
