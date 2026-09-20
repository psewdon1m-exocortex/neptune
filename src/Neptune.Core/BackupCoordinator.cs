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
        string? requestedRunId = null,
        Func<ExportArtifactReceipt?>? exportedReceipt = null)
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
            var previous = await stateStore.GetRunAsync(clientInstanceId, runId, cancellationToken);
            if (previous is not null && previous.State is not ("export-failed" or "exporting"))
                throw new InvalidOperationException("An existing durable backup run cannot be replaced by a fresh export.");
            var attempted = new BackupRun(runId, registration.ProjectId, clientInstanceId, "exporting", null,
                null, null, null, 0, (previous?.Attempt ?? 0) + 1, previous?.CreatedAt ?? now, now, null);
            if (previous is null) await stateStore.AddRunAsync(attempted, cancellationToken);
            else await stateStore.UpdateRunAsync(attempted, cancellationToken);
            try
            {
                await exportToFile(spoolPath, cancellationToken);
                await using var stream = File.OpenRead(spoolPath);
                var hash = Convert.ToHexStringLower(await SHA256.HashDataAsync(stream, cancellationToken));
                var receipt = exportedReceipt?.Invoke();
                if (receipt is not null && (receipt.Size != stream.Length || receipt.Sha256 != hash))
                    throw new InvalidDataException("Export receipt does not match the durable spool.");
                if (registration.ProjectId == "mastermind" && receipt is null)
                    throw new InvalidDataException("Mastermind archive requires a verified generation receipt.");
                var run = attempted with { State = "spooled", SpoolPath = spoolPath, Size = stream.Length,
                    Sha256 = hash, UpdatedAt = DateTimeOffset.UtcNow, ExportGeneration = receipt?.Generation };
                await stateStore.UpdateRunAsync(run, cancellationToken);
                return run;
            }
            catch
            {
                try { File.Delete(spoolPath); }
                catch (IOException) { }
                await stateStore.UpdateRunAsync(attempted with { State = "export-failed", UpdatedAt = DateTimeOffset.UtcNow,
                    Error = "EXPORT_NOT_COMPLETED" }, CancellationToken.None);
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
