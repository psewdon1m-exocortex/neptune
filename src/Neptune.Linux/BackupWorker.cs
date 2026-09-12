using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using Neptune.Core;

namespace Neptune.Linux;

public sealed class BackupWorker(
    LinuxOptions options,
    ProjectRegistry registry,
    NeptuneStateStore state,
    IHttpClientFactory clients,
    ILogger<BackupWorker> logger) : BackgroundService
{
    private readonly ConcurrentDictionary<string, Task<bool>> _active = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _parallel = new(Math.Max(1, options.MaxParallelProjects));
    private string? _clientInstanceId;
    private CancellationToken _stoppingToken;

    public string ClientInstanceId => _clientInstanceId ?? "initializing";
    public int ActiveCount => _active.Count;
    public bool IsActive(string projectId) => _active.ContainsKey(projectId);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stoppingToken = stoppingToken;
        _clientInstanceId = await new ClientIdentityStore(options.StateDirectory).GetOrCreateAsync(stoppingToken);
        await state.InitializeAsync(stoppingToken);
        await state.PruneSpoolAsync(Path.Combine(options.StateDirectory, "spool"), stoppingToken);
        var registrations = (await registry.ReadAsync(stoppingToken)).ToDictionary(item => item.ProjectId, StringComparer.Ordinal);
        foreach (var run in await state.ListRecoverableRunsAsync(ClientInstanceId, stoppingToken))
        {
            if (run.SpoolPath is not null && File.Exists(run.SpoolPath) && registrations.TryGetValue(run.ProjectId, out var project))
                _ = Start(project, run);
        }
        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTimeOffset.UtcNow;
            foreach (var project in await registry.ReadAsync(stoppingToken))
            {
                if (project.Enabled && (project.NextRunAt is null || project.NextRunAt <= now))
                    _ = Start(project);
            }
            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
        }
    }

    public bool Start(ProjectRegistration project, BackupRun? existingRun = null)
        => TryStart(project, existingRun, null) is not null;

    public async Task RunCommandAsync(ProjectRegistration project, string commandId, CancellationToken cancellationToken)
    {
        var runId = Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(project.ProjectId + "\n" + commandId)));
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            // Startup recovery may already be uploading this exact durable run.
            // Wait for it, then re-read its receipt before deciding to start work.
            if (_active.TryGetValue(project.ProjectId, out var active))
            {
                await active.WaitAsync(cancellationToken);
                continue;
            }
            var existing = await state.GetRunAsync(ClientInstanceId, runId, cancellationToken);
            if (existing?.State == "complete") return;
            var task = TryStart(project, existing, runId);
            if (task is null) continue;
            if (!await task.WaitAsync(cancellationToken))
                throw new InvalidOperationException("The archive pipeline did not complete; see the project run status.");
            return;
        }
    }

    private Task<bool>? TryStart(ProjectRegistration project, BackupRun? existingRun, string? requestedRunId)
    {
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var task = RunAfterGateAsync(project, existingRun, requestedRunId, gate.Task, _stoppingToken);
        if (!_active.TryAdd(project.ProjectId, task))
        {
            gate.SetResult(false);
            return null;
        }
        gate.SetResult(true);
        return task;
    }

    private async Task<bool> RunAfterGateAsync(ProjectRegistration project, BackupRun? existingRun, string? requestedRunId, Task<bool> gate, CancellationToken cancellationToken)
    {
        if (!await gate) return false;
        return await RunGuardedAsync(project, existingRun, requestedRunId, cancellationToken);
    }

    private async Task<bool> RunGuardedAsync(ProjectRegistration project, BackupRun? existingRun, string? requestedRunId, CancellationToken cancellationToken)
    {
        var enteredParallel = false;
        try
        {
            await _parallel.WaitAsync(cancellationToken);
            enteredParallel = true;
            await RunOnceAsync(project, existingRun, requestedRunId, cancellationToken);
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger.LogInformation("Backup stopped for project {ProjectId}; a completed spool remains recoverable", project.ProjectId);
            throw;
        }
        catch (Exception error)
        {
            logger.LogError(error, "Backup failed for project {ProjectId}", project.ProjectId);
            var run = (await state.ListRecoverableRunsAsync(ClientInstanceId, CancellationToken.None)).FirstOrDefault(item => item.ProjectId == project.ProjectId);
            if (run is not null)
                await state.UpdateRunAsync(run with { State = "retry-wait", UpdatedAt = DateTimeOffset.UtcNow, Error = error.Message }, CancellationToken.None);
            await registry.UpdateNextRunAsync(project.ProjectId, DateTimeOffset.UtcNow.AddMinutes(15), CancellationToken.None);
            return false;
        }
        finally
        {
            if (enteredParallel) _parallel.Release();
            _active.TryRemove(project.ProjectId, out _);
        }
    }

    private async Task RunOnceAsync(ProjectRegistration project, BackupRun? existingRun, string? requestedRunId, CancellationToken cancellationToken)
    {
        var http = clients.CreateClient("neptune");
        var coordinator = new BackupCoordinator(state, ClientInstanceId, Path.Combine(options.StateDirectory, "spool"), options.MaxParallelProjects);
        var exporter = new ProjectBackupExporter(http);
        var run = existingRun;
        if (run is null && requestedRunId is null)
            run = (await state.ListRecoverableRunsAsync(ClientInstanceId, cancellationToken)).FirstOrDefault(item => item.ProjectId == project.ProjectId && item.SpoolPath is not null && File.Exists(item.SpoolPath));
        if (run is null)
        {
            var exportToken = await ReadSecretAsync(project.ExportTokenFile, cancellationToken);
            run = await coordinator.ExportAsync(project, (path, token) => exporter.ExportAsync(project.ExportUri, exportToken, path, token), cancellationToken, requestedRunId);
        }
        run = run with { State = "uploading", Attempt = run.Attempt + 1, UpdatedAt = DateTimeOffset.UtcNow, Error = null };
        await state.UpdateRunAsync(run, cancellationToken);

        using var snapshot = await ReadRegisterAsync(http, cancellationToken);
        var values = snapshot.RootElement.GetProperty("values");
        var saturnOrigin = RegisterValues.HttpsOrigin(values, "saturn");
        var backupPath = RegisterValues.RequiredString(values, "services.saturn.paths.backup_ingest");
        var slug = project.SaturnSlug ?? RegisterValues.RequiredString(values, project.SaturnSlugRegisterKey);
        var token = await ReadSecretAsync(project.SaturnTokenFile, cancellationToken);
        var receipt = await new SaturnBackupClient(http).UploadAsync(
            new Uri(saturnOrigin, backupPath.TrimEnd('/') + "/"), slug, token, run,
            coordinator.CreateIdempotencyKey(run), ProductVersion(), cancellationToken);

        if (!string.Equals(receipt.Sha256, run.Sha256, StringComparison.OrdinalIgnoreCase) || receipt.SizeBytes != run.Size)
            throw new InvalidDataException("Saturn receipt does not match the exact exported archive.");
        await state.UpdateRunAsync(run with { State = "complete", SpoolPath = null, UploadedBytes = run.Size.Value, UpdatedAt = DateTimeOffset.UtcNow }, cancellationToken);
        File.Delete(run.SpoolPath!);
        await registry.UpdateNextRunAsync(project.ProjectId, DateTimeOffset.UtcNow.AddHours(project.IntervalHours), cancellationToken);
        logger.LogInformation("Backup {RunId} for {ProjectId} committed to {LogicalPath}", run.RunId, project.ProjectId, receipt.LogicalPath);
    }

    private async Task<JsonDocument> ReadRegisterAsync(HttpClient http, CancellationToken cancellationToken)
    {
        var token = await ReadSecretAsync(options.KernelTokenFile, cancellationToken);
        var client = new KernelRegisterClient(http, Path.Combine(options.StateDirectory, "register-lkg.json"));
        return await client.GetSnapshotAsync(options.KernelOrigin, token, cancellationToken);
    }

    private static async Task<string> ReadSecretAsync(string path, CancellationToken cancellationToken) =>
        (await File.ReadAllTextAsync(path, cancellationToken)).Trim();

    private static string ProductVersion() =>
        Assembly.GetExecutingAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "0.1.1-dev";
}
