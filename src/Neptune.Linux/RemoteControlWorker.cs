using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Reflection;
using System.Text.Json;
using System.Collections.Concurrent;
using Neptune.Core;

namespace Neptune.Linux;

public sealed class RemoteControlWorker(
    LinuxOptions options,
    ProjectRegistry registry,
    NeptuneStateStore state,
    BackupWorker backups,
    MirrorWorker mirrors,
    IHttpClientFactory clients,
    ILogger<RemoteControlWorker> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly ConcurrentDictionary<string, Task> _commands = new(StringComparer.Ordinal);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (backups.ClientInstanceId != "initializing")
                    foreach (var project in await registry.ReadAsync(stoppingToken))
                    {
                        try { await CheckInAsync(project, stoppingToken); }
                        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { throw; }
                        catch (Exception error) { logger.LogWarning(error, "Saturn remote control check-in failed for project {ProjectId}; other projects will continue", project.ProjectId); }
                    }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception error)
            {
                logger.LogWarning(error, "Saturn remote control loop failed; last applied schedules remain active");
            }
            await Task.Delay(TimeSpan.FromSeconds(Math.Clamp(options.RemoteControlPollSeconds, 5, 300)), stoppingToken);
        }
    }

    private async Task CheckInAsync(ProjectRegistration project, CancellationToken cancellationToken)
    {
        var http = clients.CreateClient("neptune");
        using var snapshot = await ReadRegisterAsync(http, cancellationToken);
        var values = snapshot.RootElement.GetProperty("values");
        var saturnOrigin = RegisterValues.HttpsOrigin(values, "saturn");
        var summary = await state.GetProjectRunSummaryAsync(backups.ClientInstanceId, project.ProjectId, cancellationToken);
        var mirrorStatus = mirrors.Status(project.ProjectId);
        var results = await state.ListRemoteCommandResultsAsync(project.ProjectId, cancellationToken);
        var payload = new
        {
            clientInstanceId = backups.ClientInstanceId,
            projectId = project.ProjectId,
            version = ProductVersion(),
            appliedRevision = project.ControlRevision,
            archive = new
            {
                active = backups.IsActive(project.ProjectId),
                state = summary.Latest?.State ?? "idle",
                lastAttemptAt = summary.Latest?.CreatedAt,
                lastSuccessAt = summary.LastSuccessAt,
                error = summary.Latest?.Error
            },
            mirror = project.Mirror is null ? null : new
            {
                active = mirrors.IsActive(project.ProjectId),
                state = mirrorStatus.State,
                lastAttemptAt = mirrorStatus.LastAttemptAt,
                lastSuccessAt = mirrorStatus.LastSuccessAt,
                error = mirrorStatus.Error,
                uploadedFiles = mirrorStatus.UploadedFiles,
                deletedEntries = mirrorStatus.DeletedEntries
            },
            latestError = summary.Latest?.Error ?? mirrorStatus.Error,
            commandResults = results.Select(item => new { id = item.Id, state = item.State, error = item.Error })
        };
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(saturnOrigin, "/api/v1/neptune/agent/check-in"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", await ReadSecretAsync(project.SaturnTokenFile, cancellationToken));
        request.Content = JsonContent.Create(payload, options: JsonOptions);
        using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        var control = await response.Content.ReadFromJsonAsync<RemoteControlResponse>(JsonOptions, cancellationToken)
            ?? throw new InvalidDataException("Saturn returned an empty remote control response.");
        if (control.Schema != "saturn.neptune.control.v1") throw new InvalidDataException("Saturn returned an unsupported remote control schema.");

        await registry.ApplyRemoteDesiredAsync(project.ProjectId, control.Desired.Revision,
            control.Desired.ArchiveEnabled, control.Desired.ArchiveIntervalHours,
            project.Mirror is not null && control.Desired.MirrorEnabled,
            control.Desired.MirrorIntervalMinutes, cancellationToken);
        var current = await registry.FindAsync(project.ProjectId, cancellationToken) ?? project;
        foreach (var command in control.Commands)
        {
            if (_commands.ContainsKey(command.Id)) continue;
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var task = ExecuteTrackedAsync(current, command, gate.Task, cancellationToken);
            if (!_commands.TryAdd(command.Id, task)) continue;
            gate.SetResult();
        }
    }

    private async Task ExecuteTrackedAsync(ProjectRegistration project, RemoteCommand command, Task gate, CancellationToken cancellationToken)
    {
        await gate;
        try { await ExecuteCommandAsync(project, command, cancellationToken); }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (Exception error) { logger.LogWarning(error, "Remote command {CommandId} remains retryable", command.Id); }
        finally { _commands.TryRemove(command.Id, out _); }
    }

    private async Task ExecuteCommandAsync(ProjectRegistration project, RemoteCommand command, CancellationToken cancellationToken)
    {
        var existing = await state.GetRemoteCommandAsync(command.Id, cancellationToken);
        if (existing?.State is "succeeded" or "failed") return;
        if (command.ExpiresAt is null || command.ExpiresAt <= DateTimeOffset.UtcNow)
        {
            await state.SaveRemoteCommandAsync(command.Id, project.ProjectId, command.Kind, "{}", "failed", "Remote command expired or has no expiry; request a new command from Saturn.", cancellationToken);
            return;
        }
        var rawPayload = JsonSerializer.Serialize(command.Payload, JsonOptions);
        if (command.Kind == "agent.update" && UpdateAlreadyApplied(command))
        {
            await state.SaveRemoteCommandAsync(command.Id, project.ProjectId, command.Kind, rawPayload, "succeeded", null, cancellationToken);
            return;
        }
        await state.SaveRemoteCommandAsync(command.Id, project.ProjectId, command.Kind, rawPayload, "executing", null, cancellationToken);
        try
        {
            switch (command.Kind)
            {
                case "archive.run":
                    await backups.RunCommandAsync(project, command.Id, cancellationToken);
                    break;
                case "mirror.run":
                    if (project.Mirror is null) throw new InvalidOperationException("This project has no mirror pipeline.");
                    await mirrors.RunCommandAsync(project, cancellationToken);
                    break;
                case "agent.update":
                    await UpdateAgentAsync(project, command, cancellationToken);
                    break;
                default:
                    throw new InvalidOperationException($"Unsupported remote command '{command.Kind}'.");
            }
            await state.SaveRemoteCommandAsync(command.Id, project.ProjectId, command.Kind, rawPayload, "succeeded", null, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
        catch (Exception error)
        {
            await state.SaveRemoteCommandAsync(command.Id, project.ProjectId, command.Kind, rawPayload, "failed", error.Message, CancellationToken.None);
        }
    }

    private bool UpdateAlreadyApplied(RemoteCommand command) =>
        TryGetString(command.Payload, "version", out var version) && string.Equals(ProductVersion(), version, StringComparison.OrdinalIgnoreCase);

    private async Task UpdateAgentAsync(ProjectRegistration project, RemoteCommand command, CancellationToken cancellationToken)
    {
        if (!TryGetString(command.Payload, "version", out var version) || string.IsNullOrWhiteSpace(version))
            throw new InvalidDataException("The update command does not contain a version.");
        var token = await ReadSecretAsync(options.UpdaterAgentTokenFile, cancellationToken);
        var handler = new SocketsHttpHandler
        {
            ConnectCallback = async (_, token) =>
            {
                var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                try
                {
                    await socket.ConnectAsync(new UnixDomainSocketEndPoint(options.UpdaterSocketPath), token);
                    return new NetworkStream(socket, ownsSocket: true);
                }
                catch
                {
                    socket.Dispose();
                    throw;
                }
            }
        };
        using var http = new HttpClient(handler) { Timeout = Timeout.InfiniteTimeSpan };
        using var request = new HttpRequestMessage(HttpMethod.Post, "http://updater.local/v1/agent/neptune-linux/update");
        request.Headers.Add("X-Neptune-Updater-Token", token);
        request.Content = JsonContent.Create(new { head_id = project.ProjectId, version }, options: JsonOptions);
        using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
    }

    private static bool TryGetString(IReadOnlyDictionary<string, JsonElement> payload, string key, out string value)
    {
        if (payload.TryGetValue(key, out var element) && element.ValueKind == JsonValueKind.String)
        {
            value = element.GetString() ?? "";
            return true;
        }
        value = "";
        return false;
    }

    private async Task<JsonDocument> ReadRegisterAsync(HttpClient http, CancellationToken cancellationToken)
    {
        var token = await ReadSecretAsync(options.KernelTokenFile, cancellationToken);
        return await new KernelRegisterClient(http, Path.Combine(options.StateDirectory, "register-lkg.json"))
            .GetSnapshotAsync(options.KernelOrigin, token, cancellationToken);
    }

    private static async Task<string> ReadSecretAsync(string path, CancellationToken cancellationToken) =>
        (await File.ReadAllTextAsync(path, cancellationToken)).Trim();

    private static string ProductVersion() =>
        Assembly.GetExecutingAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0] ?? "0.1.1-dev";
}
