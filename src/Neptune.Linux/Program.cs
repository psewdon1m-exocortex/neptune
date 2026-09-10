using System.Security.Cryptography;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Neptune.Core;
using Neptune.Linux;
using BadHttpRequestException = Microsoft.AspNetCore.Http.BadHttpRequestException;

if (args is ["version"])
{
    Console.WriteLine(typeof(Program).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0] ?? "0.1.0-dev");
    return;
}

if (args is ["register-project", var projectId, var envFile])
{
    var environment = File.ReadAllLines(envFile)
        .Where(line => !string.IsNullOrWhiteSpace(line) && !line.TrimStart().StartsWith('#'))
        .Select(line => line.Split('=', 2))
        .Where(parts => parts.Length == 2)
        .ToDictionary(parts => parts[0].Trim(), parts => parts[1].Trim(), StringComparer.Ordinal);
    string Required(string name) => environment.TryGetValue(name, out var value) && value.Length > 0
        ? value : throw new InvalidDataException($"{name} is required in {envFile}.");
    var registryPath = environment.GetValueOrDefault("NEPTUNE_REGISTRY_PATH", "/var/lib/neptune/projects.json");
    var saturnSlug = environment.GetValueOrDefault("NEPTUNE_SATURN_SLUG", "");
    var saturnSlugKey = environment.GetValueOrDefault("NEPTUNE_SATURN_SLUG_REGISTER_KEY", "");
    if (saturnSlug.Length == 0 && saturnSlugKey.Length == 0)
        throw new InvalidDataException("NEPTUNE_SATURN_SLUG or NEPTUNE_SATURN_SLUG_REGISTER_KEY is required.");
    var mirrorRoot = environment.GetValueOrDefault("NEPTUNE_MIRROR_ROOT", "");
    MirrorRegistration? mirror = null;
    if (mirrorRoot.Length > 0)
    {
        var mirrorMode = environment.GetValueOrDefault("NEPTUNE_MIRROR_MODE", mirrorRoot == "volt" ? "single-file" : "zip-tree");
        mirror = new MirrorRegistration(
            new Uri(Required("NEPTUNE_MIRROR_EXPORT_URL")),
            mirrorRoot,
            Required("NEPTUNE_MIRROR_TOKEN_FILE"),
            mirrorMode,
            mirrorMode == "single-file" ? environment.GetValueOrDefault("NEPTUNE_MIRROR_TARGET_FILENAME", "personal.volt") : null,
            bool.TryParse(environment.GetValueOrDefault("NEPTUNE_MIRROR_ENABLED"), out var mirrorEnabled) && mirrorEnabled,
            int.TryParse(environment.GetValueOrDefault("NEPTUNE_MIRROR_INTERVAL_MINUTES"), out var mirrorMinutes) ? mirrorMinutes : 5);
    }
    var registration = new ProjectRegistration(
        projectId,
        new Uri(Required("NEPTUNE_BACKUP_EXPORT_URL")),
        Required("NEPTUNE_CONTROL_TOKEN_FILE"),
        Required("NEPTUNE_EXPORT_TOKEN_FILE"),
        Required("NEPTUNE_SATURN_TOKEN_FILE"),
        saturnSlugKey,
        bool.TryParse(environment.GetValueOrDefault("NEPTUNE_BACKUP_ENABLED"), out var enabled) && enabled,
        int.TryParse(environment.GetValueOrDefault("NEPTUNE_BACKUP_INTERVAL_HOURS"), out var hours) ? hours : 24,
        SaturnSlug: saturnSlug.Length == 0 ? null : saturnSlug,
        Mirror: mirror);
    await new ProjectRegistry(registryPath).UpsertAsync(registration);
    Console.WriteLine($"Registered Neptune project '{projectId}'.");
    return;
}

var builder = WebApplication.CreateBuilder(args);
var options = builder.Configuration.GetSection("Neptune").Get<LinuxOptions>()
    ?? throw new InvalidOperationException("Neptune configuration is required.");
FileStream? instanceLock = null;
builder.WebHost.ConfigureKestrel(server =>
{
    if (OperatingSystem.IsWindows()) server.ListenLocalhost(7846);
    else if (OperatingSystem.IsLinux())
    {
        Directory.CreateDirectory(Path.GetDirectoryName(options.SocketPath)!);
        var lockPath = options.SocketPath + ".lock";
        instanceLock = new FileStream(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
        try { instanceLock.Lock(0, 1); }
        catch (IOException error)
        {
            instanceLock.Dispose();
            throw new InvalidOperationException($"Another Neptune daemon owns '{options.SocketPath}'.", error);
        }
        File.Delete(options.SocketPath);
        server.ListenUnixSocket(options.SocketPath, listen => listen.Protocols = HttpProtocols.Http1);
    }
    else throw new PlatformNotSupportedException("Neptune.Linux supports Linux hosts only.");
});
builder.Services.AddSingleton(options);
builder.Services.AddSingleton(new ProjectRegistry(options.RegistryPath));
builder.Services.AddSingleton(new NeptuneStateStore(Path.Combine(options.StateDirectory, "neptune.db")));
builder.Services.AddHttpClient("neptune", client => client.Timeout = TimeSpan.FromHours(2));
builder.Services.AddSingleton<BackupWorker>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<BackupWorker>());
builder.Services.AddSingleton<MirrorWorker>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<MirrorWorker>());
builder.Services.AddHostedService<RemoteControlWorker>();

var app = builder.Build();
if (OperatingSystem.IsLinux())
{
    app.Lifetime.ApplicationStarted.Register(() =>
    {
        if (OperatingSystem.IsLinux())
            File.SetUnixFileMode(options.SocketPath,
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead | UnixFileMode.GroupWrite);
    });
    app.Lifetime.ApplicationStopped.Register(() =>
    {
        try { File.Delete(options.SocketPath); }
        finally
        {
            if (OperatingSystem.IsLinux()) instanceLock?.Unlock(0, 1);
            instanceLock?.Dispose();
        }
    });
}

async Task<ProjectRegistration> AuthorizedProjectAsync(HttpContext context, string projectId)
{
    var project = await context.RequestServices.GetRequiredService<ProjectRegistry>().FindAsync(projectId, context.RequestAborted)
        ?? throw new BadHttpRequestException("Unknown project.", 404);
    var supplied = context.Request.Headers["X-Neptune-Token"].ToString();
    var expected = (await File.ReadAllTextAsync(project.ControlTokenFile, context.RequestAborted)).Trim();
    var equal = supplied.Length == expected.Length && CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(supplied), Encoding.UTF8.GetBytes(expected));
    if (!equal) throw new BadHttpRequestException("Invalid project control token.", 401);
    return project;
}

app.MapGet("/v1/projects/{projectId}/status", async (HttpContext context, string projectId, BackupWorker worker, MirrorWorker mirrorWorker, NeptuneStateStore state) =>
{
    var project = await AuthorizedProjectAsync(context, projectId);
    var runs = await state.GetProjectRunSummaryAsync(worker.ClientInstanceId, projectId, context.RequestAborted);
    return Results.Ok(new
    {
        product = "neptune-linux",
        version = typeof(Program).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0] ?? "0.1.0-dev",
        client_instance_id = worker.ClientInstanceId,
        project = new
        {
            project.ProjectId,
            project.Enabled,
            interval_hours = project.IntervalHours,
            next_run_at = project.NextRunAt,
            mirror = project.Mirror is null ? null : new
            {
                root = project.Mirror.SaturnRoot,
                mode = project.Mirror.Mode,
                enabled = project.Mirror.Enabled,
                interval_minutes = project.Mirror.IntervalMinutes,
                next_run_at = project.Mirror.NextRunAt
            }
        },
        active = worker.IsActive(project.ProjectId),
        last_attempt_at = runs.Latest?.CreatedAt,
        last_success_at = runs.LastSuccessAt,
        latest_error = runs.Latest?.Error,
        latest_run_state = runs.Latest?.State,
        mirror_active = mirrorWorker.IsActive(project.ProjectId),
        mirror = mirrorWorker.Status(project.ProjectId)
    });
});

app.MapGet("/v1/health", (BackupWorker worker) => Results.Ok(new
{
    status = "ok",
    product = "neptune-linux",
    version = typeof(Program).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0] ?? "0.1.0-dev",
    client_instance_id = worker.ClientInstanceId
}));

app.MapPut("/v1/projects/{projectId}/schedule", async (HttpContext context, string projectId, ProjectRegistry registry) =>
{
    await AuthorizedProjectAsync(context, projectId);
    var body = await JsonSerializer.DeserializeAsync<ScheduleRequest>(context.Request.Body, cancellationToken: context.RequestAborted)
        ?? throw new BadHttpRequestException("Schedule body is required.");
    await registry.UpdateScheduleAsync(projectId, body.Enabled, body.IntervalHours, context.RequestAborted);
    return Results.NoContent();
});

app.MapPost("/v1/projects/{projectId}/runs", async (HttpContext context, string projectId, BackupWorker worker) =>
{
    var project = await AuthorizedProjectAsync(context, projectId);
    var accepted = worker.Start(project);
    return accepted ? Results.Accepted() : Results.Conflict(new { error = "project_backup_already_active" });
});

app.MapPut("/v1/projects/{projectId}/mirror/schedule", async (HttpContext context, string projectId, ProjectRegistry registry) =>
{
    await AuthorizedProjectAsync(context, projectId);
    var body = await JsonSerializer.DeserializeAsync<MirrorScheduleRequest>(context.Request.Body, cancellationToken: context.RequestAborted)
        ?? throw new BadHttpRequestException("Mirror schedule body is required.");
    await registry.UpdateMirrorScheduleAsync(projectId, body.Enabled, body.IntervalMinutes, context.RequestAborted);
    return Results.NoContent();
});

app.MapPost("/v1/projects/{projectId}/mirror/runs", async (HttpContext context, string projectId, MirrorWorker worker) =>
{
    var project = await AuthorizedProjectAsync(context, projectId);
    if (project.Mirror is null) return Results.NotFound(new { error = "project_mirror_not_configured" });
    var accepted = worker.Start(project);
    return accepted ? Results.Accepted() : Results.Conflict(new { error = "project_mirror_already_active" });
});

app.Run();

public sealed record ScheduleRequest(bool Enabled, int IntervalHours);
public sealed record MirrorScheduleRequest(bool Enabled, int IntervalMinutes);
