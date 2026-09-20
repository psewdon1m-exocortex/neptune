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
    Console.WriteLine(typeof(Program).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0] ?? "0.1.8-dev");
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
            int.TryParse(environment.GetValueOrDefault("NEPTUNE_MIRROR_INTERVAL_MINUTES"), out var mirrorMinutes) ? mirrorMinutes : 1440);
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
        Mirror: mirror,
        Reader: environment.TryGetValue("NEPTUNE_READER_TOKEN_FILE", out var readerToken)
            ? new ReaderRegistration(readerToken, environment.GetValueOrDefault("NEPTUNE_READER_ROOT", "root")) : null);
    await new ProjectRegistry(registryPath).UpsertAsync(registration, preservePolicy: true);
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
builder.Services.AddHttpClient("neptune", client => client.Timeout = TimeSpan.FromHours(2))
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler { AllowAutoRedirect = false,
        MaxConnectionsPerServer = 16, ConnectTimeout = TimeSpan.FromSeconds(5) });
builder.Services.AddHttpClient("resource-reader", client => client.Timeout = Timeout.InfiniteTimeSpan)
    .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler { AllowAutoRedirect = false, UseProxy = false,
        MaxConnectionsPerServer = 8, ConnectTimeout = TimeSpan.FromSeconds(5) });
builder.Services.AddSingleton<ResourceReader>();
builder.Services.AddSingleton<BackupWorker>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<BackupWorker>());
builder.Services.AddSingleton<MirrorWorker>();
builder.Services.AddHostedService(provider => provider.GetRequiredService<MirrorWorker>());
builder.Services.AddHostedService<RemoteControlWorker>();
builder.Services.AddSingleton<ServicePolicyClient>();

var app = builder.Build();
app.Use(async (context, next) => {
    context.Response.Headers["X-Robots-Tag"] = "noindex, nofollow, noarchive, nosnippet, noimageindex";
    context.Response.Headers["Cache-Control"] = "no-store";
    context.Response.Headers["X-Content-Type-Options"] = "nosniff";
    try { await next(context); }
    catch (PolicyProtocolException error) {
        context.Response.StatusCode = error.StatusCode;
        await context.Response.WriteAsJsonAsync(new { error = error.Message });
    }
    catch (JsonException) when (context.Request.Path.Value?.Contains("/policy", StringComparison.Ordinal) == true) {
        context.Response.StatusCode = 400;
        await context.Response.WriteAsJsonAsync(new { error = "Invalid backup policy JSON" });
    }
    catch (Exception error) when (context.Request.Path.Value?.Contains("/policy", StringComparison.Ordinal) == true &&
        error is HttpRequestException or IOException or InvalidDataException or OperationCanceledException) {
        if (context.RequestAborted.IsCancellationRequested) return;
        context.Response.StatusCode = 503;
        await context.Response.WriteAsJsonAsync(new { error = "Backup policy backend is unavailable or incompatible; no local policy was overwritten" });
    }
});
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
    var expected = await CredentialFile.ReadAsync(project.ControlTokenFile, context.RequestAborted);
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
        version = typeof(Program).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0] ?? "0.1.8-dev",
        client_instance_id = worker.ClientInstanceId,
        project = new
        {
            project.ProjectId,
            project.Enabled,
            interval_hours = project.IntervalHours,
            policy_paused = project.PolicyPaused,
            applied_revision = project.ControlRevision,
            next_run_at = project.NextRunAt,
            reader = project.Reader is null ? null : new
            {
                root = project.Reader.Root,
                capability = ResourceReaderContract.Capability,
                credential_ready = File.Exists(project.Reader.SaturnTokenFile)
            },
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
    version = typeof(Program).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0] ?? "0.1.8-dev",
    client_instance_id = worker.ClientInstanceId,
    policy_protocol = 1
}));

app.MapMethods("/v1/projects/{projectId}/policy", ["GET", "PUT"], async (HttpContext context, string projectId, ServicePolicyClient policies) =>
{
    var project = await AuthorizedProjectAsync(context, projectId);
    var method = context.Request.Method == "GET" ? HttpMethod.Get : HttpMethod.Put;
    var body = method == HttpMethod.Put ? await ServicePolicyClient.ReadBodyAsync(context.Request.Body, context.RequestAborted) : null;
    return Results.Json(await policies.RequestAsync(project, method, body: body, cancellationToken: context.RequestAborted));
});

app.MapMethods("/v1/projects/{projectId}/policy/runs", ["GET", "POST"], async (HttpContext context, string projectId, ServicePolicyClient policies) =>
{
    var project = await AuthorizedProjectAsync(context, projectId);
    var method = context.Request.Method == "GET" ? HttpMethod.Get : HttpMethod.Post;
    var body = method == HttpMethod.Post ? await ServicePolicyClient.ReadBodyAsync(context.Request.Body, context.RequestAborted) : null;
    return Results.Json(await policies.RequestAsync(project, method, "/runs", body, context.RequestAborted));
});

foreach (var obsolete in new[] { "schedule", "runs", "mirror/schedule", "mirror/runs" })
{
    app.MapMethods("/v1/projects/{projectId}/" + obsolete, ["POST", "PUT"], async (HttpContext context, string projectId) =>
    {
        await AuthorizedProjectAsync(context, projectId);
        return Results.Json(new { error = "Use service-owned /policy with an expected revision and request ID", code = "policy_protocol_required" }, statusCode: 426);
    });
}

foreach (var (route, operation) in new[] { ("resources", "list"), ("resource-metadata", "metadata"), ("resource-content", "content") })
{
    app.MapGet("/api/v1/projects/{projectId}/" + route, async (HttpContext context, string projectId, ResourceReader reader) =>
    {
        var project = await AuthorizedProjectAsync(context, projectId);
        await reader.TransferAsync(context, project, operation);
    });
}

app.Run();

public sealed record ScheduleRequest(bool Enabled, int IntervalHours);
public sealed record MirrorScheduleRequest(bool Enabled, int IntervalMinutes);
