using System.Text.Json;
using Neptune.Core;

namespace Neptune.Linux;

public sealed class ProjectRegistry(string path)
{
    private readonly SemaphoreSlim _mutex = new(1, 1);
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    public async Task<IReadOnlyList<ProjectRegistration>> ReadAsync(CancellationToken cancellationToken = default)
    {
        await _mutex.WaitAsync(cancellationToken);
        try
        {
            if (!File.Exists(path)) return [];
            await using var stream = File.OpenRead(path);
            return (await JsonSerializer.DeserializeAsync<List<ProjectRegistration>>(stream, JsonOptions, cancellationToken) ?? [])
                .Select(item => item.Validate()).ToArray();
        }
        finally { _mutex.Release(); }
    }

    public async Task<ProjectRegistration?> FindAsync(string projectId, CancellationToken cancellationToken = default) =>
        (await ReadAsync(cancellationToken)).SingleOrDefault(item => item.ProjectId == projectId);

    public async Task UpsertAsync(ProjectRegistration registration, CancellationToken cancellationToken = default)
    {
        registration.Validate();
        await _mutex.WaitAsync(cancellationToken);
        try
        {
            List<ProjectRegistration> projects;
            if (File.Exists(path))
            {
                await using var input = File.OpenRead(path);
                projects = await JsonSerializer.DeserializeAsync<List<ProjectRegistration>>(input, JsonOptions, cancellationToken) ?? [];
            }
            else projects = [];

            projects.RemoveAll(item => item.ProjectId == registration.ProjectId);
            projects.Add(registration);
            projects.Sort((a, b) => StringComparer.Ordinal.Compare(a.ProjectId, b.ProjectId));
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            var temporary = path + ".tmp";
            await using (var output = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None))
                await JsonSerializer.SerializeAsync(output, projects, JsonOptions, cancellationToken);
            File.Move(temporary, path, overwrite: true);
            if (!OperatingSystem.IsWindows())
                File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead);
        }
        finally { _mutex.Release(); }
    }

    public async Task UpdateScheduleAsync(string projectId, bool enabled, int intervalHours, CancellationToken cancellationToken = default)
    {
        if (intervalHours is < 1 or > 8760) throw new ArgumentOutOfRangeException(nameof(intervalHours));
        await MutateAsync(projectId, registration => registration with
        {
            Enabled = enabled,
            IntervalHours = intervalHours,
            NextRunAt = enabled ? DateTimeOffset.UtcNow.AddHours(intervalHours) : null
        }, cancellationToken);
    }

    public Task UpdateNextRunAsync(string projectId, DateTimeOffset nextRunAt, CancellationToken cancellationToken = default) =>
        MutateAsync(projectId, registration => registration with
        {
            NextRunAt = registration.Enabled ? nextRunAt : null
        }, cancellationToken);

    public Task UpdateMirrorScheduleAsync(string projectId, bool enabled, int intervalMinutes, CancellationToken cancellationToken = default)
    {
        if (intervalMinutes is < 1 or > 10_080) throw new ArgumentOutOfRangeException(nameof(intervalMinutes));
        return MutateAsync(projectId, registration => registration.Mirror is null
            ? throw new InvalidOperationException($"Project '{projectId}' has no mirror pipeline.")
            : registration with
            {
                Mirror = registration.Mirror with
                {
                    Enabled = enabled,
                    IntervalMinutes = intervalMinutes,
                    NextRunAt = enabled ? DateTimeOffset.UtcNow.AddMinutes(intervalMinutes) : null
                }
            }, cancellationToken);
    }

    public Task UpdateMirrorNextRunAsync(string projectId, DateTimeOffset nextRunAt, CancellationToken cancellationToken = default) =>
        MutateAsync(projectId, registration => registration.Mirror is null
            ? registration
            : registration with { Mirror = registration.Mirror with { NextRunAt = registration.Mirror.Enabled ? nextRunAt : null } }, cancellationToken);

    public Task ApplyRemoteDesiredAsync(
        string projectId,
        long revision,
        bool archiveEnabled,
        int archiveIntervalHours,
        bool mirrorEnabled,
        int mirrorIntervalMinutes,
        CancellationToken cancellationToken = default) =>
        MutateAsync(projectId, registration =>
        {
            if (revision <= registration.ControlRevision) return registration;
            var now = DateTimeOffset.UtcNow;
            var archiveChanged = registration.Enabled != archiveEnabled || registration.IntervalHours != archiveIntervalHours;
            var mirror = registration.Mirror;
            if (mirror is not null)
            {
                var mirrorChanged = mirror.Enabled != mirrorEnabled || mirror.IntervalMinutes != mirrorIntervalMinutes;
                mirror = mirror with
                {
                    Enabled = mirrorEnabled,
                    IntervalMinutes = mirrorIntervalMinutes,
                    NextRunAt = mirrorEnabled ? (mirrorChanged ? now.AddMinutes(mirrorIntervalMinutes) : mirror.NextRunAt) : null
                };
            }
            return registration with
            {
                Enabled = archiveEnabled,
                IntervalHours = archiveIntervalHours,
                NextRunAt = archiveEnabled ? (archiveChanged ? now.AddHours(archiveIntervalHours) : registration.NextRunAt) : null,
                Mirror = mirror,
                ControlRevision = revision
            };
        }, cancellationToken);

    private async Task MutateAsync(
        string projectId,
        Func<ProjectRegistration, ProjectRegistration> mutate,
        CancellationToken cancellationToken)
    {
        await _mutex.WaitAsync(cancellationToken);
        try
        {
            if (!File.Exists(path))
                throw new KeyNotFoundException($"Project '{projectId}' is not registered.");

            List<ProjectRegistration> projects;
            await using (var input = File.OpenRead(path))
                projects = await JsonSerializer.DeserializeAsync<List<ProjectRegistration>>(input, JsonOptions, cancellationToken) ?? [];

            var index = projects.FindIndex(item => item.ProjectId == projectId);
            if (index < 0)
                throw new KeyNotFoundException($"Project '{projectId}' is not registered.");
            projects[index] = mutate(projects[index]).Validate();

            var temporary = path + ".tmp";
            await using (var output = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None))
                await JsonSerializer.SerializeAsync(output, projects, JsonOptions, cancellationToken);
            File.Move(temporary, path, overwrite: true);
            if (!OperatingSystem.IsWindows())
                File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead);
        }
        finally { _mutex.Release(); }
    }
}
