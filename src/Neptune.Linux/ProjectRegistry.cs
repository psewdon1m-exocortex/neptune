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

    public async Task UpsertAsync(ProjectRegistration registration, CancellationToken cancellationToken = default, bool preservePolicy = false)
    {
        registration.Validate();
        await _mutex.WaitAsync(cancellationToken);
        try
        {
            using var writer = await AcquireWriterAsync(cancellationToken);
            List<ProjectRegistration> projects;
            if (File.Exists(path))
            {
                await using var input = File.OpenRead(path);
                projects = await JsonSerializer.DeserializeAsync<List<ProjectRegistration>>(input, JsonOptions, cancellationToken) ?? [];
            }
            else projects = [];

            var previous = projects.SingleOrDefault(item => item.ProjectId == registration.ProjectId);
            if (preservePolicy && previous is not null)
                registration = registration with {
                    Enabled = registration.ArchiveAvailable && previous.Enabled, IntervalHours = previous.IntervalHours, NextRunAt = previous.NextRunAt,
                    ControlRevision = previous.ControlRevision, PolicyPaused = previous.PolicyPaused,
                    Mirror = registration.Mirror is not null && previous.Mirror is not null
                        ? registration.Mirror with { Enabled = previous.Mirror.Enabled, IntervalMinutes = previous.Mirror.IntervalMinutes,
                            NextRunAt = previous.Mirror.NextRunAt } : registration.Mirror
                };
            projects.RemoveAll(item => item.ProjectId == registration.ProjectId);
            projects.Add(registration);
            projects.Sort((a, b) => StringComparer.Ordinal.Compare(a.ProjectId, b.ProjectId));
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            var temporary = path + ".tmp";
            await using (var output = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None)) {
                await JsonSerializer.SerializeAsync(output, projects, JsonOptions, cancellationToken);
                output.Flush(flushToDisk: true);
            }
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
            Enabled = registration.ArchiveAvailable && enabled,
            IntervalHours = intervalHours,
            NextRunAt = registration.ArchiveAvailable && enabled ? DateTimeOffset.UtcNow.AddHours(intervalHours) : null
        }, cancellationToken);
    }

    public Task UpdateNextRunAsync(string projectId, DateTimeOffset nextRunAt, CancellationToken cancellationToken = default, long? expectedRevision = null) =>
        MutateAsync(projectId, registration => expectedRevision is not null && registration.ControlRevision != expectedRevision
            ? registration : registration with
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

    public Task UpdateMirrorNextRunAsync(string projectId, DateTimeOffset nextRunAt, CancellationToken cancellationToken = default, long? expectedRevision = null) =>
        MutateAsync(projectId, registration => registration.Mirror is null || expectedRevision is not null && registration.ControlRevision != expectedRevision
            ? registration
            : registration with { Mirror = registration.Mirror with { NextRunAt = registration.Mirror.Enabled ? nextRunAt : null } }, cancellationToken);

    public Task PrepareUnlinkAsync(string projectId, CancellationToken cancellationToken = default) =>
        MutateAsync(projectId, registration => registration with
        {
            Unlinking = true,
            Enabled = false,
            NextRunAt = null,
            Mirror = registration.Mirror is null ? null : registration.Mirror with { Enabled = false, NextRunAt = null }
        }, cancellationToken);

    public Task MarkRemoteDisconnectedAsync(string projectId, CancellationToken cancellationToken = default) =>
        MutateAsync(projectId, registration => registration with { RemoteDisconnected = true }, cancellationToken);

    public async Task RemoveAsync(string projectId, CancellationToken cancellationToken = default)
    {
        await _mutex.WaitAsync(cancellationToken);
        try
        {
            using var writer = await AcquireWriterAsync(cancellationToken);
            if (!File.Exists(path)) return;
            List<ProjectRegistration> projects;
            await using (var input = File.OpenRead(path))
                projects = await JsonSerializer.DeserializeAsync<List<ProjectRegistration>>(input, JsonOptions, cancellationToken) ?? [];
            projects.RemoveAll(item => item.ProjectId == projectId);
            var temporary = path + ".tmp";
            await using (var output = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                await JsonSerializer.SerializeAsync(output, projects, JsonOptions, cancellationToken);
                output.Flush(flushToDisk: true);
            }
            File.Move(temporary, path, overwrite: true);
            if (!OperatingSystem.IsWindows())
                File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead);
        }
        finally { _mutex.Release(); }
    }

    public Task ApplyRemoteDesiredAsync(
        string projectId,
        long revision,
        bool archiveEnabled,
        int archiveIntervalHours,
        bool mirrorEnabled,
        int mirrorIntervalMinutes,
        CancellationToken cancellationToken = default,
        bool paused = false) =>
        MutateAsync(projectId, registration =>
        {
            if (registration.Unlinking) return registration;
            if (revision < registration.ControlRevision) return registration;
            var now = DateTimeOffset.UtcNow;
            // Keep old readers safe too: paused intent lives in Saturn, while
            // the compatible local enabled flags govern actual execution.
            archiveEnabled = registration.ArchiveAvailable && archiveEnabled && !paused;
            mirrorEnabled = mirrorEnabled && !paused;
            var archiveChanged = registration.Enabled != archiveEnabled || registration.IntervalHours != archiveIntervalHours
                || registration.PolicyPaused && !paused;
            var mirror = registration.Mirror;
            if (mirror is not null)
            {
                var mirrorChanged = mirror.Enabled != mirrorEnabled || mirror.IntervalMinutes != mirrorIntervalMinutes
                    || registration.PolicyPaused && !paused;
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
                ControlRevision = revision,
                PolicyPaused = paused
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
            using var writer = await AcquireWriterAsync(cancellationToken);
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
            await using (var output = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None)) {
                await JsonSerializer.SerializeAsync(output, projects, JsonOptions, cancellationToken);
                output.Flush(flushToDisk: true);
            }
            File.Move(temporary, path, overwrite: true);
            if (!OperatingSystem.IsWindows())
                File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead);
        }
        finally { _mutex.Release(); }
    }

    private async Task<FileStream> AcquireWriterAsync(CancellationToken token)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        for (var attempt = 0; ; attempt++)
        {
            token.ThrowIfCancellationRequested();
            try { return new FileStream(path + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None); }
            catch (IOException) when (attempt < 100) { await Task.Delay(50, token); }
        }
    }
}
