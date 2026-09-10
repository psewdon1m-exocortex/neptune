namespace Neptune.Core;

public sealed record NeptunePaths(
    Uri SaturnOrigin,
    string BackupIngestPath,
    string SyncPath,
    Uri NeptuneRepository);

public sealed record KernelRegisterSnapshot(
    string Schema,
    long Revision,
    string Checksum,
    DateTimeOffset PublishedAt,
    DateTimeOffset? ValidUntil,
    IReadOnlyDictionary<string, object?> Values);

public sealed record ProjectRegistration(
    string ProjectId,
    Uri ExportUri,
    string ControlTokenFile,
    string ExportTokenFile,
    string SaturnTokenFile,
    string SaturnSlugRegisterKey,
    bool Enabled,
    int IntervalHours,
    DateTimeOffset? NextRunAt = null,
    string? SaturnSlug = null,
    MirrorRegistration? Mirror = null,
    long ControlRevision = 0)
{
    public ProjectRegistration Validate()
    {
        if (string.IsNullOrWhiteSpace(ProjectId) || ProjectId.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '-' or '_')))
            throw new ArgumentException("ProjectId must contain only ASCII letters, digits, '-' or '_'.", nameof(ProjectId));
        if (!ExportUri.IsLoopback)
            throw new ArgumentException("Project backup export endpoint must be loopback-only.", nameof(ExportUri));
        if (IntervalHours is < 1 or > 8760)
            throw new ArgumentOutOfRangeException(nameof(IntervalHours), "Interval must be between 1 and 8760 hours.");
        if (string.IsNullOrWhiteSpace(SaturnSlugRegisterKey) && string.IsNullOrWhiteSpace(SaturnSlug))
            throw new ArgumentException("A Saturn producer slug or Register key is required.", nameof(SaturnSlug));
        if (!string.IsNullOrWhiteSpace(SaturnSlug) && SaturnSlug.Any(c => !(char.IsAsciiLetterOrDigit(c) || c == '-')))
            throw new ArgumentException("SaturnSlug must contain only ASCII letters, digits or '-'.", nameof(SaturnSlug));
        Mirror?.Validate();
        return this;
    }
}

public sealed record MirrorRegistration(
    Uri ExportUri,
    string SaturnRoot,
    string SaturnTokenFile,
    string Mode,
    string? TargetFilename,
    bool Enabled,
    int IntervalMinutes,
    DateTimeOffset? NextRunAt = null)
{
    public MirrorRegistration Validate()
    {
        if (!ExportUri.IsLoopback)
            throw new ArgumentException("Mirror export endpoint must be loopback-only.", nameof(ExportUri));
        if (SaturnRoot is not ("volt" or "mastermind"))
            throw new ArgumentException("Mirror root must be volt or mastermind.", nameof(SaturnRoot));
        if (Mode is not ("single-file" or "zip-tree"))
            throw new ArgumentException("Mirror mode must be single-file or zip-tree.", nameof(Mode));
        if (Mode == "single-file" && (string.IsNullOrWhiteSpace(TargetFilename) || TargetFilename.IndexOfAny(['/', '\\']) >= 0))
            throw new ArgumentException("Single-file mirrors require a safe target filename.", nameof(TargetFilename));
        if (Mode == "zip-tree" && !string.IsNullOrWhiteSpace(TargetFilename))
            throw new ArgumentException("Tree mirrors do not use a target filename.", nameof(TargetFilename));
        if (IntervalMinutes is < 1 or > 10_080)
            throw new ArgumentOutOfRangeException(nameof(IntervalMinutes));
        if (string.IsNullOrWhiteSpace(SaturnTokenFile))
            throw new ArgumentException("Mirror token file is required.", nameof(SaturnTokenFile));
        return this;
    }
}

public sealed record MirrorRunStatus(
    string State,
    DateTimeOffset? LastAttemptAt,
    DateTimeOffset? LastSuccessAt,
    string? Error,
    int UploadedFiles,
    int DeletedEntries);

public sealed record SaturnRunCreated(string Id, string State, long ReceivedSize);

public sealed record SaturnBackupCapabilities(
    string Schema,
    int ProtocolVersion,
    bool Resumable,
    string Checksum,
    int MaxChunkBytes,
    bool ArchiveEncryptionDeclaredPerRun);

public sealed record BackupReceipt(
    string Schema,
    string RunId,
    string ServiceId,
    string ServiceSlug,
    string LogicalPath,
    long SizeBytes,
    string Sha256,
    DateTimeOffset CommittedAt);

public sealed record ProjectRunSummary(BackupRun? Latest, DateTimeOffset? LastSuccessAt);

public sealed record BackupRun(
    string RunId,
    string ProjectId,
    string ClientInstanceId,
    string State,
    string? SpoolPath,
    long? Size,
    string? Sha256,
    string? SaturnRunId,
    long UploadedBytes,
    int Attempt,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    string? Error);

public sealed record SyncMapping(
    string MappingId,
    string ClientInstanceId,
    string LocalPath,
    bool Enabled,
    DateTimeOffset CreatedAt);

public sealed record SyncFileRecord(
    string MappingId,
    string RelativePath,
    long LocalSize,
    DateTimeOffset LocalModifiedAt,
    string Sha256,
    string? RemoteEtag,
    string State,
    DateTimeOffset UpdatedAt,
    string? Error);

public sealed record NeptuneStatus(
    string Product,
    string Version,
    string ClientInstanceId,
    bool RegisterAvailable,
    int ActiveRuns,
    int QueuedRuns);

public sealed record RemoteCommandResult(string Id, string State, string? Error);

public sealed record RemoteCommand(string Id, string Kind, IReadOnlyDictionary<string, System.Text.Json.JsonElement> Payload);

public sealed record RemoteDesiredState(
    long Revision,
    bool ArchiveEnabled,
    int ArchiveIntervalHours,
    bool MirrorEnabled,
    int MirrorIntervalMinutes,
    string? Version);

public sealed record RemoteControlResponse(
    string Schema,
    RemoteDesiredState Desired,
    IReadOnlyList<RemoteCommand> Commands);
