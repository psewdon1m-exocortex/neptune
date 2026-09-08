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
    DateTimeOffset? NextRunAt = null)
{
    public ProjectRegistration Validate()
    {
        if (string.IsNullOrWhiteSpace(ProjectId) || ProjectId.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '-' or '_')))
            throw new ArgumentException("ProjectId must contain only ASCII letters, digits, '-' or '_'.", nameof(ProjectId));
        if (!ExportUri.IsLoopback)
            throw new ArgumentException("Project backup export endpoint must be loopback-only.", nameof(ExportUri));
        if (IntervalHours is < 1 or > 8760)
            throw new ArgumentOutOfRangeException(nameof(IntervalHours), "Interval must be between 1 and 8760 hours.");
        return this;
    }
}

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
