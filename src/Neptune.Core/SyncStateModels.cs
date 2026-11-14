namespace Neptune.Core;

// These are durable local state contracts, not the future wire protocol. A
// missing baseline/remote row means UNKNOWN, never a proven absent destination.
public enum SyncEntryKind { File, Directory }
public enum SyncEntryPresence { Unknown, Present, Absent }
public enum SyncIntentKind { PutFile, EnsureDirectory, AdoptEqual }
public enum SyncExpectedKind { Absent, Version }
public enum SyncOperationStage { LocalPlanned, Ready, Sending, OutcomeUnknown, RetryWait, CancelRequested, Cancelled, Conflict, ClientAcked, Superseded }

[Flags]
public enum SyncDirtyReason { None = 0, LocalChange = 1, Rename = 2, JournalGap = 4, LegacyWrite = 8, Retry = 16, RemoteChange = 32 }

public sealed record SyncRootIdentity(string MappingId, string ClientInstanceId, string ProfileId, string DeviceId,
    Uri ServerOrigin, string ServerRootId, string RootEpoch, string SourceVolumeIdentity, string SourceRootIdentity,
    string RulesVersion, string NameCodec = "legacy", string Mode = "upload_mirror_guarded");

public sealed record SyncRootState(string MappingId, SyncRootIdentity? Identity, long LegacyRevision,
    long InvalidationGeneration, bool NeedsReconciliation, bool MappingEnabled);

public sealed record SyncLocalEntry(string EntryId, string? ParentEntryId, string RawName, string NameKey,
    SyncEntryKind Kind, string? FileSystemId, long IdentityGeneration, long Size, DateTimeOffset ModifiedAt,
    long LocalGeneration, string? Sha256, long? HashGeneration, SyncEntryPresence Presence, string RulesVersion);

public sealed record SyncBaseline(long AgreedLocalGeneration, string RemoteNodeId, string RemoteVersion, string RootEpoch,
    long PublishedSequence, string? Sha256, long Size, SyncEntryKind Kind, string ParentRemoteNodeId, string RawName,
    long InvalidationGeneration);

public sealed record SyncRemoteEntry(string NodeId, string? ParentNodeId, string RawName, string NameKey, SyncEntryKind Kind,
    long Size, string? Sha256, string Version, long LastSequence, bool Deleted, bool Authoritative = true);

public sealed record SyncEntryState(SyncLocalEntry? Local, SyncBaseline? Baseline, SyncRemoteEntry? Remote);
public sealed record SyncDirtyHint(string EntryId, long ObservedGeneration, SyncDirtyReason Reasons, DateTimeOffset FirstDirtyAt, DateTimeOffset DueAt);
public sealed record SyncJournalCursor(string JournalId, long Position, bool Valid);

public sealed record SyncExpectedDestination(SyncExpectedKind Kind, string? NodeId = null, string? Version = null);
public sealed record SyncOperationIntent(string OperationId, string MappingId, string EntryId, long LocalGeneration,
    string RootEpoch, SyncIntentKind Kind, string RulesVersion, SyncExpectedDestination Expected, string ParentRemoteNodeId,
    string RawName, string NameKey, long Size, string? Sha256, string? SourceSnapshotIdentity, IReadOnlyList<string> Dependencies);

public sealed record SyncDurableOperation(SyncOperationIntent Intent, string Fingerprint, SyncOperationStage Stage,
    long InvalidationGeneration, long LegacyRevision, int Attempt, string? WorkerId, DateTimeOffset? LeaseUntil,
    SyncPublicationReceipt? Receipt, SyncCancellationFence? CancellationFence = null);

// Supplied only by an adapter with a durable registry/tombstone capability;
// momentary absence is not a cancellation proof or permission for a new ID.
public sealed record SyncCancellationFence(string OperationId, string ServerRootId, string RootEpoch, string IntentFingerprint, string FenceId);

public sealed record SyncPublicationReceipt(string OperationId, string ServerRootId, string RootEpoch, string IntentFingerprint,
    long LocalGeneration, string NodeId, string Version, long PublishedSequence, string? Sha256, long Size,
    bool Published, string Durability);
