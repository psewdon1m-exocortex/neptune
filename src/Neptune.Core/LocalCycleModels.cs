using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

// Agreement is at processed historical event boundaries. It is not a claim
// that unreported cached user writes, open writers or future events do not exist.
public sealed record WindowsCycleConfiguration(string MappingId, string ClientId, string ProfileId,
    string RulesVersion, Uri ServerOrigin, string DeviceId, string CredentialScope,
    string BackendIdentity, SyncRootBinding Binding, bool AllowVerifiedEqualAdoption = false, string? RemoteAnchorId = null);
public sealed record WindowsCycleBudget(int DataJobs = 4, int LargeJobs = 2,
    long LargeFileThreshold = 64L * 1024 * 1024, long SpoolBytes = 1024L * 1024 * 1024,
    long MinimumFreeDiskBytes = 40L * 1024 * 1024 * 1024, long ProfileBytes = 16L * 1024 * 1024 * 1024,
    long? MaximumNewFileBytes = null);
public enum LocalCycleWorkPhase { Queued, Sealed, Prepared, Sending, OutcomeUnknown, Published, AdoptedEqual, Conflict, Deferred, Failed }
public sealed record LocalCycleGeneration(long Journal, long Watcher);
public sealed record LocalCycleEntry(string NodeId, string ParentId, string RelativePath,
    string RawName, long Created, bool Directory, long EnumeratedSize,
    LocalCycleGeneration Generation, string? BaselineJson, string? RemoteNodeId)
{
    // NodeId remains the immutable ledger key. A qualified same-path atomic
    // save may replace only the current native incarnation used for capture.
    public string? NativeNodeId { get; init; }
    internal string SourceNodeId => NativeNodeId ?? NodeId;
}
public sealed record LocalCycleWork(string OperationId, string ScopeId, LocalCycleEntry Entry,
    LocalCycleWorkPhase Phase, string? SpoolPath, long? Size, string? Sha256,
    string? CaptureProof, string? InputJson, string? IntentHash, string? UploadId,
    long ReceivedBytes, string? ResultJson, string? Error);
public sealed record LocalCycleRemoteProgress(string? SnapshotId, string? ReadyJson,
    string? EntriesCursor, string? AfterNodeId, string? ChangesCursor, long Sequence,
    bool Complete, string? Error, long RequestSequence = 0, bool SnapshotComplete = false);
public sealed record LocalCycleBaseline(SyncDescriptor Description, string CaptureProof,
    LocalCycleGeneration Generation, long RemoteSequence, long WorkNumber);
public sealed record WindowsCycleResult(string ScopeId, string? CoherenceId,
    string NamespaceCoverage, string HistoricalByteAgreement, string Decision,
    long AppliedLocalUsn, long AppliedRemoteSequence, long PendingEntries,
    long CapturedFiles, long CapturedBytes, long ReceivedBytes, long PublishedFiles,
    long PublishedDirectories, long Conflicts, long Deferred, string? Error)
{public string? RetryState{get;init;}}
public enum WindowsCycleStage { ScopeOpened, BranchEnumerated, FileCaptured, FilePublished, DirectoryPublished, CycleObserved }
public sealed record WindowsCycleObservation(WindowsCycleStage Stage, string ScopeId,
    string? OperationId = null, string? RelativePath = null);
public sealed record WindowsCycleHistoryRetirement(LocalCycleHistoryRetirement Work,LocalCoherenceRetirement Journal);
public sealed record LocalCycleCaptureEvidence(string Schema, string ScopeId, string OperationId,
    string NodeId, long Created, string ParentId, string RelativePath, long JournalGeneration,
    long WatcherGeneration, string RootFileId, long RootCreated, string VolumeGuid,
    string LegacyVolumeSerial, string FileId64, string SizeBytes, string Sha256,
    string LastFileUsn, string Nonce);
public sealed class LocalCycleSeal
{
    internal LocalCycleSeal(string path, long size, string hash, string proof)
        => (SpoolPath, Size, Sha256, ProofJson) = (path, size, hash, proof);
    public string SpoolPath { get; }
    public long Size { get; }
    public string Sha256 { get; }
    public string ProofJson { get; }
}
public sealed record CatalogParentTopology(string NodeId, string? ParentId, string Name);
public sealed record CycleFolderInput(string OperationId,string IdempotencyKey,string RootId,string Epoch,
    string BindingVersion,string ParentId,string Name,string Mode,IReadOnlyList<CatalogParentTopology> ParentTopology);
public sealed record CycleFolderOutcome(SyncDescriptor Description,string RawJson,string IntentHash);
public sealed record CycleUploadCondition(string Kind, string? NodeId = null, string? MetadataVersion = null);
public sealed record CycleUploadIntent(string Schema, string DeviceKind, string DeviceId,
    string RootId, string RequestPath, string IdempotencyKey, string ParentId, string Name,
    string? TargetId, string SizeBytes, string Sha256, string? IfMatch, string? IfNoneMatch,
    bool RequireAbsent, string? ExpectedVersionId, CycleUploadCondition Condition,
    IReadOnlyList<CatalogParentTopology> ParentTopology);
public sealed record CycleUploadInput(string Epoch, string BindingVersion, string BackendIdentity,
    CycleUploadIntent Intent);
public sealed record CycleUploadStatus(string UploadId, string OperationId, string RootId,
    string IntentHash, long ExpectedBytes, long ReceivedBytes, string Phase,
    bool TransferPending, bool Replayed, string? ResultJson, SyncDescriptor? PublishedDescription,
    long? FirstSequence, long? LastSequence);

// Tests may supply a transport; actual interoperability acceptance requires
// SaturnGuardedCycleTransport and a separately qualified/enrolled server root.
public interface IWindowsCycleRemote
{
    SyncRootBinding Binding { get; }
    Task<byte[]> StartSnapshotAsync(string snapshotId, CancellationToken cancellationToken);
    Task<byte[]> StepSnapshotAsync(string snapshotId, CancellationToken cancellationToken);
    Task<SyncSnapshotReady> ReadySnapshotAsync(string snapshotId, CancellationToken cancellationToken);
    Task<SyncSnapshotEntriesPage> ReadEntriesAsync(SyncSnapshotReady ready, string cursor, string? after,
        CancellationToken cancellationToken);
    Task<SyncFeedPage> ReadChangesAsync(string cursor, long after, CancellationToken cancellationToken);
    Task<CycleFolderOutcome> EnsureDirectoryAsync(CycleFolderInput original,
        CancellationToken cancellationToken);
    Task<CycleUploadStatus> UploadAsync(string command, CycleUploadInput original,
        string? uploadId, string originalIntentHash, CancellationToken cancellationToken);
    Task<CycleUploadStatus> AppendAsync(CycleUploadInput original, string uploadId,
        string originalIntentHash, long offset, ReadOnlyMemory<byte> bytes,
        CancellationToken cancellationToken);
}
