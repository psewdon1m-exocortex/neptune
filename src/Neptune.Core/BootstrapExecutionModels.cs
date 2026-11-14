namespace Neptune.Core;

// Opt-in legacy CREATE_ONLY execution. None of these records creates an L/B/R
// baseline, server epoch, continuous source cut or permission to delete.
public enum BootstrapWorkPhase { Queued, Captured, Transferring, Received, Published, ObservedEqual, Conflict, Failed, Deferred }
public enum BootstrapPublicationProof { QualifiedImmutableReceipt, LegacyObservedActive, ObservedEqual, Uncertain }
public enum BootstrapReceiptPolicy { LegacyOnly, ValidateWhenPresent, RequireImmutable }
public sealed record BootstrapReceiptExpectation(string AssignedRootId,
    BootstrapReceiptPolicy Policy = BootstrapReceiptPolicy.ValidateWhenPresent)
{
    internal BootstrapReceiptExpectation Validated()
    {
        if (!BootstrapUploadReceiptParser.IsUuid(AssignedRootId) || !Enum.IsDefined(Policy))
            throw new ArgumentException("Receipt expectation requires an enrolled root UUID and supported policy.");
        return this with { AssignedRootId = Guid.Parse(AssignedRootId).ToString("D") };
    }
}
// Historical server publication, not current R/CAS/B, presence or a root cut.
public sealed record SaturnUploadPublicationReceipt(string Schema, string UploadId, string OperationId, string IntentHash,
    string RootId, string ResourceId, string VersionId, string ParentId, string Name, string SizeBytes,
    string Sha256, string Etag, string PublishedAt);
public sealed record BootstrapExecutionLimits(int DataJobs = 4, int LargeJobs = 2, long LargeFileThreshold = 16 * 1024 * 1024,
    int ReadyQueueCapacity = 16, long SpoolBudgetBytes = 1024L * 1024 * 1024, long DiskFreeFloorBytes = 1024L * 1024 * 1024,
    int MaximumSpoolFiles = 64)
{
    public void Validate()
    {
        if (DataJobs is < 1 or > 4 || LargeJobs is < 1 or > 2 || LargeJobs > DataJobs || LargeFileThreshold < 1024 * 1024
            || ReadyQueueCapacity is < 1 or > 256 || SpoolBudgetBytes < 1 || DiskFreeFloorBytes < 0 || MaximumSpoolFiles is < 1 or > 256)
            throw new ArgumentOutOfRangeException(nameof(BootstrapExecutionLimits));
    }
}
public sealed record BootstrapExecutionRequest(string MappingId, string ClientInstanceId, string SourceRoot, string SpoolDirectory,
    BootstrapExecutionLimits? Limits = null);
public sealed record BootstrapSealedSource(string SpoolPath, long Size, string Sha256, string SourceIdentity);
public sealed record BootstrapWorkRecord(long WorkId, string SessionId, string ScanId, long ScanGeneration, long EntryId,
    string OperationId, string MappingId, string SourceRoot, string TargetRoot, string CredentialScope, string RelativePath,
    bool IsDirectory, long EnumeratedSize, DateTimeOffset EnumeratedModifiedAt, BootstrapWorkPhase Phase,
    string? SpoolPath = null, long? SealedSize = null, string? Sha256 = null, string? SourceIdentity = null,
    int Attempt = 0, string? TransportMode = null, string? UploadId = null, long ReceivedBytes = 0,
    string? Receipt = null, string? Error = null, string? ClientInstanceId = null,
    string? ExpectedServerRootId = null, BootstrapReceiptPolicy ReceiptPolicy = BootstrapReceiptPolicy.LegacyOnly,
    string? OriginalIntentHash = null, string? ReceiptRequestPath = null, string? NativeCaptureProofJson = null);
public sealed record BootstrapTransferCheckpoint(string TransportMode, string? UploadId, long ReceivedBytes, string? IntentHash = null);
public sealed record BootstrapPublicationReceipt(string OperationId, string TargetUri, string RelativePath, bool IsDirectory,
    long Size, string? Sha256, string? Etag, BootstrapPublicationProof Proof,
    SaturnUploadPublicationReceipt? ServerReceipt = null, string? ImmutableReceiptJson = null)
{
    // Only historical publication authority; never trusted baseline adoption.
    public bool ImmutablePublicationAuthority => Proof == BootstrapPublicationProof.QualifiedImmutableReceipt;
}
public sealed record BootstrapExecutionResult(string SessionId, BootstrapScanState? Scan, long QueuedEntries,
    long PublishedFiles, long PublishedDirectories, long ObservedEqualFiles, long ReceivedBytes, long PublishedBytes,
    long Conflicts, long Failures, long Deferred, bool Cancelled, string? ScanError, int MaximumDataJobs,
    int MaximumLargeJobs, int MaximumSourceReaders, int MaximumReadyQueue, long MaximumSpoolBytes, int PostCommitDiagnostics = 0)
{
    public bool DestructiveCleanupAllowed => false;
}
public interface ICreateOnlyBootstrapTransport
{
    Uri TargetRoot { get; }
    string CredentialScope { get; }
    BootstrapReceiptExpectation? ReceiptExpectation => null;
    Task<BootstrapPublicationReceipt> TransferAsync(BootstrapWorkRecord work,
        Func<BootstrapTransferCheckpoint, Task> checkpoint, CancellationToken cancellationToken);
}
public interface IBootstrapSourceCapture
{
    Task ValidateDirectoryAsync(string sourceRoot, string relativePath, CancellationToken cancellationToken);
    Task<BootstrapSealedSource> CaptureAsync(string sourceRoot, string relativePath, string spoolPath,
        long maximumBytes, CancellationToken cancellationToken);
}
public sealed class BootstrapCreateOnlyConflictException(string message) : IOException(message);
public sealed class BootstrapCaptureDeferredException(string message) : IOException(message);
public sealed class BootstrapPublicationUncertainException(string message) : IOException(message);
