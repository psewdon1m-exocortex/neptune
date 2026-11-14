namespace Neptune.Core;

// EnumerationComplete proves only successful metadata enumeration. It is not
// the capture/scan/replay cut needed for destructive cleanup or steady state.
public enum BootstrapScanStatus { Scanning, EnumerationComplete, Incomplete, Interrupted }
public sealed record BootstrapScanState(string ScanId, string MappingId, string OwnerToken, string SourceRoot,
    BootstrapScanStatus Status, long Files, long Directories, long Bytes, string? Error, long Generation = 0)
{
    public bool DestructiveCleanupAllowed => false;
}
public sealed record BootstrapDirectory(string ScanId, long DirectoryId, string RelativePath, string OwnerToken);
public sealed record BootstrapSourceEntry(string RawName, bool IsDirectory, long Size, DateTimeOffset ModifiedAt);
public sealed record BootstrapEligibleEntry(long EntryId, long ParentDirectoryId, string RelativePath, bool IsDirectory,
    long Size, DateTimeOffset ModifiedAt);
public sealed record BootstrapDirectoryStamp(DateTimeOffset CreatedAt, DateTimeOffset ModifiedAt);

public sealed record BootstrapFrontierBatch(long ParentDirectoryId, int AddedDirectories, bool ParentComplete, long Cursor);
public sealed record BootstrapRetirementBatch(string? ScanId, int RemovedEntries, int RemovedDirectories, bool RemovedGeneration);
public sealed record BootstrapWriteMetrics(long Transactions, long MaximumChangedRows, double MaximumCommitMilliseconds,
    long LastChangedRows = 0, double LastCommitMilliseconds = 0);
public sealed record BootstrapCheckpointResult(bool Attempted, long PhysicalWalBytes, long LogFrames, long CheckpointedFrames,
    long UncheckpointedBytes, bool Busy, bool Backpressure);
public sealed record BootstrapCheckpointMetrics(long Attempts, long AdmissionStops, BootstrapCheckpointResult? LastMeasured);
public sealed class BootstrapStorageBackpressureException(BootstrapCheckpointResult checkpoint)
    : IOException("Bootstrap metadata admission stopped because uncheckpointed WAL exceeded its 64 MiB budget.")
{
    public BootstrapCheckpointResult Checkpoint { get; } = checkpoint;
}

public interface IBootstrapScanSource
{
    BootstrapDirectoryStamp ReadDirectoryStamp(string fullPath);
    IEnumerable<BootstrapSourceEntry> EnumerateDirectory(string fullPath);
}
