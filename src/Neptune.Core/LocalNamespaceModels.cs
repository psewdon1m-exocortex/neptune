namespace Neptune.Core;

// Metadata observations are a candidate namespace, never a continuous source
// cut, common B, a closed writer cycle, or authority for cleanup/NOOP.
public sealed record LocalNamespaceHandleProof(string FileId128, string LegacyFileId64,
    string VolumeSerial64, uint LegacyVolumeSerial, long CreationTime, uint Attributes,
    bool CaseSensitive);
public sealed record LocalNamespaceRootProof(string RootPath, string VolumeGuid,
    string FileSystem, LocalNamespaceHandleProof Handle);
public sealed record LocalNamespaceEntry(string Name, string FileId128, long CreationTime,
    long LastWriteTime, long ChangeTime, long Size, uint Attributes, uint ReparseTag)
{
    public bool IsDirectory => (Attributes & 0x10) != 0;
    public bool Excluded => (Attributes & (0x400 | 0x1000 | 0x40000 | 0x400000)) != 0;
}
public sealed record LocalNamespacePage(IReadOnlyList<LocalNamespaceEntry> Entries, bool End);
public sealed record LocalNamespaceDirectory(string CandidateId, string FileId128,
    string RelativePath, long CreationTime, string ClaimToken);
public enum LocalNamespaceStatus { Enumerating, EnumerationComplete, Incomplete, Superseded }
public sealed record LocalNamespaceCandidate(string CandidateId, LocalNamespaceStatus Status,
    string RootFileId128, long OwnerEpoch, long ObservedIngestStart, long LegacyRevision,
    long Entries, long Bytes, long ValidatedParents, long Exclusions, long EligibleThrough,
    string? Error)
{
    public string LocalCompleteness => "Unknown";
    public string SourceCycle => "Unknown";
}
public sealed record LocalNamespaceAdmission(long MaximumCandidateBytes = 1024L * 1024 * 1024,
    long MaximumProfileBytes = 1024L * 1024 * 1024, long MinimumFreeDiskBytes = 1024L * 1024 * 1024);
public sealed record LocalNamespaceWrite(int Entries, long ChangedRows);
public sealed record LocalNamespaceEligibleEntry(long EntryId, string ParentId128, LocalNamespaceEntry Metadata);
public sealed record LocalNamespaceRefill(int Added, bool Complete, long ChangedRows);
public sealed record LocalNamespaceRetirement(int Rows, long Bytes, bool Removed, long ChangedRows);
public sealed record LocalNamespaceMetrics(long Transactions, long MaximumChangedRows, long MaximumCommitTicks);

public interface ILocalNamespaceSource : IAsyncDisposable
{
    LocalNamespaceRootProof RootProof { get; }
    ValueTask<ILocalNamespaceDirectoryReader> OpenDirectoryAsync(LocalNamespaceDirectory directory,
        CancellationToken cancellationToken = default);
}
public interface ILocalNamespaceDirectoryReader : IAsyncDisposable
{
    LocalNamespaceHandleProof Proof { get; }
    ValueTask<LocalNamespacePage> ReadPageAsync(CancellationToken cancellationToken = default);
    ValueTask ValidateUnchangedAsync(CancellationToken cancellationToken = default);
}
