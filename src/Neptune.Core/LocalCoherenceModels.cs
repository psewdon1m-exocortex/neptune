namespace Neptune.Core;

// Namespace membership at an observed historical boundary is not a byte seal,
// common baseline, closed writer cycle, current remote R, or permission to delete.
public enum LocalCoherenceStatus { Building, Anchored, Uncertain, Superseded }
public sealed record LocalCoherenceState(string CoherenceId,string CandidateId,
    LocalCoherenceStatus Status,string JournalId,long StartUsn,long CutUsn,long AppliedUsn,
    long OwnerEpoch,long LegacyRevision,long GapEpoch,long MatchedEntries,long VerifiedParents,
    long DirtyNodes,long Unresolved,long OutsideRecords,long ProcessedRecords,long Bytes,string? Error)
{
    public string NamespaceCoverage=>Status==LocalCoherenceStatus.Anchored?"QualifiedAtAppliedBoundary":"Unknown";
    public string SourceCycle=>"Unknown";
    public string LocalCompleteness=>"Unknown";
    public string Baseline=>"Unknown";
}
public sealed record LocalDirectoryAlias(string Name,string FileId64,long CreationTime,uint Attributes)
{public bool IsDirectory=>(Attributes&0x10)!=0;}
public sealed record LocalDirectoryAliasPage(IReadOnlyList<LocalDirectoryAlias> Entries,bool End);
public sealed record LocalCoherenceApply(bool Replayed,int Records,int Dirty,int Outside,int Unresolved,
    long AppliedUsn,long ChangedRows);
public sealed record LocalCoherenceDirty(string NodeId128,long CreationTime,bool Directory,
    long Generation,long LastUsn,uint Reasons);
public sealed record LocalCoherenceUnresolved(long Usn,string Code,LocalUsnRecord Record,string Sha256);
public sealed record LocalCoherenceRetirement(int Rows,long Bytes,bool Removed,long ChangedRows);
public sealed record LocalCoherenceMetrics(long Transactions,long MaximumChangedRows,long MaximumCommitTicks);
public sealed record LocalCoherenceAdmission(long MaximumEvidenceBytes=1024L*1024*1024,
    long MaximumProfileBytes=1024L*1024*1024,long MinimumFreeDiskBytes=1024L*1024*1024);

public enum LocalCoherenceStage { Parent128Enumerated, ParentAliasesEnumerated, BeforeCut }
public sealed record LocalCoherenceObservation(LocalCoherenceStage Stage,string CandidateId,string ParentId128);

public interface ILocalDirectoryAliasSource : IAsyncDisposable
{
    ValueTask<ILocalDirectoryAliasReader> OpenAsync(LocalNamespaceDirectory parent,
        LocalNamespaceHandleProof expected,CancellationToken cancellationToken=default);
}
public interface ILocalDirectoryAliasReader : IAsyncDisposable
{
    LocalNamespaceHandleProof Proof { get; }
    ValueTask<LocalDirectoryAliasPage> ReadPageAsync(CancellationToken cancellationToken=default);
    ValueTask ValidateUnchangedAsync(CancellationToken cancellationToken=default);
}
