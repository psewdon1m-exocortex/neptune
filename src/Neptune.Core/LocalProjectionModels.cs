namespace Neptune.Core;

// An applied prefix has durable local dispositions. It is not a source cut,
// closed writer cycle, common B, current R, or authorization for deletion/NOOP.
public enum LocalProjectionStatus { Active, Gapped, Superseded }
public sealed record LocalProjectionAdmission(long MaximumProjectionBytes=128L*1024*1024,
    long MaximumProfileBytes=1024L*1024*1024,long MinimumFreeDiskBytes=1024L*1024*1024);
public sealed record LocalProjectionState(string ProjectionId,string CandidateId,
    LocalProjectionStatus Status,string JournalId,long StartUsn,long AppliedUsn,long IngestUsn,
    long OwnerEpoch,long LegacyRevision,long GapEpoch,string? GapCode,long DirtyNodes,
    long Unresolved,long Bytes)
{
    public string LocalCompleteness=>"Unknown";
    public string SourceCycle=>"Unknown";
}
public sealed record LocalProjectionApply(bool Duplicate,int Records,int Known,int Unresolved,
    long AppliedUsn,long ChangedRows);
public sealed record LocalProjectionDirty(string NodeId128,long CreationTime,long Generation,
    long LastUsn,uint Reasons,bool Directory,long SubtreeEpoch);
public sealed record LocalProjectionUnresolved(long Usn,ushort Major,string FileId,string ParentId,
    string Name,uint Reasons,string Code,string PayloadSha256,bool PresentQualified);
public sealed record LocalProjectionRetirement(int Rows,long Bytes,bool Removed,long ChangedRows);
public sealed record LocalProjectionMetrics(long Transactions,long MaximumChangedRows,long MaximumCommitTicks);

// Native resolution may qualify a present object only. Constructors are not
// public: caller JSON/name/mtime cannot supply OS proof. The raw event stays
// durable; historical rename/deletion/outside membership is never inferred.
public sealed class LocalProjectionMetadataClaim
{
    internal LocalProjectionMetadataClaim(string projection,long usn,string hash,LocalUsnRecord record,
        LocalNamespaceRootProof root,LocalNamespaceDirectory parent)
        =>(ProjectionId,Usn,PayloadSha256,Record,Root,Parent)=(projection,usn,hash,record,root,parent);
    public string ProjectionId{get;} public long Usn{get;} public string PayloadSha256{get;}
    public LocalUsnRecord Record{get;} public LocalNamespaceRootProof Root{get;}
    public LocalNamespaceDirectory Parent{get;}
}
public sealed class LocalProjectionMetadataProof
{
    internal LocalProjectionMetadataProof(LocalProjectionMetadataClaim claim,LocalNamespaceHandleProof handle,long links)
        =>(Claim,Handle,Links)=(claim,handle,links);
    public LocalProjectionMetadataClaim Claim{get;} public LocalNamespaceHandleProof Handle{get;} public long Links{get;}
}
