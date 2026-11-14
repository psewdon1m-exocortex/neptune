namespace Neptune.Core;

// A current-membership successor is not a rewrite of historical Outside,
// not a byte seal, and never permission to delete a missing remote item.
public enum LocalParentReconciliationStatus { Scanning, Verified, Importing, Installed, Refused }
public sealed record WindowsCycleNativeOptions(LocalJournalReadMode ReadMode,bool ReconcileParents=false);
public sealed record LocalParentReconciliation(string PlanId,string ScopeId,string CoherenceId,
    LocalParentReconciliationStatus Status,long StartUsn,long CutUsn,long Parents,long Entries,string? Error);
public sealed record LocalParentReconciliationMetrics(long Transactions,long MaximumChangedRows,long MaximumCommitTicks);
public sealed record LocalParentReconciliationConflict(string NodeId,string RelativePath,string Reason,string PlanId);
internal sealed record ParentReconcileDirectory(string PlanId,string NodeId,string Path,long Created,string Claim,int State);
internal sealed record ParentReconcileEntry(long Id,string Node,string Parent,string Name,long Created,int Kind,long Size,string? Wire64,string Path);
