namespace Neptune.Core;

public sealed partial class WindowsFreshSyncEngine
{
    private readonly SyncWorkProgressTracker _syncProgress=new();
    public bool ProgressEnabled { get=>_syncProgress.Enabled; set=>_syncProgress.Enabled=value; }
    public SyncWorkProgressSnapshot WorkProgress => _syncProgress.Snapshot();
    private async Task PlanProgress(CancellationToken ct)
    {
        if(ProgressEnabled)_syncProgress.Plan((await _session.ReadCyclePendingAsync(ScopeId,ct)).Pending);
    }
}
