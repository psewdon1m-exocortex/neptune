namespace Neptune.Core;

public sealed partial class WindowsFreshSyncEngine
{
    private RootMutationAdmission? _rootMutationAdmission;
    private async Task<RootMutationAdmission.Lease?> AcquireRootMutation(string? parent,CancellationToken ct)
        => _rootMutationAdmission is { } gate ? await gate.AcquireAsync(parent,ct) : null;
    private readonly AsyncLocal<string?> _scheduledOperation=new();
    private readonly AsyncLocal<SyncWorkLane> _workLane=new();
    private SyncOperationSchedule _schedule=new(0,0,null,null);
    private sealed class OperationNotDueException():IOException("Durable retry deadline has not arrived; originals retained.");
    private Task<IDisposable> TransferSlot(long bytes,CancellationToken ct)=>_session.ResourceScheduler.AcquireAsync(_workLane.Value,bytes,true,ct);
    private async Task<IDisposable> DataSlot(long bytes,CancellationToken ct)
    {
        await _resources.Data.WaitAsync(ct);
        try
        {
            var shared=await _session.ResourceScheduler.AcquireAsync(_workLane.Value,bytes,false,ct);
            _resources.StartData();return new DataLease(_resources,shared);
        }
        catch{_resources.Data.Release();throw;}
    }
    private sealed class DataLease(LocalCycleResources resources,IDisposable shared):IDisposable
    {
        private int _disposed;
        public void Dispose(){if(Interlocked.Exchange(ref _disposed,1)!=0)return;resources.EndData();shared.Dispose();resources.Data.Release();}
    }
    private async Task<IDisposable> SourceReadSlot(CancellationToken ct)
    {await _resources.Reader.WaitAsync(ct);try{var cpu=await SyncResourceScheduler.CpuSlot(ct);_resources.StartReader();return new SourceLease(_resources,cpu);}catch{_resources.Reader.Release();throw;}}
    private sealed class SourceLease(LocalCycleResources resources,IDisposable cpu):IDisposable
    {
        private int _disposed;
        public void Dispose(){if(Interlocked.Exchange(ref _disposed,1)!=0)return;cpu.Dispose();resources.EndReader();resources.Reader.Release();}
    }
    private async Task RecordRetry(string operation,Exception error,CancellationToken ct)
    {
        if(SyncRetryController.Classify(error)is not { } reason)return;
        await _session.SaveOperationWaitAsync(ScopeId,operation,reason,ct);
        _coordinator.Diagnostics.Retry(reason.State);
        if(error is not RootMutationDeferredException&&(reason.State is "RETRY_WAIT" or "WAIT_CONNECTIVITY"))_session.ResourceScheduler.Pressure();
    }
    private async Task FinishScheduledOperation(string operation,DateTimeOffset started,bool failed,CancellationToken ct)
    {
        var current=await _session.ReadCycleWorkAsync(ScopeId,operation,ct);
        if(current.Phase is LocalCycleWorkPhase.Published or LocalCycleWorkPhase.AdoptedEqual or LocalCycleWorkPhase.Conflict or LocalCycleWorkPhase.Failed)
        {
            await _session.ClearOperationWaitAsync(ScopeId,operation,ct);
            if(current.Phase==LocalCycleWorkPhase.Published)_session.ResourceScheduler.Published(current.Size??0,DateTimeOffset.UtcNow-started);
        }
        else if(!failed&&current.Phase is (LocalCycleWorkPhase.Prepared or LocalCycleWorkPhase.Sending or LocalCycleWorkPhase.OutcomeUnknown))
        {
            // A successful HTTP response can still mean an unresolved effect.
            // Keep the same original and inspect it after a durable delay.
            await _session.SaveOperationWaitAsync(ScopeId,operation,new("RETRY_WAIT","outcome_pending"),ct);
        }
    }
}
