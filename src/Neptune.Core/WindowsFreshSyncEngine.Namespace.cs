using System.Text;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

public sealed partial class WindowsFreshSyncEngine
{
    public async Task<NamespaceDeletePreview> PreviewDeletesAsync(CancellationToken ct=default)
    {await _serial.WaitAsync(ct);try{return await _session.PreviewDeletesAsync(ScopeId,ct);}finally{_serial.Release();}}
    public async Task ApproveDeletesAsync(string digest,CancellationToken ct=default)
    {await _serial.WaitAsync(ct);try{if(_remote is not IWindowsCycleExtensions{NamespaceRevision:>=2})throw new InvalidOperationException("Reviewed bulk deletion requires K1 r2 server approval.");await Observe(ct);await SyncRemote(ct);foreach(var operation in await _session.ReadNamespaceOperationsAsync(ScopeId,ct))if(operation.Request.Kind=="quarantine-delete"&&operation.State=="QUEUED"&&!await _session.ValidateNamespaceAsync(ScopeId,operation,ct))throw new InvalidOperationException("Delete proof changed; refresh the plan before approval.");await _session.ApproveDeletesAsync(ScopeId,digest,ct);}finally{_serial.Release();}}
    private async Task EnsureDeleteApproval(NamespaceOperation operation,IWindowsCycleExtensions remote,CancellationToken ct)
    {
        if(operation.Request.Kind!="quarantine-delete"||!operation.Request.Input.TryGetProperty("planId",out var id)||id.ValueKind==System.Text.Json.JsonValueKind.Null)return;
        var stored=await _session.ReadDeletePlanAsync(ScopeId,id.GetString()!,ct);
        if(stored.Original.PlanDigest!=operation.Request.Input.GetProperty("planDigest").GetString()||stored.Original.Namespace!=operation.Request.Namespace)throw new InvalidDataException("Delete mutation differs from its reviewed plan.");
        if(stored.Receipt is null)
        {var receipt=await Admission(()=>remote.ApproveDeletePlanAsync(stored.Original,ct),ct);await _session.SaveDeleteApprovalAsync(ScopeId,stored.Original,receipt,ct);}
    }
    private async Task ProcessNamespace(CancellationToken ct)
    {
        if(!_reconcileParents||_remote is not IWindowsCycleExtensions remote||!remote.Supports("namespace-mutations-v1")||remote.NamespacePolicy is not { } policy)return;
        var rechecks=await _session.ReadDeletedNamespaceRechecksAsync(ScopeId,ct);
        if(rechecks.Count>0&&await _session.RequestFullParentProofAsync(ScopeId,ct))
        {
            var plan=await _session.BeginParentReconciliationAsync(ScopeId,ct);
            var proof=await _coordinator.ReconcileParentsAsync(_session,ScopeId,_coherence!,ct);
            if(proof.Status!=LocalCoherenceStatus.Anchored)throw new LocalSourceGapException("DeletedNamespaceUncertain","New full native membership proof must close before retiring later delete hints.");
            while(await _session.ImportCycleDirtyAsync(ScopeId,ct)>0)ct.ThrowIfCancellationRequested();
            await SyncRemote(ct);
            await _session.RetireDeletedNamespaceRechecksAsync(ScopeId,plan.PlanId,rechecks,ct);
        }
        await _session.PlanNamespaceAsync(ScopeId,policy,ct);
        foreach(var original in await _session.ReadNamespaceOperationsAsync(ScopeId,ct))
        {
            var scheduled=false;
            try
            {
            if(original.Request.Kind is not("move" or "quarantine-delete"))continue;
            if(!original.Approved){if(!await _session.ValidateNamespaceAsync(ScopeId,original,ct))await _session.RecordNamespaceAsync(ScopeId,original.Request.OperationId,"CANCELLED",null,ct);continue;}
            if(!await _session.ScheduleReadyAsync(ScopeId,original.Request.OperationId,"namespace",ct))continue;
            scheduled=true;_scheduledOperation.Value=original.Request.OperationId;
            await EnsureDeleteApproval(original,remote,ct);
            SyncMutationReceipt? receipt=null;
            if(original.Receipt is not null)receipt=SyncMutationWire.Read(Encoding.UTF8.GetBytes(original.Receipt),original.Request);
            if(receipt?.State!="PUBLISHED")
            {
                if(original.State!="QUEUED")
                {
                    try{receipt=await Admission(()=>remote.MutateAsync(original.Request,true,ct),ct);}
                    catch(GuardedCycleRemoteException e)when(e.StructuredWireCode&&e.Status==System.Net.HttpStatusCode.Conflict&&e.Code=="operation_reused")
                    {receipt=await Admission(()=>remote.MutateAsync(original.Request,false,ct),ct);}
                    if(original.Receipt is not null&&SyncMutationWire.Read(Encoding.UTF8.GetBytes(original.Receipt),original.Request).IntentHash!=receipt.IntentHash)throw new InvalidDataException("Original mutation hash changed.");
                    await _session.RecordNamespaceAsync(ScopeId,original.Request.OperationId,"RECOVERING",receipt,ct);
                    if(receipt.State is "UNKNOWN" or "PHYSICAL_PENDING")
                    {await _session.SaveOperationWaitAsync(ScopeId,original.Request.OperationId,new("RETRY_WAIT","outcome_pending"),ct);continue;}
                    if(receipt.State=="NOT_PUBLISHED")
                    {await _session.RecordNamespaceAsync(ScopeId,original.Request.OperationId,"NOT_PUBLISHED",receipt,ct);await _session.ClearOperationWaitAsync(ScopeId,original.Request.OperationId,ct);continue;}
                }
                if(receipt?.State!="PUBLISHED")
                {
                    if(!await _session.ValidateNamespaceAsync(ScopeId,original,ct))
                    {if(original.State=="QUEUED"){await _session.RecordNamespaceAsync(ScopeId,original.Request.OperationId,"CANCELLED",null,ct);await _session.ClearOperationWaitAsync(ScopeId,original.Request.OperationId,ct);}continue;}
                    if(original.Request.Kind=="move")
                    {if(original.Entry.Directory)await _capture.ValidateDirectoryAsync(original.Entry,ct);else await _capture.MeasureFileAsync(original.Entry,ct);}
                    await _session.RecordNamespaceAsync(ScopeId,original.Request.OperationId,"UNKNOWN",null,ct);
                    receipt=await Admission(()=>remote.MutateAsync(original.Request,false,ct),ct);
                    await _session.RecordNamespaceAsync(ScopeId,original.Request.OperationId,"RECEIPT_PENDING",receipt,ct);
                }
            }
            if(receipt?.State=="PUBLISHED")
            {await SyncRemote(ct);await _session.ApplyNamespaceReceiptAsync(ScopeId,original,receipt,ct);}
            else if(receipt?.State=="NOT_PUBLISHED")await _session.RecordNamespaceAsync(ScopeId,original.Request.OperationId,"NOT_PUBLISHED",receipt,ct);
            if(receipt?.State is "PUBLISHED" or "NOT_PUBLISHED")await _session.ClearOperationWaitAsync(ScopeId,original.Request.OperationId,ct);
            else await _session.SaveOperationWaitAsync(ScopeId,original.Request.OperationId,new("RETRY_WAIT","outcome_pending"),ct);
            }
            catch(GuardedCycleRemoteException e)
            {
                var refused=original.State=="QUEUED"&&e.StructuredWireCode&&(e.Status==System.Net.HttpStatusCode.PreconditionFailed||e.Status==System.Net.HttpStatusCode.Conflict&&e.Code is "directory_not_empty" or "membership_changed" or "approval_stale");
                await _session.RecordNamespaceFailureAsync(ScopeId,original.Request.OperationId,e.Code,refused,ct);
                if(scheduled){if(refused)await _session.ClearOperationWaitAsync(ScopeId,original.Request.OperationId,ct);else await RecordRetry(original.Request.OperationId,e,ct);}
                if(!refused){PauseNew("Original namespace operation awaits recovery: "+e.Code,scheduled&&SyncRetryController.Classify(e)is not null?e:null);break;}
            }
            catch(Exception e)when(scheduled&&!ct.IsCancellationRequested&&SyncRetryController.Classify(e)is not null)
            {await RecordRetry(original.Request.OperationId,e,ct);PauseNew("Original namespace operation awaits recovery.",e);break;}
            finally{_scheduledOperation.Value=null;}
        }
    }
}
