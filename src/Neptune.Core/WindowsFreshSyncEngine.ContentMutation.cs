using System.Text;
using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;
public sealed partial class WindowsFreshSyncEngine
{
    private async Task<bool> TryContentMutation(LocalCycleWork work,string parent,SyncDescriptor? target,CancellationToken ct)
    {
        var retained=await _session.ReadContentMutationAsync(ScopeId,work.OperationId,ct);
        if(retained?.State is "NOT_PUBLISHED" or "CANCELLED")return false;
        if(_remote is not IWindowsCycleExtensions remote||!remote.Supports("namespace-mutations-v1")||remote.NamespacePolicy is not {} policy)
        {if(retained is not null)throw new BootstrapCaptureDeferredException("Original content mutation capability is unavailable.");return false;}
        RootMutationAdmission.Lease? admission=null;
        try
        {
        if(retained is null)
        {
            SyncMutationRequest request;
            var metadata=await _session.ReadMetadataSealAsync(ScopeId,work.OperationId,ct);
            if(metadata is not null&&target is not null)
                request=SyncMutationWire.Create(_configuration.Binding,policy,work.OperationId,"metadata-patch",target,new Dictionary<string,object?>{{"contentVersionId",target.ContentVersionId},{"sha256",work.Sha256},{"modifiedAt",metadata.ModifiedAt}});
            else if(work.Size==0&&target is null)
                request=SyncMutationWire.Create(_configuration.Binding,policy,work.OperationId,"create-empty",null,new Dictionary<string,object?>{{"parentId",parent},{"name",work.Entry.RawName}});
            else if(target is null&&remote.Supports("clone-content-v1"))
            {
                // A separate lookup costs a request for every tiny file before
                // the bounded batch can form. Send its retained bytes directly.
                // Recorded mutations were handled above and still recover first.
                if(_smallBatchScheduler is not null&&_smallBatchCapability is {} batch&&work.Size is >0&&work.Size<=batch.MaximumFileBytes)return false;
                admission=await AcquireRootMutation(null,ct);
                SyncContentReference? source;
                try{source=await Admission(()=>remote.LookupContentAsync(work.Size!.Value,work.Sha256!,ct),ct);}
                catch(GuardedCycleRemoteException e)when(e.StructuredWireCode&&e.Status is System.Net.HttpStatusCode.NotFound or System.Net.HttpStatusCode.Forbidden or System.Net.HttpStatusCode.PreconditionFailed){return false;}
                if(source is null)return false;
                request=SyncMutationWire.Create(_configuration.Binding,policy,work.OperationId,"clone-content",null,new Dictionary<string,object?>{{"parentId",parent},{"name",work.Entry.RawName},{"source",source}});
            }
            else return false;
            await _session.SaveContentMutationAsync(ScopeId,work,request,ct);retained=new(request,work.Entry,"QUEUED",true,null);
        }
        admission??=await AcquireRootMutation(null,ct);
        SyncMutationReceipt? receipt=retained.Receipt is null?null:SyncMutationWire.Read(Encoding.UTF8.GetBytes(retained.Receipt),retained.Request);
        try
        {
            admission?.Attempt();
            if(receipt?.State!="PUBLISHED"&&retained.State!="QUEUED")
            {
                // K1 reports an absent status as operation_reused as well as
                // identity mismatch. Replaying execute with the SAME input
                // resolves absence; mismatch still fails server-side.
                try{receipt=await Admission(()=>remote.MutateAsync(retained.Request,true,ct),ct);}
                catch(GuardedCycleRemoteException e)when(e.StructuredWireCode&&e.Status==System.Net.HttpStatusCode.Conflict&&e.Code=="operation_reused"){}
                if(receipt is not null&&retained.Receipt is not null&&SyncMutationWire.Read(Encoding.UTF8.GetBytes(retained.Receipt),retained.Request).IntentHash!=receipt.IntentHash)throw new InvalidDataException("Original content mutation hash changed.");
            }
            if(receipt?.State is "UNKNOWN" or "PHYSICAL_PENDING")throw new BootstrapCaptureDeferredException("Original content mutation outcome remains unknown.");
            if(receipt?.State=="NOT_PUBLISHED")
            {await _session.RecordNamespaceAsync(ScopeId,work.OperationId,"NOT_PUBLISHED",receipt,ct);admission?.Complete();return false;}
            if(receipt?.State!="PUBLISHED")
            {
                await _session.RecordNamespaceAsync(ScopeId,work.OperationId,"UNKNOWN",receipt,ct);
                try{receipt=await Admission(()=>remote.MutateAsync(retained.Request,false,ct),ct);}
                catch(GuardedCycleRemoteException e)when(retained.State=="QUEUED"&&receipt is null&&e.StructuredWireCode&&e.Status==System.Net.HttpStatusCode.PreconditionFailed)
                {await _session.RecordNamespaceAsync(ScopeId,work.OperationId,"NOT_PUBLISHED",null,ct);admission?.Complete();return false;}
                await _session.RecordNamespaceAsync(ScopeId,work.OperationId,"RECEIPT_PENDING",receipt,ct);
            }
            if(receipt.State!="PUBLISHED")throw new BootstrapCaptureDeferredException("Original content mutation has no terminal publication.");
            await _session.CompleteContentMutationAsync(ScopeId,work.OperationId,receipt,ct);admission?.Complete();Interlocked.Increment(ref _publishedFiles);return true;
        }
        catch(Exception e)when(e is not OperationCanceledException&&e is not BootstrapCaptureDeferredException&&NetworkFailure.State(e) is null)
        {throw new BootstrapCaptureDeferredException("Original content mutation retained: "+e.GetType().Name);}
        }
        finally{admission?.Dispose();}
    }
}
