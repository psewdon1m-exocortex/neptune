using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    internal async Task<NamespaceOperation?> ReadOwnedContentMutation(string root,string owner,string scope,string operation,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        await using var q=SyncCommand(c,t,"SELECT request_json,entry_json,state,approved,receipt_json FROM client_namespace_operations WHERE scope_id=$scope AND operation_id=$op");CycleId(q,scope);q.Parameters.AddWithValue("$op",operation);await using var r=await q.ExecuteReaderAsync(ct);
        return await r.ReadAsync(ct)?new(JsonSerializer.Deserialize<SyncMutationRequest>(r.GetString(0),LocalCycleWire.Json)!,JsonSerializer.Deserialize<LocalCycleEntry>(r.GetString(1),LocalCycleWire.Json)!,r.GetString(2),r.GetInt64(3)==1,r.IsDBNull(4)?null:r.GetString(4)):null;
    }
    internal async Task SaveOwnedContentMutation(string root,string owner,string scope,LocalCycleWork original,SyncMutationRequest request,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scope,16,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,false,ct);var work=await ReadCycleWork(c,t,original.OperationId,ct);
        if(work.ScopeId!=scope||work.Entry!=original.Entry||work.Sha256!=original.Sha256||work.CaptureProof!=original.CaptureProof||work.InputJson is not null||work.Phase!=LocalCycleWorkPhase.Sealed||request.OperationId!=work.OperationId||request.Kind is not("create-empty" or "clone-content" or "metadata-patch"))throw new InvalidDataException("Content mutation requires the original native seal.");
        await SaveNamespaceOperation(c,t,scope,request,work.Entry,true,ct);await t.CommitAsync(ct);
    }
    internal async Task CompleteOwnedContentMutation(string root,string owner,string scope,string operation,SyncMutationReceipt receipt,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scope,16,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,true,ct);var work=await ReadCycleWork(c,t,operation,ct);var d=receipt.Description;
        if(work.ScopeId!=scope||receipt.OperationId!=operation||receipt.State!="PUBLISHED"||receipt.Tombstone||d is null||d.Type!="file"||d.Availability!="AVAILABLE"||d.ContentSha256!=work.Sha256||d.SizeBytes!=work.Size?.ToString(System.Globalization.CultureInfo.InvariantCulture)||d.Name!=work.Entry.RawName||work.CaptureProof is null)throw new InvalidDataException("Content mutation changed original sealed bytes.");
        // The namespace receipt remains separately durable. The existing
        // equality ACK path verifies the same native proof against fresh R.
        await using var q=SyncCommand(c,t,"UPDATE client_namespace_operations SET state='PUBLISHED',receipt_json=$receipt WHERE scope_id=$scope AND operation_id=$op;UPDATE local_cycle_work SET phase=6,result_json=$description,error=NULL WHERE scope_id=$scope AND operation_id=$op AND active=1;UPDATE local_cycle_files SET phase=1,remote_node=$remote WHERE scope_id=$scope AND node_id=$node");CycleId(q,scope);q.Parameters.AddWithValue("$op",operation);q.Parameters.AddWithValue("$receipt",receipt.RawJson);q.Parameters.AddWithValue("$description",JsonSerializer.Serialize(d,LocalCycleWire.Json));q.Parameters.AddWithValue("$remote",d.NodeId);q.Parameters.AddWithValue("$node",work.Entry.NodeId);await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<NamespaceOperation?> ReadContentMutationAsync(string scope,string op,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedContentMutation(_root,_owner,scope,op,ct),ct);
    internal Task SaveContentMutationAsync(string scope,LocalCycleWork work,SyncMutationRequest request,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedContentMutation(_root,_owner,scope,work,request,ct);return true;},ct);
    internal Task CompleteContentMutationAsync(string scope,string op,SyncMutationReceipt receipt,CancellationToken ct)=>SerialAsync(async()=>{await _store.CompleteOwnedContentMutation(_root,_owner,scope,op,receipt,ct);return true;},ct);
}
