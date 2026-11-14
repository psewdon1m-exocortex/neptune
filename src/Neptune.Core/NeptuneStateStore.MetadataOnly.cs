using System.Text.Json;

namespace Neptune.Core;
internal sealed record MetadataOnlySeal(StoredStreamSeal Seal,string ModifiedAt);
public sealed partial class NeptuneStateStore
{
    internal async Task<long?> ReadOwnedMetadataBoundary(string root,string owner,string scope,LocalCycleWork original,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);var s=await RequireCycle(c,t,root,owner,scope,false,ct);
        if(original.Entry.Directory||original.Entry.BaselineJson is null||original.InputJson is not null||original.Phase!=LocalCycleWorkPhase.Queued)return null;
        var current=await CurrentCycleEntry(c,t,s,original.Entry.NodeId,ct);if(current!=original.Entry)return null;
        var coherence=await RequireCoherenceAsync(c,t,root,owner,s.CoherenceId!,false,ct);if(coherence.State.Status!=LocalCoherenceStatus.Anchored)return null;
        // This cumulative reason mask is deliberately conservative: any data,
        // stream, encryption, compression, link or structural reason falls
        // back to native byte capture, even if it predates the baseline.
        await using var q=SyncCommand(c,t,"SELECT reasons FROM local_coherence_dirty WHERE coherence_id=$coherence AND node_id=$node AND generation=$generation");q.Parameters.AddWithValue("$coherence",s.CoherenceId!);q.Parameters.AddWithValue("$node",current.SourceNodeId);q.Parameters.AddWithValue("$generation",current.Generation.Journal);
        var reasons=await q.ExecuteScalarAsync(ct);if(reasons is not long mask||(mask&~0x80008000L)!=0||(mask&0x8000)==0)return null;
        return coherence.State.AppliedUsn;
    }
    internal async Task<MetadataOnlySeal?> ReadOwnedMetadataSeal(string root,string owner,string scope,string operation,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        await using var q=SyncCommand(c,t,"SELECT seal_json,modified_at FROM client_metadata_seals WHERE scope_id=$scope AND operation_id=$op");CycleId(q,scope);q.Parameters.AddWithValue("$op",operation);await using var r=await q.ExecuteReaderAsync(ct);return await r.ReadAsync(ct)?new(JsonSerializer.Deserialize<StoredStreamSeal>(r.GetString(0),LocalCycleWire.Json)!,r.GetString(1)):null;
    }
    internal async Task SaveOwnedMetadataSeal(string root,string owner,string scope,LocalCycleWork work,MetadataOnlySeal seal,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scope,8,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,false,ct);
        var current=await ReadCycleWork(c,t,work.OperationId,ct);if(current.ScopeId!=scope||current.Entry!=work.Entry||current.Phase!=LocalCycleWorkPhase.Queued||current.InputJson is not null)throw new InvalidOperationException("Metadata witness cannot replace original work.");
        await using var q=SyncCommand(c,t,"INSERT INTO client_metadata_seals VALUES($op,$scope,$seal,$modified)");CycleId(q,scope);q.Parameters.AddWithValue("$op",work.OperationId);q.Parameters.AddWithValue("$seal",JsonSerializer.Serialize(seal.Seal,LocalCycleWire.Json));q.Parameters.AddWithValue("$modified",seal.ModifiedAt);await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<long?> ReadMetadataBoundaryAsync(string scope,LocalCycleWork work,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedMetadataBoundary(_root,_owner,scope,work,ct),ct);
    internal Task<MetadataOnlySeal?> ReadMetadataSealAsync(string scope,string op,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedMetadataSeal(_root,_owner,scope,op,ct),ct);
    internal Task SaveMetadataSealAsync(string scope,LocalCycleWork work,MetadataOnlySeal seal,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedMetadataSeal(_root,_owner,scope,work,seal,ct);return true;},ct);
}
