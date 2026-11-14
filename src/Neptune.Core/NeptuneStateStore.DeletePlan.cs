using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;
internal sealed record StoredDeletePlan(SyncDeletePlan Original,string? Receipt);
public sealed partial class NeptuneStateStore
{
    internal async Task<StoredDeletePlan> ReadOwnedDeletePlan(string root,string owner,string scope,string plan,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        await using var q=SyncCommand(c,t,"SELECT plan_json,receipt_json FROM client_delete_plans WHERE scope_id=$scope AND plan_id=$plan");CycleId(q,scope);q.Parameters.AddWithValue("$plan",plan);await using var r=await q.ExecuteReaderAsync(ct);
        if(!await r.ReadAsync(ct))throw new InvalidDataException("Original reviewed delete plan is missing.");
        return new(JsonSerializer.Deserialize<SyncDeletePlan>(r.GetString(0),LocalCycleWire.Json)!,r.IsDBNull(1)?null:r.GetString(1));
    }
    internal async Task SaveOwnedDeleteApproval(string root,string owner,string scope,SyncDeletePlan plan,string receipt,CancellationToken ct)
    {
        if(receipt.Length>16*1024)throw new InvalidDataException("Delete approval receipt exceeds limit.");
        using var doc=JsonDocument.Parse(receipt);var result=doc.RootElement;
        if(result.GetProperty("schema").GetString()!="saturn.delete-plan-approved.v1"||result.GetProperty("planId").GetString()!=plan.PlanId||result.GetProperty("planDigest").GetString()!=plan.PlanDigest||result.GetProperty("itemCount").GetInt32()!=plan.Items.Count)throw new InvalidDataException("Delete approval does not match the original plan.");
        await ClientAdmission(root,owner,scope,8,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        await using var q=SyncCommand(c,t,"UPDATE client_delete_plans SET receipt_json=$receipt WHERE scope_id=$scope AND plan_id=$plan AND plan_json=$json AND (receipt_json IS NULL OR receipt_json=$receipt)");CycleId(q,scope);q.Parameters.AddWithValue("$plan",plan.PlanId);q.Parameters.AddWithValue("$json",JsonSerializer.Serialize(plan,LocalCycleWire.Json));q.Parameters.AddWithValue("$receipt",receipt);
        if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidDataException("Original delete-plan registration changed.");await t.CommitAsync(ct);
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<StoredDeletePlan> ReadDeletePlanAsync(string scope,string plan,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedDeletePlan(_root,_owner,scope,plan,ct),ct);
    internal Task SaveDeleteApprovalAsync(string scope,SyncDeletePlan plan,string receipt,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedDeleteApproval(_root,_owner,scope,plan,receipt,ct);return true;},ct);
}
