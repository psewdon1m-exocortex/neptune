namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private static async Task<int> MarkFullMembershipMissing(Microsoft.Data.Sqlite.SqliteConnection c,Microsoft.Data.Sqlite.SqliteTransaction t,string scope,string plan,CancellationToken ct)
    {
        await using(var exists=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='client_full_missing'"))if(await exists.ExecuteScalarAsync(ct)is null)return 0;
        await using(var full=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_plans WHERE plan_id=$plan AND full_membership=1 AND hardlink_verified=1")){full.Parameters.AddWithValue("$plan",plan);if(await full.ExecuteScalarAsync(ct)is null)return 0;}
        string after;await using(var q=SyncCommand(c,t,"SELECT after_node FROM client_full_missing WHERE plan_id=$plan")){q.Parameters.AddWithValue("$plan",plan);after=await q.ExecuteScalarAsync(ct)as string??"";}
        var nodes=new List<string>();await using(var q=SyncCommand(c,t,"SELECT f.node_id FROM local_cycle_files f WHERE f.scope_id=$scope AND f.node_id>$after AND (f.baseline IS NOT NULL OR f.remote_node IS NOT NULL OR EXISTS(SELECT 1 FROM local_cycle_work w WHERE w.scope_id=f.scope_id AND w.node_id=f.node_id AND w.active=1)) AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_entries e WHERE e.plan_id=$plan AND e.node_id=COALESCE((SELECT i.native_id FROM local_parent_reconcile_incarnations i WHERE i.scope_id=f.scope_id AND i.node_id=f.node_id),f.node_id)) ORDER BY f.node_id LIMIT 120"))
        {CycleId(q,scope);q.Parameters.AddWithValue("$plan",plan);q.Parameters.AddWithValue("$after",after);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))nodes.Add(r.GetString(0));}
        foreach(var node in nodes)await ParentConflict(c,t,scope,plan,node,"Managed object absent from complete root membership, including descendants of an absent directory; quarantine requires review.",ct);
        if(nodes.Count>0){await using var q=SyncCommand(c,t,"INSERT INTO client_full_missing VALUES($plan,$after) ON CONFLICT(plan_id) DO UPDATE SET after_node=$after");q.Parameters.AddWithValue("$plan",plan);q.Parameters.AddWithValue("$after",nodes[^1]);await q.ExecuteNonQueryAsync(ct);}
        return nodes.Count;
    }
    internal async Task<bool> RequestFullParentProof(string root,string owner,string scope,LocalJournalAdmission admission,CancellationToken ct)
    {
        await MarkOwnedCycleUncertain(root,owner,scope,StructuralHint,ct);
        while(await PromoteParentVersions(root,owner,scope,admission,ct)>0)ct.ThrowIfCancellationRequested();
        var plan=await BeginParentPlan(root,owner,scope,admission,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);
        var row=await RequireParentCycle(c,t,root,owner,scope,ct);var p=await RequireParentPlan(c,t,root,owner,scope,plan.PlanId,ct);
        if(p.Status!=LocalParentReconciliationStatus.Scanning)return false;
        var coherence=await ReadCoherenceAsync(c,t,row.CoherenceId!,ct);var ns=await ReadNamespaceAsync(c,t,coherence.State.CandidateId,ct);
        var identity=await ParentIdentity(c,t,row,3,ns.State.RootFileId128,ct)??throw new InvalidDataException("Full reconciliation lost root identity.");
        await using var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_plans SET full_membership=1,hardlink_verified=0 WHERE plan_id=$plan;INSERT INTO local_parent_reconcile_parents(plan_id,node_id,path,created,claim) VALUES($plan,$node,'',$created,$claim) ON CONFLICT(plan_id,node_id) DO NOTHING;UPDATE local_parent_reconcile_plans SET parents=parents+changes() WHERE plan_id=$plan");
        q.Parameters.AddWithValue("$plan",plan.PlanId);q.Parameters.AddWithValue("$node",ns.State.RootFileId128);q.Parameters.AddWithValue("$created",identity.Created);q.Parameters.AddWithValue("$claim",Guid.NewGuid().ToString("N"));await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);return true;
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<bool> RequestFullParentProofAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.RequestFullParentProof(_root,_owner,scope,_admission,ct),ct);
}
