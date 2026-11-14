using Microsoft.Data.Sqlite;

namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    private static async Task<bool> IsRetainedCycleNative(SqliteConnection c,SqliteTransaction t,string coherence,LocalUsnRecord record,CancellationToken ct)
    {
        var incarnations=await HasIncarnations(c,t,ct);
        var identities=record.MajorVersion==3?"SELECT $wire":"SELECT node_id FROM local_coherence_nodes WHERE coherence_id=$coherence AND wire64=$wire UNION SELECT e.node_id FROM local_parent_reconcile_entries e JOIN local_parent_reconcile_plans p ON p.plan_id=e.plan_id WHERE p.coherence_id=$coherence AND e.wire64=$wire";
        // Outside-volume rename traffic must probe native identities, not scan
        // every managed file. CROSS JOIN preserves the indexed probe order
        // even before ANALYZE has statistics for a newly seeded large corpus.
        var direct="SELECT 1 FROM local_cycle_scopes s CROSS JOIN native_ids n CROSS JOIN local_cycle_files f ON f.scope_id=s.scope_id AND f.node_id=n.node_id WHERE s.coherence_id=$coherence AND s.status<>3";
        if(incarnations)direct+=" AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_incarnations i WHERE i.scope_id=f.scope_id AND i.node_id=f.node_id AND i.native_id<>n.node_id)";
        var replaced=incarnations?" UNION ALL SELECT 1 FROM local_cycle_scopes s CROSS JOIN native_ids n CROSS JOIN local_parent_reconcile_incarnations i ON i.scope_id=s.scope_id AND i.native_id=n.node_id CROSS JOIN local_cycle_files f ON f.scope_id=i.scope_id AND f.node_id=i.node_id WHERE s.coherence_id=$coherence AND s.status<>3":"";
        await using var q=SyncCommand(c,t,"WITH native_ids(node_id) AS ("+identities+") "+direct+replaced+" LIMIT 1");
        q.Parameters.AddWithValue("$coherence",coherence);q.Parameters.AddWithValue("$wire",record.FileIdHex);return await q.ExecuteScalarAsync(ct)is not null;
    }
    internal async Task<bool> HasRecoverableParentReplacement(string root,string owner,string scope,CancellationToken ct)
    {
        if(await PreflightParentReconcileAsync(ct)==0)return false;
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireParentCycle(c,t,root,owner,scope,ct);
        var native=await HasIncarnations(c,t,ct)?"COALESCE((SELECT i.native_id FROM local_parent_reconcile_incarnations i WHERE i.scope_id=f.scope_id AND i.node_id=f.node_id),f.node_id)":"f.node_id";
        await using var q=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_conflicts p JOIN local_cycle_files f ON f.scope_id=p.scope_id AND f.node_id=p.node_id JOIN local_parent_reconcile_plans proof ON proof.plan_id=p.plan_id AND proof.status=3 WHERE f.scope_id=$scope AND f.kind=0 AND p.reason LIKE 'Different current native identity occupies%' AND EXISTS(SELECT 1 FROM local_parent_reconcile_entries e WHERE e.plan_id=p.plan_id AND e.path=f.path AND e.kind=0) AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_entries e WHERE e.plan_id=p.plan_id AND e.node_id="+native+") LIMIT 1");
        CycleId(q,scope);return await q.ExecuteScalarAsync(ct)is not null;
    }
    // Preserve the logical ledger key and every old sealed/input JSON. Only a
    // completed, hardlink-qualified full membership may move its current native
    // identity. The remote descriptor remains the prior CAS, never name adoption.
    private static async Task<bool> ReplaceParentIncarnation(SqliteConnection c,SqliteTransaction t,string scope,LocalParentReconciliation plan,string node,ParentReconcileEntry entry,CancellationToken ct)
    {
        if(entry.Kind!=0)return false;
        string prior;long created,baselineWork;
        await using(var q=SyncCommand(c,t,"SELECT COALESCE(i.native_id,f.node_id),f.created,f.baseline_work FROM local_cycle_files f LEFT JOIN local_parent_reconcile_incarnations i ON i.scope_id=f.scope_id AND i.node_id=f.node_id WHERE f.scope_id=$scope AND f.node_id=$node AND f.kind=0 AND f.path=$path AND f.parent_id=$parent AND EXISTS(SELECT 1 FROM local_parent_reconcile_plans p WHERE p.plan_id=$plan AND p.full_membership=1 AND p.hardlink_verified=1 AND p.status IN(1,2))"))
        {
            CycleId(q,scope);q.Parameters.AddWithValue("$node",node);q.Parameters.AddWithValue("$path",entry.Path);q.Parameters.AddWithValue("$parent",entry.Parent);q.Parameters.AddWithValue("$plan",plan.PlanId);
            await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))return false;prior=r.GetString(0);created=r.GetInt64(1);baselineWork=r.GetInt64(2);
        }
        if(prior==entry.Node)return false;
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_entries WHERE plan_id=$plan AND node_id=$prior UNION ALL SELECT 1 FROM local_cycle_files f LEFT JOIN local_parent_reconcile_incarnations i ON i.scope_id=f.scope_id AND i.node_id=f.node_id WHERE f.scope_id=$scope AND f.node_id<>$node AND COALESCE(i.native_id,f.node_id)=$native LIMIT 1"))
        {CycleId(q,scope);q.Parameters.AddWithValue("$plan",plan.PlanId);q.Parameters.AddWithValue("$prior",prior);q.Parameters.AddWithValue("$node",node);q.Parameters.AddWithValue("$native",entry.Node);if(await q.ExecuteScalarAsync(ct)is not null)return false;}
        await using(var charge=SyncCommand(c,t,"UPDATE local_parent_reconcile_format SET bytes=bytes+512 WHERE singleton=1 AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_incarnations WHERE scope_id=$scope AND node_id=$node)"))
        {CycleId(charge,scope);charge.Parameters.AddWithValue("$node",node);await charge.ExecuteNonQueryAsync(ct);}
        // An unsealed, never-attempted capture of the vanished incarnation can
        // be superseded. Attempted/sealed/extended originals continue unchanged.
        await using(var q=SyncCommand(c,t,"UPDATE local_cycle_work SET active=NULL,phase=9,error='Unattempted capture superseded by qualified atomic save.' WHERE scope_id=$scope AND node_id=$node AND active=1 AND input_json IS NULL AND capture_proof IS NULL AND phase IN(0,8,9) AND NOT EXISTS(SELECT 1 FROM client_stream_operations s WHERE s.operation_id=local_cycle_work.operation_id) AND NOT EXISTS(SELECT 1 FROM client_namespace_operations n WHERE n.operation_id=local_cycle_work.operation_id) AND NOT EXISTS(SELECT 1 FROM client_metadata_seals m WHERE m.operation_id=local_cycle_work.operation_id)"))
        {CycleId(q,scope);q.Parameters.AddWithValue("$node",node);await q.ExecuteNonQueryAsync(ct);}
        await using(var q=SyncCommand(c,t,"INSERT INTO local_parent_reconcile_incarnations VALUES($scope,$node,$native,$created,$prior,$oldCreated,$plan,$cut,$work) ON CONFLICT(scope_id,node_id) DO UPDATE SET native_id=excluded.native_id,created=excluded.created,prior_native_id=excluded.prior_native_id,prior_created=excluded.prior_created,plan_id=excluded.plan_id,cut_usn=excluded.cut_usn,baseline_work=excluded.baseline_work;UPDATE local_cycle_files SET created=$created,size=$size,watcher_generation=watcher_generation+1,ack_journal=-1,ack_watcher=-1,phase=CASE WHEN EXISTS(SELECT 1 FROM local_cycle_work w WHERE w.scope_id=$scope AND w.node_id=$node AND w.active=1) THEN 1 ELSE 0 END WHERE scope_id=$scope AND node_id=$node;DELETE FROM local_parent_reconcile_conflicts WHERE scope_id=$scope AND node_id=$node"))
        {CycleId(q,scope);q.Parameters.AddWithValue("$node",node);q.Parameters.AddWithValue("$native",entry.Node);q.Parameters.AddWithValue("$created",entry.Created);q.Parameters.AddWithValue("$prior",prior);q.Parameters.AddWithValue("$oldCreated",created);q.Parameters.AddWithValue("$plan",plan.PlanId);q.Parameters.AddWithValue("$cut",plan.CutUsn);q.Parameters.AddWithValue("$work",baselineWork);q.Parameters.AddWithValue("$size",entry.Size);await q.ExecuteNonQueryAsync(ct);}
        return true;
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<bool> HasRecoverableParentReplacementAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.HasRecoverableParentReplacement(_root,_owner,scope,ct),ct);
}
