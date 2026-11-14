using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    // The execution ledger and historical USN dispositions do not refer to
    // parent plan IDs. They remain untouched. Any current/pending membership
    // or explicit conflict reference pins the ENTIRE plan conservatively.
    // In particular this does not compact partially referenced multi-parent
    // plans, and never retires the latest installed cut used by the next plan.
    internal async Task<int> RetireParentPlans(string root,string owner,string scope,LocalJournalAdmission local,CancellationToken ct)
    {
        if(await PreflightParentReconcileAsync(ct)==0)return 0;
        await ParentAdmission(root,owner,scope,local,130,0,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);
        var before=await NamespaceChanges(c,t,ct);_=await RequireParentCycle(c,t,root,owner,scope,ct);
        string? id;int phase;
        await using(var q=SyncCommand(c,t,"SELECT p.plan_id,p.retire_phase FROM local_parent_reconcile_plans p WHERE p.scope_id=$scope AND p.status IN(3,4) AND (p.status=4 OR p.plan_id<>(SELECT plan_id FROM local_parent_reconcile_plans WHERE scope_id=$scope AND status=3 ORDER BY cut_usn DESC,plan_id DESC LIMIT 1)) AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_versions WHERE current_plan=p.plan_id) AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_versions WHERE pending_plan=p.plan_id) AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_conflicts WHERE plan_id=p.plan_id) ORDER BY p.retire_phase DESC,p.cut_usn,p.plan_id LIMIT 1"))
        {q.Parameters.AddWithValue("$scope",scope);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)){await t.CommitAsync(ct);return 0;}id=r.GetString(0);phase=r.GetInt32(1);}
        if(phase==3)
        {
            await using(var exists=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='local_parent_reconcile_orphan_closes'"))
            if(await exists.ExecuteScalarAsync(ct)is not null)
            {
                await using var remove=SyncCommand(c,t,"DELETE FROM local_parent_reconcile_orphan_closes WHERE plan_id=$id AND usn IN(SELECT usn FROM local_parent_reconcile_orphan_closes WHERE plan_id=$id ORDER BY usn LIMIT 128)");remove.Parameters.AddWithValue("$id",id);
                if(await remove.ExecuteNonQueryAsync(ct)>0){await CommitParent(c,t,before,ct);return 1;}
            }
            await using var q=SyncCommand(c,t,"DELETE FROM local_parent_reconcile_plans WHERE plan_id=$id AND scope_id=$scope AND status IN(3,4) AND retire_phase=3");q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$scope",scope);await q.ExecuteNonQueryAsync(ct);
        }
        else
        {
            var table=phase==0?"local_parent_reconcile_entries":phase==1?"local_parent_reconcile_parents":"local_parent_reconcile_rename";
            var key=phase==0?"entry_id":phase==1?"node_id":"wire";
            var keys=new List<(object Key,int Major)>();await using(var q=SyncCommand(c,t,phase==2?"SELECT wire,major FROM local_parent_reconcile_rename WHERE plan_id=$id ORDER BY major,wire LIMIT 128":$"SELECT {key},0 FROM {table} WHERE plan_id=$id ORDER BY {key} LIMIT 128"))
            {q.Parameters.AddWithValue("$id",id);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))keys.Add((r.GetValue(0),r.GetInt32(1)));}
            foreach(var row in keys){await using var q=SyncCommand(c,t,$"DELETE FROM {table} WHERE plan_id=$id AND {key}=$key"+(phase==2?" AND major=$major":""));q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$key",row.Key);if(phase==2)q.Parameters.AddWithValue("$major",row.Major);await q.ExecuteNonQueryAsync(ct);}
            if(keys.Count<128){await using var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_plans SET retire_phase=retire_phase+1 WHERE plan_id=$id AND retire_phase=$phase");q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$phase",phase);await q.ExecuteNonQueryAsync(ct);}
        }
        await CommitParent(c,t,before,ct);return 1;
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task<int> RetireParentReconciliationAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.RetireParentPlans(_root,_owner,scope,_admission,ct),ct);
}
