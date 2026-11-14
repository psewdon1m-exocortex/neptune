using Microsoft.Data.Sqlite;
using System.Text.Json;

namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    internal async Task RejectChangedParent(string root,string owner,string scope,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await ParentAdmission(root,owner,scope,local,2,0,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);
        var before=await NamespaceChanges(c,t,ct);var plan=await RequireParentPlan(c,t,root,owner,scope,id,ct);
        if(plan.Status!=LocalParentReconciliationStatus.Scanning)throw new InvalidOperationException("Only an uninstalled scan can be superseded.");
        await using var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_plans SET status=4,error='ParentChangedDuringClosure' WHERE plan_id=$id AND status=0");
        q.Parameters.AddWithValue("$id",id);await q.ExecuteNonQueryAsync(ct);await CommitParent(c,t,before,ct);
    }
    internal async Task<int> PromoteParentVersions(string root,string owner,string scope,LocalJournalAdmission local,CancellationToken ct)
    {
        if(await PreflightParentReconcileAsync(ct)==0)return 0;await ParentAdmission(root,owner,scope,local,122,0,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var s=await RequireParentCycle(c,t,root,owner,scope,ct);var rows=new List<string>();
        await using(var q=SyncCommand(c,t,"SELECT v.parent_id FROM local_parent_reconcile_versions v JOIN local_parent_reconcile_plans p ON p.plan_id=v.pending_plan WHERE v.coherence_id=$coherence AND p.status=3 ORDER BY v.parent_id LIMIT 120")){q.Parameters.AddWithValue("$coherence",s.CoherenceId!);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))rows.Add(r.GetString(0));}
        await using(var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_versions SET current_plan=pending_plan,pending_plan=NULL WHERE coherence_id=$coherence AND parent_id IN(SELECT value FROM json_each($nodes))")){q.Parameters.AddWithValue("$coherence",s.CoherenceId!);q.Parameters.AddWithValue("$nodes",JsonSerializer.Serialize(rows));await q.ExecuteNonQueryAsync(ct);}await CommitParent(c,t,before,ct);return rows.Count;
    }
    internal async Task<int> ResetClaimedParent(string root,string owner,string scope,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await ParentAdmission(root,owner,scope,local,132,0,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var p=await RequireParentPlan(c,t,root,owner,scope,id,ct);if(p.Status!=LocalParentReconciliationStatus.Scanning){await t.CommitAsync(ct);return 0;}string? parent;await using(var q=SyncCommand(c,t,"SELECT node_id FROM local_parent_reconcile_parents WHERE plan_id=$id AND state=1 ORDER BY node_id LIMIT 1")){q.Parameters.AddWithValue("$id",id);parent=await q.ExecuteScalarAsync(ct)as string;}if(parent is null){await t.CommitAsync(ct);return 0;}
        var entries=new List<long>();await using(var q=SyncCommand(c,t,"SELECT entry_id FROM local_parent_reconcile_entries WHERE plan_id=$id AND parent_id=$parent ORDER BY entry_id LIMIT 128")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$parent",parent);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))entries.Add(r.GetInt64(0));}
        await using(var q=SyncCommand(c,t,"DELETE FROM local_parent_reconcile_entries WHERE entry_id IN(SELECT value FROM json_each($entries)) AND plan_id=$id")){q.Parameters.AddWithValue("$entries",JsonSerializer.Serialize(entries));q.Parameters.AddWithValue("$id",id);await q.ExecuteNonQueryAsync(ct);}
        await using(var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_plans SET entries=entries-$count WHERE plan_id=$id;UPDATE local_parent_reconcile_parents SET entries=entries-$count,matched=0,state=CASE WHEN $done=1 THEN 0 ELSE 1 END,child_after=0,refilled=0 WHERE plan_id=$id AND node_id=$parent")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$parent",parent);q.Parameters.AddWithValue("$count",entries.Count);q.Parameters.AddWithValue("$done",entries.Count<128?1:0);await q.ExecuteNonQueryAsync(ct);}await CommitParent(c,t,before,ct);return 1;
    }
    internal async Task<int> RestoreParentConflicts(string root,string owner,string scope,LocalJournalAdmission local,CancellationToken ct)
    {
        if(await PreflightParentReconcileAsync(ct)==0)return 0;await CycleAdmission(root,owner,scope,local,121,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireCycle(c,t,root,owner,scope,false,ct);var nodes=new List<string>();
        await using(var q=SyncCommand(c,t,"SELECT f.node_id FROM local_parent_reconcile_conflicts p JOIN local_cycle_files f ON f.scope_id=p.scope_id AND f.node_id=p.node_id WHERE f.scope_id=$scope AND f.phase<>2 ORDER BY f.node_id LIMIT 120")){q.Parameters.AddWithValue("$scope",scope);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))nodes.Add(r.GetString(0));}
        foreach(var node in nodes){await using var q=SyncCommand(c,t,"UPDATE local_cycle_files SET phase=2 WHERE scope_id=$scope AND node_id=$node");q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$node",node);await q.ExecuteNonQueryAsync(ct);}await CommitParent(c,t,before,ct);return nodes.Count;
    }
    internal async Task<IReadOnlyList<LocalParentReconciliationConflict>> ReadParentConflicts(string root,string owner,string scope,string after,CancellationToken ct)
    {
        if(after.Length!=0&&(after.Length!=32||after!=after.ToLowerInvariant()||after.Any(x=>!char.IsAsciiHexDigit(x))))throw new ArgumentException("Conflict page requires canonical native node key.");if(await PreflightParentReconcileAsync(ct)==0)return [];
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,scope,true,ct);var rows=new List<LocalParentReconciliationConflict>();
        await using var q=SyncCommand(c,t,"SELECT p.node_id,f.path,p.reason,p.plan_id FROM local_parent_reconcile_conflicts p JOIN local_cycle_files f ON f.scope_id=p.scope_id AND f.node_id=p.node_id WHERE p.scope_id=$scope AND p.node_id>$after ORDER BY p.node_id LIMIT 120");q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$after",after);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))rows.Add(new(r.GetString(0),r.GetString(1),r.GetString(2),r.GetString(3)));return rows.AsReadOnly();
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task RejectChangedParentAsync(string scope,string id,CancellationToken ct)=>SerialAsync(async()=>{await _store.RejectChangedParent(_root,_owner,scope,id,_admission,ct);return true;},ct);
    internal Task<int> PromoteParentReconciliationAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.PromoteParentVersions(_root,_owner,scope,_admission,ct),ct);
    internal Task<int> ResetClaimedParentReconciliationAsync(string scope,string id,CancellationToken ct)=>SerialAsync(()=>_store.ResetClaimedParent(_root,_owner,scope,id,_admission,ct),ct);
    internal Task<int> RestoreParentConflictsAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.RestoreParentConflicts(_root,_owner,scope,_admission,ct),ct);
    internal Task<IReadOnlyList<LocalParentReconciliationConflict>> ReadParentConflictsAsync(string scope,string after,CancellationToken ct)=>SerialAsync(()=>_store.ReadParentConflicts(_root,_owner,scope,after,ct),ct);
}
