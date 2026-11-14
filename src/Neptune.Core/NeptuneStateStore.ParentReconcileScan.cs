using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    private static void ParentId(SqliteCommand q,ParentReconcileDirectory p){q.Parameters.AddWithValue("$id",p.PlanId);q.Parameters.AddWithValue("$parent",p.NodeId);q.Parameters.AddWithValue("$claim",p.Claim);}
    private static async Task RequireScanningParent(SqliteConnection c,SqliteTransaction t,ParentReconcileDirectory p,CancellationToken ct)
    {await using var q=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_parents WHERE plan_id=$id AND node_id=$parent AND claim=$claim AND state=1");ParentId(q,p);if(await q.ExecuteScalarAsync(ct)is null)throw new InvalidOperationException("Native parent claim is stale.");}
    internal async Task StageParentEntries(string root,string owner,string scope,ParentReconcileDirectory parent,IReadOnlyList<LocalNamespaceEntry> entries,LocalJournalAdmission local,CancellationToken ct)
    {
        if(entries.Count is <1 or >128)throw new ArgumentOutOfRangeException(nameof(entries));long bytes=0;foreach(var e in entries){WindowsDirectoryPageParser.ValidateEntry(e);if(e.Excluded||e.FileId128==parent.NodeId)throw new InvalidDataException("Excluded/reparse/self child cannot qualify a complete parent.");bytes+=512+Encoding.UTF8.GetByteCount(e.Name)*2+Encoding.UTF8.GetByteCount(parent.Path)+1;}
        await ParentAdmission(root,owner,scope,local,entries.Count+3,bytes,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var plan=await RequireParentPlan(c,t,root,owner,scope,parent.PlanId,ct);if(plan.Status!=LocalParentReconciliationStatus.Scanning)throw new InvalidOperationException("Parent plan is not scanning.");await RequireScanningParent(c,t,parent,ct);
        var page=JsonSerializer.Serialize(entries.Select(e=>
        {
            var path=parent.Path.Length==0?e.Name:parent.Path+"/"+e.Name;if(path.Length>4096||path.Count(x=>x=='/')>=128)throw new InvalidDataException("Current parent ancestry exceeds capture path budget.");
            return new{node=e.FileId128,name=e.Name,key=WebDavSyncClient.CanonicalPathKey(e.Name),created=e.CreationTime,kind=e.IsDirectory?1:0,size=e.Size,path};
        }));
        // Bounded indexed probes and one INSERT for the native page. Identity
        // and dual-pass proof are unchanged; no per-entry round trips.
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM json_each($page) j CROSS JOIN local_cycle_files f WHERE f.scope_id=$scope AND f.node_id=json_extract(j.value,'$.node') AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_incarnations i WHERE i.scope_id=f.scope_id AND i.node_id=f.node_id) AND (f.created<>json_extract(j.value,'$.created') OR f.kind<>json_extract(j.value,'$.kind')) UNION ALL SELECT 1 FROM json_each($page) j CROSS JOIN local_parent_reconcile_incarnations i CROSS JOIN local_cycle_files f ON f.scope_id=i.scope_id AND f.node_id=i.node_id WHERE i.scope_id=$scope AND i.native_id=json_extract(j.value,'$.node') AND (f.created<>json_extract(j.value,'$.created') OR f.kind<>json_extract(j.value,'$.kind')) LIMIT 1"))
        {
            q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$page",page);
            if(await q.ExecuteScalarAsync(ct)is not null)throw new LocalSourceGapException("ParentIncarnationChanged","Reused native ID or type cannot replace retained baseline/original.");
        }
        await using(var q=SyncCommand(c,t,"INSERT INTO local_parent_reconcile_entries(plan_id,parent_id,node_id,name,canonical,created,kind,size,path) SELECT $id,$parent,json_extract(value,'$.node'),json_extract(value,'$.name'),json_extract(value,'$.key'),json_extract(value,'$.created'),json_extract(value,'$.kind'),json_extract(value,'$.size'),json_extract(value,'$.path') FROM json_each($page)"))
        {ParentId(q,parent);q.Parameters.AddWithValue("$page",page);await q.ExecuteNonQueryAsync(ct);}
        await using(var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_parents SET entries=entries+$count WHERE plan_id=$id AND node_id=$parent;UPDATE local_parent_reconcile_plans SET entries=entries+$count WHERE plan_id=$id;UPDATE local_parent_reconcile_format SET bytes=bytes+$bytes WHERE singleton=1")){ParentId(q,parent);q.Parameters.AddWithValue("$count",entries.Count);q.Parameters.AddWithValue("$bytes",bytes);await q.ExecuteNonQueryAsync(ct);}await CommitParent(c,t,before,ct);
    }
    internal async Task StageParentAliases(string root,string owner,string scope,ParentReconcileDirectory parent,IReadOnlyList<LocalDirectoryAlias> aliases,LocalJournalAdmission local,CancellationToken ct)
    {
        if(aliases.Count is <1 or >128)throw new ArgumentOutOfRangeException(nameof(aliases));foreach(var a in aliases)WindowsDirectoryAliasParser.Validate(a);await ParentAdmission(root,owner,scope,local,aliases.Count+1,0,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var plan=await RequireParentPlan(c,t,root,owner,scope,parent.PlanId,ct);await RequireScanningParent(c,t,parent,ct);
        var priorNodes=await ParentAliasNodes(c,t,plan.CoherenceId,aliases,ct);
        var aliasPage=JsonSerializer.Serialize(aliases.Select(a=>new{wire=a.FileId64,name=a.Name,key=WebDavSyncClient.CanonicalPathKey(a.Name),created=a.CreationTime,kind=a.IsDirectory?1:0}));
        var staged=new Dictionary<string,(string Node,long Created)>(StringComparer.Ordinal);
        await using(var q=SyncCommand(c,t,"SELECT e.name,e.node_id,e.created FROM json_each($page) j CROSS JOIN local_parent_reconcile_entries e WHERE e.plan_id=$id AND e.parent_id=$parent AND e.name=json_extract(j.value,'$.name')"))
        {ParentId(q,parent);q.Parameters.AddWithValue("$page",aliasPage);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))if(!staged.TryAdd(r.GetString(0),(r.GetString(1),r.GetInt64(2))))throw new InvalidDataException("Duplicate native alias page.");}
        foreach(var a in aliases){
            var prior=priorNodes.GetValueOrDefault(a.FileId64);
            if(prior is not null)
            {if(!staged.TryGetValue(a.Name,out var e)||e.Node!=prior.Id||e.Created!=prior.Created)throw new LocalSourceGapException("ParentAliasRebound","Native legacy alias cannot silently rebind retained identity/incarnation.");}
        }
        await using(var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_entries AS e SET wire64=json_extract(j.value,'$.wire') FROM json_each($page) j WHERE e.plan_id=$id AND e.parent_id=$parent AND e.name=json_extract(j.value,'$.name') AND e.canonical=json_extract(j.value,'$.key') AND e.created=json_extract(j.value,'$.created') AND e.kind=json_extract(j.value,'$.kind') AND e.wire64 IS NULL"))
        {ParentId(q,parent);q.Parameters.AddWithValue("$page",aliasPage);if(await q.ExecuteNonQueryAsync(ct)!=aliases.Count)throw new InvalidDataException("Dual native parent passes differ; no fabricated alias.");}
        await using(var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_parents SET matched=matched+$count WHERE plan_id=$id AND node_id=$parent")){ParentId(q,parent);q.Parameters.AddWithValue("$count",aliases.Count);await q.ExecuteNonQueryAsync(ct);}await CommitParent(c,t,before,ct);
    }
    internal async Task CompleteParentDirectory(string root,string owner,string scope,ParentReconcileDirectory parent,LocalNamespaceHandleProof proof,LocalJournalAdmission local,CancellationToken ct)
    {
        ValidateNamespaceProof(proof);await ParentAdmission(root,owner,scope,local,2,0,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireParentPlan(c,t,root,owner,scope,parent.PlanId,ct);await RequireScanningParent(c,t,parent,ct);var source=await ReadLocalStateAsync(c,t,root,owner,ct);
        if(proof.FileId128!=parent.NodeId||proof.CreationTime!=parent.Created||proof.LegacyVolumeSerial.ToString("x8")!=source.Binding.VolumeSerial)throw new InvalidDataException("Current native parent binding changed.");
        await using(var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_parents SET state=2 WHERE plan_id=$id AND node_id=$parent AND claim=$claim AND entries=matched")){ParentId(q,parent);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidDataException("Incomplete parent cannot become eligible.");}await CommitParent(c,t,before,ct);
    }
    internal async Task<int> RefillParentDirectory(string root,string owner,string scope,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await ParentAdmission(root,owner,scope,local,123,0,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireParentPlan(c,t,root,owner,scope,id,ct);string? parent;long after;
        await using(var q=SyncCommand(c,t,"SELECT node_id,child_after FROM local_parent_reconcile_parents WHERE plan_id=$id AND state=2 AND refilled=0 ORDER BY node_id LIMIT 1")){q.Parameters.AddWithValue("$id",id);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)){await t.CommitAsync(ct);return 0;}parent=r.GetString(0);after=r.GetInt64(1);}
        var entries=new List<ParentReconcileEntry>();await using(var q=SyncCommand(c,t,"SELECT entry_id,node_id,parent_id,name,created,kind,size,wire64,path FROM local_parent_reconcile_entries WHERE plan_id=$id AND parent_id=$parent AND kind=1 AND entry_id>$after ORDER BY entry_id LIMIT 120")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$parent",parent);q.Parameters.AddWithValue("$after",after);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))entries.Add(ParentEntry(r));}
        var full=await IsFullParentMembership(c,t,id,ct);int added;
        await using(var q=SyncCommand(c,t,"INSERT INTO local_parent_reconcile_parents(plan_id,node_id,path,created,claim) SELECT $id,json_extract(j.value,'$.Node'),json_extract(j.value,'$.Path'),json_extract(j.value,'$.Created'),json_extract(j.value,'$.claim') FROM json_each($page) j WHERE $full=1 OR NOT EXISTS(SELECT 1 FROM local_cycle_files f WHERE f.scope_id=$scope AND f.node_id=json_extract(j.value,'$.Node') AND f.path=json_extract(j.value,'$.Path')) ON CONFLICT(plan_id,node_id) DO NOTHING"))
        {q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$full",full?1:0);q.Parameters.AddWithValue("$page",JsonSerializer.Serialize(entries.Select(e=>new{e.Node,e.Path,e.Created,claim=Guid.NewGuid().ToString("N")})));added=await q.ExecuteNonQueryAsync(ct);}
        await using(var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_parents SET child_after=$after,refilled=$done WHERE plan_id=$id AND node_id=$parent;UPDATE local_parent_reconcile_plans SET parents=parents+$count WHERE plan_id=$id")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$parent",parent);q.Parameters.AddWithValue("$after",entries.Count>0?entries[^1].Id:after);q.Parameters.AddWithValue("$done",entries.Count<120?1:0);q.Parameters.AddWithValue("$count",added);await q.ExecuteNonQueryAsync(ct);}await CommitParent(c,t,before,ct);return 1;
    }
    private static ParentReconcileEntry ParentEntry(SqliteDataReader r)=>new(r.GetInt64(0),r.GetString(1),r.GetString(2),r.GetString(3),r.GetInt64(4),r.GetInt32(5),r.GetInt64(6),r.IsDBNull(7)?null:r.GetString(7),r.GetString(8));
}
public sealed partial class LocalSourceJournalSession
{
    internal Task StageParentReconciliationEntriesAsync(string scope,ParentReconcileDirectory p,IReadOnlyList<LocalNamespaceEntry> entries,CancellationToken ct)=>SerialAsync(async()=>{await _store.StageParentEntries(_root,_owner,scope,p,entries,_admission,ct);return true;},ct);
    internal Task StageParentReconciliationAliasesAsync(string scope,ParentReconcileDirectory p,IReadOnlyList<LocalDirectoryAlias> entries,CancellationToken ct)=>SerialAsync(async()=>{await _store.StageParentAliases(_root,_owner,scope,p,entries,_admission,ct);return true;},ct);
    internal Task CompleteParentReconciliationDirectoryAsync(string scope,ParentReconcileDirectory p,LocalNamespaceHandleProof proof,CancellationToken ct)=>SerialAsync(async()=>{await _store.CompleteParentDirectory(_root,_owner,scope,p,proof,_admission,ct);return true;},ct);
    internal Task<int> RefillParentReconciliationAsync(string scope,string id,CancellationToken ct)=>SerialAsync(()=>_store.RefillParentDirectory(_root,_owner,scope,id,_admission,ct),ct);
}
