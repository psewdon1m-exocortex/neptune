using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    internal async Task<int> VerifyParentPlan(string root,string owner,string scope,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await ParentAdmission(root,owner,scope,local,2,0,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var p=await RequireParentPlan(c,t,root,owner,scope,id,ct);var source=await ReadLocalStateAsync(c,t,root,owner,ct);
        if(p.Status!=LocalParentReconciliationStatus.Scanning||p.Parents==0)throw new LocalSourceGapException("ParentUnscopedAmbiguity","No proved affected parent; explicit broader reconciliation required.");
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_parents WHERE plan_id=$id AND (state<>2 OR refilled<>1) LIMIT 1")){q.Parameters.AddWithValue("$id",id);if(await q.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Partial parent/frontier cannot qualify.");}
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_rename WHERE plan_id=$id AND state<>2 LIMIT 1")){q.Parameters.AddWithValue("$id",id);if(await q.ExecuteScalarAsync(ct)is not null)throw new LocalSourceGapException("ParentRenameHalfMissing","Unpaired rename is retained; current absence is not historical membership proof.");}
        if(await VerifyParentHardlinkAbsence(c,t,p,source.Cursor.JournalIdHex,source.Cursor.NextReadUsn,ct)){await CommitParent(c,t,before,ct);return 1;}
        if(await VerifyOrphanParentCloses(c,t,p,ct)>0){await CommitParent(c,t,before,ct);return 1;}
        // Native enumeration was bracketed by these REAL ingest boundaries.
        // Validate every retained record subpage before making its version
        // pointer visible. Current names come only from the dual native pass.
        long cursor;await using(var q=SyncCommand(c,t,"SELECT verify_after FROM local_parent_reconcile_plans WHERE plan_id=$id")){q.Parameters.AddWithValue("$id",id);cursor=Convert.ToInt64(await q.ExecuteScalarAsync(ct));}
        var cut=p.CutUsn==0?source.Cursor.NextReadUsn:p.CutUsn;var records=new List<LocalUsnRecord>();
        await using(var q=SyncCommand(c,t,"SELECT usn,payload FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn>=$after AND usn<$end ORDER BY usn LIMIT 257")){q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",source.Cursor.JournalIdHex);q.Parameters.AddWithValue("$after",cursor);q.Parameters.AddWithValue("$end",cut);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))records.Add(ProjectionRecord(source.Cursor.JournalIdHex,r.GetInt64(0),source.Cursor.NextReadUsn,(byte[])r.GetValue(1)));}
        foreach(var record in records.Take(256))
        {
            if((record.Reason&0x10000)!=0)throw new LocalSourceGapException("ParentChangedDuringClosure","Hardlink changed during parent closure; a new full membership proof is required.");
            if((record.Reason&(NamespaceTransition|DirectoryPolicy))==0)continue;
            await using var q=SyncCommand(c,t,record.MajorVersion==3
                ?"SELECT 1 FROM local_parent_reconcile_parents WHERE plan_id=$id AND node_id IN($node,$parent) UNION ALL SELECT 1 FROM local_parent_reconcile_entries WHERE plan_id=$id AND node_id IN($node,$parent) LIMIT 1"
                :"SELECT 1 FROM local_parent_reconcile_entries WHERE plan_id=$id AND wire64 IN($node,$parent) UNION ALL SELECT 1 FROM local_coherence_nodes n JOIN local_parent_reconcile_parents p ON p.plan_id=$id AND p.node_id=n.node_id WHERE n.coherence_id=$coherence AND n.wire64 IN($node,$parent) LIMIT 1");q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$coherence",p.CoherenceId);q.Parameters.AddWithValue("$node",record.FileIdHex);q.Parameters.AddWithValue("$parent",record.ParentIdHex);if(await q.ExecuteScalarAsync(ct)is not null)throw new LocalSourceGapException("ParentChangedDuringClosure","Affected namespace changed during native enumeration; incomplete version retained.");
        }
        // Only an actually retained first-omitted record or sealed native
        // read boundary advances the durable proof prefix. Never lastUSN+1.
        var next=records.Count==257?records[256].Usn:cut;
        await using(var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_plans SET status=$status,cut_usn=$cut,verify_after=$next WHERE plan_id=$id AND status=0")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$status",next==cut?1:0);q.Parameters.AddWithValue("$cut",cut);q.Parameters.AddWithValue("$next",next);await q.ExecuteNonQueryAsync(ct);}await CommitParent(c,t,before,ct);return next==cut?0:records.Count-1;
    }

    internal async Task<int> ImportParentPlan(string root,string owner,string scope,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await ParentAdmission(root,owner,scope,local,256,128*1024,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var p=await RequireParentPlan(c,t,root,owner,scope,id,ct);if(p.Status is not(LocalParentReconciliationStatus.Verified or LocalParentReconciliationStatus.Importing))throw new InvalidOperationException("Only qualified internal native version imports into same scope.");
        long after;await using(var q=SyncCommand(c,t,"SELECT import_after FROM local_parent_reconcile_plans WHERE plan_id=$id")){q.Parameters.AddWithValue("$id",id);after=Convert.ToInt64(await q.ExecuteScalarAsync(ct));}
        var entries=new List<ParentReconcileEntry>();await using(var q=SyncCommand(c,t,"SELECT entry_id,node_id,parent_id,name,created,kind,size,wire64,path FROM local_parent_reconcile_entries WHERE plan_id=$id AND entry_id>$after ORDER BY entry_id LIMIT 40")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$after",after);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))entries.Add(ParentEntry(r));}
        var knownPage=await ParentKnownPage(c,t,scope,entries,ct);var restoredNodes=new List<string>();
        var added=0;long bytes=0;foreach(var e in entries)
        {
            var known=knownPage.TryGetValue(e.Node,out var prior);var logical=prior?.Logical??e.Node;var conflict=prior is not null&&(prior.Path!=e.Path||prior.Created!=e.Created||prior.Kind!=e.Kind);
            if(conflict)
            {await ParentConflict(c,t,scope,id,logical,"Native item renamed/moved or changed type; prior baseline and original operation retained, no delete/overwrite.",ct);continue;}
            if(known)
            {
                // A temporarily absent file may return with its original identity.
                // Revoke only the local absence/replacement conflict, not rename,
                // remote precondition, or original-operation evidence.
                restoredNodes.Add(logical);continue;
            }
            // A different current native ID occupying a retained path cannot
            // replace the prior baseline or acquire its remote CAS authority.
            string? occupied;await using(var q=SyncCommand(c,t,"SELECT node_id FROM local_cycle_files WHERE scope_id=$scope AND path=$path")){q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$path",e.Path);occupied=await q.ExecuteScalarAsync(ct)as string;}
            if(occupied is not null && await ReplaceParentIncarnation(c,t,scope,p,occupied,e,ct))continue;
            if(occupied is not null){await ParentConflict(c,t,scope,id,occupied,"Different current native identity occupies a retained path; prior baseline and original operation retained, replacement requires explicit conflict resolution.",ct);continue;}
            var rowBytes=256+Encoding.UTF8.GetByteCount(e.Path)+Encoding.UTF8.GetByteCount(e.Name);
            await using(var q=SyncCommand(c,t,"INSERT INTO local_cycle_files(scope_id,node_id,parent_id,path,name,created,kind,size,bytes) VALUES($scope,$node,$parent,$path,$name,$created,$kind,$size,$bytes)")){q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$node",e.Node);q.Parameters.AddWithValue("$parent",e.Parent);q.Parameters.AddWithValue("$path",e.Path);q.Parameters.AddWithValue("$name",e.Name);q.Parameters.AddWithValue("$created",e.Created);q.Parameters.AddWithValue("$kind",e.Kind);q.Parameters.AddWithValue("$size",e.Size);q.Parameters.AddWithValue("$bytes",rowBytes);await q.ExecuteNonQueryAsync(ct);}added++;bytes+=rowBytes;
        }
        await RestoreKnownParentPage(c,t,scope,restoredNodes,ct);
        await using(var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_plans SET import_after=$after,status=2 WHERE plan_id=$id;UPDATE local_cycle_scopes SET files=files+$added WHERE scope_id=$scope")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$after",entries.Count>0?entries[^1].Id:after);q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$added",added);await q.ExecuteNonQueryAsync(ct);}await AddCycleBytes(c,t,scope,bytes,ct);await CommitParent(c,t,before,ct);return entries.Count;
    }
    private static async Task ParentConflict(SqliteConnection c,SqliteTransaction t,string scope,string id,string node,string reason,CancellationToken ct)
    {await using var q=SyncCommand(c,t,"INSERT INTO local_parent_reconcile_conflicts VALUES($scope,$node,$id,$reason) ON CONFLICT(scope_id,node_id) DO UPDATE SET plan_id=excluded.plan_id,reason=excluded.reason;UPDATE local_cycle_files SET phase=2 WHERE scope_id=$scope AND node_id=$node");q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$node",node);q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$reason",reason);await q.ExecuteNonQueryAsync(ct);}
    internal async Task<int> MarkParentMissing(string root,string owner,string scope,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await ParentAdmission(root,owner,scope,local,123,0,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var plan=await RequireParentPlan(c,t,root,owner,scope,id,ct);var coherence=await ReadCoherenceAsync(c,t,plan.CoherenceId,ct);
        string parent;int phase;long after;
        await using(var q=SyncCommand(c,t,"SELECT node_id,missing_phase,missing_after FROM local_parent_reconcile_parents WHERE plan_id=$id AND missing_phase<4 ORDER BY node_id LIMIT 1")){q.Parameters.AddWithValue("$id",id);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)){await r.DisposeAsync();var missing=await MarkFullMembershipMissing(c,t,scope,id,ct);await CommitParent(c,t,before,ct);return missing;}parent=r.GetString(0);phase=r.GetInt32(1);after=r.GetInt64(2);}
        // Revisit ONLY the affected parent's immutable original/current
        // membership pages. Do not scan every file in the whole cycle scope.
        string? priorVersion=null;await using(var q=SyncCommand(c,t,"SELECT current_plan FROM local_parent_reconcile_versions WHERE coherence_id=$coherence AND parent_id=$parent")){q.Parameters.AddWithValue("$coherence",plan.CoherenceId);q.Parameters.AddWithValue("$parent",parent);priorVersion=await q.ExecuteScalarAsync(ct)as string;}
        var entries=new List<(long Id,string Node)>();
        if(phase<2||priorVersion is not null)
        {
            var table=phase<2?"local_namespace_entries":"local_parent_reconcile_entries";var key=phase<2?"candidate_id":"plan_id";
            await using var q=SyncCommand(c,t,$"SELECT entry_id,node_id FROM {table} WHERE {key}=$version AND parent_id=$parent AND kind=$kind AND entry_id>$after"+(phase<2?" AND excluded=0":"")+" ORDER BY entry_id LIMIT 60");q.Parameters.AddWithValue("$version",phase<2?coherence.State.CandidateId:priorVersion!);q.Parameters.AddWithValue("$parent",parent);q.Parameters.AddWithValue("$kind",phase%2);q.Parameters.AddWithValue("$after",after);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))entries.Add((r.GetInt64(0),r.GetString(1)));
        }
        foreach(var entry in entries)
        {
            await using var q=SyncCommand(c,t,"SELECT 1 FROM local_cycle_files f WHERE scope_id=$scope AND node_id=$node AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_entries e WHERE e.plan_id=$id AND e.node_id=COALESCE((SELECT i.native_id FROM local_parent_reconcile_incarnations i WHERE i.scope_id=f.scope_id AND i.node_id=f.node_id),f.node_id))");q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$node",entry.Node);q.Parameters.AddWithValue("$id",id);
            // Historical membership remains retained after a reviewed delete.
            // Do not recreate an absence conflict for an already settled
            // tombstone. Any newer dirty pair or current replacement still
            // takes the ordinary conflict/reconciliation path.
            if(await q.ExecuteScalarAsync(ct)is not null&&!await IsSettledDeletedNamespace(c,t,scope,entry.Node,ct))await ParentConflict(c,t,scope,id,entry.Node,"Previously known source item is absent in qualified current parent; historical membership and original operation retained, no deletion authorized.",ct);
        }
        var nextPhase=entries.Count<60?phase+1:phase;await using(var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_parents SET missing_phase=$phase,missing_after=$after WHERE plan_id=$id AND node_id=$parent")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$parent",parent);q.Parameters.AddWithValue("$phase",nextPhase);q.Parameters.AddWithValue("$after",nextPhase!=phase?0:entries[^1].Id);await q.ExecuteNonQueryAsync(ct);}await CommitParent(c,t,before,ct);return 1;
    }

    internal async Task<int> InstallParentVersions(string root,string owner,string scope,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await ParentAdmission(root,owner,scope,local,122,0,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var p=await RequireParentPlan(c,t,root,owner,scope,id,ct);if(p.Status!=LocalParentReconciliationStatus.Importing)throw new InvalidOperationException("Current parent import is not complete.");var parents=new List<string>();
        await using(var q=SyncCommand(c,t,"SELECT p.node_id FROM local_parent_reconcile_parents p LEFT JOIN local_parent_reconcile_versions v ON v.coherence_id=$coherence AND v.parent_id=p.node_id WHERE p.plan_id=$id AND (v.pending_plan IS NULL OR v.pending_plan<>$id) ORDER BY p.node_id LIMIT 120")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$coherence",p.CoherenceId);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))parents.Add(r.GetString(0));}
        await using(var q=SyncCommand(c,t,"INSERT INTO local_parent_reconcile_versions SELECT $coherence,value,NULL,$id FROM json_each($parents) WHERE 1 ON CONFLICT(coherence_id,parent_id) DO UPDATE SET pending_plan=$id")){q.Parameters.AddWithValue("$coherence",p.CoherenceId);q.Parameters.AddWithValue("$parents",JsonSerializer.Serialize(parents));q.Parameters.AddWithValue("$id",id);await q.ExecuteNonQueryAsync(ct);}await CommitParent(c,t,before,ct);return parents.Count;
    }
    internal async Task FinishParentPlan(string root,string owner,string scope,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await ParentAdmission(root,owner,scope,local,3,0,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var p=await RequireParentPlan(c,t,root,owner,scope,id,ct);
        if(p.Status!=LocalParentReconciliationStatus.Importing)throw new InvalidOperationException("Parent activation requires qualified bounded import.");
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_plans WHERE plan_id=$id AND full_membership=1 AND hardlink_verified=0")){q.Parameters.AddWithValue("$id",id);if(await q.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Full membership cannot activate before bounded hardlink absence proof.");}
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_entries WHERE plan_id=$id AND entry_id>(SELECT import_after FROM local_parent_reconcile_plans WHERE plan_id=$id) LIMIT 1")){q.Parameters.AddWithValue("$id",id);if(await q.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Unimported current entries cannot activate.");}
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_parents WHERE plan_id=$id AND (state<>2 OR refilled<>1 OR missing_phase<>4) LIMIT 1")){q.Parameters.AddWithValue("$id",id);if(await q.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Incomplete parent/dependency/missing comparison cannot activate.");}
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_parents p LEFT JOIN local_parent_reconcile_versions v ON v.coherence_id=$coherence AND v.parent_id=p.node_id WHERE p.plan_id=$id AND (v.pending_plan IS NULL OR v.pending_plan<>$id) LIMIT 1")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$coherence",p.CoherenceId);if(await q.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Not all parent versions have been staged.");}
        // A resumed verified plan may predate later ingested events. Install
        // its proved version, but never clear uncertainty beyond that cut.
        await using(var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_plans SET status=3 WHERE plan_id=$id;UPDATE local_coherence_roots SET status=CASE WHEN applied_usn=$cut THEN 1 ELSE 2 END,error=CASE WHEN applied_usn=$cut THEN NULL ELSE COALESCE(error,'ScopedNamespaceTransitionUnresolved') END WHERE coherence_id=$coherence AND status IN(1,2);UPDATE local_cycle_scopes SET status=0,error=NULL WHERE scope_id=$scope AND (status=0 OR status=2 AND error=$reason)")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$coherence",p.CoherenceId);q.Parameters.AddWithValue("$cut",p.CutUsn);q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$reason",StructuralHint);await q.ExecuteNonQueryAsync(ct);}await CommitParent(c,t,before,ct);
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<int> VerifyParentReconciliationAsync(string scope,string id,CancellationToken ct)=>SerialAsync(()=>_store.VerifyParentPlan(_root,_owner,scope,id,_admission,ct),ct);
    internal Task<int> ImportParentReconciliationAsync(string scope,string id,CancellationToken ct)=>SerialAsync(()=>_store.ImportParentPlan(_root,_owner,scope,id,_admission,ct),ct);
    internal Task<int> MarkParentReconciliationMissingAsync(string scope,string id,CancellationToken ct)=>SerialAsync(()=>_store.MarkParentMissing(_root,_owner,scope,id,_admission,ct),ct);
    internal Task<int> InstallParentReconciliationVersionsAsync(string scope,string id,CancellationToken ct)=>SerialAsync(()=>_store.InstallParentVersions(_root,_owner,scope,id,_admission,ct),ct);
    internal Task FinishParentReconciliationAsync(string scope,string id,CancellationToken ct)=>SerialAsync(async()=>{await _store.FinishParentPlan(_root,_owner,scope,id,_admission,ct);return true;},ct);
}
