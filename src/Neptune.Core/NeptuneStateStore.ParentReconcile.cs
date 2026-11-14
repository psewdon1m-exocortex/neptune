using System.Diagnostics;
using System.Text;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private long _parentTransactions,_parentRows,_parentCommit;
    public LocalParentReconciliationMetrics GetParentReconciliationMetrics()=>new(Interlocked.Read(ref _parentTransactions),Interlocked.Read(ref _parentRows),Interlocked.Read(ref _parentCommit));
    private const string StructuralHint="Watcher unknown/short alias or structural branch requires reconciliation.";
    private static async Task<bool> HasParentModule(SqliteConnection c,SqliteTransaction t,CancellationToken ct)
    {await using var q=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='local_parent_reconcile_format'");return await q.ExecuteScalarAsync(ct)is not null;}
    private static async Task<LocalCycleScope> RequireParentCycle(SqliteConnection c,SqliteTransaction t,string root,string owner,string scope,CancellationToken ct)
    {
        var row=await RequireCycle(c,t,root,owner,scope,true,ct);var local=await ReadLocalStateAsync(c,t,root,owner,ct);await RequireLocalMappingAsync(c,t,local.Binding,ct);
        if(!local.Cursor.Valid||row.Revision!=local.LegacyRevision||row.Revision!=await LocalRevisionAsync(c,t,ct)||row.Gap!=local.GapEpoch||row.Status==3||row.Status==2&&row.Error!=StructuralHint||row.CoherenceId is null||!row.SeedComplete)
            throw new LocalSourceGapException("ParentContinuityGap","Parent reconciliation cannot clear a lifecycle, owner, rules or journal gap.");
        return row;
    }
    private async Task ParentAdmission(string root,string owner,string scope,LocalJournalAdmission local,int rows,long bytes,CancellationToken ct)
    {await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);var s=await RequireParentCycle(c,t,root,owner,scope,ct);await t.CommitAsync(ct);await CheckLocalAdmissionAsync(CycleBudget(local,s),bytes,NamespaceReserve(rows),ct);}
    private async Task<long> CommitParent(SqliteConnection c,SqliteTransaction t,long before,CancellationToken ct)
    {var rows=await NamespaceChanges(c,t,ct)-before;var clock=Stopwatch.StartNew();await t.CommitAsync(ct);Interlocked.Increment(ref _parentTransactions);UpdateBootstrapMaximum(ref _parentRows,rows);UpdateBootstrapMaximum(ref _parentCommit,clock.Elapsed.Ticks);return rows;}
    private static async Task<LocalParentReconciliation> ReadParentPlan(SqliteConnection c,SqliteTransaction t,string id,CancellationToken ct)
    {await using var q=SyncCommand(c,t,"SELECT scope_id,coherence_id,status,CASE WHEN scan_start_usn>0 THEN scan_start_usn ELSE start_usn END,cut_usn,parents,entries,error FROM local_parent_reconcile_plans WHERE plan_id=$id");q.Parameters.AddWithValue("$id",id);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))throw new InvalidOperationException("Parent plan is absent.");return new(id,r.GetString(0),r.GetString(1),(LocalParentReconciliationStatus)r.GetInt32(2),r.GetInt64(3),r.GetInt64(4),r.GetInt64(5),r.GetInt64(6),r.IsDBNull(7)?null:r.GetString(7));}
    private static async Task<LocalParentReconciliation> RequireParentPlan(SqliteConnection c,SqliteTransaction t,string root,string owner,string scope,string id,CancellationToken ct)
    {
        var s=await RequireParentCycle(c,t,root,owner,scope,ct);var local=await ReadLocalStateAsync(c,t,root,owner,ct);var p=await ReadParentPlan(c,t,id,ct);
        await using var q=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_plans WHERE plan_id=$id AND root_key=$root AND scope_id=$scope AND coherence_id=$coherence AND owner_epoch=$owner AND revision=$revision AND gap=$gap AND journal=$journal AND CASE WHEN scan_start_usn>0 THEN scan_start_usn ELSE start_usn END>=$first");
        q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$coherence",s.CoherenceId!);q.Parameters.AddWithValue("$owner",local.Cursor.OwnerEpoch);q.Parameters.AddWithValue("$revision",local.LegacyRevision);q.Parameters.AddWithValue("$gap",local.GapEpoch);q.Parameters.AddWithValue("$journal",local.Cursor.JournalIdHex);q.Parameters.AddWithValue("$first",local.RetiredThroughUsn);
        if(await q.ExecuteScalarAsync(ct)is null||p.ScopeId!=scope||p.Status==LocalParentReconciliationStatus.Refused)throw new LocalSourceGapException("ParentPlanBinding","Pending parent plan identity/continuity changed; original evidence retained.");return p;
    }
    internal async Task<LocalParentReconciliation> BeginParentPlan(string root,string owner,string scope,LocalJournalAdmission local,CancellationToken ct)
    {
        await using(var c=await OpenCycleConnection(ct))using(var t=c.BeginTransaction(deferred:true))
        {var s=await RequireParentCycle(c,t,root,owner,scope,ct);await t.CommitAsync(ct);await EnsureParentReconcileAsync(CycleBudget(local,s),ct);}
        await ParentAdmission(root,owner,scope,local,8,1024,ct);await using var db=await OpenCycleConnection(ct);using var tx=db.BeginTransaction(deferred:false);var before=await NamespaceChanges(db,tx,ct);var row=await RequireParentCycle(db,tx,root,owner,scope,ct);var state=await ReadLocalStateAsync(db,tx,root,owner,ct);
        await using(var old=SyncCommand(db,tx,"SELECT plan_id FROM local_parent_reconcile_plans WHERE scope_id=$scope AND status<3"))
        {old.Parameters.AddWithValue("$scope",scope);if(await old.ExecuteScalarAsync(ct)is string pending)
        {
            var p=await RequireParentPlan(db,tx,root,owner,scope,pending,ct);
            // A retained empty plan has not observed a native directory yet.
            // Pin its first real scan boundary separately, preserving its
            // original registered boundary and every retained USN record.
            if(p.Status==LocalParentReconciliationStatus.Scanning&&p.Parents==0&&p.Entries==0)
            {await using var q=SyncCommand(db,tx,"UPDATE local_parent_reconcile_plans SET scan_start_usn=$start,verify_after=$start WHERE plan_id=$id AND scan_start_usn=0 AND cut_usn=0 AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_parents WHERE plan_id=$id) AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_entries WHERE plan_id=$id)");q.Parameters.AddWithValue("$id",pending);q.Parameters.AddWithValue("$start",state.Cursor.NextReadUsn);await q.ExecuteNonQueryAsync(ct);p=await RequireParentPlan(db,tx,root,owner,scope,pending,ct);}
            await tx.CommitAsync(ct);return p;
        }}
        // Previous installed versions must have been promoted before a new
        // pending pointer can be assigned. Promotion is bounded separately.
        await using(var old=SyncCommand(db,tx,"SELECT 1 FROM local_parent_reconcile_versions WHERE coherence_id=$coherence AND pending_plan IS NOT NULL LIMIT 1")){old.Parameters.AddWithValue("$coherence",row.CoherenceId!);if(await old.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Promote committed parent versions before another plan.");}
        var coherence=await RequireCoherenceAsync(db,tx,root,owner,row.CoherenceId!,false,ct);var id=Guid.NewGuid().ToString("N");var start=coherence.State.CutUsn;
        await using(var q=SyncCommand(db,tx,"SELECT cut_usn FROM local_parent_reconcile_plans WHERE scope_id=$scope AND status=3 ORDER BY cut_usn DESC LIMIT 1")){q.Parameters.AddWithValue("$scope",scope);if(await q.ExecuteScalarAsync(ct)is long previousCut)start=Math.Max(start,previousCut);}
        // A qualified current external cut leaves historical UNKNOWN rows
        // intact, but later inside changes must not seed those old hardlinks
        // into a new full-tree plan. It proves the unchanged managed namespace
        // through this boundary; subsequent records still drive reconciliation.
        await using(var exists=SyncCommand(db,tx,"SELECT 1 FROM sqlite_master WHERE name='local_parent_reconcile_external_cuts'"))
        if(await exists.ExecuteScalarAsync(ct)is not null)
        {await using var q=SyncCommand(db,tx,"SELECT max(cut_usn) FROM local_parent_reconcile_external_cuts WHERE scope_id=$scope AND coherence_id=$coherence AND journal=$journal AND cut_usn<=$applied");q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$coherence",row.CoherenceId!);q.Parameters.AddWithValue("$journal",state.Cursor.JournalIdHex);q.Parameters.AddWithValue("$applied",coherence.State.AppliedUsn);if(await q.ExecuteScalarAsync(ct)is long externalCut)start=Math.Max(start,externalCut);}
        await using(var q=SyncCommand(db,tx,"INSERT INTO local_parent_reconcile_plans(plan_id,root_key,scope_id,coherence_id,owner_epoch,revision,gap,journal,start_usn,status,record_after,verify_after) VALUES($id,$root,$scope,$coherence,$owner,$revision,$gap,$journal,$start,0,$after,$start);UPDATE local_parent_reconcile_format SET bytes=bytes+1024 WHERE singleton=1"))
        {q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$coherence",row.CoherenceId!);q.Parameters.AddWithValue("$owner",state.Cursor.OwnerEpoch);q.Parameters.AddWithValue("$revision",state.LegacyRevision);q.Parameters.AddWithValue("$gap",state.GapEpoch);q.Parameters.AddWithValue("$journal",state.Cursor.JournalIdHex);q.Parameters.AddWithValue("$start",state.Cursor.NextReadUsn);q.Parameters.AddWithValue("$after",start==0?-1:start-1);await q.ExecuteNonQueryAsync(ct);}
        var result=await ReadParentPlan(db,tx,id,ct);await CommitParent(db,tx,before,ct);return result;
    }
    private static async Task<(string Id,string Path,long Created)?> ParentIdentity(SqliteConnection c,SqliteTransaction t,LocalCycleScope s,ushort major,string wire,CancellationToken ct)
    {
        var known=await CoherenceNodeAsync(c,t,s.CoherenceId!,major,wire,ct,await HasParentModule(c,t,ct));if(known is null||known.Kind!=1)return null;
        var coherence=await ReadCoherenceAsync(c,t,s.CoherenceId!,ct);var ns=await ReadNamespaceAsync(c,t,coherence.State.CandidateId,ct);
        if(known.Id==ns.State.RootFileId128)return (known.Id,"",known.Created);
        await using var q=SyncCommand(c,t,"SELECT path,created FROM local_cycle_files WHERE scope_id=$scope AND node_id=$node AND kind=1 AND phase<>2");q.Parameters.AddWithValue("$scope",s.ScopeId);q.Parameters.AddWithValue("$node",known.Id);await using var r=await q.ExecuteReaderAsync(ct);return await r.ReadAsync(ct)?(known.Id,r.GetString(0),r.GetInt64(1)):null;
    }
    internal async Task<int> SeedParentFrontier(string root,string owner,string scope,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await ParentAdmission(root,owner,scope,local,256,0,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var p=await RequireParentPlan(c,t,root,owner,scope,id,ct);if(p.Status!=LocalParentReconciliationStatus.Scanning)throw new InvalidOperationException("Only scanning plan accepts retained transitions.");var s=await RequireParentCycle(c,t,root,owner,scope,ct);var source=await ReadLocalStateAsync(c,t,root,owner,ct);
        long after;await using(var q=SyncCommand(c,t,"SELECT record_after FROM local_parent_reconcile_plans WHERE plan_id=$id")){q.Parameters.AddWithValue("$id",id);after=Convert.ToInt64(await q.ExecuteScalarAsync(ct));}
        // Historical uncertainty includes ordinary data-only events. Decode a
        // bounded larger page, but keep the former 40-record mutation budget.
        // This avoids a write transaction per 40 irrelevant retained records.
        var records=new List<LocalUsnRecord>();await using(var q=SyncCommand(c,t,"SELECT usn,payload FROM local_coherence_unresolved WHERE coherence_id=$coherence AND usn>$after AND usn<$start ORDER BY usn LIMIT 256"))
        {q.Parameters.AddWithValue("$coherence",s.CoherenceId!);q.Parameters.AddWithValue("$after",after);q.Parameters.AddWithValue("$start",p.StartUsn);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))records.Add(ProjectionRecord(source.Cursor.JournalIdHex,r.GetInt64(0),source.Cursor.NextReadUsn,(byte[])r.GetValue(1)));}
        var added=0;var consumed=0;var relevant=0;var next=after;foreach(var record in records)
        {
            if((record.Reason&(NamespaceTransition|DirectoryPolicy))!=0&&relevant==40)break;
            consumed++;next=record.Usn;
            if((record.Reason&(NamespaceTransition|DirectoryPolicy))==0)continue;
            relevant++;
            (string Id,string Path,long Created)? parent;
            if((record.Reason&0x10200)!=0)
            {
                if((record.Reason&0x10000)!=0&&await CoherenceNodeAsync(c,t,s.CoherenceId!,record.MajorVersion,record.FileIdHex,ct,true)is not null)throw new LocalSourceGapException("ParentHardlinkAmbiguity","Known inside hardlink retains its baseline and requires explicit conflict resolution.");
                // An unknown link can have an inside alias. Enumerate the
                // ENTIRE current tree and prove absence at a new native cut;
                // never reinterpret the historical record as Outside.
                var coherence=await ReadCoherenceAsync(c,t,s.CoherenceId!,ct);var candidate=await ReadNamespaceAsync(c,t,coherence.State.CandidateId,ct);
                parent=await ParentIdentity(c,t,s,3,candidate.State.RootFileId128,ct)??throw new InvalidDataException("Native root identity is absent.");
                await using var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_plans SET hardlink_after=CASE WHEN full_membership=0 THEN $after ELSE hardlink_after END,full_membership=1 WHERE plan_id=$id");q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$after",after);await q.ExecuteNonQueryAsync(ct);
            }
            else
            {
                parent=await ParentIdentity(c,t,s,record.MajorVersion,record.ParentIdHex,ct);
                if(parent is null&&(record.Reason&DirectoryPolicy)!=0)parent=await ParentIdentity(c,t,s,record.MajorVersion,record.FileIdHex,ct);
            }
            if((record.Reason&0x3000)!=0)
            {
                await RestoreParentRenameOld(c,t,id,record,ct);await CarryParentRenameClose(c,t,id,record,ct);
                if(await DeferUnknownParentRenameClose(c,t,p,record,ct))
                {
                    var coherence=await ReadCoherenceAsync(c,t,s.CoherenceId!,ct);var candidate=await ReadNamespaceAsync(c,t,coherence.State.CandidateId,ct);
                    parent=await ParentIdentity(c,t,s,3,candidate.State.RootFileId128,ct)??throw new InvalidDataException("Native root identity is absent.");
                    await using var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_plans SET hardlink_after=CASE WHEN full_membership=0 THEN $after ELSE hardlink_after END,full_membership=1,hardlink_verified=0 WHERE plan_id=$id");q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$after",after);await q.ExecuteNonQueryAsync(ct);
                }
                else await RetainParentRename(c,t,id,record,ct);
            }
            if(parent is not null)
            {await using var q=SyncCommand(c,t,"INSERT INTO local_parent_reconcile_parents(plan_id,node_id,path,created,claim) VALUES($id,$node,$path,$created,$claim) ON CONFLICT(plan_id,node_id) DO NOTHING");q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$node",parent.Value.Id);q.Parameters.AddWithValue("$path",parent.Value.Path);q.Parameters.AddWithValue("$created",parent.Value.Created);q.Parameters.AddWithValue("$claim",Guid.NewGuid().ToString("N"));added+=await q.ExecuteNonQueryAsync(ct);}
        }
        await using(var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_plans SET record_after=$after,parents=parents+$added WHERE plan_id=$id")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$after",next);q.Parameters.AddWithValue("$added",added);await q.ExecuteNonQueryAsync(ct);}await CommitParent(c,t,before,ct);return consumed;
    }
    internal async Task<ParentReconcileDirectory?> ClaimParentDirectory(string root,string owner,string scope,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await ParentAdmission(root,owner,scope,local,2,0,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var p=await RequireParentPlan(c,t,root,owner,scope,id,ct);if(p.Status!=LocalParentReconciliationStatus.Scanning)return null;
        ParentReconcileDirectory? row;await using(var q=SyncCommand(c,t,"SELECT node_id,path,created,claim,state FROM local_parent_reconcile_parents WHERE plan_id=$id AND state=0 ORDER BY node_id LIMIT 1")){q.Parameters.AddWithValue("$id",id);await using var r=await q.ExecuteReaderAsync(ct);row=await r.ReadAsync(ct)?new(id,r.GetString(0),r.GetString(1),r.GetInt64(2),r.GetString(3),r.GetInt32(4)):null;}if(row is null){await t.CommitAsync(ct);return null;}
        await using(var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_parents SET state=1 WHERE plan_id=$id AND node_id=$node AND state=0")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$node",row.NodeId);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("Parent claim raced.");}await CommitParent(c,t,before,ct);return row with{State=1};
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task<LocalParentReconciliation> BeginParentReconciliationAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.BeginParentPlan(_root,_owner,scope,_admission,ct),ct);
    internal Task<int> SeedParentReconciliationAsync(string scope,string plan,CancellationToken ct)=>SerialAsync(()=>_store.SeedParentFrontier(_root,_owner,scope,plan,_admission,ct),ct);
    internal Task<ParentReconcileDirectory?> ClaimParentReconciliationAsync(string scope,string plan,CancellationToken ct)=>SerialAsync(()=>_store.ClaimParentDirectory(_root,_owner,scope,plan,_admission,ct),ct);
}
