using Microsoft.Data.Sqlite;

namespace Neptune.Core;
internal sealed record ReplacementOriginalPage(long After,IReadOnlyList<(long Number,string Operation,bool Directory)> Items);
internal sealed record ReplacementCompletedAnchor(LocalCoherenceState State,int Attempt);
public sealed partial class NeptuneStateStore
{
    internal async Task<ReplacementCompletedAnchor?> ReadReplacementCompletedAnchor(string root,string owner,string scope,string id,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);
        var plan=await RequireReplacementStage(c,t,root,owner,scope,id,ct);
        var current=await RequireCycle(c,t,root,owner,scope,false,ct);
        if(plan.Status!=LocalJournalReplacementStatus.SourceOpened)throw new InvalidOperationException("Only pending local qualification can resume its completed native cut.");
        if(current.CoherenceId is null)return null;
        var cohere=await ReadCoherenceAsync(c,t,current.CoherenceId,ct);
        var candidate=await ReadNamespaceAsync(c,t,cohere.State.CandidateId,ct);
        if(cohere.State.Status!=LocalCoherenceStatus.Anchored||candidate.State.Status!=LocalNamespaceStatus.EnumerationComplete)return null;
        _=await RequireCoherenceAsync(c,t,root,owner,current.CoherenceId,false,ct);
        // Native qualification and cycle seeding commit independently. A
        // completed owned cut survives an interruption in the bounded seed
        // loop; its exact attempt/IDs must remain pending, never be replaced.
        await using var q=SyncCommand(c,t,"SELECT 1 FROM local_cycle_replacement_attempts WHERE replacement_id=$id AND attempt=$attempt AND state=0 AND candidate_id=$candidate AND coherence_id=$cohere");
        q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$attempt",plan.CandidateAttempts);q.Parameters.AddWithValue("$candidate",cohere.State.CandidateId);q.Parameters.AddWithValue("$cohere",current.CoherenceId);
        if(await q.ExecuteScalarAsync(ct)is null)throw new LocalSourceGapException("ReplacementAttemptMismatch","Completed native cut is not the original pending candidate attempt.");
        return new(cohere.State,plan.CandidateAttempts);
    }
    private static async Task<LocalJournalReplacement> RequireReplacementStage(SqliteConnection c,SqliteTransaction t,string root,string owner,string scope,string id,CancellationToken ct)
    {
        var current=await RequireCycle(c,t,root,owner,scope,false,ct);var plan=await ReadReplacement(c,t,id,ct);
        if(plan.CurrentScopeId!=scope||!await HasReplacementLineage(c,t,scope,plan.OriginalScopeId,root,(await CycleScope(c,t,plan.OriginalScopeId,ct)).Root,current.Configuration,false,ct))throw new InvalidDataException("Replacement phase cannot rebind its owner/configuration/lineage.");return plan;
    }
    internal async Task<ReplacementOriginalPage> ReadReplacementOriginalPage(string root,string owner,string scope,string id,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);var p=await RequireReplacementStage(c,t,root,owner,scope,id,ct);long after;await using(var q=SyncCommand(c,t,"SELECT original_after FROM local_cycle_replacement_requests WHERE replacement_id=$id")){q.Parameters.AddWithValue("$id",id);after=Convert.ToInt64(await q.ExecuteScalarAsync(ct));}
        var items=new List<(long,string,bool)>();await using(var q=SyncCommand(c,t,"SELECT work_no,operation_id,entry_json FROM local_cycle_work WHERE scope_id=$old AND work_no>$after AND input_json IS NOT NULL ORDER BY work_no LIMIT 48")){q.Parameters.AddWithValue("$old",p.OriginalScopeId);q.Parameters.AddWithValue("$after",after);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct)){var e=System.Text.Json.JsonSerializer.Deserialize<LocalCycleEntry>(r.GetString(2),LocalCycleWire.Json)!;items.Add((r.GetInt64(0),r.GetString(1),e.Directory));}}return new(after,items);
    }
    internal async Task AdvanceReplacementOriginal(string root,string owner,string scope,string id,long expected,long next,LocalJournalAdmission local,CancellationToken ct)
    {
        if(next<=expected)throw new ArgumentException("Replacement original cursor must advance a qualified work page.");await CycleAdmission(root,owner,scope,local,2,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireReplacementStage(c,t,root,owner,scope,id,ct);
        await using var q=SyncCommand(c,t,"UPDATE local_cycle_replacement_requests SET original_after=$next WHERE replacement_id=$id AND original_after=$expected AND EXISTS(SELECT 1 FROM local_cycle_replacement_originals WHERE replacement_id=$id AND work_no=$next AND status IN('published','not_published')) AND NOT EXISTS(SELECT 1 FROM local_cycle_work w WHERE w.scope_id=local_cycle_replacement_requests.original_scope AND w.work_no>$expected AND w.work_no<=$next AND w.input_json IS NOT NULL AND NOT EXISTS(SELECT 1 FROM local_cycle_replacement_originals o WHERE o.replacement_id=$id AND o.operation_id=w.operation_id AND o.work_no=w.work_no AND o.status IN('published','not_published')))");q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$next",next);q.Parameters.AddWithValue("$expected",expected);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidDataException("Original cursor cannot skip an unresolved outcome.");await t.CommitAsync(ct);
    }
    internal async Task<int> ClearIncompleteReplacementSeed(string root,string owner,string scope,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,scope,local,130,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var p=await RequireReplacementStage(c,t,root,owner,scope,id,ct);var current=await CycleScope(c,t,scope,ct);if(p.Status!=LocalJournalReplacementStatus.SourceOpened)throw new InvalidOperationException("Only unqualified pending seed may be retired.");
        if(current.CoherenceId is not null){var cohere=await ReadCoherenceAsync(c,t,current.CoherenceId,ct);if((await ReadNamespaceAsync(c,t,cohere.State.CandidateId,ct)).State.Status!=LocalNamespaceStatus.Incomplete)throw new LocalSourceGapException("ReplacementBuildingRestart","Only explicitly incomplete candidate cleanup is allowed.");}
        await using(var guard=SyncCommand(c,t,"SELECT 1 FROM local_cycle_work WHERE scope_id=$scope UNION ALL SELECT 1 FROM local_cycle_files WHERE scope_id=$scope AND baseline IS NOT NULL LIMIT 1")){guard.Parameters.AddWithValue("$scope",scope);if(await guard.ExecuteScalarAsync(ct)is not null)throw new InvalidDataException("Current B/work pins replacement seed.");}
        var nodes=new List<string>();await using(var q=SyncCommand(c,t,"SELECT node_id FROM local_cycle_files WHERE scope_id=$scope ORDER BY node_id LIMIT 128")){q.Parameters.AddWithValue("$scope",scope);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))nodes.Add(r.GetString(0));}
        foreach(var node in nodes){await using var q=SyncCommand(c,t,"DELETE FROM local_cycle_files WHERE scope_id=$scope AND node_id=$node AND baseline IS NULL");q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$node",node);await q.ExecuteNonQueryAsync(ct);}
        if(nodes.Count<128){await using var q=SyncCommand(c,t,"UPDATE local_cycle_scopes SET coherence_id=NULL,seed_after=0,seed_complete=0,files=0,local_after_usn=-1,local_after_node='' WHERE scope_id=$scope");q.Parameters.AddWithValue("$scope",scope);await q.ExecuteNonQueryAsync(ct);}await t.CommitAsync(ct);return nodes.Count;
    }
    internal async Task<int> BeginReplacementAttempt(string root,string owner,string scope,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,scope,local,3,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var p=await RequireReplacementStage(c,t,root,owner,scope,id,ct);if(p.Status!=LocalJournalReplacementStatus.SourceOpened||p.CandidateAttempts>=2)throw new LocalSourceGapException("ReplacementAttemptLimit","At most two explicit native candidate attempts; original request retained.");
        var attempt=p.CandidateAttempts+1;await using var q=SyncCommand(c,t,"INSERT INTO local_cycle_replacement_attempts(replacement_id,attempt,state) VALUES($id,$attempt,0);UPDATE local_cycle_replacement_requests SET candidate_attempts=$attempt,error=NULL WHERE replacement_id=$id");q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$attempt",attempt);await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);return attempt;
    }
    internal async Task RecordReplacementCandidate(string root,string owner,string scope,string id,int attempt,string candidate,string coherence,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,scope,local,3,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireReplacementStage(c,t,root,owner,scope,id,ct);var actual=await RequireCoherenceAsync(c,t,root,owner,coherence,true,ct);if(actual.State.CandidateId!=candidate)throw new InvalidDataException("Native allocated candidate/coherence binding changed.");
        await using var q=SyncCommand(c,t,"UPDATE local_cycle_replacement_attempts SET candidate_id=$candidate,coherence_id=$cohere WHERE replacement_id=$id AND attempt=$attempt AND candidate_id IS NULL");q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$attempt",attempt);q.Parameters.AddWithValue("$candidate",candidate);q.Parameters.AddWithValue("$cohere",coherence);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidDataException("Immutable native attempt IDs already recorded.");
        // Persist the actual candidate in the scope before enumeration. An
        // abrupt exit cannot disguise a Building candidate as an empty scope.
        await using(var bind=SyncCommand(c,t,"UPDATE local_cycle_scopes SET coherence_id=$cohere WHERE scope_id=$scope AND coherence_id IS NULL")){bind.Parameters.AddWithValue("$scope",scope);bind.Parameters.AddWithValue("$cohere",coherence);if(await bind.ExecuteNonQueryAsync(ct)!=1)throw new InvalidDataException("Replacement scope already owns another candidate.");}await t.CommitAsync(ct);
    }
    internal async Task FailReplacementAttempt(string root,string owner,string scope,string id,int attempt,string reason,LocalJournalAdmission local,CancellationToken ct)
    {
        if(reason.Length>1024)reason=reason[..1024];await CycleAdmission(root,owner,scope,local,2,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireReplacementStage(c,t,root,owner,scope,id,ct);
        await using var q=SyncCommand(c,t,"UPDATE local_cycle_replacement_attempts SET state=2,reason=$reason WHERE replacement_id=$id AND attempt=$attempt AND state=0;UPDATE local_cycle_replacement_requests SET error=$reason WHERE replacement_id=$id AND status=0");q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$attempt",attempt);q.Parameters.AddWithValue("$reason",reason);await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);
    }
    internal async Task CompleteReplacementStage(string root,string owner,string scope,string id,LocalJournalReplacementStatus next,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,scope,local,3,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var p=await RequireReplacementStage(c,t,root,owner,scope,id,ct);var current=await CycleScope(c,t,scope,ct);if((int)next!=(int)p.Status+1||next>LocalJournalReplacementStatus.Installed)throw new InvalidDataException("Replacement phase is not a conditional forward transition.");
        if(current.CoherenceId is null||!current.SeedComplete||(await RequireCoherenceAsync(c,t,root,owner,current.CoherenceId,false,ct)).State.Status!=LocalCoherenceStatus.Anchored)throw new LocalSourceGapException("ReplacementLocalUnknown","Complete current native membership/cut required.");
        await using(var pending=SyncCommand(c,t,"SELECT 1 FROM local_cycle_work WHERE scope_id=$old AND work_no>(SELECT original_after FROM local_cycle_replacement_requests WHERE replacement_id=$id) AND input_json IS NOT NULL LIMIT 1")){pending.Parameters.AddWithValue("$old",p.OriginalScopeId);pending.Parameters.AddWithValue("$id",id);if(await pending.ExecuteScalarAsync(ct)is not null)throw new LocalSourceGapException("ReplacementOriginalUnresolved","Original registry outcomes must be qualified before current lineage activation.");}
        if(next>=LocalJournalReplacementStatus.RemoteQualified){await using var q=SyncCommand(c,t,"SELECT 1 FROM local_cycle_scopes WHERE scope_id=$scope AND remote_complete=1 AND snapshot_complete=1 AND pending_remote IS NULL");q.Parameters.AddWithValue("$scope",scope);if(await q.ExecuteScalarAsync(ct)is null)throw new LocalSourceGapException("ReplacementRemoteUnknown","Qualified complete current R/feed required before lineage activation.");}
        if(next==LocalJournalReplacementStatus.LocalQualified){await using var completed=SyncCommand(c,t,"UPDATE local_cycle_replacement_attempts SET state=1,reason=NULL WHERE replacement_id=$id AND attempt=$attempt AND state=0 AND coherence_id=$cohere;UPDATE local_cycle_replacement_requests SET error=NULL WHERE replacement_id=$id");completed.Parameters.AddWithValue("$id",id);completed.Parameters.AddWithValue("$attempt",p.CandidateAttempts);completed.Parameters.AddWithValue("$cohere",current.CoherenceId);if(await completed.ExecuteNonQueryAsync(ct)!=2)throw new InvalidDataException("Replacement candidate attempt is not the completed owned cut.");}
        await using(var q=SyncCommand(c,t,"UPDATE local_cycle_replacement_requests SET status=$next WHERE replacement_id=$id AND status=$previous")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$next",(int)next);q.Parameters.AddWithValue("$previous",(int)p.Status);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidDataException("Replacement activation raced.");}await t.CommitAsync(ct);
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<ReplacementCompletedAnchor?> ReadReplacementCompletedAnchorAsync(string scope,string id,CancellationToken ct)=>SerialAsync(()=>_store.ReadReplacementCompletedAnchor(_root,_owner,scope,id,ct),ct);
    internal Task<ReplacementOriginalPage> ReadReplacementOriginalPageAsync(string scope,string id,CancellationToken ct)=>SerialAsync(()=>_store.ReadReplacementOriginalPage(_root,_owner,scope,id,ct),ct);
    internal Task AdvanceReplacementOriginalAsync(string scope,string id,long expected,long next,CancellationToken ct)=>SerialAsync(async()=>{await _store.AdvanceReplacementOriginal(_root,_owner,scope,id,expected,next,_admission,ct);return true;},ct);
    internal Task<int> ClearIncompleteReplacementSeedAsync(string scope,string id,CancellationToken ct)=>SerialAsync(()=>_store.ClearIncompleteReplacementSeed(_root,_owner,scope,id,_admission,ct),ct);
    internal Task<int> BeginReplacementAttemptAsync(string scope,string id,CancellationToken ct)=>SerialAsync(()=>_store.BeginReplacementAttempt(_root,_owner,scope,id,_admission,ct),ct);
    internal Task RecordReplacementCandidateAsync(string scope,string id,int attempt,string candidate,string coherence,CancellationToken ct)=>SerialAsync(async()=>{await _store.RecordReplacementCandidate(_root,_owner,scope,id,attempt,candidate,coherence,_admission,ct);return true;},ct);
    internal Task FailReplacementAttemptAsync(string scope,string id,int attempt,string reason,CancellationToken ct)=>SerialAsync(async()=>{await _store.FailReplacementAttempt(_root,_owner,scope,id,attempt,reason,_admission,ct);return true;},ct);
    internal Task CompleteReplacementStageAsync(string scope,string id,LocalJournalReplacementStatus next,CancellationToken ct)=>SerialAsync(async()=>{await _store.CompleteReplacementStage(_root,_owner,scope,id,next,_admission,ct);return true;},ct);
}
