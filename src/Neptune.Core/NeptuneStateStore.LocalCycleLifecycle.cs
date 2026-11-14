using Microsoft.Data.Sqlite;

namespace Neptune.Core;

internal sealed record CyclePending(long Pending,long Conflicts,long Deferred);
public sealed partial class NeptuneStateStore
{
    internal async Task<LocalCycleWork?> DispatchOwnedCycleWork(string root,string owner,string id,string dispatch,LocalJournalAdmission local,CancellationToken ct,bool originalsOnly=false)
    {
        if(!Guid.TryParseExact(dispatch,"N",out _))throw new ArgumentException("Dispatch scope must be bounded UUID.");await CycleAdmission(root,owner,id,local,2,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireCycle(c,t,root,owner,id,false,ct);
        bool extensions;await using(var module=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='client_evolution_format'"))extensions=await module.ExecuteScalarAsync(ct)is not null;
        var result=await DispatchCycleWork(c,t,id,dispatch,extensions,originalsOnly,ct);
        if(result is not null)await CommitCycle(c,t,before,ct);else await t.CommitAsync(ct);
        return result;
    }
    private async Task<LocalCycleWork?> DispatchCycleWork(SqliteConnection c,SqliteTransaction t,string id,string dispatch,bool extensions,bool originalsOnly,CancellationToken ct)
    {
        string? operation=null;
        // Recover accepted original upload IDs before replaying lost prepare
        // replies. Each returned row and write remains bounded; no new intent
        // is allocated by this dispatcher, and no mutable target is consulted.
        for(var priority=0;priority<(originalsOnly?2:1);priority++)
        {
            var filter=originalsOnly?" AND input_json IS NOT NULL AND upload_id IS "+(priority==0?"NOT NULL":"NULL"):"";
            if(originalsOnly&&priority==1&&extensions)filter=" AND ((input_json IS NOT NULL AND upload_id IS NULL) OR EXISTS(SELECT 1 FROM client_stream_operations e WHERE e.operation_id=local_cycle_work.operation_id) OR EXISTS(SELECT 1 FROM client_namespace_operations e WHERE e.operation_id=local_cycle_work.operation_id) OR EXISTS(SELECT 1 FROM client_metadata_seals e WHERE e.operation_id=local_cycle_work.operation_id))";
            var due=extensions?" AND NOT EXISTS(SELECT 1 FROM client_operation_schedule s WHERE s.scope_id=local_cycle_work.scope_id AND s.operation_id=local_cycle_work.operation_id AND s.not_before>$now)":"";
            await using var q=SyncCommand(c,t,"SELECT operation_id FROM local_cycle_work WHERE scope_id=$scope AND active=1 AND phase IN(0,1,2,3,4) AND (dispatch_id IS NULL OR dispatch_id<>$dispatch)"+filter+due+" ORDER BY work_no LIMIT 1");CycleId(q,id);q.Parameters.AddWithValue("$dispatch",dispatch);if(extensions)q.Parameters.AddWithValue("$now",Clock.GetUtcNow().ToUnixTimeMilliseconds());operation=await q.ExecuteScalarAsync(ct)as string;if(operation is not null)break;
        }
        if(operation is null)return null;
        await using(var q=SyncCommand(c,t,"UPDATE local_cycle_work SET dispatch_id=$dispatch WHERE operation_id=$operation")){q.Parameters.AddWithValue("$dispatch",dispatch);q.Parameters.AddWithValue("$operation",operation);await q.ExecuteNonQueryAsync(ct);}
        return await ReadCycleWork(c,t,operation,ct);
    }
    internal async Task<IReadOnlyList<LocalCycleWork>> DispatchOwnedCycleWorkset(string root,string owner,string id,string dispatch,int maximum,LocalJournalAdmission local,CancellationToken ct)
    {
        if(!Guid.TryParseExact(dispatch,"N",out _)||maximum is <1 or >16)throw new ArgumentException("Invalid bounded dispatch workset.");
        await CycleAdmission(root,owner,id,local,8*maximum,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);
        var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);
        bool extensions;await using(var module=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='client_evolution_format'"))extensions=await module.ExecuteScalarAsync(ct)is not null;
        var result=new List<LocalCycleWork>(maximum);var existingDrained=false;
        while(result.Count<maximum)
        {
            var work=existingDrained?null:await DispatchCycleWork(c,t,id,dispatch,extensions,false,ct);
            if(work is null)
            {
                existingDrained=true;work=await ClaimCycleWork(c,t,scope,root,owner,ct);
                if(work is null)break;
                await using var mark=SyncCommand(c,t,"UPDATE local_cycle_work SET dispatch_id=$dispatch WHERE operation_id=$operation AND active=1");
                mark.Parameters.AddWithValue("$dispatch",dispatch);mark.Parameters.AddWithValue("$operation",work.OperationId);
                if(await mark.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("New work lost dispatch ownership.");
            }
            if(extensions)
            {
                await using var schedule=SyncCommand(c,t,"INSERT OR IGNORE INTO client_operation_schedule(scope_id,operation_id,kind,created_at) VALUES($scope,$operation,'work',$now)");
                CycleId(schedule,id);schedule.Parameters.AddWithValue("$operation",work.OperationId);schedule.Parameters.AddWithValue("$now",Clock.GetUtcNow().ToUnixTimeMilliseconds());
                await schedule.ExecuteNonQueryAsync(ct);
            }
            result.Add(work);
        }
        // No source read, capture or network effect runs before this commit.
        // A crash leaves every selected original available to the next dispatch ID.
        if(result.Count>0)await CommitCycle(c,t,before,ct);else await t.CommitAsync(ct);
        return result;
    }
    internal async Task<CyclePending> ReadOwnedCyclePending(string root,string owner,string id,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,id,false,ct);
        // A later journal/watcher generation can queue the file again while
        // its original writer-blocked operation is still Deferred. Count each
        // file once, and retain that explicit active-work state in the result.
        var policy=await ReadMappingPolicy(c,t,id,ct);const string included=" AND NOT EXISTS(SELECT 1 FROM json_each($excludes) e WHERE neptune_path_within(f.path,e.value))";
        long pending=0,conflicts=0,deferred=0;await using var q=SyncCommand(c,t,"SELECT f.phase,COALESCE(w.phase,-1),COUNT(*) FROM local_cycle_files f LEFT JOIN local_cycle_work w ON w.scope_id=f.scope_id AND w.node_id=f.node_id AND w.active=1 WHERE f.scope_id=$scope AND f.phase<4"+included+" GROUP BY f.phase,w.phase");CycleId(q,id);q.Parameters.AddWithValue("$excludes",System.Text.Json.JsonSerializer.Serialize(policy.ExcludedPrefixes));await using var r=await q.ExecuteReaderAsync(ct);
        while(await r.ReadAsync(ct)){var count=r.GetInt64(2);pending+=count;if(r.GetInt32(0)==2)conflicts+=count;if(r.GetInt32(0)==3||r.GetInt32(1)==8)deferred+=count;}
        await r.DisposeAsync();
        q.CommandText="SELECT COUNT(*),COALESCE(SUM(CASE WHEN f.phase=4 THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN f.phase=2 THEN 0 ELSE 1 END),0) FROM local_cycle_scopes s JOIN local_cycle_refreshes g ON g.scope_id=s.active_remote JOIN local_cycle_refresh_conflicts x ON x.scope_id=g.scope_id AND x.pass=g.reconcile_pass JOIN local_cycle_files f ON f.scope_id=s.scope_id AND f.node_id=x.node_id WHERE s.scope_id=$scope";
        q.CommandText+=included;await using var extra=await q.ExecuteReaderAsync(ct);await extra.ReadAsync(ct);pending+=extra.GetInt64(1);conflicts+=extra.GetInt64(2);
        return new(pending,conflicts,deferred);
    }
    // Resume unattempted deferred work only after an explicit caller request.
    // Original attempted intent, upload ID and sealed bytes are never replaced.
    internal async Task<int> RetryOwnedCycleDeferred(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,id,local,256,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireCycle(c,t,root,owner,id,false,ct);
        bool scheduled;await using(var module=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='client_operation_schedule'"))scheduled=await module.ExecuteScalarAsync(ct)is not null;
        var due=scheduled?" AND NOT EXISTS(SELECT 1 FROM client_operation_schedule s WHERE s.scope_id=local_cycle_work.scope_id AND s.operation_id=local_cycle_work.operation_id AND s.not_before>$now)":"";
        var rows=new List<(string Operation,string Node)>();await using(var q=SyncCommand(c,t,"SELECT operation_id,node_id FROM local_cycle_work WHERE scope_id=$scope AND active=1 AND phase=8 AND input_json IS NULL"+due+" ORDER BY work_no LIMIT 120")){CycleId(q,id);if(scheduled)q.Parameters.AddWithValue("$now",Clock.GetUtcNow().ToUnixTimeMilliseconds());await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))rows.Add((r.GetString(0),r.GetString(1)));}
        foreach(var row in rows)
        {
            await using var q=SyncCommand(c,t,"UPDATE local_cycle_work SET phase=CASE WHEN capture_proof IS NULL THEN 0 ELSE 1 END,error=NULL WHERE operation_id=$operation AND phase=8 AND input_json IS NULL;UPDATE local_cycle_files SET phase=1 WHERE scope_id=$scope AND node_id=$node");CycleId(q,id);q.Parameters.AddWithValue("$operation",row.Operation);q.Parameters.AddWithValue("$node",row.Node);await q.ExecuteNonQueryAsync(ct);
        }
        await CommitCycle(c,t,before,ct);return rows.Count;
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<IReadOnlyList<LocalCycleWork>> DispatchCycleWorksetAsync(string id,string dispatch,int maximum,CancellationToken ct)
        =>SerialAsync(()=>_store.DispatchOwnedCycleWorkset(_root,_owner,id,dispatch,maximum,_admission,ct),ct);
    internal Task<LocalCycleWork?> DispatchCycleWorkAsync(string id,string dispatch,CancellationToken ct)=>SerialAsync(()=>_store.DispatchOwnedCycleWork(_root,_owner,id,dispatch,_admission,ct),ct);
    internal Task<LocalCycleWork?> DispatchCycleOriginalAsync(string id,string dispatch,CancellationToken ct)=>SerialAsync(()=>_store.DispatchOwnedCycleWork(_root,_owner,id,dispatch,_admission,ct,originalsOnly:true),ct);
    internal Task<CyclePending> ReadCyclePendingAsync(string id,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedCyclePending(_root,_owner,id,ct),ct);
    internal Task<int> RetryCycleDeferredAsync(string id,CancellationToken ct)=>SerialAsync(()=>_store.RetryOwnedCycleDeferred(_root,_owner,id,_admission,ct),ct);
}
