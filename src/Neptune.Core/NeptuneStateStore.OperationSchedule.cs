using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed record SyncOperationSchedule(long Waiting, long OldestPendingMilliseconds, long? NextAttemptUnixMilliseconds, string? State);
internal sealed record SyncRetryReason(string State,string Reason,TimeSpan? RetryAfter=null);

public sealed partial class NeptuneStateStore
{
    internal async Task<bool> ScheduleReady(string root,string owner,string scope,string operation,string kind,CancellationToken ct)
    {
        // A bounded dispatch can already have created the durable row. Recheck
        // the current owner/deadline, without a redundant writer transaction.
        await using(var read=await OpenCycleConnection(ct))
        {
            using var snapshot=read.BeginTransaction(deferred:true);_=await RequireCycle(read,snapshot,root,owner,scope,true,ct);
            await using var query=SyncCommand(read,snapshot,"SELECT not_before FROM client_operation_schedule WHERE scope_id=$scope AND operation_id=$operation");
            CycleId(query,scope);query.Parameters.AddWithValue("$operation",operation);
            if(await query.ExecuteScalarAsync(ct)is long deadline)return deadline<=Clock.GetUtcNow().ToUnixTimeMilliseconds();
        }
        await ClientAdmission(root,owner,scope,2,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);
        _=await RequireCycle(c,t,root,owner,scope,true,ct);
        await using var q=SyncCommand(c,t,"INSERT OR IGNORE INTO client_operation_schedule(scope_id,operation_id,kind,created_at) VALUES($scope,$operation,$kind,$now)");
        CycleId(q,scope);q.Parameters.AddWithValue("$operation",operation);q.Parameters.AddWithValue("$kind",kind);q.Parameters.AddWithValue("$now",Clock.GetUtcNow().ToUnixTimeMilliseconds());
        await q.ExecuteNonQueryAsync(ct);q.CommandText="SELECT not_before FROM client_operation_schedule WHERE scope_id=$scope AND operation_id=$operation";
        var due=Convert.ToInt64(await q.ExecuteScalarAsync(ct));await t.CommitAsync(ct);return due<=Clock.GetUtcNow().ToUnixTimeMilliseconds();
    }
    internal async Task SaveOperationWait(string root,string owner,string scope,string operation,SyncRetryReason reason,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scope,2,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        await using var q=SyncCommand(c,t,"SELECT attempts FROM client_operation_schedule WHERE scope_id=$scope AND operation_id=$operation");CycleId(q,scope);q.Parameters.AddWithValue("$operation",operation);
        var count=await q.ExecuteScalarAsync(ct)??throw new InvalidOperationException("Operation was not admitted to its durable schedule.");
        var attempts=Math.Min(1024,Convert.ToInt32(count)+1);var now=Clock.GetUtcNow().ToUnixTimeMilliseconds();
        var ceiling=Math.Min(60_000d,1000d*Math.Pow(2,Math.Min(attempts-1,6)));
        var delay=attempts>=8?300_000+Random.Shared.Next(0,60_001):Random.Shared.NextDouble()*ceiling;
        delay=Math.Max(delay,reason.RetryAfter?.TotalMilliseconds??0);
        var next=reason.State=="AUTH_REQUIRED"?long.MaxValue:checked(now+(long)Math.Min(delay,long.MaxValue-now));
        var state=attempts>=8&&reason.State!="AUTH_REQUIRED"?"DEGRADED":reason.State;
        q.CommandText="UPDATE client_operation_schedule SET state=$state,reason=$reason,attempts=$attempts,not_before=$next WHERE scope_id=$scope AND operation_id=$operation";
        q.Parameters.AddWithValue("$state",state);q.Parameters.AddWithValue("$reason",reason.Reason);q.Parameters.AddWithValue("$attempts",attempts);q.Parameters.AddWithValue("$next",next);
        await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);
    }
    internal async Task ClearOperationWait(string root,string owner,string scope,string operation,CancellationToken ct)
    {
        // A terminal batch receipt clears its schedule in the same commit.
        // Individual outcomes retain the existing guarded writer path below.
        await using(var read=await OpenCycleConnection(ct))
        {
            using var snapshot=read.BeginTransaction(deferred:true);_=await RequireCycle(read,snapshot,root,owner,scope,true,ct);
            await using var query=SyncCommand(read,snapshot,"SELECT 1 FROM client_operation_schedule WHERE scope_id=$scope AND operation_id=$operation");
            CycleId(query,scope);query.Parameters.AddWithValue("$operation",operation);
            if(await query.ExecuteScalarAsync(ct)is null)return;
        }
        await ClientAdmission(root,owner,scope,2,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        await using var q=SyncCommand(c,t,"DELETE FROM client_operation_schedule WHERE scope_id=$scope AND operation_id=$operation");CycleId(q,scope);q.Parameters.AddWithValue("$operation",operation);await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);
    }
    internal async Task<int> ResumeDueOperations(string root,string owner,string scope,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scope,375,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        // Scheduler metadata is not historical authority. Retire only rows
        // whose immutable work/mutation is already terminal or no longer exists.
        await using(var clean=SyncCommand(c,t,"DELETE FROM client_operation_schedule WHERE scope_id=$scope AND operation_id IN(SELECT s.operation_id FROM client_operation_schedule s WHERE s.scope_id=$scope AND s.kind<>'cycle' AND NOT EXISTS(SELECT 1 FROM local_cycle_work w WHERE w.scope_id=s.scope_id AND w.operation_id=s.operation_id AND w.active=1) AND NOT EXISTS(SELECT 1 FROM client_namespace_operations n WHERE n.scope_id=s.scope_id AND n.operation_id=s.operation_id AND n.state NOT IN('PUBLISHED','NOT_PUBLISHED','CANCELLED')) LIMIT 120)")){CycleId(clean,scope);await clean.ExecuteNonQueryAsync(ct);}
        await using var q=SyncCommand(c,t,"SELECT w.operation_id,w.node_id FROM local_cycle_work w JOIN client_operation_schedule s ON s.scope_id=w.scope_id AND s.operation_id=w.operation_id WHERE w.scope_id=$scope AND w.active=1 AND w.phase=8 AND s.not_before<=$now ORDER BY s.not_before LIMIT 120");CycleId(q,scope);q.Parameters.AddWithValue("$now",Clock.GetUtcNow().ToUnixTimeMilliseconds());
        var rows=new List<(string Operation,string Node)>();await using(var r=await q.ExecuteReaderAsync(ct))while(await r.ReadAsync(ct))rows.Add((r.GetString(0),r.GetString(1)));
        foreach(var row in rows)
        {
            await using var update=SyncCommand(c,t,"UPDATE local_cycle_work SET phase=CASE WHEN input_json IS NOT NULL THEN 4 WHEN capture_proof IS NOT NULL THEN 1 ELSE 0 END WHERE operation_id=$operation AND active=1 AND phase=8;UPDATE local_cycle_files SET phase=1 WHERE scope_id=$scope AND node_id=$node AND phase=3");CycleId(update,scope);update.Parameters.AddWithValue("$operation",row.Operation);update.Parameters.AddWithValue("$node",row.Node);await update.ExecuteNonQueryAsync(ct);
        }
        await t.CommitAsync(ct);return rows.Count;
    }
    internal async Task<SyncOperationSchedule> ReadOperationSchedule(string root,string owner,string scope,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        await using var q=SyncCommand(c,t,"SELECT COUNT(*),MIN(created_at),MIN(CASE WHEN state<>'READY' THEN not_before END) FROM client_operation_schedule WHERE scope_id=$scope");CycleId(q,scope);
        long count,age;long? next;await using(var r=await q.ExecuteReaderAsync(ct)){await r.ReadAsync(ct);count=r.GetInt64(0);age=r.IsDBNull(1)?0:Math.Max(0,Clock.GetUtcNow().ToUnixTimeMilliseconds()-r.GetInt64(1));next=r.IsDBNull(2)?null:r.GetInt64(2);}
        q.CommandText="SELECT state FROM client_operation_schedule WHERE scope_id=$scope AND state<>'READY' ORDER BY CASE WHEN state='AUTH_REQUIRED' THEN 0 ELSE 1 END,not_before LIMIT 1";
        return new(count,age,next,await q.ExecuteScalarAsync(ct)as string);
    }
    public async Task ResumeClientAuthorizationAsync(CancellationToken ct=default)
    {
        if(!await PreflightClientEvolution(ct))return;
        await using var owner=new FileStream(Path.GetFullPath(databasePath)+".local-source-owner.lock",FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None);
        await ValidateProfileCompatibilityAsync(ct);
        while(true)
        {
            await using var c=await OpenCycleConnection(ct);string scope;LocalJournalAdmission admission;
            using(var snapshot=c.BeginTransaction(deferred:true))
            {
                await using var find=SyncCommand(c,snapshot,"SELECT c.scope_id,c.max_profile,c.free_floor FROM local_cycle_scopes c WHERE EXISTS(SELECT 1 FROM client_operation_schedule s WHERE s.scope_id=c.scope_id AND s.state='AUTH_REQUIRED') LIMIT 1");
                await using var row=await find.ExecuteReaderAsync(ct);if(!await row.ReadAsync(ct))return;
                scope=row.GetString(0);admission=new(MaximumDatabaseBytes:row.GetInt64(1),MinimumFreeDiskBytes:row.GetInt64(2));
            }
            await CheckLocalAdmissionAsync(admission,0,NamespaceReserve(128),ct);using var t=c.BeginTransaction(deferred:false);
            await using var q=SyncCommand(c,t,"UPDATE client_operation_schedule SET state='RETRY_WAIT',not_before=0,attempts=0 WHERE scope_id=$scope AND operation_id IN(SELECT operation_id FROM client_operation_schedule WHERE scope_id=$scope AND state='AUTH_REQUIRED' LIMIT 120)");CycleId(q,scope);
            await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);
        }
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<bool> ScheduleReadyAsync(string scope,string operation,string kind,CancellationToken ct)=>SerialAsync(()=>_store.ScheduleReady(_root,_owner,scope,operation,kind,ct),ct);
    internal Task SaveOperationWaitAsync(string scope,string operation,SyncRetryReason reason,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOperationWait(_root,_owner,scope,operation,reason,ct);return true;},ct);
    internal Task ClearOperationWaitAsync(string scope,string operation,CancellationToken ct)=>SerialAsync(async()=>{await _store.ClearOperationWait(_root,_owner,scope,operation,ct);return true;},ct);
    internal Task<int> ResumeDueOperationsAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.ResumeDueOperations(_root,_owner,scope,ct),ct);
    internal Task<SyncOperationSchedule> ReadOperationScheduleAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.ReadOperationSchedule(_root,_owner,scope,ct),ct);
}
