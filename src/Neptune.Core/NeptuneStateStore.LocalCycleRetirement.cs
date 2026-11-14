using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed record LocalCycleHistoryRetirement(int Records,long Bytes,bool ScopeRemoved,long ChangedRows,string? Blocked=null);
internal sealed record LocalCycleRetirementPage(IReadOnlyList<LocalCycleWork> Records,long After,long Last);
public sealed partial class NeptuneStateStore
{
    // No current file/dirty row is mutated. The indexed node join pins its
    // latest baseline work; active and uncertain originals are never selected.
    internal async Task<LocalCycleRetirementPage> ReadCycleRetirableWork(string root,string owner,string id,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,id,false,ct);
        long after;await using(var cursor=SyncCommand(c,t,"SELECT work_retire_after FROM local_cycle_scopes WHERE scope_id=$scope")){CycleId(cursor,id);after=Convert.ToInt64(await cursor.ExecuteScalarAsync(ct));}
        // Two exact-phase keyset pages examine at most 240 terminal rows,
        // then merge at most 120. Reuse the existing phase/work index: adding
        // a 500k-row partial index would make migration an unbounded write.
        var pages=new List<(LocalCycleWork Work,long Number)>(240);
        foreach(var phase in new[]{5,6})
        {
            await using var q=SyncCommand(c,t,$"SELECT {string.Join(',',CycleWorkColumns.Split(',').Select(s=>"w."+s))},w.work_no FROM local_cycle_work w INDEXED BY local_cycle_work_page WHERE w.scope_id=$scope AND w.active IS NULL AND w.phase=$phase AND w.work_no>$after ORDER BY w.work_no LIMIT 120");
            CycleId(q,id);q.Parameters.AddWithValue("$after",after);q.Parameters.AddWithValue("$phase",phase);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))pages.Add((CycleWork(r),r.GetInt64(r.FieldCount-1)));
        }
        var rows=pages.OrderBy(p=>p.Number).Take(120).ToArray();return new(rows.Select(p=>p.Work).ToArray(),after,rows.Length==0?0:rows[^1].Number);
    }
    internal async Task<LocalCycleHistoryRetirement> RetireCycleWork(string root,string owner,string id,LocalCycleRetirementPage page,IReadOnlyList<string> operations,LocalJournalAdmission local,CancellationToken ct)
    {
        if(page.Records.Count>120||operations.Count>120||operations.Distinct(StringComparer.Ordinal).Count()!=operations.Count||operations.Any(op=>!page.Records.Any(w=>w.OperationId==op)))throw new ArgumentOutOfRangeException(nameof(operations));
        await CycleAdmission(root,owner,id,local,operations.Count+3,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireCycle(c,t,root,owner,id,false,ct);long bytes=0;var count=0;
        await using(var cursor=SyncCommand(c,t,"UPDATE local_cycle_scopes SET work_retire_after=$last WHERE scope_id=$scope AND work_retire_after=$after")){CycleId(cursor,id);cursor.Parameters.AddWithValue("$last",page.Last);cursor.Parameters.AddWithValue("$after",page.After);if(await cursor.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("Retirement page cursor changed; no history was discarded.");}
        foreach(var operation in operations)
        {
            if(await HasCycleBatchState(c,t,ct))
            {await using var pin=SyncCommand(c,t,"SELECT 1 FROM local_cycle_batch_items WHERE operation_id=$op LIMIT 1");pin.Parameters.AddWithValue("$op",operation);if(await pin.ExecuteScalarAsync(ct)is not null)continue;}
            ct.ThrowIfCancellationRequested();await using var find=SyncCommand(c,t,"SELECT w.bytes FROM local_cycle_work w WHERE w.scope_id=$scope AND w.operation_id=$operation AND w.active IS NULL AND w.phase IN(5,6) AND NOT EXISTS(SELECT 1 FROM local_cycle_files f WHERE f.scope_id=w.scope_id AND f.node_id=w.node_id AND f.baseline_work=w.work_no)");CycleId(find,id);find.Parameters.AddWithValue("$operation",operation);
            if(await find.ExecuteScalarAsync(ct)is not long size)continue;
            await using var q=SyncCommand(c,t,"DELETE FROM local_cycle_work WHERE operation_id=$operation");q.Parameters.AddWithValue("$operation",operation);count+=await q.ExecuteNonQueryAsync(ct);bytes+=size;
        }
        await AddCycleBytes(c,t,id,-bytes,ct);return new(count,bytes,false,await CommitCycle(c,t,before,ct));
    }
    // An explicit superseded scope is removable only without historical byte
    // authority or attempted/retained work. Unknown originals deliberately pin
    // the scope; capacity refusal is safer than losing their recovery identity.
    internal async Task<LocalCycleHistoryRetirement> RetireCycleScope(string root,string owner,string current,string previous,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,current,local,122,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var live=await RequireCycle(c,t,root,owner,current,false,ct);var old=await CycleScope(c,t,previous,ct);
        if(previous==current||old.Root!=root||old.Status!=3||!SameCyclePrincipal(live.Configuration,old.Configuration))throw new InvalidOperationException("Retire only an explicit superseded scope with the same root/principal.");
        if(await HasClientOriginals(c,t,previous,false,ct)){await t.CommitAsync(ct);return new(0,0,false,0,"Historical namespace/stream originals pin this scope.");}
        if(await HasCycleBatchState(c,t,ct))
        {await using var pin=SyncCommand(c,t,"SELECT 1 FROM local_cycle_batches WHERE scope_id=$scope LIMIT 1");CycleId(pin,previous);if(await pin.ExecuteScalarAsync(ct)is not null){await t.CommitAsync(ct);return new(0,0,false,0,"Retained batch originals pin this scope.");}}
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_cycle_active WHERE scope_id=$scope"))
        {CycleId(q,previous);if(await q.ExecuteScalarAsync(ct)is not null){await t.CommitAsync(ct);return new(0,0,false,0,"Active scope is pinned.");}}
        int phase;await using(var q=SyncCommand(c,t,"SELECT retire_phase FROM local_cycle_scopes WHERE scope_id=$scope")){CycleId(q,previous);phase=Convert.ToInt32(await q.ExecuteScalarAsync(ct));}
        if(phase is <0 or >3)throw new InvalidDataException("Unknown retirement phase.");
        var keys=new List<(object Id,long Bytes)>();var table=phase switch{0=>"local_cycle_work",1=>"local_cycle_remote",_=>"local_cycle_files"};var column=phase==0?"work_no":"node_id";
        if(phase<3)
        {
            var pinColumn=phase==0?"input_json IS NOT NULL OR spool_path IS NOT NULL OR phase NOT IN(0,7,9)":phase==2?"baseline_work<>0":"0";
            await using(var q=SyncCommand(c,t,$"SELECT {column},bytes,({pinColumn}) FROM {table} WHERE scope_id=$scope ORDER BY {column} LIMIT 120"))
            {CycleId(q,previous);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct)){if(r.GetInt64(2)!=0){await r.DisposeAsync();await t.CommitAsync(ct);return new(0,0,false,0,"Retained baseline, sealed bytes or original attempt pins bounded retirement page.");}keys.Add((r.GetValue(0),r.GetInt64(1)));}}
            var incarnations=phase==2&&await HasIncarnations(c,t,ct);
            // Preserve the same row bound: drain incarnation references first;
            // the following call can remove their now unreferenced file rows.
            if(incarnations)
            {
                var count=0;
                foreach(var key in keys){await using var drop=SyncCommand(c,t,"DELETE FROM local_parent_reconcile_incarnations WHERE scope_id=$scope AND node_id=$key");CycleId(drop,previous);drop.Parameters.AddWithValue("$key",key.Id);count+=await drop.ExecuteNonQueryAsync(ct);}
                if(count>0){await using var charge=SyncCommand(c,t,"UPDATE local_parent_reconcile_format SET bytes=bytes-$bytes WHERE singleton=1");charge.Parameters.AddWithValue("$bytes",512*count);await charge.ExecuteNonQueryAsync(ct);return new(count,0,false,await CommitCycle(c,t,before,ct));}
            }
            foreach(var key in keys){ct.ThrowIfCancellationRequested();await using var q=SyncCommand(c,t,$"DELETE FROM {table} WHERE scope_id=$scope AND {column}=$key");CycleId(q,previous);q.Parameters.AddWithValue("$key",key.Id);await q.ExecuteNonQueryAsync(ct);}
        }
        var removed=phase==3;var bytes=removed?await CycleStoredBytes(c,t,previous,ct):keys.Sum(k=>k.Bytes);
        if(removed)
        {
            // Empty probes use each table's scope prefix, rather than scanning
            // every historical row to infer that no pin remains.
            foreach(var retained in new[]{"local_cycle_work","local_cycle_remote","local_cycle_files"})
            {await using var pin=SyncCommand(c,t,$"SELECT 1 FROM {retained} WHERE scope_id=$scope LIMIT 1");CycleId(pin,previous);if(await pin.ExecuteScalarAsync(ct)is not null){await t.CommitAsync(ct);return new(0,0,false,0,"Retirement stages still contain retained rows.");}}
            // Generation rows reference the superseded scope. Drain one
            // bounded page only after work/files/legacy R are proven empty;
            // their earlier stages pin every retained original baseline.
            string? refresh;await using(var find=SyncCommand(c,t,"SELECT scope_id FROM local_cycle_refreshes WHERE cycle_id=$scope ORDER BY scope_id LIMIT 1")){CycleId(find,previous);refresh=await find.ExecuteScalarAsync(ct)as string;}
            if(refresh is not null)
            {
                var remoteRows=new List<(string Node,long Bytes)>(120);var remoteTable="local_cycle_refresh_remote";
                async Task ReadRefreshRows(){await using var find=SyncCommand(c,t,$"SELECT node_id,bytes FROM {remoteTable} WHERE scope_id=$scope ORDER BY node_id LIMIT 120");CycleId(find,refresh);await using var reader=await find.ExecuteReaderAsync(ct);while(await reader.ReadAsync(ct))remoteRows.Add((reader.GetString(0),reader.GetInt64(1)));}
                await ReadRefreshRows();if(remoteRows.Count==0){remoteTable="local_cycle_refresh_conflicts";await ReadRefreshRows();}
                long refreshBytes=0;foreach(var row in remoteRows){await using var remove=SyncCommand(c,t,$"DELETE FROM {remoteTable} WHERE scope_id=$scope AND node_id=$node");CycleId(remove,refresh);remove.Parameters.AddWithValue("$node",row.Node);await remove.ExecuteNonQueryAsync(ct);refreshBytes+=row.Bytes;}
                if(remoteRows.Count==0){await using var remove=SyncCommand(c,t,"SELECT bytes FROM local_cycle_refreshes WHERE scope_id=$scope");CycleId(remove,refresh);refreshBytes=Convert.ToInt64(await remove.ExecuteScalarAsync(ct));remove.CommandText="DELETE FROM local_cycle_refreshes WHERE scope_id=$scope";await remove.ExecuteNonQueryAsync(ct);}
                await AddCycleBytes(c,t,previous,-refreshBytes,ct);return new(remoteRows.Count,refreshBytes,false,await CommitCycle(c,t,before,ct));
            }
            await using var q=SyncCommand(c,t,"DELETE FROM local_cycle_scopes WHERE scope_id=$scope");CycleId(q,previous);await q.ExecuteNonQueryAsync(ct);await using var f=SyncCommand(c,t,"UPDATE local_cycle_format SET bytes=bytes-$bytes WHERE singleton=1");f.Parameters.AddWithValue("$bytes",bytes);await f.ExecuteNonQueryAsync(ct);
        }
        else
        {
            await AddCycleBytes(c,t,previous,-bytes,ct);await using var q=SyncCommand(c,t,"UPDATE local_cycle_scopes SET retire_phase=retire_phase+$next WHERE scope_id=$scope");CycleId(q,previous);q.Parameters.AddWithValue("$next",keys.Count<120?1:0);await q.ExecuteNonQueryAsync(ct);
        }
        return new(keys.Count,bytes,removed,await CommitCycle(c,t,before,ct));
    }
    private static async Task<long> CycleStoredBytes(SqliteConnection c,SqliteTransaction t,string id,CancellationToken ct)
    {await using var q=SyncCommand(c,t,"SELECT bytes FROM local_cycle_scopes WHERE scope_id=$scope");CycleId(q,id);return Convert.ToInt64(await q.ExecuteScalarAsync(ct));}
    private static async Task<bool> HasCycleBatchState(SqliteConnection c,SqliteTransaction t,CancellationToken ct)
    {await using var q=SyncCommand(c,t,"SELECT version FROM local_cycle_format WHERE singleton=1");return Equals(await q.ExecuteScalarAsync(ct),7L);}
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<LocalCycleRetirementPage> ReadCycleRetirableWorkAsync(string id,CancellationToken ct)=>SerialAsync(()=>_store.ReadCycleRetirableWork(_root,_owner,id,ct),ct);
    internal Task<LocalCycleHistoryRetirement> RetireCycleWorkAsync(string id,LocalCycleRetirementPage page,IReadOnlyList<string> operations,CancellationToken ct)=>SerialAsync(()=>_store.RetireCycleWork(_root,_owner,id,page,operations,_admission,ct),ct);
    internal Task<LocalCycleHistoryRetirement> RetireCycleScopeAsync(string id,string previous,CancellationToken ct)=>SerialAsync(()=>_store.RetireCycleScope(_root,_owner,id,previous,_admission,ct),ct);
}
