using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

// Totals describe stored confirmed historical baselines. They are neither
// bytes read/captured this cycle nor an instantaneous source-clean claim.
public sealed record WindowsCycleConfirmedTotals(long Files,long Bytes);
public sealed partial class NeptuneStateStore
{
    private static long CycleBaselineBytes(string raw)
    {
        if(Encoding.UTF8.GetByteCount(raw)>40*1024)throw new InvalidDataException("Stored baseline exceeds fixed bound.");
        using var doc=Sync.Protocol.SyncProtocolReader.Parse(Encoding.UTF8.GetBytes(raw));var baseline=JsonSerializer.Deserialize<LocalCycleBaseline>(doc.RootElement.GetRawText(),LocalCycleWire.Json)??throw new InvalidDataException("Stored baseline absent.");
        if(baseline.Description.Type!="file")throw new InvalidDataException("File baseline has another type.");return LocalCycleWire.Decimal(baseline.Description.SizeBytes);
    }
    private async Task<Dictionary<string,WindowsCycleConfirmedTotals>> BackfillCycleConfirmedTotals(CancellationToken ct)
    {
        // Once per explicit v1 migration, never on repeat/restart. Read-only
        // snapshot, <=256 rows/page and <=16 totals retained in memory.
        await using var c=new SqliteConnection(ReadOnlySyncConnectionString(databasePath));await c.OpenAsync(ct);using var t=c.BeginTransaction(deferred:true);var scopes=new List<string>(16);
        await using(var q=SyncCommand(c,t,"SELECT scope_id FROM local_cycle_scopes ORDER BY scope_id LIMIT 17")){await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))scopes.Add(r.GetString(0));}
        if(scopes.Count>16)throw new InvalidDataException("V1 scope count exceeds recognized bound.");var totals=new Dictionary<string,WindowsCycleConfirmedTotals>(StringComparer.Ordinal);
        foreach(var scope in scopes)
        {
            string after="";long files=0,bytes=0;
            while(true)
            {
                ct.ThrowIfCancellationRequested();var rows=new List<(string Node,string Baseline)>(256);
                await using(var q=SyncCommand(c,t,"SELECT node_id,baseline FROM local_cycle_files WHERE scope_id=$scope AND node_id>$after AND kind=0 AND baseline IS NOT NULL ORDER BY node_id LIMIT 256")){CycleId(q,scope);q.Parameters.AddWithValue("$after",after);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))rows.Add((r.GetString(0),r.GetString(1)));}
                foreach(var row in rows){files=checked(files+1);bytes=checked(bytes+CycleBaselineBytes(row.Baseline));}if(rows.Count<256)break;after=rows[^1].Node;
            }
            totals.Add(scope,new(files,bytes));
        }
        return totals;
    }
    private static async Task WriteCycleConfirmedTotals(SqliteConnection c,SqliteTransaction t,string scope,WindowsCycleConfirmedTotals totals,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,"UPDATE local_cycle_scopes SET confirmed_files=$files,confirmed_bytes=$bytes WHERE scope_id=$scope");CycleId(q,scope);q.Parameters.AddWithValue("$files",totals.Files);q.Parameters.AddWithValue("$bytes",totals.Bytes);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidDataException("Migration lost totals scope.");
    }
    private static async Task PromoteCycleConfirmedTotals(SqliteConnection c,SqliteTransaction t,string scope,LocalCycleEntry current,Sync.Protocol.SyncDescriptor historical,long number,CancellationToken ct)
    {
        if(current.Directory)return;long previous;await using(var q=SyncCommand(c,t,"SELECT baseline_work FROM local_cycle_files WHERE scope_id=$scope AND node_id=$node")){CycleId(q,scope);q.Parameters.AddWithValue("$node",current.NodeId);previous=Convert.ToInt64(await q.ExecuteScalarAsync(ct));}
        if(previous>number)return;var old=current.BaselineJson is null?0:CycleBaselineBytes(current.BaselineJson);var added=current.BaselineJson is null?1:0;var delta=checked(LocalCycleWire.Decimal(historical.SizeBytes)-old);
        if(previous==number&&current.BaselineJson is not null&&delta!=0)throw new InvalidDataException("Same baseline work changed its confirmed bytes.");
        if(added==0&&delta==0)return;
        await using var update=SyncCommand(c,t,"UPDATE local_cycle_scopes SET confirmed_files=confirmed_files+$count,confirmed_bytes=confirmed_bytes+$bytes WHERE scope_id=$scope AND confirmed_files>=-$count AND confirmed_bytes>=-$bytes AND confirmed_files<=9223372036854775807-$count AND ($bytes<=0 OR confirmed_bytes<=9223372036854775807-$bytes)");CycleId(update,scope);update.Parameters.AddWithValue("$count",added);update.Parameters.AddWithValue("$bytes",delta);if(await update.ExecuteNonQueryAsync(ct)!=1)throw new InvalidDataException("Confirmed baseline totals overflow or lost consistency.");
    }
    internal async Task<WindowsCycleConfirmedTotals> ReadOwnedCycleConfirmedTotals(string root,string owner,string id,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,id,false,ct);await using var q=SyncCommand(c,t,"SELECT confirmed_files,confirmed_bytes FROM local_cycle_scopes WHERE scope_id=$scope");CycleId(q,id);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))throw new InvalidDataException("Totals scope absent.");return new(r.GetInt64(0),r.GetInt64(1));
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<WindowsCycleConfirmedTotals> ReadCycleConfirmedTotalsAsync(string id,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedCycleConfirmedTotals(_root,_owner,id,ct),ct);
}
