using Microsoft.Data.Sqlite;

namespace Neptune.Core;

internal sealed record LocalEventHint(string Path, long Generation);
internal sealed record LocalEventPage(IReadOnlyList<LocalEventHint> Hints, bool Gap,long GapGeneration=0,long? NextDue=null);

public sealed partial class NeptuneStateStore
{
    private static readonly Dictionary<string,string> EventInboxSchema=new(StringComparer.Ordinal)
    {
        ["client_event_format"]="CREATE TABLE client_event_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL)",
        ["client_event_inbox"]="CREATE TABLE client_event_inbox(root_key TEXT NOT NULL,path TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),first_at INTEGER NOT NULL,last_at INTEGER NOT NULL,due_at INTEGER NOT NULL,PRIMARY KEY(root_key,path)) WITHOUT ROWID",
        ["client_event_due"]="CREATE INDEX client_event_due ON client_event_inbox(root_key,due_at,path)",
        ["client_event_gaps"]="CREATE TABLE client_event_gaps(root_key TEXT PRIMARY KEY,generation INTEGER NOT NULL)"
    };
    private async Task<bool> PreflightEventInbox(CancellationToken ct)
    {
        if(!File.Exists(databasePath))return false;
        await using var c=new SqliteConnection(ReadOnlySyncConnectionString(databasePath));await c.OpenAsync(ct);using var t=c.BeginTransaction(deferred:true);
        await using var q=SyncCommand(c,t,"SELECT name,sql FROM sqlite_master WHERE (name GLOB 'client_event_*' OR tbl_name GLOB 'client_event_*') AND name NOT LIKE 'sqlite_autoindex_%'");
        var rows=new Dictionary<string,string>(StringComparer.Ordinal);
        await using(var r=await q.ExecuteReaderAsync(ct))while(await r.ReadAsync(ct))
        {if(rows.Count>=EventInboxSchema.Count)throw new InvalidDataException("Unknown durable event inbox object.");rows.Add(r.GetString(0),r.IsDBNull(1)?"":r.GetString(1));}
        if(rows.Count==0)return false;
        static string Normalize(string value)=>string.Concat(value.Where(c=>!char.IsWhiteSpace(c))).TrimEnd(';');
        if(rows.Count!=EventInboxSchema.Count||EventInboxSchema.Any(pair=>!rows.TryGetValue(pair.Key,out var sql)||Normalize(sql)!=Normalize(pair.Value)))throw new InvalidDataException("Unsupported durable event inbox schema; preserved without writes.");
        q.CommandText="SELECT version FROM client_event_format WHERE singleton=1";
        if(await q.ExecuteScalarAsync(ct)is not long version||version!=1)throw new InvalidDataException("Unsupported durable event inbox version.");
        return true;
    }
    internal async Task EnsureEventInboxAsync(CancellationToken ct)
    {
        if(await PreflightEventInbox(ct))return;
        await using var migration = await AcquireSyncMigrationLockAsync(ct);
        if(await PreflightEventInbox(ct))return;
        await using var c = await OpenLocalSourceConnectionAsync(ct);
        using var t = c.BeginTransaction(deferred: false);
        foreach(var sql in EventInboxSchema.Values)await ExecuteSyncAsync(c,t,sql,ct);
        await ExecuteSyncAsync(c,t,"INSERT INTO client_event_format VALUES(1,1)",ct);
        await t.CommitAsync(ct);
    }
    internal async Task<long?> SaveEventHintsAsync(string root,string owner,IReadOnlyList<string> paths,bool gap,LocalJournalAdmission admission,CancellationToken ct)
    {
        if(paths.Count>128)throw new ArgumentOutOfRangeException(nameof(paths));
        await CheckLocalAdmissionAsync(admission,0,NamespaceReserve(paths.Count+4),ct);
        await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);
        _=await ReadLocalStateAsync(c,t,root,owner,ct);
        var now=Clock.GetUtcNow().ToUnixTimeMilliseconds();
        foreach(var path in paths.Distinct(StringComparer.Ordinal))
        {
            if(path.Length is <1 or >4096||Path.IsPathRooted(path)||path.Split('/').Any(p=>p is "" or "." or ".."))throw new InvalidDataException("Invalid event alias.");
            await using var q=SyncCommand(c,t,"""
                INSERT INTO client_event_inbox(root_key,path,generation,first_at,last_at,due_at)
                SELECT $root,$path,1,$now,$now,$now+750 WHERE EXISTS(SELECT 1 FROM client_event_inbox WHERE root_key=$root AND path=$path) OR (SELECT COUNT(*) FROM client_event_inbox WHERE root_key=$root)<4096
                ON CONFLICT(root_key,path) DO UPDATE SET generation=generation+1,last_at=$now,due_at=MIN(first_at+5000,$now+750)
                WHERE generation<9223372036854775807;
                """);
            q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$path",path);q.Parameters.AddWithValue("$now",now);
            if(await q.ExecuteNonQueryAsync(ct)!=1)gap=true;
        }
        if(gap)
        {
            await using var q=SyncCommand(c,t,"INSERT INTO client_event_gaps VALUES($root,1) ON CONFLICT(root_key) DO UPDATE SET generation=generation+1");
            q.Parameters.AddWithValue("$root",root);await q.ExecuteNonQueryAsync(ct);
        }
        await using var next=SyncCommand(c,t,"SELECT MIN(due_at) FROM client_event_inbox WHERE root_key=$root");next.Parameters.AddWithValue("$root",root);
        var due=await next.ExecuteScalarAsync(ct);await t.CommitAsync(ct);
        return gap?now:due is long value?value:null;
    }
    internal async Task<LocalEventPage> ReadEventHintsAsync(string root,string owner,CancellationToken ct,bool dueOnly=false)
    {
        await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:true);_=await ReadLocalStateAsync(c,t,root,owner,ct);
        var hints=new List<LocalEventHint>(128);
        await using(var q=SyncCommand(c,t,"SELECT path,generation FROM client_event_inbox WHERE root_key=$root AND ($due=0 OR due_at<=$now) ORDER BY due_at,path LIMIT 128"))
        {q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$due",dueOnly?1:0);q.Parameters.AddWithValue("$now",Clock.GetUtcNow().ToUnixTimeMilliseconds());await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))hints.Add(new(r.GetString(0),r.GetInt64(1)));}
        await using var gap=SyncCommand(c,t,"SELECT generation FROM client_event_gaps WHERE root_key=$root");gap.Parameters.AddWithValue("$root",root);
        var generation=await gap.ExecuteScalarAsync(ct);gap.CommandText="SELECT MIN(due_at) FROM client_event_inbox WHERE root_key=$root";var due=await gap.ExecuteScalarAsync(ct);return new(hints,generation is long,generation is long value?value:0,due is long next?next:null);
    }
    internal async Task AckEventGapAsync(string root,string owner,long generation,CancellationToken ct)
    {
        await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);_=await ReadLocalStateAsync(c,t,root,owner,ct);
        await using var q=SyncCommand(c,t,"DELETE FROM client_event_gaps WHERE root_key=$root AND generation=$generation");q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$generation",generation);await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);
    }
    internal async Task AckEventHintsAsync(string root,string owner,IReadOnlyList<LocalEventHint> hints,CancellationToken ct)
    {
        await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);_=await ReadLocalStateAsync(c,t,root,owner,ct);
        foreach(var hint in hints)
        {
            await using var q=SyncCommand(c,t,"DELETE FROM client_event_inbox WHERE root_key=$root AND path=$path AND generation=$generation");
            q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$path",hint.Path);q.Parameters.AddWithValue("$generation",hint.Generation);await q.ExecuteNonQueryAsync(ct);
        }
        await t.CommitAsync(ct);
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal TimeProvider Clock => _store.Clock;
    internal Task InitializeEventInboxAsync(CancellationToken ct)=>SerialAsync(async()=>{await _store.EnsureEventInboxAsync(ct);return true;},ct);
    internal Task<long?> SaveEventHintsAsync(IReadOnlyList<string> paths,bool gap,CancellationToken ct)=>SerialAsync(()=>_store.SaveEventHintsAsync(_root,_owner,paths,gap,_admission,ct),ct);
    internal Task<LocalEventPage> ReadEventHintsAsync(CancellationToken ct,bool dueOnly=false)=>SerialAsync(()=>_store.ReadEventHintsAsync(_root,_owner,ct,dueOnly),ct);
    internal Task AckEventGapAsync(long generation,CancellationToken ct)=>SerialAsync(async()=>{await _store.AckEventGapAsync(_root,_owner,generation,ct);return true;},ct);
    internal Task AckEventHintsAsync(IReadOnlyList<LocalEventHint> hints,CancellationToken ct)=>SerialAsync(async()=>{await _store.AckEventHintsAsync(_root,_owner,hints,ct);return true;},ct);
}
