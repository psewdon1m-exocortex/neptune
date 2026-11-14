using Microsoft.Data.Sqlite;

namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    private async Task ClientAdmission(string root,string owner,string scope,int rows,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);
        var original=await RequireCycle(c,t,root,owner,scope,true,ct);await t.CommitAsync(ct);
        await CheckLocalAdmissionAsync(new(MaximumDatabaseBytes:original.Budget.ProfileBytes,MinimumFreeDiskBytes:original.Budget.MinimumFreeDiskBytes),0,NamespaceReserve(rows),ct);
    }
    private static async Task<bool> HasClientOriginals(Microsoft.Data.Sqlite.SqliteConnection c,Microsoft.Data.Sqlite.SqliteTransaction t,string scope,bool unresolvedOnly,CancellationToken ct)
    {
        await using var exists=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='client_evolution_format'");if(await exists.ExecuteScalarAsync(ct)is null)return false;
        if(!unresolvedOnly)
        {await using var pin=SyncCommand(c,t,"SELECT 1 FROM client_binding_migrations WHERE old_scope=$scope OR new_scope=$scope LIMIT 1");CycleId(pin,scope);if(await pin.ExecuteScalarAsync(ct)is not null)return true;}
        await using var q=SyncCommand(c,t,"SELECT 1 FROM client_namespace_operations WHERE scope_id=$scope"+(unresolvedOnly?" AND state NOT IN('PUBLISHED','NOT_PUBLISHED','CANCELLED')":"")+" UNION ALL SELECT 1 FROM client_stream_operations WHERE scope_id=$scope"+(unresolvedOnly?" AND COALESCE(json_extract(status_json,'$.state'),'UNKNOWN')<>'PUBLISHED' AND COALESCE(json_extract(status_json,'$.publication.session.phase'),'')<>'not_published'":"")+" LIMIT 1");CycleId(q,scope);return await q.ExecuteScalarAsync(ct)is not null;
    }
    // An old binary rejects this additional local_cycle object during its
    // existing exact-schema preflight. It cannot reinterpret a chunk spool or
    // ignore a namespace original after a binary-only rollback.
    private const string ClientWriterFenceName="local_cycle_client_writer_v1";
    private const string ClientWriterFence="CREATE TRIGGER local_cycle_client_writer_v1 BEFORE DELETE ON local_cycle_work WHEN EXISTS(SELECT 1 FROM client_stream_operations s WHERE s.operation_id=OLD.operation_id AND COALESCE(json_extract(s.status_json,'$.state'),'UNKNOWN')<>'PUBLISHED' AND COALESCE(json_extract(s.status_json,'$.publication.session.phase'),'')<>'not_published') OR EXISTS(SELECT 1 FROM client_namespace_operations n WHERE n.scope_id=OLD.scope_id AND n.node_id=OLD.node_id AND n.state NOT IN('PUBLISHED','NOT_PUBLISHED','CANCELLED')) BEGIN SELECT RAISE(ABORT,'unresolved client original pins work');END";
    private static readonly Dictionary<string,string> ClientEvolutionSchema=new(StringComparer.Ordinal)
    {
        ["client_evolution_format"]="CREATE TABLE client_evolution_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL CHECK(version=1),minimum_writer TEXT NOT NULL)",
        ["client_mapping_policies"]="CREATE TABLE client_mapping_policies(scope_id TEXT PRIMARY KEY,policy_json TEXT NOT NULL,policy_hash TEXT NOT NULL)",
        ["client_namespace_bindings"]="CREATE TABLE client_namespace_bindings(scope_id TEXT PRIMARY KEY,codec TEXT NOT NULL,policy_hash TEXT NOT NULL)",
        ["client_policy_changes"]="CREATE TABLE client_policy_changes(scope_id TEXT PRIMARY KEY,previous_json TEXT NOT NULL,proposed_hash TEXT NOT NULL,after_node TEXT NOT NULL DEFAULT '')",
        ["client_policy_previews"]="CREATE TABLE client_policy_previews(scope_id TEXT PRIMARY KEY,policy_json TEXT NOT NULL,preview_json TEXT NOT NULL)",
        ["client_delete_plans"]="CREATE TABLE client_delete_plans(plan_id TEXT PRIMARY KEY,scope_id TEXT NOT NULL,plan_json TEXT NOT NULL,receipt_json TEXT)",
        ["client_binding_migrations"]="CREATE TABLE client_binding_migrations(plan_id TEXT PRIMARY KEY,old_scope TEXT NOT NULL,new_scope TEXT NOT NULL,plan_json TEXT NOT NULL,after_node TEXT NOT NULL DEFAULT '',remote_after TEXT NOT NULL DEFAULT '',state TEXT NOT NULL DEFAULT 'COPYING')",
        ["client_full_missing"]="CREATE TABLE client_full_missing(plan_id TEXT PRIMARY KEY,after_node TEXT NOT NULL DEFAULT '')",
        ["client_operation_schedule"]="CREATE TABLE client_operation_schedule(scope_id TEXT NOT NULL,operation_id TEXT NOT NULL,kind TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'READY',reason TEXT,attempts INTEGER NOT NULL DEFAULT 0,created_at INTEGER NOT NULL,not_before INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(scope_id,operation_id)) WITHOUT ROWID",
        ["client_operation_due"]="CREATE INDEX client_operation_due ON client_operation_schedule(scope_id,state,not_before,operation_id)",
        ["client_stream_operations"]="CREATE TABLE client_stream_operations(operation_id TEXT PRIMARY KEY,scope_id TEXT NOT NULL,input_json TEXT NOT NULL,status_json TEXT,seal_json TEXT)",
        ["client_metadata_seals"]="CREATE TABLE client_metadata_seals(operation_id TEXT PRIMARY KEY,scope_id TEXT NOT NULL,seal_json TEXT NOT NULL,modified_at TEXT NOT NULL)",
        ["client_namespace_operations"]="CREATE TABLE client_namespace_operations(operation_id TEXT PRIMARY KEY,scope_id TEXT NOT NULL,node_id TEXT NOT NULL,kind TEXT NOT NULL,request_json TEXT NOT NULL,entry_json TEXT NOT NULL,plan_id TEXT,plan_digest TEXT,approved INTEGER NOT NULL DEFAULT 0,state TEXT NOT NULL DEFAULT 'QUEUED',receipt_json TEXT,error TEXT,created_at INTEGER NOT NULL)",
        ["client_namespace_pending"]="CREATE INDEX client_namespace_pending ON client_namespace_operations(scope_id,state,kind,created_at)",
        ["client_namespace_active_node"]="CREATE UNIQUE INDEX client_namespace_active_node ON client_namespace_operations(scope_id,node_id) WHERE state NOT IN('PUBLISHED','NOT_PUBLISHED','CANCELLED')"
    };
    private async Task<bool> PreflightClientEvolution(CancellationToken ct)
    {
        if(!File.Exists(databasePath))return false;
        await using var c=new SqliteConnection(ReadOnlySyncConnectionString(databasePath));await c.OpenAsync(ct);using var t=c.BeginTransaction(deferred:true);
        await using var q=SyncCommand(c,t,"SELECT name,sql FROM sqlite_master WHERE (name GLOB 'client_*' OR tbl_name GLOB 'client_*') AND name NOT GLOB 'client_event_*' AND tbl_name NOT GLOB 'client_event_*' AND name NOT LIKE 'sqlite_autoindex_%'");
        var rows=new Dictionary<string,string>(StringComparer.Ordinal);await using(var r=await q.ExecuteReaderAsync(ct))while(await r.ReadAsync(ct)){if(rows.Count>=ClientEvolutionSchema.Count+1)throw new InvalidDataException("Unknown client evolution objects.");rows.Add(r.GetString(0),r.IsDBNull(1)?"":r.GetString(1));}
        if(rows.Count==0)
        {q.CommandText="SELECT 1 FROM sqlite_master WHERE name='local_cycle_client_writer_v1'";if(await q.ExecuteScalarAsync(ct)is not null)throw new InvalidDataException("Client writer fence has no matching schema.");return false;}
        static string Normalize(string value)=>string.Concat(value.Where(c=>!char.IsWhiteSpace(c))).TrimEnd(';');
        var hasStreamParts=rows.TryGetValue("client_stream_parts",out var parts);
        if(hasStreamParts&&Normalize(parts!)!=Normalize(StreamPartsSchema)||rows.Count!=ClientEvolutionSchema.Count+(hasStreamParts?1:0)||ClientEvolutionSchema.Any(pair=>!rows.TryGetValue(pair.Key,out var sql)||Normalize(sql)!=Normalize(pair.Value)))throw new InvalidDataException("Unsupported client evolution schema; preserved without writes.");
        q.CommandText="SELECT minimum_writer FROM client_evolution_format WHERE singleton=1 AND version=1";
        if(await q.ExecuteScalarAsync(ct)is not string writer||writer!=(hasStreamParts?"neptune.client-evolution.v2":"neptune.client-evolution.v1"))throw new InvalidDataException("Unsupported client writer.");
        q.CommandText="SELECT sql FROM sqlite_master WHERE name='local_cycle_client_writer_v1'";
        if(await q.ExecuteScalarAsync(ct)is not string fence||Normalize(fence)!=Normalize(ClientWriterFence))throw new InvalidDataException("Client writer fence missing.");
        return true;
    }
    private async Task InstallClientEvolution(CancellationToken ct)
    {
        if(await PreflightClientEvolution(ct))return;
        await using var migration=await AcquireSyncMigrationLockAsync(ct);if(await PreflightClientEvolution(ct))return;
        var copy=checked(new FileInfo(databasePath).Length+(File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0));
        CheckLocalDiskAdmission(new(),checked(copy+NamespaceReserve(32)));
        var backup=databasePath+".pre-client-evolution-"+Guid.NewGuid().ToString("N")+".db";
        await CreateCoherenceDatabaseCopy(databasePath,backup,ct);
        var hash=await HashSyncBackupAsync(backup,ct);
        await File.WriteAllTextAsync(backup+".json",System.Text.Json.JsonSerializer.Serialize(new{source=Path.GetFullPath(databasePath),backup=Path.GetFullPath(backup),sha256=hash,minimumWriter="neptune.client-evolution.v1"}),ct);
        using(var file=new FileStream(backup+".json",FileMode.Open,FileAccess.Write,FileShare.None))file.Flush(true);
        await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);
        foreach(var sql in ClientEvolutionSchema.Values)await ExecuteSyncAsync(c,t,sql,ct);
        await ExecuteSyncAsync(c,t,"INSERT INTO client_evolution_format VALUES(1,1,'neptune.client-evolution.v1')",ct);
        await ExecuteSyncAsync(c,t,ClientWriterFence,ct);await t.CommitAsync(ct);
    }
}
