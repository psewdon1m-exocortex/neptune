using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private static readonly Dictionary<string,string> CycleSchemaV1=new(StringComparer.Ordinal)
    {
        ["local_cycle_format"]="CREATE TABLE local_cycle_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL CHECK(version=1),bytes INTEGER NOT NULL DEFAULT 0)",
        ["local_cycle_scopes"]="CREATE TABLE local_cycle_scopes(scope_id TEXT PRIMARY KEY,root_key TEXT NOT NULL REFERENCES local_source_roots(root_key),coherence_id TEXT REFERENCES local_coherence_roots(coherence_id),configuration TEXT NOT NULL,fingerprint TEXT NOT NULL,owner_epoch INTEGER NOT NULL,legacy_revision INTEGER NOT NULL,gap_epoch INTEGER NOT NULL,status INTEGER NOT NULL DEFAULT 0 CHECK(status BETWEEN 0 AND 3),max_profile INTEGER NOT NULL,free_floor INTEGER NOT NULL,anchor_json TEXT,snapshot_id TEXT,ready_json TEXT,entries_cursor TEXT,after_node TEXT,changes_cursor TEXT,changes_from INTEGER NOT NULL DEFAULT 0,remote_seq INTEGER NOT NULL DEFAULT 0,remote_complete INTEGER NOT NULL DEFAULT 0 CHECK(remote_complete IN(0,1)),snapshot_complete INTEGER NOT NULL DEFAULT 0 CHECK(snapshot_complete IN(0,1)),watcher_cursor INTEGER NOT NULL DEFAULT 0,local_after_usn INTEGER NOT NULL DEFAULT -1,local_after_node TEXT NOT NULL DEFAULT '',seed_after INTEGER NOT NULL DEFAULT 0,seed_complete INTEGER NOT NULL DEFAULT 0 CHECK(seed_complete IN(0,1)),files INTEGER NOT NULL DEFAULT 0,bytes INTEGER NOT NULL DEFAULT 1024,error TEXT,retire_phase INTEGER NOT NULL DEFAULT 0)",
        ["local_cycle_remote"]="CREATE TABLE local_cycle_remote(scope_id TEXT NOT NULL REFERENCES local_cycle_scopes(scope_id),node_id TEXT NOT NULL,parent_id TEXT,name TEXT NOT NULL,name_key TEXT NOT NULL,kind INTEGER NOT NULL CHECK(kind IN(0,1)),description TEXT NOT NULL,seq INTEGER NOT NULL,deleted INTEGER NOT NULL CHECK(deleted IN(0,1)),bytes INTEGER NOT NULL,PRIMARY KEY(scope_id,node_id)) WITHOUT ROWID",
        ["local_cycle_files"]="CREATE TABLE local_cycle_files(scope_id TEXT NOT NULL REFERENCES local_cycle_scopes(scope_id),node_id TEXT NOT NULL,parent_id TEXT NOT NULL,path TEXT NOT NULL,name TEXT NOT NULL,created INTEGER NOT NULL,kind INTEGER NOT NULL CHECK(kind IN(0,1)),size INTEGER NOT NULL,watcher_generation INTEGER NOT NULL DEFAULT 0,ack_journal INTEGER NOT NULL DEFAULT -1,ack_watcher INTEGER NOT NULL DEFAULT -1,baseline TEXT,baseline_work INTEGER NOT NULL DEFAULT 0,remote_node TEXT,phase INTEGER NOT NULL DEFAULT 0 CHECK(phase BETWEEN 0 AND 4),bytes INTEGER NOT NULL,PRIMARY KEY(scope_id,node_id),UNIQUE(scope_id,path),UNIQUE(scope_id,remote_node)) WITHOUT ROWID",
        ["local_cycle_work"]="CREATE TABLE local_cycle_work(work_no INTEGER PRIMARY KEY AUTOINCREMENT,operation_id TEXT NOT NULL UNIQUE,scope_id TEXT NOT NULL,node_id TEXT NOT NULL,journal_generation INTEGER NOT NULL,watcher_generation INTEGER NOT NULL,entry_json TEXT NOT NULL,phase INTEGER NOT NULL CHECK(phase BETWEEN 0 AND 10),spool_path TEXT,size INTEGER,sha256 TEXT,capture_proof TEXT,input_json TEXT,intent_hash TEXT,upload_id TEXT,received INTEGER NOT NULL DEFAULT 0,result_json TEXT,error TEXT,active INTEGER CHECK(active=1 OR active IS NULL),dispatch_id TEXT,bytes INTEGER NOT NULL DEFAULT 512,FOREIGN KEY(scope_id,node_id) REFERENCES local_cycle_files(scope_id,node_id),UNIQUE(scope_id,node_id,active))",
        ["local_cycle_remote_name"]="CREATE INDEX local_cycle_remote_name ON local_cycle_remote(scope_id,parent_id,name_key,deleted,node_id)",
        ["local_cycle_files_page"]="CREATE INDEX local_cycle_files_page ON local_cycle_files(scope_id,phase,node_id)",
        ["local_cycle_work_page"]="CREATE INDEX local_cycle_work_page ON local_cycle_work(scope_id,active,phase,work_no)",
        ["local_cycle_work_node"]="CREATE INDEX local_cycle_work_node ON local_cycle_work(scope_id,node_id,phase,work_no)",
        ["local_cycle_coherence_pin"]="CREATE INDEX local_cycle_coherence_pin ON local_cycle_scopes(coherence_id,scope_id)"
    };
    private static readonly Dictionary<string,string> CycleSchemaV2=NewCycleSchema();
    private static readonly Dictionary<string,string> CycleSchemaV3=NewRefreshCycleSchema();
    private static readonly Dictionary<string,string> CycleSchema=NewParentCycleSchema();
    private static Dictionary<string,string> NewParentCycleSchema()
    {
        var schema=new Dictionary<string,string>(CycleSchemaV3,StringComparer.Ordinal);
        schema["local_cycle_format"]=schema["local_cycle_format"].Replace("CHECK(version=3)","CHECK(version=4)",StringComparison.Ordinal);
        return schema;
    }
    private static Dictionary<string,string> NewRefreshCycleSchema()
    {
        var schema=new Dictionary<string,string>(CycleSchemaV2,StringComparer.Ordinal);
        schema["local_cycle_format"]=schema["local_cycle_format"].Replace("CHECK(version=2)","CHECK(version=3)",StringComparison.Ordinal);
        schema["local_cycle_scopes"]=schema["local_cycle_scopes"][..^1]+",active_remote TEXT,pending_remote TEXT)";
        foreach(var pair in CycleRefreshSchema) schema.Add(pair.Key,pair.Value);
        return schema;
    }
    private static Dictionary<string,string> NewCycleSchema()
    {
        var schema=new Dictionary<string,string>(CycleSchemaV1,StringComparer.Ordinal);
        schema["local_cycle_format"]=CycleSchemaV1["local_cycle_format"].Replace("CHECK(version=1)","CHECK(version=2)",StringComparison.Ordinal);
        schema["local_cycle_scopes"]=CycleSchemaV1["local_cycle_scopes"][..^1]+",work_retire_after INTEGER NOT NULL DEFAULT 0 CHECK(work_retire_after>=0),confirmed_files INTEGER NOT NULL DEFAULT 0 CHECK(confirmed_files>=0),confirmed_bytes INTEGER NOT NULL DEFAULT 0 CHECK(confirmed_bytes>=0))";
        schema["local_cycle_active"]="CREATE TABLE local_cycle_active(root_key TEXT PRIMARY KEY REFERENCES local_source_roots(root_key),scope_id TEXT NOT NULL UNIQUE REFERENCES local_cycle_scopes(scope_id))";
        schema["local_cycle_scopes_root"]="CREATE INDEX local_cycle_scopes_root ON local_cycle_scopes(root_key,status,scope_id)";
        return schema;
    }
    private async Task ValidateOptionalLocalCycleBeforeInitializeAsync(CancellationToken ct)
    {await PreflightCycleAsync(ct);await PreflightClientEvolution(ct);await PreflightEventInbox(ct);}
    private async Task<int> PreflightCycleAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();if(!File.Exists(databasePath))return 0;
        try
        {
            await using var c=new SqliteConnection(ReadOnlySyncConnectionString(databasePath));await c.OpenAsync(ct);using var t=c.BeginTransaction(deferred:true);
            await using var q=SyncCommand(c,t,"SELECT name,sql FROM sqlite_master WHERE (name GLOB 'local_cycle_*' OR tbl_name GLOB 'local_cycle_*' AND type IN('trigger','index')) AND name NOT LIKE 'sqlite_autoindex_%'");
            var actual=new Dictionary<string,string>(StringComparer.Ordinal);await using(var r=await q.ExecuteReaderAsync(ct))while(await r.ReadAsync(ct)){if(actual.Count>=CycleSchemaV7.Count+1)throw new InvalidDataException("Cycle schema has unknown objects.");actual.Add(r.GetString(0),r.IsDBNull(1)?"":r.GetString(1));}
            if(actual.Count==0)return 0;if(!actual.ContainsKey("local_cycle_format"))throw new InvalidDataException("Cycle module is incomplete.");
            q.CommandText="SELECT version FROM local_cycle_format WHERE singleton=1";if(await q.ExecuteScalarAsync(ct)is not long version||version is not(1 or 2 or 3 or 4 or 5 or 6 or 7))throw new InvalidDataException("Unsupported cycle format; no writable open attempted.");
            static string Normal(string sql)=>string.Concat(sql.Where(c=>!char.IsWhiteSpace(c))).TrimEnd(';');
            var expected=version==1?CycleSchemaV1:version==2?CycleSchemaV2:version==3?CycleSchemaV3:version==4?CycleSchema:version==5?CycleSchemaV5:version==6?CycleSchemaV6:CycleSchemaV7;
            if(actual.Remove(ClientWriterFenceName,out var fence)&&Normal(fence)!=Normal(ClientWriterFence))throw new InvalidDataException("Client writer fence changed.");
            if(actual.Count!=expected.Count||expected.Any(p=>!actual.TryGetValue(p.Key,out var sql)||Normal(sql)!=Normal(p.Value)))throw new InvalidDataException("Cycle schema is incompatible.");return (int)version;
        }
        catch(SqliteException e){throw new InvalidDataException("Cycle schema cannot be inspected safely.",e);}
    }
    private sealed record CycleBackup(string SourcePath,string BackupPath,string Sha256,int TargetFormat);
    private async Task EnsureCycleAsync(LocalJournalAdmission admission,CancellationToken ct)
    {
        if(await PreflightCycleAsync(ct)is 4 or 5 or 6 or 7)return;
        await using var migration=await AcquireSyncMigrationLockAsync(ct);var previous=await PreflightCycleAsync(ct);if(previous is 4 or 5 or 6 or 7)return;
        var totals=previous==1?await BackfillCycleConfirmedTotals(ct):null;
        var target=previous==0?1:previous==1?2:previous==2?3:4;
        var copy=checked(new FileInfo(databasePath).Length+(File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0));CheckLocalDiskAdmission(admission,checked(copy+NamespaceReserve(64)));
        var temp=databasePath+".local-cycle-"+Guid.NewGuid().ToString("N")+".partial";
        try
        {
            await CreateCoherenceDatabaseCopy(databasePath,temp,ct);var hash=await HashSyncBackupAsync(temp,ct);var backup=Path.GetFullPath(databasePath)+".pre-local-cycle-v"+target+"-"+hash+".db";
            if(File.Exists(backup)){if(await HashSyncBackupAsync(backup,ct)!=hash)throw new InvalidDataException("Cycle backup changed.");File.Delete(temp);}else File.Move(temp,backup);
            var manifest=JsonSerializer.Serialize(new CycleBackup(Path.GetFullPath(databasePath),backup,hash,target),LocalCycleWire.Json);var manifestTemp=databasePath+".local-cycle-backup."+Guid.NewGuid().ToString("N")+".partial";
            try{await File.WriteAllTextAsync(manifestTemp,manifest,ct);using(var f=new FileStream(manifestTemp,FileMode.Open,FileAccess.Write,FileShare.None))f.Flush(true);File.Move(manifestTemp,databasePath+".local-cycle-v"+target+"-backup.json",true);}finally{if(File.Exists(manifestTemp))File.Delete(manifestTemp);}
            await CheckLocalAdmissionAsync(admission,0,NamespaceReserve(64),ct);await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);
            if(previous==0)
            {foreach(var sql in CycleSchema.Values)await ExecuteSyncAsync(c,t,sql,ct);await ExecuteSyncAsync(c,t,"INSERT INTO local_cycle_format(singleton,version) VALUES(1,4)",ct);}
            else
            {
                await ExecuteSyncAsync(c,t,"ALTER TABLE local_cycle_format RENAME TO local_cycle_format_previous",ct);
                await ExecuteSyncAsync(c,t,CycleSchema["local_cycle_format"],ct);
                await ExecuteSyncAsync(c,t,"INSERT INTO local_cycle_format SELECT singleton,4,bytes FROM local_cycle_format_previous;DROP TABLE local_cycle_format_previous",ct);
                if(previous==1)
                {
                await ExecuteSyncAsync(c,t,"ALTER TABLE local_cycle_scopes ADD COLUMN work_retire_after INTEGER NOT NULL DEFAULT 0 CHECK(work_retire_after>=0)",ct);
                await ExecuteSyncAsync(c,t,"ALTER TABLE local_cycle_scopes ADD COLUMN confirmed_files INTEGER NOT NULL DEFAULT 0 CHECK(confirmed_files>=0);ALTER TABLE local_cycle_scopes ADD COLUMN confirmed_bytes INTEGER NOT NULL DEFAULT 0 CHECK(confirmed_bytes>=0)",ct);
                await ExecuteSyncAsync(c,t,CycleSchema["local_cycle_active"],ct);await ExecuteSyncAsync(c,t,CycleSchema["local_cycle_scopes_root"],ct);
                // V1 is capped at sixteen scopes. The latest original scope is
                // only a candidate pointer; resume verifies its full identity,
                // active coherence and all local continuity guards atomically.
                await ExecuteSyncAsync(c,t,"INSERT INTO local_cycle_active SELECT s.root_key,s.scope_id FROM local_cycle_scopes s WHERE s.rowid=(SELECT MAX(p.rowid) FROM local_cycle_scopes p WHERE p.root_key=s.root_key)",ct);
                foreach(var pair in totals!)await WriteCycleConfirmedTotals(c,t,pair.Key,pair.Value,ct);
                }
                if(previous<3)
                {await ExecuteSyncAsync(c,t,"ALTER TABLE local_cycle_scopes ADD COLUMN active_remote TEXT;ALTER TABLE local_cycle_scopes ADD COLUMN pending_remote TEXT",ct);
                foreach(var sql in CycleRefreshSchema.Values) await ExecuteSyncAsync(c,t,sql,ct);}
            }
            await t.CommitAsync(ct);
        }
        finally{if(File.Exists(temp))File.Delete(temp);}
    }
    public Task<string> CreateLocalCycleRollbackCopyAsync(string destination,CancellationToken cancellationToken=default)
        =>CreateCycleRollbackCopy(destination,1,cancellationToken);
    public Task<string> CreateLocalCycleV1RollbackCopyAsync(string destination,CancellationToken cancellationToken=default)
        =>CreateCycleRollbackCopy(destination,2,cancellationToken);
    public Task<string> CreateLocalCycleV2RollbackCopyAsync(string destination,CancellationToken cancellationToken=default)
        =>CreateCycleRollbackCopy(destination,3,cancellationToken);
    public Task<string> CreateLocalCycleV3RollbackCopyAsync(string destination,CancellationToken cancellationToken=default)
        =>CreateCycleRollbackCopy(destination,4,cancellationToken);
    private async Task<string> CreateCycleRollbackCopy(string destination,int target,CancellationToken cancellationToken)
    {
        var ct=cancellationToken;destination=Path.GetFullPath(destination);
        if(string.Equals(destination,Path.GetFullPath(databasePath),OperatingSystem.IsWindows()?StringComparison.OrdinalIgnoreCase:StringComparison.Ordinal)||File.Exists(destination)||File.Exists(destination+"-wal")||File.Exists(destination+"-shm"))throw new InvalidOperationException("Cycle rollback requires a separate absent destination.");
        var manifestPath=databasePath+".local-cycle-v"+target+"-backup.json";if(new FileInfo(manifestPath).Length>16*1024)throw new InvalidDataException("Cycle backup manifest exceeds fixed limit.");
        using var doc=Sync.Protocol.SyncProtocolReader.Parse(await File.ReadAllBytesAsync(manifestPath,ct));string[] fields=["sourcePath","backupPath","sha256","targetFormat"];
        if(doc.RootElement.ValueKind!=JsonValueKind.Object||doc.RootElement.EnumerateObject().Count()!=fields.Length||doc.RootElement.EnumerateObject().Any(p=>!fields.Contains(p.Name,StringComparer.Ordinal)))throw new InvalidDataException("Cycle manifest has unknown/missing fields.");
        var m=JsonSerializer.Deserialize<CycleBackup>(doc.RootElement.GetRawText(),LocalCycleWire.Json)??throw new InvalidDataException("Cycle backup manifest missing.");
        if(m.TargetFormat!=target||m.SourcePath!=Path.GetFullPath(databasePath)||!LocalCycleWire.Hash(m.Sha256)||m.BackupPath!=Path.GetFullPath(databasePath)+".pre-local-cycle-v"+target+"-"+m.Sha256+".db"||await HashSyncBackupAsync(m.BackupPath,ct)!=m.Sha256)throw new InvalidDataException("Cycle backup binding/hash changed.");
        Directory.CreateDirectory(Path.GetDirectoryName(destination)!);await CreateCoherenceDatabaseCopy(m.BackupPath,destination,ct);return destination;
    }
}
