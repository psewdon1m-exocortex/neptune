using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private static readonly Dictionary<string,string> ProjectionSchema=new(StringComparer.Ordinal)
    {
        ["local_projection_format"]="CREATE TABLE local_projection_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL,bytes INTEGER NOT NULL DEFAULT 0 CHECK(bytes>=0))",
        ["local_projection_roots"]="CREATE TABLE local_projection_roots(projection_id TEXT PRIMARY KEY,root_key TEXT NOT NULL,base_candidate TEXT NOT NULL REFERENCES local_namespace_candidates(candidate_id),owner_epoch INTEGER NOT NULL CHECK(owner_epoch>0),journal_id TEXT NOT NULL,start_usn INTEGER NOT NULL CHECK(start_usn>=0),applied_usn INTEGER NOT NULL CHECK(applied_usn>=start_usn),legacy_revision INTEGER NOT NULL CHECK(legacy_revision>=0),gap_epoch INTEGER NOT NULL CHECK(gap_epoch>=0),status INTEGER NOT NULL CHECK(status BETWEEN 0 AND 2),gap_code TEXT,dirty_count INTEGER NOT NULL DEFAULT 0 CHECK(dirty_count>=0),unresolved_count INTEGER NOT NULL DEFAULT 0 CHECK(unresolved_count>=0),bytes INTEGER NOT NULL CHECK(bytes>=0),max_bytes INTEGER NOT NULL,profile_cap INTEGER NOT NULL,free_floor INTEGER NOT NULL,last_start INTEGER,last_end INTEGER,last_hash TEXT,last_records INTEGER,last_known INTEGER,last_unresolved INTEGER,retire_phase INTEGER NOT NULL DEFAULT 0 CHECK(retire_phase BETWEEN 0 AND 5))",
        ["local_projection_active"]="CREATE TABLE local_projection_active(root_key TEXT PRIMARY KEY,projection_id TEXT NOT NULL UNIQUE REFERENCES local_projection_roots(projection_id))",
        ["local_projection_retired"]="CREATE TABLE local_projection_retired(root_key TEXT PRIMARY KEY,journal_id TEXT NOT NULL,boundary INTEGER NOT NULL CHECK(boundary>=0))",
        ["local_projection_nodes"]="CREATE TABLE local_projection_nodes(projection_id TEXT NOT NULL REFERENCES local_projection_roots(projection_id),node_id TEXT NOT NULL,created INTEGER NOT NULL,kind INTEGER NOT NULL CHECK(kind IN(0,1)),generation INTEGER NOT NULL CHECK(generation>0),last_usn INTEGER NOT NULL CHECK(last_usn>=0),reasons INTEGER NOT NULL,subtree_epoch INTEGER NOT NULL DEFAULT 0 CHECK(subtree_epoch>=0),bytes INTEGER NOT NULL CHECK(bytes>0),PRIMARY KEY(projection_id,node_id)) WITHOUT ROWID",
        ["local_projection_edges"]="CREATE TABLE local_projection_edges(projection_id TEXT NOT NULL REFERENCES local_projection_roots(projection_id),parent_id TEXT NOT NULL,canonical TEXT NOT NULL,name TEXT NOT NULL,node_id TEXT NOT NULL,created INTEGER NOT NULL,kind INTEGER NOT NULL CHECK(kind IN(0,1)),action INTEGER NOT NULL CHECK(action IN(0,1)),source_usn INTEGER NOT NULL CHECK(source_usn>=0),bytes INTEGER NOT NULL CHECK(bytes>0),PRIMARY KEY(projection_id,parent_id,canonical)) WITHOUT ROWID",
        ["local_projection_aliases"]="CREATE TABLE local_projection_aliases(projection_id TEXT NOT NULL REFERENCES local_projection_roots(projection_id),major INTEGER NOT NULL CHECK(major IN(2,3)),wire_id TEXT NOT NULL,node_id TEXT NOT NULL,created INTEGER NOT NULL,serial64 TEXT NOT NULL,proof_usn INTEGER NOT NULL CHECK(proof_usn>=0),bytes INTEGER NOT NULL CHECK(bytes>0),PRIMARY KEY(projection_id,major,wire_id)) WITHOUT ROWID",
        ["local_projection_unresolved"]="CREATE TABLE local_projection_unresolved(projection_id TEXT NOT NULL REFERENCES local_projection_roots(projection_id),usn INTEGER NOT NULL CHECK(usn>=0),payload BLOB NOT NULL CHECK(length(payload)<=592),sha256 TEXT NOT NULL,code TEXT NOT NULL,state INTEGER NOT NULL CHECK(state IN(0,1,2)),bytes INTEGER NOT NULL CHECK(bytes>0),PRIMARY KEY(projection_id,usn)) WITHOUT ROWID",
        ["local_projection_rename"]="CREATE TABLE local_projection_rename(projection_id TEXT NOT NULL REFERENCES local_projection_roots(projection_id),major INTEGER NOT NULL CHECK(major IN(2,3)),wire_id TEXT NOT NULL,usn INTEGER NOT NULL CHECK(usn>=0),parent_id TEXT NOT NULL,name TEXT NOT NULL,sha256 TEXT NOT NULL,bytes INTEGER NOT NULL CHECK(bytes>0),PRIMARY KEY(projection_id,major,wire_id,usn)) WITHOUT ROWID",
        ["local_projection_dirty_page"]="CREATE INDEX local_projection_dirty_page ON local_projection_nodes(projection_id,last_usn,node_id)",
        ["local_projection_node_edges"]="CREATE INDEX local_projection_node_edges ON local_projection_edges(projection_id,node_id,action,parent_id,canonical)",
        ["local_projection_unresolved_page"]="CREATE INDEX local_projection_unresolved_page ON local_projection_unresolved(projection_id,state,usn)",
        ["local_projection_pins"]="CREATE INDEX local_projection_pins ON local_projection_roots(base_candidate,projection_id)",
        ["local_projection_root_pins"]="CREATE INDEX local_projection_root_pins ON local_projection_roots(root_key,projection_id)"
    };
    private Task ValidateOptionalLocalProjectionBeforeInitializeAsync(CancellationToken ct)=>PreflightProjectionAsync(ct);
    private async Task<int> PreflightProjectionAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();if(!File.Exists(databasePath))return 0;
        try
        {
            await using var c=new SqliteConnection(ReadOnlySyncConnectionString(databasePath));await c.OpenAsync(ct);using var t=c.BeginTransaction(deferred:true);
            await using var q=SyncCommand(c,t,"SELECT name,sql FROM sqlite_master WHERE (name GLOB 'local_projection_*' OR tbl_name GLOB 'local_projection_*' AND type IN('trigger','index')) AND name NOT LIKE 'sqlite_autoindex_%'");
            var actual=new Dictionary<string,string>(StringComparer.Ordinal);await using(var r=await q.ExecuteReaderAsync(ct))while(await r.ReadAsync(ct)){if(actual.Count>=ProjectionSchema.Count)throw new InvalidDataException("Projection schema has unexpected extra objects.");actual.Add(r.GetString(0),r.IsDBNull(1)?"":r.GetString(1));}
            if(actual.Count==0)return 0;if(!actual.ContainsKey("local_projection_format"))throw new InvalidDataException("Projection module is incomplete.");
            q.CommandText="SELECT version FROM local_projection_format WHERE singleton=1";
            if(await q.ExecuteScalarAsync(ct)is not long version||version!=1)throw new InvalidDataException("Unsupported optional projection format; no writes attempted.");
            static string Normal(string s)=>string.Concat(s.Where(x=>!char.IsWhiteSpace(x))).TrimEnd(';');
            if(actual.Count!=ProjectionSchema.Count||ProjectionSchema.Any(x=>!actual.TryGetValue(x.Key,out var sql)||Normal(sql)!=Normal(x.Value)))throw new InvalidDataException("Projection schema is incompatible.");
            return 1;
        }
        catch(SqliteException error){throw new InvalidDataException("Projection module cannot be safely inspected.",error);}
    }
    private async Task EnsureProjectionAsync(LocalJournalAdmission admission,CancellationToken ct)
    {
        if(await PreflightProjectionAsync(ct)==1)return;
        await using var migration=await AcquireSyncMigrationLockAsync(ct);if(await PreflightProjectionAsync(ct)==1)return;
        var copyBytes=checked(new FileInfo(databasePath).Length+(File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0));
        CheckLocalDiskAdmission(admission,checked(copyBytes+NamespaceReserve(32)));
        var temporary=databasePath+".local-projection-"+Guid.NewGuid().ToString("N")+".partial";
        try
        {
            await CreateConsistentSyncDatabaseCopyAsync(databasePath,temporary,ct);var hash=await HashSyncBackupAsync(temporary,ct);
            var backup=Path.GetFullPath(databasePath)+".pre-local-projection-v1-"+hash+".db";
            if(File.Exists(backup)){if(await HashSyncBackupAsync(backup,ct)!=hash)throw new InvalidDataException("Projection backup changed.");File.Delete(temporary);}else File.Move(temporary,backup);
            var manifest=JsonSerializer.Serialize(new ProjectionBackup(Path.GetFullPath(databasePath),backup,hash,1),SyncStateJson);
            var tempManifest=databasePath+".local-projection-v1-backup.json."+Guid.NewGuid().ToString("N")+".partial";
            try{await File.WriteAllTextAsync(tempManifest,manifest,ct);using(var f=new FileStream(tempManifest,FileMode.Open,FileAccess.Write,FileShare.None))f.Flush(true);File.Move(tempManifest,databasePath+".local-projection-v1-backup.json",true);}
            finally{if(File.Exists(tempManifest))File.Delete(tempManifest);}
            await CheckLocalAdmissionAsync(admission,0,NamespaceReserve(32),ct);await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);
            foreach(var sql in ProjectionSchema.Values)await ExecuteSyncAsync(c,t,sql,ct);await ExecuteSyncAsync(c,t,"INSERT INTO local_projection_format(singleton,version) VALUES(1,1)",ct);await t.CommitAsync(ct);
        }
        finally{if(File.Exists(temporary))File.Delete(temporary);}
    }
    private sealed record ProjectionBackup(string SourcePath,string BackupPath,string Sha256,int TargetFormat);
    public async Task<string> CreateLocalProjectionRollbackCopyAsync(string destination,CancellationToken cancellationToken=default)
    {
        destination=Path.GetFullPath(destination);if(string.Equals(destination,Path.GetFullPath(databasePath),OperatingSystem.IsWindows()?StringComparison.OrdinalIgnoreCase:StringComparison.Ordinal)||File.Exists(destination)||File.Exists(destination+"-wal")||File.Exists(destination+"-shm"))throw new InvalidOperationException("Projection rollback requires a separate absent destination.");
        var path=databasePath+".local-projection-v1-backup.json";if(new FileInfo(path).Length>16*1024)throw new InvalidDataException("Unbounded projection rollback manifest.");
        using var json=JsonDocument.Parse(await File.ReadAllBytesAsync(path,cancellationToken),new(){MaxDepth=8});var root=json.RootElement;
        if(root.ValueKind!=JsonValueKind.Object)throw new InvalidDataException("Projection rollback manifest is not an object.");
        var expected=new HashSet<string>(["sourcePath","backupPath","sha256","targetFormat"],StringComparer.Ordinal);
        foreach(var property in root.EnumerateObject())if(!expected.Remove(property.Name))throw new InvalidDataException("Projection rollback fields are duplicate/unexpected.");
        if(expected.Count!=0||root.GetProperty("targetFormat").GetRawText()!="1")throw new InvalidDataException("Projection rollback format is invalid.");
        static string Text(JsonElement r,string n)=>r.GetProperty(n).ValueKind==JsonValueKind.String?r.GetProperty(n).GetString()!:throw new InvalidDataException("Projection manifest field is not a string.");
        var hash=Text(root,"sha256");var backup=Text(root,"backupPath");
        if(Text(root,"sourcePath")!=Path.GetFullPath(databasePath)||hash.Length!=64||hash!=hash.ToLowerInvariant()||hash.Any(x=>!char.IsAsciiHexDigit(x))||backup!=Path.GetFullPath(databasePath)+".pre-local-projection-v1-"+hash+".db"||await HashSyncBackupAsync(backup,cancellationToken)!=hash)throw new InvalidDataException("Projection rollback identity changed.");
        Directory.CreateDirectory(Path.GetDirectoryName(destination)!);await CreateConsistentSyncDatabaseCopyAsync(backup,destination,cancellationToken);return destination;
    }
    private async Task RefusePinnedLocalProjectionAsync(string id,bool candidate,CancellationToken ct)
    {
        if(await PreflightProjectionAsync(ct)==0)return;await using var c=new SqliteConnection(ReadOnlySyncConnectionString(databasePath));await c.OpenAsync(ct);
        await using var q=c.CreateCommand();q.CommandText=candidate?"SELECT 1 FROM local_projection_roots WHERE base_candidate=$id LIMIT 1":"SELECT 1 FROM local_projection_roots WHERE root_key=$id LIMIT 1";q.Parameters.AddWithValue("$id",id);
        if(await q.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Projection pins this history; retire its bounded dependent ledger before deleting base evidence.");
    }
}
