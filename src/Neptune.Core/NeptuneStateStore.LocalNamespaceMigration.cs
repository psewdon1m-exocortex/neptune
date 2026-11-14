using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private static readonly Dictionary<string,string> NamespaceSchema=new(StringComparer.Ordinal)
    {
        ["local_namespace_format"]="CREATE TABLE local_namespace_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL,bytes INTEGER NOT NULL DEFAULT 0 CHECK(bytes>=0))",
        ["local_namespace_candidates"]="CREATE TABLE local_namespace_candidates(candidate_id TEXT PRIMARY KEY,root_key TEXT NOT NULL,mapping_id TEXT NOT NULL,client_id TEXT NOT NULL,root_id TEXT NOT NULL,root_created INTEGER NOT NULL,volume_serial64 TEXT NOT NULL,owner_epoch INTEGER NOT NULL CHECK(owner_epoch>0),observed_start INTEGER NOT NULL CHECK(observed_start>=0),legacy_revision INTEGER NOT NULL CHECK(legacy_revision>=0),status INTEGER NOT NULL CHECK(status BETWEEN 0 AND 3),entries INTEGER NOT NULL DEFAULT 0 CHECK(entries>=0),bytes INTEGER NOT NULL CHECK(bytes>=0),validated INTEGER NOT NULL DEFAULT 0 CHECK(validated>=0),exclusions INTEGER NOT NULL DEFAULT 0 CHECK(exclusions>=0),eligible_through INTEGER NOT NULL DEFAULT 0 CHECK(eligible_through>=0),error TEXT,max_candidate INTEGER NOT NULL,max_profile INTEGER NOT NULL,free_floor INTEGER NOT NULL,retire_phase INTEGER NOT NULL DEFAULT 0 CHECK(retire_phase BETWEEN 0 AND 4))",
        ["local_namespace_active"]="CREATE TABLE local_namespace_active(mapping_id TEXT PRIMARY KEY,candidate_id TEXT NOT NULL UNIQUE REFERENCES local_namespace_candidates(candidate_id))",
        ["local_namespace_nodes"]="CREATE TABLE local_namespace_nodes(candidate_id TEXT NOT NULL REFERENCES local_namespace_candidates(candidate_id),node_id TEXT NOT NULL,kind INTEGER NOT NULL CHECK(kind IN(0,1)),created INTEGER NOT NULL,PRIMARY KEY(candidate_id,node_id)) WITHOUT ROWID",
        ["local_namespace_entries"]="CREATE TABLE local_namespace_entries(entry_id INTEGER PRIMARY KEY,candidate_id TEXT NOT NULL,parent_id TEXT NOT NULL,node_id TEXT NOT NULL,name TEXT NOT NULL,canonical TEXT NOT NULL,kind INTEGER NOT NULL CHECK(kind IN(0,1)),created INTEGER NOT NULL,written INTEGER NOT NULL,changed INTEGER NOT NULL,size INTEGER NOT NULL CHECK(size>=0),attributes INTEGER NOT NULL,tag INTEGER NOT NULL,excluded INTEGER NOT NULL CHECK(excluded IN(0,1)),bytes INTEGER NOT NULL CHECK(bytes>0),FOREIGN KEY(candidate_id,node_id) REFERENCES local_namespace_nodes(candidate_id,node_id),FOREIGN KEY(candidate_id,parent_id) REFERENCES local_namespace_parents(candidate_id,node_id),UNIQUE(candidate_id,parent_id,canonical))",
        ["local_namespace_parents"]="CREATE TABLE local_namespace_parents(candidate_id TEXT NOT NULL,node_id TEXT NOT NULL,entry_id INTEGER NOT NULL CHECK(entry_id>=0),created INTEGER NOT NULL,state INTEGER NOT NULL CHECK(state BETWEEN 0 AND 2),claim TEXT,entries INTEGER NOT NULL DEFAULT 0 CHECK(entries>=0),bytes INTEGER NOT NULL DEFAULT 0 CHECK(bytes>=0),max_entry INTEGER NOT NULL DEFAULT 0 CHECK(max_entry>=0),child_cursor INTEGER NOT NULL DEFAULT 0 CHECK(child_cursor>=0),refilled INTEGER NOT NULL DEFAULT 0 CHECK(refilled IN(0,1)),case_sensitive INTEGER,PRIMARY KEY(candidate_id,node_id),FOREIGN KEY(candidate_id,node_id) REFERENCES local_namespace_nodes(candidate_id,node_id)) WITHOUT ROWID",
        ["local_namespace_aliases"]="CREATE TABLE local_namespace_aliases(candidate_id TEXT NOT NULL,node_id TEXT NOT NULL,major INTEGER NOT NULL CHECK(major IN(2,3)),wire_id TEXT NOT NULL,volume_serial64 TEXT NOT NULL,created INTEGER NOT NULL,PRIMARY KEY(candidate_id,major,wire_id),FOREIGN KEY(candidate_id,node_id) REFERENCES local_namespace_nodes(candidate_id,node_id)) WITHOUT ROWID",
        ["local_namespace_children"]="CREATE INDEX local_namespace_children ON local_namespace_entries(candidate_id,parent_id,kind,excluded,entry_id)",
        ["local_namespace_node_edges"]="CREATE INDEX local_namespace_node_edges ON local_namespace_entries(candidate_id,node_id,entry_id)",
        ["local_namespace_directory_edge"]="CREATE UNIQUE INDEX local_namespace_directory_edge ON local_namespace_entries(candidate_id,node_id) WHERE kind=1",
        ["local_namespace_frontier"]="CREATE INDEX local_namespace_frontier ON local_namespace_parents(candidate_id,state,refilled,entry_id)",
        ["local_namespace_entry_page"]="CREATE INDEX local_namespace_entry_page ON local_namespace_entries(candidate_id,entry_id)",
        ["local_namespace_history"]="CREATE INDEX local_namespace_history ON local_namespace_candidates(mapping_id,status,candidate_id)"
    };
    private Task ValidateOptionalLocalNamespaceBeforeInitializeAsync(CancellationToken ct)=>PreflightNamespaceAsync(ct);
    private async Task<int> PreflightNamespaceAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();if(!File.Exists(databasePath))return 0;
        try
        {
            await using var c=new SqliteConnection(ReadOnlySyncConnectionString(databasePath));await c.OpenAsync(ct);using var t=c.BeginTransaction(deferred:true);
            await using var q=SyncCommand(c,t,"SELECT name,sql FROM sqlite_master WHERE (name GLOB 'local_namespace_*' OR tbl_name GLOB 'local_namespace_*' AND type IN('trigger','index')) AND name NOT LIKE 'sqlite_autoindex_%'");
            var actual=new Dictionary<string,string>(StringComparer.Ordinal);
            await using(var r=await q.ExecuteReaderAsync(ct))while(await r.ReadAsync(ct))actual.Add(r.GetString(0),r.IsDBNull(1)?"":r.GetString(1));
            if(actual.Count==0)return 0;if(!actual.ContainsKey("local_namespace_format"))throw new InvalidDataException("Namespace module is incomplete.");
            q.CommandText="SELECT version FROM local_namespace_format WHERE singleton=1";
            if(await q.ExecuteScalarAsync(ct) is not long version || version!=1)throw new InvalidDataException("Unsupported optional namespace format; no writes attempted.");
            static string Normal(string s)=>string.Concat(s.Where(x=>!char.IsWhiteSpace(x))).TrimEnd(';');
            if(actual.Count!=NamespaceSchema.Count || NamespaceSchema.Any(x=>!actual.TryGetValue(x.Key,out var sql)||Normal(sql)!=Normal(x.Value)))throw new InvalidDataException("Namespace schema is incompatible.");
            return 1;
        }
        catch(SqliteException error){throw new InvalidDataException("Namespace module cannot be inspected safely.",error);}
    }
    private async Task EnsureNamespaceAsync(LocalJournalAdmission admission,CancellationToken ct)
    {
        if(await PreflightNamespaceAsync(ct)==1)return;
        await using var migration=await AcquireSyncMigrationLockAsync(ct);if(await PreflightNamespaceAsync(ct)==1)return;
        var reserve=NamespaceReserve(32);var copyBytes=checked(new FileInfo(databasePath).Length+(File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0));
        // Copy and DDL reservations coexist; all committed WAL and earlier
        // rollback copies count toward the same physical profile cap.
        CheckLocalDiskAdmission(admission,checked(copyBytes+reserve));
        var temporary=databasePath+".local-namespace-"+Guid.NewGuid().ToString("N")+".partial";
        try
        {
            await CreateConsistentSyncDatabaseCopyAsync(databasePath,temporary,ct);var hash=await HashSyncBackupAsync(temporary,ct);
            var backup=Path.GetFullPath(databasePath)+".pre-local-namespace-v1-"+hash+".db";
            if(File.Exists(backup)){if(await HashSyncBackupAsync(backup,ct)!=hash)throw new InvalidDataException("Namespace backup changed.");File.Delete(temporary);}else File.Move(temporary,backup);
            var manifest=JsonSerializer.Serialize(new NamespaceBackup(Path.GetFullPath(databasePath),backup,hash,1),SyncStateJson);
            var tempManifest=databasePath+".local-namespace-v1-backup.json."+Guid.NewGuid().ToString("N")+".partial";
            try{await File.WriteAllTextAsync(tempManifest,manifest,ct);using(var f=new FileStream(tempManifest,FileMode.Open,FileAccess.Write,FileShare.None))f.Flush(true);File.Move(tempManifest,databasePath+".local-namespace-v1-backup.json",true);}
            finally{if(File.Exists(tempManifest))File.Delete(tempManifest);}
            await CheckLocalAdmissionAsync(admission,0,reserve,ct);
            await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);
            foreach(var sql in NamespaceSchema.Values)await ExecuteSyncAsync(c,t,sql,ct);
            await ExecuteSyncAsync(c,t,"INSERT INTO local_namespace_format(singleton,version) VALUES(1,1)",ct);await t.CommitAsync(ct);
        }
        finally{if(File.Exists(temporary))File.Delete(temporary);}
    }
    private sealed record NamespaceBackup(string SourcePath,string BackupPath,string Sha256,int TargetFormat);
    public async Task<string> CreateLocalNamespaceRollbackCopyAsync(string destination,CancellationToken cancellationToken=default)
    {
        destination=Path.GetFullPath(destination);
        if(string.Equals(destination,Path.GetFullPath(databasePath),OperatingSystem.IsWindows()?StringComparison.OrdinalIgnoreCase:StringComparison.Ordinal)
            ||File.Exists(destination)||File.Exists(destination+"-wal")||File.Exists(destination+"-shm"))throw new InvalidOperationException("Namespace rollback requires a separate absent destination.");
        var path=databasePath+".local-namespace-v1-backup.json";if(new FileInfo(path).Length>16*1024)throw new InvalidDataException("Unbounded namespace rollback manifest.");
        var bytes=await File.ReadAllBytesAsync(path,cancellationToken);using var json=JsonDocument.Parse(bytes,new(){MaxDepth=8});
        if(json.RootElement.ValueKind!=JsonValueKind.Object)throw new InvalidDataException("Namespace rollback manifest is not an object.");
        var expected=new HashSet<string>(["sourcePath","backupPath","sha256","targetFormat"],StringComparer.Ordinal);
        foreach(var property in json.RootElement.EnumerateObject())if(!expected.Remove(property.Name))throw new InvalidDataException("Namespace rollback manifest has duplicate/unexpected fields.");
        if(expected.Count!=0 || json.RootElement.GetProperty("targetFormat").GetRawText()!="1")throw new InvalidDataException("Namespace rollback manifest format is invalid.");
        static string Text(JsonElement root,string name)=>root.GetProperty(name).ValueKind==JsonValueKind.String?root.GetProperty(name).GetString()!:throw new InvalidDataException("Namespace rollback manifest field is not a string.");
        var manifest=new NamespaceBackup(Text(json.RootElement,"sourcePath"),Text(json.RootElement,"backupPath"),Text(json.RootElement,"sha256"),1);
        if(manifest.SourcePath!=Path.GetFullPath(databasePath) || manifest.Sha256.Length!=64 || manifest.Sha256!=manifest.Sha256.ToLowerInvariant() || manifest.Sha256.Any(x=>!char.IsAsciiHexDigit(x))
            || manifest.BackupPath!=Path.GetFullPath(databasePath)+".pre-local-namespace-v1-"+manifest.Sha256+".db"
            || await HashSyncBackupAsync(manifest.BackupPath,cancellationToken)!=manifest.Sha256)throw new InvalidDataException("Namespace rollback identity is invalid.");
        Directory.CreateDirectory(Path.GetDirectoryName(destination)!);await CreateConsistentSyncDatabaseCopyAsync(manifest.BackupPath,destination,cancellationToken);return destination;
    }
}
