using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private static readonly Dictionary<string,string> CoherenceSchema=new(StringComparer.Ordinal)
    {
        ["local_coherence_format"]="CREATE TABLE local_coherence_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL,bytes INTEGER NOT NULL DEFAULT 0 CHECK(bytes>=0))",
        ["local_coherence_roots"]="CREATE TABLE local_coherence_roots(coherence_id TEXT PRIMARY KEY,root_key TEXT NOT NULL,candidate_id TEXT NOT NULL REFERENCES local_namespace_candidates(candidate_id),owner_epoch INTEGER NOT NULL,journal_id TEXT NOT NULL,start_usn INTEGER NOT NULL CHECK(start_usn>=0),cut_usn INTEGER NOT NULL DEFAULT 0 CHECK(cut_usn>=0),applied_usn INTEGER NOT NULL CHECK(applied_usn>=start_usn),legacy_revision INTEGER NOT NULL,gap_epoch INTEGER NOT NULL,status INTEGER NOT NULL CHECK(status BETWEEN 0 AND 3),matched INTEGER NOT NULL DEFAULT 0,verified INTEGER NOT NULL DEFAULT 0,dirty_count INTEGER NOT NULL DEFAULT 0,unresolved_count INTEGER NOT NULL DEFAULT 0,outside_count INTEGER NOT NULL DEFAULT 0,record_count INTEGER NOT NULL DEFAULT 0,bytes INTEGER NOT NULL,max_bytes INTEGER NOT NULL,profile_cap INTEGER NOT NULL,free_floor INTEGER NOT NULL,error TEXT,last_start INTEGER,last_end INTEGER,last_hash TEXT,last_records INTEGER,last_dirty INTEGER,last_outside INTEGER,last_unknown INTEGER,retire_phase INTEGER NOT NULL DEFAULT 0 CHECK(retire_phase BETWEEN 0 AND 6))",
        ["local_coherence_active"]="CREATE TABLE local_coherence_active(root_key TEXT PRIMARY KEY,coherence_id TEXT NOT NULL UNIQUE REFERENCES local_coherence_roots(coherence_id))",
        ["local_coherence_nodes"]="CREATE TABLE local_coherence_nodes(coherence_id TEXT NOT NULL REFERENCES local_coherence_roots(coherence_id),node_id TEXT NOT NULL,wire64 TEXT NOT NULL,created INTEGER NOT NULL,kind INTEGER NOT NULL CHECK(kind IN(0,1)),PRIMARY KEY(coherence_id,node_id),UNIQUE(coherence_id,wire64)) WITHOUT ROWID",
        ["local_coherence_parents"]="CREATE TABLE local_coherence_parents(coherence_id TEXT NOT NULL REFERENCES local_coherence_roots(coherence_id),node_id TEXT NOT NULL,claim TEXT NOT NULL,matched INTEGER NOT NULL DEFAULT 0,state INTEGER NOT NULL CHECK(state IN(0,1)),PRIMARY KEY(coherence_id,node_id)) WITHOUT ROWID",
        ["local_coherence_matches"]="CREATE TABLE local_coherence_matches(coherence_id TEXT NOT NULL REFERENCES local_coherence_roots(coherence_id),entry_id INTEGER NOT NULL,PRIMARY KEY(coherence_id,entry_id)) WITHOUT ROWID",
        ["local_coherence_dirty"]="CREATE TABLE local_coherence_dirty(coherence_id TEXT NOT NULL REFERENCES local_coherence_roots(coherence_id),node_id TEXT NOT NULL,created INTEGER NOT NULL,kind INTEGER NOT NULL CHECK(kind IN(0,1)),generation INTEGER NOT NULL CHECK(generation>0),last_usn INTEGER NOT NULL,reasons INTEGER NOT NULL,PRIMARY KEY(coherence_id,node_id)) WITHOUT ROWID",
        ["local_coherence_unresolved"]="CREATE TABLE local_coherence_unresolved(coherence_id TEXT NOT NULL REFERENCES local_coherence_roots(coherence_id),usn INTEGER NOT NULL,payload BLOB NOT NULL CHECK(length(payload)<=592),sha256 TEXT NOT NULL,code TEXT NOT NULL,bytes INTEGER NOT NULL,PRIMARY KEY(coherence_id,usn)) WITHOUT ROWID",
        ["local_coherence_dispositions"]="CREATE TABLE local_coherence_dispositions(coherence_id TEXT NOT NULL REFERENCES local_coherence_roots(coherence_id),usn INTEGER NOT NULL,sha256 TEXT NOT NULL,kind INTEGER NOT NULL CHECK(kind IN(0,1,2)),bytes INTEGER NOT NULL,PRIMARY KEY(coherence_id,usn)) WITHOUT ROWID",
        ["local_coherence_candidate_pins"]="CREATE INDEX local_coherence_candidate_pins ON local_coherence_roots(candidate_id,coherence_id)",
        ["local_coherence_root_pins"]="CREATE INDEX local_coherence_root_pins ON local_coherence_roots(root_key,journal_id,start_usn,applied_usn)",
        ["local_coherence_dirty_page"]="CREATE INDEX local_coherence_dirty_page ON local_coherence_dirty(coherence_id,last_usn,node_id)"
    };
    private Task ValidateOptionalLocalCoherenceBeforeInitializeAsync(CancellationToken ct)=>PreflightCoherenceAsync(ct);
    private async Task<int> PreflightCoherenceAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();if(!File.Exists(databasePath))return 0;
        try
        {
            await using var c=new SqliteConnection(ReadOnlySyncConnectionString(databasePath));await c.OpenAsync(ct);using var t=c.BeginTransaction(deferred:true);
            await using var q=SyncCommand(c,t,"SELECT name,sql FROM sqlite_master WHERE (name GLOB 'local_coherence_*' OR tbl_name GLOB 'local_coherence_*' AND type IN('trigger','index')) AND name NOT LIKE 'sqlite_autoindex_%'");
            var actual=new Dictionary<string,string>(StringComparer.Ordinal);await using(var r=await q.ExecuteReaderAsync(ct))while(await r.ReadAsync(ct)){if(actual.Count>=CoherenceSchema.Count)throw new InvalidDataException("Coherence schema has unexpected objects.");actual.Add(r.GetString(0),r.IsDBNull(1)?"":r.GetString(1));}
            if(actual.Count==0)return 0;if(!actual.ContainsKey("local_coherence_format"))throw new InvalidDataException("Coherence module is incomplete.");
            q.CommandText="SELECT version FROM local_coherence_format WHERE singleton=1";
            if(await q.ExecuteScalarAsync(ct)is not long version||version!=1)throw new InvalidDataException("Unsupported optional coherence format; no writes attempted.");
            static string Normal(string s)=>string.Concat(s.Where(x=>!char.IsWhiteSpace(x))).TrimEnd(';');
            if(actual.Count!=CoherenceSchema.Count||CoherenceSchema.Any(x=>!actual.TryGetValue(x.Key,out var sql)||Normal(sql)!=Normal(x.Value)))throw new InvalidDataException("Coherence schema is incompatible.");
            return 1;
        }
        catch(SqliteException e){throw new InvalidDataException("Coherence module cannot be inspected safely.",e);}
    }
    private async Task EnsureCoherenceAsync(LocalJournalAdmission admission,CancellationToken ct)
    {
        if(await PreflightCoherenceAsync(ct)==1)return;
        await using var migration=await AcquireSyncMigrationLockAsync(ct);if(await PreflightCoherenceAsync(ct)==1)return;
        var copy=checked(new FileInfo(databasePath).Length+(File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0));
        CheckLocalDiskAdmission(admission,checked(copy+NamespaceReserve(32)));
        var temporary=databasePath+".local-coherence-"+Guid.NewGuid().ToString("N")+".partial";
        try
        {
            await CreateCoherenceDatabaseCopy(databasePath,temporary,ct);var hash=await HashSyncBackupAsync(temporary,ct);
            var backup=Path.GetFullPath(databasePath)+".pre-local-coherence-v1-"+hash+".db";
            if(File.Exists(backup)){if(await HashSyncBackupAsync(backup,ct)!=hash)throw new InvalidDataException("Coherence backup changed.");File.Delete(temporary);}else File.Move(temporary,backup);
            var manifest=JsonSerializer.Serialize(new CoherenceBackup(Path.GetFullPath(databasePath),backup,hash,1),SyncStateJson);
            var temp=databasePath+".local-coherence-v1-backup.json."+Guid.NewGuid().ToString("N")+".partial";
            try{await File.WriteAllTextAsync(temp,manifest,ct);using(var f=new FileStream(temp,FileMode.Open,FileAccess.Write,FileShare.None))f.Flush(true);File.Move(temp,databasePath+".local-coherence-v1-backup.json",true);}finally{if(File.Exists(temp))File.Delete(temp);}
            await CheckLocalAdmissionAsync(admission,0,NamespaceReserve(32),ct);
            await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);
            foreach(var sql in CoherenceSchema.Values)await ExecuteSyncAsync(c,t,sql,ct);
            await ExecuteSyncAsync(c,t,"INSERT INTO local_coherence_format(singleton,version) VALUES(1,1)",ct);await t.CommitAsync(ct);
        }
        finally{if(File.Exists(temporary))File.Delete(temporary);}
    }
    private static Task CreateCoherenceDatabaseCopy(string source,string destination,CancellationToken ct)
    {
        static string Native(string p)=>OperatingSystem.IsWindows()&&p.Length>=260&&!p.StartsWith("\\\\?\\",StringComparison.Ordinal)?"\\\\?\\"+p:p;
        return CreateConsistentSyncDatabaseCopyAsync(Native(Path.GetFullPath(source)),Native(Path.GetFullPath(destination)),ct);
    }
    private sealed record CoherenceBackup(string SourcePath,string BackupPath,string Sha256,int TargetFormat);
    public async Task<string> CreateLocalCoherenceRollbackCopyAsync(string destination,CancellationToken cancellationToken=default)
    {
        destination=Path.GetFullPath(destination);var ct=cancellationToken;
        if(string.Equals(destination,Path.GetFullPath(databasePath),OperatingSystem.IsWindows()?StringComparison.OrdinalIgnoreCase:StringComparison.Ordinal)
            ||File.Exists(destination)||File.Exists(destination+"-wal")||File.Exists(destination+"-shm"))throw new InvalidOperationException("Coherence rollback requires a separate absent path.");
        var path=databasePath+".local-coherence-v1-backup.json";if(new FileInfo(path).Length>16*1024)throw new InvalidDataException("Coherence backup manifest exceeds bounds.");
        using var json=JsonDocument.Parse(await File.ReadAllBytesAsync(path,ct),new(){MaxDepth=4});if(json.RootElement.ValueKind!=JsonValueKind.Object)throw new InvalidDataException("Coherence backup manifest is not an object.");
        var fields=new HashSet<string>(["sourcePath","backupPath","sha256","targetFormat"],StringComparer.Ordinal);foreach(var p in json.RootElement.EnumerateObject())if(!fields.Remove(p.Name))throw new InvalidDataException("Coherence manifest duplicate/unknown field.");
        if(fields.Count!=0||json.RootElement.GetProperty("targetFormat").GetRawText()!="1")throw new InvalidDataException("Coherence manifest version is invalid.");
        static string Text(JsonElement e,string p)=>e.GetProperty(p).ValueKind==JsonValueKind.String?e.GetProperty(p).GetString()!:throw new InvalidDataException("Coherence manifest field must be a string.");
        var m=new CoherenceBackup(Text(json.RootElement,"sourcePath"),Text(json.RootElement,"backupPath"),Text(json.RootElement,"sha256"),1);
        if(m.SourcePath!=Path.GetFullPath(databasePath)||m.Sha256.Length!=64||m.Sha256!=m.Sha256.ToLowerInvariant()||m.Sha256.Any(x=>!char.IsAsciiHexDigit(x))
            ||m.BackupPath!=Path.GetFullPath(databasePath)+".pre-local-coherence-v1-"+m.Sha256+".db"||await HashSyncBackupAsync(m.BackupPath,ct)!=m.Sha256)throw new InvalidDataException("Coherence backup binding/hash changed.");
        Directory.CreateDirectory(Path.GetDirectoryName(destination)!);await CreateCoherenceDatabaseCopy(m.BackupPath,destination,ct);return destination;
    }
}
