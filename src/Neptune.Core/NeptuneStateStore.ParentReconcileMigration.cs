using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private static readonly Dictionary<string,string> ParentReconcileSchema=new(StringComparer.Ordinal)
    {
        ["local_parent_reconcile_format"]="CREATE TABLE local_parent_reconcile_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL,bytes INTEGER NOT NULL DEFAULT 0 CHECK(bytes>=0))",
        ["local_parent_reconcile_plans"]="CREATE TABLE local_parent_reconcile_plans(plan_id TEXT PRIMARY KEY,root_key TEXT NOT NULL,scope_id TEXT NOT NULL,coherence_id TEXT NOT NULL,owner_epoch INTEGER NOT NULL,revision INTEGER NOT NULL,gap INTEGER NOT NULL,journal TEXT NOT NULL,start_usn INTEGER NOT NULL,cut_usn INTEGER NOT NULL DEFAULT 0,status INTEGER NOT NULL CHECK(status BETWEEN 0 AND 4),record_after INTEGER NOT NULL DEFAULT -1,verify_after INTEGER NOT NULL DEFAULT 0,import_after INTEGER NOT NULL DEFAULT 0,missing_after TEXT NOT NULL DEFAULT '',parents INTEGER NOT NULL DEFAULT 0,entries INTEGER NOT NULL DEFAULT 0,error TEXT,retire_phase INTEGER NOT NULL DEFAULT 0 CHECK(retire_phase BETWEEN 0 AND 3))",
        ["local_parent_reconcile_history"]="CREATE INDEX local_parent_reconcile_history ON local_parent_reconcile_plans(scope_id,status,cut_usn)",
        ["local_parent_reconcile_retirement"]="CREATE INDEX local_parent_reconcile_retirement ON local_parent_reconcile_plans(scope_id,status,retire_phase,cut_usn)",
        ["local_parent_reconcile_pending"]="CREATE UNIQUE INDEX local_parent_reconcile_pending ON local_parent_reconcile_plans(scope_id) WHERE status<3",
        ["local_parent_reconcile_parents"]="CREATE TABLE local_parent_reconcile_parents(plan_id TEXT NOT NULL,node_id TEXT NOT NULL,path TEXT NOT NULL,created INTEGER NOT NULL,claim TEXT NOT NULL,state INTEGER NOT NULL DEFAULT 0 CHECK(state BETWEEN 0 AND 2),child_after INTEGER NOT NULL DEFAULT 0,refilled INTEGER NOT NULL DEFAULT 0,entries INTEGER NOT NULL DEFAULT 0,matched INTEGER NOT NULL DEFAULT 0,missing_phase INTEGER NOT NULL DEFAULT 0 CHECK(missing_phase BETWEEN 0 AND 4),missing_after INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(plan_id,node_id),UNIQUE(plan_id,path)) WITHOUT ROWID",
        ["local_parent_reconcile_frontier"]="CREATE INDEX local_parent_reconcile_frontier ON local_parent_reconcile_parents(plan_id,state,refilled,node_id)",
        ["local_parent_reconcile_entries"]="CREATE TABLE local_parent_reconcile_entries(entry_id INTEGER PRIMARY KEY,plan_id TEXT NOT NULL,parent_id TEXT NOT NULL,node_id TEXT NOT NULL,name TEXT NOT NULL,canonical TEXT NOT NULL,created INTEGER NOT NULL,kind INTEGER NOT NULL CHECK(kind IN(0,1)),size INTEGER NOT NULL CHECK(size>=0),wire64 TEXT,path TEXT NOT NULL,UNIQUE(plan_id,parent_id,canonical),UNIQUE(plan_id,node_id),FOREIGN KEY(plan_id,parent_id) REFERENCES local_parent_reconcile_parents(plan_id,node_id))",
        ["local_parent_reconcile_children"]="CREATE INDEX local_parent_reconcile_children ON local_parent_reconcile_entries(plan_id,parent_id,kind,entry_id)",
        ["local_parent_reconcile_wire"]="CREATE UNIQUE INDEX local_parent_reconcile_wire ON local_parent_reconcile_entries(plan_id,wire64) WHERE wire64 IS NOT NULL",
        ["local_parent_reconcile_page"]="CREATE INDEX local_parent_reconcile_page ON local_parent_reconcile_entries(plan_id,entry_id)",
        ["local_parent_reconcile_versions"]="CREATE TABLE local_parent_reconcile_versions(coherence_id TEXT NOT NULL,parent_id TEXT NOT NULL,current_plan TEXT,pending_plan TEXT,PRIMARY KEY(coherence_id,parent_id)) WITHOUT ROWID",
        ["local_parent_reconcile_version_current"]="CREATE INDEX local_parent_reconcile_version_current ON local_parent_reconcile_versions(current_plan) WHERE current_plan IS NOT NULL",
        ["local_parent_reconcile_version_pending"]="CREATE INDEX local_parent_reconcile_version_pending ON local_parent_reconcile_versions(pending_plan) WHERE pending_plan IS NOT NULL",
        ["local_parent_reconcile_rename"]="CREATE TABLE local_parent_reconcile_rename(plan_id TEXT NOT NULL,wire TEXT NOT NULL,major INTEGER NOT NULL,state INTEGER NOT NULL CHECK(state IN(1,2)),old_parent TEXT NOT NULL,new_parent TEXT,last_usn INTEGER NOT NULL,PRIMARY KEY(plan_id,major,wire)) WITHOUT ROWID",
        ["local_parent_reconcile_conflicts"]="CREATE TABLE local_parent_reconcile_conflicts(scope_id TEXT NOT NULL,node_id TEXT NOT NULL,plan_id TEXT NOT NULL,reason TEXT NOT NULL,PRIMARY KEY(scope_id,node_id)) WITHOUT ROWID",
        ["local_parent_reconcile_conflict_plan"]="CREATE INDEX local_parent_reconcile_conflict_plan ON local_parent_reconcile_conflicts(plan_id)"
    };
    private Task ValidateOptionalParentReconcileBeforeInitializeAsync(CancellationToken ct)=>PreflightParentReconcileAsync(ct);
    private async Task<int> PreflightParentReconcileAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();if(!File.Exists(databasePath))return 0;
        try
        {
            await using var c=new SqliteConnection(ReadOnlySyncConnectionString(databasePath));await c.OpenAsync(ct);using var t=c.BeginTransaction(deferred:true);
            await using var q=SyncCommand(c,t,"SELECT name,sql FROM sqlite_master WHERE (name GLOB 'local_parent_reconcile_*' OR tbl_name GLOB 'local_parent_reconcile_*' AND type IN('trigger','index')) AND name NOT LIKE 'sqlite_autoindex_%'");
            var actual=new Dictionary<string,string>(StringComparer.Ordinal);
            await using(var r=await q.ExecuteReaderAsync(ct))while(await r.ReadAsync(ct))actual.Add(r.GetString(0),r.IsDBNull(1)?"":r.GetString(1));
            if(actual.Count==0)return 0;if(!actual.ContainsKey("local_parent_reconcile_format"))throw new InvalidDataException("Parent reconciliation module is incomplete.");
            q.CommandText="SELECT version FROM local_parent_reconcile_format WHERE singleton=1";
            if(await q.ExecuteScalarAsync(ct) is not long version || version is not(1 or 2 or 3 or 4 or 5))throw new InvalidDataException("Unsupported optional namespace format; no writes attempted.");
            static string Normal(string s)=>string.Concat(s.Where(x=>!char.IsWhiteSpace(x))).TrimEnd(';');
            var schema=version==1?ParentReconcileSchema:version==2?ParentMembershipSchema:version==3?ParentIncarnationSchema:version==4?ParentOrphanCloseSchema:ParentExternalCutSchema;
            if(actual.Count!=schema.Count || schema.Any(x=>!actual.TryGetValue(x.Key,out var sql)||Normal(sql)!=Normal(x.Value)))throw new InvalidDataException("Parent reconciliation schema is incompatible.");
            return (int)version;
        }
        catch(SqliteException error){throw new InvalidDataException("Parent reconciliation module cannot be inspected safely.",error);}
    }
    private async Task EnsureParentReconcileV1Async(LocalJournalAdmission admission,CancellationToken ct)
    {
        if(await PreflightParentReconcileAsync(ct)>=1)return;
        await using var migration=await AcquireSyncMigrationLockAsync(ct);if(await PreflightParentReconcileAsync(ct)>=1)return;
        var reserve=NamespaceReserve(32);var copyBytes=checked(new FileInfo(databasePath).Length+(File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0));
        // Copy and DDL reservations coexist; all committed WAL and earlier
        // rollback copies count toward the same physical profile cap.
        CheckLocalDiskAdmission(admission,checked(copyBytes+reserve));
        var temporary=databasePath+".local-parent-reconcile-"+Guid.NewGuid().ToString("N")+".partial";
        try
        {
            await CreateConsistentSyncDatabaseCopyAsync(databasePath,temporary,ct);var hash=await HashSyncBackupAsync(temporary,ct);
            var backup=Path.GetFullPath(databasePath)+".pre-local-parent-reconcile-v1-"+hash+".db";
            if(File.Exists(backup)){if(await HashSyncBackupAsync(backup,ct)!=hash)throw new InvalidDataException("Namespace backup changed.");File.Delete(temporary);}else File.Move(temporary,backup);
            var manifest=JsonSerializer.Serialize(new ParentReconcileBackup(Path.GetFullPath(databasePath),backup,hash,1),SyncStateJson);
            var tempManifest=databasePath+".local-parent-reconcile-v1-backup.json."+Guid.NewGuid().ToString("N")+".partial";
            try{await File.WriteAllTextAsync(tempManifest,manifest,ct);using(var f=new FileStream(tempManifest,FileMode.Open,FileAccess.Write,FileShare.None))f.Flush(true);File.Move(tempManifest,databasePath+".local-parent-reconcile-v1-backup.json",true);}
            finally{if(File.Exists(tempManifest))File.Delete(tempManifest);}
            await CheckLocalAdmissionAsync(admission,0,reserve,ct);
            await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);
            foreach(var sql in ParentReconcileSchema.Values)await ExecuteSyncAsync(c,t,sql,ct);
            await ExecuteSyncAsync(c,t,"INSERT INTO local_parent_reconcile_format(singleton,version) VALUES(1,1)",ct);await t.CommitAsync(ct);
        }
        finally{if(File.Exists(temporary))File.Delete(temporary);}
    }
    private sealed record ParentReconcileBackup(string SourcePath,string BackupPath,string Sha256,int TargetFormat);
    public Task<string> CreateParentReconciliationRollbackCopyAsync(string destination,CancellationToken cancellationToken=default)
        =>CreateParentRollbackCopy(destination,1,cancellationToken);
    public Task<string> CreateParentMembershipRollbackCopyAsync(string destination,CancellationToken cancellationToken=default)
        =>CreateParentRollbackCopy(destination,2,cancellationToken);
    private async Task<string> CreateParentRollbackCopy(string destination,int targetFormat,CancellationToken cancellationToken)
    {
        destination=Path.GetFullPath(destination);
        if(string.Equals(destination,Path.GetFullPath(databasePath),OperatingSystem.IsWindows()?StringComparison.OrdinalIgnoreCase:StringComparison.Ordinal)
            ||File.Exists(destination)||File.Exists(destination+"-wal")||File.Exists(destination+"-shm"))throw new InvalidOperationException("Parent reconciliation rollback requires a separate absent destination.");
        var path=databasePath+$".local-parent-reconcile-v{targetFormat}-backup.json";if(new FileInfo(path).Length>16*1024)throw new InvalidDataException("Unbounded parent reconciliation rollback manifest.");
        var bytes=await File.ReadAllBytesAsync(path,cancellationToken);using var json=JsonDocument.Parse(bytes,new(){MaxDepth=8});
        if(json.RootElement.ValueKind!=JsonValueKind.Object)throw new InvalidDataException("Parent reconciliation rollback manifest is not an object.");
        var expected=new HashSet<string>(["sourcePath","backupPath","sha256","targetFormat"],StringComparer.Ordinal);
        foreach(var property in json.RootElement.EnumerateObject())if(!expected.Remove(property.Name))throw new InvalidDataException("Parent reconciliation rollback manifest has duplicate/unexpected fields.");
        if(expected.Count!=0 || json.RootElement.GetProperty("targetFormat").GetRawText()!=targetFormat.ToString(System.Globalization.CultureInfo.InvariantCulture))throw new InvalidDataException("Parent reconciliation rollback manifest format is invalid.");
        static string Text(JsonElement root,string name)=>root.GetProperty(name).ValueKind==JsonValueKind.String?root.GetProperty(name).GetString()!:throw new InvalidDataException("Parent reconciliation rollback manifest field is not a string.");
        var manifest=new ParentReconcileBackup(Text(json.RootElement,"sourcePath"),Text(json.RootElement,"backupPath"),Text(json.RootElement,"sha256"),targetFormat);
        if(manifest.SourcePath!=Path.GetFullPath(databasePath) || manifest.Sha256.Length!=64 || manifest.Sha256!=manifest.Sha256.ToLowerInvariant() || manifest.Sha256.Any(x=>!char.IsAsciiHexDigit(x))
            || manifest.BackupPath!=Path.GetFullPath(databasePath)+$".pre-local-parent-reconcile-v{targetFormat}-"+manifest.Sha256+".db"
            || await HashSyncBackupAsync(manifest.BackupPath,cancellationToken)!=manifest.Sha256)throw new InvalidDataException("Parent reconciliation rollback identity is invalid.");
        Directory.CreateDirectory(Path.GetDirectoryName(destination)!);await CreateConsistentSyncDatabaseCopyAsync(manifest.BackupPath,destination,cancellationToken);return destination;
    }
}
