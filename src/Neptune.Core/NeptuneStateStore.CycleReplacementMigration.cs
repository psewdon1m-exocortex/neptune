using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    private static readonly Dictionary<string,string> CycleReplacementSchema=new(StringComparer.Ordinal)
    {
        ["local_cycle_replacement_requests"]="CREATE TABLE local_cycle_replacement_requests(replacement_id TEXT PRIMARY KEY,mapping_id TEXT NOT NULL,original_scope TEXT NOT NULL,new_scope TEXT NOT NULL UNIQUE,original_root TEXT NOT NULL,new_root TEXT NOT NULL UNIQUE,configuration TEXT NOT NULL,fingerprint TEXT NOT NULL,original_binding TEXT NOT NULL,new_binding TEXT NOT NULL,original_journal TEXT NOT NULL,original_cursor INTEGER NOT NULL,original_gap TEXT NOT NULL,original_gap_epoch INTEGER NOT NULL,revision INTEGER NOT NULL,original_owner INTEGER NOT NULL,new_owner INTEGER NOT NULL,journal TEXT NOT NULL,start_usn INTEGER NOT NULL,status INTEGER NOT NULL DEFAULT 0 CHECK(status BETWEEN 0 AND 4),original_after INTEGER NOT NULL DEFAULT 0,candidate_attempts INTEGER NOT NULL DEFAULT 0 CHECK(candidate_attempts BETWEEN 0 AND 2),error TEXT)",
        ["local_cycle_replacement_pending"]="CREATE UNIQUE INDEX local_cycle_replacement_pending ON local_cycle_replacement_requests(mapping_id) WHERE status<>3",
        ["local_cycle_replacement_lineage"]="CREATE INDEX local_cycle_replacement_lineage ON local_cycle_replacement_requests(new_scope,original_scope,status)",
        ["local_cycle_replacement_attempts"]="CREATE TABLE local_cycle_replacement_attempts(replacement_id TEXT NOT NULL REFERENCES local_cycle_replacement_requests(replacement_id),attempt INTEGER NOT NULL CHECK(attempt BETWEEN 1 AND 2),candidate_id TEXT,coherence_id TEXT,state INTEGER NOT NULL CHECK(state BETWEEN 0 AND 2),reason TEXT,PRIMARY KEY(replacement_id,attempt)) WITHOUT ROWID",
        ["local_cycle_replacement_originals"]="CREATE TABLE local_cycle_replacement_originals(replacement_id TEXT NOT NULL REFERENCES local_cycle_replacement_requests(replacement_id),operation_id TEXT NOT NULL,work_no INTEGER NOT NULL,original_scope TEXT NOT NULL,node_id TEXT NOT NULL,kind INTEGER NOT NULL CHECK(kind IN(0,1)),input_hash TEXT NOT NULL,outcome_hash TEXT,status TEXT NOT NULL,PRIMARY KEY(replacement_id,operation_id)) WITHOUT ROWID",
        ["local_cycle_replacement_original_node"]="CREATE INDEX local_cycle_replacement_original_node ON local_cycle_replacement_originals(replacement_id,node_id,kind,work_no)"
    };
    private static readonly Lazy<Dictionary<string,string>> ReplacementCycleSchema=new(NewReplacementCycleSchema);
    private static Dictionary<string,string> CycleSchemaV5=>ReplacementCycleSchema.Value;
    private static Dictionary<string,string> NewReplacementCycleSchema()
    {
        var schema=new Dictionary<string,string>(CycleSchema,StringComparer.Ordinal);
        schema["local_cycle_format"]=schema["local_cycle_format"].Replace("CHECK(version=4)","CHECK(version=5)",StringComparison.Ordinal);
        foreach(var pair in CycleReplacementSchema)schema.Add(pair.Key,pair.Value);return schema;
    }
    // Only an explicit replacement caller installs format5. Ordinary new
    // scopes keep4; old compiled4 refuses5 before writable Initialize.
    private async Task EnsureCycleReplacementAsync(LocalJournalAdmission admission,CancellationToken ct)
    {
        if(await PreflightCycleAsync(ct)is 5 or 6 or 7)return;
        await EnsureCycleAsync(admission,ct);await using var migration=await AcquireSyncMigrationLockAsync(ct);
        var previous=await PreflightCycleAsync(ct);if(previous is 5 or 6 or 7)return;if(previous!=4)throw new InvalidDataException("Replacement requires recognized current cycle4.");
        var copy=checked(new FileInfo(databasePath).Length+(File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0));CheckLocalDiskAdmission(admission,checked(copy+NamespaceReserve(64)));
        var temp=databasePath+".cycle-replacement-"+Guid.NewGuid().ToString("N")+".partial";
        try
        {
            await CreateCoherenceDatabaseCopy(databasePath,temp,ct);var hash=await HashSyncBackupAsync(temp,ct);var backup=Path.GetFullPath(databasePath)+".pre-local-cycle-v5-"+hash+".db";
            if(File.Exists(backup)){if(await HashSyncBackupAsync(backup,ct)!=hash)throw new InvalidDataException("Replacement backup changed.");File.Delete(temp);}else File.Move(temp,backup);
            var manifest=JsonSerializer.Serialize(new CycleBackup(Path.GetFullPath(databasePath),backup,hash,5),LocalCycleWire.Json);var partial=databasePath+".local-cycle-v5-backup.json."+Guid.NewGuid().ToString("N")+".partial";
            try{await File.WriteAllTextAsync(partial,manifest,ct);using(var f=new FileStream(partial,FileMode.Open,FileAccess.Write,FileShare.None))f.Flush(true);File.Move(partial,databasePath+".local-cycle-v5-backup.json",true);}finally{if(File.Exists(partial))File.Delete(partial);}
            await CheckLocalAdmissionAsync(admission,0,NamespaceReserve(64),ct);await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);
            await ExecuteSyncAsync(c,t,"ALTER TABLE local_cycle_format RENAME TO previous_cycle_format",ct);await ExecuteSyncAsync(c,t,CycleSchemaV5["local_cycle_format"],ct);await ExecuteSyncAsync(c,t,"INSERT INTO local_cycle_format SELECT singleton,5,bytes FROM previous_cycle_format;DROP TABLE previous_cycle_format",ct);
            foreach(var sql in CycleReplacementSchema.Values)await ExecuteSyncAsync(c,t,sql,ct);await t.CommitAsync(ct);
        }
        finally{if(File.Exists(temp))File.Delete(temp);}
    }
    public Task<string> CreateLocalCycleV4RollbackCopyAsync(string destination,CancellationToken cancellationToken=default)=>CreateCycleRollbackCopy(destination,5,cancellationToken);
}
