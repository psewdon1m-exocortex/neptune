using System.Text.Json;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private static readonly string[] CycleSupersessionColumns=["supersedes_id TEXT","superseded_by TEXT","replacement_gap_json TEXT"];
    private static readonly Lazy<Dictionary<string,string>> SupersessionCycleSchema=new(()=>
    {
        var schema=new Dictionary<string,string>(CycleSchemaV5,StringComparer.Ordinal);
        schema["local_cycle_format"]=schema["local_cycle_format"].Replace("CHECK(version=5)","CHECK(version=6)",StringComparison.Ordinal);
        schema["local_cycle_replacement_requests"]=schema["local_cycle_replacement_requests"][..^1]+","+string.Join(',',CycleSupersessionColumns)+")";
        schema["local_cycle_replacement_pending"]="CREATE UNIQUE INDEX local_cycle_replacement_pending ON local_cycle_replacement_requests(mapping_id) WHERE status<>3 AND superseded_by IS NULL";
        schema["local_cycle_replacement_successor"]="CREATE UNIQUE INDEX local_cycle_replacement_successor ON local_cycle_replacement_requests(supersedes_id) WHERE supersedes_id IS NOT NULL";
        return schema;
    });
    private static Dictionary<string,string> CycleSchemaV6=>SupersessionCycleSchema.Value;

    // Installed only for an explicit supersession after read-only native-loss
    // admission. Ordinary replacements retain their existing format5 path.
    private async Task EnsureCycleSupersessionAsync(LocalJournalAdmission admission,CancellationToken ct)
    {
        if(await PreflightCycleAsync(ct)is 6 or 7)return;
        await using var migration=await AcquireSyncMigrationLockAsync(ct);var previous=await PreflightCycleAsync(ct);if(previous is 6 or 7)return;
        if(previous!=5)throw new InvalidDataException("Supersession requires a recognized pending format5 request.");
        var copy=checked(new FileInfo(databasePath).Length+(File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0));CheckLocalDiskAdmission(admission,checked(copy+NamespaceReserve(64)));
        var temp=databasePath+".cycle-supersession-"+Guid.NewGuid().ToString("N")+".partial";
        try
        {
            await CreateCoherenceDatabaseCopy(databasePath,temp,ct);var hash=await HashSyncBackupAsync(temp,ct);var backup=Path.GetFullPath(databasePath)+".pre-local-cycle-v6-"+hash+".db";
            if(File.Exists(backup)){if(await HashSyncBackupAsync(backup,ct)!=hash)throw new InvalidDataException("Supersession backup changed.");File.Delete(temp);}else File.Move(temp,backup);
            var manifest=JsonSerializer.Serialize(new CycleBackup(Path.GetFullPath(databasePath),backup,hash,6),LocalCycleWire.Json);var partial=databasePath+".local-cycle-v6-backup.json."+Guid.NewGuid().ToString("N")+".partial";
            try{await File.WriteAllTextAsync(partial,manifest,ct);using(var f=new FileStream(partial,FileMode.Open,FileAccess.Write,FileShare.None))f.Flush(true);File.Move(partial,databasePath+".local-cycle-v6-backup.json",true);}finally{if(File.Exists(partial))File.Delete(partial);}
            await CheckLocalAdmissionAsync(admission,0,NamespaceReserve(64),ct);await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);
            await ExecuteSyncAsync(c,t,"ALTER TABLE local_cycle_format RENAME TO previous_cycle_format",ct);await ExecuteSyncAsync(c,t,CycleSchemaV6["local_cycle_format"],ct);await ExecuteSyncAsync(c,t,"INSERT INTO local_cycle_format SELECT singleton,6,bytes FROM previous_cycle_format;DROP TABLE previous_cycle_format",ct);
            foreach(var column in CycleSupersessionColumns)await ExecuteSyncAsync(c,t,"ALTER TABLE local_cycle_replacement_requests ADD COLUMN "+column,ct);
            await ExecuteSyncAsync(c,t,"DROP INDEX local_cycle_replacement_pending",ct);await ExecuteSyncAsync(c,t,CycleSchemaV6["local_cycle_replacement_pending"],ct);await ExecuteSyncAsync(c,t,CycleSchemaV6["local_cycle_replacement_successor"],ct);await t.CommitAsync(ct);
        }
        finally{if(File.Exists(temp))File.Delete(temp);}
    }
    public Task<string> CreateLocalCycleV5RollbackCopyAsync(string destination,CancellationToken cancellationToken=default)=>CreateCycleRollbackCopy(destination,6,cancellationToken);
}
