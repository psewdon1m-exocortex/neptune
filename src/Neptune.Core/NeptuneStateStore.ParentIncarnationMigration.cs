using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    private const string IncarnationTable="local_parent_reconcile_incarnations";
    private const string IncarnationDdl="CREATE TABLE local_parent_reconcile_incarnations(scope_id TEXT NOT NULL,node_id TEXT NOT NULL,native_id TEXT NOT NULL,created INTEGER NOT NULL,prior_native_id TEXT NOT NULL,prior_created INTEGER NOT NULL,plan_id TEXT NOT NULL,cut_usn INTEGER NOT NULL,baseline_work INTEGER NOT NULL,PRIMARY KEY(scope_id,node_id),UNIQUE(scope_id,native_id),FOREIGN KEY(scope_id,node_id) REFERENCES local_cycle_files(scope_id,node_id)) WITHOUT ROWID";
    private static Dictionary<string,string> ParentIncarnationSchema=>new(ParentMembershipSchema,StringComparer.Ordinal){[IncarnationTable]=IncarnationDdl};
    private static async Task<bool> HasIncarnations(SqliteConnection c,SqliteTransaction t,CancellationToken ct)
    {await using var q=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='local_parent_reconcile_incarnations'");return await q.ExecuteScalarAsync(ct)is not null;}
    private async Task EnsureParentReconcileV3Async(LocalJournalAdmission admission,CancellationToken ct)
    {
        await EnsureParentReconcileV2Async(admission,ct);if(await PreflightParentReconcileAsync(ct)>=3)return;
        await using var migration=await AcquireSyncMigrationLockAsync(ct);if(await PreflightParentReconcileAsync(ct)>=3)return;
        var reserve=NamespaceReserve(8);var copy=checked(new FileInfo(databasePath).Length+(File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0));CheckLocalDiskAdmission(admission,checked(copy+reserve));
        var temporary=databasePath+".parent-incarnation-"+Guid.NewGuid().ToString("N")+".partial";
        try
        {
            await CreateConsistentSyncDatabaseCopyAsync(databasePath,temporary,ct);var hash=await HashSyncBackupAsync(temporary,ct);var backup=Path.GetFullPath(databasePath)+".pre-local-parent-reconcile-v3-"+hash+".db";
            if(File.Exists(backup)){if(await HashSyncBackupAsync(backup,ct)!=hash)throw new InvalidDataException("Incarnation backup changed.");File.Delete(temporary);}else File.Move(temporary,backup);
            var manifest=JsonSerializer.Serialize(new ParentReconcileBackup(Path.GetFullPath(databasePath),backup,hash,3),SyncStateJson);var pending=databasePath+".local-parent-reconcile-v3-backup.json."+Guid.NewGuid().ToString("N")+".partial";
            try{await File.WriteAllTextAsync(pending,manifest,ct);using(var f=new FileStream(pending,FileMode.Open,FileAccess.Write,FileShare.None))f.Flush(true);File.Move(pending,databasePath+".local-parent-reconcile-v3-backup.json",true);}finally{if(File.Exists(pending))File.Delete(pending);}
            await CheckLocalAdmissionAsync(admission,0,reserve,ct);await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);
            await ExecuteSyncAsync(c,t,IncarnationDdl,ct);await ExecuteSyncAsync(c,t,"UPDATE local_parent_reconcile_format SET version=3 WHERE singleton=1 AND version=2",ct);await t.CommitAsync(ct);
        }
        finally{if(File.Exists(temporary))File.Delete(temporary);}
    }
    public Task<string> CreateParentIncarnationRollbackCopyAsync(string destination,CancellationToken cancellationToken=default)=>CreateParentRollbackCopy(destination,3,cancellationToken);
}
