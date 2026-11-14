using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    private const string OrphanCloseTable="local_parent_reconcile_orphan_closes";
    private const string OrphanCloseDdl="CREATE TABLE local_parent_reconcile_orphan_closes(plan_id TEXT NOT NULL,usn INTEGER NOT NULL,major INTEGER NOT NULL CHECK(major IN(2,3)),wire TEXT NOT NULL,parent TEXT NOT NULL,payload_hash TEXT NOT NULL,verified INTEGER NOT NULL DEFAULT 0 CHECK(verified IN(0,1)),PRIMARY KEY(plan_id,usn)) WITHOUT ROWID";
    private static Dictionary<string,string> ParentOrphanCloseSchema=>new(ParentIncarnationSchema,StringComparer.Ordinal){[OrphanCloseTable]=OrphanCloseDdl};
    private async Task EnsureParentReconcileAsync(LocalJournalAdmission admission,CancellationToken ct)
    {
        await EnsureParentReconcileV3Async(admission,ct);if(await PreflightParentReconcileAsync(ct)>=4)return;
        await using var migration=await AcquireSyncMigrationLockAsync(ct);if(await PreflightParentReconcileAsync(ct)>=4)return;
        var reserve=NamespaceReserve(8);var copy=checked(new FileInfo(databasePath).Length+(File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0));CheckLocalDiskAdmission(admission,checked(copy+reserve));
        var temporary=databasePath+".parent-orphan-close-"+Guid.NewGuid().ToString("N")+".partial";
        try
        {
            await CreateConsistentSyncDatabaseCopyAsync(databasePath,temporary,ct);var hash=await HashSyncBackupAsync(temporary,ct);var backup=Path.GetFullPath(databasePath)+".pre-local-parent-reconcile-v4-"+hash+".db";
            if(File.Exists(backup)){if(await HashSyncBackupAsync(backup,ct)!=hash)throw new InvalidDataException("Orphan-close backup changed.");File.Delete(temporary);}else File.Move(temporary,backup);
            var manifest=JsonSerializer.Serialize(new ParentReconcileBackup(Path.GetFullPath(databasePath),backup,hash,4),SyncStateJson);var pending=databasePath+".local-parent-reconcile-v4-backup.json."+Guid.NewGuid().ToString("N")+".partial";
            try{await File.WriteAllTextAsync(pending,manifest,ct);using(var f=new FileStream(pending,FileMode.Open,FileAccess.Write,FileShare.None))f.Flush(true);File.Move(pending,databasePath+".local-parent-reconcile-v4-backup.json",true);}finally{if(File.Exists(pending))File.Delete(pending);}
            await CheckLocalAdmissionAsync(admission,0,reserve,ct);await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);
            await ExecuteSyncAsync(c,t,OrphanCloseDdl,ct);await ExecuteSyncAsync(c,t,"UPDATE local_parent_reconcile_format SET version=4 WHERE singleton=1 AND version=3",ct);await t.CommitAsync(ct);
        }
        finally{if(File.Exists(temporary))File.Delete(temporary);}
    }
    public Task<string> CreateParentOrphanCloseRollbackCopyAsync(string destination,CancellationToken cancellationToken=default)=>CreateParentRollbackCopy(destination,4,cancellationToken);
}
