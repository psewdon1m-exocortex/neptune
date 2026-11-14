using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    private static readonly string[] ParentMembershipColumns=[
        "full_membership INTEGER NOT NULL DEFAULT 0 CHECK(full_membership IN(0,1))",
        "hardlink_after INTEGER NOT NULL DEFAULT -1",
        "hardlink_verified INTEGER NOT NULL DEFAULT 0 CHECK(hardlink_verified IN(0,1))",
        "scan_start_usn INTEGER NOT NULL DEFAULT 0 CHECK(scan_start_usn>=0)"];
    private static Dictionary<string,string> ParentMembershipSchema
    {
        get{var result=new Dictionary<string,string>(ParentReconcileSchema,StringComparer.Ordinal);var table=result["local_parent_reconcile_plans"];result["local_parent_reconcile_plans"]=table[..^1]+","+string.Join(',',ParentMembershipColumns)+")";return result;}
    }
    private async Task EnsureParentReconcileV2Async(LocalJournalAdmission admission,CancellationToken ct)
    {
        await EnsureParentReconcileV1Async(admission,ct);
        if(await PreflightParentReconcileAsync(ct)>=2)return;
        await using var migration=await AcquireSyncMigrationLockAsync(ct);if(await PreflightParentReconcileAsync(ct)>=2)return;
        var reserve=NamespaceReserve(8);var copyBytes=checked(new FileInfo(databasePath).Length+(File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0));CheckLocalDiskAdmission(admission,checked(copyBytes+reserve));
        var temporary=databasePath+".local-parent-membership-"+Guid.NewGuid().ToString("N")+".partial";
        try
        {
            await CreateConsistentSyncDatabaseCopyAsync(databasePath,temporary,ct);var hash=await HashSyncBackupAsync(temporary,ct);var backup=Path.GetFullPath(databasePath)+".pre-local-parent-reconcile-v2-"+hash+".db";
            if(File.Exists(backup)){if(await HashSyncBackupAsync(backup,ct)!=hash)throw new InvalidDataException("Membership migration backup changed.");File.Delete(temporary);}else File.Move(temporary,backup);
            var manifest=JsonSerializer.Serialize(new ParentReconcileBackup(Path.GetFullPath(databasePath),backup,hash,2),SyncStateJson);var pending=databasePath+".local-parent-reconcile-v2-backup.json."+Guid.NewGuid().ToString("N")+".partial";
            try{await File.WriteAllTextAsync(pending,manifest,ct);using(var f=new FileStream(pending,FileMode.Open,FileAccess.Write,FileShare.None))f.Flush(true);File.Move(pending,databasePath+".local-parent-reconcile-v2-backup.json",true);}finally{if(File.Exists(pending))File.Delete(pending);}
            await CheckLocalAdmissionAsync(admission,0,reserve,ct);await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);
            foreach(var column in ParentMembershipColumns)await ExecuteSyncAsync(c,t,"ALTER TABLE local_parent_reconcile_plans ADD COLUMN "+column,ct);
            await ExecuteSyncAsync(c,t,"UPDATE local_parent_reconcile_format SET version=2 WHERE singleton=1 AND version=1",ct);await t.CommitAsync(ct);
        }
        finally{if(File.Exists(temporary))File.Delete(temporary);}
    }
    private static async Task<bool> IsFullParentMembership(SqliteConnection c,SqliteTransaction t,string id,CancellationToken ct)
    {await using var q=SyncCommand(c,t,"SELECT full_membership FROM local_parent_reconcile_plans WHERE plan_id=$id");q.Parameters.AddWithValue("$id",id);return await q.ExecuteScalarAsync(ct)is long value&&value==1;}
    private static async Task<bool> VerifyParentHardlinkAbsence(SqliteConnection c,SqliteTransaction t,LocalParentReconciliation plan,string journal,long end,CancellationToken ct)
    {
        long after;
        await using(var q=SyncCommand(c,t,"SELECT hardlink_after FROM local_parent_reconcile_plans WHERE plan_id=$id AND full_membership=1 AND hardlink_verified=0")){q.Parameters.AddWithValue("$id",plan.PlanId);if(await q.ExecuteScalarAsync(ct)is not long cursor)return false;after=cursor;}
        var records=new List<LocalUsnRecord>();await using(var q=SyncCommand(c,t,"SELECT usn,payload FROM local_coherence_unresolved WHERE coherence_id=$coherence AND usn>$after AND usn<$start ORDER BY usn LIMIT 257"))
        {q.Parameters.AddWithValue("$coherence",plan.CoherenceId);q.Parameters.AddWithValue("$after",after);q.Parameters.AddWithValue("$start",plan.StartUsn);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))records.Add(ProjectionRecord(journal,r.GetInt64(0),end,(byte[])r.GetValue(1)));}
        foreach(var record in records.Take(256))
        {
            if((record.Reason&0x10000)==0)continue;
            await using var q=SyncCommand(c,t,record.MajorVersion==3?"SELECT 1 FROM local_parent_reconcile_entries WHERE plan_id=$id AND node_id=$node LIMIT 1":"SELECT 1 FROM local_parent_reconcile_entries WHERE plan_id=$id AND wire64=$node LIMIT 1");q.Parameters.AddWithValue("$id",plan.PlanId);q.Parameters.AddWithValue("$node",record.FileIdHex);
            if(await q.ExecuteScalarAsync(ct)is not null)throw new LocalSourceGapException("ParentHardlinkAmbiguity","Unknown historical hardlink has current inside membership; original evidence retained.");
        }
        await using(var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_plans SET hardlink_after=$after,hardlink_verified=$done WHERE plan_id=$id AND full_membership=1 AND hardlink_verified=0")){q.Parameters.AddWithValue("$id",plan.PlanId);q.Parameters.AddWithValue("$after",records.Count==0?after:records[Math.Min(256,records.Count)-1].Usn);q.Parameters.AddWithValue("$done",records.Count<257?1:0);await q.ExecuteNonQueryAsync(ct);}
        return true;
    }
}
