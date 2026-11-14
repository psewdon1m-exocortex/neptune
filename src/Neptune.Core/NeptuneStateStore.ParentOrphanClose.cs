using Microsoft.Data.Sqlite;

namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    private static async Task<bool> KnownParentCloseIdentity(SqliteConnection c,SqliteTransaction t,LocalParentReconciliation p,LocalUsnRecord record,CancellationToken ct)
    {
        foreach(var wire in new[]{record.FileIdHex,record.ParentIdHex})
        {
            if(await CoherenceNodeAsync(c,t,p.CoherenceId,record.MajorVersion,wire,ct,true)is not null)return true;
            // Include retained logical baselines and current native incarnations.
            if(record.MajorVersion==3)
            {await using var q=SyncCommand(c,t,ParentNativeIdentitySql("f.node_id"));q.Parameters.AddWithValue("$scope",p.ScopeId);q.Parameters.AddWithValue("$node",wire);if(await q.ExecuteScalarAsync(ct)is not null)return true;}
        }
        return false;
    }
    private static async Task<bool> DeferUnknownParentRenameClose(SqliteConnection c,SqliteTransaction t,LocalParentReconciliation p,LocalUsnRecord record,CancellationToken ct)
    {
        if((record.Reason&0x80003000)!=0x80002000||record.Usn>=p.StartUsn)return false;
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_rename WHERE plan_id=$id AND major=$major AND wire=$wire"))
        {q.Parameters.AddWithValue("$id",p.PlanId);q.Parameters.AddWithValue("$major",record.MajorVersion);q.Parameters.AddWithValue("$wire",record.FileIdHex);if(await q.ExecuteScalarAsync(ct)is not null)return false;}
        if(await KnownParentCloseIdentity(c,t,p,record,ct))return false;
        string hash;
        await using(var q=SyncCommand(c,t,"SELECT u.payload,u.sha256,p.journal FROM local_coherence_unresolved u JOIN local_parent_reconcile_plans p ON p.coherence_id=u.coherence_id WHERE p.plan_id=$id AND u.usn=$usn"))
        {
            q.Parameters.AddWithValue("$id",p.PlanId);q.Parameters.AddWithValue("$usn",record.Usn);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))return false;
            var raw=(byte[])r.GetValue(0);hash=r.GetString(1);
            if(ProjectionHash(raw)!=hash||ProjectionRecord(r.GetString(2),record.Usn,p.StartUsn,raw)!=record)throw new InvalidDataException("Retained orphan-close evidence changed.");
        }
        // This is NOT a synthetic OLD or an Outside disposition. A separate
        // durable obligation must prove current absence before the cut installs.
        await using var write=SyncCommand(c,t,"INSERT INTO local_parent_reconcile_orphan_closes(plan_id,usn,major,wire,parent,payload_hash) VALUES($id,$usn,$major,$wire,$parent,$hash) ON CONFLICT(plan_id,usn) DO NOTHING");
        write.Parameters.AddWithValue("$id",p.PlanId);write.Parameters.AddWithValue("$usn",record.Usn);write.Parameters.AddWithValue("$major",record.MajorVersion);write.Parameters.AddWithValue("$wire",record.FileIdHex);write.Parameters.AddWithValue("$parent",record.ParentIdHex);write.Parameters.AddWithValue("$hash",hash);await write.ExecuteNonQueryAsync(ct);return true;
    }
    private static async Task<int> VerifyOrphanParentCloses(SqliteConnection c,SqliteTransaction t,LocalParentReconciliation p,CancellationToken ct)
    {
        var rows=new List<(long Usn,ushort Major,string Wire,string Parent,string Hash)>();
        await using(var q=SyncCommand(c,t,"SELECT usn,major,wire,parent,payload_hash FROM local_parent_reconcile_orphan_closes WHERE plan_id=$id AND verified=0 ORDER BY usn LIMIT 128"))
        {q.Parameters.AddWithValue("$id",p.PlanId);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))rows.Add((r.GetInt64(0),(ushort)r.GetInt32(1),r.GetString(2),r.GetString(3),r.GetString(4)));}
        if(rows.Count==0)return 0;
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_plans p WHERE plan_id=$id AND full_membership=1 AND hardlink_verified=1 AND parents>0 AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_parents n WHERE n.plan_id=p.plan_id AND (state<>2 OR refilled<>1))"))
        {q.Parameters.AddWithValue("$id",p.PlanId);if(await q.ExecuteScalarAsync(ct)is null)throw new LocalSourceGapException("ParentRenameHalfMissing","An orphan CLOSE requires completed full membership and hardlink proof.");}
        foreach(var row in rows)
        {
            LocalUsnRecord record;
            await using(var q=SyncCommand(c,t,"SELECT u.payload,p.journal FROM local_coherence_unresolved u JOIN local_parent_reconcile_plans p ON p.coherence_id=u.coherence_id WHERE p.plan_id=$id AND u.usn=$usn AND u.sha256=$hash"))
            {q.Parameters.AddWithValue("$id",p.PlanId);q.Parameters.AddWithValue("$usn",row.Usn);q.Parameters.AddWithValue("$hash",row.Hash);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))throw new InvalidDataException("Orphan-close history is absent.");var raw=(byte[])r.GetValue(0);if(ProjectionHash(raw)!=row.Hash)throw new InvalidDataException("Orphan-close history changed.");record=ProjectionRecord(r.GetString(1),row.Usn,p.StartUsn,raw);}
            if(record.MajorVersion!=row.Major||record.FileIdHex!=row.Wire||record.ParentIdHex!=row.Parent||(record.Reason&0x80003000)!=0x80002000||await KnownParentCloseIdentity(c,t,p,record,ct))throw new LocalSourceGapException("ParentRenameHalfMissing","An orphan CLOSE cannot qualify a managed identity or parent.");
            await using(var q=SyncCommand(c,t,$"SELECT 1 FROM local_parent_reconcile_entries WHERE plan_id=$id AND {(row.Major==3?"node_id":"wire64")} IN($node,$parent) LIMIT 1"))
            {q.Parameters.AddWithValue("$id",p.PlanId);q.Parameters.AddWithValue("$node",row.Wire);q.Parameters.AddWithValue("$parent",row.Parent);if(await q.ExecuteScalarAsync(ct)is not null)throw new LocalSourceGapException("ParentRenameHalfMissing","Orphan CLOSE has current inside membership; original history retained.");}
            await using var mark=SyncCommand(c,t,"UPDATE local_parent_reconcile_orphan_closes SET verified=1 WHERE plan_id=$id AND usn=$usn AND payload_hash=$hash AND verified=0");mark.Parameters.AddWithValue("$id",p.PlanId);mark.Parameters.AddWithValue("$usn",row.Usn);mark.Parameters.AddWithValue("$hash",row.Hash);await mark.ExecuteNonQueryAsync(ct);
        }
        return rows.Count;
    }
}
