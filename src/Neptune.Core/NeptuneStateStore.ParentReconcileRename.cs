using Microsoft.Data.Sqlite;
namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    private static async Task RestoreParentRenameOld(SqliteConnection c,SqliteTransaction t,string id,LocalUsnRecord record,CancellationToken ct)
    {
        if((record.Reason&0x3000)!=0x2000)return;
        string root,journal;long after,end;
        await using(var q=SyncCommand(c,t,"SELECT p.root_key,p.journal,MAX(c.cut_usn,COALESCE((SELECT MAX(old.cut_usn) FROM local_parent_reconcile_plans old WHERE old.scope_id=p.scope_id AND old.status=3),0)),p.start_usn FROM local_parent_reconcile_plans p JOIN local_coherence_roots c ON c.coherence_id=p.coherence_id WHERE p.plan_id=$id AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_rename r WHERE r.plan_id=$id AND r.major=$major AND r.wire=$wire)"))
        {q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$major",record.MajorVersion);q.Parameters.AddWithValue("$wire",record.FileIdHex);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))return;root=r.GetString(0);journal=r.GetString(1);after=r.GetInt64(2);end=r.GetInt64(3);}
        // The OLD side can have a proven Outside disposition while its NEW
        // side enters the root. Recover the actual retained native record,
        // never synthesize an OLD from current membership or a previous NEW.
        await using var find=SyncCommand(c,t,"SELECT usn,payload FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn>=$after AND usn<$usn AND substr(payload,17,$length)=$wire ORDER BY usn DESC LIMIT 1");
        find.Parameters.AddWithValue("$root",root);find.Parameters.AddWithValue("$journal",journal);find.Parameters.AddWithValue("$after",after);find.Parameters.AddWithValue("$usn",record.Usn);find.Parameters.AddWithValue("$length",record.MajorVersion==3?16:8);find.Parameters.AddWithValue("$wire",Convert.FromHexString(record.FileIdHex));
        LocalUsnRecord? prior=null;await using(var r=await find.ExecuteReaderAsync(ct))if(await r.ReadAsync(ct))prior=ProjectionRecord(journal,r.GetInt64(0),end,(byte[])r.GetValue(1));
        if(prior is not null&&prior.MajorVersion==record.MajorVersion&&prior.FileIdHex==record.FileIdHex&&(prior.Reason&0x80003000)==0x1000)await RetainParentRename(c,t,id,prior,ct);
    }
    private static async Task CarryParentRenameClose(SqliteConnection c,SqliteTransaction t,string id,LocalUsnRecord record,CancellationToken ct)
    {
        if((record.Reason&0x80003000)!=0x80002000)return;
        // NTFS may deliver the cumulative NEW|CLOSE after an installed cut.
        // Reuse only the actual completed ordered pair from that scope's
        // retained previous plan. A bare NEW, missing pair or changed parent
        // still fails in RetainParentRename; current names prove none of these.
        await using var q=SyncCommand(c,t,"INSERT INTO local_parent_reconcile_rename(plan_id,wire,major,state,old_parent,new_parent,last_usn) SELECT $id,r.wire,r.major,r.state,r.old_parent,r.new_parent,r.last_usn FROM local_parent_reconcile_rename r JOIN local_parent_reconcile_plans p ON p.plan_id=r.plan_id JOIN local_parent_reconcile_plans n ON n.plan_id=$id AND n.scope_id=p.scope_id AND n.coherence_id=p.coherence_id AND n.journal=p.journal WHERE p.status=3 AND r.state=2 AND r.wire=$wire AND r.major=$major AND r.new_parent=$parent AND r.last_usn<$usn AND p.cut_usn<=n.start_usn AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_rename current WHERE current.plan_id=$id AND current.wire=$wire AND current.major=$major) ORDER BY p.cut_usn DESC,r.last_usn DESC LIMIT 1");
        q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$wire",record.FileIdHex);q.Parameters.AddWithValue("$major",record.MajorVersion);q.Parameters.AddWithValue("$parent",record.ParentIdHex);q.Parameters.AddWithValue("$usn",record.Usn);await q.ExecuteNonQueryAsync(ct);
    }
    private static async Task RetainParentRename(SqliteConnection c,SqliteTransaction t,string id,LocalUsnRecord record,CancellationToken ct)
    {
        var bits=record.Reason&0x3000;var close=(record.Reason&0x80000000)!=0;int state=0;string? old=null,current=null;long last=-1;
        await using(var q=SyncCommand(c,t,"SELECT state,old_parent,new_parent,last_usn FROM local_parent_reconcile_rename WHERE plan_id=$id AND major=$major AND wire=$wire")){q.Parameters.AddWithValue("$id",id);q.Parameters.AddWithValue("$major",record.MajorVersion);q.Parameters.AddWithValue("$wire",record.FileIdHex);await using var r=await q.ExecuteReaderAsync(ct);if(await r.ReadAsync(ct)){state=r.GetInt32(0);old=r.GetString(1);current=r.IsDBNull(2)?null:r.GetString(2);last=r.GetInt64(3);}}
        if(record.Usn<=last||bits==0x3000)throw new LocalSourceGapException("ParentRenameOrder","Rename order/coalesced both sides is ambiguous; no names inferred.");
        if(bits==0x1000)
        {
            if(close&&state==1&&old==record.ParentIdHex)return; // cumulative CLOSE of the retained same OLD side
            if(close||state==1||state==2&&current!=record.ParentIdHex)throw new LocalSourceGapException("ParentRenameHalfMissing","New rename lacks an ordered prior current parent/OLD transition.");
            old=record.ParentIdHex;current=null;state=1;
        }
        else
        {
            if(close&&state==2&&current==record.ParentIdHex)return; // actual native 0x80002000 repeats the prior NEW reason
            if(state!=1||old is null)throw new LocalSourceGapException("ParentRenameHalfMissing","NEW side lacks retained earlier OLD side for the same native identity.");
            current=record.ParentIdHex;state=2;
        }
        await using var write=SyncCommand(c,t,"INSERT INTO local_parent_reconcile_rename VALUES($id,$wire,$major,$state,$old,$new,$usn) ON CONFLICT(plan_id,major,wire) DO UPDATE SET state=excluded.state,old_parent=excluded.old_parent,new_parent=excluded.new_parent,last_usn=excluded.last_usn");write.Parameters.AddWithValue("$id",id);write.Parameters.AddWithValue("$wire",record.FileIdHex);write.Parameters.AddWithValue("$major",record.MajorVersion);write.Parameters.AddWithValue("$state",state);write.Parameters.AddWithValue("$old",old);write.Parameters.AddWithValue("$new",(object?)current??DBNull.Value);write.Parameters.AddWithValue("$usn",record.Usn);await write.ExecuteNonQueryAsync(ct);
    }
}
