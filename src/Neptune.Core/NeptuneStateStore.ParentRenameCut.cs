using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    internal async Task<bool> RejectCrossCutParentRename(string root,string owner,string scope,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await ParentAdmission(root,owner,scope,local,2,0,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);
        var before=await NamespaceChanges(c,t,ct);var p=await RequireParentPlan(c,t,root,owner,scope,id,ct);
        if(p.Status!=LocalParentReconciliationStatus.Scanning)return false;
        var source=await ReadLocalStateAsync(c,t,root,owner,ct);
        var rejected=await RejectRetainedCrossCutRename(c,t,id,source.Cursor.NextReadUsn,ct);
        await CommitParent(c,t,before,ct);return rejected;
    }

    // Only supersede an UNINSTALLED candidate. A retained OLD before its scan
    // cut and an actual ordered NEW at/after that cut cannot both seed it.
    // No rename obligation is completed here, and no history, work, baseline or
    // installed membership is changed. The successor must pass every normal
    // native enumeration, hardlink, replay and installation gate afresh.
    private static async Task<bool> RejectRetainedCrossCutRename(SqliteConnection c,SqliteTransaction t,string id,long end,CancellationToken ct)
    {
        string root,journal;long start;
        await using(var q=SyncCommand(c,t,"SELECT root_key,journal,CASE WHEN scan_start_usn>0 THEN scan_start_usn ELSE start_usn END FROM local_parent_reconcile_plans WHERE plan_id=$id AND status=0"))
        {q.Parameters.AddWithValue("$id",id);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))return false;root=r.GetString(0);journal=r.GetString(1);start=r.GetInt64(2);}
        if(end<=start)return false;
        var rows=new List<(string Wire,ushort Major,string Parent,long Usn)>();
        await using(var q=SyncCommand(c,t,"SELECT wire,major,old_parent,last_usn,state FROM local_parent_reconcile_rename WHERE plan_id=$id AND state<>2 ORDER BY major,wire LIMIT 129"))
        {q.Parameters.AddWithValue("$id",id);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct)){if(r.GetInt32(4)!=1||r.IsDBNull(2))return false;rows.Add((r.GetString(0),(ushort)r.GetInt32(1),r.GetString(2),r.GetInt64(3)));}}
        if(rows.Count is <1 or >128)return false;
        var bytes=0;
        foreach(var old in rows)
        {
            if(old.Major is not(2 or 3)||old.Usn>=start)return false;
            var records=new List<LocalUsnRecord>();
            await using(var q=SyncCommand(c,t,"SELECT usn,CASE WHEN length(payload)<=65536 THEN payload ELSE NULL END FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn>=$old AND usn<$end AND substr(payload,17,$length)=$wire ORDER BY usn LIMIT 129"))
            {
                q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",journal);q.Parameters.AddWithValue("$old",old.Usn);q.Parameters.AddWithValue("$end",end);q.Parameters.AddWithValue("$length",old.Major==3?16:8);q.Parameters.AddWithValue("$wire",Convert.FromHexString(old.Wire));
                await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))
                {
                    if(r.IsDBNull(1))return false;var raw=(byte[])r.GetValue(1);bytes+=raw.Length;if(bytes>2*1024*1024)return false;
                    if(!raw.AsSpan().StartsWith("NUSN"u8))return false;
                    records.Add(ProjectionRecord(journal,r.GetInt64(0),end,raw));
                }
            }
            if(records.Count is <2 or >128)return false;
            var first=records[0];
            if(first.Usn!=old.Usn||first.MajorVersion!=old.Major||first.FileIdHex!=old.Wire||first.ParentIdHex!=old.Parent||(first.Reason&0x3000)!=0x1000||first.ReadMode!=LocalJournalReadMode.IdentityOnly)return false;
            var found=false;
            foreach(var record in records.Skip(1))
            {
                if(record.MajorVersion!=old.Major||record.FileIdHex!=old.Wire||record.ReadMode!=LocalJournalReadMode.IdentityOnly||(record.Reason&0x300)!=0)return false;
                var bits=record.Reason&0x3000;
                if(bits==0)continue;
                if(bits==0x1000&&(record.Reason&0x80000000)!=0&&record.ParentIdHex==old.Parent)continue;
                if(bits!=0x2000||record.Usn<start)return false;
                found=true;break;
            }
            if(!found)return false;
        }
        await using var reject=SyncCommand(c,t,"UPDATE local_parent_reconcile_plans SET status=4,error='ParentRenameCrossCut' WHERE plan_id=$id AND status=0");reject.Parameters.AddWithValue("$id",id);
        return await reject.ExecuteNonQueryAsync(ct)==1;
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task<bool> RejectCrossCutParentRenameAsync(string scope,string id,CancellationToken ct)=>SerialAsync(()=>_store.RejectCrossCutParentRename(_root,_owner,scope,id,_admission,ct),ct);
}
