using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    // The complete stable namespace window is certified only after all [S,D)
    // records are processed. This second bounded pass uses that full interval
    // proof; a currently absent path or an isolated candidate cannot call it.
    internal async Task<int> QualifyCoherenceHistoricalOutside(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        var limit=Math.Max(1,NamespaceMutationCapacity/2);await CoherenceAdmission(root,owner,id,local,limit*2+3,ct);
        await using var c=await OpenCoherenceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var row=await RequireCoherenceAsync(c,t,root,owner,id,false,ct);
        if(row.State.Status!=LocalCoherenceStatus.Anchored||row.State.AppliedUsn<row.State.CutUsn)throw new InvalidOperationException("Historical outside requires the certified complete stable [S,D) namespace window.");
        var records=new List<(long Usn,byte[] Raw,string Hash,long Bytes)>(limit);
        await using(var q=SyncCommand(c,t,"SELECT usn,payload,sha256,bytes FROM local_coherence_unresolved WHERE coherence_id=$coherence AND code='HistoricalBeforeAnchor' AND usn>= $start AND usn<$cut ORDER BY usn LIMIT $limit")){CoherenceId(q,id);q.Parameters.AddWithValue("$start",row.State.StartUsn);q.Parameters.AddWithValue("$cut",row.State.CutUsn);q.Parameters.AddWithValue("$limit",limit);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))records.Add((r.GetInt64(0),(byte[])r.GetValue(1),r.GetString(2),r.GetInt64(3)));}
        long bytes=0;var outside=0;
        foreach(var item in records)
        {
            ct.ThrowIfCancellationRequested();var record=ProjectionRecord(row.State.JournalId,item.Usn,row.State.CutUsn,item.Raw);
            var node=await CoherenceNodeAsync(c,t,id,record.MajorVersion,record.FileIdHex,ct);var parent=await CoherenceNodeAsync(c,t,id,record.MajorVersion,record.ParentIdHex,ct);
            if(node is not null||parent is not null||(record.Reason&0x10000)!=0)
            {await using var q=SyncCommand(c,t,"UPDATE local_coherence_unresolved SET code='HistoricalInsideIdentityUnresolved' WHERE coherence_id=$coherence AND usn=$usn");CoherenceId(q,id);q.Parameters.AddWithValue("$usn",item.Usn);await q.ExecuteNonQueryAsync(ct);continue;}
            if(ProjectionHash(item.Raw)!=item.Hash)throw new InvalidDataException("Historical raw evidence hash changed.");
            await using(var q=SyncCommand(c,t,"UPDATE local_coherence_dispositions SET kind=0 WHERE coherence_id=$coherence AND usn=$usn AND sha256=$hash AND kind=2")){CoherenceId(q,id);q.Parameters.AddWithValue("$usn",item.Usn);q.Parameters.AddWithValue("$hash",item.Hash);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidDataException("Historical disposition cannot bind to original evidence.");}
            await using(var q=SyncCommand(c,t,"DELETE FROM local_coherence_unresolved WHERE coherence_id=$coherence AND usn=$usn")){CoherenceId(q,id);q.Parameters.AddWithValue("$usn",item.Usn);await q.ExecuteNonQueryAsync(ct);}outside++;bytes+=item.Bytes;
        }
        await using(var q=SyncCommand(c,t,"UPDATE local_coherence_roots SET unresolved_count=unresolved_count-$count,outside_count=outside_count+$count,bytes=bytes-$bytes WHERE coherence_id=$coherence;UPDATE local_coherence_format SET bytes=bytes-$bytes WHERE singleton=1")){CoherenceId(q,id);q.Parameters.AddWithValue("$count",outside);q.Parameters.AddWithValue("$bytes",bytes);await q.ExecuteNonQueryAsync(ct);}
        await CommitCoherence(c,t,before,ct);return records.Count;
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task<int> QualifyHistoricalCoherenceOutsideAsync(string id,CancellationToken ct)=>SerialAsync(()=>_store.QualifyCoherenceHistoricalOutside(_root,_owner,id,_admission,ct),ct);
}
