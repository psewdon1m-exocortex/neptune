using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    internal async Task<LocalCoherenceRetirement> RetireCoherenceInboxAsync(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct,bool stopAtPin=false)
    {
        var limit=NamespaceMutationCapacity/2;await CoherenceAdmission(root,owner,id,local,limit*2+4,ct);await using var c=await OpenCoherenceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var row=await RequireCoherenceAsync(c,t,root,owner,id,false,ct);
        var records=new List<(long Usn,long Bytes,string Hash)>(limit);await using(var q=SyncCommand(c,t,"SELECT usn,byte_count,payload FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn>=$start AND usn<$end ORDER BY usn LIMIT $limit")){q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",row.State.JournalId);q.Parameters.AddWithValue("$start",row.State.StartUsn);q.Parameters.AddWithValue("$end",row.State.AppliedUsn);q.Parameters.AddWithValue("$limit",limit);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))records.Add((r.GetInt64(0),r.GetInt64(1),ProjectionHash((byte[])r.GetValue(2))));}
        if(records.Count==0){await t.CommitAsync(ct);return new(0,0,false,0);}
        if(records.Count>0)
        {
            // Interval arithmetic is only a retirement pin predicate, never an
            // invented journal cursor. AppliedUsn retains its OS/subpage proof.
            if(!await RequireNoCoherenceInboxPin(c,t,root,row.State.JournalId,records[0].Usn,checked(records[^1].Usn+1),ct,stopAtPin)){await t.CommitAsync(ct);return new(0,0,false,0);}
            if(await PreflightProjectionAsync(ct)!=0)
            {await using var q=SyncCommand(c,t,"SELECT 1 FROM local_projection_roots WHERE root_key=$root AND journal_id=$journal AND start_usn<$end AND applied_usn<=$last LIMIT 1");q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",row.State.JournalId);q.Parameters.AddWithValue("$end",checked(records[^1].Usn+1));q.Parameters.AddWithValue("$last",records[^1].Usn);if(await q.ExecuteScalarAsync(ct)is not null){if(stopAtPin){await t.CommitAsync(ct);return new(0,0,false,0);}throw new InvalidOperationException("Unapplied projection still pins this journal prefix.");}}
        }
        var dispositions=new List<(long Usn,long Bytes)>(limit);
        foreach(var item in records)
        {
            int kind;long size;
            await using(var q=SyncCommand(c,t,"SELECT sha256,kind,bytes FROM local_coherence_dispositions WHERE coherence_id=$coherence AND usn=$usn"))
            {CoherenceId(q,id);q.Parameters.AddWithValue("$usn",item.Usn);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)||r.GetString(0)!=item.Hash)throw new InvalidDataException("Retired record lacks exact durable membership disposition.");kind=r.GetInt32(1);size=r.GetInt64(2);}
            if(kind==2)
            {
                // UNKNOWN remains UNKNOWN, with its identical raw evidence.
                // Parent reconciliation consumes this retained copy; deleting
                // a duplicate inbox record must never destroy that proof.
                await using var q=SyncCommand(c,t,"SELECT payload,sha256 FROM local_coherence_unresolved WHERE coherence_id=$coherence AND usn=$usn");CoherenceId(q,id);q.Parameters.AddWithValue("$usn",item.Usn);await using var r=await q.ExecuteReaderAsync(ct);
                if(!await r.ReadAsync(ct)||r.GetString(1)!=item.Hash||ProjectionHash((byte[])r.GetValue(0))!=item.Hash)throw new InvalidDataException("Unresolved raw evidence does not match the retired inbox record.");
            }
            else dispositions.Add((item.Usn,size));
        }
        long bytes=0;foreach(var item in records){ct.ThrowIfCancellationRequested();await using var q=SyncCommand(c,t,"DELETE FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn=$usn");q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",row.State.JournalId);q.Parameters.AddWithValue("$usn",item.Usn);await q.ExecuteNonQueryAsync(ct);bytes+=item.Bytes;}
        await using(var q=SyncCommand(c,t,"UPDATE local_source_roots SET inbox_records=inbox_records-$count,inbox_bytes=inbox_bytes-$bytes,retired_through=MAX(retired_through,$last) WHERE root_key=$root;UPDATE local_source_format SET inbox_records=inbox_records-$count,inbox_bytes=inbox_bytes-$bytes WHERE singleton=1")){q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$count",records.Count);q.Parameters.AddWithValue("$bytes",bytes);q.Parameters.AddWithValue("$last",records.Count==0?0:records[^1].Usn);await q.ExecuteNonQueryAsync(ct);}
        long dispositionBytes=0;foreach(var item in dispositions){await using var q=SyncCommand(c,t,"DELETE FROM local_coherence_dispositions WHERE coherence_id=$coherence AND usn=$usn");CoherenceId(q,id);q.Parameters.AddWithValue("$usn",item.Usn);await q.ExecuteNonQueryAsync(ct);dispositionBytes+=item.Bytes;}
        await using(var q=SyncCommand(c,t,"UPDATE local_coherence_roots SET bytes=bytes-$bytes WHERE coherence_id=$coherence;UPDATE local_coherence_format SET bytes=bytes-$bytes WHERE singleton=1")){CoherenceId(q,id);q.Parameters.AddWithValue("$bytes",dispositionBytes);await q.ExecuteNonQueryAsync(ct);}
        // Last replay's digest/counters and all dirty generations are stored
        // on the coherence root/dirty rows, independently of retired records.
        return new(records.Count,bytes+dispositionBytes,false,await CommitCoherence(c,t,before,ct));
    }
    internal async Task<LocalCoherenceRetirement> RetireOwnedCoherenceAsync(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await using var c=await OpenCoherenceAsync(ct);using var read=c.BeginTransaction(deferred:true);_=await ReadLocalStateAsync(c,read,root,owner,ct);var row=await ReadCoherenceAsync(c,read,id,ct);
        if(row.Root!=root||row.State.Status!=LocalCoherenceStatus.Superseded)throw new InvalidOperationException("Only superseded coherence evidence may retire.");
        if(await PreflightCycleAsync(ct)!=0){await using var pin=SyncCommand(c,read,"SELECT 1 FROM local_cycle_scopes WHERE coherence_id=$coherence LIMIT 1");CoherenceId(pin,id);if(await pin.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Retained cycle evidence pins coherence before any retirement writes.");}await read.CommitAsync(ct);
        var limit=NamespaceMutationCapacity;await CheckLocalAdmissionAsync(CoherenceBudget(local,row.Admission),0,NamespaceReserve(limit+4),ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await ReadLocalStateAsync(c,t,root,owner,ct);row=await ReadCoherenceAsync(c,t,id,ct);
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_coherence_active WHERE coherence_id=$coherence")){CoherenceId(q,id);if(row.State.Status!=LocalCoherenceStatus.Superseded||await q.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Active coherence scope cannot retire.");}
        int phase;await using(var q=SyncCommand(c,t,"SELECT retire_phase FROM local_coherence_roots WHERE coherence_id=$coherence")){CoherenceId(q,id);phase=Convert.ToInt32(await q.ExecuteScalarAsync(ct));}
        var tables=new[]{"local_coherence_dispositions","local_coherence_unresolved","local_coherence_dirty","local_coherence_matches","local_coherence_parents","local_coherence_nodes"};var keys=new List<(object Key,long Bytes)>(limit);
        if(phase<6)
        {
            var columns=phase switch{0=>"usn,bytes",1=>"usn,bytes",2=>"node_id,256",3=>"entry_id,64",4=>"node_id,96",_=>"node_id,256"};await using(var q=SyncCommand(c,t,$"SELECT {columns} FROM {tables[phase]} WHERE coherence_id=$coherence LIMIT $limit")){CoherenceId(q,id);q.Parameters.AddWithValue("$limit",limit);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))keys.Add((r.GetValue(0),r.GetInt64(1)));}
            foreach(var key in keys){var column=phase switch{0 or 1=>"usn",3=>"entry_id",_=>"node_id"};await using var q=SyncCommand(c,t,$"DELETE FROM {tables[phase]} WHERE coherence_id=$coherence AND {column}=$key");CoherenceId(q,id);q.Parameters.AddWithValue("$key",key.Key);await q.ExecuteNonQueryAsync(ct);}
        }
        var removed=phase==6;var bytes=removed?row.State.Bytes:keys.Sum(x=>x.Bytes);
        if(removed){await using var q=SyncCommand(c,t,"DELETE FROM local_coherence_roots WHERE coherence_id=$coherence");CoherenceId(q,id);await q.ExecuteNonQueryAsync(ct);}
        else{await using var q=SyncCommand(c,t,"UPDATE local_coherence_roots SET bytes=bytes-$bytes,retire_phase=retire_phase+$next WHERE coherence_id=$coherence");CoherenceId(q,id);q.Parameters.AddWithValue("$bytes",bytes);q.Parameters.AddWithValue("$next",keys.Count<limit?1:0);await q.ExecuteNonQueryAsync(ct);}
        await using(var q=SyncCommand(c,t,"UPDATE local_coherence_format SET bytes=bytes-$bytes WHERE singleton=1")){q.Parameters.AddWithValue("$bytes",bytes);await q.ExecuteNonQueryAsync(ct);}return new(keys.Count,bytes,removed,await CommitCoherence(c,t,before,ct));
    }
    internal async Task<LocalCoherenceRetirement> RetireCycleAppliedHistory(string root,string owner,string scope,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await CoherenceAdmission(root,owner,id,local,244,ct);await using var c=await OpenCoherenceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);
        var cycle=await RequireCycle(c,t,root,owner,scope,false,ct);var row=await RequireCoherenceAsync(c,t,root,owner,id,false,ct);
        if(cycle.CoherenceId!=id||row.State.Status!=LocalCoherenceStatus.Anchored||!cycle.SeedComplete)throw new InvalidOperationException("Active completed cycle anchor required for applied history retirement.");
        long replayStart;await using(var q=SyncCommand(c,t,"SELECT COALESCE(last_start,start_usn) FROM local_coherence_roots WHERE coherence_id=$coherence")){CoherenceId(q,id);replayStart=Convert.ToInt64(await q.ExecuteScalarAsync(ct));}
        var records=new List<(long Usn,string Hash,byte[]? Raw,long RawBytes,long DispositionBytes)>(120);
        await using(var q=SyncCommand(c,t,"SELECT d.usn,d.sha256,i.payload,COALESCE(i.byte_count,0),d.bytes FROM local_coherence_dispositions d LEFT JOIN local_source_inbox i ON i.root_key=$root AND i.journal_id=$journal AND i.usn=d.usn WHERE d.coherence_id=$coherence AND d.usn<$replay ORDER BY d.usn LIMIT 120"))
        {CoherenceId(q,id);q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",row.State.JournalId);q.Parameters.AddWithValue("$replay",Math.Min(replayStart,row.State.AppliedUsn));await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))records.Add((r.GetInt64(0),r.GetString(1),r.IsDBNull(2)?null:(byte[])r.GetValue(2),r.GetInt64(3),r.GetInt64(4)));}
        // Choose the fixed prefix before inspecting pins. A long unresolved
        // prefix must not turn LIMIT 120 into a scan of the whole history.
        for(var index=records.Count-1;index>=0;index--)
        {await using var pin=SyncCommand(c,t,"SELECT 1 FROM local_coherence_unresolved WHERE coherence_id=$coherence AND usn=$usn");CoherenceId(pin,id);pin.Parameters.AddWithValue("$usn",records[index].Usn);if(await pin.ExecuteScalarAsync(ct)is not null)records.RemoveAt(index);}
        if(records.Count==0){await t.CommitAsync(ct);return new(0,0,false,0);}
        var end=checked(records[^1].Usn+1);await RequireNoCoherenceInboxPin(c,t,root,row.State.JournalId,records[0].Usn,end,ct);
        if(await PreflightProjectionAsync(ct)!=0)
        {await using var q=SyncCommand(c,t,"SELECT 1 FROM local_projection_roots WHERE root_key=$root AND journal_id=$journal AND start_usn<$end AND applied_usn<=$last LIMIT 1");q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",row.State.JournalId);q.Parameters.AddWithValue("$end",end);q.Parameters.AddWithValue("$last",records[^1].Usn);if(await q.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Unapplied projection pins exact applied history.");}
        long rawBytes=0,dispositionBytes=0,last=0;var rawCount=0;
        foreach(var record in records)
        {
            ct.ThrowIfCancellationRequested();if(record.Raw is not null)
            {
                if(ProjectionHash(record.Raw)!=record.Hash)throw new InvalidDataException("Applied disposition does not bind original raw journal evidence.");
                await using var q=SyncCommand(c,t,"DELETE FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn=$usn");q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",row.State.JournalId);q.Parameters.AddWithValue("$usn",record.Usn);rawCount+=await q.ExecuteNonQueryAsync(ct);rawBytes+=record.RawBytes;last=record.Usn;
            }
            await using(var q=SyncCommand(c,t,"DELETE FROM local_coherence_dispositions WHERE coherence_id=$coherence AND usn=$usn")){CoherenceId(q,id);q.Parameters.AddWithValue("$usn",record.Usn);await q.ExecuteNonQueryAsync(ct);}dispositionBytes+=record.DispositionBytes;
        }
        await using(var q=SyncCommand(c,t,"UPDATE local_source_roots SET inbox_records=inbox_records-$count,inbox_bytes=inbox_bytes-$raw,retired_through=MAX(retired_through,$last) WHERE root_key=$root;UPDATE local_source_format SET inbox_records=inbox_records-$count,inbox_bytes=inbox_bytes-$raw WHERE singleton=1;UPDATE local_coherence_roots SET bytes=bytes-$bytes WHERE coherence_id=$coherence;UPDATE local_coherence_format SET bytes=bytes-$bytes WHERE singleton=1"))
        {q.Parameters.AddWithValue("$root",root);CoherenceId(q,id);q.Parameters.AddWithValue("$count",rawCount);q.Parameters.AddWithValue("$raw",rawBytes);q.Parameters.AddWithValue("$last",last);q.Parameters.AddWithValue("$bytes",dispositionBytes);await q.ExecuteNonQueryAsync(ct);}
        // Per-node dirty generations and their last_usn remain untouched. The
        // latest replay window also remains; no new read cursor is invented.
        return new(records.Count,rawBytes+dispositionBytes,false,await CommitCoherence(c,t,before,ct));
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task<LocalCoherenceRetirement> RetireAvailableCoherenceInboxBatchAsync(string id,CancellationToken ct)=>SerialAsync(()=>_store.RetireCoherenceInboxAsync(_root,_owner,id,_admission,ct,true),ct);
    public Task<LocalCoherenceRetirement> RetireCoherenceInboxBatchAsync(string id,CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.RetireCoherenceInboxAsync(_root,_owner,id,_admission,cancellationToken),cancellationToken);
    public Task<LocalCoherenceRetirement> RetireCoherenceBatchAsync(string id,CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.RetireOwnedCoherenceAsync(_root,_owner,id,_admission,cancellationToken),cancellationToken);
    internal Task<LocalCoherenceRetirement> RetireCycleAppliedHistoryAsync(string scope,string id,CancellationToken ct)=>SerialAsync(()=>_store.RetireCycleAppliedHistory(_root,_owner,scope,id,_admission,ct),ct);
}
