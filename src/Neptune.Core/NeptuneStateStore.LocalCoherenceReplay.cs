using System.Text;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private sealed record CoherenceNode(string Id,long Created,int Kind);
    private static async Task<CoherenceNode?> CoherenceNodeAsync(SqliteConnection c,SqliteTransaction t,string id,ushort major,string wire,CancellationToken ct,bool parentVersions=false,CoherenceParentLookup? lookup=null)
    {
        if(major is not(2 or 3))throw new InvalidDataException("Unsupported historical alias version.");
        if(parentVersions)
        {
            lookup??=await ReadCoherenceParentLookup(c,t,id,ct);
            if(lookup.Plans is null||lookup.Plans.Count>0)
            {
                var sql=lookup.Plans is null
                    ?$"SELECT e.node_id,e.created,e.kind FROM local_parent_reconcile_versions v LEFT JOIN local_parent_reconcile_plans p ON p.plan_id=v.pending_plan JOIN local_parent_reconcile_entries e ON e.plan_id=CASE WHEN p.status=3 THEN v.pending_plan ELSE v.current_plan END AND e.parent_id=v.parent_id WHERE v.coherence_id=$coherence AND e.{(major==2?"wire64":"node_id")}=$wire LIMIT 2"
                    :$"SELECT e.node_id,e.created,e.kind FROM local_parent_reconcile_entries e CROSS JOIN local_parent_reconcile_versions v LEFT JOIN local_parent_reconcile_plans p ON p.plan_id=v.pending_plan WHERE e.plan_id IN({string.Join(',',Enumerable.Range(0,lookup.Plans.Count).Select(n=>"$plan"+n))}) AND e.{(major==2?"wire64":"node_id")}=$wire AND v.coherence_id=$coherence AND v.parent_id=e.parent_id AND e.plan_id=CASE WHEN p.status=3 THEN v.pending_plan ELSE v.current_plan END LIMIT 2";
                await using var version=SyncCommand(c,t,sql);CoherenceId(version,id);version.Parameters.AddWithValue("$wire",wire);
                if(lookup.Plans is not null)for(var n=0;n<lookup.Plans.Count;n++)version.Parameters.AddWithValue("$plan"+n,lookup.Plans[n]);
                await using var reader=await version.ExecuteReaderAsync(ct);
                if(await reader.ReadAsync(ct)){var found=new CoherenceNode(reader.GetString(0),reader.GetInt64(1),reader.GetInt32(2));if(await reader.ReadAsync(ct))throw new LocalSourceGapException("ParentAliasAmbiguity","Current version contains multiple hardlink/identity aliases.");return found;}
            }
        }
        await using var q=SyncCommand(c,t,$"SELECT node_id,created,kind FROM local_coherence_nodes WHERE coherence_id=$coherence AND {(major==2?"wire64":"node_id")}=$wire");CoherenceId(q,id);q.Parameters.AddWithValue("$wire",wire);await using var r=await q.ExecuteReaderAsync(ct);return await r.ReadAsync(ct)?new(r.GetString(0),r.GetInt64(1),r.GetInt32(2)):null;
    }
    // Creation/deletion/rename change namespace. Unknown hardlinks may expose
    // an inside link without naming it in the record, so they fence all Outside.
    private const uint NamespaceTransition=0x00013300,DirectoryPolicy=0x00108800;
    internal async Task<LocalCoherenceApply> ApplyOwnedCoherenceAsync(string root,string owner,string id,long expected,LocalJournalAdmission local,CancellationToken ct)
    {
        if(expected<0)throw new ArgumentOutOfRangeException(nameof(expected));
        await using(var prior=await OpenCoherenceAsync(ct))using(var read=prior.BeginTransaction(deferred:true))
        {
            var previous=await RequireCoherenceAsync(prior,read,root,owner,id,false,ct);
            if(previous.State.AppliedUsn!=expected)return await ReplayCoherence(prior,read,id,expected,ct);
        }
        var limit=NamespaceMutationCapacity;await CoherenceAdmission(root,owner,id,local,limit+4,ct);
        await using var c=await OpenCoherenceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var row=await RequireCoherenceAsync(c,t,root,owner,id,false,ct);
        if(row.State.AppliedUsn!=expected)return await ReplayCoherence(c,t,id,expected,ct);
        if(row.State.CutUsn==0)
        {
            await using var observed=SyncCommand(c,t,"SELECT 1 FROM local_coherence_roots WHERE coherence_id=$coherence AND start_usn=0 AND last_start IS NOT NULL AND last_end IS NOT NULL AND last_hash IS NOT NULL");CoherenceId(observed,id);
            if(await observed.ExecuteScalarAsync(ct)is null)throw new InvalidOperationException("Observed complete scan/journal cut is required before classification.");
        }
        var source=await ReadLocalStateAsync(c,t,root,owner,ct);var target=row.State.Status==LocalCoherenceStatus.Building?row.State.CutUsn:source.Cursor.NextReadUsn;
        var retired=source.RetiredThroughUsn>=expected&&expected<target;
        if(retired&&expected==0&&source.RetiredThroughUsn==0)
        {
            // The format's initial retirement watermark is also zero. Only
            // the retained original record can prove that USN 0 was not retired.
            await using var first=SyncCommand(c,t,"SELECT 1 FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn=0");first.Parameters.AddWithValue("$root",root);first.Parameters.AddWithValue("$journal",row.State.JournalId);
            retired=await first.ExecuteScalarAsync(ct)is null;
        }
        if(expected<row.State.StartUsn||expected>target||retired)throw new LocalSourceGapException("CoherencePrefixGap","Required historical prefix was retired or is invalid.");
        var records=new List<(LocalUsnRecord Record,byte[] Raw)>(257);
        await using(var q=SyncCommand(c,t,"SELECT usn,payload FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn>=$start AND usn<$target ORDER BY usn LIMIT 257"))
        {q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",row.State.JournalId);q.Parameters.AddWithValue("$start",expected);q.Parameters.AddWithValue("$target",target);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct)){var raw=(byte[])r.GetValue(1);records.Add((ProjectionRecord(row.State.JournalId,r.GetInt64(0),source.Cursor.NextReadUsn,raw),raw));}}
        var anchored=row.State.Status==LocalCoherenceStatus.Anchored;var uncertain=row.State.Status==LocalCoherenceStatus.Uncertain;var error=row.State.Error;
        var processed=0;var effects=0;var outside=0;var dirty=0;var unknown=0;long bytes=0;var digest=new StringBuilder();
        var rootNode=(await ReadNamespaceAsync(c,t,row.State.CandidateId,ct)).State.RootFileId128;
        var parentVersions=await HasParentModule(c,t,ct);
        var parentLookup=parentVersions?await ReadCoherenceParentLookup(c,t,id,ct):null;
        foreach(var (record,raw) in records.Take(256))
        {
            ct.ThrowIfCancellationRequested();var node=await CoherenceNodeAsync(c,t,id,record.MajorVersion,record.FileIdHex,ct,parentVersions,parentLookup);var parent=await CoherenceNodeAsync(c,t,id,record.MajorVersion,record.ParentIdHex,ct,parentVersions,parentLookup);
            var kind=(record.FileAttributes&0x10)!=0?1:0;var known=node is not null;var typeMismatch=known&&node!.Kind!=kind;
            var touches=known||parent?.Kind==1;
            // An absent managed file can rename back from an outside parent.
            // Its OLD half remains relevant even though the latest membership
            // no longer contains that ID; retain it before inbox retirement.
            if(!touches&&parentVersions&&(record.Reason&0x3000)!=0)
                touches=await IsRetainedCycleNative(c,t,id,record,ct);
            var ambiguous=(record.Reason&0x10000)!=0||touches&&((record.Reason&NamespaceTransition)!=0||(record.Reason&0x100000)!=0||typeMismatch
                ||kind==1&&(record.Reason&DirectoryPolicy)!=0||(record.FileAttributes&(0x400|0x1000|0x40000|0x400000))!=0||node?.Id==rootNode&&(record.Reason&~0x80000000u)!=0);
            // No current filesystem lookup occurs here. Before certification,
            // unknown events have a retained historical-unscoped disposition.
            var isOutside=anchored&&!uncertain&&!ambiguous&&!touches;
            var needsUnknown=!isOutside&&(!known||ambiguous||uncertain);
            var cost=1+(known?1:0)+(needsUnknown?1:0);
            if(effects+cost>limit)break;
            if(ambiguous){uncertain=true;error=typeMismatch?"HistoricalTypeUnresolved":(record.Reason&0x10000)!=0?"HardlinkMembershipUnresolved":"ScopedNamespaceTransitionUnresolved";}
            var sha=ProjectionHash(raw);digest.Append(sha);effects+=cost;processed++;
            if(known)
            {
                bool exists;await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_coherence_dirty WHERE coherence_id=$coherence AND node_id=$node")){CoherenceId(q,id);q.Parameters.AddWithValue("$node",node!.Id);exists=await q.ExecuteScalarAsync(ct)is not null;}
                await using(var q=SyncCommand(c,t,"INSERT INTO local_coherence_dirty VALUES($coherence,$node,$created,$kind,1,$usn,$reasons) ON CONFLICT(coherence_id,node_id) DO UPDATE SET generation=generation+1,last_usn=MAX(last_usn,excluded.last_usn),reasons=reasons|excluded.reasons"))
                {CoherenceId(q,id);q.Parameters.AddWithValue("$node",node!.Id);q.Parameters.AddWithValue("$created",node.Created);q.Parameters.AddWithValue("$kind",node.Kind);q.Parameters.AddWithValue("$usn",record.Usn);q.Parameters.AddWithValue("$reasons",record.Reason);await q.ExecuteNonQueryAsync(ct);}
                if(!exists){dirty++;bytes+=256;}
            }
            if(needsUnknown)
            {
                var code=ambiguous?error!:uncertain?"PriorMembershipUncertainty":row.State.Status==LocalCoherenceStatus.Building?"HistoricalBeforeAnchor":"InsideIdentityUnresolved";
                var costBytes=raw.Length+256L;await using var q=SyncCommand(c,t,"INSERT INTO local_coherence_unresolved VALUES($coherence,$usn,$raw,$hash,$code,$bytes)");CoherenceId(q,id);q.Parameters.AddWithValue("$usn",record.Usn);q.Parameters.AddWithValue("$raw",raw);q.Parameters.AddWithValue("$hash",sha);q.Parameters.AddWithValue("$code",code);q.Parameters.AddWithValue("$bytes",costBytes);await q.ExecuteNonQueryAsync(ct);unknown++;bytes+=costBytes;
            }
            if(isOutside)outside++;
            await using(var q=SyncCommand(c,t,"INSERT INTO local_coherence_dispositions VALUES($coherence,$usn,$hash,$kind,96)")){CoherenceId(q,id);q.Parameters.AddWithValue("$usn",record.Usn);q.Parameters.AddWithValue("$hash",sha);q.Parameters.AddWithValue("$kind",isOutside?0:needsUnknown?2:1);await q.ExecuteNonQueryAsync(ct);}bytes+=96;
        }
        var next=processed<records.Count?records[processed].Record.Usn:target;
        if(next<expected||next>target)throw new LocalSourceGapException("CoherencePrefixGap","Durable record subpage/observed cut boundary is invalid.");
        var status=uncertain?LocalCoherenceStatus.Uncertain:row.State.Status==LocalCoherenceStatus.Building&&next==row.State.CutUsn?LocalCoherenceStatus.Anchored:row.State.Status;
        await AddCoherenceBytes(c,t,row,bytes,ct);
        await using(var q=SyncCommand(c,t,"UPDATE local_coherence_roots SET status=$state,error=$error,applied_usn=$end,dirty_count=dirty_count+$dirty,unresolved_count=unresolved_count+$unknown,outside_count=outside_count+$outside,record_count=record_count+$count,last_start=$start,last_end=$end,last_hash=$hash,last_records=$count,last_dirty=$dirty,last_outside=$outside,last_unknown=$unknown WHERE coherence_id=$coherence"))
        {CoherenceId(q,id);q.Parameters.AddWithValue("$state",(int)status);q.Parameters.AddWithValue("$error",(object?)error??DBNull.Value);q.Parameters.AddWithValue("$end",next);q.Parameters.AddWithValue("$dirty",dirty);q.Parameters.AddWithValue("$unknown",unknown);q.Parameters.AddWithValue("$outside",outside);q.Parameters.AddWithValue("$count",processed);q.Parameters.AddWithValue("$start",expected);q.Parameters.AddWithValue("$hash",LocalHash(digest.ToString()));await q.ExecuteNonQueryAsync(ct);}
        return new(false,processed,dirty,outside,unknown,next,await CommitCoherence(c,t,before,ct));
    }
    private static async Task<LocalCoherenceApply> ReplayCoherence(SqliteConnection c,SqliteTransaction t,string id,long expected,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,"SELECT last_records,last_dirty,last_outside,last_unknown,last_end FROM local_coherence_roots WHERE coherence_id=$coherence AND last_start=$start AND last_end=applied_usn");CoherenceId(q,id);q.Parameters.AddWithValue("$start",expected);await using var r=await q.ExecuteReaderAsync(ct);
        if(!await r.ReadAsync(ct))throw new InvalidOperationException("Expected coherence prefix is stale.");return new(true,r.GetInt32(0),r.GetInt32(1),r.GetInt32(2),r.GetInt32(3),r.GetInt64(4),0);
    }
    internal async Task<IReadOnlyList<LocalCoherenceDirty>> ReadOwnedCoherenceDirty(string root,string owner,string id,long afterUsn,string afterNode,CancellationToken ct)
    {
        if(afterUsn< -1||afterNode.Length>32)throw new ArgumentOutOfRangeException(nameof(afterUsn));await using var c=await OpenCoherenceAsync(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCoherenceAsync(c,t,root,owner,id,true,ct);
        await using var q=SyncCommand(c,t,"SELECT node_id,created,kind,generation,last_usn,reasons FROM local_coherence_dirty WHERE coherence_id=$coherence AND (last_usn>$usn OR last_usn=$usn AND node_id>$node) ORDER BY last_usn,node_id LIMIT 256");CoherenceId(q,id);q.Parameters.AddWithValue("$usn",afterUsn);q.Parameters.AddWithValue("$node",afterNode);var result=new List<LocalCoherenceDirty>();await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))result.Add(new(r.GetString(0),r.GetInt64(1),r.GetInt32(2)!=0,r.GetInt64(3),r.GetInt64(4),checked((uint)r.GetInt64(5))));return result.AsReadOnly();
    }
    internal async Task<IReadOnlyList<LocalCoherenceUnresolved>> ReadOwnedCoherenceUnresolved(string root,string owner,string id,long after,CancellationToken ct)
    {
        await using var c=await OpenCoherenceAsync(ct);using var t=c.BeginTransaction(deferred:true);var row=await RequireCoherenceAsync(c,t,root,owner,id,true,ct);var source=await ReadLocalStateAsync(c,t,root,owner,ct);
        await using var q=SyncCommand(c,t,"SELECT usn,code,payload,sha256 FROM local_coherence_unresolved WHERE coherence_id=$coherence AND usn>$after ORDER BY usn LIMIT 256");CoherenceId(q,id);q.Parameters.AddWithValue("$after",after);var result=new List<LocalCoherenceUnresolved>();await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct)){var raw=(byte[])r.GetValue(2);result.Add(new(r.GetInt64(0),r.GetString(1),ProjectionRecord(row.State.JournalId,r.GetInt64(0),source.Cursor.NextReadUsn,raw),r.GetString(3)));}return result.AsReadOnly();
    }
    private async Task<bool> RequireNoCoherenceInboxPin(SqliteConnection c,SqliteTransaction t,string root,string journal,long first,long end,CancellationToken ct,bool stopAtPin=false)
    {
        if(await PreflightCoherenceAsync(ct)==0)return true;
        await using var q=SyncCommand(c,t,"SELECT 1 FROM local_coherence_roots WHERE root_key=$root AND journal_id=$journal AND start_usn<$end AND applied_usn<=$last LIMIT 1");q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",journal);q.Parameters.AddWithValue("$end",end);q.Parameters.AddWithValue("$last",Math.Max(first,end-1));
        if(await q.ExecuteScalarAsync(ct)is not null){if(stopAtPin)return false;throw new InvalidOperationException("Unapplied coherence window pins this journal prefix.");}
        if(await HasParentModule(c,t,ct))
        {
            await using var pin=SyncCommand(c,t,"SELECT 1 FROM local_cycle_scopes s CROSS JOIN local_parent_reconcile_plans p INDEXED BY local_parent_reconcile_history WHERE s.root_key=$root AND p.scope_id=s.scope_id AND p.root_key=$root AND p.journal=$journal AND p.status<3 AND CASE WHEN p.scan_start_usn>0 THEN p.scan_start_usn ELSE p.start_usn END<=$last LIMIT 1");
            pin.Parameters.AddWithValue("$root",root);pin.Parameters.AddWithValue("$journal",journal);pin.Parameters.AddWithValue("$last",end-1);
            if(await pin.ExecuteScalarAsync(ct)is not null){if(stopAtPin)return false;throw new InvalidOperationException("Pending native parent scan pins this journal interval.");}
        }
        return true;
    }
    private async Task RequireNoCoherenceCandidatePin(SqliteConnection c,SqliteTransaction t,string candidate,CancellationToken ct)
    {
        if(await PreflightCoherenceAsync(ct)==0)return;await using var q=SyncCommand(c,t,"SELECT 1 FROM local_coherence_roots WHERE candidate_id=$candidate LIMIT 1");q.Parameters.AddWithValue("$candidate",candidate);if(await q.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Retire derived coherence evidence before its namespace candidate.");
    }
    private async Task RequireNoCoherenceRootPin(SqliteConnection c,SqliteTransaction t,string root,CancellationToken ct)
    {
        if(await PreflightCoherenceAsync(ct)==0)return;await using var q=SyncCommand(c,t,"SELECT 1 FROM local_coherence_roots WHERE root_key=$root LIMIT 1");q.Parameters.AddWithValue("$root",root);if(await q.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Coherence evidence pins the superseded local subscription.");
    }
}

public sealed partial class LocalSourceJournalSession
{
    public Task<LocalCoherenceApply> ApplyNextCoherenceAsync(string id,long expectedAppliedBoundary,CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.ApplyOwnedCoherenceAsync(_root,_owner,id,expectedAppliedBoundary,_admission,cancellationToken),cancellationToken);
    public Task<IReadOnlyList<LocalCoherenceDirty>> ReadCoherenceDirtyPageAsync(string id,long afterUsn=-1,string afterNode="",CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.ReadOwnedCoherenceDirty(_root,_owner,id,afterUsn,afterNode,cancellationToken),cancellationToken);
    public Task<IReadOnlyList<LocalCoherenceUnresolved>> ReadCoherenceUnresolvedPageAsync(string id,long afterUsn=-1,CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.ReadOwnedCoherenceUnresolved(_root,_owner,id,afterUsn,cancellationToken),cancellationToken);
}
