using System.Diagnostics;
using System.Text;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private long _coherenceTransactions,_coherenceMaxChanges,_coherenceMaxCommit;
    public LocalCoherenceMetrics GetLocalCoherenceMetrics()=>new(Interlocked.Read(ref _coherenceTransactions),Interlocked.Read(ref _coherenceMaxChanges),Interlocked.Read(ref _coherenceMaxCommit));
    private sealed record CoherenceRow(LocalCoherenceState State,string Root,LocalCoherenceAdmission Admission);
    private static void CoherenceId(SqliteCommand q,string id)=>q.Parameters.AddWithValue("$coherence",id);
    private static LocalJournalAdmission CoherenceBudget(LocalJournalAdmission local,LocalCoherenceAdmission p)=>new(local.MaximumInboxBytes,Math.Max(local.MinimumFreeDiskBytes,p.MinimumFreeDiskBytes),Math.Min(local.MaximumDatabaseBytes,p.MaximumProfileBytes));
    private async Task<SqliteConnection> OpenCoherenceAsync(CancellationToken ct){if(await PreflightCoherenceAsync(ct)!=1)throw new InvalidDataException("Coherence module is absent.");return await OpenLocalSourceConnectionAsync(ct);}
    private static async Task<CoherenceRow> ReadCoherenceAsync(SqliteConnection c,SqliteTransaction t,string id,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,"SELECT candidate_id,status,journal_id,start_usn,cut_usn,applied_usn,owner_epoch,legacy_revision,gap_epoch,matched,verified,dirty_count,unresolved_count,outside_count,record_count,bytes,error,root_key,max_bytes,profile_cap,free_floor FROM local_coherence_roots WHERE coherence_id=$coherence");CoherenceId(q,id);
        await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))throw new InvalidOperationException("Coherence scope is absent.");
        return new(new(id,r.GetString(0),(LocalCoherenceStatus)r.GetInt32(1),r.GetString(2),r.GetInt64(3),r.GetInt64(4),r.GetInt64(5),r.GetInt64(6),r.GetInt64(7),r.GetInt64(8),r.GetInt64(9),r.GetInt64(10),r.GetInt64(11),r.GetInt64(12),r.GetInt64(13),r.GetInt64(14),r.GetInt64(15),r.IsDBNull(16)?null:r.GetString(16)),r.GetString(17),new(r.GetInt64(18),r.GetInt64(19),r.GetInt64(20)));
    }
    private static async Task<CoherenceRow> RequireCoherenceAsync(SqliteConnection c,SqliteTransaction t,string root,string owner,string id,bool terminal,CancellationToken ct)
    {
        var local=await ReadLocalStateAsync(c,t,root,owner,ct);var row=await ReadCoherenceAsync(c,t,id,ct);
        if(row.Root!=root||row.State.OwnerEpoch!=local.Cursor.OwnerEpoch)throw new InvalidOperationException("Coherence owner/generation changed; recovery required.");
        if(!terminal)
        {
            await RequireLocalMappingAsync(c,t,local.Binding,ct);
            if(!local.Cursor.Valid||row.State.JournalId!=local.Cursor.JournalIdHex||row.State.GapEpoch!=local.GapEpoch
                ||row.State.LegacyRevision!=local.LegacyRevision||local.LegacyRevision!=await LocalRevisionAsync(c,t,ct))throw new LocalSourceGapException("CoherenceContinuityGap","Coherence cannot use stale local continuity/revision.");
            await using var q=SyncCommand(c,t,"SELECT 1 FROM local_coherence_active a JOIN local_namespace_active n ON n.candidate_id=$candidate WHERE a.root_key=$root AND a.coherence_id=$coherence");CoherenceId(q,id);q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$candidate",row.State.CandidateId);
            if(row.State.Status==LocalCoherenceStatus.Superseded||await q.ExecuteScalarAsync(ct)is null)throw new InvalidOperationException("A superseded coherence/candidate cannot accept work.");
        }
        return row;
    }
    private async Task<long> CommitCoherence(SqliteConnection c,SqliteTransaction t,long before,CancellationToken ct)
    {var changes=await NamespaceChanges(c,t,ct)-before;var timer=Stopwatch.StartNew();await t.CommitAsync(ct);Interlocked.Increment(ref _coherenceTransactions);UpdateBootstrapMaximum(ref _coherenceMaxChanges,changes);UpdateBootstrapMaximum(ref _coherenceMaxCommit,timer.Elapsed.Ticks);return changes;}
    private async Task<CoherenceRow> CoherenceAdmission(string root,string owner,string id,LocalJournalAdmission local,int rows,CancellationToken ct)
    {await using var c=await OpenCoherenceAsync(ct);using var t=c.BeginTransaction(deferred:true);var row=await RequireCoherenceAsync(c,t,root,owner,id,false,ct);await t.CommitAsync(ct);await CheckLocalAdmissionAsync(CoherenceBudget(local,row.Admission),0,NamespaceReserve(rows),ct);return row;}
    internal async Task<LocalCoherenceState> BeginOwnedCoherenceAsync(string root,string owner,string candidate,LocalJournalAdmission local,LocalCoherenceAdmission admission,CancellationToken ct)
    {
        if(admission.MaximumEvidenceBytes is <1024 or >1024L*1024*1024||admission.MaximumProfileBytes is <1024*1024 or >16L*1024*1024*1024||admission.MinimumFreeDiskBytes<0)throw new ArgumentOutOfRangeException(nameof(admission));
        await EnsureCoherenceAsync(CoherenceBudget(local,admission),ct);await CheckLocalAdmissionAsync(CoherenceBudget(local,admission),1024,NamespaceReserve(12),ct);
        await using var c=await OpenCoherenceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var source=await ReadLocalStateAsync(c,t,root,owner,ct);
        var ns=await RequireNamespaceAsync(c,t,root,owner,candidate,false,ct);
        if(ns.State.ObservedIngestStart!=source.Cursor.NextReadUsn)throw new InvalidOperationException("Begin coherence before any newer ingest changes the candidate start.");
        await using(var count=SyncCommand(c,t,"SELECT COUNT(*) FROM (SELECT 1 FROM local_coherence_roots LIMIT 17)"))if(Convert.ToInt64(await count.ExecuteScalarAsync(ct))>=16)throw new LocalSourceGapException("CoherenceCapacity","Retire old coherence scopes before creating more.");
        await using(var quota=SyncCommand(c,t,"SELECT bytes FROM local_coherence_format WHERE singleton=1"))if(Convert.ToInt64(await quota.ExecuteScalarAsync(ct))>admission.MaximumEvidenceBytes-1024)throw new LocalSourceGapException("CoherenceCapacity","Retained scope logical quota is exhausted.");
        var id=Guid.NewGuid().ToString("N");await using(var old=SyncCommand(c,t,"UPDATE local_coherence_roots SET status=3,error='Fresh candidate superseded previous membership authority.' WHERE coherence_id IN(SELECT coherence_id FROM local_coherence_active WHERE root_key=$root)")){old.Parameters.AddWithValue("$root",root);await old.ExecuteNonQueryAsync(ct);}
        await using(var q=SyncCommand(c,t,"INSERT INTO local_coherence_roots(coherence_id,root_key,candidate_id,owner_epoch,journal_id,start_usn,applied_usn,legacy_revision,gap_epoch,status,bytes,max_bytes,profile_cap,free_floor) VALUES($coherence,$root,$candidate,$epoch,$journal,$start,$start,$revision,$gap,0,1024,$max,$profile,$floor);INSERT INTO local_coherence_active VALUES($root,$coherence) ON CONFLICT(root_key) DO UPDATE SET coherence_id=$coherence;UPDATE local_coherence_format SET bytes=bytes+1024 WHERE singleton=1"))
        {CoherenceId(q,id);q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$candidate",candidate);q.Parameters.AddWithValue("$epoch",source.Cursor.OwnerEpoch);q.Parameters.AddWithValue("$journal",source.Cursor.JournalIdHex);q.Parameters.AddWithValue("$start",source.Cursor.NextReadUsn);q.Parameters.AddWithValue("$revision",source.LegacyRevision);q.Parameters.AddWithValue("$gap",source.GapEpoch);q.Parameters.AddWithValue("$max",admission.MaximumEvidenceBytes);q.Parameters.AddWithValue("$profile",admission.MaximumProfileBytes);q.Parameters.AddWithValue("$floor",admission.MinimumFreeDiskBytes);await q.ExecuteNonQueryAsync(ct);}
        var result=(await ReadCoherenceAsync(c,t,id,ct)).State;await CommitCoherence(c,t,before,ct);return result;
    }
    private static async Task AddCoherenceNode(SqliteConnection c,SqliteTransaction t,string id,string node,string wire,long created,int kind,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,"INSERT INTO local_coherence_nodes VALUES($coherence,$node,$wire,$created,$kind) ON CONFLICT(coherence_id,node_id) DO NOTHING;SELECT wire64,created,kind FROM local_coherence_nodes WHERE coherence_id=$coherence AND node_id=$node");CoherenceId(q,id);q.Parameters.AddWithValue("$node",node);q.Parameters.AddWithValue("$wire",wire);q.Parameters.AddWithValue("$created",created);q.Parameters.AddWithValue("$kind",kind);
        await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)||r.GetString(0)!=wire||r.GetInt64(1)!=created||r.GetInt32(2)!=kind)throw new InvalidDataException("Native alias/incarnation was rebound.");
    }
    internal async Task RecordCoherenceParentAsync(string root,string owner,string id,LocalNamespaceDirectory parent,LocalNamespaceHandleProof proof,LocalJournalAdmission local,CancellationToken ct)
    {
        ValidateNamespaceProof(proof);await CoherenceAdmission(root,owner,id,local,6,ct);await using var c=await OpenCoherenceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var row=await RequireCoherenceAsync(c,t,root,owner,id,false,ct);
        if(row.State.Status!=LocalCoherenceStatus.Building||row.State.CandidateId!=parent.CandidateId)throw new InvalidOperationException("Only current building namespace accepts native parent aliases.");
        await RequireNamespaceParent(c,t,parent,1,ct);var ns=await ReadNamespaceAsync(c,t,parent.CandidateId,ct);
        if(proof.FileId128!=parent.FileId128||proof.CreationTime!=parent.CreationTime||proof.VolumeSerial64!=ns.Serial64)throw new InvalidDataException("Coherence parent/native binding changed.");
        bool exists;await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_coherence_nodes WHERE coherence_id=$coherence AND node_id=$node")){CoherenceId(q,id);q.Parameters.AddWithValue("$node",parent.FileId128);exists=await q.ExecuteScalarAsync(ct)is not null;}
        await AddCoherenceNode(c,t,id,proof.FileId128,proof.LegacyFileId64,proof.CreationTime,1,ct);
        await using(var q=SyncCommand(c,t,"INSERT INTO local_coherence_parents VALUES($coherence,$node,$claim,0,0)")){CoherenceId(q,id);q.Parameters.AddWithValue("$node",parent.FileId128);q.Parameters.AddWithValue("$claim",parent.ClaimToken);await q.ExecuteNonQueryAsync(ct);}
        await AddCoherenceBytes(c,t,row,exists?96:352,ct);await CommitCoherence(c,t,before,ct);
    }
    private static async Task AddCoherenceBytes(SqliteConnection c,SqliteTransaction t,CoherenceRow row,long bytes,CancellationToken ct)
    {
        await using(var q=SyncCommand(c,t,"SELECT bytes FROM local_coherence_format WHERE singleton=1"))if(Convert.ToInt64(await q.ExecuteScalarAsync(ct))>row.Admission.MaximumEvidenceBytes-bytes)throw new LocalSourceGapException("CoherenceCapacity","Retained membership evidence exceeds its logical quota.");
        await using(var q=SyncCommand(c,t,"UPDATE local_coherence_roots SET bytes=bytes+$bytes WHERE coherence_id=$coherence;UPDATE local_coherence_format SET bytes=bytes+$bytes WHERE singleton=1")){CoherenceId(q,row.State.CoherenceId);q.Parameters.AddWithValue("$bytes",bytes);await q.ExecuteNonQueryAsync(ct);}
    }
    internal async Task<long> StageCoherenceAliasesAsync(string root,string owner,string id,LocalNamespaceDirectory parent,IReadOnlyList<LocalDirectoryAlias> aliases,LocalJournalAdmission local,CancellationToken ct)
    {
        if(aliases.Count is <1||aliases.Count>NamespaceStageCapacity)throw new ArgumentOutOfRangeException(nameof(aliases));foreach(var a in aliases)WindowsDirectoryAliasParser.Validate(a);
        await CoherenceAdmission(root,owner,id,local,aliases.Count*2+4,ct);await using var c=await OpenCoherenceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var row=await RequireCoherenceAsync(c,t,root,owner,id,false,ct);
        if(row.State.CandidateId!=parent.CandidateId||row.State.Status!=LocalCoherenceStatus.Building)throw new InvalidOperationException("Native alias batch belongs to a stale/nonbuilding candidate.");await RequireNamespaceParent(c,t,parent,1,ct);
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_coherence_parents WHERE coherence_id=$coherence AND node_id=$node AND claim=$claim AND state=0")){CoherenceId(q,id);q.Parameters.AddWithValue("$node",parent.FileId128);q.Parameters.AddWithValue("$claim",parent.ClaimToken);if(await q.ExecuteScalarAsync(ct)is null)throw new InvalidOperationException("Coherence parent claim changed.");}
        long bytes=0;
        foreach(var alias in aliases)
        {
            long entry;string node;long created;int kind;
            await using(var q=SyncCommand(c,t,"SELECT entry_id,node_id,created,kind,name FROM local_namespace_entries WHERE candidate_id=$candidate AND parent_id=$parent AND canonical=$canonical"))
            {q.Parameters.AddWithValue("$candidate",parent.CandidateId);q.Parameters.AddWithValue("$parent",parent.FileId128);q.Parameters.AddWithValue("$canonical",WebDavSyncClient.CanonicalPathKey(alias.Name));await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)||r.GetString(4)!=alias.Name)throw new InvalidDataException("Native ID64 raw child name differs from the original ID128 pass.");entry=r.GetInt64(0);node=r.GetString(1);created=r.GetInt64(2);kind=r.GetInt32(3);}
            if(created!=alias.CreationTime||kind!=(alias.IsDirectory?1:0))throw new InvalidDataException("Native alias passes disagree on child incarnation/type.");
            bool exists;await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_coherence_nodes WHERE coherence_id=$coherence AND node_id=$node")){CoherenceId(q,id);q.Parameters.AddWithValue("$node",node);exists=await q.ExecuteScalarAsync(ct)is not null;}
            await AddCoherenceNode(c,t,id,node,alias.FileId64,created,kind,ct);
            await using(var q=SyncCommand(c,t,"INSERT INTO local_coherence_matches VALUES($coherence,$entry)")){CoherenceId(q,id);q.Parameters.AddWithValue("$entry",entry);await q.ExecuteNonQueryAsync(ct);}
            bytes+=exists?64:320;
        }
        await using(var q=SyncCommand(c,t,"UPDATE local_coherence_parents SET matched=matched+$count WHERE coherence_id=$coherence AND node_id=$node;UPDATE local_coherence_roots SET matched=matched+$count WHERE coherence_id=$coherence")){CoherenceId(q,id);q.Parameters.AddWithValue("$node",parent.FileId128);q.Parameters.AddWithValue("$count",aliases.Count);await q.ExecuteNonQueryAsync(ct);}
        await AddCoherenceBytes(c,t,row,bytes,ct);return await CommitCoherence(c,t,before,ct);
    }
    internal async Task CompleteCoherenceParentAsync(string root,string owner,string id,LocalNamespaceDirectory parent,LocalJournalAdmission local,CancellationToken ct)
    {
        await CoherenceAdmission(root,owner,id,local,4,ct);await using var c=await OpenCoherenceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var row=await RequireCoherenceAsync(c,t,root,owner,id,false,ct);
        if(row.State.Status!=LocalCoherenceStatus.Building||row.State.CandidateId!=parent.CandidateId)throw new InvalidOperationException("Parent completion is fenced.");await RequireNamespaceParent(c,t,parent,1,ct);
        await using(var q=SyncCommand(c,t,"SELECT p.matched,n.entries FROM local_coherence_parents p JOIN local_namespace_parents n ON n.candidate_id=$candidate AND n.node_id=p.node_id WHERE p.coherence_id=$coherence AND p.node_id=$node AND p.claim=$claim AND p.state=0")){CoherenceId(q,id);q.Parameters.AddWithValue("$candidate",parent.CandidateId);q.Parameters.AddWithValue("$node",parent.FileId128);q.Parameters.AddWithValue("$claim",parent.ClaimToken);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)||r.GetInt64(0)!=r.GetInt64(1))throw new InvalidDataException("Dual native enumeration has missing/extra children.");}
        await using(var q=SyncCommand(c,t,"UPDATE local_coherence_parents SET state=1 WHERE coherence_id=$coherence AND node_id=$node;UPDATE local_coherence_roots SET verified=verified+1 WHERE coherence_id=$coherence")){CoherenceId(q,id);q.Parameters.AddWithValue("$node",parent.FileId128);await q.ExecuteNonQueryAsync(ct);}await CommitCoherence(c,t,before,ct);
    }
    internal async Task SetCoherenceCutAsync(string root,string owner,string id,LocalJournalBounds bounds,LocalJournalAdmission local,CancellationToken ct)
    {
        WindowsUsnJournalReader.ValidateBounds(bounds);await CoherenceAdmission(root,owner,id,local,2,ct);await using var c=await OpenCoherenceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var row=await RequireCoherenceAsync(c,t,root,owner,id,false,ct);var state=await ReadLocalStateAsync(c,t,root,owner,ct);var ns=await ReadNamespaceAsync(c,t,row.State.CandidateId,ct);
        if(row.State.Status!=LocalCoherenceStatus.Building||row.State.CutUsn!=0||bounds.JournalIdHex!=row.State.JournalId||bounds.NextUsn<row.State.StartUsn||bounds.NextUsn>state.Cursor.NextReadUsn||Math.Max(bounds.FirstUsn,bounds.LowestValidUsn)>row.State.StartUsn
            ||ns.State.Status!=LocalNamespaceStatus.EnumerationComplete||ns.State.Exclusions!=0||row.State.MatchedEntries!=ns.State.Entries||row.State.VerifiedParents!=ns.State.ValidatedParents)
            throw new InvalidDataException("Complete native namespace and observed retained journal boundary are required.");
        await using(var q=SyncCommand(c,t,"UPDATE local_coherence_roots SET cut_usn=$cut WHERE coherence_id=$coherence")){CoherenceId(q,id);q.Parameters.AddWithValue("$cut",bounds.NextUsn);await q.ExecuteNonQueryAsync(ct);}await CommitCoherence(c,t,before,ct);
    }
    internal async Task<LocalCoherenceState> ReadOwnedCoherenceAsync(string root,string owner,string id,CancellationToken ct)
    {
        await using var c=await OpenCoherenceAsync(ct);using var t=c.BeginTransaction(deferred:false);var row=await RequireCoherenceAsync(c,t,root,owner,id,true,ct);var source=await ReadLocalStateAsync(c,t,root,owner,ct);
        if(row.State.Status!=LocalCoherenceStatus.Superseded&&(!source.Cursor.Valid||source.LegacyRevision!=row.State.LegacyRevision||source.LegacyRevision!=await LocalRevisionAsync(c,t,ct)||source.GapEpoch!=row.State.GapEpoch||source.Cursor.JournalIdHex!=row.State.JournalId))
        {await using var q=SyncCommand(c,t,"UPDATE local_coherence_roots SET status=2,error=COALESCE(error,'Local continuity/revision gap; membership qualification lost.') WHERE coherence_id=$coherence");CoherenceId(q,id);await q.ExecuteNonQueryAsync(ct);}
        row=await ReadCoherenceAsync(c,t,id,ct);await t.CommitAsync(ct);return row.State;
    }
    internal async Task MarkOwnedCoherenceAsync(string root,string owner,string id,string error,bool supersede,CancellationToken ct)
    {
        if(error.Length>1024)error=error[..1024];await using var c=await OpenCoherenceAsync(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCoherenceAsync(c,t,root,owner,id,true,ct);
        await using(var q=SyncCommand(c,t,"UPDATE local_coherence_roots SET status=$state,error=$error WHERE coherence_id=$coherence AND status<>3")){CoherenceId(q,id);q.Parameters.AddWithValue("$state",supersede?3:2);q.Parameters.AddWithValue("$error",error);await q.ExecuteNonQueryAsync(ct);}
        if(supersede){await using var q=SyncCommand(c,t,"DELETE FROM local_coherence_active WHERE coherence_id=$coherence");CoherenceId(q,id);await q.ExecuteNonQueryAsync(ct);}await t.CommitAsync(ct);
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task<LocalCoherenceState> BeginCoherenceAsync(string candidate,LocalCoherenceAdmission admission,CancellationToken ct)=>SerialAsync(()=>_store.BeginOwnedCoherenceAsync(_root,_owner,candidate,_admission,admission,ct),ct);
    internal Task RecordCoherenceParentAsync(string id,LocalNamespaceDirectory parent,LocalNamespaceHandleProof proof,CancellationToken ct)=>SerialAsync(async()=>{await _store.RecordCoherenceParentAsync(_root,_owner,id,parent,proof,_admission,ct);return true;},ct);
    internal Task<long> StageCoherenceAliasesAsync(string id,LocalNamespaceDirectory parent,IReadOnlyList<LocalDirectoryAlias> aliases,CancellationToken ct)=>SerialAsync(()=>_store.StageCoherenceAliasesAsync(_root,_owner,id,parent,aliases,_admission,ct),ct);
    internal Task CompleteCoherenceParentAsync(string id,LocalNamespaceDirectory parent,CancellationToken ct)=>SerialAsync(async()=>{await _store.CompleteCoherenceParentAsync(_root,_owner,id,parent,_admission,ct);return true;},ct);
    internal Task SetCoherenceCutAsync(string id,LocalJournalBounds bounds,CancellationToken ct)=>SerialAsync(async()=>{await _store.SetCoherenceCutAsync(_root,_owner,id,bounds,_admission,ct);return true;},ct);
    public Task<LocalCoherenceState> ReadCoherenceAsync(string id,CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.ReadOwnedCoherenceAsync(_root,_owner,id,cancellationToken),cancellationToken);
    public Task MarkCoherenceUncertainAsync(string id,string error,CancellationToken cancellationToken=default)=>SerialAsync(async()=>{await _store.MarkOwnedCoherenceAsync(_root,_owner,id,error,false,cancellationToken);return true;},cancellationToken);
    public Task SupersedeCoherenceAsync(string id,string reason,CancellationToken cancellationToken=default)=>SerialAsync(async()=>{await _store.MarkOwnedCoherenceAsync(_root,_owner,id,reason,true,cancellationToken);return true;},cancellationToken);
}
