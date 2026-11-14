using System.Diagnostics;
using System.Globalization;
using System.Text;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    public const int NamespaceStageLimit=128,NamespaceRowLimit=256;
    // Adapt predictable write amplification to the actual legal page size.
    // Half the outstanding-WAL cap leaves room for existing pinned frames.
    private int NamespaceMutationCapacity=>Math.Min(NamespaceRowLimit,checked((int)Math.Max(2,(LocalWalBudget/2/(Interlocked.Read(ref _localPageSize)+24)-64)/8)));
    public int NamespaceStageCapacity=>Math.Min(NamespaceStageLimit,NamespaceMutationCapacity/2);
    private long _namespaceTransactions,_namespaceMaxChanges,_namespaceMaxCommit;
    public LocalNamespaceMetrics GetLocalNamespaceMetrics()=>new(Interlocked.Read(ref _namespaceTransactions),Interlocked.Read(ref _namespaceMaxChanges),Interlocked.Read(ref _namespaceMaxCommit));
    private long NamespaceReserve(int rows)=>checked((8L*rows+64)*(Interlocked.Read(ref _localPageSize)+24)+32);
    private sealed record NamespaceRow(LocalNamespaceCandidate State,string RootKey,string Mapping,string Client,string Serial64,LocalNamespaceAdmission Admission);
    private static void ValidateNamespaceProof(LocalNamespaceHandleProof p)
    {
        static bool Hex(string s,int length)=>s.Length==length && s==s.ToLowerInvariant() && s.All(char.IsAsciiHexDigit);
        if(!Hex(p.FileId128,32)||p.FileId128==new string('0',32)||!Hex(p.LegacyFileId64,16)||!Hex(p.VolumeSerial64,16)
            ||(p.Attributes&0x10)==0||(p.Attributes&0x400)!=0)throw new InvalidDataException("Namespace parent proof is not exact ordinary-directory metadata.");
    }
    private static LocalJournalAdmission NamespaceBudget(LocalJournalAdmission local,LocalNamespaceAdmission candidate)=>new(local.MaximumInboxBytes,
        Math.Max(local.MinimumFreeDiskBytes,candidate.MinimumFreeDiskBytes),Math.Min(local.MaximumDatabaseBytes,candidate.MaximumProfileBytes));
    private static void AddNamespace(SqliteCommand q,string candidate,string? parent=null)
    {q.Parameters.AddWithValue("$id",candidate);if(parent is not null)q.Parameters.AddWithValue("$parent",parent);}
    private static async Task<NamespaceRow> ReadNamespaceAsync(SqliteConnection c,SqliteTransaction t,string candidate,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,"SELECT root_key,mapping_id,client_id,root_id,owner_epoch,observed_start,legacy_revision,status,entries,bytes,validated,exclusions,eligible_through,error,volume_serial64,max_candidate,max_profile,free_floor FROM local_namespace_candidates WHERE candidate_id=$id");AddNamespace(q,candidate);
        await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))throw new InvalidOperationException("Namespace candidate is absent.");
        return new(new(candidate,(LocalNamespaceStatus)r.GetInt32(7),r.GetString(3),r.GetInt64(4),r.GetInt64(5),r.GetInt64(6),r.GetInt64(8),r.GetInt64(9),r.GetInt64(10),r.GetInt64(11),r.GetInt64(12),r.IsDBNull(13)?null:r.GetString(13)),
            r.GetString(0),r.GetString(1),r.GetString(2),r.GetString(14),new(r.GetInt64(15),r.GetInt64(16),r.GetInt64(17)));
    }
    private static async Task<NamespaceRow> RequireNamespaceAsync(SqliteConnection c,SqliteTransaction t,string root,string owner,string candidate,bool terminal,CancellationToken ct)
    {
        var local=await ReadLocalStateAsync(c,t,root,owner,ct);var row=await ReadNamespaceAsync(c,t,candidate,ct);
        if(row.RootKey!=root || row.State.OwnerEpoch!=local.Cursor.OwnerEpoch || row.Mapping!=local.Binding.MappingId || row.Client!=local.Binding.ClientId)
            throw new InvalidOperationException("Namespace owner/generation binding changed.");
        if(!terminal)
        {
            await RequireLocalMappingAsync(c,t,local.Binding,ct);
            if(!local.Cursor.Valid || local.LegacyRevision!=await LocalRevisionAsync(c,t,ct) || row.State.LegacyRevision!=local.LegacyRevision)
                throw new LocalSourceGapException("NamespaceLegacyGap","Local ingestion/mapping revision cannot qualify this candidate.");
            if(row.State.Status!=LocalNamespaceStatus.Enumerating)throw new InvalidOperationException("Only the active enumerating candidate can accept work.");
            await using var active=SyncCommand(c,t,"SELECT 1 FROM local_namespace_active WHERE candidate_id=$id");AddNamespace(active,candidate);
            if(await active.ExecuteScalarAsync(ct) is null)throw new InvalidOperationException("Namespace candidate was superseded.");
        }
        return row;
    }
    private static async Task<long> NamespaceChanges(SqliteConnection c,SqliteTransaction t,CancellationToken ct)
    {await using var q=SyncCommand(c,t,"SELECT total_changes()");return Convert.ToInt64(await q.ExecuteScalarAsync(ct));}
    private async Task<long> CommitNamespace(SqliteConnection c,SqliteTransaction t,long before,CancellationToken ct)
    {
        var changes=await NamespaceChanges(c,t,ct)-before;var watch=Stopwatch.StartNew();await t.CommitAsync(ct);
        Interlocked.Increment(ref _namespaceTransactions);UpdateBootstrapMaximum(ref _namespaceMaxChanges,changes);UpdateBootstrapMaximum(ref _namespaceMaxCommit,watch.Elapsed.Ticks);return changes;
    }
    private async Task<SqliteConnection> OpenNamespaceAsync(CancellationToken ct)
    {if(await PreflightNamespaceAsync(ct)!=1)throw new InvalidDataException("Namespace module is absent.");return await OpenLocalSourceConnectionAsync(ct);}
    private async Task<NamespaceRow> NamespaceAdmissionAsync(string root,string owner,string id,LocalJournalAdmission local,int rows,CancellationToken ct)
    {
        await using var c=await OpenNamespaceAsync(ct);using var t=c.BeginTransaction(deferred:true);var row=await RequireNamespaceAsync(c,t,root,owner,id,false,ct);await t.CommitAsync(ct);
        await CheckLocalAdmissionAsync(NamespaceBudget(local,row.Admission),0,NamespaceReserve(rows),ct);return row;
    }
    internal async Task<LocalNamespaceCandidate> BeginOwnedNamespaceAsync(string root,string owner,LocalJournalAdmission local,LocalNamespaceRootProof proof,LocalNamespaceAdmission admission,CancellationToken ct)
    {
        ValidateNamespaceProof(proof.Handle);
        if(admission.MaximumCandidateBytes is <1024 or >1024L*1024*1024 || admission.MaximumProfileBytes is <1024*1024 or >16L*1024*1024*1024 || admission.MinimumFreeDiskBytes<0)
            throw new ArgumentOutOfRangeException(nameof(admission));
        var state=await ReadOwnedLocalStateAsync(root,owner,ct);
        if(!state.Cursor.Valid)throw new LocalSourceGapException("NamespaceIngestGap","Namespace candidates require a valid observed ingest subscription.");
        if(proof.RootPath!=Path.TrimEndingDirectorySeparator(state.Binding.SourceRoot) || proof.VolumeGuid!=state.Binding.VolumeGuid || proof.FileSystem!=state.Binding.FileSystem
            ||proof.Handle.FileId128!=state.Binding.RootFileId || proof.Handle.LegacyVolumeSerial.ToString("x8",CultureInfo.InvariantCulture)!=state.Binding.VolumeSerial)
            throw new InvalidDataException("Root handle proof does not match the local subscription.");
        var budget=NamespaceBudget(local,admission);await EnsureNamespaceAsync(budget,ct);await CheckLocalAdmissionAsync(budget,1024,NamespaceReserve(16),ct);
        await using var c=await OpenNamespaceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);
        state=await ReadLocalStateAsync(c,t,root,owner,ct);await RequireLocalMappingAsync(c,t,state.Binding,ct);
        if(!state.Cursor.Valid || state.LegacyRevision!=await LocalRevisionAsync(c,t,ct))throw new LocalSourceGapException("NamespaceLegacyGap","Legacy writes interrupted candidate initialization.");
        await using(var count=SyncCommand(c,t,"SELECT COUNT(*) FROM (SELECT 1 FROM local_namespace_candidates LIMIT 17)"))
            if(Convert.ToInt64(await count.ExecuteScalarAsync(ct))>=16)throw new LocalSourceGapException("NamespaceHistoryCapacity","Retire old candidates in bounded batches before starting another.");
        await using(var bytes=SyncCommand(c,t,"SELECT bytes FROM local_namespace_format WHERE singleton=1"))
            if(Convert.ToInt64(await bytes.ExecuteScalarAsync(ct))>admission.MaximumCandidateBytes-1024)throw new LocalSourceGapException("NamespaceCapacity","Retained candidate namespace exceeds its logical cap.");
        var id=Guid.NewGuid().ToString("N");
        await using(var old=SyncCommand(c,t,"UPDATE local_namespace_candidates SET status=3,error='Fresh candidate required; previous enumeration is not a cut.' WHERE candidate_id IN(SELECT candidate_id FROM local_namespace_active WHERE mapping_id=$mapping)"))
        {old.Parameters.AddWithValue("$mapping",state.Binding.MappingId);await old.ExecuteNonQueryAsync(ct);}
        await using(var q=SyncCommand(c,t,"INSERT INTO local_namespace_candidates(candidate_id,root_key,mapping_id,client_id,root_id,root_created,volume_serial64,owner_epoch,observed_start,legacy_revision,status,bytes,max_candidate,max_profile,free_floor) VALUES($id,$root,$mapping,$client,$node,$created,$serial,$epoch,$start,$revision,0,1024,$cap,$profile,$floor); INSERT INTO local_namespace_active(mapping_id,candidate_id) VALUES($mapping,$id) ON CONFLICT(mapping_id) DO UPDATE SET candidate_id=$id; INSERT INTO local_namespace_nodes VALUES($id,$node,1,$created); INSERT INTO local_namespace_parents(candidate_id,node_id,entry_id,created,state) VALUES($id,$node,0,$created,0); UPDATE local_namespace_format SET bytes=bytes+1024 WHERE singleton=1"))
        {
            AddNamespace(q,id);q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$mapping",state.Binding.MappingId);q.Parameters.AddWithValue("$client",state.Binding.ClientId);
            q.Parameters.AddWithValue("$node",proof.Handle.FileId128);q.Parameters.AddWithValue("$created",proof.Handle.CreationTime);q.Parameters.AddWithValue("$serial",proof.Handle.VolumeSerial64);q.Parameters.AddWithValue("$epoch",state.Cursor.OwnerEpoch);
            q.Parameters.AddWithValue("$start",state.Cursor.NextReadUsn);q.Parameters.AddWithValue("$revision",state.LegacyRevision);q.Parameters.AddWithValue("$cap",admission.MaximumCandidateBytes);q.Parameters.AddWithValue("$profile",budget.MaximumDatabaseBytes);q.Parameters.AddWithValue("$floor",budget.MinimumFreeDiskBytes);await q.ExecuteNonQueryAsync(ct);
        }
        var result=(await ReadNamespaceAsync(c,t,id,ct)).State;await CommitNamespace(c,t,before,ct);return result;
    }
    internal async Task<LocalNamespaceCandidate> ReadOwnedNamespaceAsync(string root,string owner,string id,CancellationToken ct)
    {
        var local=await ReadOwnedLocalStateAsync(root,owner,ct);await using var c=await OpenNamespaceAsync(ct);using var t=c.BeginTransaction(deferred:false);
        var row=await RequireNamespaceAsync(c,t,root,owner,id,true,ct);
        if(!local.Cursor.Valid || local.LegacyRevision!=row.State.LegacyRevision)
        {await using var q=SyncCommand(c,t,"UPDATE local_namespace_candidates SET status=2,error=COALESCE(error,'Local revision or ingestion gap; continuous completeness remains unknown.') WHERE candidate_id=$id AND status IN(0,1)");AddNamespace(q,id);await q.ExecuteNonQueryAsync(ct);}
        row=await ReadNamespaceAsync(c,t,id,ct);await t.CommitAsync(ct);return row.State;
    }
    internal async Task<LocalNamespaceDirectory?> ClaimOwnedNamespaceAsync(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await NamespaceAdmissionAsync(root,owner,id,local,4,ct);await using var c=await OpenNamespaceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);
        var row=await RequireNamespaceAsync(c,t,root,owner,id,false,ct);
        await using(var busy=SyncCommand(c,t,"SELECT 1 FROM local_namespace_parents WHERE candidate_id=$id AND (state=1 OR state=2 AND refilled=0) LIMIT 1"))
        {AddNamespace(busy,id);if(await busy.ExecuteScalarAsync(ct) is not null)throw new InvalidOperationException("Finish the claimed parent and its bounded frontier refill before claiming another.");}
        string node;long created;
        await using(var q=SyncCommand(c,t,"SELECT node_id,created FROM local_namespace_parents WHERE candidate_id=$id AND state=0 ORDER BY entry_id LIMIT 1"))
        {AddNamespace(q,id);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)){await t.CommitAsync(ct);return null;}node=r.GetString(0);created=r.GetInt64(1);}
        var names=new List<string>();var cursor=node;
        for(var depth=0;cursor!=row.State.RootFileId128;depth++)
        {
            if(depth>=512)throw new InvalidDataException("Namespace ancestry is cyclic/deeper than its supported bound.");
            await using var q=SyncCommand(c,t,"SELECT e.name,e.parent_id FROM local_namespace_parents p JOIN local_namespace_entries e ON e.candidate_id=p.candidate_id AND e.entry_id=p.entry_id WHERE p.candidate_id=$id AND p.node_id=$parent");AddNamespace(q,id,cursor);
            await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))throw new InvalidDataException("Namespace ancestry is unresolved.");names.Add(r.GetString(0));cursor=r.GetString(1);
        }
        names.Reverse();var relative=string.Join('/',names);if(relative.Length>4096)throw new InvalidDataException("Namespace path exceeds its supported bound.");
        var claim=Guid.NewGuid().ToString("N");await using(var q=SyncCommand(c,t,"UPDATE local_namespace_parents SET state=1,claim=$claim WHERE candidate_id=$id AND node_id=$parent AND state=0"))
        {AddNamespace(q,id,node);q.Parameters.AddWithValue("$claim",claim);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("Namespace claim changed.");}
        await CommitNamespace(c,t,before,ct);return new(id,node,relative,created,claim);
    }
    private static async Task RequireNamespaceParent(SqliteConnection c,SqliteTransaction t,LocalNamespaceDirectory parent,int expected,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,"SELECT created FROM local_namespace_parents WHERE candidate_id=$id AND node_id=$parent AND claim=$claim AND state=$state");AddNamespace(q,parent.CandidateId,parent.FileId128);
        q.Parameters.AddWithValue("$claim",parent.ClaimToken);q.Parameters.AddWithValue("$state",expected);
        if(await q.ExecuteScalarAsync(ct) is not long created || created!=parent.CreationTime)throw new InvalidOperationException("Namespace parent claim/identity changed.");
    }
    internal async Task RecordOwnedNamespaceHandleAsync(string root,string owner,LocalNamespaceDirectory parent,LocalNamespaceHandleProof proof,LocalJournalAdmission local,CancellationToken ct)
    {
        ValidateNamespaceProof(proof);await NamespaceAdmissionAsync(root,owner,parent.CandidateId,local,4,ct);
        await using var c=await OpenNamespaceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);
        var row=await RequireNamespaceAsync(c,t,root,owner,parent.CandidateId,false,ct);await RequireNamespaceParent(c,t,parent,1,ct);
        var localState=await ReadLocalStateAsync(c,t,root,owner,ct);
        if(proof.FileId128!=parent.FileId128 || proof.CreationTime!=parent.CreationTime || proof.VolumeSerial64!=row.Serial64 || proof.LegacyVolumeSerial.ToString("x8",CultureInfo.InvariantCulture)!=localState.Binding.VolumeSerial)
            throw new InvalidDataException("Opened parent does not match candidate identity.");
        foreach(var (major,wire) in new[]{(2,proof.LegacyFileId64),(3,proof.FileId128)})
        {
            await using var q=SyncCommand(c,t,"INSERT INTO local_namespace_aliases VALUES($id,$parent,$major,$wire,$serial,$created) ON CONFLICT(candidate_id,major,wire_id) DO NOTHING;SELECT node_id,volume_serial64,created FROM local_namespace_aliases WHERE candidate_id=$id AND major=$major AND wire_id=$wire");
            AddNamespace(q,parent.CandidateId,parent.FileId128);q.Parameters.AddWithValue("$major",major);q.Parameters.AddWithValue("$wire",wire);q.Parameters.AddWithValue("$serial",proof.VolumeSerial64);q.Parameters.AddWithValue("$created",proof.CreationTime);
            await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)||r.GetString(0)!=parent.FileId128||r.GetString(1)!=proof.VolumeSerial64||r.GetInt64(2)!=proof.CreationTime)throw new InvalidDataException("Wire identity alias was rebound.");
        }
        await using(var q=SyncCommand(c,t,"UPDATE local_namespace_parents SET case_sensitive=$case WHERE candidate_id=$id AND node_id=$parent"))
        {AddNamespace(q,parent.CandidateId,parent.FileId128);q.Parameters.AddWithValue("$case",proof.CaseSensitive?1:0);await q.ExecuteNonQueryAsync(ct);}
        await CommitNamespace(c,t,before,ct);
    }
    internal async Task<LocalNamespaceWrite> StageOwnedNamespaceAsync(string root,string owner,LocalNamespaceDirectory parent,IReadOnlyList<LocalNamespaceEntry> entries,LocalJournalAdmission local,CancellationToken ct)
    {
        if(entries.Count<1 || entries.Count>NamespaceStageCapacity)throw new ArgumentOutOfRangeException(nameof(entries));foreach(var e in entries)WindowsDirectoryPageParser.ValidateEntry(e);
        var row=await NamespaceAdmissionAsync(root,owner,parent.CandidateId,local,entries.Count*2,ct);
        var incoming=entries.Sum(e=>256L+Encoding.UTF8.GetByteCount(e.Name));
        await using var c=await OpenNamespaceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);
        row=await RequireNamespaceAsync(c,t,root,owner,parent.CandidateId,false,ct);await RequireNamespaceParent(c,t,parent,1,ct);
        await using(var quota=SyncCommand(c,t,"SELECT bytes FROM local_namespace_format WHERE singleton=1"))
            if(Convert.ToInt64(await quota.ExecuteScalarAsync(ct))>row.Admission.MaximumCandidateBytes-incoming)throw new LocalSourceGapException("NamespaceCapacity","Namespace retained history exceeds its bounded logical quota.");
        long bytes=0,max=0,added=0,excluded=0;
        foreach(var e in entries)
        {
            if(e.FileId128==parent.FileId128 || e.FileId128==row.State.RootFileId128)throw new InvalidDataException("Self/root ancestry alias is invalid.");
            var canonical=WebDavSyncClient.CanonicalPathKey(e.Name);
            await using(var exists=SyncCommand(c,t,"SELECT name,node_id,created,written,changed,size,attributes,tag FROM local_namespace_entries WHERE candidate_id=$id AND parent_id=$parent AND canonical=$key"))
            {
                AddNamespace(exists,parent.CandidateId,parent.FileId128);exists.Parameters.AddWithValue("$key",canonical);await using var r=await exists.ExecuteReaderAsync(ct);
                if(await r.ReadAsync(ct))
                {if(r.GetString(0)!=e.Name||r.GetString(1)!=e.FileId128||r.GetInt64(2)!=e.CreationTime||r.GetInt64(3)!=e.LastWriteTime||r.GetInt64(4)!=e.ChangeTime||r.GetInt64(5)!=e.Size||r.GetInt64(6)!=e.Attributes||r.GetInt64(7)!=e.ReparseTag)throw new InvalidDataException("Siblings collide or changed during candidate enumeration.");continue;}
            }
            await using(var q=SyncCommand(c,t,"INSERT INTO local_namespace_nodes VALUES($id,$node,$kind,$created) ON CONFLICT(candidate_id,node_id) DO NOTHING;SELECT kind,created FROM local_namespace_nodes WHERE candidate_id=$id AND node_id=$node"))
            {AddNamespace(q,parent.CandidateId);q.Parameters.AddWithValue("$node",e.FileId128);q.Parameters.AddWithValue("$kind",e.IsDirectory?1:0);q.Parameters.AddWithValue("$created",e.CreationTime);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)||r.GetInt32(0)!=(e.IsDirectory?1:0)||r.GetInt64(1)!=e.CreationTime)throw new InvalidDataException("Candidate node incarnation/type changed.");}
            var charge=256+Encoding.UTF8.GetByteCount(e.Name);
            await using(var q=SyncCommand(c,t,"INSERT INTO local_namespace_entries(candidate_id,parent_id,node_id,name,canonical,kind,created,written,changed,size,attributes,tag,excluded,bytes) VALUES($id,$parent,$node,$name,$key,$kind,$created,$written,$changed,$size,$attributes,$tag,$excluded,$bytes) RETURNING entry_id"))
            {
                AddNamespace(q,parent.CandidateId,parent.FileId128);q.Parameters.AddWithValue("$node",e.FileId128);q.Parameters.AddWithValue("$name",e.Name);q.Parameters.AddWithValue("$key",canonical);q.Parameters.AddWithValue("$kind",e.IsDirectory?1:0);q.Parameters.AddWithValue("$created",e.CreationTime);
                q.Parameters.AddWithValue("$written",e.LastWriteTime);q.Parameters.AddWithValue("$changed",e.ChangeTime);q.Parameters.AddWithValue("$size",e.Size);q.Parameters.AddWithValue("$attributes",e.Attributes);q.Parameters.AddWithValue("$tag",e.ReparseTag);q.Parameters.AddWithValue("$excluded",e.Excluded?1:0);q.Parameters.AddWithValue("$bytes",charge);
                max=Convert.ToInt64(await q.ExecuteScalarAsync(ct));
            }
            added++;bytes+=charge;if(e.Excluded)excluded++;
        }
        if(added>0)
        {
            await using var q=SyncCommand(c,t,"UPDATE local_namespace_parents SET entries=entries+$count,bytes=bytes+$bytes,max_entry=$max WHERE candidate_id=$id AND node_id=$parent;UPDATE local_namespace_candidates SET entries=entries+$count,bytes=bytes+$bytes,exclusions=exclusions+$excluded WHERE candidate_id=$id;UPDATE local_namespace_format SET bytes=bytes+$bytes WHERE singleton=1");
            AddNamespace(q,parent.CandidateId,parent.FileId128);q.Parameters.AddWithValue("$count",added);q.Parameters.AddWithValue("$bytes",bytes);q.Parameters.AddWithValue("$max",max);q.Parameters.AddWithValue("$excluded",excluded);await q.ExecuteNonQueryAsync(ct);
        }
        return new((int)added,await CommitNamespace(c,t,before,ct));
    }
    internal async Task<long> CompleteOwnedNamespaceParentAsync(string root,string owner,LocalNamespaceDirectory parent,LocalJournalAdmission local,CancellationToken ct)
    {
        await NamespaceAdmissionAsync(root,owner,parent.CandidateId,local,4,ct);await using var c=await OpenNamespaceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);
        await RequireNamespaceAsync(c,t,root,owner,parent.CandidateId,false,ct);await RequireNamespaceParent(c,t,parent,1,ct);
        await using var q=SyncCommand(c,t,"UPDATE local_namespace_parents SET state=2 WHERE candidate_id=$id AND node_id=$parent AND case_sensitive IS NOT NULL;UPDATE local_namespace_candidates SET validated=validated+1,eligible_through=MAX(eligible_through,(SELECT max_entry FROM local_namespace_parents WHERE candidate_id=$id AND node_id=$parent)) WHERE candidate_id=$id AND EXISTS(SELECT 1 FROM local_namespace_parents WHERE candidate_id=$id AND node_id=$parent AND state=2)");AddNamespace(q,parent.CandidateId,parent.FileId128);
        if(await q.ExecuteNonQueryAsync(ct)!=2)throw new InvalidOperationException("A parent requires verified handle metadata before eligibility.");return await CommitNamespace(c,t,before,ct);
    }
    internal async Task<LocalNamespaceRefill?> RefillOwnedNamespaceAsync(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        var limit=NamespaceMutationCapacity;await NamespaceAdmissionAsync(root,owner,id,local,limit,ct);await using var c=await OpenNamespaceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);
        await RequireNamespaceAsync(c,t,root,owner,id,false,ct);string parent;long cursor;
        await using(var q=SyncCommand(c,t,"SELECT node_id,child_cursor FROM local_namespace_parents WHERE candidate_id=$id AND state=2 AND refilled=0 ORDER BY entry_id LIMIT 1"))
        {AddNamespace(q,id);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)){await t.CommitAsync(ct);return null;}parent=r.GetString(0);cursor=r.GetInt64(1);}
        var children=new List<(long Entry,string Node,long Created)>();
        await using(var q=SyncCommand(c,t,"SELECT entry_id,node_id,created FROM local_namespace_entries WHERE candidate_id=$id AND parent_id=$parent AND kind=1 AND excluded=0 AND entry_id>$cursor ORDER BY entry_id LIMIT $limit"))
        {AddNamespace(q,id,parent);q.Parameters.AddWithValue("$cursor",cursor);q.Parameters.AddWithValue("$limit",limit);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))children.Add((r.GetInt64(0),r.GetString(1),r.GetInt64(2)));}
        foreach(var child in children)
        {await using var q=SyncCommand(c,t,"INSERT INTO local_namespace_parents(candidate_id,node_id,entry_id,created,state) VALUES($id,$node,$entry,$created,0)");AddNamespace(q,id);q.Parameters.AddWithValue("$node",child.Node);q.Parameters.AddWithValue("$entry",child.Entry);q.Parameters.AddWithValue("$created",child.Created);await q.ExecuteNonQueryAsync(ct);}
        await using(var q=SyncCommand(c,t,"UPDATE local_namespace_parents SET child_cursor=$cursor,refilled=$complete WHERE candidate_id=$id AND node_id=$parent"))
        {AddNamespace(q,id,parent);q.Parameters.AddWithValue("$cursor",children.Count>0?children[^1].Entry:cursor);q.Parameters.AddWithValue("$complete",children.Count<limit?1:0);await q.ExecuteNonQueryAsync(ct);}
        return new(children.Count,children.Count<limit,await CommitNamespace(c,t,before,ct));
    }
    internal async Task<long> FinishOwnedNamespaceAsync(string root,string owner,string id,LocalNamespaceStatus status,string? error,LocalJournalAdmission local,CancellationToken ct)
    {
        if(status is not(LocalNamespaceStatus.EnumerationComplete or LocalNamespaceStatus.Incomplete) || error?.Length>1024)throw new ArgumentException("Invalid namespace terminal result.");
        await using var c=await OpenNamespaceAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);
        await RequireNamespaceAsync(c,t,root,owner,id,status==LocalNamespaceStatus.Incomplete,ct);
        if(status==LocalNamespaceStatus.EnumerationComplete)
        {await using var q=SyncCommand(c,t,"SELECT 1 FROM local_namespace_parents WHERE candidate_id=$id AND (state<>2 OR refilled=0) LIMIT 1");AddNamespace(q,id);if(await q.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Frontier/refill is incomplete.");}
        await using(var q=SyncCommand(c,t,"UPDATE local_namespace_candidates SET status=$status,error=$error WHERE candidate_id=$id AND status=0"))
        {AddNamespace(q,id);q.Parameters.AddWithValue("$status",(int)status);q.Parameters.AddWithValue("$error",(object?)error??DBNull.Value);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("Namespace terminal state cannot be overwritten.");}
        return await CommitNamespace(c,t,before,ct);
    }
    internal async Task<IReadOnlyList<LocalNamespaceEligibleEntry>> ReadOwnedNamespaceEntriesAsync(string root,string owner,string id,long after,CancellationToken ct)
    {
        await using var c=await OpenNamespaceAsync(ct);using var t=c.BeginTransaction(deferred:true);var row=await RequireNamespaceAsync(c,t,root,owner,id,true,ct);
        var local=await ReadLocalStateAsync(c,t,root,owner,ct);await RequireLocalMappingAsync(c,t,local.Binding,ct);
        if(!local.Cursor.Valid || local.LegacyRevision!=row.State.LegacyRevision || local.LegacyRevision!=await LocalRevisionAsync(c,t,ct))
            throw new LocalSourceGapException("NamespaceLegacyGap","Stale local candidate cannot expose eligible entries.");
        if(row.State.Status is LocalNamespaceStatus.Incomplete or LocalNamespaceStatus.Superseded)throw new InvalidOperationException("Incomplete/superseded candidates do not expose eligible pages.");
        var entries=new List<LocalNamespaceEligibleEntry>();await using var q=SyncCommand(c,t,"SELECT e.entry_id,e.parent_id,e.name,e.node_id,e.created,e.written,e.changed,e.size,e.attributes,e.tag FROM local_namespace_entries e JOIN local_namespace_parents p ON p.candidate_id=e.candidate_id AND p.node_id=e.parent_id WHERE e.candidate_id=$id AND e.entry_id>$after AND e.entry_id<=$through AND p.state=2 ORDER BY e.entry_id LIMIT 256");AddNamespace(q,id);q.Parameters.AddWithValue("$after",after);q.Parameters.AddWithValue("$through",row.State.EligibleThrough);
        await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))entries.Add(new(r.GetInt64(0),r.GetString(1),new(r.GetString(2),r.GetString(3),r.GetInt64(4),r.GetInt64(5),r.GetInt64(6),r.GetInt64(7),(uint)r.GetInt64(8),(uint)r.GetInt64(9))));return entries.AsReadOnly();
    }
    internal async Task<LocalNamespaceRetirement> RetireOwnedNamespaceAsync(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await RefusePinnedLocalProjectionAsync(id,true,ct);
        var current=await ReadOwnedLocalStateAsync(root,owner,ct);await using var c=await OpenNamespaceAsync(ct);using var t=c.BeginTransaction(deferred:true);var row=await ReadNamespaceAsync(c,t,id,ct);
        await RequireNoCoherenceCandidatePin(c,t,id,ct);
        if(row.Mapping!=current.Binding.MappingId || row.Client!=current.Binding.ClientId || row.State.Status!=LocalNamespaceStatus.Superseded)throw new InvalidOperationException("Retire only an explicit superseded candidate of this mapping/client.");
        var limit=NamespaceMutationCapacity;await t.CommitAsync(ct);await CheckLocalAdmissionAsync(NamespaceBudget(local,row.Admission),0,NamespaceReserve(limit),ct);using var write=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,write,ct);
        _=await ReadLocalStateAsync(c,write,root,owner,ct);row=await ReadNamespaceAsync(c,write,id,ct);
        await using(var active=SyncCommand(c,write,"SELECT 1 FROM local_namespace_active WHERE candidate_id=$id")){AddNamespace(active,id);if(await active.ExecuteScalarAsync(ct)is not null || row.State.Status!=LocalNamespaceStatus.Superseded)throw new InvalidOperationException("Active namespace cannot be retired.");}
        int phase;await using(var q=SyncCommand(c,write,"SELECT retire_phase FROM local_namespace_candidates WHERE candidate_id=$id")){AddNamespace(q,id);phase=Convert.ToInt32(await q.ExecuteScalarAsync(ct));}
        var table=new[]{"local_namespace_entries","local_namespace_aliases","local_namespace_parents","local_namespace_nodes"}[Math.Min(phase,3)];var keys=new List<(object A,object? B,long Bytes)>();
        if(phase<4)
        {
            var select=phase switch{0=>"entry_id,NULL,bytes",1=>"major,wire_id,0",_=>"node_id,NULL,0"};await using var q=SyncCommand(c,write,$"SELECT {select} FROM {table} WHERE candidate_id=$id LIMIT $limit");AddNamespace(q,id);q.Parameters.AddWithValue("$limit",limit);
            await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))keys.Add((r.GetValue(0),r.IsDBNull(1)?null:r.GetValue(1),r.GetInt64(2)));
            foreach(var key in keys)
            {var where=phase switch{0=>"entry_id=$a",1=>"major=$a AND wire_id=$b",_=>"node_id=$a"};await using var delete=SyncCommand(c,write,$"DELETE FROM {table} WHERE candidate_id=$id AND {where}");AddNamespace(delete,id);delete.Parameters.AddWithValue("$a",key.A);if(key.B is not null)delete.Parameters.AddWithValue("$b",key.B);await delete.ExecuteNonQueryAsync(ct);}
        }
        var removed=phase==4;var bytes=removed?row.State.Bytes:keys.Sum(x=>x.Bytes);
        if(removed){await using var q=SyncCommand(c,write,"DELETE FROM local_namespace_candidates WHERE candidate_id=$id");AddNamespace(q,id);await q.ExecuteNonQueryAsync(ct);}
        else{await using var q=SyncCommand(c,write,"UPDATE local_namespace_candidates SET bytes=bytes-$bytes,retire_phase=retire_phase+$next WHERE candidate_id=$id");AddNamespace(q,id);q.Parameters.AddWithValue("$bytes",bytes);q.Parameters.AddWithValue("$next",keys.Count<limit?1:0);await q.ExecuteNonQueryAsync(ct);}
        if(bytes>0){await using var q=SyncCommand(c,write,"UPDATE local_namespace_format SET bytes=bytes-$bytes WHERE singleton=1");q.Parameters.AddWithValue("$bytes",bytes);await q.ExecuteNonQueryAsync(ct);}
        return new(keys.Count,bytes,removed,await CommitNamespace(c,write,before,ct));
    }
}

public sealed partial class LocalSourceJournalSession
{
    public int NamespaceStageCapacity=>_store.NamespaceStageCapacity;
    public Task<LocalNamespaceCandidate> BeginNamespaceCandidateAsync(LocalNamespaceRootProof proof,LocalNamespaceAdmission? admission=null,CancellationToken cancellationToken=default)
        =>SerialAsync(()=>_store.BeginOwnedNamespaceAsync(_root,_owner,_admission,proof,admission??new(),cancellationToken),cancellationToken);
    public Task<LocalNamespaceCandidate> ReadNamespaceCandidateAsync(string candidate,CancellationToken cancellationToken=default)
        =>SerialAsync(()=>_store.ReadOwnedNamespaceAsync(_root,_owner,candidate,cancellationToken),cancellationToken);
    public Task<LocalNamespaceDirectory?> ClaimNamespaceDirectoryAsync(string candidate,CancellationToken cancellationToken=default)
        =>SerialAsync(()=>_store.ClaimOwnedNamespaceAsync(_root,_owner,candidate,_admission,cancellationToken),cancellationToken);
    public Task RecordNamespaceHandleAsync(LocalNamespaceDirectory directory,LocalNamespaceHandleProof proof,CancellationToken cancellationToken=default)
        =>SerialAsync(async()=>{await _store.RecordOwnedNamespaceHandleAsync(_root,_owner,directory,proof,_admission,cancellationToken);return true;},cancellationToken);
    public Task<LocalNamespaceWrite> StageNamespaceEntriesAsync(LocalNamespaceDirectory directory,IReadOnlyList<LocalNamespaceEntry> entries,CancellationToken cancellationToken=default)
        =>SerialAsync(()=>_store.StageOwnedNamespaceAsync(_root,_owner,directory,entries,_admission,cancellationToken),cancellationToken);
    public Task<long> CompleteNamespaceDirectoryAsync(LocalNamespaceDirectory directory,CancellationToken cancellationToken=default)
        =>SerialAsync(()=>_store.CompleteOwnedNamespaceParentAsync(_root,_owner,directory,_admission,cancellationToken),cancellationToken);
    public Task<LocalNamespaceRefill?> RefillNamespaceFrontierAsync(string candidate,CancellationToken cancellationToken=default)
        =>SerialAsync(()=>_store.RefillOwnedNamespaceAsync(_root,_owner,candidate,_admission,cancellationToken),cancellationToken);
    public Task<long> FinishNamespaceAsync(string candidate,LocalNamespaceStatus status,string? error=null,CancellationToken cancellationToken=default)
        =>SerialAsync(()=>_store.FinishOwnedNamespaceAsync(_root,_owner,candidate,status,error,_admission,cancellationToken),cancellationToken);
    public Task<IReadOnlyList<LocalNamespaceEligibleEntry>> ReadNamespaceEligibleEntriesAsync(string candidate,long afterEntryId=0,CancellationToken cancellationToken=default)
        =>SerialAsync(()=>_store.ReadOwnedNamespaceEntriesAsync(_root,_owner,candidate,afterEntryId,cancellationToken),cancellationToken);
    public Task<LocalNamespaceRetirement> RetireNamespaceBatchAsync(string candidate,CancellationToken cancellationToken=default)
        =>SerialAsync(()=>_store.RetireOwnedNamespaceAsync(_root,_owner,candidate,_admission,cancellationToken),cancellationToken);
}
