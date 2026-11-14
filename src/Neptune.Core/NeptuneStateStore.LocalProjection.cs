using System.Buffers.Binary;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private long _projectionTransactions,_projectionMaxChanges,_projectionMaxCommit;
    public int LocalProjectionApplyCapacity=>Math.Min(32,Math.Max(1,NamespaceMutationCapacity/8));
    public LocalProjectionMetrics GetLocalProjectionMetrics()=>new(Interlocked.Read(ref _projectionTransactions),Interlocked.Read(ref _projectionMaxChanges),Interlocked.Read(ref _projectionMaxCommit));
    private sealed record ProjectionRow(LocalProjectionState State,string RootKey,LocalProjectionAdmission Admission);
    private sealed record ProjectionAlias(string Node,long Created,int Kind);
    private sealed record ProjectionEdge(string Name,string Node,long Created,int Kind,bool Tombstone);
    private static void ProjectionId(SqliteCommand q,string id){q.Parameters.AddWithValue("$projection",id);}
    private static LocalJournalAdmission ProjectionBudget(LocalJournalAdmission local,LocalProjectionAdmission p)=>new(local.MaximumInboxBytes,Math.Max(local.MinimumFreeDiskBytes,p.MinimumFreeDiskBytes),Math.Min(local.MaximumDatabaseBytes,p.MaximumProfileBytes));
    private async Task<SqliteConnection> OpenProjectionAsync(CancellationToken ct)
    {if(await PreflightProjectionAsync(ct)!=1)throw new InvalidDataException("Projection module is absent.");return await OpenLocalSourceConnectionAsync(ct);}
    private static async Task<ProjectionRow> ReadProjectionAsync(SqliteConnection c,SqliteTransaction t,string id,long ingest,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,"SELECT root_key,base_candidate,status,journal_id,start_usn,applied_usn,owner_epoch,legacy_revision,gap_epoch,gap_code,dirty_count,unresolved_count,bytes,max_bytes,profile_cap,free_floor FROM local_projection_roots WHERE projection_id=$projection");ProjectionId(q,id);
        await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))throw new InvalidOperationException("Projection is absent.");
        return new(new(id,r.GetString(1),(LocalProjectionStatus)r.GetInt32(2),r.GetString(3),r.GetInt64(4),r.GetInt64(5),ingest,r.GetInt64(6),r.GetInt64(7),r.GetInt64(8),r.IsDBNull(9)?null:r.GetString(9),r.GetInt64(10),r.GetInt64(11),r.GetInt64(12)),r.GetString(0),new(r.GetInt64(13),r.GetInt64(14),r.GetInt64(15)));
    }
    private static async Task<ProjectionRow> RequireProjectionAsync(SqliteConnection c,SqliteTransaction t,string root,string owner,string id,bool terminal,CancellationToken ct)
    {
        var local=await ReadLocalStateAsync(c,t,root,owner,ct);var row=await ReadProjectionAsync(c,t,id,local.Cursor.NextReadUsn,ct);
        if(row.RootKey!=root||row.State.OwnerEpoch!=local.Cursor.OwnerEpoch)throw new InvalidOperationException("Projection owner binding changed; fresh candidate/recovery required.");
        if(!terminal)
        {
            await RequireLocalMappingAsync(c,t,local.Binding,ct);
            if(!local.Cursor.Valid||local.LegacyRevision!=await LocalRevisionAsync(c,t,ct)||row.State.LegacyRevision!=local.LegacyRevision||row.State.JournalId!=local.Cursor.JournalIdHex||row.State.GapEpoch!=local.GapEpoch)throw new LocalSourceGapException("ProjectionContinuityGap","Projection cannot qualify a stale local subscription/revision.");
            await using var q=SyncCommand(c,t,"SELECT 1 FROM local_projection_active WHERE root_key=$root AND projection_id=$projection");q.Parameters.AddWithValue("$root",root);ProjectionId(q,id);
            if(row.State.Status!=LocalProjectionStatus.Active||await q.ExecuteScalarAsync(ct)is null)throw new InvalidOperationException("Only the active projection can accept work.");
            await using var candidate=SyncCommand(c,t,"SELECT 1 FROM local_namespace_active a JOIN local_namespace_candidates n ON n.candidate_id=a.candidate_id WHERE n.candidate_id=$candidate AND n.status=1 AND n.owner_epoch=$epoch");candidate.Parameters.AddWithValue("$candidate",row.State.CandidateId);candidate.Parameters.AddWithValue("$epoch",local.Cursor.OwnerEpoch);
            if(await candidate.ExecuteScalarAsync(ct)is null)throw new InvalidOperationException("A new/incomplete namespace candidate fences the previous projection; recovery is explicit.");
        }
        return row;
    }
    private async Task<long> CommitProjection(SqliteConnection c,SqliteTransaction t,long before,CancellationToken ct)
    {var changes=await NamespaceChanges(c,t,ct)-before;var timer=Stopwatch.StartNew();await t.CommitAsync(ct);Interlocked.Increment(ref _projectionTransactions);UpdateBootstrapMaximum(ref _projectionMaxChanges,changes);UpdateBootstrapMaximum(ref _projectionMaxCommit,timer.Elapsed.Ticks);return changes;}
    private async Task GapProjection(string root,string owner,string id,string code,CancellationToken ct)
    {
        await using var c=await OpenProjectionAsync(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireProjectionAsync(c,t,root,owner,id,true,ct);
        await using var q=SyncCommand(c,t,"UPDATE local_projection_roots SET status=1,gap_epoch=gap_epoch+1,gap_code=$code WHERE projection_id=$projection AND status=0");ProjectionId(q,id);q.Parameters.AddWithValue("$code",code);await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);
    }
    private async Task<ProjectionRow> ProjectionAdmission(string root,string owner,string id,LocalJournalAdmission local,int rows,CancellationToken ct)
    {await using var c=await OpenProjectionAsync(ct);using var t=c.BeginTransaction(deferred:true);var row=await RequireProjectionAsync(c,t,root,owner,id,false,ct);await t.CommitAsync(ct);await CheckLocalAdmissionAsync(ProjectionBudget(local,row.Admission),0,NamespaceReserve(rows),ct);return row;}
    internal async Task<LocalProjectionState> BeginOwnedProjectionAsync(string root,string owner,string candidate,LocalJournalAdmission local,LocalProjectionAdmission admission,CancellationToken ct)
    {
        if(admission.MaximumProjectionBytes is <1024 or >128L*1024*1024||admission.MaximumProfileBytes is <1024*1024 or >1024L*1024*1024||admission.MinimumFreeDiskBytes<0)throw new ArgumentOutOfRangeException(nameof(admission));
        var state=await ReadOwnedLocalStateAsync(root,owner,ct);if(!state.Cursor.Valid)throw new LocalSourceGapException("ProjectionIngestGap","Valid ingestion is required.");
        await EnsureProjectionAsync(ProjectionBudget(local,admission),ct);
        // A repeated Begin is a read: it must not reserve another migration/root.
        await using(var prior=await OpenProjectionAsync(ct))
        using(var read=prior.BeginTransaction(deferred:true))
        {
            var current=await ReadLocalStateAsync(prior,read,root,owner,ct);
            await RequireLocalMappingAsync(prior,read,current.Binding,ct);
            await using var q=SyncCommand(prior,read,"SELECT projection_id FROM local_projection_active WHERE root_key=$root");q.Parameters.AddWithValue("$root",root);
            if(await q.ExecuteScalarAsync(ct)is string existing)
            {var old=await ReadProjectionAsync(prior,read,existing,current.Cursor.NextReadUsn,ct);if(old.State.CandidateId==candidate&&old.State.OwnerEpoch==current.Cursor.OwnerEpoch&&old.State.Status==LocalProjectionStatus.Active)return (await RequireProjectionAsync(prior,read,root,owner,existing,false,ct)).State;}
        }
        await CheckLocalAdmissionAsync(ProjectionBudget(local,admission),1024,NamespaceReserve(8),ct);
        await using var c=await OpenProjectionAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);state=await ReadLocalStateAsync(c,t,root,owner,ct);
        var source=await RequireNamespaceAsync(c,t,root,owner,candidate,true,ct);await RequireLocalMappingAsync(c,t,state.Binding,ct);
        if(!state.Cursor.Valid||source.State.Status!=LocalNamespaceStatus.EnumerationComplete||source.State.LegacyRevision!=state.LegacyRevision||state.LegacyRevision!=await LocalRevisionAsync(c,t,ct))throw new LocalSourceGapException("ProjectionCandidateGap","Require a completed current candidate; enumeration is not a source cut.");
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_namespace_active WHERE candidate_id=$id")){AddNamespace(q,candidate);if(await q.ExecuteScalarAsync(ct)is null)throw new InvalidOperationException("Candidate is not active.");}
        await using(var q=SyncCommand(c,t,"SELECT projection_id FROM local_projection_active WHERE root_key=$root"))
        {q.Parameters.AddWithValue("$root",root);if(await q.ExecuteScalarAsync(ct)is string existing){var old=await ReadProjectionAsync(c,t,existing,state.Cursor.NextReadUsn,ct);if(old.State.CandidateId==candidate&&old.State.OwnerEpoch==state.Cursor.OwnerEpoch&&old.State.Status==LocalProjectionStatus.Active){await t.CommitAsync(ct);return old.State;}}}
        await using(var q=SyncCommand(c,t,"SELECT journal_id,boundary FROM local_projection_retired WHERE root_key=$root"))
        {q.Parameters.AddWithValue("$root",root);await using var r=await q.ExecuteReaderAsync(ct);if(await r.ReadAsync(ct)&&(r.GetString(0)!=state.Cursor.JournalIdHex||r.GetInt64(1)>source.State.ObservedIngestStart))throw new LocalSourceGapException("ProjectionPrefixRetired","Required validated inbox prefix was retired.");}
        await using(var q=SyncCommand(c,t,"SELECT COUNT(*) FROM (SELECT 1 FROM local_projection_roots WHERE root_key=$root LIMIT 4)")){q.Parameters.AddWithValue("$root",root);if(Convert.ToInt64(await q.ExecuteScalarAsync(ct))>=4)throw new LocalSourceGapException("ProjectionHistoryCapacity","Retire old projection history explicitly.");}
        await using(var q=SyncCommand(c,t,"SELECT bytes FROM local_projection_format WHERE singleton=1"))if(Convert.ToInt64(await q.ExecuteScalarAsync(ct))>admission.MaximumProjectionBytes-1024)throw new LocalSourceGapException("ProjectionCapacity","Retained projection ledger exceeds its logical cap.");
        var id=Guid.NewGuid().ToString("N");await using(var q=SyncCommand(c,t,"UPDATE local_projection_roots SET status=2,gap_code=COALESCE(gap_code,'Fresh candidate/owner recovery; completeness remains unknown.') WHERE projection_id IN(SELECT projection_id FROM local_projection_active WHERE root_key=$root);INSERT INTO local_projection_roots(projection_id,root_key,base_candidate,owner_epoch,journal_id,start_usn,applied_usn,legacy_revision,gap_epoch,status,bytes,max_bytes,profile_cap,free_floor) VALUES($projection,$root,$candidate,$epoch,$journal,$start,$start,$revision,$gap,0,1024,$cap,$profile,$floor);INSERT INTO local_projection_active VALUES($root,$projection) ON CONFLICT(root_key) DO UPDATE SET projection_id=$projection;UPDATE local_projection_format SET bytes=bytes+1024 WHERE singleton=1"))
        {ProjectionId(q,id);q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$candidate",candidate);q.Parameters.AddWithValue("$epoch",state.Cursor.OwnerEpoch);q.Parameters.AddWithValue("$journal",state.Cursor.JournalIdHex);q.Parameters.AddWithValue("$start",source.State.ObservedIngestStart);q.Parameters.AddWithValue("$revision",state.LegacyRevision);q.Parameters.AddWithValue("$gap",state.GapEpoch);q.Parameters.AddWithValue("$cap",admission.MaximumProjectionBytes);q.Parameters.AddWithValue("$profile",Math.Min(local.MaximumDatabaseBytes,admission.MaximumProfileBytes));q.Parameters.AddWithValue("$floor",Math.Max(local.MinimumFreeDiskBytes,admission.MinimumFreeDiskBytes));await q.ExecuteNonQueryAsync(ct);}
        var result=(await ReadProjectionAsync(c,t,id,state.Cursor.NextReadUsn,ct)).State;await CommitProjection(c,t,before,ct);return result;
    }
    internal async Task<LocalProjectionState> ReadOwnedProjectionAsync(string root,string owner,string id,CancellationToken ct)
    {
        var local=await ReadOwnedLocalStateAsync(root,owner,ct);await using var c=await OpenProjectionAsync(ct);using var t=c.BeginTransaction(deferred:false);var row=await RequireProjectionAsync(c,t,root,owner,id,true,ct);
        if(!local.Cursor.Valid||row.State.LegacyRevision!=local.LegacyRevision){await using var q=SyncCommand(c,t,"UPDATE local_projection_roots SET status=1,gap_epoch=gap_epoch+1,gap_code=COALESCE(gap_code,'Local revision/ingestion gap.') WHERE projection_id=$projection AND status=0");ProjectionId(q,id);await q.ExecuteNonQueryAsync(ct);}
        row=await ReadProjectionAsync(c,t,id,local.Cursor.NextReadUsn,ct);await t.CommitAsync(ct);return row.State;
    }
    private static LocalUsnRecord ProjectionRecord(string journal,long usn,long end,byte[] raw)
        => UsnRecordParser.ParseStoredRecord(journal,usn,end,raw);
    private static string ProjectionHash(byte[] raw)=>Convert.ToHexStringLower(SHA256.HashData(raw));
    private static async Task<ProjectionAlias?> ProjectionAliasAsync(SqliteConnection c,SqliteTransaction t,ProjectionRow row,ushort major,string wire,CancellationToken ct)
    {
        ProjectionAlias? found=null;
        foreach(var sql in new[]{"SELECT a.node_id,a.created,n.kind FROM local_projection_aliases a JOIN local_projection_nodes n ON n.projection_id=a.projection_id AND n.node_id=a.node_id WHERE a.projection_id=$projection AND a.major=$major AND a.wire_id=$wire","SELECT a.node_id,a.created,n.kind FROM local_namespace_aliases a JOIN local_namespace_nodes n ON n.candidate_id=a.candidate_id AND n.node_id=a.node_id WHERE a.candidate_id=$candidate AND a.major=$major AND a.wire_id=$wire"})
        {await using var q=SyncCommand(c,t,sql);ProjectionId(q,row.State.ProjectionId);q.Parameters.AddWithValue("$candidate",row.State.CandidateId);q.Parameters.AddWithValue("$major",major);q.Parameters.AddWithValue("$wire",wire);await using var r=await q.ExecuteReaderAsync(ct);if(await r.ReadAsync(ct)){var current=new ProjectionAlias(r.GetString(0),r.GetInt64(1),r.GetInt32(2));if(found is not null&&found!=current)throw new LocalSourceGapException("ProjectionAliasRebind","Exact wire alias was rebound.");found=current;}}
        return found;
    }
    private static async Task<ProjectionEdge?> ProjectionEdgeAsync(SqliteConnection c,SqliteTransaction t,ProjectionRow row,string parent,string name,CancellationToken ct)
    {
        var key=WebDavSyncClient.CanonicalPathKey(name);
        foreach(var sql in new[]{"SELECT name,node_id,created,kind,action FROM local_projection_edges WHERE projection_id=$projection AND parent_id=$parent AND canonical=$name","SELECT name,node_id,created,kind,0 FROM local_namespace_entries WHERE candidate_id=$candidate AND parent_id=$parent AND canonical=$name"})
        {await using var q=SyncCommand(c,t,sql);ProjectionId(q,row.State.ProjectionId);q.Parameters.AddWithValue("$candidate",row.State.CandidateId);q.Parameters.AddWithValue("$parent",parent);q.Parameters.AddWithValue("$name",key);await using var r=await q.ExecuteReaderAsync(ct);if(await r.ReadAsync(ct))return new(r.GetString(0),r.GetString(1),r.GetInt64(2),r.GetInt32(3),r.GetInt32(4)!=0);}
        return null;
    }
    private static async Task<(long Bytes,int Added)> DirtyProjectionNode(SqliteConnection c,SqliteTransaction t,string id,ProjectionAlias node,LocalUsnRecord record,CancellationToken ct)
    {
        bool exists;await using(var q=SyncCommand(c,t,"SELECT created,kind,generation,subtree_epoch FROM local_projection_nodes WHERE projection_id=$projection AND node_id=$node"))
        {ProjectionId(q,id);q.Parameters.AddWithValue("$node",node.Node);await using var r=await q.ExecuteReaderAsync(ct);exists=await r.ReadAsync(ct);if(exists&&(r.GetInt64(0)!=node.Created||r.GetInt32(1)!=node.Kind||r.GetInt64(2)==long.MaxValue||node.Kind==1&&r.GetInt64(3)==long.MaxValue))throw new LocalSourceGapException("ProjectionIncarnationGap","Dirty incarnation changed/overflowed.");}
        await using(var q=SyncCommand(c,t,"INSERT INTO local_projection_nodes VALUES($projection,$node,$created,$kind,1,$usn,$reasons,$subtree,256) ON CONFLICT(projection_id,node_id) DO UPDATE SET generation=generation+1,last_usn=MAX(last_usn,excluded.last_usn),reasons=reasons|excluded.reasons,subtree_epoch=subtree_epoch+excluded.subtree_epoch"))
        {ProjectionId(q,id);q.Parameters.AddWithValue("$node",node.Node);q.Parameters.AddWithValue("$created",node.Created);q.Parameters.AddWithValue("$kind",node.Kind);q.Parameters.AddWithValue("$usn",record.Usn);q.Parameters.AddWithValue("$reasons",record.Reason);q.Parameters.AddWithValue("$subtree",node.Kind);await q.ExecuteNonQueryAsync(ct);}
        return(exists?0:256,exists?0:1);
    }
    private static async Task<long> StoreProjectionUnknown(SqliteConnection c,SqliteTransaction t,string id,LocalUsnRecord record,byte[] raw,string code,CancellationToken ct)
    {var bytes=raw.Length+256L;await using var q=SyncCommand(c,t,"INSERT INTO local_projection_unresolved VALUES($projection,$usn,$raw,$hash,$code,0,$bytes)");ProjectionId(q,id);q.Parameters.AddWithValue("$usn",record.Usn);q.Parameters.AddWithValue("$raw",raw);q.Parameters.AddWithValue("$hash",ProjectionHash(raw));q.Parameters.AddWithValue("$code",code);q.Parameters.AddWithValue("$bytes",bytes);await q.ExecuteNonQueryAsync(ct);return bytes;}
    private static async Task<long> StoreProjectionRenameOld(SqliteConnection c,SqliteTransaction t,string id,LocalUsnRecord record,byte[] raw,CancellationToken ct)
    {
        if((record.Reason&0x1000)==0||record.ReadMode!=LocalJournalReadMode.Named)return 0;
        // A bounded lookup hint, not historical pairing/membership proof. If
        // this hint quota fills, every further OLD record still survives raw.
        await using(var count=SyncCommand(c,t,"SELECT COUNT(*) FROM (SELECT 1 FROM local_projection_rename WHERE projection_id=$projection AND major=$major AND wire_id=$wire LIMIT 16)"))
        {ProjectionId(count,id);count.Parameters.AddWithValue("$major",record.MajorVersion);count.Parameters.AddWithValue("$wire",record.FileIdHex);if(Convert.ToInt64(await count.ExecuteScalarAsync(ct))>=16)return 0;}
        var bytes=192L+Encoding.UTF8.GetByteCount(record.Name);
        await using var q=SyncCommand(c,t,"INSERT INTO local_projection_rename VALUES($projection,$major,$wire,$usn,$parent,$name,$hash,$bytes)");ProjectionId(q,id);q.Parameters.AddWithValue("$major",record.MajorVersion);q.Parameters.AddWithValue("$wire",record.FileIdHex);q.Parameters.AddWithValue("$usn",record.Usn);q.Parameters.AddWithValue("$parent",record.ParentIdHex);q.Parameters.AddWithValue("$name",record.Name);q.Parameters.AddWithValue("$hash",ProjectionHash(raw));q.Parameters.AddWithValue("$bytes",bytes);await q.ExecuteNonQueryAsync(ct);return bytes;
    }
    private static async Task<long> WriteProjectionEdge(SqliteConnection c,SqliteTransaction t,ProjectionRow row,string parent,string name,ProjectionAlias node,long usn,bool tombstone,CancellationToken ct)
    {
        var current=await ProjectionEdgeAsync(c,t,row,parent,name,ct);
        if(!tombstone&&current is not null&&!current.Tombstone&&(current.Node!=node.Node||current.Created!=node.Created))throw new LocalSourceGapException("ProjectionCanonicalCollision","Current sibling canonical ownership conflicts.");
        var key=WebDavSyncClient.CanonicalPathKey(name);long previous=0;await using(var q=SyncCommand(c,t,"SELECT bytes FROM local_projection_edges WHERE projection_id=$projection AND parent_id=$parent AND canonical=$key")){ProjectionId(q,row.State.ProjectionId);q.Parameters.AddWithValue("$parent",parent);q.Parameters.AddWithValue("$key",key);if(await q.ExecuteScalarAsync(ct)is long value)previous=value;}
        var bytes=256L+Encoding.UTF8.GetByteCount(name);await using(var q=SyncCommand(c,t,"INSERT INTO local_projection_edges VALUES($projection,$parent,$key,$name,$node,$created,$kind,$action,$usn,$bytes) ON CONFLICT(projection_id,parent_id,canonical) DO UPDATE SET name=excluded.name,node_id=excluded.node_id,created=excluded.created,kind=excluded.kind,action=excluded.action,source_usn=excluded.source_usn,bytes=excluded.bytes"))
        {ProjectionId(q,row.State.ProjectionId);q.Parameters.AddWithValue("$parent",parent);q.Parameters.AddWithValue("$key",key);q.Parameters.AddWithValue("$name",name);q.Parameters.AddWithValue("$node",node.Node);q.Parameters.AddWithValue("$created",node.Created);q.Parameters.AddWithValue("$kind",node.Kind);q.Parameters.AddWithValue("$action",tombstone?1:0);q.Parameters.AddWithValue("$usn",usn);q.Parameters.AddWithValue("$bytes",bytes);await q.ExecuteNonQueryAsync(ct);}
        return bytes-previous;
    }
    internal async Task<LocalProjectionApply> ApplyOwnedProjectionAsync(string root,string owner,string id,long expected,LocalJournalAdmission local,CancellationToken ct)
    {
        if(expected<0)throw new ArgumentOutOfRangeException(nameof(expected));
        try
        {
            var capacity=LocalProjectionApplyCapacity;
            // Check lost-response replay without new WAL/disk admission. The
            // owner/binding and persisted exact boundary are still required.
            await using(var prior=await OpenProjectionAsync(ct))using(var read=prior.BeginTransaction(deferred:true))
            {
                var previous=await RequireProjectionAsync(prior,read,root,owner,id,false,ct);
                if(previous.State.AppliedUsn!=expected)
                {await using var replay=SyncCommand(prior,read,"SELECT last_end,last_records,last_known,last_unresolved FROM local_projection_roots WHERE projection_id=$projection AND last_start=$start AND last_end=applied_usn");ProjectionId(replay,id);replay.Parameters.AddWithValue("$start",expected);await using var r=await replay.ExecuteReaderAsync(ct);if(await r.ReadAsync(ct))return new(true,r.GetInt32(1),r.GetInt32(2),r.GetInt32(3),r.GetInt64(0),0);throw new InvalidOperationException("Expected applied boundary changed.");}
            }
            await ProjectionAdmission(root,owner,id,local,capacity*8+4,ct);
            await using var c=await OpenProjectionAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var row=await RequireProjectionAsync(c,t,root,owner,id,false,ct);
            if(row.State.AppliedUsn!=expected)
            {await using var replay=SyncCommand(c,t,"SELECT last_end,last_records,last_known,last_unresolved FROM local_projection_roots WHERE projection_id=$projection AND last_start=$start AND last_end=applied_usn");ProjectionId(replay,id);replay.Parameters.AddWithValue("$start",expected);await using var r=await replay.ExecuteReaderAsync(ct);if(await r.ReadAsync(ct))return new(true,r.GetInt32(1),r.GetInt32(2),r.GetInt32(3),r.GetInt64(0),0);throw new InvalidOperationException("Expected applied boundary changed.");}
            var records=new List<(LocalUsnRecord Record,byte[] Raw)>(capacity+1);
            await using(var q=SyncCommand(c,t,"SELECT usn,payload FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn>=$start ORDER BY usn LIMIT $limit"))
            {q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",row.State.JournalId);q.Parameters.AddWithValue("$start",expected);q.Parameters.AddWithValue("$limit",capacity+1);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct)){var raw=(byte[])r.GetValue(1);records.Add((ProjectionRecord(row.State.JournalId,r.GetInt64(0),row.State.IngestUsn,raw),raw));}}
            var next=records.Count>capacity?records[capacity].Record.Usn:row.State.IngestUsn;
            if(next<expected||next>row.State.IngestUsn)throw new LocalSourceGapException("ProjectionBoundaryGap","Validated prefix boundary is unavailable.");
            var count=Math.Min(capacity,records.Count);if(count==0&&next==expected){await t.CommitAsync(ct);return new(true,0,0,0,next,0);}
            long bytes=0;int dirty=0,known=0,unknown=0;var prefixHash=new StringBuilder();
            var rootNode=(await ReadNamespaceAsync(c,t,row.State.CandidateId,ct)).State.RootFileId128;
            foreach(var item in records.Take(count))
            {
                ct.ThrowIfCancellationRequested();var record=item.Record;prefixHash.Append(ProjectionHash(item.Raw));
                bytes+=await StoreProjectionRenameOld(c,t,id,record,item.Raw,ct);
                var node=await ProjectionAliasAsync(c,t,row,record.MajorVersion,record.FileIdHex,ct);var parent=await ProjectionAliasAsync(c,t,row,record.MajorVersion,record.ParentIdHex,ct);
                if(node is null||node.Kind!=((record.FileAttributes&0x10)!=0?1:0)){bytes+=await StoreProjectionUnknown(c,t,id,record,item.Raw,node is not null?"HistoricalTypeUnresolved":parent?.Kind==1?"UnknownIdentity":"UnknownHistoricalMembership",ct);unknown++;continue;}
                var dirtied=await DirtyProjectionNode(c,t,id,node,record,ct);bytes+=dirtied.Bytes;dirty+=dirtied.Added;
                if(record.ReadMode==LocalJournalReadMode.IdentityOnly&&(record.Reason&0x00313370)!=0)
                {bytes+=await StoreProjectionUnknown(c,t,id,record,item.Raw,"IdentityOnlyNamespaceUnresolved",ct);unknown++;continue;}
                // Root moves, creates/ID reuse, reparse, streams and hardlinks
                // require more evidence. Their raw historical record survives.
                if(node.Node==rootNode||(record.Reason&0x00310170)!=0||(record.Reason&0x3000)!=0)
                {bytes+=await StoreProjectionUnknown(c,t,id,record,item.Raw,(record.Reason&0x3000)!=0?"HistoricalRenameUnresolved":"IncarnationOrPolicyUnresolved",ct);unknown++;continue;}
                if((record.Reason&0x200)!=0)
                {var edge=parent?.Kind==1?await ProjectionEdgeAsync(c,t,row,parent.Node,record.Name,ct):null;if(edge is null||edge.Tombstone||edge.Node!=node.Node||edge.Name!=record.Name||edge.Created!=node.Created){bytes+=await StoreProjectionUnknown(c,t,id,record,item.Raw,"HistoricalDeleteUnresolved",ct);unknown++;continue;}bytes+=await WriteProjectionEdge(c,t,row,parent!.Node,record.Name,node,record.Usn,true,ct);}
                known++;
            }
            await using(var q=SyncCommand(c,t,"SELECT bytes FROM local_projection_format WHERE singleton=1"))if(Convert.ToInt64(await q.ExecuteScalarAsync(ct))>row.Admission.MaximumProjectionBytes-bytes)throw new LocalSourceGapException("ProjectionCapacity","Projection dispositions exceed logical quota; prefix remains unapplied.");
            await using(var q=SyncCommand(c,t,"UPDATE local_projection_roots SET applied_usn=$end,dirty_count=dirty_count+$dirty,unresolved_count=unresolved_count+$unknown,bytes=bytes+$bytes,last_start=$start,last_end=$end,last_hash=$hash,last_records=$records,last_known=$known,last_unresolved=$unknown WHERE projection_id=$projection;UPDATE local_projection_format SET bytes=bytes+$bytes WHERE singleton=1"))
            {ProjectionId(q,id);q.Parameters.AddWithValue("$end",next);q.Parameters.AddWithValue("$dirty",dirty);q.Parameters.AddWithValue("$unknown",unknown);q.Parameters.AddWithValue("$bytes",bytes);q.Parameters.AddWithValue("$start",expected);q.Parameters.AddWithValue("$hash",LocalHash(prefixHash.ToString()));q.Parameters.AddWithValue("$records",count);q.Parameters.AddWithValue("$known",known);await q.ExecuteNonQueryAsync(ct);}
            return new(false,count,known,unknown,next,await CommitProjection(c,t,before,ct));
        }
        catch(LocalSourceGapException error){try{await GapProjection(root,owner,id,error.Code,CancellationToken.None);}catch(InvalidOperationException){/* fenced owner cannot alter successor */}throw;}
    }
}

public sealed partial class LocalSourceJournalSession
{
    public int LocalProjectionApplyCapacity=>_store.LocalProjectionApplyCapacity;
    public Task<LocalProjectionState> BeginLocalProjectionAsync(string candidate,LocalProjectionAdmission? admission=null,CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.BeginOwnedProjectionAsync(_root,_owner,candidate,_admission,admission??new(),cancellationToken),cancellationToken);
    public Task<LocalProjectionState> ReadLocalProjectionAsync(string projection,CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.ReadOwnedProjectionAsync(_root,_owner,projection,cancellationToken),cancellationToken);
    public Task<LocalProjectionApply> ApplyNextLocalInboxAsync(string projection,long expectedAppliedBoundary,CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.ApplyOwnedProjectionAsync(_root,_owner,projection,expectedAppliedBoundary,_admission,cancellationToken),cancellationToken);
}
