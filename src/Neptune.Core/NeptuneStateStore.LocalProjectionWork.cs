using System.Globalization;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    internal async Task<IReadOnlyList<LocalProjectionDirty>> ReadOwnedProjectionDirtyAsync(string root,string owner,string id,long afterUsn,string afterNode,CancellationToken ct)
    {
        if(afterUsn< -1||afterNode.Length>32)throw new ArgumentOutOfRangeException(nameof(afterUsn));await using var c=await OpenProjectionAsync(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireProjectionAsync(c,t,root,owner,id,true,ct);
        var rows=new List<LocalProjectionDirty>(256);await using var q=SyncCommand(c,t,"SELECT node_id,created,generation,last_usn,reasons,kind,subtree_epoch FROM local_projection_nodes WHERE projection_id=$projection AND (last_usn>$usn OR last_usn=$usn AND node_id>$node) ORDER BY last_usn,node_id LIMIT 256");ProjectionId(q,id);q.Parameters.AddWithValue("$usn",afterUsn);q.Parameters.AddWithValue("$node",afterNode);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))rows.Add(new(r.GetString(0),r.GetInt64(1),r.GetInt64(2),r.GetInt64(3),(uint)r.GetInt64(4),r.GetInt32(5)==1,r.GetInt64(6)));return rows.AsReadOnly();
    }
    internal async Task<IReadOnlyList<LocalProjectionUnresolved>> ReadOwnedProjectionUnresolvedAsync(string root,string owner,string id,long afterUsn,CancellationToken ct)
    {
        if(afterUsn< -1)throw new ArgumentOutOfRangeException(nameof(afterUsn));await using var c=await OpenProjectionAsync(ct);using var t=c.BeginTransaction(deferred:true);var row=await RequireProjectionAsync(c,t,root,owner,id,true,ct);
        var rows=new List<LocalProjectionUnresolved>(256);await using var q=SyncCommand(c,t,"SELECT usn,payload,sha256,code,state FROM local_projection_unresolved WHERE projection_id=$projection AND usn>$usn ORDER BY usn LIMIT 256");ProjectionId(q,id);q.Parameters.AddWithValue("$usn",afterUsn);await using var r=await q.ExecuteReaderAsync(ct);
        while(await r.ReadAsync(ct)){var raw=(byte[])r.GetValue(1);if(ProjectionHash(raw)!=r.GetString(2))throw new InvalidDataException("Retained unresolved payload changed.");var record=ProjectionRecord(row.State.JournalId,r.GetInt64(0),row.State.IngestUsn,raw);rows.Add(new(record.Usn,record.MajorVersion,record.FileIdHex,record.ParentIdHex,record.Name,record.Reason,r.GetString(3),r.GetString(2),r.GetInt32(4)==1));}return rows.AsReadOnly();
    }
    private static async Task<string> ProjectionDirectoryPath(SqliteConnection c,SqliteTransaction t,ProjectionRow row,string node,string rootNode,CancellationToken ct)
    {
        var names=new List<string>();var seen=new HashSet<string>(StringComparer.Ordinal);
        while(node!=rootNode)
        {
            if(names.Count>=512||!seen.Add(node))throw new LocalSourceGapException("ProjectionAncestryGap","Cyclic/deep ancestry cannot qualify a native path.");
            await using var q=SyncCommand(c,t,"SELECT parent_id,name FROM local_projection_edges WHERE projection_id=$projection AND node_id=$node AND kind=1 AND action=0 UNION ALL SELECT e.parent_id,e.name FROM local_namespace_entries e WHERE e.candidate_id=$candidate AND e.node_id=$node AND e.kind=1 AND NOT EXISTS(SELECT 1 FROM local_projection_edges p WHERE p.projection_id=$projection AND p.parent_id=e.parent_id AND p.canonical=e.canonical) LIMIT 2");ProjectionId(q,row.State.ProjectionId);q.Parameters.AddWithValue("$candidate",row.State.CandidateId);q.Parameters.AddWithValue("$node",node);
            var edges=new List<(string Parent,string Name)>(2);await using(var r=await q.ExecuteReaderAsync(ct))while(await r.ReadAsync(ct))edges.Add((r.GetString(0),r.GetString(1)));if(edges.Count!=1)throw new LocalSourceGapException("ProjectionAncestryGap","Directory ancestry is missing/ambiguous.");names.Add(edges[0].Name);node=edges[0].Parent;
        }
        names.Reverse();var path=string.Join('/',names);if(path.Length>4096)throw new LocalSourceGapException("ProjectionAncestryGap","Current ancestry path exceeds its bound.");return path;
    }
    internal async Task<LocalProjectionMetadataClaim?> ReadOwnedProjectionClaimAsync(string root,string owner,string id,long usn,CancellationToken ct)
    {
        if(usn<0)throw new ArgumentOutOfRangeException(nameof(usn));await using var c=await OpenProjectionAsync(ct);using var t=c.BeginTransaction(deferred:true);var row=await RequireProjectionAsync(c,t,root,owner,id,false,ct);
        byte[] raw;string hash;await using(var q=SyncCommand(c,t,"SELECT payload,sha256 FROM local_projection_unresolved WHERE projection_id=$projection AND usn=$usn AND state=0")){ProjectionId(q,id);q.Parameters.AddWithValue("$usn",usn);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))return null;raw=(byte[])r.GetValue(0);hash=r.GetString(1);}
        if(ProjectionHash(raw)!=hash)throw new InvalidDataException("Unresolved identity changed.");var record=ProjectionRecord(row.State.JournalId,usn,row.State.IngestUsn,raw);
        // This named resolver cannot invent the missing historical leaf alias.
        if(record.ReadMode!=LocalJournalReadMode.Named)return null;
        var parent=await ProjectionAliasAsync(c,t,row,record.MajorVersion,record.ParentIdHex,ct);if(parent is null||parent.Kind!=1)return null;
        var source=await ReadNamespaceAsync(c,t,row.State.CandidateId,ct);var local=await ReadLocalStateAsync(c,t,root,owner,ct);
        long created;bool caseSensitive;await using(var q=SyncCommand(c,t,"SELECT n.root_created,p.case_sensitive FROM local_namespace_candidates n JOIN local_namespace_parents p ON p.candidate_id=n.candidate_id AND p.node_id=n.root_id WHERE n.candidate_id=$candidate")){q.Parameters.AddWithValue("$candidate",row.State.CandidateId);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))throw new InvalidDataException("Root proof is absent.");created=r.GetInt64(0);caseSensitive=r.GetInt32(1)!=0;}
        string legacy;await using(var q=SyncCommand(c,t,"SELECT wire_id FROM local_namespace_aliases WHERE candidate_id=$candidate AND node_id=$node AND major=2 LIMIT 1")){q.Parameters.AddWithValue("$candidate",row.State.CandidateId);q.Parameters.AddWithValue("$node",source.State.RootFileId128);legacy=await q.ExecuteScalarAsync(ct)as string??throw new InvalidDataException("Root exact legacy alias is absent.");}
        var proof=new LocalNamespaceRootProof(local.Binding.SourceRoot,local.Binding.VolumeGuid,local.Binding.FileSystem,new(source.State.RootFileId128,legacy,source.Serial64,uint.Parse(local.Binding.VolumeSerial,NumberStyles.HexNumber,CultureInfo.InvariantCulture),created,0x10,caseSensitive));
        var path=await ProjectionDirectoryPath(c,t,row,parent.Node,source.State.RootFileId128,ct);return new(id,usn,hash,record,proof,new(row.State.CandidateId,parent.Node,path,parent.Created,id));
    }
    internal async Task<bool> CommitOwnedProjectionProofAsync(string root,string owner,LocalProjectionMetadataProof proof,LocalJournalAdmission local,CancellationToken ct)
    {
        var claim=proof.Claim;var id=claim.ProjectionId;var h=proof.Handle;
        if(claim.Record.ReadMode!=LocalJournalReadMode.Named)throw new InvalidDataException("Named metadata proof cannot qualify an identity-only event.");
        static bool Hex(string s,int n)=>s.Length==n&&s==s.ToLowerInvariant()&&s.All(char.IsAsciiHexDigit);
        if(!Hex(h.FileId128,32)||h.FileId128==new string('0',32)||!Hex(h.LegacyFileId64,16)||!Hex(h.VolumeSerial64,16)||h.CreationTime<0||proof.Links<1||(h.Attributes&(0x400|0x1000|0x40000|0x400000))!=0||((h.Attributes&0x10)!=0)!=((claim.Record.FileAttributes&0x10)!=0))throw new InvalidDataException("Native proof is malformed/excluded or has a different type.");
        if((claim.Record.MajorVersion==2?h.LegacyFileId64:h.FileId128)!=claim.Record.FileIdHex||h.VolumeSerial64!=claim.Root.Handle.VolumeSerial64||h.LegacyVolumeSerial!=claim.Root.Handle.LegacyVolumeSerial)throw new InvalidDataException("Native proof does not match raw event/volume identity.");
        await using(var prior=await OpenProjectionAsync(ct))using(var read=prior.BeginTransaction(deferred:true))
        {
            var priorRow=await RequireProjectionAsync(prior,read,root,owner,id,false,ct);
            await using var q=SyncCommand(prior,read,"SELECT payload,sha256,state FROM local_projection_unresolved WHERE projection_id=$projection AND usn=$usn");ProjectionId(q,id);q.Parameters.AddWithValue("$usn",claim.Usn);await using var r=await q.ExecuteReaderAsync(ct);
            if(!await r.ReadAsync(ct)||r.GetString(1)!=claim.PayloadSha256||ProjectionHash((byte[])r.GetValue(0))!=claim.PayloadSha256||ProjectionRecord(priorRow.State.JournalId,claim.Usn,priorRow.State.IngestUsn,(byte[])r.GetValue(0))!=claim.Record)throw new InvalidOperationException("Unresolved claim changed.");
            if(r.GetInt32(2)!=0)
            {
                var persistedState=r.GetInt32(2);await r.DisposeAsync();
                var expected=new ProjectionAlias(h.FileId128,h.CreationTime,(h.Attributes&0x10)!=0?1:0);
                if(await ProjectionAliasAsync(prior,read,priorRow,3,h.FileId128,ct)!=expected||await ProjectionAliasAsync(prior,read,priorRow,2,h.LegacyFileId64,ct)!=expected)throw new InvalidOperationException("Repeated native proof differs from durable exact incarnation aliases.");
                return persistedState==1;
            }
        }
        await ProjectionAdmission(root,owner,id,local,12,ct);await using var c=await OpenProjectionAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var row=await RequireProjectionAsync(c,t,root,owner,id,false,ct);
        byte[] raw;int state;await using(var q=SyncCommand(c,t,"SELECT payload,sha256,state FROM local_projection_unresolved WHERE projection_id=$projection AND usn=$usn")){ProjectionId(q,id);q.Parameters.AddWithValue("$usn",claim.Usn);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)||r.GetString(1)!=claim.PayloadSha256)throw new InvalidOperationException("Unresolved claim changed.");raw=(byte[])r.GetValue(0);state=r.GetInt32(2);}
        if(ProjectionHash(raw)!=claim.PayloadSha256||ProjectionRecord(row.State.JournalId,claim.Usn,row.State.IngestUsn,raw)!=claim.Record)throw new InvalidDataException("Original event proof changed.");
        if(state!=0){await t.CommitAsync(ct);return state==1;}
        var source=await ReadNamespaceAsync(c,t,row.State.CandidateId,ct);if(source.State.RootFileId128!=claim.Root.Handle.FileId128||source.Serial64!=h.VolumeSerial64||source.State.CandidateId!=claim.Parent.CandidateId)throw new InvalidOperationException("Native proof root binding changed.");
        var parent=await ProjectionAliasAsync(c,t,row,claim.Record.MajorVersion,claim.Record.ParentIdHex,ct);if(parent is null||parent.Node!=claim.Parent.FileId128||parent.Created!=claim.Parent.CreationTime||parent.Kind!=1||await ProjectionDirectoryPath(c,t,row,parent.Node,source.State.RootFileId128,ct)!=claim.Parent.RelativePath)throw new InvalidOperationException("Resolved parent/path changed.");
        var node=new ProjectionAlias(h.FileId128,h.CreationTime,(h.Attributes&0x10)!=0?1:0);
        await using(var q=SyncCommand(c,t,"SELECT kind,created FROM local_namespace_nodes WHERE candidate_id=$candidate AND node_id=$node")){q.Parameters.AddWithValue("$candidate",row.State.CandidateId);q.Parameters.AddWithValue("$node",node.Node);await using var r=await q.ExecuteReaderAsync(ct);if(await r.ReadAsync(ct)&&(r.GetInt32(0)!=node.Kind||r.GetInt64(1)!=node.Created))throw new LocalSourceGapException("ProjectionIncarnationGap","Present object differs from candidate incarnation.");}
        await using(var q=SyncCommand(c,t,"SELECT last_usn FROM local_projection_nodes WHERE projection_id=$projection AND node_id=$node")){ProjectionId(q,id);q.Parameters.AddWithValue("$node",node.Node);if(await q.ExecuteScalarAsync(ct)is long later&&later>claim.Usn)throw new InvalidOperationException("A later dirty generation fences this metadata claim.");}
        long bytes=0;var dirty=await DirtyProjectionNode(c,t,id,node,claim.Record,ct);bytes+=dirty.Bytes;
        foreach(var (major,wire) in new[]{(2,h.LegacyFileId64),(3,h.FileId128)})
        {
            var existing=await ProjectionAliasAsync(c,t,row,(ushort)major,wire,ct);if(existing is not null&&existing!=node)throw new LocalSourceGapException("ProjectionAliasRebind","Native exact alias was rebound.");
            await using var q=SyncCommand(c,t,"INSERT INTO local_projection_aliases VALUES($projection,$major,$wire,$node,$created,$serial,$usn,192) ON CONFLICT(projection_id,major,wire_id) DO NOTHING");ProjectionId(q,id);q.Parameters.AddWithValue("$major",major);q.Parameters.AddWithValue("$wire",wire);q.Parameters.AddWithValue("$node",node.Node);q.Parameters.AddWithValue("$created",node.Created);q.Parameters.AddWithValue("$serial",h.VolumeSerial64);q.Parameters.AddWithValue("$usn",claim.Usn);if(await q.ExecuteNonQueryAsync(ct)==1)bytes+=192;
        }
        // A current present handle qualifies ordinary data/basic events only.
        // Historical create/delete/rename/link/policy ambiguity remains raw.
        var qualified=proof.Links==1&&(claim.Record.Reason&0x00313370)==0;
        if(qualified)bytes+=await WriteProjectionEdge(c,t,row,parent.Node,claim.Record.Name,node,claim.Usn,false,ct);
        await using(var q=SyncCommand(c,t,"SELECT bytes FROM local_projection_format WHERE singleton=1"))if(Convert.ToInt64(await q.ExecuteScalarAsync(ct))>row.Admission.MaximumProjectionBytes-bytes)throw new LocalSourceGapException("ProjectionCapacity","Metadata proof exceeds retained ledger quota.");
        await using(var q=SyncCommand(c,t,"UPDATE local_projection_unresolved SET state=$state,code=$code WHERE projection_id=$projection AND usn=$usn;UPDATE local_projection_roots SET dirty_count=dirty_count+$dirty,unresolved_count=unresolved_count-$resolved,bytes=bytes+$bytes WHERE projection_id=$projection;UPDATE local_projection_format SET bytes=bytes+$bytes WHERE singleton=1"))
        {ProjectionId(q,id);q.Parameters.AddWithValue("$state",qualified?1:2);q.Parameters.AddWithValue("$code",qualified?"PresentMetadataQualified":"PresentMetadataHistoricalUnresolved");q.Parameters.AddWithValue("$usn",claim.Usn);q.Parameters.AddWithValue("$dirty",dirty.Added);q.Parameters.AddWithValue("$resolved",qualified?1:0);q.Parameters.AddWithValue("$bytes",bytes);await q.ExecuteNonQueryAsync(ct);}
        await CommitProjection(c,t,before,ct);return qualified;
    }
    internal async Task<LocalProjectionRetirement> RetireAppliedProjectionInboxAsync(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        var limit=NamespaceMutationCapacity;await ProjectionAdmission(root,owner,id,local,limit+4,ct);await using var c=await OpenProjectionAsync(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var row=await RequireProjectionAsync(c,t,root,owner,id,false,ct);
        var records=new List<(long Usn,long Bytes,byte[] Raw)>(limit);await using(var q=SyncCommand(c,t,"SELECT usn,byte_count,payload FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn<$end ORDER BY usn LIMIT $limit")){q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",row.State.JournalId);q.Parameters.AddWithValue("$end",row.State.AppliedUsn);q.Parameters.AddWithValue("$limit",limit);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))records.Add((r.GetInt64(0),r.GetInt64(1),(byte[])r.GetValue(2)));}
        if(records.Count>0)await RequireNoCoherenceInboxPin(c,t,root,row.State.JournalId,records[0].Usn,checked(records[^1].Usn+1),ct);
        foreach(var item in records)
        {
            if(item.Usn<row.State.StartUsn)throw new LocalSourceGapException("ProjectionRetirementGap","Earlier unqualified prefix needs its original projection/recovery.");
            var record=ProjectionRecord(row.State.JournalId,item.Usn,row.State.IngestUsn,item.Raw);bool durable;
            await using(var q=SyncCommand(c,t,"SELECT sha256 FROM local_projection_unresolved WHERE projection_id=$projection AND usn=$usn")){ProjectionId(q,id);q.Parameters.AddWithValue("$usn",item.Usn);var hash=await q.ExecuteScalarAsync(ct)as string;durable=hash==ProjectionHash(item.Raw);}
            if(!durable){var node=await ProjectionAliasAsync(c,t,row,record.MajorVersion,record.FileIdHex,ct);if(node is null)throw new InvalidDataException("Processed record has no durable disposition.");await using var q=SyncCommand(c,t,"SELECT last_usn FROM local_projection_nodes WHERE projection_id=$projection AND node_id=$node");ProjectionId(q,id);q.Parameters.AddWithValue("$node",node.Node);durable=await q.ExecuteScalarAsync(ct)is long last&&last>=item.Usn;}
            if(!durable)throw new InvalidDataException("Processed record disposition is unavailable.");
        }
        long bytes=0;foreach(var item in records){ct.ThrowIfCancellationRequested();await using var q=SyncCommand(c,t,"DELETE FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn=$usn");q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",row.State.JournalId);q.Parameters.AddWithValue("$usn",item.Usn);await q.ExecuteNonQueryAsync(ct);bytes+=item.Bytes;}
        long boundary=row.State.AppliedUsn;await using(var q=SyncCommand(c,t,"SELECT MIN(usn) FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal")){q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",row.State.JournalId);if(await q.ExecuteScalarAsync(ct)is long next)boundary=Math.Min(boundary,next);}
        await using(var q=SyncCommand(c,t,"UPDATE local_source_roots SET inbox_records=inbox_records-$count,inbox_bytes=inbox_bytes-$bytes,retired_through=MAX(retired_through,$last) WHERE root_key=$root;UPDATE local_source_format SET inbox_records=inbox_records-$count,inbox_bytes=inbox_bytes-$bytes WHERE singleton=1;INSERT INTO local_projection_retired VALUES($root,$journal,$boundary) ON CONFLICT(root_key) DO UPDATE SET boundary=MAX(boundary,excluded.boundary) WHERE journal_id=excluded.journal_id"))
        {q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",row.State.JournalId);q.Parameters.AddWithValue("$count",records.Count);q.Parameters.AddWithValue("$bytes",bytes);q.Parameters.AddWithValue("$last",records.Count==0?0:records[^1].Usn);q.Parameters.AddWithValue("$boundary",boundary);await q.ExecuteNonQueryAsync(ct);}
        return new(records.Count,bytes,false,await CommitProjection(c,t,before,ct));
    }
    internal async Task<LocalProjectionRetirement> RetireOwnedProjectionAsync(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await using var c=await OpenProjectionAsync(ct);using var read=c.BeginTransaction(deferred:true);_=await ReadLocalStateAsync(c,read,root,owner,ct);var row=await ReadProjectionAsync(c,read,id,0,ct);
        if(row.RootKey!=root||row.State.Status!=LocalProjectionStatus.Superseded)throw new InvalidOperationException("Only explicit superseded projection history may retire; completeness remains unknown.");
        await read.CommitAsync(ct);var limit=NamespaceMutationCapacity;await CheckLocalAdmissionAsync(ProjectionBudget(local,row.Admission),0,NamespaceReserve(limit+4),ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await ReadLocalStateAsync(c,t,root,owner,ct);row=await ReadProjectionAsync(c,t,id,0,ct);
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_projection_active WHERE projection_id=$projection")){ProjectionId(q,id);if(await q.ExecuteScalarAsync(ct)is not null||row.State.Status!=LocalProjectionStatus.Superseded)throw new InvalidOperationException("Active projection cannot retire.");}
        int phase;await using(var q=SyncCommand(c,t,"SELECT retire_phase FROM local_projection_roots WHERE projection_id=$projection")){ProjectionId(q,id);phase=Convert.ToInt32(await q.ExecuteScalarAsync(ct));}
        var tables=new[]{"local_projection_unresolved","local_projection_rename","local_projection_aliases","local_projection_edges","local_projection_nodes"};var keys=new List<(object A,object? B,object? C,long Bytes)>();
        if(phase<5)
        {
            var columns=phase switch{0=>"usn,NULL,NULL,bytes",1=>"major,wire_id,usn,bytes",2=>"major,wire_id,NULL,bytes",3=>"parent_id,canonical,NULL,bytes",_=>"node_id,NULL,NULL,bytes"};await using(var q=SyncCommand(c,t,$"SELECT {columns} FROM {tables[phase]} WHERE projection_id=$projection LIMIT $limit")){ProjectionId(q,id);q.Parameters.AddWithValue("$limit",limit);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))keys.Add((r.GetValue(0),r.IsDBNull(1)?null:r.GetValue(1),r.IsDBNull(2)?null:r.GetValue(2),r.GetInt64(3)));}
            foreach(var key in keys){var where=phase switch{0=>"usn=$a",1=>"major=$a AND wire_id=$b AND usn=$c",2=>"major=$a AND wire_id=$b",3=>"parent_id=$a AND canonical=$b",_=>"node_id=$a"};await using var q=SyncCommand(c,t,$"DELETE FROM {tables[phase]} WHERE projection_id=$projection AND {where}");ProjectionId(q,id);q.Parameters.AddWithValue("$a",key.A);if(key.B is not null)q.Parameters.AddWithValue("$b",key.B);if(key.C is not null)q.Parameters.AddWithValue("$c",key.C);await q.ExecuteNonQueryAsync(ct);}
        }
        var removed=phase==5;var bytes=removed?row.State.Bytes:keys.Sum(x=>x.Bytes);
        if(removed){await using var q=SyncCommand(c,t,"DELETE FROM local_projection_roots WHERE projection_id=$projection");ProjectionId(q,id);await q.ExecuteNonQueryAsync(ct);}
        else{await using var q=SyncCommand(c,t,"UPDATE local_projection_roots SET bytes=bytes-$bytes,retire_phase=retire_phase+$next WHERE projection_id=$projection");ProjectionId(q,id);q.Parameters.AddWithValue("$bytes",bytes);q.Parameters.AddWithValue("$next",keys.Count<limit?1:0);await q.ExecuteNonQueryAsync(ct);}
        await using(var q=SyncCommand(c,t,"UPDATE local_projection_format SET bytes=bytes-$bytes WHERE singleton=1")){q.Parameters.AddWithValue("$bytes",bytes);await q.ExecuteNonQueryAsync(ct);}
        return new(keys.Count,bytes,removed,await CommitProjection(c,t,before,ct));
    }
}

public sealed partial class LocalSourceJournalSession
{
    public Task<IReadOnlyList<LocalProjectionDirty>> ReadLocalDirtyPageAsync(string projection,long afterUsn=-1,string afterNode="",CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.ReadOwnedProjectionDirtyAsync(_root,_owner,projection,afterUsn,afterNode,cancellationToken),cancellationToken);
    public Task<IReadOnlyList<LocalProjectionUnresolved>> ReadLocalUnresolvedPageAsync(string projection,long afterUsn=-1,CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.ReadOwnedProjectionUnresolvedAsync(_root,_owner,projection,afterUsn,cancellationToken),cancellationToken);
    public Task<LocalProjectionMetadataClaim?> ReadLocalMetadataClaimAsync(string projection,long usn,CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.ReadOwnedProjectionClaimAsync(_root,_owner,projection,usn,cancellationToken),cancellationToken);
    public Task<bool> CommitLocalMetadataProofAsync(LocalProjectionMetadataProof proof,CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.CommitOwnedProjectionProofAsync(_root,_owner,proof,_admission,cancellationToken),cancellationToken);
    public Task<LocalProjectionRetirement> RetireAppliedInboxBatchAsync(string projection,CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.RetireAppliedProjectionInboxAsync(_root,_owner,projection,_admission,cancellationToken),cancellationToken);
    public Task<LocalProjectionRetirement> RetireLocalProjectionBatchAsync(string supersededProjection,CancellationToken cancellationToken=default)=>SerialAsync(()=>_store.RetireOwnedProjectionAsync(_root,_owner,supersededProjection,_admission,cancellationToken),cancellationToken);
}
