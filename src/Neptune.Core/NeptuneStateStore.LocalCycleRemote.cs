using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    // Resolve the unique remote directory before probing its active original.
    // An IN subquery lets SQLite scan all active publications in newest-first
    // order, once per ancestor, until the next feed ACK retires that history.
    private const string PublishedDirectoryTargetSql="SELECT w.result_json FROM local_cycle_files f CROSS JOIN local_cycle_work w ON w.scope_id=f.scope_id AND w.node_id=f.node_id WHERE f.scope_id=$scope AND f.remote_node=$node AND f.kind=1 AND w.phase=5 AND w.active=1 ORDER BY w.work_no DESC LIMIT 1";
    internal async Task ValidateOwnedCycleAnchor(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct,bool pending=false)
    {
        await CycleAdmission(root,owner,id,local,4,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);if(!(await CycleProgress(c,t,id,ct,pending)).Complete)throw new InvalidOperationException("Remote anchor requires a complete applied catalog prefix.");
        var next=scope.Configuration.RemoteAnchorId??scope.Configuration.Binding.RootId;var chain=new List<CatalogParentTopology>();var seen=new HashSet<string>();
        while(true){if(chain.Count>=129||!seen.Add(next))throw new InvalidDataException("Mapping anchor has cyclic/unbounded ancestry.");var d=await CycleRemoteNode(c,t,id,next,ct,pending);if(d is null||d.Type!="folder"||d.Availability!="AVAILABLE")throw new InvalidDataException("Mapping anchor is missing/trashed/not an available folder.");chain.Add(new(d.NodeId,d.ParentId,d.Name));if(d.ParentId is null){if(d.NodeId!=scope.Configuration.Binding.RootId)throw new InvalidDataException("Mapping anchor escaped enrolled root.");break;}next=d.ParentId;}
        var json=JsonSerializer.Serialize(chain,LocalCycleWire.Json);string? original;await using(var q=SyncCommand(c,t,"SELECT anchor_json FROM local_cycle_scopes WHERE scope_id=$scope")){CycleId(q,id);original=await q.ExecuteScalarAsync(ct)as string;}
        if(original is not null&&original!=json)throw new InvalidDataException("Original anchor/ancestor identity or raw names changed; explicit remapping required.");
        if(original is null){await using var q=SyncCommand(c,t,"UPDATE local_cycle_scopes SET anchor_json=$json WHERE scope_id=$scope AND anchor_json IS NULL");CycleId(q,id);q.Parameters.AddWithValue("$json",json);await q.ExecuteNonQueryAsync(ct);await AddCycleBytes(c,t,id,Encoding.UTF8.GetByteCount(json),ct);}await CommitCycle(c,t,before,ct);
    }
    private static async Task<LocalCycleRemoteProgress> CycleProgress(SqliteConnection c,SqliteTransaction t,string id,CancellationToken ct,bool pending=false)
    {
        var location=await CycleRemoteLocation(c,t,id,pending,ct);
        await using var q=SyncCommand(c,t,$"SELECT snapshot_id,ready_json,entries_cursor,after_node,changes_cursor,remote_seq,remote_complete,error,changes_from,snapshot_complete FROM {location.Progress} WHERE scope_id=$scope");CycleId(q,location.Id);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))throw new InvalidOperationException("Cycle progress absent.");
        string? S(int i)=>r.IsDBNull(i)?null:r.GetString(i);return new(S(0),S(1),S(2),S(3),S(4),r.GetInt64(5),r.GetInt32(6)!=0,S(7),r.GetInt64(8),r.GetInt32(9)!=0);
    }
    internal async Task<LocalCycleRemoteProgress> ReadOwnedCycleProgress(string root,string owner,string id,CancellationToken ct,bool pending=false)
    {await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,id,false,ct);return await CycleProgress(c,t,id,ct,pending);}
    internal async Task<string> ReserveOwnedCycleSnapshot(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct,bool pending=false)
    {
        await CycleAdmission(root,owner,id,local,2,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireCycle(c,t,root,owner,id,false,ct);var location=await CycleRemoteLocation(c,t,id,pending,ct);var p=await CycleProgress(c,t,id,ct,pending);var snapshot=p.SnapshotId??Guid.NewGuid().ToString("D");
        if(p.SnapshotId is null){await using var q=SyncCommand(c,t,$"UPDATE {location.Progress} SET snapshot_id=$snapshot WHERE scope_id=$scope AND snapshot_id IS NULL");CycleId(q,location.Id);q.Parameters.AddWithValue("$snapshot",snapshot);await q.ExecuteNonQueryAsync(ct);}await CommitCycle(c,t,before,ct);return snapshot;
    }
    internal async Task SaveOwnedCycleReady(string root,string owner,string id,SyncSnapshotReady ready,LocalJournalAdmission local,CancellationToken ct,bool pending=false)
    {
        await CycleAdmission(root,owner,id,local,3,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);var location=await CycleRemoteLocation(c,t,id,pending,ct);var p=await CycleProgress(c,t,id,ct,pending);
        if(ready.Binding!=scope.Configuration.Binding||ready.SnapshotId!=p.SnapshotId||p.ReadyJson is not null)throw new InvalidOperationException("Original snapshot binding cannot be reset.");
        var seq=LocalCycleWire.Decimal(ready.CutSeq);var raw=JsonSerializer.Serialize(ready,LocalCycleWire.Json);
        await using var q=SyncCommand(c,t,$"UPDATE {location.Progress} SET ready_json=$ready,entries_cursor=$entries,changes_cursor=$changes,changes_from=$seq,remote_seq=$seq WHERE scope_id=$scope AND ready_json IS NULL");CycleId(q,location.Id);q.Parameters.AddWithValue("$ready",raw);q.Parameters.AddWithValue("$entries",ready.EntriesCursor);q.Parameters.AddWithValue("$changes",ready.ChangesCursor);q.Parameters.AddWithValue("$seq",seq);await q.ExecuteNonQueryAsync(ct);await AddCycleBytes(c,t,id,Encoding.UTF8.GetByteCount(raw)+ready.EntriesCursor.Length+ready.ChangesCursor.Length,ct);if(pending){await using var account=SyncCommand(c,t,"UPDATE local_cycle_refreshes SET bytes=bytes+$bytes WHERE scope_id=$scope");CycleId(account,location.Id);account.Parameters.AddWithValue("$bytes",Encoding.UTF8.GetByteCount(raw)+ready.EntriesCursor.Length+ready.ChangesCursor.Length);await account.ExecuteNonQueryAsync(ct);}await CommitCycle(c,t,before,ct);
    }
    private static async Task<long> UpsertCycleRemote(SqliteConnection c,SqliteTransaction t,string id,SyncDescriptor d,long seq,bool deleted,bool snapshot,CancellationToken ct,bool pending=false)
    {
        var location=await CycleRemoteLocation(c,t,id,pending,ct);
        var json=JsonSerializer.Serialize(d,LocalCycleWire.Json);var bytes=checked(256+Encoding.UTF8.GetByteCount(json));long previousBytes=0;string? previous=null;long previousSeq=-1;bool previousDeleted=false;
        await using(var q=SyncCommand(c,t,$"SELECT description,seq,deleted,bytes FROM {location.Remote} WHERE scope_id=$scope AND node_id=$node")){CycleId(q,location.Id);q.Parameters.AddWithValue("$node",d.NodeId);await using var r=await q.ExecuteReaderAsync(ct);if(await r.ReadAsync(ct)){previous=r.GetString(0);previousSeq=r.GetInt64(1);previousDeleted=r.GetInt32(2)!=0;previousBytes=r.GetInt64(3);}}
        if(previousSeq>seq)return 0;
        if(previousSeq==seq){if(previous!=json||previousDeleted!=deleted)throw new InvalidDataException("An immutable remote prefix changed on replay.");return 0;}
        await using(var q=SyncCommand(c,t,$"INSERT INTO {location.Remote} VALUES($scope,$node,$parent,$name,$key,$kind,$json,$seq,$deleted,$bytes) ON CONFLICT(scope_id,node_id) DO UPDATE SET parent_id=$parent,name=$name,name_key=$key,kind=$kind,description=$json,seq=$seq,deleted=$deleted,bytes=$bytes"))
        {CycleId(q,location.Id);q.Parameters.AddWithValue("$node",d.NodeId);q.Parameters.AddWithValue("$parent",(object?)d.ParentId??DBNull.Value);q.Parameters.AddWithValue("$name",d.Name);q.Parameters.AddWithValue("$key",d.ParentId is null?"":await CycleNameKey(c,t,id,d.Name,ct));q.Parameters.AddWithValue("$kind",d.Type=="folder"?1:0);q.Parameters.AddWithValue("$json",json);q.Parameters.AddWithValue("$seq",seq);q.Parameters.AddWithValue("$deleted",deleted?1:0);q.Parameters.AddWithValue("$bytes",bytes);await q.ExecuteNonQueryAsync(ct);}
        if(!snapshot&&!pending)
        {
            // R changing alone cannot authorize overwriting unknown remote
            // content. Active immutable jobs retain their original condition.
            await using var q=SyncCommand(c,t,"UPDATE local_cycle_files SET phase=2 WHERE scope_id=$scope AND remote_node=$node AND kind=0 AND phase<>1 AND baseline IS NOT NULL AND json_extract(baseline,'$.description.version')<>$version");CycleId(q,id);q.Parameters.AddWithValue("$node",d.NodeId);q.Parameters.AddWithValue("$version",deleted?"deleted":d.Version);await q.ExecuteNonQueryAsync(ct);
        }
        return bytes-previousBytes;
    }
    internal async Task SaveOwnedCycleSnapshotEntries(string root,string owner,string id,SyncSnapshotEntriesPage page,int offset,LocalJournalAdmission local,CancellationToken ct,bool pending=false)
    {
        if(offset<0||offset>page.Items.Count||page.Items.Count>500)throw new ArgumentOutOfRangeException(nameof(offset));var items=page.Items.Skip(offset).Take(120).ToArray();
        await CycleAdmission(root,owner,id,local,items.Length*3+4,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);var location=await CycleRemoteLocation(c,t,id,pending,ct);var p=await CycleProgress(c,t,id,ct,pending);
        if(page.Binding!=scope.Configuration.Binding||page.SnapshotId!=p.SnapshotId||p.ReadyJson is null||page.CutSeq!=JsonSerializer.Deserialize<SyncSnapshotReady>(p.ReadyJson,LocalCycleWire.Json)!.CutSeq||page.AfterNodeId!=p.AfterNodeId||p.Complete)throw new InvalidDataException("Snapshot page cannot change original prefix/binding.");
        var bytes=await UpsertCycleRemoteBatch(c,t,id,items.Select(d=>(d,LocalCycleWire.Decimal(page.CutSeq),false,false)).ToArray(),true,pending,ct);await AddCycleBytes(c,t,id,bytes,ct);
        if(offset+items.Length==page.Items.Count)
        {await using var q=SyncCommand(c,t,$"UPDATE {location.Progress} SET entries_cursor=$cursor,after_node=$after,remote_complete=$complete,snapshot_complete=$complete WHERE scope_id=$scope");CycleId(q,location.Id);q.Parameters.AddWithValue("$cursor",page.NextCursor);q.Parameters.AddWithValue("$after",(object?)page.LastNodeId??DBNull.Value);q.Parameters.AddWithValue("$complete",page.HasMore?0:1);await q.ExecuteNonQueryAsync(ct);}
        await CommitCycle(c,t,before,ct);
    }
    internal async Task SaveOwnedCycleFeed(string root,string owner,string id,SyncFeedPage page,int offset,LocalJournalAdmission local,CancellationToken ct,bool pending=false)
    {
        if(offset<0||offset>page.Items.Count||page.Items.Count>500)throw new ArgumentOutOfRangeException(nameof(offset));var items=page.Items.Skip(offset).Take(64).ToArray();
        await CycleAdmission(root,owner,id,local,items.Length*5+3,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);var location=await CycleRemoteLocation(c,t,id,pending,ct);var p=await CycleProgress(c,t,id,ct,pending);
        if(page.Binding!=scope.Configuration.Binding||LocalCycleWire.Decimal(page.FromExclusive)!=p.RequestSequence||p.ReadyJson is null)throw new InvalidDataException("Remote feed cannot change original request prefix/binding.");var sequence=p.Sequence;
        if(pending&&offset==0&&page.Items.Any(item=>LocalCycleWire.Decimal(item.Seq)>sequence))
        {await using var reset=SyncCommand(c,t,"UPDATE local_cycle_refreshes SET reconcile_after='',reconcile_complete=0,reconcile_pass=reconcile_pass+1 WHERE scope_id=$scope AND (reconcile_after<>'' OR reconcile_complete=1) AND reconcile_pass<9223372036854775807");CycleId(reset,location.Id);await reset.ExecuteNonQueryAsync(ct);}
        var events=new List<(SyncDescriptor,long,bool,bool)>();foreach(var item in items)
        {
            var next=LocalCycleWire.Decimal(item.Seq);
            if(next<=sequence)
            {
                events.Add((item.Descriptor,next,item.Kind=="tombstone",true));
                continue;
            }
            if(sequence==long.MaxValue||next!=sequence+1)throw new InvalidDataException("Remote applied sequence has a gap.");
            events.Add((item.Descriptor,next,item.Kind=="tombstone",false));sequence=next;
        }
        var bytes=await UpsertCycleRemoteBatch(c,t,id,events,false,pending,ct);
        var final=offset+items.Length==page.Items.Count;
        await using(var q=SyncCommand(c,t,$"UPDATE {location.Progress} SET remote_seq=$seq,remote_complete=$complete,changes_cursor=CASE WHEN $complete=1 THEN $cursor ELSE changes_cursor END,changes_from=CASE WHEN $complete=1 THEN $seq ELSE changes_from END WHERE scope_id=$scope"))
        {CycleId(q,location.Id);q.Parameters.AddWithValue("$seq",sequence);q.Parameters.AddWithValue("$complete",final?1:0);q.Parameters.AddWithValue("$cursor",page.NextCursor);await q.ExecuteNonQueryAsync(ct);}
        if(final&&sequence!=LocalCycleWire.Decimal(page.ToInclusive))throw new InvalidDataException("Remote terminal boundary differs from applied contiguous prefix.");await AddCycleBytes(c,t,id,bytes,ct);await CommitCycle(c,t,before,ct);
    }
    private static async Task<SyncDescriptor?> CycleRemoteNode(SqliteConnection c,SqliteTransaction t,string id,string node,CancellationToken ct,bool pending=false)
    {
        var location=await CycleRemoteLocation(c,t,id,pending,ct);
        await using var q=SyncCommand(c,t,$"SELECT description FROM {location.Remote} WHERE scope_id=$scope AND node_id=$node AND deleted=0");CycleId(q,location.Id);q.Parameters.AddWithValue("$node",node);
        var raw=await q.ExecuteScalarAsync(ct)as string;return raw is null?null:JsonSerializer.Deserialize<SyncDescriptor>(raw,LocalCycleWire.Json)??throw new InvalidDataException("Stored remote descriptor missing.");
    }
    private static async Task<string> CycleTargetParent(SqliteConnection c,SqliteTransaction t,LocalCycleScope scope,LocalCycleEntry entry,CancellationToken ct)
    {
        var local=await ReadCoherenceAsync(c,t,scope.CoherenceId??throw new InvalidOperationException("Cycle lacks source membership."),ct);var ns=await ReadNamespaceAsync(c,t,local.State.CandidateId,ct);
        if(entry.ParentId==ns.State.RootFileId128)return scope.Configuration.RemoteAnchorId??scope.Configuration.Binding.RootId;
        await using var q=SyncCommand(c,t,"SELECT remote_node FROM local_cycle_files WHERE scope_id=$scope AND node_id=$parent AND kind=1 AND phase<>2");CycleId(q,scope.ScopeId);q.Parameters.AddWithValue("$parent",entry.ParentId);
        return await q.ExecuteScalarAsync(ct)as string??throw new BootstrapCaptureDeferredException("Original parent directory is not yet published/prepared.");
    }
    private static string CycleRemoteTargetSql(string table)
    {
        // Without cardinality statistics SQLite may scan the whole scope via
        // the primary key for every absent/present filename. Both retained
        // generations validate this exact name index during schema admission.
        var index = table switch
        {
            "local_cycle_remote" => "local_cycle_remote_name",
            "local_cycle_refresh_remote" => "local_cycle_refresh_name",
            _ => throw new InvalidDataException("Unknown remote catalog generation.")
        };
        return $"SELECT description FROM {table} INDEXED BY {index} WHERE scope_id=$scope AND parent_id=$parent AND name_key=$key AND deleted=0 LIMIT 2";
    }
    internal async Task<(string Parent,SyncDescriptor? Target,IReadOnlyList<CatalogParentTopology> Topology)> ReadOwnedCycleTarget(string root,string owner,string id,LocalCycleEntry entry,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);var scope=await RequireCycle(c,t,root,owner,id,false,ct);if(!(await CycleProgress(c,t,id,ct)).Complete)throw new InvalidOperationException("Remote snapshot/feed import is incomplete.");
        await RequireCycleRefreshTarget(c,t,id,entry.NodeId,entry.ParentId,ct);var location=await CycleRemoteLocation(c,t,id,false,ct);
        var parent=await CycleTargetParent(c,t,scope,entry,ct);var targetRows=new List<SyncDescriptor>();
        await using(var q=SyncCommand(c,t,CycleRemoteTargetSql(location.Remote))){CycleId(q,location.Id);q.Parameters.AddWithValue("$parent",parent);q.Parameters.AddWithValue("$key",await CycleNameKey(c,t,id,entry.RawName,ct));await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))targetRows.Add(JsonSerializer.Deserialize<SyncDescriptor>(r.GetString(0),LocalCycleWire.Json)!);}
        if(targetRows.Count>1||targetRows.Count==1&&targetRows[0].Name!=entry.RawName)throw new InvalidDataException("Legacy canonical remote name collision cannot alias the original path.");
        var topology=new List<CatalogParentTopology>();var seen=new HashSet<string>(StringComparer.Ordinal);var next=parent;
        while(true)
        {
            if(topology.Count>=129||!seen.Add(next))throw new InvalidDataException("Remote topology is cyclic/unbounded.");
            var d=await CycleRemoteNode(c,t,id,next,ct);
            if(d is null)
            {await using var q=SyncCommand(c,t,PublishedDirectoryTargetSql);CycleId(q,id);q.Parameters.AddWithValue("$node",next);var raw=await q.ExecuteScalarAsync(ct)as string;if(raw is null)d=null;else{using var folder=SyncProtocolReader.Parse(Encoding.UTF8.GetBytes(raw));d=SyncProtocolReader.Descriptor(folder.RootElement.GetProperty("result").GetProperty("description"));}}
            if(d is null||d.Type!="folder"||d.Availability!="AVAILABLE")throw new InvalidDataException("Original target ancestry is missing/not an available folder.");topology.Add(new(d.NodeId,d.ParentId,d.Name));
            if(d.ParentId is null){if(d.NodeId!=scope.Configuration.Binding.RootId)throw new InvalidDataException("Target topology escaped enrolled root.");break;}next=d.ParentId;
        }
        return(parent,targetRows.SingleOrDefault(),topology.AsReadOnly());
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task ValidateCycleAnchorAsync(string id,CancellationToken ct)=>SerialAsync(async()=>{await _store.ValidateOwnedCycleAnchor(_root,_owner,id,_admission,ct);return true;},ct);
    internal Task<LocalCycleRemoteProgress> ReadCycleRemoteProgressAsync(string id,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedCycleProgress(_root,_owner,id,ct),ct);
    internal Task<string> ReserveCycleSnapshotAsync(string id,CancellationToken ct)=>SerialAsync(()=>_store.ReserveOwnedCycleSnapshot(_root,_owner,id,_admission,ct),ct);
    internal Task SaveCycleReadyAsync(string id,SyncSnapshotReady ready,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedCycleReady(_root,_owner,id,ready,_admission,ct);return true;},ct);
    internal Task SaveCycleSnapshotEntriesAsync(string id,SyncSnapshotEntriesPage page,int offset,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedCycleSnapshotEntries(_root,_owner,id,page,offset,_admission,ct);return true;},ct);
    internal Task SaveCycleFeedAsync(string id,SyncFeedPage page,int offset,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedCycleFeed(_root,_owner,id,page,offset,_admission,ct);return true;},ct);
    internal Task<(string Parent,SyncDescriptor? Target,IReadOnlyList<CatalogParentTopology> Topology)> ReadCycleTargetAsync(string id,LocalCycleEntry entry,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedCycleTarget(_root,_owner,id,entry,ct),ct);
}
