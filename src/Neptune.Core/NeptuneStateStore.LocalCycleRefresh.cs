using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

internal sealed record CycleRemoteLocation(string Progress,string Remote,string Id);
internal sealed record CycleRefreshState(string Id,string SnapshotId,bool Reconciled,int Status);
public sealed partial class NeptuneStateStore
{
    // Generation zero remains in the original tables. Migration adds only
    // empty tables and two pointers, never rewrites all current remote rows.
    private static Dictionary<string,string> CycleRefreshSchema=>new(StringComparer.Ordinal)
    {
        ["local_cycle_refreshes"]="CREATE TABLE local_cycle_refreshes(scope_id TEXT PRIMARY KEY,cycle_id TEXT NOT NULL REFERENCES local_cycle_scopes(scope_id),fingerprint TEXT NOT NULL,prior_remote TEXT,snapshot_id TEXT NOT NULL UNIQUE,ready_json TEXT,entries_cursor TEXT,after_node TEXT,changes_cursor TEXT,changes_from INTEGER NOT NULL DEFAULT 0,remote_seq INTEGER NOT NULL DEFAULT 0,remote_complete INTEGER NOT NULL DEFAULT 0 CHECK(remote_complete IN(0,1)),snapshot_complete INTEGER NOT NULL DEFAULT 0 CHECK(snapshot_complete IN(0,1)),error TEXT,reconcile_after TEXT NOT NULL DEFAULT '',reconcile_complete INTEGER NOT NULL DEFAULT 0 CHECK(reconcile_complete IN(0,1)),reconcile_pass INTEGER NOT NULL DEFAULT 0 CHECK(reconcile_pass>=0),status INTEGER NOT NULL DEFAULT 0 CHECK(status BETWEEN 0 AND 2),bytes INTEGER NOT NULL DEFAULT 1024)",
        ["local_cycle_refresh_remote"]="CREATE TABLE local_cycle_refresh_remote(scope_id TEXT NOT NULL REFERENCES local_cycle_refreshes(scope_id),node_id TEXT NOT NULL,parent_id TEXT,name TEXT NOT NULL,name_key TEXT NOT NULL,kind INTEGER NOT NULL CHECK(kind IN(0,1)),description TEXT NOT NULL,seq INTEGER NOT NULL,deleted INTEGER NOT NULL CHECK(deleted IN(0,1)),bytes INTEGER NOT NULL,PRIMARY KEY(scope_id,node_id)) WITHOUT ROWID",
        ["local_cycle_refresh_name"]="CREATE INDEX local_cycle_refresh_name ON local_cycle_refresh_remote(scope_id,parent_id,name_key,deleted,node_id)",
        ["local_cycle_refresh_parent"]="CREATE INDEX local_cycle_refresh_parent ON local_cycle_refreshes(cycle_id,status,scope_id)",
        ["local_cycle_refresh_conflicts"]="CREATE TABLE local_cycle_refresh_conflicts(scope_id TEXT NOT NULL REFERENCES local_cycle_refreshes(scope_id),node_id TEXT NOT NULL,pass INTEGER NOT NULL CHECK(pass>=0),reason TEXT NOT NULL,bytes INTEGER NOT NULL DEFAULT 256,PRIMARY KEY(scope_id,node_id)) WITHOUT ROWID",
        ["local_cycle_refresh_conflicts_pass"]="CREATE INDEX local_cycle_refresh_conflicts_pass ON local_cycle_refresh_conflicts(scope_id,pass,node_id)"
    };
    private static async Task<CycleRemoteLocation> CycleRemoteLocation(SqliteConnection c,SqliteTransaction t,string id,bool pending,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,"SELECT "+(pending?"pending_remote":"active_remote")+" FROM local_cycle_scopes WHERE scope_id=$scope");CycleId(q,id);var next=await q.ExecuteScalarAsync(ct)as string;
        if(next is null){if(pending)throw new InvalidOperationException("No original pending remote stage.");return new("local_cycle_scopes","local_cycle_remote",id);}
        await using var check=SyncCommand(c,t,"SELECT 1 FROM local_cycle_refreshes r JOIN local_cycle_scopes s ON s.scope_id=r.cycle_id WHERE r.scope_id=$next AND r.cycle_id=$scope AND r.fingerprint=s.fingerprint AND r.status=$status");CycleId(check,id);check.Parameters.AddWithValue("$next",next);check.Parameters.AddWithValue("$status",pending?0:1);if(await check.ExecuteScalarAsync(ct)is null)throw new InvalidDataException("Remote generation pointer/binding/status changed.");
        return new("local_cycle_refreshes","local_cycle_refresh_remote",next);
    }
    internal async Task<CycleRefreshState?> ReadOwnedCycleRefresh(string root,string owner,string id,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,id,false,ct);
        await using var q=SyncCommand(c,t,"SELECT r.scope_id,r.snapshot_id,r.reconcile_complete,r.status FROM local_cycle_scopes s JOIN local_cycle_refreshes r ON r.scope_id=s.pending_remote AND r.cycle_id=s.scope_id AND r.fingerprint=s.fingerprint WHERE s.scope_id=$scope");CycleId(q,id);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))return null;return new(r.GetString(0),r.GetString(1),r.GetInt32(2)!=0,r.GetInt32(3));
    }
    internal async Task<CycleRefreshState> BeginOwnedCycleRefresh(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct,bool replace=false)
    {
        await CycleAdmission(root,owner,id,local,5,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);
        await RequireRefreshLocalCut(c,t,root,owner,scope,ct);
        string? pending,active,fingerprint;await using(var q=SyncCommand(c,t,"SELECT pending_remote,active_remote,fingerprint FROM local_cycle_scopes WHERE scope_id=$scope")){CycleId(q,id);await using var r=await q.ExecuteReaderAsync(ct);await r.ReadAsync(ct);pending=r.IsDBNull(0)?null:r.GetString(0);active=r.IsDBNull(1)?null:r.GetString(1);fingerprint=r.GetString(2);}
        if(pending is not null&&!replace){var p=await CycleProgress(c,t,id,ct,true);await t.CommitAsync(ct);return new(pending,p.SnapshotId!,false,0);}
        if(pending is not null){await using var old=SyncCommand(c,t,"UPDATE local_cycle_refreshes SET status=2,error='Typed remote cursor gap; original stage retained.' WHERE scope_id=$pending AND status=0");old.Parameters.AddWithValue("$pending",pending);if(await old.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("Original pending stage changed.");}
        var next=Guid.NewGuid().ToString("N");var snapshot=Guid.NewGuid().ToString("D");await using(var q=SyncCommand(c,t,"INSERT INTO local_cycle_refreshes(scope_id,cycle_id,fingerprint,prior_remote,snapshot_id) VALUES($next,$scope,$fingerprint,$active,$snapshot);UPDATE local_cycle_scopes SET pending_remote=$next WHERE scope_id=$scope")){CycleId(q,id);q.Parameters.AddWithValue("$next",next);q.Parameters.AddWithValue("$fingerprint",fingerprint);q.Parameters.AddWithValue("$active",(object?)active??DBNull.Value);q.Parameters.AddWithValue("$snapshot",snapshot);if(await q.ExecuteNonQueryAsync(ct)!=2)throw new InvalidOperationException("Remote stage reservation lost original scope.");}
        await AddCycleBytes(c,t,id,1024,ct);await CommitCycle(c,t,before,ct);return new(next,snapshot,false,0);
    }
    private static async Task RequireRefreshLocalCut(SqliteConnection c,SqliteTransaction t,string root,string owner,LocalCycleScope scope,CancellationToken ct)
    {
        if(!scope.SeedComplete||scope.CoherenceId is null)throw new LocalSourceGapException("RefreshSourceUnqualified","Remote replacement requires a retained complete native membership.");
        var coherence=await RequireCoherenceAsync(c,t,root,owner,scope.CoherenceId,false,ct);var state=await ReadLocalStateAsync(c,t,root,owner,ct);
        if(coherence.State.Status!=LocalCoherenceStatus.Anchored||coherence.State.AppliedUsn<state.Cursor.NextReadUsn)throw new LocalSourceGapException("RefreshSourceGap","Native evidence must be observed/applied before remote replacement.");
    }
    internal async Task<int> ReconcileOwnedCycleRefresh(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,id,local,124,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);await RequireRefreshLocalCut(c,t,root,owner,scope,ct);
        var location=await CycleRemoteLocation(c,t,id,true,ct);var progress=await CycleProgress(c,t,id,ct,true);if(!progress.Complete||!progress.SnapshotComplete)throw new InvalidOperationException("Cannot reconcile an incomplete replacement remote prefix.");
        string after;long pass;await using(var q=SyncCommand(c,t,"SELECT reconcile_after,reconcile_pass FROM local_cycle_refreshes WHERE scope_id=$scope")){CycleId(q,location.Id);await using var r=await q.ExecuteReaderAsync(ct);await r.ReadAsync(ct);after=r.GetString(0);pass=r.GetInt64(1);}
        // Page the scope primary key first; do not scan N baseline JSON rows
        // under a lock or hide an unbounded filter inside LIMIT.
        var rows=new List<(string Node,string? Baseline,bool Active)>(120);await using(var q=SyncCommand(c,t,"SELECT f.node_id,f.baseline,EXISTS(SELECT 1 FROM local_cycle_work w WHERE w.scope_id=f.scope_id AND w.node_id=f.node_id AND w.active=1 AND (w.input_json IS NOT NULL OR w.phase IN(5,6))) FROM local_cycle_files f WHERE f.scope_id=$scope AND f.node_id>$after ORDER BY f.node_id LIMIT 120")){CycleId(q,id);q.Parameters.AddWithValue("$after",after);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))rows.Add((r.GetString(0),r.IsDBNull(1)?null:r.GetString(1),r.GetInt64(2)!=0));}
        long bytes=0;foreach(var row in rows)
        {
            if(row.Baseline is null||row.Active)continue;var baseline=JsonSerializer.Deserialize<LocalCycleBaseline>(row.Baseline,LocalCycleWire.Json)??throw new InvalidDataException("Historical B is malformed.");var b=baseline.Description;var remote=await CycleRemoteNode(c,t,id,b.NodeId,ct,true);
            var match=b.Type=="folder"?remote is not null&&remote.Type=="folder"&&remote.Availability=="AVAILABLE"&&remote.NodeId==b.NodeId&&remote.ParentId==b.ParentId&&remote.Name==b.Name:remote==b;
            if(match)continue;
            await using var existing=SyncCommand(c,t,"SELECT 1 FROM local_cycle_refresh_conflicts WHERE scope_id=$scope AND node_id=$node");CycleId(existing,location.Id);existing.Parameters.AddWithValue("$node",row.Node);var absent=await existing.ExecuteScalarAsync(ct)is null;
            await using var q=SyncCommand(c,t,"INSERT INTO local_cycle_refresh_conflicts(scope_id,node_id,pass,reason) VALUES($scope,$node,$pass,'Current replacement R differs from historical B.') ON CONFLICT(scope_id,node_id) DO UPDATE SET pass=$pass,reason=excluded.reason");CycleId(q,location.Id);q.Parameters.AddWithValue("$node",row.Node);q.Parameters.AddWithValue("$pass",pass);await q.ExecuteNonQueryAsync(ct);if(absent)bytes+=256;
        }
        await using(var q=SyncCommand(c,t,"UPDATE local_cycle_refreshes SET reconcile_after=$after,reconcile_complete=$complete WHERE scope_id=$scope")){CycleId(q,location.Id);q.Parameters.AddWithValue("$after",rows.Count==0?after:rows[^1].Node);q.Parameters.AddWithValue("$complete",rows.Count<120?1:0);await q.ExecuteNonQueryAsync(ct);}
        // Physical admission includes every retained stage/WAL/backup file.
        await AddCycleBytes(c,t,id,bytes,ct);await CommitCycle(c,t,before,ct);return rows.Count;
    }
    internal async Task InstallOwnedCycleRefresh(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,id,local,3,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);await RequireRefreshLocalCut(c,t,root,owner,scope,ct);var location=await CycleRemoteLocation(c,t,id,true,ct);
        // Anchor proof is evaluated again inside the installation transaction,
        // not merely before a page of remote data or against generation zero.
        await VerifyCycleAnchorChain(c,t,scope,location.Id,ct);
        await using(var q=SyncCommand(c,t,"UPDATE local_cycle_scopes SET active_remote=pending_remote,pending_remote=NULL WHERE scope_id=$scope AND EXISTS(SELECT 1 FROM local_cycle_refreshes r WHERE r.scope_id=pending_remote AND r.cycle_id=$scope AND r.fingerprint=local_cycle_scopes.fingerprint AND r.prior_remote IS local_cycle_scopes.active_remote AND r.remote_complete=1 AND r.snapshot_complete=1 AND r.reconcile_complete=1 AND r.status=0)")){CycleId(q,id);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("Replacement generation is incomplete/stale; current R remains unchanged.");}
        await using(var q=SyncCommand(c,t,"UPDATE local_cycle_refreshes SET status=1 WHERE scope_id=$scope AND status=0")){CycleId(q,location.Id);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("Replacement stage was not original pending generation.");}
        await CommitCycle(c,t,before,ct);
    }
    private static async Task VerifyCycleAnchorChain(SqliteConnection c,SqliteTransaction t,LocalCycleScope scope,string refresh,CancellationToken ct)
    {
        var next=scope.Configuration.RemoteAnchorId??scope.Configuration.Binding.RootId;var chain=new List<CatalogParentTopology>();var seen=new HashSet<string>();
        while(true){if(chain.Count>=129||!seen.Add(next))throw new InvalidDataException("Replacement anchor ancestry is cyclic/unbounded.");var d=await CycleRemoteNode(c,t,scope.ScopeId,next,ct,true);if(d is null||d.Type!="folder"||d.Availability!="AVAILABLE")throw new InvalidDataException("Replacement anchor is missing/trashed/not a folder.");chain.Add(new(d.NodeId,d.ParentId,d.Name));if(d.ParentId is null){if(d.NodeId!=scope.Configuration.Binding.RootId)throw new InvalidDataException("Replacement anchor escaped enrolled root.");break;}next=d.ParentId;}
        await using var q=SyncCommand(c,t,"SELECT anchor_json FROM local_cycle_scopes WHERE scope_id=$scope");CycleId(q,scope.ScopeId);var original=await q.ExecuteScalarAsync(ct)as string;if(original is null||original!=JsonSerializer.Serialize(chain,LocalCycleWire.Json))throw new InvalidDataException("Replacement snapshot changed original mapping anchor/ancestry.");
    }
    private static async Task RequireCycleRefreshTarget(SqliteConnection c,SqliteTransaction t,string id,string node,string parent,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,"SELECT 1 FROM local_cycle_scopes s WHERE s.scope_id=$scope AND (s.pending_remote IS NOT NULL OR EXISTS(SELECT 1 FROM local_cycle_refresh_conflicts x JOIN local_cycle_refreshes r ON r.scope_id=x.scope_id WHERE x.scope_id=s.active_remote AND x.pass=r.reconcile_pass AND x.node_id IN($node,$parent)))");CycleId(q,id);q.Parameters.AddWithValue("$node",node);q.Parameters.AddWithValue("$parent",parent);if(await q.ExecuteScalarAsync(ct)is not null)throw new BootstrapCaptureDeferredException("Current replacement R has a pending stage or target/parent conflict; original B and dirty evidence retained.");
    }
    internal async Task<LocalCycleHistoryRetirement> RetireOwnedCycleRefresh(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,id,local,123,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireCycle(c,t,root,owner,id,false,ct);
        string? stage;await using(var q=SyncCommand(c,t,"SELECT r.scope_id FROM local_cycle_refreshes r JOIN local_cycle_scopes s ON s.scope_id=r.cycle_id WHERE r.cycle_id=$scope AND r.scope_id IS NOT s.active_remote AND r.scope_id IS NOT s.pending_remote ORDER BY r.scope_id LIMIT 1")){CycleId(q,id);stage=await q.ExecuteScalarAsync(ct)as string;}
        if(stage is null){await t.CommitAsync(ct);return new(0,0,false,0,"Installed/pending remote generations are pinned.");}
        var table="local_cycle_refresh_remote";var rows=new List<(string Node,long Bytes)>(120);
        async Task ReadRows(){await using var q=SyncCommand(c,t,$"SELECT node_id,bytes FROM {table} WHERE scope_id=$scope ORDER BY node_id LIMIT 120");CycleId(q,stage);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))rows.Add((r.GetString(0),r.GetInt64(1)));}
        await ReadRows();if(rows.Count==0){table="local_cycle_refresh_conflicts";await ReadRows();}
        long bytes=0;foreach(var row in rows){await using var q=SyncCommand(c,t,$"DELETE FROM {table} WHERE scope_id=$scope AND node_id=$node");CycleId(q,stage);q.Parameters.AddWithValue("$node",row.Node);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidDataException("Remote retirement page changed.");bytes+=row.Bytes;}
        var removed=rows.Count==0;if(removed){await using var q=SyncCommand(c,t,"SELECT bytes FROM local_cycle_refreshes WHERE scope_id=$scope");CycleId(q,stage);bytes=Convert.ToInt64(await q.ExecuteScalarAsync(ct));q.CommandText="DELETE FROM local_cycle_refreshes WHERE scope_id=$scope";await q.ExecuteNonQueryAsync(ct);}
        await AddCycleBytes(c,t,id,-bytes,ct);return new(rows.Count,bytes,removed,await CommitCycle(c,t,before,ct));
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<CycleRefreshState?> ReadCycleRefreshAsync(string id,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedCycleRefresh(_root,_owner,id,ct),ct);
    internal Task<CycleRefreshState> BeginCycleRefreshAsync(string id,bool replace,CancellationToken ct)=>SerialAsync(()=>_store.BeginOwnedCycleRefresh(_root,_owner,id,_admission,ct,replace),ct);
    internal Task<LocalCycleRemoteProgress> ReadCycleStageProgressAsync(string id,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedCycleProgress(_root,_owner,id,ct,true),ct);
    internal Task SaveCycleStageReadyAsync(string id,SyncSnapshotReady ready,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedCycleReady(_root,_owner,id,ready,_admission,ct,true);return true;},ct);
    internal Task SaveCycleStageEntriesAsync(string id,SyncSnapshotEntriesPage page,int offset,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedCycleSnapshotEntries(_root,_owner,id,page,offset,_admission,ct,true);return true;},ct);
    internal Task SaveCycleStageFeedAsync(string id,SyncFeedPage page,int offset,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedCycleFeed(_root,_owner,id,page,offset,_admission,ct,true);return true;},ct);
    internal Task<int> ReconcileCycleStageAsync(string id,CancellationToken ct)=>SerialAsync(()=>_store.ReconcileOwnedCycleRefresh(_root,_owner,id,_admission,ct),ct);
    internal Task InstallCycleStageAsync(string id,CancellationToken ct)=>SerialAsync(async()=>{await _store.InstallOwnedCycleRefresh(_root,_owner,id,_admission,ct);return true;},ct);
    internal Task<LocalCycleHistoryRetirement> RetireCycleRemoteHistoryAsync(string id,CancellationToken ct)=>SerialAsync(()=>_store.RetireOwnedCycleRefresh(_root,_owner,id,_admission,ct),ct);
}
