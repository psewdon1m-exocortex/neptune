using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

internal sealed record LocalCycleScope(string ScopeId,string Root,string? CoherenceId,
    WindowsCycleConfiguration Configuration,long OwnerEpoch,long Revision,long Gap,int Status,
    WindowsCycleBudget Budget,long SeedAfter,bool SeedComplete,long Files,string? Error);
public sealed record LocalCycleMetrics(long Transactions,long MaximumChangedRows,long MaximumCommitTicks);

public sealed partial class NeptuneStateStore
{
    private long _cycleTransactions,_cycleMaxRows,_cycleMaxCommit;
    public LocalCycleMetrics GetLocalCycleMetrics()=>new(Interlocked.Read(ref _cycleTransactions),Interlocked.Read(ref _cycleMaxRows),Interlocked.Read(ref _cycleMaxCommit));
    private static string CycleHash(string value)=>Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
    private static void CycleId(SqliteCommand q,string id)=>q.Parameters.AddWithValue("$scope",id);
    private async Task<SqliteConnection> OpenCycleConnection(CancellationToken ct)
    {
        var connection=await OpenLocalSourceConnectionAsync(ct);
        connection.CreateFunction<string,string,bool>("neptune_path_within",MappingSyncPolicy.IsWithin,isDeterministic:true);
        try{await using var q=connection.CreateCommand();q.CommandText="SELECT version FROM local_cycle_format WHERE singleton=1";if(await q.ExecuteScalarAsync(ct)is not long version||version is not(4 or 5 or 6 or 7))throw new InvalidDataException("Cycle module requires recognized format 4, 5, 6 or 7.");return connection;}
        catch{await connection.DisposeAsync();throw;}
    }
    private static async Task<LocalCycleScope> CycleScope(SqliteConnection c,SqliteTransaction t,string id,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,"SELECT root_key,coherence_id,configuration,fingerprint,owner_epoch,legacy_revision,gap_epoch,status,max_profile,free_floor,seed_after,seed_complete,files,error FROM local_cycle_scopes WHERE scope_id=$scope");CycleId(q,id);await using var r=await q.ExecuteReaderAsync(ct);
        if(!await r.ReadAsync(ct))throw new InvalidOperationException("Cycle scope is absent.");var raw=r.GetString(2);if(Encoding.UTF8.GetByteCount(raw)>16*1024||CycleHash(raw)!=r.GetString(3))throw new InvalidDataException("Cycle binding fingerprint changed.");
        var configuration=JsonSerializer.Deserialize<WindowsCycleConfiguration>(raw,LocalCycleWire.Json)??throw new InvalidDataException("Cycle configuration missing.");LocalCycleWire.Configuration(configuration);
        return new(id,r.GetString(0),r.IsDBNull(1)?null:r.GetString(1),configuration,r.GetInt64(4),r.GetInt64(5),r.GetInt64(6),r.GetInt32(7),new(ProfileBytes:r.GetInt64(8),MinimumFreeDiskBytes:r.GetInt64(9)),r.GetInt64(10),r.GetInt32(11)!=0,r.GetInt64(12),r.IsDBNull(13)?null:r.GetString(13));
    }
    private static async Task<LocalCycleScope> RequireCycle(SqliteConnection c,SqliteTransaction t,string root,string owner,string id,bool evidenceOnly,CancellationToken ct)
    {
        var local=await ReadLocalStateAsync(c,t,root,owner,ct);var row=await CycleScope(c,t,id,ct);var b=local.Binding;
        if(row.Root!=root||row.OwnerEpoch!=local.Cursor.OwnerEpoch||row.Configuration.MappingId!=b.MappingId||row.Configuration.ClientId!=b.ClientId||row.Configuration.ProfileId!=b.ProfileId||row.Configuration.RulesVersion!=b.RulesVersion)
            throw new InvalidOperationException("Cycle owner/root/rules changed; explicit recovery required.");
        if(!evidenceOnly)
        {
            await RequireLocalMappingAsync(c,t,b,ct);
            if(row.Status is 2 or 3||!local.Cursor.Valid||row.Revision!=local.LegacyRevision||row.Revision!=await LocalRevisionAsync(c,t,ct)||row.Gap!=local.GapEpoch)
                throw new LocalSourceGapException("CycleContinuityGap","Cycle requires intact local lifecycle/revision and retained dirty evidence.");
        }
        return row;
    }
    private static LocalJournalAdmission CycleBudget(LocalJournalAdmission local,LocalCycleScope row)
        =>new(local.MaximumInboxBytes,Math.Max(local.MinimumFreeDiskBytes,row.Budget.MinimumFreeDiskBytes),Math.Min(local.MaximumDatabaseBytes,row.Budget.ProfileBytes));
    private async Task CycleAdmission(string root,string owner,string id,LocalJournalAdmission local,int rows,CancellationToken ct)
    {await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);var row=await RequireCycle(c,t,root,owner,id,false,ct);await t.CommitAsync(ct);await CheckLocalAdmissionAsync(CycleBudget(local,row),0,NamespaceReserve(rows),ct);}
    private async Task<long> CommitCycle(SqliteConnection c,SqliteTransaction t,long before,CancellationToken ct)
    {var changes=await NamespaceChanges(c,t,ct)-before;var timer=Stopwatch.StartNew();await t.CommitAsync(ct);Interlocked.Increment(ref _cycleTransactions);UpdateBootstrapMaximum(ref _cycleMaxRows,changes);UpdateBootstrapMaximum(ref _cycleMaxCommit,timer.Elapsed.Ticks);return changes;}
    private static async Task AddCycleBytes(SqliteConnection c,SqliteTransaction t,string id,long bytes,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,"UPDATE local_cycle_scopes SET bytes=bytes+$bytes WHERE scope_id=$scope;UPDATE local_cycle_format SET bytes=bytes+$bytes WHERE singleton=1");CycleId(q,id);q.Parameters.AddWithValue("$bytes",bytes);await q.ExecuteNonQueryAsync(ct);
    }
    internal async Task<string> BeginOwnedCycle(string root,string owner,WindowsCycleConfiguration configuration,WindowsCycleBudget budget,LocalJournalAdmission local,CancellationToken ct)
    {
        LocalCycleWire.Configuration(configuration);
        ValidateCycleBudget(budget);
        var admission=new LocalJournalAdmission(local.MaximumInboxBytes,Math.Max(local.MinimumFreeDiskBytes,budget.MinimumFreeDiskBytes),Math.Min(local.MaximumDatabaseBytes,budget.ProfileBytes));
        await EnsureNamespaceAsync(admission,ct);await EnsureCoherenceAsync(admission,ct);await EnsureCycleAsync(admission,ct);await CheckLocalAdmissionAsync(admission,0,NamespaceReserve(16),ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var state=await ReadLocalStateAsync(c,t,root,owner,ct);await RequireLocalMappingAsync(c,t,state.Binding,ct);
        await RequireNoPendingBindingMigration(c,t,ct);
        if(configuration.MappingId!=state.Binding.MappingId||configuration.ClientId!=state.Binding.ClientId||configuration.ProfileId!=state.Binding.ProfileId||configuration.RulesVersion!=state.Binding.RulesVersion||!state.Cursor.Valid)throw new InvalidDataException("Enrolled cycle cannot rebind local subscription.");
        await using(var count=SyncCommand(c,t,"SELECT COUNT(*) FROM(SELECT 1 FROM local_cycle_scopes LIMIT 17)"))if(Convert.ToInt64(await count.ExecuteScalarAsync(ct))>=16)throw new LocalSourceGapException("CycleCapacity","Retire retained cycle scopes explicitly before creating more.");
        var id=Guid.NewGuid().ToString("N");var json=JsonSerializer.Serialize(configuration,LocalCycleWire.Json);
        await using(var q=SyncCommand(c,t,"INSERT INTO local_cycle_scopes(scope_id,root_key,configuration,fingerprint,owner_epoch,legacy_revision,gap_epoch,max_profile,free_floor) VALUES($scope,$root,$json,$hash,$owner,$revision,$gap,$profile,$floor)"))
        {CycleId(q,id);q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$json",json);q.Parameters.AddWithValue("$hash",CycleHash(json));q.Parameters.AddWithValue("$owner",state.Cursor.OwnerEpoch);q.Parameters.AddWithValue("$revision",state.LegacyRevision);q.Parameters.AddWithValue("$gap",state.GapEpoch);q.Parameters.AddWithValue("$profile",budget.ProfileBytes);q.Parameters.AddWithValue("$floor",budget.MinimumFreeDiskBytes);await q.ExecuteNonQueryAsync(ct);}
        await using(var q=SyncCommand(c,t,"UPDATE local_cycle_scopes SET status=3,error='Explicit fresh scope superseded retained cycle.' WHERE scope_id IN(SELECT scope_id FROM local_cycle_active WHERE root_key=$root);INSERT INTO local_cycle_active VALUES($root,$scope) ON CONFLICT(root_key) DO UPDATE SET scope_id=$scope;UPDATE local_cycle_format SET bytes=bytes+1024 WHERE singleton=1"))
        {CycleId(q,id);q.Parameters.AddWithValue("$root",root);await q.ExecuteNonQueryAsync(ct);}await CommitCycle(c,t,before,ct);return id;
    }
    private static void ValidateCycleBudget(WindowsCycleBudget budget)
    {
        if(budget.DataJobs is <1 or >8||budget.LargeJobs is <1 or >2||budget.LargeJobs>budget.DataJobs||budget.LargeFileThreshold<1||budget.SpoolBytes is <1024 or >1024L*1024*1024||budget.ProfileBytes is <1024*1024 or >16L*1024*1024*1024||budget.MinimumFreeDiskBytes<0||budget.MaximumNewFileBytes is <0 or >9007199254740991L)
            throw new ArgumentOutOfRangeException(nameof(budget));
    }
    internal async Task BindOwnedCycleCoherence(string root,string owner,string id,string? coherence,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,id,local,2,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);
        if(coherence is null)
        {await using var find=SyncCommand(c,t,"SELECT a.coherence_id FROM local_coherence_active a WHERE a.root_key=$root");find.Parameters.AddWithValue("$root",root);coherence=await find.ExecuteScalarAsync(ct)as string??throw new InvalidOperationException("Native coherence is not initialized.");}
        var proof=await RequireCoherenceAsync(c,t,root,owner,coherence,false,ct);
        if(scope.CoherenceId is not null&&scope.CoherenceId!=coherence||proof.State.Status is not (LocalCoherenceStatus.Building or LocalCoherenceStatus.Anchored))throw new InvalidOperationException("Cycle cannot silently change original coherence.");
        await using var q=SyncCommand(c,t,"UPDATE local_cycle_scopes SET coherence_id=$coherence WHERE scope_id=$scope AND (coherence_id IS NULL OR coherence_id=$coherence)");CycleId(q,id);q.Parameters.AddWithValue("$coherence",coherence);await q.ExecuteNonQueryAsync(ct);await CommitCycle(c,t,before,ct);
    }
    internal async Task<LocalCycleScope> ReadOwnedCycleScope(string root,string owner,string id,CancellationToken ct)
    {await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);return await RequireCycle(c,t,root,owner,id,true,ct);}
    private static async Task<string> CyclePath(SqliteConnection c,SqliteTransaction t,LocalCycleScope scope,string parent,string name,CancellationToken ct)
    {
        if(scope.CoherenceId is null)throw new InvalidOperationException("Cycle lacks its native candidate.");var coherence=await ReadCoherenceAsync(c,t,scope.CoherenceId,ct);
        var ns=await ReadNamespaceAsync(c,t,coherence.State.CandidateId,ct);var parts=new List<string>{name};var seen=new HashSet<string>(StringComparer.Ordinal);
        while(parent!=ns.State.RootFileId128)
        {
            if(parts.Count>=128||!seen.Add(parent))throw new InvalidDataException("Cycle ancestry exceeds bounded topology or contains a cycle.");
            await using var q=SyncCommand(c,t,"SELECT parent_id,name,kind FROM local_namespace_entries WHERE candidate_id=$candidate AND node_id=$node");q.Parameters.AddWithValue("$candidate",coherence.State.CandidateId);q.Parameters.AddWithValue("$node",parent);await using var r=await q.ExecuteReaderAsync(ct);
            if(!await r.ReadAsync(ct)||r.GetInt32(2)!=1)throw new InvalidDataException("Cycle source parent is missing/not a directory.");parent=r.GetString(0);parts.Add(r.GetString(1));
        }
        parts.Reverse();var path=string.Join('/',parts);if(path.Length>4096)throw new InvalidDataException("Cycle path exceeds fixed limit.");return path;
    }
    internal async Task<int> SeedOwnedCycleEntries(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,id,local,256,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);
        if(scope.CoherenceId is null)throw new InvalidOperationException("Cycle needs original coherent candidate.");var coherence=await RequireCoherenceAsync(c,t,root,owner,scope.CoherenceId,false,ct);var ns=await ReadNamespaceAsync(c,t,coherence.State.CandidateId,ct);
        if(coherence.State.Status is not (LocalCoherenceStatus.Building or LocalCoherenceStatus.Anchored))throw new LocalSourceGapException("CycleNamespaceUncertain","Ambiguous source branch cannot become eligible.");
        var entries=new List<(long Id,string Node,string Parent,string Name,long Created,int Kind,long Size)>();
        await using(var q=SyncCommand(c,t,"SELECT e.entry_id,e.node_id,e.parent_id,e.name,e.created,e.kind,e.size FROM local_namespace_entries e JOIN local_namespace_parents p ON p.candidate_id=e.candidate_id AND p.node_id=e.parent_id WHERE e.candidate_id=$candidate AND e.entry_id>$after AND e.entry_id<=$through AND p.state=2 AND e.excluded=0 ORDER BY e.entry_id LIMIT 250"))
        {q.Parameters.AddWithValue("$candidate",coherence.State.CandidateId);q.Parameters.AddWithValue("$after",scope.SeedAfter);q.Parameters.AddWithValue("$through",ns.State.EligibleThrough);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))entries.Add((r.GetInt64(0),r.GetString(1),r.GetString(2),r.GetString(3),r.GetInt64(4),r.GetInt32(5),r.GetInt64(6)));}
        long bytes=0;foreach(var e in entries)
        {
            var path=await CyclePath(c,t,scope,e.Parent,e.Name,ct);var size=checked(256+Encoding.UTF8.GetByteCount(path)+Encoding.UTF8.GetByteCount(e.Name));
            await using var q=SyncCommand(c,t,"INSERT INTO local_cycle_files(scope_id,node_id,parent_id,path,name,created,kind,size,bytes) VALUES($scope,$node,$parent,$path,$name,$created,$kind,$size,$bytes)");CycleId(q,id);q.Parameters.AddWithValue("$node",e.Node);q.Parameters.AddWithValue("$parent",e.Parent);q.Parameters.AddWithValue("$path",path);q.Parameters.AddWithValue("$name",e.Name);q.Parameters.AddWithValue("$created",e.Created);q.Parameters.AddWithValue("$kind",e.Kind);q.Parameters.AddWithValue("$size",e.Size);q.Parameters.AddWithValue("$bytes",size);await q.ExecuteNonQueryAsync(ct);bytes+=size;
        }
        var complete=ns.State.Status==LocalNamespaceStatus.EnumerationComplete&&entries.Count<250;
        await using(var q=SyncCommand(c,t,"UPDATE local_cycle_scopes SET seed_after=$after,seed_complete=$complete,files=files+$count WHERE scope_id=$scope")){CycleId(q,id);q.Parameters.AddWithValue("$after",entries.Count>0?entries[^1].Id:scope.SeedAfter);q.Parameters.AddWithValue("$complete",complete?1:0);q.Parameters.AddWithValue("$count",entries.Count);await q.ExecuteNonQueryAsync(ct);}
        await AddCycleBytes(c,t,id,bytes,ct);await CommitCycle(c,t,before,ct);return entries.Count;
    }
    internal async Task MarkOwnedCycleUncertain(string root,string owner,string id,string error,CancellationToken ct)
    {
        if(error.Length>1024)error=error[..1024];await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireCycle(c,t,root,owner,id,true,ct);
        await using var q=SyncCommand(c,t,"UPDATE local_cycle_scopes SET status=2,error=$error WHERE scope_id=$scope AND status<>3");CycleId(q,id);q.Parameters.AddWithValue("$error",error);await q.ExecuteNonQueryAsync(ct);await CommitCycle(c,t,before,ct);
    }
    internal async Task MarkOwnedCycleHints(string root,string owner,string id,IReadOnlyList<string> exactPaths,LocalJournalAdmission local,CancellationToken ct,bool parentReconciliation=false)
    {
        if(exactPaths.Count is <1 or >128||exactPaths.Any(p=>p.Length is <1 or >4096))throw new ArgumentOutOfRangeException(nameof(exactPaths));if(parentReconciliation)await ParentAdmission(root,owner,id,local,exactPaths.Count+2,0,ct);else await CycleAdmission(root,owner,id,local,exactPaths.Count+2,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=parentReconciliation?await RequireParentCycle(c,t,root,owner,id,ct):await RequireCycle(c,t,root,owner,id,false,ct);var unknown=false;
        foreach(var path in exactPaths)
        {
            await using var q=SyncCommand(c,t,"UPDATE local_cycle_files SET watcher_generation=watcher_generation+1,phase=CASE WHEN phase=1 THEN 1 ELSE 0 END WHERE scope_id=$scope AND path=$path AND watcher_generation<9223372036854775807");CycleId(q,id);q.Parameters.AddWithValue("$path",path);if(await q.ExecuteNonQueryAsync(ct)!=1)unknown=true;
        }
        await using(var q=SyncCommand(c,t,"UPDATE local_cycle_scopes SET watcher_cursor=watcher_cursor+$count,status=CASE WHEN $unknown=1 THEN 2 ELSE status END,error=CASE WHEN $unknown=1 THEN 'Watcher unknown/short alias or structural branch requires reconciliation.' ELSE error END WHERE scope_id=$scope AND watcher_cursor<=9223372036854775807-$count")){CycleId(q,id);q.Parameters.AddWithValue("$count",exactPaths.Count);q.Parameters.AddWithValue("$unknown",unknown?1:0);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new LocalSourceGapException("CycleCounter","Watcher counter exhausted; no hint skipped.");}
        await CommitCycle(c,t,before,ct);
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task<string> BeginCycleAsync(WindowsCycleConfiguration config,WindowsCycleBudget budget,CancellationToken ct)
        =>SerialAsync(()=>_store.BeginOwnedCycle(_root,_owner,config,budget,_admission,ct),ct);
    internal Task BindCycleCoherenceAsync(string id,string? coherence,CancellationToken ct)
        =>SerialAsync(async()=>{await _store.BindOwnedCycleCoherence(_root,_owner,id,coherence,_admission,ct);return true;},ct);
    internal Task<LocalCycleScope> ReadCycleScopeAsync(string id,CancellationToken ct)
        =>SerialAsync(()=>_store.ReadOwnedCycleScope(_root,_owner,id,ct),ct);
    internal Task<int> SeedCycleEntriesAsync(string id,CancellationToken ct)
        =>SerialAsync(()=>_store.SeedOwnedCycleEntries(_root,_owner,id,_admission,ct),ct);
    internal Task MarkCycleUncertainAsync(string id,string error,CancellationToken ct)
        =>SerialAsync(async()=>{await _store.MarkOwnedCycleUncertain(_root,_owner,id,error,ct);return true;},ct);
    internal Task MarkCycleHintsAsync(string id,IReadOnlyList<string> paths,CancellationToken ct)
        =>SerialAsync(async()=>{await _store.MarkOwnedCycleHints(_root,_owner,id,paths,_admission,ct);return true;},ct);
    internal Task MarkParentCycleHintsAsync(string id,IReadOnlyList<string> paths,CancellationToken ct)
        =>SerialAsync(async()=>{await _store.MarkOwnedCycleHints(_root,_owner,id,paths,_admission,ct,true);return true;},ct);
}
