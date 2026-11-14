using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<SqliteTransaction,Dictionary<string,MappingSyncPolicy>> PolicyTransactions=new();
    internal async Task EnsureClientEvolutionAsync(CancellationToken ct)
    {
        await InstallClientEvolution(ct);
    }
    private static async Task<MappingSyncPolicy> ReadMappingPolicy(SqliteConnection c,SqliteTransaction t,string scope,CancellationToken ct)
    {
        var cache=PolicyTransactions.GetValue(t,_=>new(StringComparer.Ordinal));if(cache.TryGetValue(scope,out var cached))return cached;
        await using(var exists=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE type='table' AND name='client_mapping_policies'"))if(await exists.ExecuteScalarAsync(ct)is null)return MappingSyncPolicy.Default;
        await using var q=SyncCommand(c,t,"SELECT policy_json,policy_hash FROM client_mapping_policies WHERE scope_id=$scope");CycleId(q,scope);await using var r=await q.ExecuteReaderAsync(ct);
        var policy=MappingSyncPolicy.Default;if(await r.ReadAsync(ct)){policy=JsonSerializer.Deserialize<MappingSyncPolicy>(r.GetString(0),LocalCycleWire.Json)??throw new InvalidDataException("Policy missing.");policy.Validate();if(policy.Hash!=r.GetString(1))throw new InvalidDataException("Policy hash changed.");}
        if(cache.Count>=16)cache.Clear();cache[scope]=policy;return policy;
    }
    private static async Task<string> CycleNameKey(SqliteConnection c,SqliteTransaction t,string scope,string name,CancellationToken ct)
    {var policy=await ReadMappingPolicy(c,t,scope,ct);return policy.Codec=="portable_v1"?SyncNameCodec.Key(name,policy.Codec):WebDavSyncClient.CanonicalPathKey(name);}
    internal async Task BindOwnedClientNamespace(string root,string owner,string scope,SyncNamespacePolicy authority,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scope,8,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        await using(var q=SyncCommand(c,t,"SELECT codec,policy_hash FROM client_namespace_bindings WHERE scope_id=$scope"))
        {CycleId(q,scope);await using var r=await q.ExecuteReaderAsync(ct);if(await r.ReadAsync(ct)){if(r.GetString(0)!=authority.Codec||r.GetString(1)!=authority.PolicyHash)throw new InvalidOperationException("Server namespace policy changed; explicit migration/adoption is required before resuming originals.");return;}}
        if(authority.Codec!="legacy")
        {
            await using var old=SyncCommand(c,t,"SELECT 1 FROM local_cycle_files WHERE scope_id=$scope LIMIT 1");CycleId(old,scope);if(await old.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Existing legacy namespace needs reviewed codec migration before portable activation.");
            var policy=new MappingSyncPolicy(authority.Codec,[]);await using var save=SyncCommand(c,t,"INSERT INTO client_mapping_policies VALUES($scope,$json,$hash)");CycleId(save,scope);save.Parameters.AddWithValue("$json",JsonSerializer.Serialize(policy,LocalCycleWire.Json));save.Parameters.AddWithValue("$hash",policy.Hash);await save.ExecuteNonQueryAsync(ct);
        }
        await using(var q=SyncCommand(c,t,"INSERT INTO client_namespace_bindings VALUES($scope,$codec,$hash)")){CycleId(q,scope);q.Parameters.AddWithValue("$codec",authority.Codec);q.Parameters.AddWithValue("$hash",authority.PolicyHash);await q.ExecuteNonQueryAsync(ct);}await t.CommitAsync(ct);
    }
    private static async Task<MappingPolicyPreview> PolicyPreview(SqliteConnection c,SqliteTransaction t,string scope,MappingSyncPolicy policy,CancellationToken ct)
    {
        policy.Validate();var previous=await ReadMappingPolicy(c,t,scope,ct);using var hash=IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        void Add(string text)=>hash.AppendData(Encoding.UTF8.GetBytes(text+"\n"));Add(scope);Add(previous.Hash);Add(policy.Hash);
        var examples=new List<string>();long affected=0,collisions=0;string? parent=null;var keys=new HashSet<string>(StringComparer.Ordinal);
        await using var q=SyncCommand(c,t,"SELECT f.node_id,f.parent_id,f.path,f.name,f.baseline,f.watcher_generation,COALESCE(d.generation,0) FROM local_cycle_files f JOIN local_cycle_scopes s ON s.scope_id=f.scope_id LEFT JOIN local_coherence_dirty d ON d.coherence_id=s.coherence_id AND d.node_id=f.node_id WHERE f.scope_id=$scope ORDER BY f.parent_id,f.node_id");CycleId(q,scope);
        await using var r=await q.ExecuteReaderAsync(ct);
        while(await r.ReadAsync(ct))
        {
            var path=r.GetString(2);var changed=previous.Excludes(path)!=policy.Excludes(path)||previous.Codec!=policy.Codec;
            if(changed){affected++;if(examples.Count<50)examples.Add(path);}
            Add(r.GetString(0));Add(path);Add(r.IsDBNull(4)?"":r.GetString(4));Add(r.GetInt64(5).ToString(System.Globalization.CultureInfo.InvariantCulture));Add(r.GetInt64(6).ToString(System.Globalization.CultureInfo.InvariantCulture));
            if(parent!=r.GetString(1)){parent=r.GetString(1);keys.Clear();}
            if(!policy.Excludes(path))try{if(!keys.Add(SyncNameCodec.Key(r.GetString(3),policy.Codec)))collisions++;}catch(InvalidDataException){collisions++;}
            if(keys.Count>100_000)throw new InvalidDataException("Codec preview parent exceeds bounded collision inventory.");
        }
        return new(scope,previous.Hash,policy.Hash,Convert.ToHexStringLower(hash.GetHashAndReset()),affected,collisions,examples);
    }
    internal async Task<MappingPolicyPreview> PreviewOwnedPolicy(string root,string owner,string scope,MappingSyncPolicy policy,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scope,16,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        var preview=await PolicyPreview(c,t,scope,policy,ct);
        await using var q=SyncCommand(c,t,"INSERT INTO client_policy_previews VALUES($scope,$policy,$preview) ON CONFLICT(scope_id) DO UPDATE SET policy_json=$policy,preview_json=$preview");CycleId(q,scope);q.Parameters.AddWithValue("$policy",JsonSerializer.Serialize(policy,LocalCycleWire.Json));q.Parameters.AddWithValue("$preview",JsonSerializer.Serialize(preview,LocalCycleWire.Json));await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);return preview;
    }
    internal async Task<MappingSyncPolicy> ReadOwnedMappingPolicy(string root,string owner,string scope,CancellationToken ct)
    {await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,scope,true,ct);return await ReadMappingPolicy(c,t,scope,ct);}
    internal async Task ApplyOwnedPolicy(string root,string owner,string scope,MappingSyncPolicy policy,string digest,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scope,16,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,false,ct);
        var preview=await PolicyPreview(c,t,scope,policy,ct);if(preview.Digest!=digest||preview.Collisions!=0)throw new InvalidOperationException("Policy preview changed or contains collisions.");
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_cycle_work WHERE scope_id=$scope AND active=1 UNION ALL SELECT 1 FROM client_namespace_operations WHERE scope_id=$scope AND state NOT IN('PUBLISHED','NOT_PUBLISHED','CANCELLED') LIMIT 1"))
        {CycleId(q,scope);if(await q.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Recover original operations before changing policy.");}
        var old=await ReadMappingPolicy(c,t,scope,ct);if(old.Codec!=policy.Codec)throw new InvalidOperationException("Codec transition also requires a qualified server root migration; preview is retained.");
        await using(var q=SyncCommand(c,t,"INSERT INTO client_mapping_policies VALUES($scope,$json,$hash) ON CONFLICT(scope_id) DO UPDATE SET policy_json=$json,policy_hash=$hash"))
        {CycleId(q,scope);q.Parameters.AddWithValue("$json",JsonSerializer.Serialize(policy,LocalCycleWire.Json));q.Parameters.AddWithValue("$hash",policy.Hash);await q.ExecuteNonQueryAsync(ct);}
        await using(var q=SyncCommand(c,t,"INSERT INTO client_policy_changes(scope_id,previous_json,proposed_hash) VALUES($scope,$previous,$hash)"))
        {CycleId(q,scope);q.Parameters.AddWithValue("$previous",JsonSerializer.Serialize(old,LocalCycleWire.Json));q.Parameters.AddWithValue("$hash",policy.Hash);await q.ExecuteNonQueryAsync(ct);}await t.CommitAsync(ct);
    }
    internal async Task<int> ContinueOwnedPolicy(string root,string owner,string scope,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scope,254,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,false,ct);
        MappingSyncPolicy old;string hash,after;
        await using(var q=SyncCommand(c,t,"SELECT previous_json,proposed_hash,after_node FROM client_policy_changes WHERE scope_id=$scope"))
        {CycleId(q,scope);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))return 0;old=JsonSerializer.Deserialize<MappingSyncPolicy>(r.GetString(0),LocalCycleWire.Json)!;hash=r.GetString(1);after=r.GetString(2);}
        var policy=await ReadMappingPolicy(c,t,scope,ct);if(policy.Hash!=hash)throw new InvalidDataException("Pending policy identity changed.");
        var rows=new List<(string Node,string Path)>();await using(var q=SyncCommand(c,t,"SELECT node_id,path FROM local_cycle_files WHERE scope_id=$scope AND node_id>$after ORDER BY node_id LIMIT 250"))
        {CycleId(q,scope);q.Parameters.AddWithValue("$after",after);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))rows.Add((r.GetString(0),r.GetString(1)));}
        foreach(var row in rows.Where(r=>old.Excludes(r.Path)&&!policy.Excludes(r.Path)))
        {await using var q=SyncCommand(c,t,"UPDATE local_cycle_files SET phase=0,watcher_generation=watcher_generation+1 WHERE scope_id=$scope AND node_id=$node AND watcher_generation<9223372036854775807");CycleId(q,scope);q.Parameters.AddWithValue("$node",row.Node);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidDataException("Policy generation exhausted.");}
        await using(var q=SyncCommand(c,t,rows.Count<250?"DELETE FROM client_policy_changes WHERE scope_id=$scope":"UPDATE client_policy_changes SET after_node=$after WHERE scope_id=$scope"))
        {CycleId(q,scope);if(rows.Count==250)q.Parameters.AddWithValue("$after",rows[^1].Node);await q.ExecuteNonQueryAsync(ct);}await t.CommitAsync(ct);return rows.Count;
    }
    private static async Task<bool> IsCycleExcluded(SqliteConnection c,SqliteTransaction t,string scope,string path,CancellationToken ct)
        =>(await ReadMappingPolicy(c,t,scope,ct)).Excludes(path);
}
public sealed partial class LocalSourceJournalSession
{
    internal Task InitializeClientEvolutionAsync(CancellationToken ct)=>SerialAsync(async()=>{await _store.EnsureClientEvolutionAsync(ct);return true;},ct);
    internal Task BindClientNamespaceAsync(string scope,SyncNamespacePolicy authority,CancellationToken ct)=>SerialAsync(async()=>{await _store.BindOwnedClientNamespace(_root,_owner,scope,authority,ct);return true;},ct);
    internal Task<MappingPolicyPreview> PreviewPolicyAsync(string scope,MappingSyncPolicy policy,CancellationToken ct)=>SerialAsync(()=>_store.PreviewOwnedPolicy(_root,_owner,scope,policy,ct),ct);
    internal Task<MappingSyncPolicy> ReadMappingPolicyAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedMappingPolicy(_root,_owner,scope,ct),ct);
    internal Task ApplyPolicyAsync(string scope,MappingSyncPolicy policy,string digest,CancellationToken ct)=>SerialAsync(async()=>{await _store.ApplyOwnedPolicy(_root,_owner,scope,policy,digest,ct);return true;},ct);
    internal Task<int> ContinuePolicyAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.ContinueOwnedPolicy(_root,_owner,scope,ct),ct);
}
