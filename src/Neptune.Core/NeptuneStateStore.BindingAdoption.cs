using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;
public sealed record ClientBindingPlan(string Schema,string PlanId,string OriginalScope,string NextScope,
    WindowsCycleConfiguration Configuration,MappingSyncPolicy Policy,SyncNamespacePolicy Namespace,
    string PreviewDigest,string ArtifactSha256,string ServerArtifactJson,IReadOnlyList<string> Blockers);

public sealed partial class NeptuneStateStore
{
    private static async Task RequireNoPendingBindingMigration(SqliteConnection c,SqliteTransaction t,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='client_binding_migrations'");if(await q.ExecuteScalarAsync(ct)is null)return;
        q.CommandText="SELECT 1 FROM client_binding_migrations WHERE state='COPYING' LIMIT 1";
        if(await q.ExecuteScalarAsync(ct)is not null)throw new InvalidOperationException("Resume the reviewed offline binding adoption before synchronization.");
    }
    public async Task<ClientBindingPlan> PreviewServerBindingAsync(string artifactJson,CancellationToken ct=default)
    {
        if(Encoding.UTF8.GetByteCount(artifactJson)>64*1024)throw new InvalidDataException("Server adoption artifact exceeds limit.");
        await ValidateProfileCompatibilityAsync(ct);
        await using var c=new SqliteConnection(ReadOnlySyncConnectionString(databasePath));await c.OpenAsync(ct);using var t=c.BeginTransaction(deferred:true);
        await RequireNoPendingBindingMigration(c,t,ct);
        // Installed journal replacement retains the old active row as history.
        // Select the scope owned by the current source subscription, then prove
        // that every other active row is its installed replacement predecessor.
        await using var q=SyncCommand(c,t,"SELECT a.scope_id FROM local_cycle_active a JOIN local_source_active s ON s.root_key=a.root_key LIMIT 2");var ids=new List<string>();
        await using(var r=await q.ExecuteReaderAsync(ct))while(await r.ReadAsync(ct))ids.Add(r.GetString(0));
        if(ids.Count!=1)throw new InvalidOperationException("Binding adoption needs one retained mapping scope.");
        var current=await CycleScope(c,t,ids[0],ct);
        var predecessors=new List<(string Scope,string Root)>();
        q.CommandText="SELECT scope_id,root_key FROM local_cycle_active WHERE scope_id<>$scope LIMIT 17";q.Parameters.AddWithValue("$scope",current.ScopeId);
        await using(var r=await q.ExecuteReaderAsync(ct))while(await r.ReadAsync(ct))predecessors.Add((r.GetString(0),r.GetString(1)));
        if(predecessors.Count>16)throw new InvalidOperationException("Binding adoption retained scope bound exceeded.");
        foreach(var previous in predecessors)
            if(!await HasReplacementLineage(c,t,current.ScopeId,previous.Scope,current.Root,previous.Root,current.Configuration,true,ct))
                throw new InvalidOperationException("Another active scope has no installed replacement lineage to the current source.");
        var old=await CycleScope(c,t,ids[0],ct);using var artifact=SyncProtocolReader.Parse(Encoding.UTF8.GetBytes(artifactJson));var body=artifact.RootElement;
        if(body.GetProperty("schema").GetString()!="saturn.sync-adoption.v1"||!LocalCycleWire.Uuid(body.GetProperty("planId").GetString()!)||!LocalCycleWire.Hash(body.GetProperty("planSha256").GetString()!)||!LocalCycleWire.Hash(body.GetProperty("treeSha256").GetString()!))throw new InvalidDataException("Unrecognized server adoption artifact.");
        var binding=SyncProtocolReader.Binding(body.GetProperty("binding"));var ns=body.GetProperty("namespace");var policy=new SyncNamespacePolicy(ns.GetProperty("codec").GetString()!,ns.GetProperty("policyHash").GetString()!);
        if(binding.RootId!=old.Configuration.Binding.RootId||binding.ServerIdentity!=old.Configuration.Binding.ServerIdentity||binding.ServerEpoch!=old.Configuration.Binding.ServerEpoch||body.GetProperty("device_id").GetString()!=old.Configuration.DeviceId||body.GetProperty("backend_identity").GetString()!=old.Configuration.BackendIdentity||!LocalCycleWire.Hash(policy.PolicyHash)||!LocalCycleWire.Hash(body.GetProperty("qualification_receipt_sha256").GetString()!))throw new InvalidDataException("Adoption cannot replace root, epoch, device or backend identity.");
        var mapping=(await ReadMappingPolicy(c,t,old.ScopeId,ct)) with{Codec=policy.Codec};mapping.Validate();
        var preview=await PolicyPreview(c,t,old.ScopeId,mapping,ct);var blockers=await BindingAdoptionBlockers(c,t,old,ct);
        if(preview.Collisions!=0)blockers.Add("Local codec collisions or incompatible names remain.");
        if(binding==old.Configuration.Binding)blockers.Add("Server artifact has not advanced the binding.");
        return new("neptune.binding-adoption.v1",Guid.NewGuid().ToString("N"),old.ScopeId,Guid.NewGuid().ToString("N"),old.Configuration with{Binding=binding},mapping,policy,preview.Digest,CycleHash(artifactJson),artifactJson,blockers);
    }
    private static async Task<List<string>> BindingAdoptionBlockers(SqliteConnection c,SqliteTransaction t,LocalCycleScope old,CancellationToken ct)
    {
        var blockers=new List<string>();
        if(!old.SeedComplete||old.CoherenceId is null||old.Status>1||!(await CycleProgress(c,t,old.ScopeId,ct)).Complete)blockers.Add("Complete the retained local/remote boundary first.");
        await using var q=SyncCommand(c,t,"SELECT 1 FROM local_cycle_work WHERE active=1 LIMIT 1");
        if(await q.ExecuteScalarAsync(ct)is not null||await HasClientOriginals(c,t,old.ScopeId,true,ct))blockers.Add("Resolve and acknowledge original operations before adoption.");
        q.CommandText="SELECT 1 FROM local_cycle_files WHERE scope_id=$scope AND phase<>4 LIMIT 1";CycleId(q,old.ScopeId);
        if(await q.ExecuteScalarAsync(ct)is not null)blockers.Add("Resolve pending local generations/conflicts before adoption.");
        q.Parameters.Clear();q.CommandText="SELECT 1 FROM sqlite_master WHERE name='client_policy_changes'";
        if(await q.ExecuteScalarAsync(ct)is not null){q.CommandText="SELECT 1 FROM client_policy_changes LIMIT 1";if(await q.ExecuteScalarAsync(ct)is not null)blockers.Add("Finish the previous policy continuation first.");}
        q.CommandText="SELECT 1 FROM local_source_roots WHERE root_key=$root AND valid=1 AND gap_code IS NULL AND legacy_revision=$revision AND gap_epoch=$gap";q.Parameters.AddWithValue("$root",old.Root);q.Parameters.AddWithValue("$revision",old.Revision);q.Parameters.AddWithValue("$gap",old.Gap);
        if(await q.ExecuteScalarAsync(ct)is null)blockers.Add("Source continuity requires recovery.");
        return blockers;
    }
    public Task ApplyServerBindingAsync(ClientBindingPlan plan,CancellationToken ct=default)=>ApplyServerBindingCore(plan,null,ct);
    internal Task ApplyServerBindingAsync(ClientBindingPlan plan,Func<Task> pageCommitted,CancellationToken ct)=>ApplyServerBindingCore(plan,pageCommitted,ct);
    private async Task ApplyServerBindingCore(ClientBindingPlan plan,Func<Task>? pageCommitted,CancellationToken ct)
    {
        if(plan.Schema!="neptune.binding-adoption.v1"||!Guid.TryParseExact(plan.PlanId,"N",out _)||!Guid.TryParseExact(plan.NextScope,"N",out _)||plan.Blockers.Count!=0||CycleHash(plan.ServerArtifactJson)!=plan.ArtifactSha256)throw new InvalidDataException("Invalid reviewed binding plan.");
        await using var owner=new FileStream(Path.GetFullPath(databasePath)+".local-source-owner.lock",FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None);
        await ValidateProfileCompatibilityAsync(ct);await EnsureClientEvolutionAsync(ct);
        // Offline adoption has no journal session to establish page size.
        // Measure it before computing the bounded continuation WAL reserve.
        await InspectLocalJournalWalAsync(force:true,cancellationToken:ct);var raw=JsonSerializer.Serialize(plan,LocalCycleWire.Json);
        await using(var read=await OpenCycleConnection(ct))
        {
            LocalCycleScope original;using(var snapshot=read.BeginTransaction(deferred:true))original=await CycleScope(read,snapshot,plan.OriginalScope,ct);
            await CheckLocalAdmissionAsync(new(MaximumDatabaseBytes:original.Budget.ProfileBytes,MinimumFreeDiskBytes:original.Budget.MinimumFreeDiskBytes),0,NamespaceReserve(16),ct);
        }
        await using(var c=await OpenCycleConnection(ct))
        {
            using var t=c.BeginTransaction(deferred:false);await using var exists=SyncCommand(c,t,"SELECT plan_json FROM client_binding_migrations WHERE plan_id=$plan");exists.Parameters.AddWithValue("$plan",plan.PlanId);
            var saved=await exists.ExecuteScalarAsync(ct)as string;
            if(saved is not null&&saved!=raw)throw new InvalidDataException("Original binding plan changed.");
            if(saved is null)
            {
                await RequireNoPendingBindingMigration(c,t,ct);var old=await CycleScope(c,t,plan.OriginalScope,ct);
                if(JsonSerializer.Serialize(plan.Configuration with{Binding=old.Configuration.Binding},LocalCycleWire.Json)!=JsonSerializer.Serialize(old.Configuration,LocalCycleWire.Json)||(await BindingAdoptionBlockers(c,t,old,ct)).Count!=0||(await PolicyPreview(c,t,old.ScopeId,plan.Policy,ct)).Digest!=plan.PreviewDigest)throw new InvalidOperationException("Binding preview changed; create a new reviewed plan.");
                using var artifact=SyncProtocolReader.Parse(Encoding.UTF8.GetBytes(plan.ServerArtifactJson));var body=artifact.RootElement;
                if(SyncProtocolReader.Binding(body.GetProperty("binding"))!=plan.Configuration.Binding||body.GetProperty("namespace").GetProperty("codec").GetString()!=plan.Namespace.Codec||body.GetProperty("namespace").GetProperty("policyHash").GetString()!=plan.Namespace.PolicyHash||plan.Policy.Codec!=plan.Namespace.Codec||plan.Configuration.Binding.RootId!=old.Configuration.Binding.RootId||plan.Configuration.Binding.ServerIdentity!=old.Configuration.Binding.ServerIdentity||plan.Configuration.Binding.ServerEpoch!=old.Configuration.Binding.ServerEpoch||body.GetProperty("device_id").GetString()!=old.Configuration.DeviceId||body.GetProperty("backend_identity").GetString()!=old.Configuration.BackendIdentity)throw new InvalidDataException("Binding plan differs from its server artifact or original authority.");
                await using(var count=SyncCommand(c,t,"SELECT COUNT(*) FROM local_cycle_scopes"))if(Convert.ToInt64(await count.ExecuteScalarAsync(ct))>=16)throw new InvalidOperationException("Retained scope limit reached; migration cannot discard history.");
                await using var q=SyncCommand(c,t,"INSERT INTO client_binding_migrations(plan_id,old_scope,new_scope,plan_json) VALUES($plan,$old,$new,$json);INSERT INTO local_cycle_scopes(scope_id,root_key,coherence_id,configuration,fingerprint,owner_epoch,legacy_revision,gap_epoch,max_profile,free_floor,seed_after,seed_complete,confirmed_files,confirmed_bytes) SELECT $new,root_key,coherence_id,$configuration,$fingerprint,owner_epoch,legacy_revision,gap_epoch,max_profile,free_floor,seed_after,1,confirmed_files,confirmed_bytes FROM local_cycle_scopes WHERE scope_id=$old;UPDATE local_cycle_format SET bytes=bytes+1024 WHERE singleton=1");
                q.Parameters.AddWithValue("$plan",plan.PlanId);q.Parameters.AddWithValue("$old",old.ScopeId);q.Parameters.AddWithValue("$new",plan.NextScope);q.Parameters.AddWithValue("$json",raw);var config=JsonSerializer.Serialize(plan.Configuration,LocalCycleWire.Json);q.Parameters.AddWithValue("$configuration",config);q.Parameters.AddWithValue("$fingerprint",CycleHash(config));await q.ExecuteNonQueryAsync(ct);
            }
            await t.CommitAsync(ct);
        }
        while(await ContinueBindingAdoption(plan,ct)){if(pageCommitted is not null)await pageCommitted();ct.ThrowIfCancellationRequested();}
    }
    private async Task<bool> ContinueBindingAdoption(ClientBindingPlan plan,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);LocalCycleScope old;using(var read=c.BeginTransaction(deferred:true))old=await CycleScope(c,read,plan.OriginalScope,ct);
        await CheckLocalAdmissionAsync(new(MaximumDatabaseBytes:old.Budget.ProfileBytes,MinimumFreeDiskBytes:old.Budget.MinimumFreeDiskBytes),0,NamespaceReserve(260),ct);
        using var t=c.BeginTransaction(deferred:false);string after;
        await using(var q=SyncCommand(c,t,"SELECT after_node,state,plan_json FROM client_binding_migrations WHERE plan_id=$plan")){q.Parameters.AddWithValue("$plan",plan.PlanId);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)||r.GetString(2)!=JsonSerializer.Serialize(plan,LocalCycleWire.Json))throw new InvalidDataException("Retained binding continuation changed.");if(r.GetString(1)!="COPYING")return false;after=r.GetString(0);}
        var nodes=new List<(string Id,long Bytes)>();await using(var q=SyncCommand(c,t,"SELECT node_id,bytes FROM local_cycle_files WHERE scope_id=$old AND node_id>$after ORDER BY node_id LIMIT 120")){q.Parameters.AddWithValue("$old",plan.OriginalScope);q.Parameters.AddWithValue("$after",after);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))nodes.Add((r.GetString(0),r.GetInt64(1)));}
        if(nodes.Count>0)
        {
            await using var q=SyncCommand(c,t,"INSERT INTO local_cycle_files SELECT $new,node_id,parent_id,path,name,created,kind,size,watcher_generation,ack_journal,ack_watcher,baseline,baseline_work,remote_node,4,bytes FROM local_cycle_files WHERE scope_id=$old AND node_id>$after AND node_id<=$last;UPDATE client_binding_migrations SET after_node=$last WHERE plan_id=$plan;UPDATE local_cycle_scopes SET files=files+$count WHERE scope_id=$new");
            q.Parameters.AddWithValue("$new",plan.NextScope);q.Parameters.AddWithValue("$old",plan.OriginalScope);q.Parameters.AddWithValue("$after",after);q.Parameters.AddWithValue("$last",nodes[^1].Id);q.Parameters.AddWithValue("$plan",plan.PlanId);q.Parameters.AddWithValue("$count",nodes.Count);await q.ExecuteNonQueryAsync(ct);await AddCycleBytes(c,t,plan.NextScope,nodes.Sum(x=>x.Bytes),ct);
            if(await HasIncarnations(c,t,ct))
            {
                await using var copy=SyncCommand(c,t,"INSERT INTO local_parent_reconcile_incarnations SELECT $new,node_id,native_id,created,prior_native_id,prior_created,plan_id,cut_usn,baseline_work FROM local_parent_reconcile_incarnations WHERE scope_id=$old AND node_id>$after AND node_id<=$last;UPDATE local_parent_reconcile_format SET bytes=bytes+512*changes() WHERE singleton=1");
                copy.Parameters.AddWithValue("$new",plan.NextScope);copy.Parameters.AddWithValue("$old",plan.OriginalScope);copy.Parameters.AddWithValue("$after",after);copy.Parameters.AddWithValue("$last",nodes[^1].Id);await copy.ExecuteNonQueryAsync(ct);
            }
        }
        if(nodes.Count<120)
        {
            await using var q=SyncCommand(c,t,"INSERT INTO client_mapping_policies VALUES($new,$policy,$hash);INSERT INTO client_namespace_bindings VALUES($new,$codec,$authority);UPDATE local_cycle_scopes SET status=3,error='Reviewed server binding adoption; original scope retained.' WHERE scope_id=$old;UPDATE local_cycle_active SET scope_id=$new WHERE scope_id=$old;UPDATE client_binding_migrations SET state='QUALIFYING' WHERE plan_id=$plan");
            q.Parameters.AddWithValue("$new",plan.NextScope);q.Parameters.AddWithValue("$old",plan.OriginalScope);q.Parameters.AddWithValue("$policy",JsonSerializer.Serialize(plan.Policy,LocalCycleWire.Json));q.Parameters.AddWithValue("$hash",plan.Policy.Hash);q.Parameters.AddWithValue("$codec",plan.Namespace.Codec);q.Parameters.AddWithValue("$authority",plan.Namespace.PolicyHash);q.Parameters.AddWithValue("$plan",plan.PlanId);await q.ExecuteNonQueryAsync(ct);
        }
        await t.CommitAsync(ct);return nodes.Count==120;
    }
    public async Task<string?> ReadAdoptedAnchorAsync(SyncRootBinding binding,SyncNamespacePolicy? policy,string qualificationReceiptSha256,CancellationToken ct=default)
    {
        if(!await PreflightClientEvolution(ct))return null;await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);await RequireNoPendingBindingMigration(c,t,ct);
        await using var q=SyncCommand(c,t,"SELECT a.scope_id FROM local_cycle_active a JOIN local_source_active s ON s.root_key=a.root_key LIMIT 2");
        var ids=new List<string>();await using(var r=await q.ExecuteReaderAsync(ct))while(await r.ReadAsync(ct))ids.Add(r.GetString(0));
        if(ids.Count!=1)throw new InvalidDataException("Adopted anchor requires one current source scope.");
        var current=await CycleScope(c,t,ids[0],ct);
        q.CommandText="SELECT m.plan_json,m.new_scope,s.root_key FROM client_binding_migrations m JOIN local_cycle_scopes s ON s.scope_id=m.new_scope WHERE m.state IN('QUALIFYING','APPLIED') LIMIT 17";
        var candidates=new List<(ClientBindingPlan Plan,string Scope,string Root)>();
        await using(var r=await q.ExecuteReaderAsync(ct))while(await r.ReadAsync(ct))candidates.Add((JsonSerializer.Deserialize<ClientBindingPlan>(r.GetString(0),LocalCycleWire.Json)!,r.GetString(1),r.GetString(2)));
        if(candidates.Count>16)throw new InvalidDataException("Adopted anchor retained history bound exceeded.");
        ClientBindingPlan? selected=null;
        foreach(var candidate in candidates)
        {
            var plan=candidate.Plan;
            if(candidate.Scope!=current.ScopeId&&!await HasReplacementLineage(c,t,current.ScopeId,candidate.Scope,current.Root,candidate.Root,plan.Configuration,true,ct))continue;
            using var artifact=SyncProtocolReader.Parse(Encoding.UTF8.GetBytes(plan.ServerArtifactJson));
            if(selected is not null||plan.Configuration.Binding!=binding||plan.Namespace!=policy||artifact.RootElement.GetProperty("qualification_receipt_sha256").GetString()!=qualificationReceiptSha256)
                throw new InvalidDataException("HTTPS capability differs from the reviewed server adoption.");
            selected=plan;
        }
        return selected?.Configuration.RemoteAnchorId;
    }
    internal async Task<int> QualifyOwnedBinding(string root,string owner,string scope,CancellationToken ct)
    {
        await using(var read=await OpenCycleConnection(ct))
        {using var snapshot=read.BeginTransaction(deferred:true);await using var pending=SyncCommand(read,snapshot,"SELECT 1 FROM client_binding_migrations WHERE new_scope=$scope AND state='QUALIFYING'");CycleId(pending,scope);if(await pending.ExecuteScalarAsync(ct)is null)return 0;}
        await ClientAdmission(root,owner,scope,254,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var current=await RequireCycle(c,t,root,owner,scope,false,ct);
        string after;await using(var q=SyncCommand(c,t,"SELECT remote_after FROM client_binding_migrations WHERE new_scope=$scope AND state='QUALIFYING'")){CycleId(q,scope);after=await q.ExecuteScalarAsync(ct)as string??"\0";}
        if(after=="\0")return 0;if(!(await CycleProgress(c,t,scope,ct)).Complete)throw new InvalidOperationException("Adoption requires a fresh complete remote cut.");
        var nodes=new List<string>();await using(var q=SyncCommand(c,t,"SELECT node_id FROM local_cycle_files WHERE scope_id=$scope AND node_id>$after ORDER BY node_id LIMIT 250")){CycleId(q,scope);q.Parameters.AddWithValue("$after",after);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))nodes.Add(r.GetString(0));}
        foreach(var node in nodes)
        {
            var entry=await CurrentCycleEntry(c,t,current,node,ct);var baseline=entry.BaselineJson is null?null:JsonSerializer.Deserialize<LocalCycleBaseline>(entry.BaselineJson,LocalCycleWire.Json);
            var remote=baseline is null?null:await CycleRemoteNode(c,t,scope,baseline.Description.NodeId,ct);
            var phase=baseline is null?0:remote!=baseline.Description?2:entry.Generation!=baseline.Generation?0:4;
            await using var q=SyncCommand(c,t,"UPDATE local_cycle_files SET phase=$phase WHERE scope_id=$scope AND node_id=$node AND phase=4");CycleId(q,scope);q.Parameters.AddWithValue("$node",node);q.Parameters.AddWithValue("$phase",phase);await q.ExecuteNonQueryAsync(ct);
        }
        await using(var q=SyncCommand(c,t,"UPDATE client_binding_migrations SET remote_after=$after,state=$state WHERE new_scope=$scope AND state='QUALIFYING'")){CycleId(q,scope);q.Parameters.AddWithValue("$after",nodes.Count==0?after:nodes[^1]);q.Parameters.AddWithValue("$state",nodes.Count<250?"APPLIED":"QUALIFYING");await q.ExecuteNonQueryAsync(ct);}await t.CommitAsync(ct);return nodes.Count;
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<int> QualifyBindingAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.QualifyOwnedBinding(_root,_owner,scope,ct),ct);
}
