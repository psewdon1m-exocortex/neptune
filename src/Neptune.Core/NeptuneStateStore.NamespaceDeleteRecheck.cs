using System.Text;
using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

internal sealed record NamespaceDeleteRecheck(NamespaceOperation Original,LocalCycleEntry Current);

public sealed partial class NeptuneStateStore
{
    private static async Task<bool> IsSettledDeletedNamespace(Microsoft.Data.Sqlite.SqliteConnection c,Microsoft.Data.Sqlite.SqliteTransaction t,string scope,string node,CancellationToken ct)
    {
        await using(var module=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='client_namespace_operations'"))if(await module.ExecuteScalarAsync(ct)is null)return false;
        var native=await HasIncarnations(c,t,ct)?"COALESCE((SELECT i.native_id FROM local_parent_reconcile_incarnations i WHERE i.scope_id=f.scope_id AND i.node_id=f.node_id),f.node_id)":"f.node_id";
        await using var q=SyncCommand(c,t,$"""
            SELECT 1 FROM local_cycle_files f WHERE f.scope_id=$scope AND f.node_id=$node AND f.phase=4 AND f.baseline IS NULL AND f.remote_node IS NULL
            AND f.ack_watcher=f.watcher_generation
            AND f.ack_journal=COALESCE((SELECT generation FROM local_coherence_dirty d WHERE d.node_id={native} AND d.coherence_id=(SELECT coherence_id FROM local_cycle_scopes WHERE scope_id=$scope)),0)
            AND EXISTS(SELECT 1 FROM client_namespace_operations o WHERE o.scope_id=f.scope_id AND o.node_id=f.node_id AND o.kind='quarantine-delete' AND o.state='PUBLISHED' AND o.approved=1 AND o.receipt_json IS NOT NULL
                AND f.created=json_extract(o.entry_json,'$.created') AND {native}=COALESCE(json_extract(o.entry_json,'$.nativeNodeId'),json_extract(o.entry_json,'$.nodeId')))
            AND NOT EXISTS(SELECT 1 FROM local_cycle_work w WHERE w.scope_id=f.scope_id AND w.node_id=f.node_id AND w.active=1)
            AND NOT EXISTS(SELECT 1 FROM client_namespace_operations active WHERE active.scope_id=f.scope_id AND active.node_id=f.node_id AND active.state NOT IN('PUBLISHED','NOT_PUBLISHED','CANCELLED'))
            """);
        CycleId(q,scope);q.Parameters.AddWithValue("$node",node);return await q.ExecuteScalarAsync(ct)is not null;
    }

    internal async Task<IReadOnlyList<NamespaceDeleteRecheck>> ReadDeletedNamespaceRechecks(string root,string owner,string scopeId,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);var scope=await RequireCycle(c,t,root,owner,scopeId,false,ct);
        if(!await HasParentModule(c,t,ct))return [];
        await using(var module=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='client_namespace_operations'"))if(await module.ExecuteScalarAsync(ct)is null)return [];
        var policy=await ReadMappingPolicy(c,t,scopeId,ct);
        var native=await HasIncarnations(c,t,ct)?"COALESCE((SELECT i.native_id FROM local_parent_reconcile_incarnations i WHERE i.scope_id=f.scope_id AND i.node_id=f.node_id),f.node_id)":"f.node_id";
        var originals=new List<NamespaceOperation>();
        await using(var q=SyncCommand(c,t,$"""
            SELECT o.request_json,o.entry_json,o.receipt_json FROM local_cycle_files f
            JOIN client_namespace_operations o ON o.operation_id=(SELECT latest.operation_id FROM client_namespace_operations latest
                WHERE latest.scope_id=f.scope_id AND latest.node_id=f.node_id AND latest.kind='quarantine-delete'
                AND latest.state='PUBLISHED' AND latest.approved=1 AND latest.receipt_json IS NOT NULL ORDER BY latest.created_at DESC LIMIT 1)
            WHERE f.scope_id=$scope AND f.phase<4 AND f.baseline IS NULL AND f.remote_node IS NULL
            AND f.created=json_extract(o.entry_json,'$.created') AND {native}=COALESCE(json_extract(o.entry_json,'$.nativeNodeId'),json_extract(o.entry_json,'$.nodeId'))
            AND NOT EXISTS(SELECT 1 FROM json_each($excludes) e WHERE neptune_path_within(f.path,e.value))
            AND NOT EXISTS(SELECT 1 FROM local_cycle_work w WHERE w.scope_id=f.scope_id AND w.node_id=f.node_id AND w.active=1)
            AND NOT EXISTS(SELECT 1 FROM client_namespace_operations active WHERE active.scope_id=f.scope_id AND active.node_id=f.node_id AND active.state NOT IN('PUBLISHED','NOT_PUBLISHED','CANCELLED'))
            ORDER BY f.node_id LIMIT 120
            """))
        {
            CycleId(q,scopeId);q.Parameters.AddWithValue("$excludes",JsonSerializer.Serialize(policy.ExcludedPrefixes));await using var r=await q.ExecuteReaderAsync(ct);
            while(await r.ReadAsync(ct))originals.Add(new(JsonSerializer.Deserialize<SyncMutationRequest>(r.GetString(0),LocalCycleWire.Json)!,JsonSerializer.Deserialize<LocalCycleEntry>(r.GetString(1),LocalCycleWire.Json)!,"PUBLISHED",true,r.GetString(2)));
        }
        var result=new List<NamespaceDeleteRecheck>();
        foreach(var original in originals)
        {
            var current=await CurrentCycleEntry(c,t,scope,original.Entry.NodeId,ct);
            if(current.SourceNodeId==original.Entry.SourceNodeId&&current.Created==original.Entry.Created&&!await IsCycleExcluded(c,t,scopeId,current.RelativePath,ct))result.Add(new(original,current));
        }
        return result;
    }

    internal async Task RetireDeletedNamespaceRechecks(string root,string owner,string scopeId,string planId,IReadOnlyList<NamespaceDeleteRecheck> checks,CancellationToken ct)
    {
        if(checks.Count is <1 or >120)throw new ArgumentOutOfRangeException(nameof(checks));
        await ClientAdmission(root,owner,scopeId,checks.Count*2,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var scope=await RequireCycle(c,t,root,owner,scopeId,false,ct);
        var local=await ReadLocalStateAsync(c,t,root,owner,ct);var coherence=await RequireCoherenceAsync(c,t,root,owner,scope.CoherenceId!,false,ct);
        var progress=await CycleProgress(c,t,scopeId,ct);
        if(!progress.Complete||coherence.State.Status!=LocalCoherenceStatus.Anchored||local.Cursor.NextReadUsn!=coherence.State.AppliedUsn)return;
        // This new native cut, rather than the historical mutation receipt,
        // may retire exactly the dirty pairs captured before its enumeration.
        await using(var proof=SyncCommand(c,t,"""
            SELECT 1 FROM local_parent_reconcile_plans p WHERE p.plan_id=$plan AND p.scope_id=$scope
            AND p.root_key=$root AND p.coherence_id=$coherence AND p.owner_epoch=$owner AND p.revision=$revision
            AND p.gap=$gap AND p.journal=$journal AND p.status=3 AND p.full_membership=1 AND p.hardlink_verified=1 AND p.cut_usn=$cut
            AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_plans pending WHERE pending.scope_id=$scope AND pending.status<3)
            """))
        {
            CycleId(proof,scopeId);proof.Parameters.AddWithValue("$plan",planId);proof.Parameters.AddWithValue("$root",root);proof.Parameters.AddWithValue("$coherence",scope.CoherenceId!);proof.Parameters.AddWithValue("$owner",local.Cursor.OwnerEpoch);proof.Parameters.AddWithValue("$revision",local.LegacyRevision);proof.Parameters.AddWithValue("$gap",local.GapEpoch);proof.Parameters.AddWithValue("$journal",local.Cursor.JournalIdHex);proof.Parameters.AddWithValue("$cut",coherence.State.AppliedUsn);
            if(await proof.ExecuteScalarAsync(ct)is null)return;
        }
        foreach(var check in checks)
        {
            var current=await CurrentCycleEntry(c,t,scope,check.Current.NodeId,ct);
            if(current.SourceNodeId!=check.Current.SourceNodeId||current.Created!=check.Current.Created||current.Generation!=check.Current.Generation||current.RelativePath!=check.Current.RelativePath||current.BaselineJson is not null||await IsCycleExcluded(c,t,scopeId,current.RelativePath,ct))continue;
            var receipt=SyncMutationWire.Read(Encoding.UTF8.GetBytes(check.Original.Receipt!),check.Original.Request);
            if(receipt.State!="PUBLISHED"||receipt.Description is null||receipt.LastSequence is not { } sequence||progress.Sequence<sequence||await CycleRemoteNode(c,t,scopeId,receipt.Description.NodeId,ct)is not null)continue;
            await using var q=SyncCommand(c,t,"""
                UPDATE local_cycle_files SET phase=4,ack_journal=$journal,ack_watcher=$watcher
                WHERE scope_id=$scope AND node_id=$node AND remote_node IS NULL AND baseline IS NULL
                AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_entries e WHERE e.plan_id=$plan AND (e.node_id=$native OR e.path=$path))
                AND NOT EXISTS(SELECT 1 FROM local_cycle_work w WHERE w.scope_id=$scope AND w.node_id=$node AND w.active=1)
                AND NOT EXISTS(SELECT 1 FROM client_namespace_operations active WHERE active.scope_id=$scope AND active.node_id=$node AND active.state NOT IN('PUBLISHED','NOT_PUBLISHED','CANCELLED'));
                DELETE FROM local_parent_reconcile_conflicts WHERE scope_id=$scope AND node_id=$node AND changes()=1;
                """);
            CycleId(q,scopeId);q.Parameters.AddWithValue("$node",current.NodeId);q.Parameters.AddWithValue("$native",current.SourceNodeId);q.Parameters.AddWithValue("$path",current.RelativePath);q.Parameters.AddWithValue("$plan",planId);q.Parameters.AddWithValue("$journal",current.Generation.Journal);q.Parameters.AddWithValue("$watcher",current.Generation.Watcher);await q.ExecuteNonQueryAsync(ct);
        }
        await t.CommitAsync(ct);
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task<IReadOnlyList<NamespaceDeleteRecheck>> ReadDeletedNamespaceRechecksAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.ReadDeletedNamespaceRechecks(_root,_owner,scope,ct),ct);
    internal Task RetireDeletedNamespaceRechecksAsync(string scope,string plan,IReadOnlyList<NamespaceDeleteRecheck> checks,CancellationToken ct)=>SerialAsync(async()=>{await _store.RetireDeletedNamespaceRechecks(_root,_owner,scope,plan,checks,ct);return true;},ct);
}
