using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

public sealed record NamespaceOperationPreview(string OperationId,string Kind,string Path,string State,bool Approved,string? Error);
public sealed record NamespaceDeletePreview(string Digest,IReadOnlyList<NamespaceOperationPreview> Operations)
{public IReadOnlyList<NamespaceOperationPreview> Failures { get; init; }=[];}
internal sealed record NamespaceOperation(SyncMutationRequest Request,LocalCycleEntry Entry,string State,bool Approved,string? Receipt);

public sealed partial class NeptuneStateStore
{
    private static async Task<bool> NamespaceBytesUnchanged(Microsoft.Data.Sqlite.SqliteConnection c,Microsoft.Data.Sqlite.SqliteTransaction t,LocalCycleScope scope,LocalCycleEntry entry,LocalCycleBaseline baseline,CancellationToken ct)
    {
        if(entry.Directory)return true;
        var captured=JsonSerializer.Deserialize<LocalCycleCaptureEvidence>(baseline.CaptureProof,LocalCycleWire.Json);
        if(captured is null||captured.NodeId!=entry.SourceNodeId||captured.Created!=entry.Created)return false;
        if(entry.Generation.Journal==baseline.Generation.Journal)return true;
        await using var q=SyncCommand(c,t,"SELECT 1 FROM local_coherence_dirty WHERE coherence_id=$coherence AND node_id=$node AND generation=$generation AND (reasons & ~2147495936)=0");
        q.Parameters.AddWithValue("$coherence",scope.CoherenceId!);q.Parameters.AddWithValue("$node",entry.SourceNodeId);q.Parameters.AddWithValue("$generation",entry.Generation.Journal);
        return await q.ExecuteScalarAsync(ct)is not null; // CLOSE | RENAME_OLD | RENAME_NEW only
    }
    private static async Task SaveNamespaceOperation(SqliteConnection c,SqliteTransaction t,string scope,SyncMutationRequest request,LocalCycleEntry entry,bool approved,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,"INSERT INTO client_namespace_operations(operation_id,scope_id,node_id,kind,request_json,entry_json,approved,created_at) VALUES($op,$scope,$node,$kind,$request,$entry,$approved,$at) ON CONFLICT(operation_id) DO NOTHING");
        CycleId(q,scope);q.Parameters.AddWithValue("$op",request.OperationId);q.Parameters.AddWithValue("$node",entry.NodeId);q.Parameters.AddWithValue("$kind",request.Kind);
        q.Parameters.AddWithValue("$request",JsonSerializer.Serialize(request,LocalCycleWire.Json));q.Parameters.AddWithValue("$entry",JsonSerializer.Serialize(entry,LocalCycleWire.Json));q.Parameters.AddWithValue("$approved",approved?1:0);q.Parameters.AddWithValue("$at",DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());await q.ExecuteNonQueryAsync(ct);
    }
    internal async Task<int> PlanOwnedNamespace(string root,string owner,string scopeId,SyncNamespacePolicy authority,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scopeId,500,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var scope=await RequireCycle(c,t,root,owner,scopeId,false,ct);
        // A fresh anchor has no retained structural transitions. The parent
        // module is installed lazily when reconciliation is first needed.
        if(!await HasParentModule(c,t,ct))return 0;
        if(!(await CycleProgress(c,t,scopeId,ct)).Complete)throw new InvalidOperationException("Namespace planning needs the complete remote cut.");
        var policy=await ReadMappingPolicy(c,t,scopeId,ct);var candidates=new List<(string Node,string Plan)>();
        await using(var q=SyncCommand(c,t,"""
            SELECT f.node_id,p.plan_id FROM local_parent_reconcile_conflicts p
            JOIN local_cycle_files f ON f.scope_id=p.scope_id AND f.node_id=p.node_id
            JOIN local_parent_reconcile_plans proof ON proof.plan_id=p.plan_id AND proof.status=3
            WHERE f.scope_id=$scope AND f.baseline IS NOT NULL
            AND NOT EXISTS(SELECT 1 FROM local_cycle_work w WHERE w.scope_id=f.scope_id AND w.node_id=f.node_id AND w.active=1)
            AND NOT EXISTS(SELECT 1 FROM client_namespace_operations o WHERE o.scope_id=f.scope_id AND o.node_id=f.node_id AND o.state NOT IN('PUBLISHED','NOT_PUBLISHED','CANCELLED'))
            AND NOT EXISTS(SELECT 1 FROM client_namespace_operations o WHERE o.scope_id=f.scope_id AND o.node_id=f.node_id AND o.state='NOT_PUBLISHED' AND o.error IS NOT NULL AND json_extract(o.entry_json,'$.generation.watcher')=f.watcher_generation AND json_extract(o.entry_json,'$.generation.journal')=COALESCE((SELECT generation FROM local_coherence_dirty d WHERE d.node_id=COALESCE((SELECT i.native_id FROM local_parent_reconcile_incarnations i WHERE i.scope_id=f.scope_id AND i.node_id=f.node_id),f.node_id) AND d.coherence_id=(SELECT coherence_id FROM local_cycle_scopes WHERE scope_id=f.scope_id)),0))
            ORDER BY length(f.path) DESC,f.node_id LIMIT 120
            """)){CycleId(q,scopeId);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))candidates.Add((r.GetString(0),r.GetString(1)));}
        var added=0;
        foreach(var candidate in candidates)
        {
            var entry=await CurrentCycleEntry(c,t,scope,candidate.Node,ct);if(entry.Directory?policy.ProtectsSubtree(entry.RelativePath):policy.Excludes(entry.RelativePath))continue;
            var baseline=JsonSerializer.Deserialize<LocalCycleBaseline>(entry.BaselineJson!,LocalCycleWire.Json)!;
            if(await CycleRemoteNode(c,t,scopeId,baseline.Description.NodeId,ct)!=baseline.Description)continue;
            ParentReconcileEntry? current=null;
            await using(var q=SyncCommand(c,t,"SELECT entry_id,node_id,parent_id,name,created,kind,size,wire64,path FROM local_parent_reconcile_entries WHERE plan_id=$plan AND node_id=$node"))
            {q.Parameters.AddWithValue("$plan",candidate.Plan);q.Parameters.AddWithValue("$node",entry.SourceNodeId);await using var r=await q.ExecuteReaderAsync(ct);if(await r.ReadAsync(ct))current=ParentEntry(r);}
            SyncMutationRequest request;LocalCycleEntry proposed;
            if(current is not null)
            {
                if(current.Created!=entry.Created||(current.Kind!=0)!=entry.Directory||(entry.Directory?policy.ProtectsSubtree(current.Path):policy.Excludes(current.Path)))continue;
                proposed=entry with{ParentId=current.Parent,RelativePath=current.Path,RawName=current.Name};
                if(proposed.ParentId==entry.ParentId&&proposed.RawName==entry.RawName)
                {
                    // A renamed ancestor changes only this derived local path.
                    var unchanged=await NamespaceBytesUnchanged(c,t,scope,entry,baseline,ct);
                    await using var path=SyncCommand(c,t,"UPDATE local_cycle_files SET path=$path,phase=$phase,ack_journal=CASE WHEN $phase=4 THEN $journal ELSE ack_journal END,ack_watcher=CASE WHEN $phase=4 THEN $watcher ELSE ack_watcher END WHERE scope_id=$scope AND node_id=$node;DELETE FROM local_parent_reconcile_conflicts WHERE scope_id=$scope AND node_id=$node");CycleId(path,scopeId);path.Parameters.AddWithValue("$node",entry.NodeId);path.Parameters.AddWithValue("$path",proposed.RelativePath);path.Parameters.AddWithValue("$phase",unchanged?4:0);path.Parameters.AddWithValue("$journal",entry.Generation.Journal);path.Parameters.AddWithValue("$watcher",entry.Generation.Watcher);await path.ExecuteNonQueryAsync(ct);continue;
                }
                string parent;try{parent=await CycleTargetParent(c,t,scope,proposed,ct);}catch(BootstrapCaptureDeferredException){continue;}
                _=SyncNameCodec.Key(proposed.RawName,authority.Codec);
                request=SyncMutationWire.Create(scope.Configuration.Binding,authority,Guid.NewGuid().ToString("D"),"move",baseline.Description,new Dictionary<string,object?>{{"parentId",parent},{"name",proposed.RawName}});
            }
            else
            {
                // A replacement at the retained path is a conflict, not an
                // absence eligible for a reviewed remote deletion.
                await using var occupied=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_entries WHERE plan_id=$plan AND path=$path LIMIT 1");occupied.Parameters.AddWithValue("$plan",candidate.Plan);occupied.Parameters.AddWithValue("$path",entry.RelativePath);if(await occupied.ExecuteScalarAsync(ct)is not null)continue;
                // Absence from a single parent is not proof against a move.
                await using var proof=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_plans WHERE plan_id=$plan AND status=3 AND full_membership=1 AND hardlink_verified=1");proof.Parameters.AddWithValue("$plan",candidate.Plan);if(await proof.ExecuteScalarAsync(ct)is null)continue;
                proposed=entry;
                request=SyncMutationWire.Create(scope.Configuration.Binding,authority,Guid.NewGuid().ToString("D"),"quarantine-delete",baseline.Description,new Dictionary<string,object?>{{"planId",null},{"planDigest",null}});
            }
            await SaveNamespaceOperation(c,t,scopeId,request,proposed,request.Kind=="move",ct);added++;
        }
        await t.CommitAsync(ct);return added;
    }
    internal async Task<IReadOnlyList<NamespaceOperation>> ReadOwnedNamespace(string root,string owner,string scope,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        var rows=new List<NamespaceOperation>();await using var q=SyncCommand(c,t,"SELECT request_json,entry_json,state,approved,receipt_json FROM client_namespace_operations WHERE scope_id=$scope AND state NOT IN('PUBLISHED','NOT_PUBLISHED','CANCELLED') ORDER BY CASE WHEN kind='quarantine-delete' THEN 1 ELSE 0 END,length(json_extract(entry_json,'$.relativePath')) DESC,created_at LIMIT 500");CycleId(q,scope);
        await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))rows.Add(new(JsonSerializer.Deserialize<SyncMutationRequest>(r.GetString(0),LocalCycleWire.Json)!,JsonSerializer.Deserialize<LocalCycleEntry>(r.GetString(1),LocalCycleWire.Json)!,r.GetString(2),r.GetInt64(3)==1,r.IsDBNull(4)?null:r.GetString(4)));return rows;
    }
    internal async Task<bool> ValidateOwnedNamespace(string root,string owner,string scopeId,NamespaceOperation operation,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);var scope=await RequireCycle(c,t,root,owner,scopeId,false,ct);
        var current=await CurrentCycleEntry(c,t,scope,operation.Entry.NodeId,ct);
        // A late duplicate removal hint can advance the local generation after
        // a reviewed delete plan was accepted, including after its lost reply.
        // Keep that immutable plan only while a new complete membership proof
        // still establishes absence of the same incarnation and remote B.
        // Native journal generations always remain exact. Only watcher hints
        // may advance; unreviewed previews and moves retain both generations.
        var reviewedDelete=operation.Request.Kind=="quarantine-delete"&&operation.Approved
            &&operation.Request.Input.TryGetProperty("planId",out var planId)&&planId.ValueKind==JsonValueKind.String
            &&Guid.TryParse(planId.GetString(),out _);
        if(current.SourceNodeId!=operation.Entry.SourceNodeId||current.Created!=operation.Entry.Created
            ||current.Generation.Journal!=operation.Entry.Generation.Journal
            ||current.Generation.Watcher!=operation.Entry.Generation.Watcher&&!reviewedDelete
            ||current.BaselineJson!=operation.Entry.BaselineJson
            ||await IsCycleExcluded(c,t,scopeId,current.RelativePath,ct)||await IsCycleExcluded(c,t,scopeId,operation.Entry.RelativePath,ct))return false;
        if(reviewedDelete)
        {
            await using var pending=SyncCommand(c,t,"SELECT 1 FROM local_cycle_work WHERE scope_id=$scope AND node_id=$node AND active=1 LIMIT 1");
            CycleId(pending,scopeId);pending.Parameters.AddWithValue("$node",current.NodeId);
            if(await pending.ExecuteScalarAsync(ct)is not null)return false;
        }
        var baseline=JsonSerializer.Deserialize<LocalCycleBaseline>(current.BaselineJson!,LocalCycleWire.Json)!;
        if(await CycleRemoteNode(c,t,scopeId,baseline.Description.NodeId,ct)!=baseline.Description)return false;
        await using var q=SyncCommand(c,t,"""
            SELECT proof.full_membership,proof.hardlink_verified,e.path,e.created,e.kind,e.parent_id,e.name
            FROM local_parent_reconcile_conflicts p JOIN local_parent_reconcile_plans proof ON proof.plan_id=p.plan_id AND proof.status=3
            LEFT JOIN local_parent_reconcile_entries e ON e.plan_id=p.plan_id AND e.node_id=COALESCE((SELECT i.native_id FROM local_parent_reconcile_incarnations i WHERE i.scope_id=p.scope_id AND i.node_id=p.node_id),p.node_id)
            WHERE p.scope_id=$scope AND p.node_id=$node
            AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_plans pending WHERE pending.scope_id=$scope AND pending.status<3)
            AND ($delete=0 OR NOT EXISTS(SELECT 1 FROM local_parent_reconcile_entries replacement WHERE replacement.plan_id=p.plan_id AND replacement.path=$path))
            """);CycleId(q,scopeId);q.Parameters.AddWithValue("$node",current.NodeId);q.Parameters.AddWithValue("$delete",operation.Request.Kind=="quarantine-delete"?1:0);q.Parameters.AddWithValue("$path",current.RelativePath);await using var r=await q.ExecuteReaderAsync(ct);
        if(!await r.ReadAsync(ct))return false;
        return operation.Request.Kind=="quarantine-delete"?r.IsDBNull(2)&&r.GetInt64(0)==1&&r.GetInt64(1)==1:
            !r.IsDBNull(2)&&r.GetString(2)==operation.Entry.RelativePath&&r.GetInt64(3)==operation.Entry.Created&&(r.GetInt32(4)!=0)==operation.Entry.Directory&&r.GetString(5)==operation.Entry.ParentId&&r.GetString(6)==operation.Entry.RawName;
    }
    internal async Task RecordOwnedNamespace(string root,string owner,string scope,string operation,string state,SyncMutationReceipt? receipt,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scope,16,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        await using var q=SyncCommand(c,t,"UPDATE client_namespace_operations SET state=$state,receipt_json=COALESCE($receipt,receipt_json) WHERE operation_id=$op AND scope_id=$scope AND state NOT IN('PUBLISHED','NOT_PUBLISHED','CANCELLED')");CycleId(q,scope);q.Parameters.AddWithValue("$op",operation);q.Parameters.AddWithValue("$state",state);q.Parameters.AddWithValue("$receipt",(object?)receipt?.RawJson??DBNull.Value);await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);
    }
    internal async Task RecordOwnedNamespaceFailure(string root,string owner,string scope,string operation,string code,bool notPublished,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scope,4,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        await using var q=SyncCommand(c,t,"UPDATE client_namespace_operations SET state=CASE WHEN $terminal=1 THEN 'NOT_PUBLISHED' ELSE state END,error=$code WHERE scope_id=$scope AND operation_id=$op AND state NOT IN('PUBLISHED','NOT_PUBLISHED','CANCELLED')");CycleId(q,scope);q.Parameters.AddWithValue("$op",operation);q.Parameters.AddWithValue("$code",code[..Math.Min(code.Length,128)]);q.Parameters.AddWithValue("$terminal",notPublished?1:0);await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);
    }
    internal async Task ApplyOwnedNamespaceReceipt(string root,string owner,string scopeId,NamespaceOperation original,SyncMutationReceipt receipt,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scopeId,16,ct);
        if(receipt.State!="PUBLISHED"||receipt.Description is null)throw new InvalidDataException("Namespace publication required.");
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var scope=await RequireCycle(c,t,root,owner,scopeId,false,ct);
        var remote=await CycleRemoteNode(c,t,scopeId,receipt.Description.NodeId,ct);
        var progress=await CycleProgress(c,t,scopeId,ct);
        var lastSequence=receipt.LastSequence??throw new InvalidDataException("Published mutation has no remote sequence.");
        if(!progress.Complete||progress.Sequence<lastSequence)throw new InvalidOperationException("Mutation receipt has not reached the current remote feed.");
        var matched=original.Request.Kind=="quarantine-delete"?remote is null:remote==receipt.Description;
        var entry=await CurrentCycleEntry(c,t,scope,original.Entry.NodeId,ct);
        if(!matched||entry.SourceNodeId!=original.Entry.SourceNodeId||entry.Created!=original.Entry.Created)
        {
            // A later remote/local change cannot erase a historical publication.
            // Preserve current B and expose the newer disagreement as a conflict.
            await using var q=SyncCommand(c,t,"UPDATE local_cycle_files SET phase=2 WHERE scope_id=$scope AND node_id=$node;UPDATE client_namespace_operations SET state='PUBLISHED',receipt_json=$receipt,error='Historical namespace publication differs from the current cut.' WHERE scope_id=$scope AND operation_id=$op");
            CycleId(q,scopeId);q.Parameters.AddWithValue("$node",entry.NodeId);q.Parameters.AddWithValue("$op",original.Request.OperationId);q.Parameters.AddWithValue("$receipt",receipt.RawJson);await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);return;
        }
        if(original.Request.Kind=="move")
        {
            var baseline=JsonSerializer.Deserialize<LocalCycleBaseline>(entry.BaselineJson!,LocalCycleWire.Json)!;
            var unchanged=entry.Generation==original.Entry.Generation&&await NamespaceBytesUnchanged(c,t,scope,entry,baseline,ct);
            var updated=baseline with{Description=receipt.Description,RemoteSequence=receipt.LastSequence!.Value,Generation=unchanged?entry.Generation:baseline.Generation};
            await using var q=SyncCommand(c,t,"UPDATE local_cycle_files SET parent_id=$parent,path=$path,name=$name,baseline=$baseline,phase=$phase,ack_journal=CASE WHEN $phase=4 THEN $journal ELSE ack_journal END,ack_watcher=CASE WHEN $phase=4 THEN $watcher ELSE ack_watcher END WHERE scope_id=$scope AND node_id=$node");CycleId(q,scopeId);q.Parameters.AddWithValue("$node",entry.NodeId);q.Parameters.AddWithValue("$parent",original.Entry.ParentId);q.Parameters.AddWithValue("$path",original.Entry.RelativePath);q.Parameters.AddWithValue("$name",original.Entry.RawName);q.Parameters.AddWithValue("$baseline",JsonSerializer.Serialize(updated,LocalCycleWire.Json));q.Parameters.AddWithValue("$phase",unchanged?4:0);q.Parameters.AddWithValue("$journal",original.Entry.Generation.Journal);q.Parameters.AddWithValue("$watcher",original.Entry.Generation.Watcher);await q.ExecuteNonQueryAsync(ct);
        }
        else if(original.Request.Kind=="quarantine-delete")
        {
            // The original entry_json retains the old baseline and receipt.
            // Remove it only from current confirmed totals/current B.
            var previous=JsonSerializer.Deserialize<LocalCycleBaseline>(entry.BaselineJson!,LocalCycleWire.Json)!;
            await using var q=SyncCommand(c,t,"UPDATE local_cycle_files SET phase=$phase,remote_node=NULL,baseline=NULL,baseline_work=0,ack_journal=$journal,ack_watcher=$watcher WHERE scope_id=$scope AND node_id=$node;UPDATE local_cycle_scopes SET confirmed_files=confirmed_files-$files,confirmed_bytes=confirmed_bytes-$bytes WHERE scope_id=$scope AND confirmed_files>=$files AND confirmed_bytes>=$bytes");CycleId(q,scopeId);q.Parameters.AddWithValue("$node",entry.NodeId);q.Parameters.AddWithValue("$phase",entry.Generation==original.Entry.Generation?4:0);q.Parameters.AddWithValue("$journal",original.Entry.Generation.Journal);q.Parameters.AddWithValue("$watcher",original.Entry.Generation.Watcher);q.Parameters.AddWithValue("$files",entry.Directory?0:1);q.Parameters.AddWithValue("$bytes",entry.Directory?0:LocalCycleWire.Decimal(previous.Description.SizeBytes));if(await q.ExecuteNonQueryAsync(ct)!=2)throw new InvalidDataException("Quarantine baseline totals lost consistency.");
        }
        await using(var q=SyncCommand(c,t,"DELETE FROM local_parent_reconcile_conflicts WHERE scope_id=$scope AND node_id=$node;UPDATE client_namespace_operations SET state='PUBLISHED',receipt_json=$receipt WHERE operation_id=$op AND scope_id=$scope")){CycleId(q,scopeId);q.Parameters.AddWithValue("$node",entry.NodeId);q.Parameters.AddWithValue("$op",original.Request.OperationId);q.Parameters.AddWithValue("$receipt",receipt.RawJson);await q.ExecuteNonQueryAsync(ct);}await t.CommitAsync(ct);
    }
    internal async Task<NamespaceDeletePreview> PreviewOwnedDeletes(string root,string owner,string scope,CancellationToken ct)
    {
        var rows=(await ReadOwnedNamespace(root,owner,scope,ct)).Where(r=>r.Request.Kind=="quarantine-delete"&&r.State=="QUEUED").ToArray();
        var digest=Convert.ToHexStringLower(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(rows.Select(r=>new{r.Request,r.Entry}),LocalCycleWire.Json)));
        var failures=new List<NamespaceOperationPreview>();await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        await using(var q=SyncCommand(c,t,"SELECT operation_id,kind,json_extract(entry_json,'$.relativePath'),state,approved,error FROM client_namespace_operations WHERE scope_id=$scope AND error IS NOT NULL ORDER BY created_at DESC LIMIT 120")){CycleId(q,scope);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))failures.Add(new(r.GetString(0),r.GetString(1),r.GetString(2),r.GetString(3),r.GetInt32(4)!=0,r.GetString(5)));}
        return new(digest,rows.Select(r=>new NamespaceOperationPreview(r.Request.OperationId,r.Request.Kind,r.Entry.RelativePath,r.State,r.Approved,null)).ToArray()){Failures=failures};
    }
    internal async Task ApproveOwnedDeletes(string root,string owner,string scope,string digest,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scope,500,ct);
        var preview=await PreviewOwnedDeletes(root,owner,scope,ct);if(preview.Digest!=digest||preview.Operations.Count==0)throw new InvalidOperationException("Delete preview changed or is empty.");
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,false,ct);
        var plan=Guid.NewGuid().ToString("D");var items=new List<SyncDeletePlanItem>();SyncNamespacePolicy? authority=null;
        foreach(var operation in preview.Operations)
        {
            await using var read=SyncCommand(c,t,"SELECT request_json FROM client_namespace_operations WHERE operation_id=$op AND scope_id=$scope AND state='QUEUED'");CycleId(read,scope);read.Parameters.AddWithValue("$op",operation.OperationId);
            var original=JsonSerializer.Deserialize<SyncMutationRequest>((string)(await read.ExecuteScalarAsync(ct)??throw new InvalidOperationException("Delete plan changed.")),LocalCycleWire.Json)!;
            authority??=original.Namespace;if(authority!=original.Namespace)throw new InvalidDataException("Delete preview mixes namespace policies.");
            var expected=original.Input.GetProperty("expected");items.Add(new(expected.GetProperty("node_id").GetString()!,expected.GetProperty("version").GetString()!));
            var fields=JsonSerializer.Deserialize<Dictionary<string,JsonElement>>(original.Input.GetRawText())!;fields["planId"]=JsonSerializer.SerializeToElement(plan);fields["planDigest"]=JsonSerializer.SerializeToElement(digest);original=original with{Input=JsonSerializer.SerializeToElement(fields)};
            await using var update=SyncCommand(c,t,"UPDATE client_namespace_operations SET approved=1,plan_id=$plan,plan_digest=$digest,request_json=$request WHERE operation_id=$op AND state='QUEUED'");update.Parameters.AddWithValue("$op",operation.OperationId);update.Parameters.AddWithValue("$plan",plan);update.Parameters.AddWithValue("$digest",digest);update.Parameters.AddWithValue("$request",JsonSerializer.Serialize(original,LocalCycleWire.Json));await update.ExecuteNonQueryAsync(ct);
        }
        await using(var q=SyncCommand(c,t,"INSERT INTO client_delete_plans VALUES($plan,$scope,$json,NULL)"))
        {CycleId(q,scope);q.Parameters.AddWithValue("$plan",plan);q.Parameters.AddWithValue("$json",JsonSerializer.Serialize(new SyncDeletePlan(plan,digest,items,authority!),LocalCycleWire.Json));await q.ExecuteNonQueryAsync(ct);}
        await t.CommitAsync(ct);
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<int> PlanNamespaceAsync(string scope,SyncNamespacePolicy policy,CancellationToken ct)=>SerialAsync(()=>_store.PlanOwnedNamespace(_root,_owner,scope,policy,ct),ct);
    internal Task<IReadOnlyList<NamespaceOperation>> ReadNamespaceOperationsAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedNamespace(_root,_owner,scope,ct),ct);
    internal Task RecordNamespaceAsync(string scope,string op,string state,SyncMutationReceipt? receipt,CancellationToken ct)=>SerialAsync(async()=>{await _store.RecordOwnedNamespace(_root,_owner,scope,op,state,receipt,ct);return true;},ct);
    internal Task RecordNamespaceFailureAsync(string scope,string op,string code,bool notPublished,CancellationToken ct)=>SerialAsync(async()=>{await _store.RecordOwnedNamespaceFailure(_root,_owner,scope,op,code,notPublished,ct);return true;},ct);
    internal Task<bool> ValidateNamespaceAsync(string scope,NamespaceOperation op,CancellationToken ct)=>SerialAsync(()=>_store.ValidateOwnedNamespace(_root,_owner,scope,op,ct),ct);
    internal Task ApplyNamespaceReceiptAsync(string scope,NamespaceOperation original,SyncMutationReceipt receipt,CancellationToken ct)=>SerialAsync(async()=>{await _store.ApplyOwnedNamespaceReceipt(_root,_owner,scope,original,receipt,ct);return true;},ct);
    internal Task<NamespaceDeletePreview> PreviewDeletesAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.PreviewOwnedDeletes(_root,_owner,scope,ct),ct);
    internal Task ApproveDeletesAsync(string scope,string digest,CancellationToken ct)=>SerialAsync(async()=>{await _store.ApproveOwnedDeletes(_root,_owner,scope,digest,ct);return true;},ct);
}
