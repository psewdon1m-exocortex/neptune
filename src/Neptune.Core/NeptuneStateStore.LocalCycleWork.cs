using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private static SyncDescriptor HistoricalCycleDescription(LocalCycleWork w)
    {
        if(w.Phase==LocalCycleWorkPhase.AdoptedEqual)return JsonSerializer.Deserialize<SyncDescriptor>(w.ResultJson!,LocalCycleWire.Json)!;
        if(w.Entry.Directory){var original=JsonSerializer.Deserialize<CycleFolderInput>(w.InputJson!,LocalCycleWire.Json)!;return LocalCycleWire.Folder(System.Text.Encoding.UTF8.GetBytes(w.ResultJson!),original).Description;}
        using var result=SyncProtocolReader.Parse(Encoding.UTF8.GetBytes(w.ResultJson!));return SyncProtocolReader.Descriptor(result.RootElement.GetProperty("publishedDescription"));
    }
    internal async Task SaveOwnedCycleFolderInput(string root,string owner,string id,string operation,CycleFolderInput input,LocalJournalAdmission local,CancellationToken ct)
    {
        LocalCycleWire.FolderInput(input);var raw=JsonSerializer.Serialize(input,LocalCycleWire.Json);if(Encoding.UTF8.GetByteCount(raw)>32*1024)throw new InvalidDataException("Folder intent exceeds fixed bound.");await CycleAdmission(root,owner,id,local,4,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);var w=await ReadCycleWork(c,t,operation,ct);
        if(w.ScopeId!=id||!w.Entry.Directory||w.InputJson is not null||w.Phase!=LocalCycleWorkPhase.Queued||input.OperationId!=operation||input.Name!=w.Entry.RawName||input.RootId!=scope.Configuration.Binding.RootId||input.Epoch!=scope.Configuration.Binding.ServerEpoch||input.BindingVersion!=scope.Configuration.Binding.BindingVersion||input.ParentId!=await CycleTargetParent(c,t,scope,w.Entry,ct))throw new InvalidDataException("Original directory intent cannot rebind.");
        await using var q=SyncCommand(c,t,"UPDATE local_cycle_work SET input_json=$raw,bytes=bytes+$bytes WHERE operation_id=$operation AND input_json IS NULL");q.Parameters.AddWithValue("$raw",raw);q.Parameters.AddWithValue("$bytes",Encoding.UTF8.GetByteCount(raw));q.Parameters.AddWithValue("$operation",operation);await q.ExecuteNonQueryAsync(ct);await AddCycleBytes(c,t,id,Encoding.UTF8.GetByteCount(raw),ct);await CommitCycle(c,t,before,ct);
    }
    private const string CycleWorkColumns="operation_id,scope_id,entry_json,phase,spool_path,size,sha256,capture_proof,input_json,intent_hash,upload_id,received,result_json,error";
    private static LocalCycleWork CycleWork(SqliteDataReader r)
    {
        string? S(int i)=>r.IsDBNull(i)?null:r.GetString(i);
        var entry=JsonSerializer.Deserialize<LocalCycleEntry>(r.GetString(2),LocalCycleWire.Json)??throw new InvalidDataException("Original cycle entry is absent.");
        return new(r.GetString(0),r.GetString(1),entry,(LocalCycleWorkPhase)r.GetInt32(3),S(4),r.IsDBNull(5)?null:r.GetInt64(5),S(6),S(7),S(8),S(9),S(10),r.GetInt64(11),S(12),S(13));
    }
    private static async Task<LocalCycleWork> ReadCycleWork(SqliteConnection c,SqliteTransaction t,string operation,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,$"SELECT {CycleWorkColumns} FROM local_cycle_work WHERE operation_id=$operation");q.Parameters.AddWithValue("$operation",operation);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))throw new InvalidOperationException("Original cycle operation is absent.");return CycleWork(r);
    }
    private static async Task<LocalCycleEntry> CurrentCycleEntry(SqliteConnection c,SqliteTransaction t,LocalCycleScope scope,string node,CancellationToken ct)
    {
        var incarnations=await HasIncarnations(c,t,ct);var native=incarnations?"COALESCE(i.native_id,f.node_id)":"f.node_id";
        await using var q=SyncCommand(c,t,"SELECT f.node_id,f.parent_id,f.path,f.name,f.created,f.kind,f.size,COALESCE(d.generation,0),f.watcher_generation,f.baseline,f.remote_node,"+native+" FROM local_cycle_files f "+(incarnations?"LEFT JOIN local_parent_reconcile_incarnations i ON i.scope_id=f.scope_id AND i.node_id=f.node_id ":"")+"LEFT JOIN local_coherence_dirty d ON d.coherence_id=$coherence AND d.node_id="+native+" WHERE f.scope_id=$scope AND f.node_id=$node");CycleId(q,scope.ScopeId);q.Parameters.AddWithValue("$coherence",(object?)scope.CoherenceId??DBNull.Value);q.Parameters.AddWithValue("$node",node);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))throw new InvalidOperationException("Original cycle file disappeared.");
        return new(r.GetString(0),r.GetString(1),r.GetString(2),r.GetString(3),r.GetInt64(4),r.GetInt32(5)!=0,r.GetInt64(6),new(r.GetInt64(7),r.GetInt64(8)),r.IsDBNull(9)?null:r.GetString(9),r.IsDBNull(10)?null:r.GetString(10)){NativeNodeId=r.GetString(11)==r.GetString(0)?null:r.GetString(11)};
    }
    internal async Task<int> ImportOwnedCycleDirty(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,id,local,253,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);
        if(scope.CoherenceId is null||!scope.SeedComplete)throw new InvalidOperationException("Import dirty only after all validated initial entries were indexed.");var coherence=await RequireCoherenceAsync(c,t,root,owner,scope.CoherenceId,false,ct);
        if(coherence.State.Status!=LocalCoherenceStatus.Anchored)throw new LocalSourceGapException("CycleNamespaceUncertain","Scoped ambiguity must reconcile before byte decisions.");
        long after;string node;await using(var q=SyncCommand(c,t,"SELECT local_after_usn,local_after_node FROM local_cycle_scopes WHERE scope_id=$scope")){CycleId(q,id);await using var r=await q.ExecuteReaderAsync(ct);await r.ReadAsync(ct);after=r.GetInt64(0);node=r.GetString(1);}
        var rows=new List<(string Node,long Usn,long Generation)>();
        await using(var q=SyncCommand(c,t,"SELECT node_id,last_usn,generation FROM local_coherence_dirty WHERE coherence_id=$coherence AND (last_usn>$after OR last_usn=$after AND node_id>$node) AND last_usn<$cut ORDER BY last_usn,node_id LIMIT 250")){q.Parameters.AddWithValue("$coherence",scope.CoherenceId);q.Parameters.AddWithValue("$after",after);q.Parameters.AddWithValue("$node",node);q.Parameters.AddWithValue("$cut",coherence.State.AppliedUsn);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))rows.Add((r.GetString(0),r.GetInt64(1),r.GetInt64(2)));}
        var incarnations=await HasIncarnations(c,t,ct);
        foreach(var row in rows)
        {await using var q=SyncCommand(c,t,"UPDATE local_cycle_files SET phase=CASE WHEN phase=1 THEN 1 ELSE 0 END WHERE scope_id=$scope AND "+(incarnations?"COALESCE((SELECT i.native_id FROM local_parent_reconcile_incarnations i WHERE i.scope_id=local_cycle_files.scope_id AND i.node_id=local_cycle_files.node_id),node_id)":"node_id")+"=$node AND ack_journal<$generation");CycleId(q,id);q.Parameters.AddWithValue("$node",row.Node);q.Parameters.AddWithValue("$generation",row.Generation);await q.ExecuteNonQueryAsync(ct);}
        if(rows.Count>0){await using var q=SyncCommand(c,t,"UPDATE local_cycle_scopes SET local_after_usn=$after,local_after_node=$node WHERE scope_id=$scope");CycleId(q,id);q.Parameters.AddWithValue("$after",rows[^1].Usn);q.Parameters.AddWithValue("$node",rows[^1].Node);await q.ExecuteNonQueryAsync(ct);}await CommitCycle(c,t,before,ct);return rows.Count;
    }
    private static string CycleClaimCandidateSql(bool directoriesOnly,bool hasEvents)
    {
        var quiet=hasEvents?" AND NOT EXISTS(SELECT 1 FROM client_event_inbox e WHERE e.root_key=$eventRoot AND e.path=f.path AND e.due_at>$now)":"";
        return "SELECT f.node_id FROM local_cycle_files f INDEXED BY local_cycle_files_page WHERE f.scope_id=$scope AND f.phase=0"+(directoriesOnly?" AND f.kind=1":"")+quiet+" AND NOT EXISTS(SELECT 1 FROM json_each($excludes) e WHERE neptune_path_within(f.path,e.value)) AND (f.parent_id=$root OR EXISTS(SELECT 1 FROM local_cycle_files p WHERE p.scope_id=f.scope_id AND p.node_id=f.parent_id AND p.kind=1 AND p.remote_node IS NOT NULL AND p.phase<>2)) AND NOT EXISTS(SELECT 1 FROM local_cycle_refresh_conflicts x JOIN local_cycle_refreshes r ON r.scope_id=x.scope_id JOIN local_cycle_scopes s ON s.active_remote=x.scope_id WHERE s.scope_id=f.scope_id AND x.pass=r.reconcile_pass AND x.node_id IN(f.node_id,f.parent_id)) AND NOT EXISTS(SELECT 1 FROM local_cycle_work w WHERE w.scope_id=f.scope_id AND w.node_id=f.node_id AND w.active=1) ORDER BY f.node_id LIMIT 1";
    }
    private string? _directoryPriorityExhaustedScope;
    internal async Task<LocalCycleWork?> ClaimOwnedCycleWork(string root,string owner,string id,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,id,local,8,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);
        var result=await ClaimCycleWork(c,t,scope,root,owner,ct);
        if(result is not null)await CommitCycle(c,t,before,ct);else await t.CommitAsync(ct);
        return result;
    }
    private async Task<LocalCycleWork?> ClaimCycleWork(SqliteConnection c,SqliteTransaction t,LocalCycleScope scope,string root,string owner,CancellationToken ct)
    {
        var id=scope.ScopeId;
        await using(var pending=SyncCommand(c,t,"SELECT 1 FROM local_cycle_scopes WHERE scope_id=$scope AND pending_remote IS NOT NULL")){CycleId(pending,id);if(await pending.ExecuteScalarAsync(ct)is not null)return null;}
        if(scope.CoherenceId is null)return null;var coherence=await RequireCoherenceAsync(c,t,root,owner,scope.CoherenceId,false,ct);
        if(coherence.State.Status is not (LocalCoherenceStatus.Building or LocalCoherenceStatus.Anchored))return null;var ns=await ReadNamespaceAsync(c,t,coherence.State.CandidateId,ct);
        var mappingPolicy=await ReadMappingPolicy(c,t,id,ct);string? node;bool hasEvents;
        await using(var exists=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='client_event_inbox'"))hasEvents=await exists.ExecuteScalarAsync(ct)is not null;
        async Task<string?> Candidate(bool directoriesOnly)
        {
        await using(var q=SyncCommand(c,t,CycleClaimCandidateSql(directoriesOnly,hasEvents)))
        {CycleId(q,id);q.Parameters.AddWithValue("$root",ns.State.RootFileId128);q.Parameters.AddWithValue("$excludes",JsonSerializer.Serialize(mappingPolicy.ExcludedPrefixes));if(hasEvents){q.Parameters.AddWithValue("$eventRoot",root);q.Parameters.AddWithValue("$now",Clock.GetUtcNow().ToUnixTimeMilliseconds());}return await q.ExecuteScalarAsync(ct)as string;}
        }
        node=null;
        // Parent-first admission avoids repeatedly visiting hundreds of thousands
        // of children whose remote parents are not published yet. The negative
        // hint changes ordering only; the ordinary query still considers all work.
        if(Volatile.Read(ref _directoryPriorityExhaustedScope)!=id)
        {
            node=await Candidate(true);
            if(node is null&&scope.SeedComplete)Volatile.Write(ref _directoryPriorityExhaustedScope,id);
        }
        node??=await Candidate(false);
        if(node is null)return null;var entry=await CurrentCycleEntry(c,t,scope,node,ct);var operation=Guid.NewGuid().ToString("D");var json=JsonSerializer.Serialize(entry,LocalCycleWire.Json);var bytes=checked(512+Encoding.UTF8.GetByteCount(json));
        await using(var q=SyncCommand(c,t,"INSERT INTO local_cycle_work(operation_id,scope_id,node_id,journal_generation,watcher_generation,entry_json,phase,active,bytes) VALUES($operation,$scope,$node,$journal,$watcher,$entry,0,1,$bytes);UPDATE local_cycle_files SET phase=1 WHERE scope_id=$scope AND node_id=$node AND phase=0")){CycleId(q,id);q.Parameters.AddWithValue("$operation",operation);q.Parameters.AddWithValue("$node",node);q.Parameters.AddWithValue("$journal",entry.Generation.Journal);q.Parameters.AddWithValue("$watcher",entry.Generation.Watcher);q.Parameters.AddWithValue("$entry",json);q.Parameters.AddWithValue("$bytes",bytes);if(await q.ExecuteNonQueryAsync(ct)!=2)throw new InvalidOperationException("Cycle work raced its file generation.");}
        await AddCycleBytes(c,t,id,bytes,ct);return await ReadCycleWork(c,t,operation,ct);
    }
    internal async Task<LocalCycleWork> ReadOwnedCycleWork(string root,string owner,string id,string operation,CancellationToken ct)
    {await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,id,true,ct);var work=await ReadCycleWork(c,t,operation,ct);if(work.ScopeId!=id)throw new InvalidDataException("Cycle operation belongs to another scope.");return work;}
    internal async Task SealOwnedCycle(string root,string owner,string id,string operation,LocalCycleSeal seal,LocalNamespaceRootProof proof,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,id,local,5,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireCycle(c,t,root,owner,id,false,ct);var w=await ReadCycleWork(c,t,operation,ct);
        _=await SealCycleWork(c,t,id,w,seal,proof,ct);await CommitCycle(c,t,before,ct);
    }
    private async Task<LocalCycleWork> SealCycleWork(SqliteConnection c,SqliteTransaction t,string id,LocalCycleWork w,LocalCycleSeal seal,LocalNamespaceRootProof proof,CancellationToken ct)
    {
        var operation=w.OperationId;
        if(w.ScopeId!=id||w.Phase!=LocalCycleWorkPhase.Queued||w.InputJson is not null||seal.Size<0)throw new InvalidOperationException("A new seal cannot replace attempted work.");WindowsNativeCycleCapture.ValidatePersisted(w,seal.Size,seal.Sha256,seal.ProofJson,proof);
        await using var q=SyncCommand(c,t,"UPDATE local_cycle_work SET phase=1,spool_path=$path,size=$size,sha256=$hash,capture_proof=$proof,bytes=bytes+$bytes WHERE operation_id=$operation AND phase=0 AND input_json IS NULL");q.Parameters.AddWithValue("$operation",operation);q.Parameters.AddWithValue("$path",seal.SpoolPath);q.Parameters.AddWithValue("$size",seal.Size);q.Parameters.AddWithValue("$hash",seal.Sha256);q.Parameters.AddWithValue("$proof",seal.ProofJson);var bytes=Encoding.UTF8.GetByteCount(seal.ProofJson)+seal.SpoolPath.Length+128;q.Parameters.AddWithValue("$bytes",bytes);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("Original queued seal state changed.");await AddCycleBytes(c,t,id,bytes,ct);
        return w with{Phase=LocalCycleWorkPhase.Sealed,SpoolPath=seal.SpoolPath,Size=seal.Size,Sha256=seal.Sha256,CaptureProof=seal.ProofJson};
    }
    internal async Task SaveOwnedCycleInput(string root,string owner,string id,string operation,CycleUploadInput input,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,id,local,5,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);var w=await ReadCycleWork(c,t,operation,ct);
        _=await SaveCycleInput(c,t,scope,w,input,ct);await CommitCycle(c,t,before,ct);
    }
    private async Task<LocalCycleWork> SaveCycleInput(SqliteConnection c,SqliteTransaction t,LocalCycleScope scope,LocalCycleWork w,CycleUploadInput input,CancellationToken ct)
    {
        var hash=LocalCycleWire.IntentHash(input);var json=JsonSerializer.Serialize(input,LocalCycleWire.Json);if(Encoding.UTF8.GetByteCount(json)>32*1024)throw new InvalidDataException("Original catalog intent exceeds fixed limit.");
        var id=scope.ScopeId;var operation=w.OperationId;var i=input.Intent;
        if(w.ScopeId!=id||w.Phase!=LocalCycleWorkPhase.Sealed||w.CaptureProof is null||w.InputJson is not null||i.SizeBytes!=w.Size?.ToString(System.Globalization.CultureInfo.InvariantCulture)||i.Sha256!=w.Sha256
            ||i.IdempotencyKey!="neptune-cycle-"+operation||i.Name!=w.Entry.RawName||i.RootId!=scope.Configuration.Binding.RootId||i.DeviceId!=scope.Configuration.DeviceId||input.Epoch!=scope.Configuration.Binding.ServerEpoch||input.BindingVersion!=scope.Configuration.Binding.BindingVersion||input.BackendIdentity!=scope.Configuration.BackendIdentity)
            throw new InvalidDataException("Catalog intent cannot rebind original native work/bytes/enrollment.");
        await using var q=SyncCommand(c,t,"UPDATE local_cycle_work SET input_json=$json,intent_hash=$hash,bytes=bytes+$bytes WHERE operation_id=$operation AND phase=1 AND input_json IS NULL");q.Parameters.AddWithValue("$operation",operation);q.Parameters.AddWithValue("$json",json);q.Parameters.AddWithValue("$hash",hash);q.Parameters.AddWithValue("$bytes",Encoding.UTF8.GetByteCount(json));if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("Original sealed input state changed.");await AddCycleBytes(c,t,id,Encoding.UTF8.GetByteCount(json),ct);
        return w with{InputJson=json,IntentHash=hash};
    }
    internal async Task<LocalCycleWork?> TrySealOwnedCycleInput(string root,string owner,string id,string operation,LocalCycleSeal seal,LocalNamespaceRootProof proof,CycleUploadInput input,LocalJournalAdmission local,CancellationToken ct)
    {
        if(seal.Size is <1 or >SmallFileBatchPlan.MaximumFileBytes||!input.Intent.RequireAbsent)throw new ArgumentException("Combined seal requires a new nonempty small file.");
        await CycleAdmission(root,owner,id,local,10,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);
        var scope=await RequireCycle(c,t,root,owner,id,false,ct);var work=await ReadCycleWork(c,t,operation,ct);
        if(work.Entry.Directory||work.Entry.BaselineJson is not null)throw new InvalidOperationException("Combined seal cannot replace historical work.");
        await using(var existing=SyncCommand(c,t,"SELECT 1 FROM client_stream_operations WHERE operation_id=$operation UNION ALL SELECT 1 FROM client_namespace_operations WHERE operation_id=$operation UNION ALL SELECT 1 FROM client_metadata_seals WHERE operation_id=$operation LIMIT 1"))
        {existing.Parameters.AddWithValue("$operation",operation);if(await existing.ExecuteScalarAsync(ct)is not null)return null;}
        work=await SealCycleWork(c,t,id,work,seal,proof,ct);
        work=await SaveCycleInput(c,t,scope,work,input,ct);
        await CommitCycle(c,t,before,ct);return work;
    }
    internal async Task MarkOwnedCycleSending(string root,string owner,string id,string operation,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,id,local,2,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireCycle(c,t,root,owner,id,false,ct);var w=await ReadCycleWork(c,t,operation,ct);
        if(w.ScopeId!=id||w.InputJson is null||w.IntentHash is null||w.Phase is not(LocalCycleWorkPhase.Sealed or LocalCycleWorkPhase.Prepared or LocalCycleWorkPhase.OutcomeUnknown or LocalCycleWorkPhase.Sending))throw new InvalidOperationException("Durable sealed intent is required before transport I/O.");
        await using var q=SyncCommand(c,t,"UPDATE local_cycle_work SET phase=3,error=NULL WHERE operation_id=$operation AND active=1");q.Parameters.AddWithValue("$operation",operation);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("Transport work is not active.");await CommitCycle(c,t,before,ct);
    }
    internal async Task SaveOwnedCycleStatus(string root,string owner,string id,string operation,CycleUploadStatus status,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,id,local,5,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireCycle(c,t,root,owner,id,false,ct);var w=await ReadCycleWork(c,t,operation,ct);
        await ApplyCycleStatus(c,t,id,w,status,ct);await CommitCycle(c,t,before,ct);
    }
    private static async Task ApplyCycleStatus(SqliteConnection c,SqliteTransaction t,string id,LocalCycleWork w,CycleUploadStatus status,CancellationToken ct)
    {
        var operation=w.OperationId;
        if(w.ScopeId!=id||w.InputJson is null||w.IntentHash is null||w.Phase is not(LocalCycleWorkPhase.Sending or LocalCycleWorkPhase.Prepared or LocalCycleWorkPhase.OutcomeUnknown))throw new InvalidOperationException("Only an attempted original intent accepts a response.");var input=JsonSerializer.Deserialize<CycleUploadInput>(w.InputJson,LocalCycleWire.Json)!;
        using var result=status.ResultJson is null?null:JsonDocument.Parse(status.ResultJson);
        object body=status.ResultJson is null?new { session=new {uploadId=status.UploadId,operationId=status.OperationId,rootId=status.RootId,intentHash=status.IntentHash,expectedSizeBytes=status.ExpectedBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),receivedSizeBytes=status.ReceivedBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),phase=status.Phase,transferPending=status.TransferPending},replayed=status.Replayed }:
            new { session=new {uploadId=status.UploadId,operationId=status.OperationId,rootId=status.RootId,intentHash=status.IntentHash,expectedSizeBytes=status.ExpectedBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),receivedSizeBytes=status.ReceivedBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),phase=status.Phase,transferPending=status.TransferPending},result=result!.RootElement,replayed=status.Replayed };
        var validated=LocalCycleWire.Status(JsonSerializer.SerializeToUtf8Bytes(body),input,w.IntentHash,w.UploadId);
        if(validated.ReceivedBytes<w.ReceivedBytes)throw new InvalidDataException("Original transport offset regressed.");
        var phase=validated.Phase=="published"?LocalCycleWorkPhase.Published:validated.Phase=="not_published"?LocalCycleWorkPhase.Conflict:LocalCycleWorkPhase.Prepared;
        await using var q=SyncCommand(c,t,"UPDATE local_cycle_work SET phase=$phase,upload_id=$upload,received=$received,result_json=$result,error=NULL WHERE operation_id=$operation AND active=1");q.Parameters.AddWithValue("$operation",operation);q.Parameters.AddWithValue("$phase",(int)phase);q.Parameters.AddWithValue("$upload",validated.UploadId);q.Parameters.AddWithValue("$received",validated.ReceivedBytes);q.Parameters.AddWithValue("$result",(object?)validated.ResultJson??DBNull.Value);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("Durable response raced original operation.");
        if(phase==LocalCycleWorkPhase.Conflict)
        {await using var terminal=SyncCommand(c,t,"UPDATE local_cycle_work SET active=NULL WHERE operation_id=$operation;UPDATE local_cycle_files SET phase=2 WHERE scope_id=$scope AND node_id=$node");terminal.Parameters.AddWithValue("$operation",operation);CycleId(terminal,id);terminal.Parameters.AddWithValue("$node",w.Entry.NodeId);await terminal.ExecuteNonQueryAsync(ct);}
    }
    internal async Task FinishOwnedCycleWork(string root,string owner,string id,string operation,LocalCycleWorkPhase phase,string? result,string? error,LocalJournalAdmission local,CancellationToken ct)
    {
        if(phase is not(LocalCycleWorkPhase.Published or LocalCycleWorkPhase.AdoptedEqual or LocalCycleWorkPhase.Conflict or LocalCycleWorkPhase.Deferred or LocalCycleWorkPhase.Failed or LocalCycleWorkPhase.OutcomeUnknown)||error?.Length>1024||result?.Length>40*1024)throw new ArgumentException("Invalid cycle outcome.");await CycleAdmission(root,owner,id,local,4,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireCycle(c,t,root,owner,id,true,ct);var w=await ReadCycleWork(c,t,operation,ct);
        if(w.ScopeId!=id||w.Phase is LocalCycleWorkPhase.Published or LocalCycleWorkPhase.AdoptedEqual)throw new InvalidOperationException("A committed publication/equality outcome cannot be downgraded.");
        if(phase==LocalCycleWorkPhase.Published&&!w.Entry.Directory||phase==LocalCycleWorkPhase.AdoptedEqual&&!w.Entry.Directory&&(w.CaptureProof is null||w.Size is null||w.Sha256 is null))throw new InvalidOperationException("Unqualified outcome kind.");
        if(phase==LocalCycleWorkPhase.AdoptedEqual&&w.Entry.Directory)
        {
            var description=JsonSerializer.Deserialize<SyncDescriptor>(result!,LocalCycleWire.Json)??throw new InvalidDataException("Current directory description missing.");
            var baseline=w.Entry.BaselineJson is null?null:JsonSerializer.Deserialize<LocalCycleBaseline>(w.Entry.BaselineJson,LocalCycleWire.Json);
            var owned=baseline?.Description is {Type:"folder"} prior&&prior.NodeId==description.NodeId&&prior.ParentId==description.ParentId&&prior.Name==description.Name&&description.Type=="folder"&&description.Availability=="AVAILABLE";
            if(!owned&&!await HasReplacementDirectoryPublication(root,owner,id,w.Entry,description,ct))throw new InvalidDataException("Directory adoption requires a retained baseline or installed explicit lineage/current topology.");
        }
        string? remote=null;if(phase is LocalCycleWorkPhase.Published or LocalCycleWorkPhase.AdoptedEqual)
        {var d=w.Entry.Directory&&phase!=LocalCycleWorkPhase.AdoptedEqual ? LocalCycleWire.Folder(Encoding.UTF8.GetBytes(result??throw new InvalidDataException("Historical folder result missing.")),JsonSerializer.Deserialize<CycleFolderInput>(w.InputJson!,LocalCycleWire.Json)!).Description : JsonSerializer.Deserialize<SyncDescriptor>(result??throw new InvalidDataException("Historical descriptor missing."),LocalCycleWire.Json)??throw new InvalidDataException("Historical descriptor missing.");if(d.Type!=(w.Entry.Directory?"folder":"file")||d.Name!=w.Entry.RawName||!w.Entry.Directory&&(d.ContentSha256!=w.Sha256||d.SizeBytes!=w.Size?.ToString(System.Globalization.CultureInfo.InvariantCulture)||d.Availability!="AVAILABLE"))throw new InvalidDataException("Equality/directory evidence cannot rebind bytes/type.");remote=d.NodeId;}
        await using(var q=SyncCommand(c,t,"UPDATE local_cycle_work SET phase=$phase,result_json=$result,error=$error,active=CASE WHEN $terminal=1 THEN NULL ELSE active END WHERE operation_id=$operation AND active=1")){q.Parameters.AddWithValue("$operation",operation);q.Parameters.AddWithValue("$phase",(int)phase);q.Parameters.AddWithValue("$result",(object?)result??DBNull.Value);q.Parameters.AddWithValue("$error",(object?)error??DBNull.Value);q.Parameters.AddWithValue("$terminal",phase is LocalCycleWorkPhase.Conflict or LocalCycleWorkPhase.Failed?1:0);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("Work is no longer active.");}
        await using(var q=SyncCommand(c,t,"UPDATE local_cycle_files SET phase=$phase,remote_node=COALESCE($remote,remote_node) WHERE scope_id=$scope AND node_id=$node")){CycleId(q,id);q.Parameters.AddWithValue("$node",w.Entry.NodeId);q.Parameters.AddWithValue("$phase",phase is LocalCycleWorkPhase.Conflict or LocalCycleWorkPhase.Failed?2:phase==LocalCycleWorkPhase.Deferred?3:1);q.Parameters.AddWithValue("$remote",(object?)remote??DBNull.Value);await q.ExecuteNonQueryAsync(ct);}await CommitCycle(c,t,before,ct);
    }
    internal async Task<IReadOnlyList<LocalCycleWork>> ReadOwnedCycleActiveWork(string root,string owner,string id,bool acknowledgements,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,id,true,ct);await using var q=SyncCommand(c,t,$"SELECT {CycleWorkColumns} FROM local_cycle_work WHERE scope_id=$scope AND active=1 AND phase {(acknowledgements?"IN(5,6)":"IN(0,1,2,3,4)")} ORDER BY work_no LIMIT 32");CycleId(q,id);var result=new List<LocalCycleWork>();await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))result.Add(CycleWork(r));return result.AsReadOnly();
    }
    internal async Task<bool> AcknowledgeOwnedCycle(string root,string owner,string id,string operation,LocalNamespaceRootProof native,LocalJournalAdmission local,CancellationToken ct)
        =>(await AcknowledgeOwnedCycleWorkset(root,owner,id,[operation],native,local,ct))[0];
    internal async Task<IReadOnlyList<bool>> AcknowledgeOwnedCycleWorkset(string root,string owner,string id,IReadOnlyList<string> operations,LocalNamespaceRootProof native,LocalJournalAdmission local,CancellationToken ct)
    {
        if(operations.Count is <1 or >32||operations.Distinct(StringComparer.Ordinal).Count()!=operations.Count)throw new ArgumentException("Invalid bounded acknowledgement set.");
        await CycleAdmission(root,owner,id,local,5*operations.Count,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);var scope=await RequireCycle(c,t,root,owner,id,false,ct);
        var progress=await CycleProgress(c,t,id,ct);
        if(scope.CoherenceId is null||!scope.SeedComplete||!progress.Complete)throw new InvalidOperationException("Historical agreement requires complete applied local/remote boundaries.");var coherence=await RequireCoherenceAsync(c,t,root,owner,scope.CoherenceId,false,ct);
        var localState=await ReadLocalStateAsync(c,t,root,owner,ct);if(coherence.State.Status!=LocalCoherenceStatus.Anchored||coherence.State.AppliedUsn<localState.Cursor.NextReadUsn)throw new InvalidOperationException("New local evidence must be applied before generation acknowledgement.");
        var results=new List<bool>(operations.Count);
        foreach(var operation in operations)results.Add(await AcknowledgeCycleWork(c,t,scope,operation,native,progress.Sequence,ct));
        await CommitCycle(c,t,before,ct);return results;
    }
    private async Task<bool> AcknowledgeCycleWork(SqliteConnection c,SqliteTransaction t,LocalCycleScope scope,string operation,LocalNamespaceRootProof native,long sequence,CancellationToken ct)
    {
        var id=scope.ScopeId;
        var w=await ReadCycleWork(c,t,operation,ct);if(w.ScopeId!=id||w.Phase is not(LocalCycleWorkPhase.Published or LocalCycleWorkPhase.AdoptedEqual))throw new InvalidOperationException("Only a committed historical outcome may agree.");
        var current=await CurrentCycleEntry(c,t,scope,w.Entry.NodeId,ct);SyncDescriptor historical;
        if(w.Entry.Directory||w.Phase==LocalCycleWorkPhase.AdoptedEqual)historical=HistoricalCycleDescription(w);
        else
        {
            using var result=SyncProtocolReader.Parse(Encoding.UTF8.GetBytes(w.ResultJson!));historical=SyncProtocolReader.Descriptor(result.RootElement.GetProperty("publishedDescription"));
            if(w.CaptureProof is null||w.Size is null||w.Sha256 is null)throw new InvalidDataException("Original native seal missing.");WindowsNativeCycleCapture.ValidatePersisted(w,w.Size.Value,w.Sha256,w.CaptureProof,native);
        }
        if(!w.Entry.Directory)WindowsNativeCycleCapture.ValidatePersisted(w,w.Size!.Value,w.Sha256!,w.CaptureProof!,native);
        var remote=await CycleRemoteNode(c,t,id,historical.NodeId,ct);var matched=w.Entry.Directory ? remote is not null && remote.NodeId==historical.NodeId && remote.ParentId==historical.ParentId && remote.Name==historical.Name && remote.Type=="folder" && remote.Availability=="AVAILABLE" : remote==historical;long number;await using(var q=SyncCommand(c,t,"SELECT work_no FROM local_cycle_work WHERE operation_id=$operation")){q.Parameters.AddWithValue("$operation",operation);number=Convert.ToInt64(await q.ExecuteScalarAsync(ct));}
        var sameIncarnation=current.SourceNodeId==w.Entry.SourceNodeId&&current.Created==w.Entry.Created;
        var sameGeneration=sameIncarnation&&current.Generation==w.Entry.Generation;
        if(matched)
        {
            await using(var clear=SyncCommand(c,t,"DELETE FROM local_cycle_refresh_conflicts WHERE scope_id=(SELECT active_remote FROM local_cycle_scopes WHERE scope_id=$scope) AND node_id=$node")){CycleId(clear,id);clear.Parameters.AddWithValue("$node",w.Entry.NodeId);await clear.ExecuteNonQueryAsync(ct);}
            await PromoteCycleConfirmedTotals(c,t,id,current,historical,number,ct);
            var baseline=new LocalCycleBaseline(historical,w.CaptureProof??"directory identity only",w.Entry.Generation,sequence,number);var json=JsonSerializer.Serialize(baseline,LocalCycleWire.Json);
            await using var q=SyncCommand(c,t,"UPDATE local_cycle_files SET baseline=CASE WHEN baseline_work<=$number THEN $baseline ELSE baseline END,baseline_work=MAX(baseline_work,$number),remote_node=$remote,ack_journal=MAX(ack_journal,$journal),ack_watcher=MAX(ack_watcher,$watcher),phase=$phase WHERE scope_id=$scope AND node_id=$node");CycleId(q,id);q.Parameters.AddWithValue("$node",w.Entry.NodeId);q.Parameters.AddWithValue("$number",number);q.Parameters.AddWithValue("$baseline",json);q.Parameters.AddWithValue("$remote",historical.NodeId);q.Parameters.AddWithValue("$journal",sameIncarnation?w.Entry.Generation.Journal:-1);q.Parameters.AddWithValue("$watcher",w.Entry.Generation.Watcher);q.Parameters.AddWithValue("$phase",sameGeneration?4:0);await q.ExecuteNonQueryAsync(ct);
        }
        else{await using var q=SyncCommand(c,t,"UPDATE local_cycle_files SET phase=2 WHERE scope_id=$scope AND node_id=$node");CycleId(q,id);q.Parameters.AddWithValue("$node",w.Entry.NodeId);await q.ExecuteNonQueryAsync(ct);}
        await using(var q=SyncCommand(c,t,"UPDATE local_cycle_work SET active=NULL,error=CASE WHEN $matched=0 THEN 'Historical publication differs from current remote applied cut.' ELSE error END WHERE operation_id=$operation AND active=1")){q.Parameters.AddWithValue("$operation",operation);q.Parameters.AddWithValue("$matched",matched?1:0);await q.ExecuteNonQueryAsync(ct);}return matched&&sameGeneration;
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task SaveCycleFolderInputAsync(string id,string operation,CycleFolderInput input,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedCycleFolderInput(_root,_owner,id,operation,input,_admission,ct);return true;},ct);
    internal Task<int> ImportCycleDirtyAsync(string id,CancellationToken ct)=>SerialAsync(()=>_store.ImportOwnedCycleDirty(_root,_owner,id,_admission,ct),ct);
    internal Task<LocalCycleWork?> ClaimCycleWorkAsync(string id,CancellationToken ct)=>SerialAsync(()=>_store.ClaimOwnedCycleWork(_root,_owner,id,_admission,ct),ct);
    internal Task<LocalCycleWork> ReadCycleWorkAsync(string id,string operation,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedCycleWork(_root,_owner,id,operation,ct),ct);
    internal Task SealCycleWorkAsync(string id,string operation,LocalCycleSeal seal,LocalNamespaceRootProof proof,CancellationToken ct)=>SerialAsync(async()=>{await _store.SealOwnedCycle(_root,_owner,id,operation,seal,proof,_admission,ct);return true;},ct);
    internal Task<LocalCycleWork?> TrySealCycleInputAsync(string id,string operation,LocalCycleSeal seal,LocalNamespaceRootProof proof,CycleUploadInput input,CancellationToken ct)=>SerialAsync(()=>_store.TrySealOwnedCycleInput(_root,_owner,id,operation,seal,proof,input,_admission,ct),ct);
    internal Task SaveCycleInputAsync(string id,string operation,CycleUploadInput input,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedCycleInput(_root,_owner,id,operation,input,_admission,ct);return true;},ct);
    internal Task MarkCycleSendingAsync(string id,string operation,CancellationToken ct)=>SerialAsync(async()=>{await _store.MarkOwnedCycleSending(_root,_owner,id,operation,_admission,ct);return true;},ct);
    internal Task SaveCycleStatusAsync(string id,string operation,CycleUploadStatus status,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedCycleStatus(_root,_owner,id,operation,status,_admission,ct);return true;},ct);
    internal Task FinishCycleWorkAsync(string id,string operation,LocalCycleWorkPhase phase,string? result,string? error,CancellationToken ct)=>SerialAsync(async()=>{await _store.FinishOwnedCycleWork(_root,_owner,id,operation,phase,result,error,_admission,ct);return true;},ct);
    internal Task<IReadOnlyList<LocalCycleWork>> ReadCycleActiveWorkAsync(string id,bool acknowledgements,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedCycleActiveWork(_root,_owner,id,acknowledgements,ct),ct);
    internal Task<bool> AcknowledgeCycleAsync(string id,string operation,LocalNamespaceRootProof proof,CancellationToken ct)=>SerialAsync(()=>_store.AcknowledgeOwnedCycle(_root,_owner,id,operation,proof,_admission,ct),ct);
    internal Task<IReadOnlyList<bool>> AcknowledgeCycleWorksetAsync(string id,IReadOnlyList<string> operations,LocalNamespaceRootProof proof,CancellationToken ct)=>SerialAsync(()=>_store.AcknowledgeOwnedCycleWorkset(_root,_owner,id,operations,proof,_admission,ct),ct);
}
