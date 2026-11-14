using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

public sealed record WindowsCycleRecovery(string OriginalScopeId,string OperationId,string Status,
    string? PublicationResult,string? Error,string Baseline="NotAdopted",string Dirty="Preserved");
public sealed partial class NeptuneStateStore
{
    private static bool SameCyclePrincipal(WindowsCycleConfiguration a,WindowsCycleConfiguration b)=>
        a.MappingId==b.MappingId&&a.ClientId==b.ClientId&&a.ProfileId==b.ProfileId&&a.RulesVersion==b.RulesVersion&&a.ServerOrigin==b.ServerOrigin
        &&a.DeviceId==b.DeviceId&&a.CredentialScope==b.CredentialScope&&a.BackendIdentity==b.BackendIdentity&&a.Binding.RootId==b.Binding.RootId&&a.Binding.ServerIdentity==b.Binding.ServerIdentity&&a.RemoteAnchorId==b.RemoteAnchorId;
    private static async Task<LocalCycleScope> RequireHistoricalCycle(SqliteConnection c,SqliteTransaction t,string root,string owner,string current,string previous,CancellationToken ct)
    {
        var live=await RequireCycle(c,t,root,owner,current,false,ct);var old=await CycleScope(c,t,previous,ct);
        if(!SameCyclePrincipal(live.Configuration,old.Configuration))throw new InvalidDataException("Historical recovery cannot rebind mapping, anchor, backend or credentials.");
        if(old.Root!=root)
        {
            if(!await HasReplacementLineage(c,t,current,previous,root,old.Root,live.Configuration,false,ct))throw new InvalidDataException("Cross-subscription historical recovery requires its explicit immutable replacement lineage.");
        }
        await using var q=SyncCommand(c,t,"SELECT binding_json FROM local_source_roots WHERE root_key=$old");q.Parameters.AddWithValue("$old",old.Root);var raw=await q.ExecuteScalarAsync(ct)as string??throw new InvalidDataException("Original local root evidence missing.");var prior=JsonSerializer.Deserialize<LocalSourceBinding>(raw,SyncStateJson)!;var active=(await ReadLocalStateAsync(c,t,root,owner,ct)).Binding;
        if(prior.SourceRoot!=active.SourceRoot||prior.RootFileId!=active.RootFileId||prior.RootIncarnation!=active.RootIncarnation||prior.VolumeGuid!=active.VolumeGuid||prior.VolumeSerial!=active.VolumeSerial)throw new InvalidDataException("Historical recovery cannot borrow another source incarnation.");return old;
    }
    internal async Task<LocalCycleWork> ReadOwnedCycleRecovery(string root,string owner,string current,string previous,string operation,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireHistoricalCycle(c,t,root,owner,current,previous,ct);var w=await ReadCycleWork(c,t,operation,ct);
        if(w.ScopeId!=previous||w.Entry.Directory||w.InputJson is null||w.CaptureProof is null||w.IntentHash is null||w.Size is null||w.Sha256 is null)throw new InvalidDataException("Recovery accepts only an original attempted sealed file operation.");return w;
    }
    internal async Task SaveOwnedCycleRecovery(string root,string owner,string current,string previous,string operation,CycleUploadStatus status,LocalJournalAdmission local,CancellationToken ct)
    {
        await CycleAdmission(root,owner,current,local,4,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireHistoricalCycle(c,t,root,owner,current,previous,ct);var w=await ReadCycleWork(c,t,operation,ct);
        if(w.ScopeId!=previous||w.Entry.Directory||w.InputJson is null||w.IntentHash is null)throw new InvalidDataException("Original immutable recovery record missing.");var input=JsonSerializer.Deserialize<CycleUploadInput>(w.InputJson,LocalCycleWire.Json)!;
        object session=new{uploadId=status.UploadId,operationId=status.OperationId,rootId=status.RootId,intentHash=status.IntentHash,expectedSizeBytes=status.ExpectedBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),receivedSizeBytes=status.ReceivedBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),phase=status.Phase,transferPending=status.TransferPending};
        object body=status.ResultJson is null?new{session,replayed=status.Replayed}:(object)new{session,result=JsonSerializer.Deserialize<JsonElement>(status.ResultJson),replayed=status.Replayed};var validated=LocalCycleWire.Status(JsonSerializer.SerializeToUtf8Bytes(body),input,w.IntentHash,w.UploadId);
        if(validated.ReceivedBytes<w.ReceivedBytes||w.Phase==LocalCycleWorkPhase.Published&&!LocalCycleWire.SameResult(validated.ResultJson,w.ResultJson))throw new InvalidDataException("Historical outcome/offset cannot regress.");
        await using var q=SyncCommand(c,t,"UPDATE local_cycle_work SET upload_id=$upload,received=$received,phase=$phase,result_json=$result,error=NULL WHERE operation_id=$operation");q.Parameters.AddWithValue("$operation",operation);q.Parameters.AddWithValue("$upload",validated.UploadId);q.Parameters.AddWithValue("$received",validated.ReceivedBytes);q.Parameters.AddWithValue("$phase",validated.Phase=="published"?5:validated.Phase=="not_published"?7:4);q.Parameters.AddWithValue("$result",(object?)validated.ResultJson??DBNull.Value);await q.ExecuteNonQueryAsync(ct);
        await RetainReplacementOriginal(c,t,current,await ReadCycleWork(c,t,operation,ct),validated.Phase,ct);
        // Historical receipt only. No baseline, current R, dirty, local applied
        // cursor, active anchor or source-cycle state is updated here.
        await CommitCycle(c,t,before,ct);
    }
    internal async Task<bool> HasOwnedCyclePublication(string root,string owner,string current,LocalCycleEntry entry,SyncDescriptor remote,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);var live=await RequireCycle(c,t,root,owner,current,false,ct);var scopes=new List<LocalCycleScope>();
        await using(var q=SyncCommand(c,t,"SELECT scope_id FROM local_cycle_scopes WHERE root_key=$root AND scope_id<>$current LIMIT 16")){q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$current",current);await using var r=await q.ExecuteReaderAsync(ct);var ids=new List<string>();while(await r.ReadAsync(ct))ids.Add(r.GetString(0));await r.DisposeAsync();foreach(var id in ids)scopes.Add(await CycleScope(c,t,id,ct));}
        scopes.AddRange(await InstalledPublicationAncestors(c,t,live,ct));
        foreach(var prior in scopes.DistinctBy(s=>s.ScopeId).Where(s=>SameCyclePrincipal(s.Configuration,live.Configuration)))
        {
            // A new journal scope is keyed by physical IDs again. Find the
            // prior stable ledger key through its qualified incarnation map,
            // then require the publication itself to prove that same native ID.
            var nodes=await HasIncarnations(c,t,ct)?"node_id IN($node,COALESCE((SELECT node_id FROM local_parent_reconcile_incarnations WHERE scope_id=$scope AND native_id=$node),$node))":"node_id=$node";
            await using var q=SyncCommand(c,t,$"SELECT {CycleWorkColumns} FROM local_cycle_work WHERE scope_id=$scope AND {nodes} AND phase=5 ORDER BY work_no DESC LIMIT 1");CycleId(q,prior.ScopeId);q.Parameters.AddWithValue("$node",entry.SourceNodeId);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))continue;var w=CycleWork(r);
            if(w.Entry.SourceNodeId==entry.SourceNodeId&&w.Entry.Created==entry.Created&&w.Entry.RelativePath==entry.RelativePath&&w.InputJson is not null&&w.IntentHash is not null&&w.UploadId is not null)
            {
                var input=JsonSerializer.Deserialize<CycleUploadInput>(w.InputJson,LocalCycleWire.Json)!;
                if(LocalCycleWire.IntentHash(input)!=w.IntentHash||w.ResultJson is null||w.Size is null||w.Sha256 is null||input.Intent.SizeBytes!=w.Size.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)||input.Intent.Sha256!=w.Sha256)throw new InvalidDataException("Original owned publication evidence changed.");
                var body=new{session=new{uploadId=w.UploadId,operationId=w.UploadId,rootId=input.Intent.RootId,intentHash=w.IntentHash,expectedSizeBytes=input.Intent.SizeBytes,receivedSizeBytes=w.ReceivedBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),phase="published",transferPending=false},result=JsonSerializer.Deserialize<JsonElement>(w.ResultJson),replayed=true};
                var validated=LocalCycleWire.Status(JsonSerializer.SerializeToUtf8Bytes(body),input,w.IntentHash,w.UploadId);
                if(validated.PublishedDescription!.NodeId==remote.NodeId&&input.Intent.RootId==live.Configuration.Binding.RootId)return true;
            }
        }
        return false;
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<LocalCycleWork> ReadCycleRecoveryAsync(string current,string previous,string operation,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedCycleRecovery(_root,_owner,current,previous,operation,ct),ct);
    internal Task SaveCycleRecoveryAsync(string current,string previous,string operation,CycleUploadStatus status,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedCycleRecovery(_root,_owner,current,previous,operation,status,_admission,ct);return true;},ct);
    internal Task<bool> HasCycleOwnershipAsync(string current,LocalCycleEntry entry,SyncDescriptor remote,CancellationToken ct)=>SerialAsync(()=>_store.HasOwnedCyclePublication(_root,_owner,current,entry,remote,ct),ct);
}
