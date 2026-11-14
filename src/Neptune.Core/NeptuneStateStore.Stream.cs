using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;
internal sealed record StoredStreamSeal(string Path,long Size,string Sha256,string Proof);
internal sealed record StoredStream(SyncStreamInput Input,StoredStreamSeal? Seal,string? Status);

public sealed partial class NeptuneStateStore
{
    internal async Task<StoredStream?> ReadOwnedStream(string root,string owner,string scope,string operation,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        await using var q=SyncCommand(c,t,"SELECT input_json,seal_json,status_json FROM client_stream_operations WHERE scope_id=$scope AND operation_id=$op");CycleId(q,scope);q.Parameters.AddWithValue("$op",operation);await using var r=await q.ExecuteReaderAsync(ct);
        return await r.ReadAsync(ct)?new(JsonSerializer.Deserialize<SyncStreamInput>(r.GetString(0),LocalCycleWire.Json)!,r.IsDBNull(1)?null:JsonSerializer.Deserialize<StoredStreamSeal>(r.GetString(1),LocalCycleWire.Json),r.IsDBNull(2)?null:r.GetString(2)):null;
    }
    internal async Task SaveOwnedStream(string root,string owner,string scope,SyncStreamInput input,SyncStreamStatus? status,StoredStreamSeal? seal,CancellationToken ct)
    {
        var parallel=input.Input.GetProperty("schema").GetString()=="saturn.stream-upload.v2";
        await ClientAdmission(root,owner,scope,64+(parallel?status?.Chunks.Count??0:0),ct);
        if(parallel)await EnsureStreamParts(ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,true,ct);var work=await ReadCycleWork(c,t,input.OperationId,ct);
        if(work.ScopeId!=scope||work.Entry.Directory)throw new InvalidDataException("Stream scope changed.");var raw=JsonSerializer.Serialize(input,LocalCycleWire.Json);
        StoredStreamSeal? previousSeal=null;string? effectiveStatus=status?.RawJson;
        await using(var q=SyncCommand(c,t,"SELECT input_json,status_json,seal_json FROM client_stream_operations WHERE operation_id=$op"))
        {
            q.Parameters.AddWithValue("$op",input.OperationId);await using var r=await q.ExecuteReaderAsync(ct);
            if(await r.ReadAsync(ct))
            {
                if(r.GetString(0)!=raw)throw new InvalidDataException("Original stream identity cannot change.");
                if(!r.IsDBNull(2))previousSeal=JsonSerializer.Deserialize<StoredStreamSeal>(r.GetString(2),LocalCycleWire.Json)!;
                if(status is not null&&!r.IsDBNull(1))
                {
                    var previous=SaturnGuardedCycleTransport.ReadStream(System.Text.Encoding.UTF8.GetBytes(r.GetString(1)),input);
                    if(!parallel&&(previous.Received>status.Received||previous.Chunks.Count>status.Chunks.Count||previous.Chunks.Where((chunk,index)=>chunk!=status.Chunks[index]).Any()))throw new InvalidDataException("Original stream ACK map changed.");
                    if(previous.FinalSha256 is not null&&previous.FinalSha256!=status.FinalSha256||previous.State=="PUBLISHED"&&status.State!="PUBLISHED")throw new InvalidDataException("Original stream seal/publication regressed.");
                    // Parallel replies can arrive out of order. Keep their
                    // immutable part ACKs, but never lower the durable total.
                    if(parallel&&previous.Received>status.Received)effectiveStatus=previous.RawJson;
                }
            }
        }
        if(previousSeal is not null&&seal is not null&&previousSeal!=seal)throw new InvalidDataException("Original native source seal cannot change.");
        var effectiveSeal=seal??previousSeal;
        if(status?.FinalSha256 is not null&&(effectiveSeal is null||status.FinalSha256!=effectiveSeal.Sha256))throw new InvalidDataException("Remote stream seal differs from retained native source.");
        await using(var q=SyncCommand(c,t,"INSERT INTO client_stream_operations VALUES($op,$scope,$input,$status,$seal) ON CONFLICT(operation_id) DO UPDATE SET status_json=COALESCE($status,status_json),seal_json=COALESCE($seal,seal_json)"))
        {CycleId(q,scope);q.Parameters.AddWithValue("$op",input.OperationId);q.Parameters.AddWithValue("$input",raw);q.Parameters.AddWithValue("$status",(object?)effectiveStatus??DBNull.Value);q.Parameters.AddWithValue("$seal",seal is null?DBNull.Value:JsonSerializer.Serialize(seal,LocalCycleWire.Json));await q.ExecuteNonQueryAsync(ct);}
        if(parallel&&status is not null)await SaveStreamParts(c,t,input,status,ct);
        await t.CommitAsync(ct);
    }
    internal async Task CompleteOwnedStream(string root,string owner,string scope,StoredStream stream,CycleUploadInput input,CycleUploadStatus status,LocalNamespaceRootProof proof,CancellationToken ct)
    {
        await ClientAdmission(root,owner,scope,16,ct);
        var seal=stream.Seal??throw new InvalidDataException("Original source seal missing.");
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);_=await RequireCycle(c,t,root,owner,scope,true,ct);var work=await ReadCycleWork(c,t,stream.Input.OperationId,ct);
        WindowsNativeCycleCapture.ValidatePersisted(work,seal.Size,seal.Sha256,seal.Proof,proof);
        if(status.Phase!="published"||status.UploadId!=work.OperationId||status.PublishedDescription?.ContentSha256!=seal.Sha256||status.ExpectedBytes!=seal.Size)throw new InvalidDataException("Stream publication changed original bytes.");
        await using var q=SyncCommand(c,t,"UPDATE local_cycle_work SET phase=5,spool_path=$path,size=$size,sha256=$hash,capture_proof=$proof,input_json=$input,intent_hash=$intent,upload_id=$op,received=$size,result_json=$result,error=NULL WHERE operation_id=$op AND scope_id=$scope AND active=1 AND phase NOT IN(5,6)");CycleId(q,scope);q.Parameters.AddWithValue("$op",work.OperationId);q.Parameters.AddWithValue("$path",seal.Path);q.Parameters.AddWithValue("$size",seal.Size);q.Parameters.AddWithValue("$hash",seal.Sha256);q.Parameters.AddWithValue("$proof",seal.Proof);q.Parameters.AddWithValue("$input",JsonSerializer.Serialize(input,LocalCycleWire.Json));q.Parameters.AddWithValue("$intent",LocalCycleWire.IntentHash(input));q.Parameters.AddWithValue("$result",status.ResultJson!);await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<StoredStream?> ReadStreamAsync(string scope,string op,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedStream(_root,_owner,scope,op,ct),ct);
    internal Task SaveStreamAsync(string scope,SyncStreamInput input,SyncStreamStatus? status,StoredStreamSeal? seal,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedStream(_root,_owner,scope,input,status,seal,ct);return true;},ct);
    internal Task CompleteStreamAsync(string scope,StoredStream stream,CycleUploadInput input,CycleUploadStatus status,LocalNamespaceRootProof proof,CancellationToken ct)=>SerialAsync(async()=>{await _store.CompleteOwnedStream(_root,_owner,scope,stream,input,status,proof,ct);return true;},ct);
}
