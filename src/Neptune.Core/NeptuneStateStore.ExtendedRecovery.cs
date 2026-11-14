using System.Text;
using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;
public sealed record ClientRecoveryTarget(string ScopeId,WindowsCycleConfiguration Configuration,string SourceRoot);
public sealed record ClientRecoveryReport(long Resolved,long Deferred,IReadOnlyList<string> Reasons);
public sealed partial class NeptuneStateStore
{
    public async Task<ClientRecoveryTarget> ReadClientRecoveryTargetAsync(string? scope=null,CancellationToken ct=default)
    {
        await ValidateProfileCompatibilityAsync(ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);
        if(scope is null){await using var q=SyncCommand(c,t,"SELECT scope_id FROM local_cycle_active LIMIT 2");await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))throw new InvalidOperationException("Retained scope missing.");scope=r.GetString(0);if(await r.ReadAsync(ct))throw new InvalidOperationException("Select an exact retained scope.");}
        var original=await ReadReplacementOriginal(c,t,scope,ct);return new(scope,original.Scope.Configuration,original.Binding.SourceRoot);
    }
    public async Task<ClientRecoveryReport> RecoverExtendedOriginalsAsync(ClientRecoveryTarget target,IWindowsCycleRemote remote,CancellationToken ct=default)
    {
        if(remote.Binding!=target.Configuration.Binding||remote is not IWindowsCycleExtensions extensions)throw new InvalidDataException("Original recovery authority unavailable.");
        await using var owner=new FileStream(Path.GetFullPath(databasePath)+".local-source-owner.lock",FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None);
        await ValidateProfileCompatibilityAsync(ct);if(!await PreflightClientEvolution(ct))return new(0,0,[]);
        await using var native=await WindowsNativeCycleCapture.OpenAsync(target.SourceRoot,ct);
        LocalCycleScope scope;
        await using(var c=await OpenCycleConnection(ct))
        {
            using var t=c.BeginTransaction(deferred:true);var old=await ReadReplacementOriginal(c,t,target.ScopeId,ct);scope=old.Scope;
            if(JsonSerializer.Serialize(old.Scope.Configuration,LocalCycleWire.Json)!=JsonSerializer.Serialize(target.Configuration,LocalCycleWire.Json)||old.Binding.SourceRoot!=native.RootProof.RootPath||old.Binding.RootFileId!=native.RootProof.Handle.FileId128||old.Binding.RootIncarnation!=native.RootProof.Handle.CreationTime||old.Binding.VolumeGuid!=native.RootProof.VolumeGuid||old.Binding.VolumeSerial!=native.RootProof.Handle.LegacyVolumeSerial.ToString("x8"))throw new InvalidDataException("Recovery cannot borrow a different source incarnation or original scope.");
        }
        long resolved=0,deferred=0;var reasons=new List<string>();
        void Deferred(string reason){deferred++;if(reasons.Count<50)reasons.Add(reason);}
        var admission=new LocalJournalAdmission(MaximumDatabaseBytes:scope.Budget.ProfileBytes,MinimumFreeDiskBytes:scope.Budget.MinimumFreeDiskBytes);
        string after="";
        while(true)
        {
            var page=new List<NamespaceOperation>();
            await using(var c=await OpenCycleConnection(ct))
            {
                using var t=c.BeginTransaction(deferred:true);await using var q=SyncCommand(c,t,"SELECT request_json,entry_json,state,approved,receipt_json FROM client_namespace_operations WHERE scope_id=$scope AND operation_id>$after AND state NOT IN('PUBLISHED','NOT_PUBLISHED','CANCELLED') ORDER BY operation_id LIMIT 120");CycleId(q,target.ScopeId);q.Parameters.AddWithValue("$after",after);
                await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))page.Add(new(JsonSerializer.Deserialize<SyncMutationRequest>(r.GetString(0),LocalCycleWire.Json)!,JsonSerializer.Deserialize<LocalCycleEntry>(r.GetString(1),LocalCycleWire.Json)!,r.GetString(2),r.GetInt32(3)!=0,r.IsDBNull(4)?null:r.GetString(4)));
            }
            foreach(var original in page)
            {
                after=original.Request.OperationId;ct.ThrowIfCancellationRequested();
                try
                {
                    // QUEUED precedes dispatch. After a gap it can be cancelled,
                    // but never newly executed from the old absence proof.
                    var receipt=original.State=="QUEUED"?null:await extensions.MutateAsync(original.Request,true,ct);
                    if(receipt is not null&&original.Receipt is not null&&SyncMutationWire.Read(Encoding.UTF8.GetBytes(original.Receipt),original.Request).IntentHash!=receipt.IntentHash)throw new InvalidDataException("Historical mutation identity changed.");
                    var terminal=original.State=="QUEUED"?"CANCELLED":receipt?.State;
                    if(terminal is not("CANCELLED" or "PUBLISHED" or "NOT_PUBLISHED")){Deferred("Namespace outcome still requires server reconciliation.");continue;}
                    await CheckLocalAdmissionAsync(admission,0,NamespaceReserve(20),ct);
                    await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);
                    await using var q=SyncCommand(c,t,"UPDATE client_namespace_operations SET state=$state,receipt_json=COALESCE($receipt,receipt_json),error='Offline historical recovery; current baseline/dirty not acknowledged.' WHERE scope_id=$scope AND operation_id=$op AND request_json=$request;UPDATE local_cycle_work SET phase=$phase,active=NULL,result_json=COALESCE($description,result_json),error='Historical extended outcome retained; no current baseline acknowledgement.' WHERE scope_id=$scope AND operation_id=$op AND input_json IS NULL AND active=1;UPDATE local_cycle_files SET phase=2 WHERE scope_id=$scope AND node_id=$node");
                    CycleId(q,target.ScopeId);q.Parameters.AddWithValue("$op",after);q.Parameters.AddWithValue("$state",terminal);q.Parameters.AddWithValue("$receipt",(object?)receipt?.RawJson??DBNull.Value);q.Parameters.AddWithValue("$request",JsonSerializer.Serialize(original.Request,LocalCycleWire.Json));q.Parameters.AddWithValue("$phase",terminal=="PUBLISHED"?6:7);q.Parameters.AddWithValue("$description",receipt?.Description is { } description?JsonSerializer.Serialize(description,LocalCycleWire.Json):DBNull.Value);q.Parameters.AddWithValue("$node",original.Entry.NodeId);await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);resolved++;
                }
                catch(GuardedCycleRemoteException error)when(error.Status is not(System.Net.HttpStatusCode.Unauthorized or System.Net.HttpStatusCode.Forbidden)){Deferred("Namespace: "+error.Code);}
            }
            if(page.Count<120)break;
        }
        after="";
        while(remote is IWindowsStreamRemote streams)
        {
            var page=new List<StoredStream>();
            await using(var c=await OpenCycleConnection(ct))
            {
                using var t=c.BeginTransaction(deferred:true);await using var q=SyncCommand(c,t,"SELECT input_json,seal_json,status_json FROM client_stream_operations WHERE scope_id=$scope AND operation_id>$after AND COALESCE(json_extract(status_json,'$.state'),'UNKNOWN')<>'PUBLISHED' AND COALESCE(json_extract(status_json,'$.publication.session.phase'),'')<>'not_published' ORDER BY operation_id LIMIT 120");CycleId(q,target.ScopeId);q.Parameters.AddWithValue("$after",after);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))page.Add(new(JsonSerializer.Deserialize<SyncStreamInput>(r.GetString(0),LocalCycleWire.Json)!,r.IsDBNull(1)?null:JsonSerializer.Deserialize<StoredStreamSeal>(r.GetString(1),LocalCycleWire.Json),r.IsDBNull(2)?null:r.GetString(2)));
            }
            foreach(var original in page)
            {
                after=original.Input.OperationId;ct.ThrowIfCancellationRequested();
                try
                {
                    var status=await streams.StreamCommandAsync("status",original.Input,null,ct);
                    if(original.Seal is not { } seal){Deferred("Stream has no final native source seal; same-version recovery is required.");continue;}
                    LocalCycleWork work;await using(var read=await OpenCycleConnection(ct)){using var snapshot=read.BeginTransaction(deferred:true);work=await ReadCycleWork(read,snapshot,after,ct);}
                    if(work.ScopeId!=target.ScopeId)throw new InvalidDataException("Stream scope changed.");WindowsNativeCycleCapture.ValidatePersisted(work,seal.Size,seal.Sha256,seal.Proof,native.RootProof);
                    if(original.Status is not null)
                    {
                        var previous=SaturnGuardedCycleTransport.ReadStream(Encoding.UTF8.GetBytes(original.Status),original.Input);
                        if(previous.Chunks.Count>status.Chunks.Count||previous.Chunks.Where((chunk,index)=>chunk!=status.Chunks[index]).Any()||previous.FinalSha256 is not null&&previous.FinalSha256!=status.FinalSha256)throw new InvalidDataException("Original stream ACK/seal changed.");
                    }
                    if(status.State=="RECEIVING"&&status.Pending is null&&status.Received==seal.Size)status=await streams.StreamCommandAsync("seal",original.Input,seal.Sha256,ct);
                    if(status.FinalSha256!=seal.Sha256||status.Pending is not null){Deferred("Stream physical chunk or final seal remains unresolved.");continue;}
                    if(status.State is "SEALED" or "PUBLISHING")status=await streams.StreamCommandAsync("complete",original.Input,null,ct);
                    if(status.Publication is not { } publication){Deferred("Stream publication still requires server reconciliation.");continue;}
                    var input=JsonSerializer.Deserialize<CycleUploadInput>(original.Input.Input.GetProperty("prepare").GetRawText(),LocalCycleWire.Json)!;input=input with{Intent=input.Intent with{Sha256=seal.Sha256}};
                    var outcome=LocalCycleWire.Status(JsonSerializer.SerializeToUtf8Bytes(publication),input,LocalCycleWire.IntentHash(input),after);
                    if(outcome.Phase is not("published" or "not_published")){Deferred("Stream catalog outcome is not terminal.");continue;}
                    await CheckLocalAdmissionAsync(admission,0,NamespaceReserve(64),ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);work=await ReadCycleWork(c,t,after,ct);
                    if(work.ScopeId!=target.ScopeId)throw new InvalidDataException("Stream scope changed.");WindowsNativeCycleCapture.ValidatePersisted(work,seal.Size,seal.Sha256,seal.Proof,native.RootProof);
                    await using var q=SyncCommand(c,t,"UPDATE client_stream_operations SET status_json=$status WHERE operation_id=$op AND scope_id=$scope;UPDATE local_cycle_work SET phase=$phase,active=NULL,spool_path=$path,size=$size,sha256=$hash,capture_proof=$proof,input_json=$input,intent_hash=$intent,upload_id=$op,received=$received,result_json=$result,error='Offline historical stream recovery; baseline/dirty retained.' WHERE operation_id=$op AND scope_id=$scope;UPDATE local_cycle_files SET phase=2 WHERE scope_id=$scope AND node_id=$node");
                    CycleId(q,target.ScopeId);q.Parameters.AddWithValue("$op",after);q.Parameters.AddWithValue("$status",status.RawJson);q.Parameters.AddWithValue("$phase",outcome.Phase=="published"?5:7);q.Parameters.AddWithValue("$path",seal.Path);q.Parameters.AddWithValue("$size",seal.Size);q.Parameters.AddWithValue("$hash",seal.Sha256);q.Parameters.AddWithValue("$proof",seal.Proof);q.Parameters.AddWithValue("$input",JsonSerializer.Serialize(input,LocalCycleWire.Json));q.Parameters.AddWithValue("$intent",LocalCycleWire.IntentHash(input));q.Parameters.AddWithValue("$received",outcome.ReceivedBytes);q.Parameters.AddWithValue("$result",(object?)outcome.ResultJson??DBNull.Value);q.Parameters.AddWithValue("$node",work.Entry.NodeId);await q.ExecuteNonQueryAsync(ct);await t.CommitAsync(ct);resolved++;
                }
                catch(GuardedCycleRemoteException error)when(error.Status is not(System.Net.HttpStatusCode.Unauthorized or System.Net.HttpStatusCode.Forbidden)){Deferred("Stream: "+error.Code);}
            }
            if(page.Count<120)break;
        }
        return new(resolved,deferred,reasons);
    }
}
