using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Neptune.Core.Sync.Protocol;
namespace Neptune.Core;

public sealed partial class WindowsFreshSyncEngine
{
    private async Task<bool> TryParallelStreamWork(LocalCycleWork work,StoredStream? retained,CancellationToken ct)
    {
        if(_remote is not IWindowsStreamRemote remote||_remote is not IWindowsCycleExtensions extensions||!extensions.Supports("scale-transfer-v2"))throw new BootstrapCaptureDeferredException("Original parallel stream capability unavailable.");
        var path=_resources.PathFor(work.OperationId);var recovery=retained is not null;
        await _resources.ReserveAsync(work.OperationId,4L*4194304,ct);var large=false;
        try{
            await _resources.Large.WaitAsync(ct);large=true;_resources.StartLarge();
            using var admission=await AcquireRootMutation(null,ct);
            SyncStreamStatus? status=null;
            if(retained is not null)
            {
                admission?.Attempt();
                var after=-1;
                do{
                    try{status=await Admission(()=>remote.StreamStatusPageAsync(retained.Input,after,ct),ct);}
                    catch(GuardedCycleRemoteException e)when(after==-1&&e.StructuredWireCode&&e.Status==System.Net.HttpStatusCode.Conflict&&e.Code=="operation_reused")
                    {status=await Admission(()=>remote.StreamCommandAsync("prepare",retained.Input,null,ct),ct);}
                    await _session.SaveStreamAsync(ScopeId,retained.Input,status,null,ct);
                    if(!status.Parallel||status.AfterIndex!=after)throw new InvalidDataException("Original stream page changed.");
                    after=status.NextIndex??-1;
                }while(after>=0);
            }
            if(status?.State=="UNKNOWN"||status?.PendingChunks.Count>0)throw new BootstrapCaptureDeferredException("Parallel stream retains unresolved physical parts.");
            if(retained?.Seal is null)
            {
                var target=retained is null?await _session.ReadCycleTargetAsync(ScopeId,work.Entry,ct):default;
                if(retained is null&&target.Target is not null)
                {var baseline=work.Entry.BaselineJson is null?null:JsonSerializer.Deserialize<LocalCycleBaseline>(work.Entry.BaselineJson,LocalCycleWire.Json);if(baseline?.Description!=target.Target)throw new CycleConflictException("Streaming cannot overwrite an unknown remote version.");}
                long confirmed=0;
                var seal=await _capture.CaptureStreamAsync(work,path,async(witness,size,token)=>
                {
                    if(size>Math.Min(_resources.Budget.MaximumNewFileBytes??9007199254740991L,9007199254740991L))throw new BootstrapCaptureDeferredException("Stream source exceeds negotiated size limit.");
                    if(retained is null)
                    {
                        var destination=target.Target;var intent=new CycleUploadIntent("saturn.upload-intent.v2","windows_sync",_configuration.DeviceId,_configuration.Binding.RootId,"/"+work.Entry.RelativePath,"neptune-cycle-"+work.OperationId,target.Parent,work.Entry.RawName,destination?.NodeId,size.ToString(CultureInfo.InvariantCulture),null!,null,destination is null?"*":null,destination is null,destination?.ContentVersionId,destination is null?new("absent"):new("match",destination.NodeId,destination.Version),target.Topology);
                        var generation=work.Entry.Generation.Journal.ToString(CultureInfo.InvariantCulture)+":"+work.Entry.Generation.Watcher.ToString(CultureInfo.InvariantCulture);
                        var input=new SyncStreamInput(work.OperationId,generation,witness,JsonSerializer.SerializeToElement(new{schema="saturn.stream-upload.v2",operationId=work.OperationId,source=new{generation,witnessSha256=witness},chunkBytes=4194304,prepare=LocalCycleWire.InputObject(new(_configuration.Binding.ServerEpoch,_configuration.Binding.BindingVersion,_configuration.BackendIdentity,intent))}));
                        retained=new(input,null,null);await _session.SaveStreamAsync(ScopeId,input,null,null,token);
                    }
                    else if(retained.Input.WitnessSha256!=witness)throw new BootstrapCaptureDeferredException("Original source version changed; retained stream cannot be recaptured.");
                    admission?.Attempt();
                    status??=await Admission(()=>remote.StreamCommandAsync("prepare",retained.Input,null,token),token);
                    if(!status.Parallel)throw new InvalidDataException("Parallel stream changed protocol.");
                    await _session.SaveStreamAsync(ScopeId,retained.Input,status,null,token);
                    confirmed=status.Received;_syncProgress.TransferStart(work.OperationId,size,confirmed);
                },async(index,bytes,token)=>
                {
                    _coordinator.Diagnostics.StreamRead(bytes.Length,recovery);
                    var chunk=new SyncStreamChunk(index,((long)index*4194304).ToString(CultureInfo.InvariantCulture),bytes.Length.ToString(CultureInfo.InvariantCulture),Convert.ToHexStringLower(SHA256.HashData(bytes.Span)));
                    var original=retained??throw new InvalidDataException("Stream original absent.");
                    var known=await _session.ReadStreamPartAsync(ScopeId,work.OperationId,index,token);
                    if(known is not null){if(known!=chunk)throw new InvalidDataException("Retained ACK differs from the same source version.");return;}
                    SyncStreamStatus answer;using(var slot=await TransferSlot(bytes.Length,token))answer=await remote.StreamChunkAsync(original.Input,chunk,bytes,token);
                    if(!answer.Parallel||!answer.Chunks.Contains(chunk))throw new BootstrapCaptureDeferredException("Original part ACK unresolved.");
                    await _session.SaveStreamAsync(ScopeId,original.Input,answer,null,token);Interlocked.Add(ref _received,bytes.Length);
                    long observed;do{observed=Interlocked.Read(ref confirmed);if(observed>=answer.Received)break;}while(Interlocked.CompareExchange(ref confirmed,answer.Received,observed)!=observed);
                    _syncProgress.Acknowledge(work.OperationId,Interlocked.Read(ref confirmed));
                },SourceReadSlot,ct,maximumInFlight:4);
                // All in-flight callbacks have drained while the native handle
                // still pins the same source version. Only now seal whole SHA.
                status=await remote.StreamStatusPageAsync(retained!.Input,-1,ct);
                if(status.Received!=seal.Size||status.PendingChunks.Count!=0||status.State!="RECEIVING")throw new BootstrapCaptureDeferredException("Parallel stream has unconfirmed parts.");
                retained=retained with{Seal=new(path,seal.Size,seal.Sha256,seal.ProofJson)};
                await _session.SaveStreamAsync(ScopeId,retained.Input,status,retained.Seal,ct);
                Interlocked.Increment(ref _capturedFiles);Interlocked.Add(ref _capturedBytes,seal.Size);
            }
            var full=retained!;var localSeal=full.Seal!;
            if(status?.State=="RECEIVING")status=await remote.StreamCommandAsync("seal",full.Input,localSeal.Sha256,ct);
            if(status?.State is "SEALED" or "PUBLISHING")status=await remote.StreamCommandAsync("complete",full.Input,null,ct);
            if(status is null)throw new InvalidDataException("Stream status absent.");await _session.SaveStreamAsync(ScopeId,full.Input,status,localSeal,ct);
            var input=JsonSerializer.Deserialize<CycleUploadInput>(full.Input.Input.GetProperty("prepare").GetRawText(),LocalCycleWire.Json)!;input=input with{Intent=input.Intent with{Sha256=localSeal.Sha256}};
            if(status.Publication is { } outcome)
            {
                var publication=LocalCycleWire.Status(JsonSerializer.SerializeToUtf8Bytes(outcome),input,LocalCycleWire.IntentHash(input),work.OperationId);
                if(publication.Phase=="not_published"){await _session.FinishCycleWorkAsync(ScopeId,work.OperationId,LocalCycleWorkPhase.Conflict,null,"Original stream publication refused by namespace CAS.",ct);admission?.Complete();return true;}
                if(status.State=="PUBLISHED")
                {await _session.CompleteStreamAsync(ScopeId,full,input,publication,_capture.RootProof,ct);admission?.Complete();Interlocked.Increment(ref _publishedFiles);await PostCommit(work,WindowsCycleStage.FilePublished,true,ct);return true;}
            }
            throw new BootstrapCaptureDeferredException("Original parallel stream publication unresolved.");
        }finally{if(large){_resources.EndLarge();_resources.Large.Release();}_resources.Release(work.OperationId,false);}
    }
}
