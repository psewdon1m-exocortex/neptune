using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;
public sealed partial class WindowsFreshSyncEngine
{
    private async Task<bool> TryStreamWork(LocalCycleWork work,CancellationToken ct)
    {
        var retained=await _session.ReadStreamAsync(ScopeId,work.OperationId,ct);
        var recoveryRead=retained is not null;
        var available=_remote is IWindowsStreamRemote&&_remote is IWindowsCycleExtensions extensions&&extensions.Supports("stream-upload-v1");
        if(retained?.Input.Input.GetProperty("schema").GetString()=="saturn.stream-upload.v2"
            ||retained is null&&available&&((IWindowsCycleExtensions)_remote).Supports("scale-transfer-v2")&&work.Phase==LocalCycleWorkPhase.Queued&&!work.Entry.Directory&&work.Entry.EnumeratedSize>=_resources.Budget.LargeFileThreshold)
            return await TryParallelStreamWork(work,retained,ct);
        if(retained is null&&(!available||work.Phase!=LocalCycleWorkPhase.Queued||work.Entry.Directory||work.Entry.EnumeratedSize<_resources.Budget.LargeFileThreshold))return false;
        if(!available)throw new BootstrapCaptureDeferredException("Original stream capability is unavailable; sealed upload fallback cannot replace it.");
        var remote=(IWindowsStreamRemote)_remote;var path=_resources.PathFor(work.OperationId);await _resources.ReserveAsync(work.OperationId,4194304,ct);
        var largeTaken=false;
        try
        {
            await _resources.Large.WaitAsync(ct);largeTaken=true;_resources.StartLarge();
            using var admission=await AcquireRootMutation(null,ct);
            SyncStreamStatus? status=null;
            if(retained is not null)
            {
                admission?.Attempt();
                try{status=await Admission(()=>remote.StreamCommandAsync("status",retained.Input,null,ct),ct);}
                catch(GuardedCycleRemoteException e)when(e.StructuredWireCode&&e.Status==System.Net.HttpStatusCode.Conflict&&e.Code=="operation_reused")
                {status=await Admission(()=>remote.StreamCommandAsync("prepare",retained.Input,null,ct),ct);}
                await _session.SaveStreamAsync(ScopeId,retained.Input,status,null,ct);
            }
            if(status?.Publication is { } refused&&retained?.Seal is { } originalSeal)
            {
                var originalInput=JsonSerializer.Deserialize<CycleUploadInput>(retained.Input.Input.GetProperty("prepare").GetRawText(),LocalCycleWire.Json)!;
                originalInput=originalInput with{Intent=originalInput.Intent with{Sha256=originalSeal.Sha256}};
                var historical=LocalCycleWire.Status(JsonSerializer.SerializeToUtf8Bytes(refused),originalInput,LocalCycleWire.IntentHash(originalInput),work.OperationId);
                if(historical.Phase=="not_published")
                {await _session.FinishCycleWorkAsync(ScopeId,work.OperationId,LocalCycleWorkPhase.Conflict,null,"Original stream publication was refused by namespace CAS.",ct);admission?.Complete();return true;}
            }
            if(status?.State=="UNKNOWN"||status?.Pending is not null)throw new BootstrapCaptureDeferredException("Stream has an unresolved physical chunk; original ACK map retained.");
            if(retained?.Seal is null)
            {
                var target=retained is null?await _session.ReadCycleTargetAsync(ScopeId,work.Entry,ct):default;
                if(retained is null&&target.Target is not null)
                {
                    var baseline=work.Entry.BaselineJson is null?null:JsonSerializer.Deserialize<LocalCycleBaseline>(work.Entry.BaselineJson,LocalCycleWire.Json);
                    if(baseline?.Description!=target.Target)
                    {
                        if(await TryAdoptVerifiedEqualStreamAsync(work,target.Target,path,ct)){admission?.Complete();return true;}
                        throw new CycleConflictException(_configuration.AllowVerifiedEqualAdoption
                            ?"Streaming cannot overwrite a remote version with different bytes."
                            :"Streaming cannot overwrite an unknown remote version.");
                    }
                }
                var seal=await _capture.CaptureStreamAsync(work,path,async(witness,size,token)=>
                {
                    if(size>Math.Min(_resources.Budget.MaximumNewFileBytes??1024L*1024*1024,1024L*1024*1024))throw new BootstrapCaptureDeferredException("Stream source exceeds negotiated size limit.");
                    if(retained is null)
                    {
                        var destination=target.Target;var intent=new CycleUploadIntent("saturn.upload-intent.v2","windows_sync",_configuration.DeviceId,_configuration.Binding.RootId,"/"+work.Entry.RelativePath,"neptune-cycle-"+work.OperationId,target.Parent,work.Entry.RawName,destination?.NodeId,size.ToString(CultureInfo.InvariantCulture),null!,null,destination is null?"*":null,destination is null,destination?.ContentVersionId,destination is null?new("absent"):new("match",destination.NodeId,destination.Version),target.Topology);
                        var generation=work.Entry.Generation.Journal.ToString(CultureInfo.InvariantCulture)+":"+work.Entry.Generation.Watcher.ToString(CultureInfo.InvariantCulture);
                        var input=new SyncStreamInput(work.OperationId,generation,witness,JsonSerializer.SerializeToElement(new{schema="saturn.stream-upload.v1",operationId=work.OperationId,source=new{generation,witnessSha256=witness},chunkBytes=4194304,prepare=LocalCycleWire.InputObject(new(_configuration.Binding.ServerEpoch,_configuration.Binding.BindingVersion,_configuration.BackendIdentity,intent))}));
                        retained=new(input,null,null);await _session.SaveStreamAsync(ScopeId,input,null,null,token);
                    }
                    else if(retained.Input.WitnessSha256!=witness)throw new BootstrapCaptureDeferredException("Original source version changed; streaming operation retained without recapture.");
                    admission?.Attempt();
                    status??=await Admission(()=>remote.StreamCommandAsync("prepare",retained.Input,null,token),token);
                    await _session.SaveStreamAsync(ScopeId,retained.Input,status,null,token);
                    _syncProgress.TransferStart(work.OperationId,size,status.Received);
                },async(index,bytes,token)=>
                {
                    _coordinator.Diagnostics.StreamRead(bytes.Length,recoveryRead);
                    var chunk=new SyncStreamChunk(index,((long)index*4194304).ToString(CultureInfo.InvariantCulture),bytes.Length.ToString(CultureInfo.InvariantCulture),Convert.ToHexStringLower(SHA256.HashData(bytes.Span)));
                    var acknowledged=status??throw new InvalidDataException("Stream has no acknowledged original state.");
                    if(index<acknowledged.Chunks.Count){if(acknowledged.Chunks[index]!=chunk)throw new InvalidDataException("Retained ACK differs from the same source version.");return;}
                    if(File.Exists(path)&&(File.GetAttributes(path)&FileAttributes.ReparsePoint)!=0)throw new InvalidDataException("Stream spool reparse refused.");
                    await using(var spool=new FileStream(path,FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None))
                    {WindowsBootstrapSourceCapture.RequireHandlePath(spool.SafeFileHandle,path);spool.SetLength(4194304);await spool.WriteAsync(bytes,token);spool.Flush(true);}
                    var streamOriginal=retained??throw new InvalidDataException("Stream original missing before chunk dispatch.");
                    using(var slot=await TransferSlot(bytes.Length,token))status=await remote.StreamChunkAsync(streamOriginal.Input,chunk,bytes,token);await _session.SaveStreamAsync(ScopeId,streamOriginal.Input,status,null,token);
                    if(status.Pending is not null||status.Received!=(long)index*4194304+bytes.Length)throw new BootstrapCaptureDeferredException("Original stream chunk ACK is unresolved.");
                    Interlocked.Add(ref _received,bytes.Length);
                    _syncProgress.Acknowledge(work.OperationId,status.Received);
                },SourceReadSlot,ct);
                retained=retained! with{Seal=new(path,seal.Size,seal.Sha256,seal.ProofJson)};await _session.SaveStreamAsync(ScopeId,retained.Input,status,retained.Seal,ct);Interlocked.Increment(ref _capturedFiles);Interlocked.Add(ref _capturedBytes,seal.Size);
            }
            var full=retained!;var sealedSource=full.Seal!;
            if(status?.State=="RECEIVING")status=await remote.StreamCommandAsync("seal",full.Input,sealedSource.Sha256,ct);
            if(status?.State is "SEALED" or "PUBLISHING")status=await remote.StreamCommandAsync("complete",full.Input,null,ct);
            if(status is null)throw new InvalidDataException("Stream status missing.");await _session.SaveStreamAsync(ScopeId,full.Input,status,sealedSource,ct);
            if(status.State!="PUBLISHED"||status.Publication is null)throw new BootstrapCaptureDeferredException("Stream final publication is unresolved.");
            var input=JsonSerializer.Deserialize<CycleUploadInput>(full.Input.Input.GetProperty("prepare").GetRawText(),LocalCycleWire.Json)!;input=input with{Intent=input.Intent with{Sha256=sealedSource.Sha256}};
            var publication=LocalCycleWire.Status(JsonSerializer.SerializeToUtf8Bytes(status.Publication.Value),input,LocalCycleWire.IntentHash(input),work.OperationId);
            await _session.CompleteStreamAsync(ScopeId,full,input,publication,_capture.RootProof,ct);admission?.Complete();Interlocked.Increment(ref _publishedFiles);await PostCommit(work,WindowsCycleStage.FilePublished,true,ct);return true;
        }
        // Preserve typed admission/auth/transport failures for the durable
        // operation scheduler. The caller always retains this stream original.
        finally{if(largeTaken){_resources.EndLarge();_resources.Large.Release();}_resources.Release(work.OperationId,false);}
    }
}
