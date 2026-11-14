using System.Globalization;
using System.ComponentModel;
using Microsoft.Win32.SafeHandles;
using System.Security.Cryptography;
using System.Text.Json;

namespace Neptune.Core;

public sealed partial class WindowsNativeCycleCapture
{
    internal async Task<LocalCycleSeal> CaptureStreamAsync(LocalCycleWork work,string chunkPath,
        Func<string,long,CancellationToken,Task> begin,Func<int,ReadOnlyMemory<byte>,CancellationToken,Task> chunk,
        Func<CancellationToken,Task<IDisposable>> readSlot,CancellationToken ct,int maximumInFlight=1)
    {
        if(maximumInFlight is <1 or >4)throw new ArgumentOutOfRangeException(nameof(maximumInFlight));
        var entry=work.Entry;var full=Resolve(entry.RelativePath);var parentPath=Path.GetDirectoryName(full)!;
        using var ancestry=WindowsBootstrapSourceCapture.HoldDirectoryPath(parentPath);using var root=Open(RootProof.RootPath,0x80,3);ValidateRoot(root);
        using var parent=Open(parentPath,0x80,3);WindowsBootstrapSourceCapture.RequireHandlePath(parent,parentPath);var parentProof=ReadIdentity(parent,true);
        if(parentProof.FileId128!=entry.ParentId)throw new IOException("Stream parent identity changed.");
        SafeFileHandle opened;
        try{opened=Open(full,0x80000000u|0x80u,1);}
        catch(Win32Exception e)when(e.NativeErrorCode is 2 or 3 or 32 or 33){throw new BootstrapCaptureDeferredException("Source changed or is held by an editor before stream begin.");}
        using var handle=opened;await using var source=new FileStream(handle,FileAccess.Read,128*1024,isAsync:false);
        WindowsBootstrapSourceCapture.RequireHandlePath(handle,full);RequireExactLeaf(handle,entry.RawName);var before=ReadIdentity(handle,false);var usn=ReadLastFileUsn(handle,before);
        if(before.FileId128!=entry.SourceNodeId||before.Created!=entry.Created)throw new BootstrapCaptureDeferredException("Source incarnation changed before stream begin; await native reconciliation.");
        if(before.Links!=1||before.Volume64!=RootProof.Handle.VolumeSerial64||before.Volume32!=RootProof.Handle.LegacyVolumeSerial||parentProof.Volume64!=before.Volume64||parentProof.Volume32!=before.Volume32||before.Size>9007199254740991L)throw new IOException("Stream source identity/limit changed.");
        var witness=Convert.ToHexStringLower(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(new{root=RootProof,NodeId=entry.SourceNodeId,entry.Created,entry.ParentId,entry.RelativePath,entry.Generation,before,usn},LocalCycleWire.Json)));
        await begin(witness,before.Size,ct);
        using var digest=IncrementalHash.CreateHash(HashAlgorithmName.SHA256);long read=0,index=0;
        using var linked=CancellationTokenSource.CreateLinkedTokenSource(ct);var pending=new Queue<Task>();
        async Task Send(int n,byte[] buffer,int length)
        {
            try{await chunk(n,buffer.AsMemory(0,length),linked.Token);}finally{System.Buffers.ArrayPool<byte>.Shared.Return(buffer);}
        }
        try{
            while(read<before.Size)
            {
                if(pending.Count>=maximumInFlight)await pending.Dequeue();
                linked.Token.ThrowIfCancellationRequested();var buffer=System.Buffers.ArrayPool<byte>.Shared.Rent(4194304);var length=(int)Math.Min(4194304,before.Size-read);
                try{using(var slot=await readSlot(linked.Token)){await source.ReadExactlyAsync(buffer.AsMemory(0,length),linked.Token);digest.AppendData(buffer,0,length);}}
                catch{System.Buffers.ArrayPool<byte>.Shared.Return(buffer);throw;}
                var send=Send(checked((int)index),buffer,length);if(index++==0)await send;else pending.Enqueue(send);read+=length;
            }
            while(pending.Count>0)await pending.Dequeue();
        }catch{await linked.CancelAsync();try{await Task.WhenAll(pending);}catch{/* preserve first failure after draining all callbacks */}throw;}
        var after=ReadIdentity(handle,false);ValidateRoot(root);WindowsBootstrapSourceCapture.RequireHandlePath(parent,parentPath);WindowsBootstrapSourceCapture.RequireHandlePath(handle,full);RequireExactLeaf(handle,entry.RawName);
        if(before!=after||ReadIdentity(parent,true).Witness!=parentProof.Witness||ReadLastFileUsn(handle,after)!=usn)throw new IOException("Same source version changed before final stream seal.");
        var hash=Convert.ToHexStringLower(digest.GetHashAndReset());
        var evidence=new LocalCycleCaptureEvidence("neptune.native-cycle-capture.v1",work.ScopeId,work.OperationId,entry.SourceNodeId,entry.Created,entry.ParentId,entry.RelativePath,entry.Generation.Journal,entry.Generation.Watcher,
            RootProof.Handle.FileId128,RootProof.Handle.CreationTime,RootProof.VolumeGuid,RootProof.Handle.LegacyVolumeSerial.ToString("x8",CultureInfo.InvariantCulture),before.LegacyFileId64,read.ToString(CultureInfo.InvariantCulture),hash,usn.ToString(CultureInfo.InvariantCulture),Guid.NewGuid().ToString("N"));
        var proof=JsonSerializer.Serialize(evidence,LocalCycleWire.Json);ValidatePersisted(work,read,hash,proof,RootProof);return new(chunkPath,read,hash,proof);
    }
}
