using System.Buffers.Binary;
using System.ComponentModel;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Win32.SafeHandles;

namespace Neptune.Core;

// A cycle seal is bound to real dirty generations, not a synthetic bootstrap
// scan generation. It is historical evidence, not an instantaneous tree cut.
public sealed partial class WindowsNativeCycleCapture : IAsyncDisposable
{
    private readonly WindowsLocalNamespaceSource _root;
    private readonly Func<NativeCaptureStage,CancellationToken,Task>? _observer;
    public LocalNamespaceRootProof RootProof=>_root.RootProof;
    private WindowsNativeCycleCapture(WindowsLocalNamespaceSource root,Func<NativeCaptureStage,CancellationToken,Task>? observer)
        =>(_root,_observer)=(root,observer);
    public static async Task<WindowsNativeCycleCapture> OpenAsync(string source,CancellationToken ct=default,
        Func<NativeCaptureStage,CancellationToken,Task>? observer=null)
        =>new(await WindowsLocalNamespaceSource.OpenAsync(source,ct),observer);
    internal async Task ValidateDirectoryAsync(LocalCycleEntry entry,CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();if(!entry.Directory)throw new InvalidDataException("Expected an original directory job.");
        var full=Resolve(entry.RelativePath);using var ancestry=WindowsBootstrapSourceCapture.HoldDirectoryPath(full);
        using var root=Open(RootProof.RootPath,0x80,3);ValidateRoot(root);using var target=Open(full,0x80,3);
        WindowsBootstrapSourceCapture.RequireHandlePath(target,full);var identity=ReadIdentity(target,true);
        if(identity.FileId128!=entry.SourceNodeId||identity.Created!=entry.Created)throw new IOException("Original directory incarnation changed.");
        await Task.CompletedTask;
    }
    internal Task<long> MeasureFileAsync(LocalCycleEntry entry,CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();if(entry.Directory)throw new InvalidOperationException();var full=Resolve(entry.RelativePath);
        using var ancestry=WindowsBootstrapSourceCapture.HoldDirectoryPath(Path.GetDirectoryName(full)!);using var root=Open(RootProof.RootPath,0x80,3);ValidateRoot(root);
        SafeFileHandle handle;try{handle=Open(full,0x80000000u|0x80u,1);}catch(Win32Exception e)when(e.NativeErrorCode is 2 or 3 or 32 or 33){throw new BootstrapCaptureDeferredException("Source writer/mapping is still open.");}
        using(handle){WindowsBootstrapSourceCapture.RequireHandlePath(handle,full);var actual=ReadIdentity(handle,false);if(actual.FileId128!=entry.SourceNodeId||actual.Created!=entry.Created||actual.Links!=1)throw new BootstrapCaptureDeferredException("Current indexed source incarnation/link count changed; await native reconciliation.");return Task.FromResult(actual.Size);}
    }
    internal async Task<LocalCycleSeal> CaptureAsync(LocalCycleWork work,string path,long maximumBytes,CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();var entry=work.Entry;
        if(entry.Directory||work.Phase!=LocalCycleWorkPhase.Queued||work.InputJson is not null||maximumBytes<0)throw new InvalidOperationException("Only an original unattempted file job can acquire a new seal.");
        var full=Resolve(entry.RelativePath);var parentPath=Path.GetDirectoryName(full)!;
        using var ancestry=WindowsBootstrapSourceCapture.HoldDirectoryPath(parentPath);
        using var root=Open(RootProof.RootPath,0x80,3);ValidateRoot(root);
        using var parent=Open(parentPath,0x80,3);WindowsBootstrapSourceCapture.RequireHandlePath(parent,parentPath);var parentProof=ReadIdentity(parent,true);
        if(parentProof.FileId128!=entry.ParentId)throw new IOException("Original cycle parent changed before capture.");
        SafeFileHandle handle;
        try{handle=Open(full,0x80000000u|0x80u,1);}
        catch(Win32Exception e)when(e.NativeErrorCode is 2 or 3 or 32 or 33){throw new BootstrapCaptureDeferredException("An active writer/delete handle or writable mapping prevents a stable cycle version.");}
        await using var source=new FileStream(handle,FileAccess.Read,128*1024,isAsync:false);
        WindowsBootstrapSourceCapture.RequireHandlePath(handle,full);RequireExactLeaf(handle,Path.GetFileName(full));
        var before=ReadIdentity(handle,false);
        if(before.FileId128!=entry.SourceNodeId||before.Created!=entry.Created||before.Volume64!=RootProof.Handle.VolumeSerial64
            ||before.Volume32!=RootProof.Handle.LegacyVolumeSerial||parentProof.Volume64!=before.Volume64||parentProof.Volume32!=before.Volume32)
            throw new BootstrapCaptureDeferredException("Original cycle file incarnation changed; await native reconciliation.");
        if(before.Size>maximumBytes)throw new BootstrapCaptureDeferredException("Stable source exceeds provisional admitted spool budget.");
        var usn=ReadLastFileUsn(handle,before);if(_observer is not null)await _observer(NativeCaptureStage.GuardOpened,ct);
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);var created=false;
        try
        {
            await using var spool=new FileStream(path,FileMode.CreateNew,FileAccess.Write,FileShare.None,128*1024,FileOptions.Asynchronous|FileOptions.SequentialScan);
            created=true;using var digest=IncrementalHash.CreateHash(HashAlgorithmName.SHA256);var buffer=new byte[128*1024];long copied=0;
            while(true){var count=await source.ReadAsync(buffer,ct);if(count==0)break;copied=checked(copied+count);if(copied>before.Size||copied>maximumBytes)throw new IOException("Source exceeded sealed admission.");digest.AppendData(buffer,0,count);await spool.WriteAsync(buffer.AsMemory(0,count),ct);}
            var after=ReadIdentity(handle,false);ValidateRoot(root);WindowsBootstrapSourceCapture.RequireHandlePath(parent,parentPath);WindowsBootstrapSourceCapture.RequireHandlePath(handle,full);RequireExactLeaf(handle,Path.GetFileName(full));
            if(copied!=before.Size||before!=after||ReadIdentity(parent,true).Witness!=parentProof.Witness||ReadLastFileUsn(handle,after)!=usn)
                throw new IOException("The same guarded native version changed during capture.");
            await spool.FlushAsync(ct);spool.Flush(true);if(_observer is not null)await _observer(NativeCaptureStage.SpoolFlushed,ct);ct.ThrowIfCancellationRequested();
            var hash=Convert.ToHexStringLower(digest.GetHashAndReset());
            var evidence=new LocalCycleCaptureEvidence("neptune.native-cycle-capture.v1",work.ScopeId,work.OperationId,entry.SourceNodeId,entry.Created,
                entry.ParentId,entry.RelativePath,entry.Generation.Journal,entry.Generation.Watcher,RootProof.Handle.FileId128,RootProof.Handle.CreationTime,
                RootProof.VolumeGuid,RootProof.Handle.LegacyVolumeSerial.ToString("x8",CultureInfo.InvariantCulture),before.LegacyFileId64,
                copied.ToString(CultureInfo.InvariantCulture),hash,usn.ToString(CultureInfo.InvariantCulture),Guid.NewGuid().ToString("N"));
            var json=JsonSerializer.Serialize(evidence,LocalCycleWire.Json);ValidatePersisted(work,copied,hash,json,RootProof);
            return new(path,copied,hash,json);
        }
        catch(IOException error)when(!created&&(error.HResult&0xffff)is 80 or 183)
        {
            // A process can die after CreateNew but before the seal is durable.
            // This work still has no immutable intent. The resource owner joins
            // and releases the incomplete capture; retry the same queued work
            // through the normal source-busy schedule instead of failing it forever.
            throw new BootstrapCaptureDeferredException("Interrupted unsealed capture awaits owned spool cleanup and retry.");
        }
        catch{if(created&&File.Exists(path))File.Delete(path);throw;}
    }
    internal static void ValidatePersisted(LocalCycleWork work,long size,string hash,string proof,LocalNamespaceRootProof root)
    {
        if(System.Text.Encoding.UTF8.GetByteCount(proof)>32*1024)throw new InvalidDataException("Cycle seal proof exceeds bound.");
        using var d=Sync.Protocol.SyncProtocolReader.Parse(System.Text.Encoding.UTF8.GetBytes(proof));
        string[] fields=["schema","scopeId","operationId","nodeId","created","parentId","relativePath","journalGeneration","watcherGeneration","rootFileId","rootCreated","volumeGuid","legacyVolumeSerial","fileId64","sizeBytes","sha256","lastFileUsn","nonce"];
        if(d.RootElement.ValueKind!=JsonValueKind.Object||d.RootElement.EnumerateObject().Count()!=fields.Length||d.RootElement.EnumerateObject().Any(p=>!fields.Contains(p.Name,StringComparer.Ordinal)))throw new InvalidDataException("Cycle seal has unknown/missing fields.");
        var e=JsonSerializer.Deserialize<LocalCycleCaptureEvidence>(proof,LocalCycleWire.Json)??throw new InvalidDataException("Cycle seal is absent.");var w=work.Entry;
        if(e.Schema!="neptune.native-cycle-capture.v1"||e.ScopeId!=work.ScopeId||e.OperationId!=work.OperationId||!LocalCycleWire.Uuid(e.OperationId)
            ||e.NodeId!=w.SourceNodeId||e.Created!=w.Created||e.ParentId!=w.ParentId||e.RelativePath!=w.RelativePath||e.JournalGeneration!=w.Generation.Journal||e.WatcherGeneration!=w.Generation.Watcher
            ||e.RootFileId!=root.Handle.FileId128||e.RootCreated!=root.Handle.CreationTime||e.VolumeGuid!=root.VolumeGuid||e.LegacyVolumeSerial!=root.Handle.LegacyVolumeSerial.ToString("x8",CultureInfo.InvariantCulture)
            ||e.SizeBytes!=size.ToString(CultureInfo.InvariantCulture)||e.Sha256!=hash||!LocalCycleWire.Hash(hash)||e.FileId64.Length!=16||e.FileId64.Any(c=>c is not (>= '0' and <= '9' or >= 'a' and <= 'f'))
            ||!Guid.TryParseExact(e.Nonce,"N",out var nonce)||nonce==Guid.Empty||LocalCycleWire.Decimal(e.LastFileUsn)<0)
            throw new InvalidDataException("Historical cycle seal cannot rebind source/generation/bytes.");
    }
    private string Resolve(string relative)
    {
        if(relative.Length is <1 or >4096||Path.IsPathRooted(relative)||relative.Contains('\\')||relative.Split('/').Any(p=>p.Length==0||p is "." or ".."||p.Contains(':')))
            throw new InvalidDataException("Cycle capture requires a bounded exact relative path.");
        var path=Path.GetFullPath(Path.Combine(RootProof.RootPath,relative.Replace('/',Path.DirectorySeparatorChar)));
        if(path.Length>4096||!path.StartsWith(RootProof.RootPath+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))throw new InvalidDataException("Cycle path escaped source root.");return path;
    }
    private void ValidateRoot(SafeFileHandle h)
    {
        WindowsBootstrapSourceCapture.RequireHandlePath(h,RootProof.RootPath);var p=ReadIdentity(h,true);var e=RootProof.Handle;
        if(p.FileId128!=e.FileId128||p.Created!=e.CreationTime||p.Volume64!=e.VolumeSerial64||p.Volume32!=e.LegacyVolumeSerial||p.CaseSensitive!=e.CaseSensitive)throw new IOException("Original native cycle root changed.");
    }
    private sealed record Identity(string FileId128,string LegacyFileId64,string Volume64,uint Volume32,long Created,long Write,long Change,long Size,uint Attributes,uint Links,bool CaseSensitive)
    {internal (string,string,string,uint,long,uint,bool) Witness=>(FileId128,LegacyFileId64,Volume64,Volume32,Created,Attributes,CaseSensitive);}
    private static Identity ReadIdentity(SafeFileHandle h,bool directory)
    {
        var id=Info(h,18,24);var basic=Info(h,0,40);var standard=Info(h,1,24);if(!Native.GetFileInformationByHandle(h,out var old))throw Failure();var attrs=BinaryPrimitives.ReadUInt32LittleEndian(basic.AsSpan(32));
        if(((attrs&0x10)!=0)!=directory||(attrs&(0x400u|0x1000u|0x40000u|0x400000u))!=0)throw new IOException("Unsupported native cycle source policy/type.");
        var sensitive=directory?BinaryPrimitives.ReadUInt32LittleEndian(Info(h,23,4)):0;if((sensitive&~1u)!=0)throw new IOException("Unknown native case mode.");
        if(!directory&&old.Links!=1)throw new BootstrapCaptureDeferredException("Multiply linked source requires explicit alias reconciliation.");var legacy=new byte[8];BinaryPrimitives.WriteUInt64LittleEndian(legacy,((ulong)old.IndexHigh<<32)|old.IndexLow);
        return new(Convert.ToHexStringLower(id.AsSpan(8,16)),Convert.ToHexStringLower(legacy),BinaryPrimitives.ReadUInt64LittleEndian(id).ToString("x16",CultureInfo.InvariantCulture),old.Volume,
            BinaryPrimitives.ReadInt64LittleEndian(basic),BinaryPrimitives.ReadInt64LittleEndian(basic.AsSpan(16)),BinaryPrimitives.ReadInt64LittleEndian(basic.AsSpan(24)),directory?0:BinaryPrimitives.ReadInt64LittleEndian(standard.AsSpan(8)),attrs,old.Links,sensitive!=0);
    }
    private static long ReadLastFileUsn(SafeFileHandle h,Identity identity)
    {
        byte[] input=[2,0,3,0];var output=new byte[1024];if(!Native.DeviceIoControl(h,0x000900ebu,input,input.Length,output,output.Length,out var returned,nint.Zero))throw Failure();
        if(returned<60||returned>output.Length)throw new InvalidDataException("Truncated native file USN.");var s=output.AsSpan(0,returned);var major=BinaryPrimitives.ReadUInt16LittleEndian(s[4..]);
        if(BinaryPrimitives.ReadUInt32LittleEndian(s)!=returned||BinaryPrimitives.ReadUInt16LittleEndian(s[6..])!=0||major is not (2 or 3)||returned<(major==2?60:76)
            ||Convert.ToHexStringLower(s.Slice(8,major==2?8:16))!=(major==2?identity.LegacyFileId64:identity.FileId128))throw new InvalidDataException("File USN identity/version mismatch.");
        var usn=BinaryPrimitives.ReadInt64LittleEndian(s[(major==2?24:40)..]);if(usn<0)throw new InvalidDataException("Invalid historical file USN.");return usn;
    }
    private static void RequireExactLeaf(SafeFileHandle h,string name)
    {var path=new char[4096];var count=Native.GetFinalPathNameByHandleW(h,path,(uint)path.Length,0);if(count==0||count>=path.Length)throw Failure();if(Path.GetFileName(new string(path,0,(int)count))!=name)throw new IOException("Native exact leaf spelling changed.");}
    private static SafeFileHandle Open(string path,uint access,uint share)
    {var h=Native.CreateFileW(path,access,share,nint.Zero,3,0x02000000u|0x00200000u|0x00100000u,nint.Zero);if(!h.IsInvalid)return h;var error=Marshal.GetLastWin32Error();h.Dispose();throw new Win32Exception(error);}
    private static byte[] Info(SafeFileHandle h,int kind,int size){var data=new byte[size];if(!Native.GetFileInformationByHandleEx(h,kind,data,(uint)size))throw Failure();return data;}
    private static Win32Exception Failure()=>new(Marshal.GetLastWin32Error(),"Readonly native cycle query failed.");
    public ValueTask DisposeAsync()=>_root.DisposeAsync();
    [StructLayout(LayoutKind.Sequential)]private readonly record struct Time(uint Low,uint High);
    [StructLayout(LayoutKind.Sequential)]private readonly record struct Legacy(uint Attributes,Time Created,Time Accessed,Time Write,uint Volume,uint SizeHigh,uint SizeLow,uint Links,uint IndexHigh,uint IndexLow);
    private static class Native
    {
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]internal static extern SafeFileHandle CreateFileW(string path,uint access,uint share,nint security,uint disposition,uint flags,nint template);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetFileInformationByHandleEx(SafeFileHandle h,int kind,[Out]byte[] data,uint size);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetFileInformationByHandle(SafeFileHandle h,out Legacy data);
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]internal static extern uint GetFinalPathNameByHandleW(SafeFileHandle h,[Out]char[] path,uint size,uint flags);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool DeviceIoControl(SafeFileHandle h,uint code,byte[] input,int inputSize,[Out]byte[] output,int outputSize,out int returned,nint overlapped);
    }
}
