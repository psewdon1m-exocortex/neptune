using System.Buffers.Binary;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace Neptune.Core;

// A current, native-only absence witness. Never reclassifies historical UNKNOWN
// records as Outside and never supplies a baseline or authorizes a deletion.
internal sealed class WindowsOutsideHardlinkProof : IDisposable
{
    internal sealed class CapacityException(string message) : IOException(message);
    internal sealed record Claim(ushort Major,string Wire,long LastEventUsn,bool ClosedDeleted=false);
    private sealed record FileWitness(Claim Claim,SafeFileHandle? Handle,long Usn,uint Links,
        IReadOnlyList<(SafeFileHandle Handle,string Path)> Aliases,IReadOnlyList<IDisposable> Parents);
    private readonly List<FileWitness> _files=[];
    private readonly SafeFileHandle _volume;
    private readonly LocalNamespaceRootProof _root;
    private bool _disposed;
    private WindowsOutsideHardlinkProof(LocalNamespaceRootProof root)
    {
        _root=root;
        _volume=Native.CreateFileW(@"\\?\Volume{"+root.VolumeGuid+"}",0,7,nint.Zero,3,0,nint.Zero);
        if(_volume.IsInvalid)throw Failure();
    }
    internal static WindowsOutsideHardlinkProof Open(LocalNamespaceRootProof root,IReadOnlyList<Claim> claims,CancellationToken ct)
    {
        if(!OperatingSystem.IsWindows()||claims.Count is <1 or >128)throw new InvalidOperationException("Bounded native hardlink claims required.");
        var proof=new WindowsOutsideHardlinkProof(root);
        try{foreach(var claim in claims){ct.ThrowIfCancellationRequested();proof.Add(claim);}proof.Validate(ct);return proof;}
        catch{proof.Dispose();throw;}
    }
    private SafeFileHandle ById(Claim claim)
    {
        var bytes=Convert.FromHexString(claim.Wire);if(claim.Major is not(2 or 3)||bytes.Length!=(claim.Major==3?16:8))throw new InvalidDataException("Unknown native identity.");
        var id=new IdDescriptor{Size=24,Type=claim.Major==3?2:0,Low=BinaryPrimitives.ReadUInt64LittleEndian(bytes),High=claim.Major==3?BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8)):0};
        return Native.OpenFileById(_volume,ref id,0x80,7,nint.Zero,0x00200000|0x02000000);
    }
    private void Add(Claim claim)
    {
        var h=ById(claim);var aliases=new List<(SafeFileHandle,string)>();var parents=new List<IDisposable>();
        try
        {
            if(h.IsInvalid)
            {
                var error=Marshal.GetLastWin32Error();h.Dispose();
                // NTFS also returns INVALID_PARAMETER for a retired file ID.
                // That code alone is never absence: require a hash-validated
                // terminal DELETE|CLOSE in the retained journal for this exact
                // identity, with no later record through the applied cut.
                if(!Absent(error,claim))throw new Win32Exception(error);
                _files.Add(new(claim,null,0,0,aliases,parents));return;
            }
            var info=Identity(h,claim);var usn=ReadUsn(h,claim);
            if(info.Links>256)throw new CapacityException("Native hardlink alias budget requires full membership proof.");
            if(usn<claim.LastEventUsn||info.Links<1)throw new IOException("Native hardlink evidence does not cover the retained event.");
            var first=FinalPath(h);uint length=4096;var name=new StringBuilder((int)length);
            var find=Native.FindFirstFileNameW(first,0,ref length,name);
            if(find==new nint(-1))throw Failure();
            var names=new HashSet<string>(StringComparer.Ordinal);
            try
            {
                do
                {
                    if(names.Count>=256||!name.ToString().StartsWith('\\'))throw new IOException("Unbounded or non-volume-relative hardlink name.");
                    var candidate=Path.GetFullPath(Path.GetPathRoot(_root.RootPath)!.TrimEnd('\\')+name);
                    if(candidate.Length>4096||!names.Add(candidate))throw new IOException("Ambiguous hardlink enumeration.");
                    parents.Add(WindowsBootstrapSourceCapture.HoldDirectoryPath(Path.GetDirectoryName(candidate)!));
                    var link=Native.CreateFileW(candidate,0x80,7,nint.Zero,3,0x00200000|0x02000000,nint.Zero);
                    if(link.IsInvalid){link.Dispose();throw Failure();}
                    aliases.Add((link,candidate));Identity(link,claim);WindowsBootstrapSourceCapture.RequireHandlePath(link,candidate);
                    var actual=FinalPath(link);
                    if(actual.Equals(_root.RootPath,StringComparison.OrdinalIgnoreCase)||actual.StartsWith(_root.RootPath+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))
                        throw new IOException("Hardlink has current inside membership.");
                    length=4096;name.Clear();
                    if(Native.FindNextFileNameW(find,ref length,name))continue;
                    if(Marshal.GetLastWin32Error()!=38)throw Failure();
                    break;
                }while(true);
            }
            finally{Native.FindClose(find);}
            if(names.Count!=info.Links)throw new IOException("Incomplete hardlink membership.");
            if(_files.Sum(x=>x.Aliases.Count)+aliases.Count>4096)throw new CapacityException("Native hardlink witness budget requires full membership proof.");
            _files.Add(new(claim,h,usn,info.Links,aliases,parents));
        }
        catch{h.Dispose();foreach(var (handle,_) in aliases)handle.Dispose();foreach(var parent in parents)parent.Dispose();throw;}
    }
    internal void Validate(CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed,this);
        foreach(var file in _files)
        {
            ct.ThrowIfCancellationRequested();
            if(file.Handle is null)
            {using var absent=ById(file.Claim);if(!absent.IsInvalid||!Absent(Marshal.GetLastWin32Error(),file.Claim))throw new IOException("Absent identity changed during proof.");continue;}
            var current=Identity(file.Handle,file.Claim);
            if(current.Links!=file.Links||ReadUsn(file.Handle,file.Claim)!=file.Usn)throw new IOException("Hardlink changed during native absence proof.");
            foreach(var (handle,path) in file.Aliases){Identity(handle,file.Claim);WindowsBootstrapSourceCapture.RequireHandlePath(handle,path);}
        }
    }
    private static bool Absent(int error,Claim claim)=>error==2||error==87&&claim.ClosedDeleted;
    internal bool Covers(Claim claim)=>!_disposed&&_files.Any(x=>x.Claim==claim);
    internal object Evidence=>_files.Select(x=>new{x.Claim.Major,x.Claim.Wire,x.Claim.LastEventUsn,x.Claim.ClosedDeleted,x.Usn,x.Links,Absent=x.Handle is null,
        PathsSha256=Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(string.Join('\n',x.Aliases.Select(a=>a.Path).Order(StringComparer.Ordinal)))))}).ToArray();
    private Legacy Identity(SafeFileHandle h,Claim claim)
    {
        var id=Info(h,18,24);if(!Native.GetFileInformationByHandle(h,out var legacy))throw Failure();
        var actual=claim.Major==3?Convert.ToHexStringLower(id.AsSpan(8,16)):Convert.ToHexStringLower(BitConverter.GetBytes(((ulong)legacy.IndexHigh<<32)|legacy.IndexLow));
        if(actual!=claim.Wire||BinaryPrimitives.ReadUInt64LittleEndian(id).ToString("x16")!=_root.Handle.VolumeSerial64||legacy.Volume!=_root.Handle.LegacyVolumeSerial
            ||(legacy.Attributes&(0x10|0x400|0x1000|0x40000|0x400000))!=0)throw new IOException("Hardlink native identity, volume or ordinary-file policy differs.");
        return legacy;
    }
    private static long ReadUsn(SafeFileHandle handle,Claim claim)
    {
        byte[] input=[2,0,3,0];var output=new byte[1024];
        if(!Native.DeviceIoControl(handle,0x900eb,input,4,output,(uint)output.Length,out var count,nint.Zero))throw Failure();
        if(count<64)throw new InvalidDataException("Truncated native USN observation.");
        var major=BinaryPrimitives.ReadUInt16LittleEndian(output.AsSpan(4));if(major is not(2 or 3))throw new InvalidDataException("Unknown native USN observation.");
        var shift=major==3?16:0;if(count<64+shift)throw new InvalidDataException("Truncated native USN observation.");
        if(major==claim.Major&&Convert.ToHexStringLower(output.AsSpan(8,major==3?16:8))!=claim.Wire)throw new InvalidDataException("Native USN identity changed.");
        // Timestamp/Reason/SourceInfo are undefined for this read-only FSCTL.
        return BinaryPrimitives.ReadInt64LittleEndian(output.AsSpan(24+shift));
    }
    private static string FinalPath(SafeFileHandle h)
    {var b=new StringBuilder(4096);var n=Native.GetFinalPathNameByHandleW(h,b,(uint)b.Capacity,0);if(n==0||n>=b.Capacity)throw Failure();var p=b.ToString();if(!p.StartsWith(@"\\?\",StringComparison.Ordinal)||p.StartsWith(@"\\?\UNC\",StringComparison.OrdinalIgnoreCase))throw new IOException("Nonlocal hardlink path.");return p[4..];}
    private static byte[] Info(SafeFileHandle h,int type,int size){var b=new byte[size];if(!Native.GetFileInformationByHandleEx(h,type,b,(uint)size))throw Failure();return b;}
    private static Win32Exception Failure()=>new(Marshal.GetLastWin32Error(),"Native current hardlink absence could not be proved.");
    public void Dispose(){if(_disposed)return;_disposed=true;foreach(var f in _files){f.Handle?.Dispose();foreach(var (h,_)in f.Aliases)h.Dispose();foreach(var p in f.Parents)p.Dispose();}_volume.Dispose();}
    [StructLayout(LayoutKind.Explicit,Size=24)]private struct IdDescriptor{[FieldOffset(0)]public uint Size;[FieldOffset(4)]public int Type;[FieldOffset(8)]public ulong Low;[FieldOffset(16)]public ulong High;}
    [StructLayout(LayoutKind.Sequential)]private readonly record struct Time(uint Low,uint High);
    [StructLayout(LayoutKind.Sequential)]private readonly record struct Legacy(uint Attributes,Time Created,Time Accessed,Time Written,uint Volume,uint SizeHigh,uint SizeLow,uint Links,uint IndexHigh,uint IndexLow);
    private static class Native
    {
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]internal static extern SafeFileHandle CreateFileW(string path,uint access,uint share,nint security,uint creation,uint flags,nint template);
        [DllImport("kernel32.dll",SetLastError=true)]internal static extern SafeFileHandle OpenFileById(SafeFileHandle volume,ref IdDescriptor id,uint access,uint share,nint security,uint flags);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetFileInformationByHandle(SafeFileHandle h,out Legacy info);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetFileInformationByHandleEx(SafeFileHandle h,int type,[Out]byte[] bytes,uint size);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool DeviceIoControl(SafeFileHandle h,uint code,byte[] input,uint inputLength,[Out]byte[] output,uint outputLength,out uint returned,nint overlap);
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]internal static extern uint GetFinalPathNameByHandleW(SafeFileHandle h,StringBuilder path,uint size,uint flags);
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]internal static extern nint FindFirstFileNameW(string path,uint flags,ref uint length,StringBuilder name);
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool FindNextFileNameW(nint find,ref uint length,StringBuilder name);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool FindClose(nint handle);
    }
}
