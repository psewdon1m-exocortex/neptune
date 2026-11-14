using System.Buffers.Binary;
using System.ComponentModel;
using System.Globalization;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Neptune.Core;

// Read-only second native directory class. The coordinator holds the original
// root and ID128 reader concurrently; this handle must match that exact proof.
public sealed class WindowsLocalDirectoryAliasSource(LocalNamespaceRootProof root):ILocalDirectoryAliasSource
{
    public ValueTask<ILocalDirectoryAliasReader> OpenAsync(LocalNamespaceDirectory parent,LocalNamespaceHandleProof expected,CancellationToken cancellationToken=default)
    {
        cancellationToken.ThrowIfCancellationRequested();if(!OperatingSystem.IsWindows())throw new PlatformNotSupportedException();
        var parts=parent.RelativePath.Split('/',StringSplitOptions.RemoveEmptyEntries);
        if(parts.Length>512||parts.Any(p=>p is "." or ".."||p.IndexOfAny(['\\','\0'])>=0))throw new InvalidDataException("Alias ancestry is invalid.");
        var path=Path.GetFullPath(Path.Combine(root.RootPath,parent.RelativePath.Replace('/',Path.DirectorySeparatorChar)));
        if(path.Length>4096||path!=root.RootPath&&!path.StartsWith(root.RootPath+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))throw new InvalidDataException("Alias parent escaped root.");
        var ancestry=WindowsBootstrapSourceCapture.HoldDirectoryPath(path);SafeFileHandle? handle=null;
        try
        {
            handle=Native.CreateFileW(path,0x81,3,nint.Zero,3,0x02000000|0x00200000,nint.Zero);
            if(handle.IsInvalid)throw new Win32Exception(Marshal.GetLastWin32Error());
            WindowsBootstrapSourceCapture.RequireHandlePath(handle,path);var proof=ReadProof(handle);
            if(proof!=expected||proof.FileId128!=parent.FileId128||proof.CreationTime!=parent.CreationTime
                ||proof.VolumeSerial64!=root.Handle.VolumeSerial64||(proof.Attributes&0x10)==0||(proof.Attributes&0x400)!=0)
                throw new IOException("Alias native parent differs from the concurrent ID128 reader.");
            return ValueTask.FromResult<ILocalDirectoryAliasReader>(new Reader(ancestry,handle,path,proof));
        }
        catch{handle?.Dispose();ancestry.Dispose();throw;}
    }
    public ValueTask DisposeAsync()=>ValueTask.CompletedTask;
    private sealed class Reader(IDisposable ancestry,SafeFileHandle handle,string path,LocalNamespaceHandleProof proof):ILocalDirectoryAliasReader
    {
        private bool _first=true,_end,_disposed;private readonly byte[] _buffer=new byte[WindowsDirectoryAliasParser.MaximumBufferBytes];
        private readonly(long Write,long Change) _stamp=Stamp(handle);
        public LocalNamespaceHandleProof Proof=>proof;
        public ValueTask<LocalDirectoryAliasPage> ReadPageAsync(CancellationToken cancellationToken=default)
        {
            cancellationToken.ThrowIfCancellationRequested();Validate();if(_end)return ValueTask.FromResult(new LocalDirectoryAliasPage([],true));Array.Clear(_buffer);
            if(!Native.GetFileInformationByHandleEx(handle,_first?11:10,_buffer,(uint)_buffer.Length))
            {var error=Marshal.GetLastWin32Error();if(error!=18)throw new Win32Exception(error,"Readonly alias enumeration failed.");_end=true;Validate();return ValueTask.FromResult(new LocalDirectoryAliasPage([],true));}
            _first=false;var entries=WindowsDirectoryAliasParser.Parse(_buffer);Validate();return ValueTask.FromResult(new LocalDirectoryAliasPage(entries,false));
        }
        private void Validate(){ObjectDisposedException.ThrowIf(_disposed,this);WindowsBootstrapSourceCapture.RequireHandlePath(handle,path);if(ReadProof(handle)!=proof||Stamp(handle)!=_stamp)throw new LocalSourceGapException("ParentChangedDuringClosure","Alias parent changed while enumerating.");}
        public ValueTask ValidateUnchangedAsync(CancellationToken cancellationToken=default){cancellationToken.ThrowIfCancellationRequested();Validate();return ValueTask.CompletedTask;}
        public ValueTask DisposeAsync(){if(!_disposed){_disposed=true;handle.Dispose();ancestry.Dispose();}return ValueTask.CompletedTask;}
    }
    private static byte[] Info(SafeFileHandle handle,int type,int length){var b=new byte[length];if(!Native.GetFileInformationByHandleEx(handle,type,b,(uint)length))throw new Win32Exception(Marshal.GetLastWin32Error());return b;}
    private static(long Write,long Change) Stamp(SafeFileHandle handle){var b=Info(handle,0,40);return(BinaryPrimitives.ReadInt64LittleEndian(b.AsSpan(16)),BinaryPrimitives.ReadInt64LittleEndian(b.AsSpan(24)));}
    private static LocalNamespaceHandleProof ReadProof(SafeFileHandle handle)
    {
        var id=Info(handle,18,24);var basic=Info(handle,0,40);var sensitivity=Info(handle,23,4);var flags=BinaryPrimitives.ReadUInt32LittleEndian(sensitivity);
        if((flags&~1u)!=0)throw new InvalidDataException("Unknown alias parent case mode.");
        if(!Native.GetFileInformationByHandle(handle,out var old))throw new Win32Exception(Marshal.GetLastWin32Error());
        var wire=new byte[8];BinaryPrimitives.WriteUInt64LittleEndian(wire,((ulong)old.IndexHigh<<32)|old.IndexLow);
        return new(Convert.ToHexStringLower(id.AsSpan(8,16)),Convert.ToHexStringLower(wire),BinaryPrimitives.ReadUInt64LittleEndian(id).ToString("x16",CultureInfo.InvariantCulture),old.Volume,BinaryPrimitives.ReadInt64LittleEndian(basic),BinaryPrimitives.ReadUInt32LittleEndian(basic.AsSpan(32)),flags!=0);
    }
    [StructLayout(LayoutKind.Sequential)]private readonly record struct FileTime(uint Low,uint High);
    [StructLayout(LayoutKind.Sequential)]private readonly record struct LegacyInfo(uint Attributes,FileTime Created,FileTime Accessed,FileTime Write,uint Volume,uint SizeHigh,uint SizeLow,uint Links,uint IndexHigh,uint IndexLow);
    private static class Native
    {
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]internal static extern SafeFileHandle CreateFileW(string path,uint access,uint share,nint security,uint creation,uint flags,nint template);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetFileInformationByHandleEx(SafeFileHandle handle,int info,[Out]byte[] output,uint size);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetFileInformationByHandle(SafeFileHandle handle,out LegacyInfo info);
    }
}
