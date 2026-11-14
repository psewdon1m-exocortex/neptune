using System.Buffers.Binary;
using System.ComponentModel;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace Neptune.Core;

// Read-only metadata handles. No journal, attribute, case-mode, hydration or
// privilege mutation is implemented. Holding ancestor handles without delete
// sharing fences rename/reparse swaps; final path + exact IDs are checked too.
public sealed class WindowsLocalNamespaceSource : ILocalNamespaceSource
{
    private readonly IDisposable _ancestry; private readonly SafeFileHandle _root=null!;
    private bool _disposed;
    public LocalNamespaceRootProof RootProof { get; }
    private WindowsLocalNamespaceSource(string sourceRoot)
    {
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException("Native namespace metadata requires Windows.");
        sourceRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(sourceRoot));
        if (sourceRoot.Length > 4096 || sourceRoot.StartsWith("\\\\",StringComparison.Ordinal)) throw new InvalidDataException("Only bounded local NTFS roots are qualified.");
        _ancestry=WindowsBootstrapSourceCapture.HoldDirectoryPath(sourceRoot);
        try
        {
            _root=OpenHandle(sourceRoot);
            var proof=ReadProof(_root); RequireDirectory(proof);
            WindowsBootstrapSourceCapture.RequireHandlePath(_root,sourceRoot);
            var mount=new StringBuilder(4096); var volume=new StringBuilder(128); var fs=new StringBuilder(64);
            if(!Native.GetVolumePathNameW(sourceRoot,mount,(uint)mount.Capacity)
                || !Native.GetVolumeNameForVolumeMountPointW(mount.ToString(),volume,(uint)volume.Capacity)
                || !Native.GetVolumeInformationW(mount.ToString(),null,0,out var serial,out _,out _,fs,fs.Capacity)) throw Failure();
            var text=volume.ToString();
            if(text.Length!=49 || !text.StartsWith("\\\\?\\Volume{",StringComparison.Ordinal) || !Guid.TryParseExact(text.Substring(11,36),"D",out var guid)
                || fs.ToString()!="NTFS" || proof.LegacyVolumeSerial!=serial) throw new InvalidDataException("Namespace root volume is not an exact supported NTFS identity.");
            RootProof=new(sourceRoot,guid.ToString("D"),fs.ToString(),proof);
        }
        catch { _root?.Dispose(); _ancestry.Dispose(); throw; }
    }
    public static ValueTask<WindowsLocalNamespaceSource> OpenAsync(string sourceRoot,CancellationToken cancellationToken=default)
    { cancellationToken.ThrowIfCancellationRequested(); return ValueTask.FromResult(new WindowsLocalNamespaceSource(sourceRoot)); }
    private void VerifyRoot()
    {
        ObjectDisposedException.ThrowIf(_disposed,this);
        WindowsBootstrapSourceCapture.RequireHandlePath(_root,RootProof.RootPath);
        var now=ReadProof(_root);
        if(now.FileId128!=RootProof.Handle.FileId128 || now.CreationTime!=RootProof.Handle.CreationTime
            || now.VolumeSerial64!=RootProof.Handle.VolumeSerial64 || now.LegacyVolumeSerial!=RootProof.Handle.LegacyVolumeSerial)
            throw new IOException("The namespace root identity changed.");
        RequireDirectory(now);
    }
    public ValueTask<ILocalNamespaceDirectoryReader> OpenDirectoryAsync(LocalNamespaceDirectory directory,CancellationToken cancellationToken=default)
    {
        cancellationToken.ThrowIfCancellationRequested(); VerifyRoot();
        var parts=directory.RelativePath.Split('/',StringSplitOptions.RemoveEmptyEntries);
        if(parts.Length>512 || parts.Any(x=>x is "." or ".." || x.IndexOfAny(['\\','\0'])>=0)) throw new InvalidDataException("Unbounded or invalid namespace ancestry.");
        var full=Path.GetFullPath(Path.Combine(RootProof.RootPath,directory.RelativePath.Replace('/',Path.DirectorySeparatorChar)));
        if(full.Length>4096 || full!=RootProof.RootPath && !full.StartsWith(RootProof.RootPath+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("Namespace directory escaped the root.");
        var ancestry=WindowsBootstrapSourceCapture.HoldDirectoryPath(full);
        SafeFileHandle? handle=null;
        try
        {
            handle=OpenHandle(full);
            WindowsBootstrapSourceCapture.RequireHandlePath(handle,full); var proof=ReadProof(handle); RequireDirectory(proof);
            if(proof.FileId128!=directory.FileId128 || proof.CreationTime!=directory.CreationTime
                || proof.VolumeSerial64!=RootProof.Handle.VolumeSerial64 || proof.LegacyVolumeSerial!=RootProof.Handle.LegacyVolumeSerial)
                throw new IOException("The opened directory differs from its enumerated identity.");
            VerifyRoot(); return ValueTask.FromResult<ILocalNamespaceDirectoryReader>(new Reader(this,ancestry,handle,full,proof));
        }
        catch {handle?.Dispose();ancestry.Dispose();throw;}
    }
    public ValueTask DisposeAsync()
    {if(!_disposed){_disposed=true;_root.Dispose();_ancestry.Dispose();}return ValueTask.CompletedTask;}
    private sealed class Reader(WindowsLocalNamespaceSource source,IDisposable ancestry,SafeFileHandle handle,string path,LocalNamespaceHandleProof proof):ILocalNamespaceDirectoryReader
    {
        private bool _first=true,_end,_disposed; private readonly byte[] _buffer=new byte[WindowsDirectoryPageParser.MaximumBufferBytes];
        private readonly (long Write,long Change) _stamp=ReadStamp(handle);
        public LocalNamespaceHandleProof Proof=>proof;
        public ValueTask<LocalNamespacePage> ReadPageAsync(CancellationToken cancellationToken=default)
        {
            cancellationToken.ThrowIfCancellationRequested(); Validate();
            if(_end)return ValueTask.FromResult(new LocalNamespacePage(Array.Empty<LocalNamespaceEntry>(),true));
            Array.Clear(_buffer);
            if(!Native.GetFileInformationByHandleEx(handle,_first?20:19,_buffer,(uint)_buffer.Length))
            {
                var error=Marshal.GetLastWin32Error();
                if(error!=18)throw new Win32Exception(error,"Readonly directory enumeration failed.");
                _end=true;Validate();return ValueTask.FromResult(new LocalNamespacePage(Array.Empty<LocalNamespaceEntry>(),true));
            }
            _first=false;var entries=WindowsDirectoryPageParser.Parse(_buffer);Validate();
            cancellationToken.ThrowIfCancellationRequested();return ValueTask.FromResult(new LocalNamespacePage(entries,false));
        }
        private void Validate()
        {
            ObjectDisposedException.ThrowIf(_disposed,this);source.VerifyRoot();WindowsBootstrapSourceCapture.RequireHandlePath(handle,path);
            var now=ReadProof(handle);RequireDirectory(now);
            if(now!=proof || ReadStamp(handle)!=_stamp)throw new LocalSourceGapException("ParentChangedDuringClosure","Directory metadata changed during candidate enumeration; it is not a continuous cut.");
        }
        public ValueTask ValidateUnchangedAsync(CancellationToken cancellationToken=default)
        {cancellationToken.ThrowIfCancellationRequested();Validate();return ValueTask.CompletedTask;}
        public ValueTask DisposeAsync(){if(!_disposed){_disposed=true;handle.Dispose();ancestry.Dispose();}return ValueTask.CompletedTask;}
    }
    private static SafeFileHandle OpenHandle(string path)
    {
        // FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES, read/write sharing only.
        var h=Native.CreateFileW(path,0x81,3,nint.Zero,3,0x02000000|0x00200000,nint.Zero);
        if(h.IsInvalid){var e=Marshal.GetLastWin32Error();h.Dispose();throw new Win32Exception(e);}return h;
    }
    private static void RequireDirectory(LocalNamespaceHandleProof proof)
    {if((proof.Attributes&0x10)==0 || (proof.Attributes&0x400)!=0)throw new IOException("Namespace parent is not an ordinary directory.");}
    private static byte[] Info(SafeFileHandle handle,int information,int size)
    {var bytes=new byte[size];if(!Native.GetFileInformationByHandleEx(handle,information,bytes,(uint)size))throw Failure();return bytes;}
    private static (long Write,long Change) ReadStamp(SafeFileHandle handle)
    {var b=Info(handle,0,40);return(BinaryPrimitives.ReadInt64LittleEndian(b.AsSpan(16)),BinaryPrimitives.ReadInt64LittleEndian(b.AsSpan(24)));}
    private static LocalNamespaceHandleProof ReadProof(SafeFileHandle handle)
    {
        var id=Info(handle,18,24);var basic=Info(handle,0,40);var sensitivity=Info(handle,23,4);
        var flags=BinaryPrimitives.ReadUInt32LittleEndian(sensitivity);
        if((flags&~1u)!=0)throw new InvalidDataException("Unknown native case-sensitivity mode.");
        if(!Native.GetFileInformationByHandle(handle,out var old))throw Failure();
        var legacy=new byte[8];BinaryPrimitives.WriteUInt64LittleEndian(legacy,((ulong)old.IndexHigh<<32)|old.IndexLow);
        return new(Convert.ToHexStringLower(id.AsSpan(8,16)),Convert.ToHexStringLower(legacy),
            BinaryPrimitives.ReadUInt64LittleEndian(id).ToString("x16",CultureInfo.InvariantCulture),old.Volume,
            BinaryPrimitives.ReadInt64LittleEndian(basic),BinaryPrimitives.ReadUInt32LittleEndian(basic.AsSpan(32)),flags!=0);
    }
    private static Win32Exception Failure()=>new(Marshal.GetLastWin32Error(),"Readonly namespace metadata query failed.");
    [StructLayout(LayoutKind.Sequential)] private readonly record struct FileTime(uint Low,uint High);
    [StructLayout(LayoutKind.Sequential)] private readonly record struct LegacyInfo(uint Attributes,FileTime Created,FileTime Accessed,FileTime Write,uint Volume,uint SizeHigh,uint SizeLow,uint Links,uint IndexHigh,uint IndexLow);
    private static class Native
    {
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]internal static extern SafeFileHandle CreateFileW(string path,uint access,uint share,nint security,uint creation,uint flags,nint template);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetFileInformationByHandleEx(SafeFileHandle handle,int information,[Out]byte[] buffer,uint size);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetFileInformationByHandle(SafeFileHandle handle,out LegacyInfo info);
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetVolumePathNameW(string path,StringBuilder mount,uint length);
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetVolumeNameForVolumeMountPointW(string mount,StringBuilder volume,uint length);
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetVolumeInformationW(string root,StringBuilder? label,int labelLength,out uint serial,out uint maximumComponent,out uint flags,StringBuilder fs,int fsLength);
    }
}
