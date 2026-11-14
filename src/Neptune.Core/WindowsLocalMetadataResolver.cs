using System.Buffers.Binary;
using System.ComponentModel;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace Neptune.Core;

// Current identity observation only. No content read, journal mutation,
// privilege change, closed-cycle witness or historical outside inference.
public sealed class WindowsLocalMetadataResolver : IAsyncDisposable
{
    private readonly WindowsLocalNamespaceSource _source;
    private WindowsLocalMetadataResolver(WindowsLocalNamespaceSource source)=>_source=source;
    public static async ValueTask<WindowsLocalMetadataResolver> OpenAsync(string root,CancellationToken cancellationToken=default)=>new(await WindowsLocalNamespaceSource.OpenAsync(root,cancellationToken));
    public async ValueTask<LocalProjectionMetadataProof> ResolveAsync(LocalProjectionMetadataClaim claim,CancellationToken cancellationToken=default)
    {
        cancellationToken.ThrowIfCancellationRequested();var actual=_source.RootProof;var expected=claim.Root;
        if(actual.RootPath!=expected.RootPath||actual.VolumeGuid!=expected.VolumeGuid||actual.FileSystem!=expected.FileSystem||actual.Handle.FileId128!=expected.Handle.FileId128||actual.Handle.CreationTime!=expected.Handle.CreationTime||actual.Handle.VolumeSerial64!=expected.Handle.VolumeSerial64||actual.Handle.LegacyVolumeSerial!=expected.Handle.LegacyVolumeSerial)throw new IOException("Metadata resolver root identity differs from the event claim.");
        WindowsDirectoryPageParser.ValidateEntry(new(claim.Record.Name,"01000000000000000000000000000000",0,0,0,0,0x20,0));
        await using var parent=await _source.OpenDirectoryAsync(claim.Parent,cancellationToken);
        var directory=Path.Combine(actual.RootPath,claim.Parent.RelativePath.Replace('/',Path.DirectorySeparatorChar));var full=Path.GetFullPath(Path.Combine(directory,claim.Record.Name));
        if(full.Length>4096||!full.StartsWith(actual.RootPath+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))throw new InvalidDataException("Metadata claim escaped its root.");
        using var handle=Native.CreateFileW(full,0x80,3,nint.Zero,3,0x02000000|0x00200000|0x00100000,nint.Zero);
        if(handle.IsInvalid)throw Failure();WindowsBootstrapSourceCapture.RequireHandlePath(handle,full);
        var resolved=new StringBuilder(4096);var length=Native.GetFinalPathNameByHandleW(handle,resolved,(uint)resolved.Capacity,0);
        if(length==0||length>=resolved.Capacity||Path.GetFileName(resolved.ToString())!=claim.Record.Name)throw new IOException("Current exact leaf name differs from historical event.");
        var id=Info(handle,18,24);var basic=Info(handle,0,40);if(!Native.GetFileInformationByHandle(handle,out var old))throw Failure();
        var legacy=new byte[8];BinaryPrimitives.WriteUInt64LittleEndian(legacy,((ulong)old.IndexHigh<<32)|old.IndexLow);
        var attributes=BinaryPrimitives.ReadUInt32LittleEndian(basic.AsSpan(32));
        if((attributes&(0x400|0x1000|0x40000|0x400000))!=0)throw new IOException("Current reparse/offline/recall object is excluded.");
        var proof=new LocalNamespaceHandleProof(Convert.ToHexStringLower(id.AsSpan(8,16)),Convert.ToHexStringLower(legacy),BinaryPrimitives.ReadUInt64LittleEndian(id).ToString("x16",CultureInfo.InvariantCulture),old.Volume,BinaryPrimitives.ReadInt64LittleEndian(basic),attributes,parent.Proof.CaseSensitive);
        if(proof.VolumeSerial64!=actual.Handle.VolumeSerial64||proof.LegacyVolumeSerial!=actual.Handle.LegacyVolumeSerial||(claim.Record.MajorVersion==2?proof.LegacyFileId64:proof.FileId128)!=claim.Record.FileIdHex)throw new IOException("Current opened object differs from raw wire identity.");
        await parent.ValidateUnchangedAsync(cancellationToken);cancellationToken.ThrowIfCancellationRequested();return new(claim,proof,old.Links);
    }
    public ValueTask DisposeAsync()=>_source.DisposeAsync();
    private static byte[] Info(SafeFileHandle h,int kind,int size){var data=new byte[size];if(!Native.GetFileInformationByHandleEx(h,kind,data,(uint)size))throw Failure();return data;}
    private static Win32Exception Failure()=>new(Marshal.GetLastWin32Error(),"Readonly owned metadata resolution failed.");
    [StructLayout(LayoutKind.Sequential)]private readonly record struct Time(uint Low,uint High);
    [StructLayout(LayoutKind.Sequential)]private readonly record struct Legacy(uint Attributes,Time Created,Time Accessed,Time Written,uint Volume,uint SizeHigh,uint SizeLow,uint Links,uint IndexHigh,uint IndexLow);
    private static class Native
    {
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]internal static extern SafeFileHandle CreateFileW(string path,uint access,uint share,nint security,uint creation,uint flags,nint template);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetFileInformationByHandleEx(SafeFileHandle handle,int kind,[Out]byte[] bytes,uint size);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetFileInformationByHandle(SafeFileHandle handle,out Legacy information);
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]internal static extern uint GetFinalPathNameByHandleW(SafeFileHandle handle,StringBuilder path,uint size,uint flags);
    }
}
