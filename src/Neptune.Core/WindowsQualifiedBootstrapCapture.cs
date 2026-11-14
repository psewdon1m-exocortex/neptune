using System.Buffers.Binary;
using System.ComponentModel;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Microsoft.Win32.SafeHandles;

namespace Neptune.Core;

// Opt-in work-aware capture. Ancestors are guarded and the file denies write
// and delete sharing for the entire copy. No writable journal IOCTL exists.
public sealed class WindowsQualifiedBootstrapCapture : IQualifiedBootstrapSourceCapture, IAsyncDisposable
{
    private readonly WindowsLocalNamespaceSource _root;
    private readonly Func<NativeCaptureStage, CancellationToken, Task>? _observer;
    private bool _disposed;
    public LocalNamespaceRootProof RootProof => _root.RootProof;
    private WindowsQualifiedBootstrapCapture(WindowsLocalNamespaceSource root,
        Func<NativeCaptureStage, CancellationToken, Task>? observer) => (_root, _observer) = (root, observer);
    public static async ValueTask<WindowsQualifiedBootstrapCapture> OpenAsync(string sourceRoot,
        CancellationToken cancellationToken = default, Func<NativeCaptureStage, CancellationToken, Task>? observer = null)
        => new(await WindowsLocalNamespaceSource.OpenAsync(sourceRoot, cancellationToken), observer);

    public Task<BootstrapSealedSource> CaptureAsync(string sourceRoot, string relativePath, string spoolPath,
        long maximumBytes, CancellationToken cancellationToken)
        => throw new InvalidOperationException("Qualified native capture requires its original durable work binding.");

    public async Task ValidateDirectoryAsync(string sourceRoot, string relativePath, CancellationToken cancellationToken)
    {
        RequireRoot(sourceRoot);
        using var ancestry = WindowsBootstrapSourceCapture.HoldDirectoryPath(Path.GetDirectoryName(Resolve(relativePath))!);
        using var root = Open(RootProof.RootPath, 0x80, 3);
        ValidateRoot(root);
        await new WindowsBootstrapSourceCapture().ValidateDirectoryAsync(sourceRoot, relativePath, cancellationToken);
        ValidateRoot(root);
    }

    public async Task<NativeBootstrapSeal> CaptureWorkAsync(BootstrapWorkRecord work, string spoolPath,
        long maximumBytes, CancellationToken cancellationToken)
    {
        RequireRoot(work.SourceRoot);
        cancellationToken.ThrowIfCancellationRequested();
        if (work.IsDirectory || work.Phase != BootstrapWorkPhase.Queued || work.Attempt != 0
            || maximumBytes < 0 || work.EnumeratedSize != maximumBytes)
            throw new InvalidOperationException("Only an unchanged queued file work item may receive a new native seal.");
        var full = Resolve(work.RelativePath);
        var parentPath = Path.GetDirectoryName(full)!;
        using var ancestry = WindowsBootstrapSourceCapture.HoldDirectoryPath(parentPath);
        using var root = Open(RootProof.RootPath, 0x80, 3);
        ValidateRoot(root);
        using var parent = Open(parentPath, 0x80, 3);
        WindowsBootstrapSourceCapture.RequireHandlePath(parent, parentPath);
        var parentProof = ReadIdentity(parent, directory: true);
        SafeFileHandle sourceHandle;
        try { sourceHandle = Open(full, 0x80000000u | 0x80u, 1); }
        catch (Win32Exception e) when (e.NativeErrorCode == 32)
        { throw new BootstrapCaptureDeferredException("An active writer/delete handle or writable mapping prevents qualified source capture."); }
        await using var source = new FileStream(sourceHandle, FileAccess.Read, 128 * 1024, isAsync: false);
        WindowsBootstrapSourceCapture.RequireHandlePath(source.SafeFileHandle, full);
        RequireExactLeaf(source.SafeFileHandle, Path.GetFileName(full));
        var before = ReadIdentity(source.SafeFileHandle, directory: false);
        if (before.Volume64 != parentProof.Volume64 || before.Volume32 != parentProof.Volume32
            || before.Volume64 != RootProof.Handle.VolumeSerial64 || before.Volume32 != RootProof.Handle.LegacyVolumeSerial)
            throw new IOException("The file escaped its qualified volume/root.");
        if (before.Size != maximumBytes) throw new BootstrapCaptureDeferredException("The source size changed after enumeration; a fresh job is required.");
        var lastUsn = ReadLastFileUsn(source.SafeFileHandle, before);
        if (_observer is not null) await _observer(NativeCaptureStage.GuardOpened, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(spoolPath))!);
        var createdSpool = false;
        try
        {
            await using var spool = new FileStream(spoolPath, FileMode.CreateNew, FileAccess.Write, FileShare.None,
                128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
            createdSpool = true;
            using var digest = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            var buffer = new byte[128 * 1024];
            long copied = 0;
            while (true)
            {
                var count = await source.ReadAsync(buffer, cancellationToken);
                if (count == 0) break;
                copied = checked(copied + count);
                if (copied > maximumBytes) throw new BootstrapCaptureDeferredException("The source exceeds the reserved spool.");
                digest.AppendData(buffer, 0, count);
                await spool.WriteAsync(buffer.AsMemory(0, count), cancellationToken);
            }
            var after = ReadIdentity(source.SafeFileHandle, directory: false);
            ValidateRoot(root);
            WindowsBootstrapSourceCapture.RequireHandlePath(parent, parentPath);
            WindowsBootstrapSourceCapture.RequireHandlePath(source.SafeFileHandle, full);
            RequireExactLeaf(source.SafeFileHandle, Path.GetFileName(full));
            if (copied != maximumBytes || after != before || ReadIdentity(parent, directory: true).Witness != parentProof.Witness
                || ReadLastFileUsn(source.SafeFileHandle, after) != lastUsn)
                throw new IOException("The guarded native identity changed during capture.");
            await spool.FlushAsync(cancellationToken); spool.Flush(flushToDisk: true);
            if (_observer is not null) await _observer(NativeCaptureStage.SpoolFlushed, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            var hash = Convert.ToHexStringLower(digest.GetHashAndReset());
            var proof = new NativeBootstrapCaptureEvidence(NativeBootstrapCaptureProof.Schema, work.SessionId, work.ScanId,
                work.ScanGeneration, work.EntryId, work.OperationId, work.MappingId, work.ClientInstanceId!, work.SourceRoot,
                work.RelativePath, work.TargetRoot, work.CredentialScope, work.ExpectedServerRootId, RootProof.VolumeGuid,
                before.Volume64, before.Volume32.ToString("x8", CultureInfo.InvariantCulture), RootProof.Handle.FileId128,
                RootProof.Handle.CreationTime, parentProof.FileId128, parentProof.Created, parentProof.CaseSensitive,
                before.FileId128, before.LegacyFileId64, before.Created, before.Attributes, before.Links,
                copied.ToString(CultureInfo.InvariantCulture), hash, lastUsn.ToString(CultureInfo.InvariantCulture), Guid.NewGuid().ToString("N"));
            var json = NativeBootstrapCaptureProof.Serialize(proof);
            var identity = NativeBootstrapCaptureProof.Identity(json);
            NativeBootstrapCaptureProof.Validate(json, work, copied, hash, identity);
            return new(new(spoolPath, copied, hash, identity), json);
        }
        catch
        {
            if (createdSpool && File.Exists(spoolPath)) File.Delete(spoolPath);
            throw;
        }
    }

    private string Resolve(string relative)
    {
        if (relative.Length is < 1 or > 4096 || Path.IsPathRooted(relative) || relative.Contains('\\')
            || relative.Split('/').Any(p => p.Length == 0 || p is "." or ".." || p.Contains(':')))
            throw new InvalidDataException("Native capture requires a bounded canonical relative path.");
        var full = Path.GetFullPath(Path.Combine(RootProof.RootPath, relative.Replace('/', Path.DirectorySeparatorChar)));
        if (full.Length > 4096 || !full.StartsWith(RootProof.RootPath + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("Native capture path escaped its root.");
        return full;
    }
    private void RequireRoot(string source)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (Path.TrimEndingDirectorySeparator(Path.GetFullPath(source)) != RootProof.RootPath)
            throw new InvalidDataException("Native capture cannot rebind the opened source root.");
    }
    private void ValidateRoot(SafeFileHandle handle)
    {
        WindowsBootstrapSourceCapture.RequireHandlePath(handle, RootProof.RootPath);
        var p = ReadIdentity(handle, directory: true); var expected = RootProof.Handle;
        if (p.FileId128 != expected.FileId128 || p.Created != expected.CreationTime || p.Volume64 != expected.VolumeSerial64
            || p.Volume32 != expected.LegacyVolumeSerial || p.CaseSensitive != expected.CaseSensitive)
            throw new IOException("The original native source root changed.");
    }
    private sealed record Identity(string FileId128, string LegacyFileId64, string Volume64, uint Volume32,
        long Created, long Write, long Change, long Size, uint Attributes, uint Links, bool CaseSensitive)
    {
        // Directory timestamps can change when unrelated siblings are created;
        // this is identity stability only, never an enumeration/cut witness.
        public (string, string, string, uint, long, uint, bool) Witness =>
            (FileId128, LegacyFileId64, Volume64, Volume32, Created, Attributes, CaseSensitive);
    }
    private static Identity ReadIdentity(SafeFileHandle handle, bool directory)
    {
        var id = Info(handle, 18, 24); var basic = Info(handle, 0, 40); var standard = Info(handle, 1, 24);
        if (!Native.GetFileInformationByHandle(handle, out var old)) throw Failure();
        var attrs = BinaryPrimitives.ReadUInt32LittleEndian(basic.AsSpan(32));
        if (((attrs & 0x10) != 0) != directory || (attrs & (0x400u | 0x1000u | 0x40000u | 0x400000u)) != 0)
            throw new IOException("The opened source type/reparse/recall policy is unsupported.");
        var sensitive = directory ? BinaryPrimitives.ReadUInt32LittleEndian(Info(handle, 23, 4)) : 0;
        if ((sensitive & ~1u) != 0) throw new IOException("Unknown native case mode.");
        if (!directory && old.Links != 1) throw new BootstrapCaptureDeferredException("A multiply linked source needs qualified alias reconciliation.");
        var legacy = new byte[8]; BinaryPrimitives.WriteUInt64LittleEndian(legacy, ((ulong)old.IndexHigh << 32) | old.IndexLow);
        return new(Convert.ToHexStringLower(id.AsSpan(8, 16)), Convert.ToHexStringLower(legacy),
            BinaryPrimitives.ReadUInt64LittleEndian(id).ToString("x16", CultureInfo.InvariantCulture), old.Volume,
            BinaryPrimitives.ReadInt64LittleEndian(basic), BinaryPrimitives.ReadInt64LittleEndian(basic.AsSpan(16)),
            BinaryPrimitives.ReadInt64LittleEndian(basic.AsSpan(24)), directory ? 0 : BinaryPrimitives.ReadInt64LittleEndian(standard.AsSpan(8)),
            attrs, old.Links, sensitive != 0);
    }
    private static long ReadLastFileUsn(SafeFileHandle handle, Identity identity)
    {
        byte[] request = [2, 0, 3, 0]; var output = new byte[1024];
        // FSCTL_READ_FILE_USN_DATA is read-only. Reason/time/source fields are
        // invalid here; this value is a file witness, never a journal cursor.
        if (!Native.DeviceIoControl(handle, 0x000900ebu, request, request.Length, output, output.Length, out var returned, nint.Zero)) throw Failure();
        if (returned < 60 || returned > output.Length) throw new InvalidDataException("File USN response is truncated.");
        var span = output.AsSpan(0, returned); var length = BinaryPrimitives.ReadUInt32LittleEndian(span);
        var major = BinaryPrimitives.ReadUInt16LittleEndian(span[4..]);
        if (length != returned || BinaryPrimitives.ReadUInt16LittleEndian(span[6..]) != 0 || major is not (2 or 3))
            throw new InvalidDataException("File USN response has unsupported framing/version.");
        var usnOffset = major == 2 ? 24 : 40; var fixedLength = major == 2 ? 60 : 76;
        if (returned < fixedLength || Convert.ToHexStringLower(span.Slice(8, major == 2 ? 8 : 16))
            != (major == 2 ? identity.LegacyFileId64 : identity.FileId128)) throw new InvalidDataException("File USN identity changed.");
        var usn = BinaryPrimitives.ReadInt64LittleEndian(span[usnOffset..]);
        if (usn < 0) throw new InvalidDataException("File USN witness is invalid.");
        return usn;
    }
    private static void RequireExactLeaf(SafeFileHandle handle, string name)
    {
        var buffer = new char[4096]; var count = Native.GetFinalPathNameByHandleW(handle, buffer, (uint)buffer.Length, 0);
        if (count == 0 || count >= buffer.Length) throw Failure();
        if (Path.GetFileName(new string(buffer, 0, (int)count)) != name) throw new IOException("The actual native leaf spelling differs from the queued path.");
    }
    private static SafeFileHandle Open(string path, uint access, uint share)
    {
        var h = Native.CreateFileW(path, access, share, nint.Zero, 3, 0x02000000u | 0x00200000u | 0x00100000u, nint.Zero);
        if (!h.IsInvalid) return h; var error = Marshal.GetLastWin32Error(); h.Dispose(); throw new Win32Exception(error);
    }
    private static byte[] Info(SafeFileHandle handle, int information, int size)
    { var data = new byte[size]; if (!Native.GetFileInformationByHandleEx(handle, information, data, (uint)size)) throw Failure(); return data; }
    private static Win32Exception Failure() => new(Marshal.GetLastWin32Error(), "Readonly native capture query failed.");
    public async ValueTask DisposeAsync() { if (!_disposed) { _disposed = true; await _root.DisposeAsync(); } }
    [StructLayout(LayoutKind.Sequential)] private readonly record struct Time(uint Low, uint High);
    [StructLayout(LayoutKind.Sequential)] private readonly record struct Legacy(uint Attributes, Time Created, Time Accessed, Time Write, uint Volume, uint SizeHigh, uint SizeLow, uint Links, uint IndexHigh, uint IndexLow);
    private static class Native
    {
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)] internal static extern SafeFileHandle CreateFileW(string path, uint access, uint share, nint security, uint disposition, uint flags, nint template);
        [DllImport("kernel32.dll", SetLastError = true)] [return: MarshalAs(UnmanagedType.Bool)] internal static extern bool GetFileInformationByHandleEx(SafeFileHandle handle, int information, [Out] byte[] data, uint size);
        [DllImport("kernel32.dll", SetLastError = true)] [return: MarshalAs(UnmanagedType.Bool)] internal static extern bool GetFileInformationByHandle(SafeFileHandle handle, out Legacy info);
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)] internal static extern uint GetFinalPathNameByHandleW(SafeFileHandle handle, [Out] char[] path, uint size, uint flags);
        [DllImport("kernel32.dll", SetLastError = true)] [return: MarshalAs(UnmanagedType.Bool)] internal static extern bool DeviceIoControl(SafeFileHandle handle, uint code, byte[] input, int inputLength, [Out] byte[] output, int outputLength, out int returned, nint overlapped);
    }
}
