using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Microsoft.Win32.SafeHandles;

namespace Neptune.Core;

// Windows deny-write/delete handles qualify a single version while it is copied
// once into an immutable spool and hashed. A directory stamp is not this proof.
// Other operating systems must supply a separately qualified capture adapter.
public sealed class WindowsBootstrapSourceCapture : IBootstrapSourceCapture
{
    public Task ValidateDirectoryAsync(string sourceRoot, string relativePath, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException("Strict bootstrap capture requires the qualified Windows handle adapter.");
        var root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(sourceRoot));
        var full = Path.GetFullPath(Path.Combine(root, relativePath.Replace('/', Path.DirectorySeparatorChar)));
        if (!full.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)) throw new InvalidDataException("Directory escaped its source root.");
        RejectReparseComponents(root, full);
        using var ancestry = HoldDirectoryPath(Path.GetDirectoryName(full)!);
        using var rootHandle = CreateFile(WindowsNativePath.ForFile(root), 0x80, 1, nint.Zero, 3, 0x02000000, nint.Zero);
        using var directory = CreateFile(WindowsNativePath.ForFile(full), 0x80, 1, nint.Zero, 3, 0x02000000, nint.Zero);
        if (rootHandle.IsInvalid || directory.IsInvalid) throw new Win32Exception(Marshal.GetLastWin32Error());
        RequirePath(rootHandle, root); RequirePath(directory, full); RejectReparseComponents(root, full);
        return Task.CompletedTask;
    }
    public async Task<BootstrapSealedSource> CaptureAsync(string sourceRoot, string relativePath, string spoolPath,
        long maximumBytes, CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException("Strict bootstrap capture requires the qualified Windows handle adapter.");
        var root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(sourceRoot));
        var full = Path.GetFullPath(Path.Combine(root, relativePath.Replace('/', Path.DirectorySeparatorChar)));
        if (!full.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)) throw new InvalidDataException("Capture escaped its source root.");
        RejectReparseComponents(root, full);
        using var ancestry = HoldDirectoryPath(Path.GetDirectoryName(full)!);
        using var rootHandle = CreateFile(WindowsNativePath.ForFile(root), 0x80, 1, nint.Zero, 3, 0x02000000, nint.Zero);
        if (rootHandle.IsInvalid) throw new Win32Exception(Marshal.GetLastWin32Error());
        RequirePath(rootHandle, root);
        await using var source = new FileStream(full, FileMode.Open, FileAccess.Read, FileShare.Read, 128 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        RequirePath(source.SafeFileHandle, full);
        RequirePath(rootHandle, root);
        RejectReparseComponents(root, full);
        var before = ReadIdentity(source.SafeFileHandle);
        if ((before.Attributes & 0x400) != 0 || (before.Attributes & 0x10) != 0) throw new IOException("Source handle is a directory or reparse point.");
        var size = ((long)before.SizeHigh << 32) | before.SizeLow;
        if (size > maximumBytes) throw new BootstrapCaptureDeferredException("The sealed source exceeds this opt-in spool budget.");
        Directory.CreateDirectory(Path.GetDirectoryName(spoolPath)!);
        try
        {
            await using var spool = new FileStream(spoolPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 128 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            var buffer = new byte[128 * 1024];
            long copied = 0;
            while (true)
            {
                var count = await source.ReadAsync(buffer, cancellationToken);
                if (count == 0) break;
                copied = checked(copied + count);
                if (copied > maximumBytes) throw new BootstrapCaptureDeferredException("Source grew beyond its spool reservation.");
                hash.AppendData(buffer, 0, count);
                await spool.WriteAsync(buffer.AsMemory(0, count), cancellationToken);
            }
            var after = ReadIdentity(source.SafeFileHandle);
            RequirePath(source.SafeFileHandle, full); RequirePath(rootHandle, root);
            if (copied != size || before.Volume != after.Volume || before.IndexHigh != after.IndexHigh || before.IndexLow != after.IndexLow
                || before.Created != after.Created || before.LastWrite != after.LastWrite || before.SizeHigh != after.SizeHigh
                || before.SizeLow != after.SizeLow || before.Attributes != after.Attributes)
                throw new IOException("The captured source identity changed during its guarded read.");
            await spool.FlushAsync(cancellationToken); spool.Flush(flushToDisk: true);
            var identity = $"windows:{before.Volume:X8}:{before.IndexHigh:X8}{before.IndexLow:X8}:{before.LastWrite.High:X8}{before.LastWrite.Low:X8}:{size}";
            return new(spoolPath, size, Convert.ToHexStringLower(hash.GetHashAndReset()), identity);
        }
        catch { if (File.Exists(spoolPath)) File.Delete(spoolPath); throw; }
    }

    private static void RejectReparseComponents(string root, string full)
    {
        var current = root;
        if ((File.GetAttributes(current) & FileAttributes.ReparsePoint) != 0) throw new IOException("Source root became a reparse point.");
        foreach (var component in Path.GetRelativePath(root, full).Split(Path.DirectorySeparatorChar))
        {
            current = Path.Combine(current, component);
            if ((File.GetAttributes(current) & FileAttributes.ReparsePoint) != 0) throw new IOException("Source branch became a reparse point.");
        }
    }
    internal static void RequireHandlePath(SafeFileHandle handle, string expected) => RequirePath(handle, Path.TrimEndingDirectorySeparator(Path.GetFullPath(expected)));
    // Hold every directory component without delete sharing. A prefix check
    // alone cannot prevent a branch/ancestor being renamed into a reparse path
    // between enumeration and FileStream.Open. Read/write sharing remains
    // available to ordinary child I/O; these handles fence ancestor renames.
    internal static IDisposable HoldDirectoryPath(string directory)
    {
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException();
        var full = Path.TrimEndingDirectorySeparator(Path.GetFullPath(directory));
        var root = Path.GetPathRoot(full)!;
        var handles = new List<SafeFileHandle>();
        try
        {
            var current = root;
            void Hold()
            {
                if ((File.GetAttributes(current) & FileAttributes.ReparsePoint) != 0) throw new IOException("A guarded path contains a reparse point.");
                var handle = CreateFile(WindowsNativePath.ForFile(current), 0x80, 3, nint.Zero, 3, 0x02000000, nint.Zero);
                if (handle.IsInvalid) { handle.Dispose(); throw new Win32Exception(Marshal.GetLastWin32Error()); }
                handles.Add(handle); RequireHandlePath(handle, current);
                if ((ReadIdentity(handle).Attributes & 0x400) != 0) throw new IOException("A guarded directory handle is a reparse point.");
            }
            Hold();
            foreach (var component in Path.GetRelativePath(root, full).Split(Path.DirectorySeparatorChar, StringSplitOptions.RemoveEmptyEntries))
            { if (component == ".") continue; current = Path.Combine(current, component); Hold(); }
            return new DirectoryHandles(handles);
        }
        catch { foreach (var handle in handles) handle.Dispose(); throw; }
    }
    private sealed class DirectoryHandles(List<SafeFileHandle> handles) : IDisposable
    { public void Dispose() { foreach (var handle in handles) handle.Dispose(); } }
    private static void RequirePath(SafeFileHandle handle, string expected)
    {
        var text = new char[32768];
        var count = GetFinalPathNameByHandle(handle, text, (uint)text.Length, 0);
        if (count == 0 || count >= text.Length) throw new Win32Exception(Marshal.GetLastWin32Error());
        var actual = new string(text, 0, (int)count);
        if (actual.StartsWith("\\\\?\\UNC\\", StringComparison.Ordinal)) actual = "\\\\" + actual[8..];
        else if (actual.StartsWith("\\\\?\\", StringComparison.Ordinal)) actual = actual[4..];
        if (!string.Equals(Path.TrimEndingDirectorySeparator(Path.GetFullPath(actual)), expected, StringComparison.OrdinalIgnoreCase))
            throw new IOException("The opened source handle no longer resolves to the expected root/entry path.");
    }
    private static FileIdentity ReadIdentity(SafeFileHandle handle)
    {
        if (!GetFileInformationByHandle(handle, out var identity)) throw new Win32Exception(Marshal.GetLastWin32Error());
        return identity;
    }
    [StructLayout(LayoutKind.Sequential)] private readonly record struct FileTime(uint Low, uint High);
    [StructLayout(LayoutKind.Sequential)] private readonly record struct FileIdentity(uint Attributes, FileTime Created,
        FileTime Accessed, FileTime LastWrite, uint Volume, uint SizeHigh, uint SizeLow, uint Links, uint IndexHigh, uint IndexLow);
    [DllImport("kernel32.dll", EntryPoint = "CreateFileW", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern SafeFileHandle CreateFile(string path, uint access, uint share, nint security, uint creation, uint flags, nint template);
    [DllImport("kernel32.dll", EntryPoint = "GetFinalPathNameByHandleW", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern uint GetFinalPathNameByHandle(SafeFileHandle handle, [Out] char[] path, uint size, uint flags);
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetFileInformationByHandle(SafeFileHandle handle, out FileIdentity identity);
}
