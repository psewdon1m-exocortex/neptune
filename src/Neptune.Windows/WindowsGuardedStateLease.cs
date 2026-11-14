using System.ComponentModel;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Neptune.Windows;

// Holds native identities for each directory and revalidates their current
// paths across asynchronous operations. Modern Windows rename semantics can
// bypass delete sharing; sharing alone is not namespace identity evidence.
internal sealed class WindowsGuardedStateLease : IDisposable
{
    private readonly List<SafeFileHandle> _handles = [];
    private readonly List<string> _paths = [];
    private WindowsGuardedStateLease() { }
    internal static WindowsGuardedStateLease Open(string path, bool create = false)
    {
        var lease = new WindowsGuardedStateLease();
        try
        {
            var full = Path.GetFullPath(path);
            var root = Path.GetPathRoot(full) ?? throw new InvalidDataException("Guarded state needs an absolute path.");
            var parts = full[root.Length..].Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length > 128) throw new InvalidDataException("Guarded state path is too deep.");
            var current = root;
            lease.Pin(current);
            foreach (var part in parts)
            {
                lease.RequireCurrent();
                current = Path.Combine(current, part);
                if (create && !Directory.Exists(current)) Directory.CreateDirectory(current);
                lease.Pin(current);
            }
            return lease;
        }
        catch { lease.Dispose(); throw; }
    }
    private void Pin(string directory)
    {
        WindowsGuardedCapabilities.RequirePlainAncestors(directory);
        if (!Directory.Exists(directory)) throw new DirectoryNotFoundException("Guarded state directory is unavailable.");
        if (!OperatingSystem.IsWindows()) return;
        // Managed IO accepts long absolute paths, but this direct Win32 call
        // needs the extended spelling as soon as hashed profile components
        // take the path past MAX_PATH. Keep the ordinary spelling for checks.
        var nativePath = directory.StartsWith("\\\\?\\", StringComparison.Ordinal) ? directory
            : directory.StartsWith("\\\\", StringComparison.Ordinal) ? "\\\\?\\UNC\\" + directory[2..]
            : "\\\\?\\" + directory;
        var handle = CreateFile(nativePath, 0x80, 3, IntPtr.Zero, 3, 0x02000000, IntPtr.Zero);
        if (handle.IsInvalid) { var error = Marshal.GetLastWin32Error(); handle.Dispose(); throw new Win32Exception(error, "Cannot pin guarded state directory."); }
        try
        {
            if (!GetFileInformationByHandle(handle, out var info)) throw new Win32Exception(Marshal.GetLastWin32Error());
            if ((info.Attributes & 0x400) != 0 || (info.Attributes & 0x10) == 0)
                throw new InvalidDataException("Guarded state cannot use a reparse or file ancestor.");
            RequirePath(handle, directory);
            _handles.Add(handle);
            _paths.Add(directory);
        }
        catch { handle.Dispose(); throw; }
    }
    internal void RequireCurrent()
    {
        for (var i = 0; i < _handles.Count; i++)
        {
            WindowsGuardedCapabilities.RequirePlainAncestors(_paths[i]);
            RequirePath(_handles[i], _paths[i]);
        }
    }
    private static void RequirePath(SafeFileHandle handle, string directory)
    {
        var buffer = new char[32768];
        var count = GetFinalPathNameByHandle(handle, buffer, (uint)buffer.Length, 0);
        if (count == 0 || count >= buffer.Length) throw new Win32Exception(Marshal.GetLastWin32Error());
        var actual = new string(buffer, 0, (int)count);
        if (actual.StartsWith("\\\\?\\UNC\\", StringComparison.Ordinal)) actual = "\\\\" + actual[8..];
        else if (actual.StartsWith("\\\\?\\", StringComparison.Ordinal)) actual = actual[4..];
        if (!string.Equals(Path.TrimEndingDirectorySeparator(Path.GetFullPath(actual)), Path.TrimEndingDirectorySeparator(Path.GetFullPath(directory)), StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("Opened guarded directory moved outside its expected path.");
    }
    public void Dispose() { foreach (var handle in _handles.AsEnumerable().Reverse()) handle.Dispose(); _handles.Clear(); _paths.Clear(); }
    [StructLayout(LayoutKind.Sequential)]
    private struct FileInformation
    {
        internal uint Attributes, CreationLow, CreationHigh, AccessLow, AccessHigh, WriteLow, WriteHigh,
            VolumeSerial, SizeHigh, SizeLow, Links, IndexHigh, IndexLow;
    }
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true, EntryPoint = "CreateFileW")]
    private static extern SafeFileHandle CreateFile(string name, uint access, uint share, IntPtr security,
        uint disposition, uint flags, IntPtr template);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetFileInformationByHandle(SafeFileHandle file, out FileInformation information);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true, EntryPoint = "GetFinalPathNameByHandleW")]
    private static extern uint GetFinalPathNameByHandle(SafeFileHandle file, [Out] char[] path, uint size, uint flags);
}
