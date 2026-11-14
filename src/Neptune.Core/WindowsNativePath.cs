namespace Neptune.Core;

// Use only at native I/O boundaries. Persisted paths and identity comparisons
// retain their ordinary spelling.
internal static class WindowsNativePath
{
    internal static string ForFile(string path)
    {
        var absolute = Path.GetFullPath(path);
        if (!OperatingSystem.IsWindows() || absolute.StartsWith("\\\\?\\", StringComparison.Ordinal)) return absolute;
        return absolute.StartsWith("\\\\", StringComparison.Ordinal) ? "\\\\?\\UNC\\" + absolute[2..] : "\\\\?\\" + absolute;
    }
}
