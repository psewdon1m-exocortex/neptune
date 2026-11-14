namespace Neptune.Windows;

internal sealed record WindowsLocalEntry(string FullPath, string RelativePath, bool IsDirectory);

internal sealed record WindowsLocalSnapshot(WindowsLocalEntry[] Entries, string[] AccessDeniedPaths)
{
    internal bool WasUnstable { get; init; }
    internal bool CanReconcileRemote => !WasUnstable && AccessDeniedPaths.Length == 0;

    internal static WindowsLocalSnapshot Read(string root, CancellationToken cancellationToken, Neptune.Core.MappingSyncPolicy? policy = null)
    {
        var entries = new List<WindowsLocalEntry>();
        var denied = new List<string>();
        var pending = new Stack<string>();
        pending.Push(root);
        while (pending.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var directory = pending.Pop();
            string[] children;
            try { children = Directory.EnumerateFileSystemEntries(directory).ToArray(); }
            catch (UnauthorizedAccessException)
            {
                // Keep scanning accessible siblings, but never treat an unreadable
                // subtree as evidence that its remote files were deleted locally.
                denied.Add(directory);
                continue;
            }

            foreach (var child in children)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var relative = Path.GetRelativePath(root, child).Replace('\\', '/');
                if (policy?.Excludes(relative) == true) continue;
                FileAttributes attributes;
                try { attributes = File.GetAttributes(child); }
                catch (UnauthorizedAccessException)
                {
                    denied.Add(child);
                    continue;
                }
                if ((attributes & FileAttributes.ReparsePoint) != 0) continue;
                var isDirectory = (attributes & FileAttributes.Directory) != 0;
                entries.Add(new WindowsLocalEntry(child, relative, isDirectory));
                if (isDirectory) pending.Push(child);
            }
        }
        return new WindowsLocalSnapshot(entries.ToArray(), denied.ToArray());
    }
}
