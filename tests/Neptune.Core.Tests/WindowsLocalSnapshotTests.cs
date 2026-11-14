using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class WindowsLocalSnapshotTests
{
    public static bool WindowsAclAvailable => OperatingSystem.IsWindows();
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact(Skip = "Requires Windows directory ACLs", SkipUnless = nameof(WindowsAclAvailable))]
    public void DeniedSubtreeKeepsAccessibleSiblingsAndBlocksRemoteReconciliation()
    {
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException();
        using var tree = new LocalTree();
        tree.Write("blocked/retained.txt");
        tree.Write("available/note.txt");
        tree.Write("root.txt");
        var blocked = tree.PathOf("blocked");
        tree.DenyListing(blocked);
        // Prove this fixture reproduces the release failure, not a simulated skip.
        Assert.Throws<UnauthorizedAccessException>(() => Directory.GetFileSystemEntries(blocked));

        var snapshot = WindowsLocalSnapshot.Read(tree.Root, Ct);

        Assert.Equal([blocked], snapshot.AccessDeniedPaths);
        Assert.False(snapshot.CanReconcileRemote);
        Assert.Equal(["available/note.txt", "root.txt"], snapshot.Entries
            .Where(entry => !entry.IsDirectory).Select(entry => entry.RelativePath).Order(StringComparer.Ordinal));
        Assert.Contains(snapshot.Entries, entry => entry.RelativePath == "blocked" && entry.IsDirectory);
    }

    [Fact(Skip = "Requires Windows directory ACLs", SkipUnless = nameof(WindowsAclAvailable))]
    public void DeniedMappingRootIsIncompleteRatherThanAnEmptySource()
    {
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException();
        using var tree = new LocalTree();
        tree.Write("retained.txt");
        tree.DenyListing(tree.Root);
        Assert.Throws<UnauthorizedAccessException>(() => Directory.GetFileSystemEntries(tree.Root));

        var snapshot = WindowsLocalSnapshot.Read(tree.Root, Ct);

        Assert.Empty(snapshot.Entries);
        Assert.Equal([tree.Root], snapshot.AccessDeniedPaths);
        Assert.False(snapshot.CanReconcileRemote);
    }

    [Fact(Skip = "Requires Windows directory ACLs", SkipUnless = nameof(WindowsAclAvailable))]
    public void RestoredAccessIncludesPreviouslyDeniedFilesOnTheNextScan()
    {
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException();
        using var tree = new LocalTree();
        tree.Write(".audit/blocked/retained.txt");
        tree.Write("available.txt");
        tree.DenyListing(tree.PathOf(".audit/blocked"));
        Assert.False(WindowsLocalSnapshot.Read(tree.Root, Ct).CanReconcileRemote);

        tree.RestoreAccess();
        var snapshot = WindowsLocalSnapshot.Read(tree.Root, Ct);

        Assert.Empty(snapshot.AccessDeniedPaths);
        Assert.True(snapshot.CanReconcileRemote);
        Assert.Equal([".audit/blocked/retained.txt", "available.txt"], snapshot.Entries
            .Where(entry => !entry.IsDirectory).Select(entry => entry.RelativePath).Order(StringComparer.Ordinal));
    }

    [Fact]
    public void CancellationIsNotReportedAsAnIncompleteScan()
    {
        using var tree = new LocalTree();
        tree.Write("note.txt");
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(() => WindowsLocalSnapshot.Read(tree.Root, cancellation.Token));
    }

    [Fact]
    public void MissingRootStillReachesTheExistingSourceMutationRetryPolicy()
    {
        using var tree = new LocalTree();
        Assert.Throws<DirectoryNotFoundException>(() => WindowsLocalSnapshot.Read(tree.PathOf("missing"), Ct));
    }

    [Fact]
    public void CompletedRescanAfterSourceMutationStillDefersRemoteReconciliation()
    {
        using var tree = new LocalTree();
        tree.Write("note.txt");
        var snapshot = WindowsLocalSnapshot.Read(tree.Root, Ct) with { WasUnstable = true };
        Assert.Single(snapshot.Entries);
        Assert.Empty(snapshot.AccessDeniedPaths);
        Assert.False(snapshot.CanReconcileRemote);
    }

    private sealed class LocalTree : IDisposable
    {
        internal string Root { get; } = Path.Combine(Path.GetTempPath(), "neptune-access-denied-" + Guid.NewGuid().ToString("N"));
        private readonly List<(string Path, byte[] Descriptor)> _originalAcls = [];

        internal LocalTree() => Directory.CreateDirectory(Root);
        internal string PathOf(string relative) => Path.Combine(Root, relative.Replace('/', Path.DirectorySeparatorChar));
        internal void Write(string relative)
        {
            var path = PathOf(relative);
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            File.WriteAllText(path, "retained content");
        }

        [SupportedOSPlatform("windows")]
        internal void DenyListing(string path)
        {
            var directory = new DirectoryInfo(path);
            var original = directory.GetAccessControl().GetSecurityDescriptorBinaryForm();
            var restricted = new DirectorySecurity();
            restricted.SetSecurityDescriptorBinaryForm(original, AccessControlSections.Access);
            using var identity = WindowsIdentity.GetCurrent();
            restricted.AddAccessRule(new FileSystemAccessRule(identity.User!, FileSystemRights.ListDirectory,
                InheritanceFlags.None, PropagationFlags.None, AccessControlType.Deny));
            _originalAcls.Add((path, original));
            directory.SetAccessControl(restricted);
        }

        [SupportedOSPlatform("windows")]
        internal void RestoreAccess()
        {
            foreach (var (path, descriptor) in _originalAcls.AsEnumerable().Reverse())
            {
                // A freshly read DirectorySecurity has no dirty access rules;
                // SetAccessControl would not write it back after the denial.
                var restored = new DirectorySecurity();
                restored.SetSecurityDescriptorBinaryForm(descriptor, AccessControlSections.Access);
                new DirectoryInfo(path).SetAccessControl(restored);
            }
            _originalAcls.Clear();
        }

        public void Dispose()
        {
            if (OperatingSystem.IsWindows()) RestoreAccess();
            Directory.Delete(Root, recursive: true);
        }
    }
}
