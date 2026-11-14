using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class BootstrapScannerTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static DateTimeOffset At => DateTimeOffset.Parse("2026-10-12T00:00:00Z");

    [Fact]
    public async Task FirstBranchEligibilityPrecedesWholeScanWhileIncompleteGenerationsNeverAuthorizeCleanup()
    {
        await using var fixture = await Fixture.CreateAsync();
        var scannedDeep = false;
        var source = new FakeSource(fixture.Root, relative => relative.Length == 0
            ? [File("first.txt"), Directory("later")]
            : MarkDeep());
        IEnumerable<BootstrapSourceEntry> MarkDeep() { scannedDeep = true; yield return File("last.txt"); }
        var readyBeforeDeep = false;
        var result = await new NeptuneBootstrapScanner(fixture.Store, source).ScanAsync("mapping", "client", fixture.Root, async (directory, token) =>
        {
            if (directory.RelativePath != "") return;
            var active = Assert.Single(await fixture.Store.ListBootstrapScansAsync("mapping", token));
            var eligible = await fixture.Store.ListBootstrapEligibleEntriesAsync(active.ScanId, cancellationToken: token);
            readyBeforeDeep = eligible.Any(entry => entry.RelativePath == "first.txt") && !scannedDeep;
            Assert.Equal(BootstrapScanStatus.Scanning, active.Status);
            Assert.False(active.DestructiveCleanupAllowed);
        }, Ct);
        Assert.True(readyBeforeDeep);
        Assert.Equal(BootstrapScanStatus.EnumerationComplete, result.Status);
        Assert.Equal(2, result.Files);
        Assert.Equal(1, result.Directories);
        Assert.False(result.DestructiveCleanupAllowed);
    }

    [Fact]
    public async Task WideAndDeepTreesUseBoundedPagingAndPreserveEmptyDirectories()
    {
        await using var fixture = await Fixture.CreateAsync();
        const int width = 2000;
        const int depth = 90;
        var source = new FakeSource(fixture.Root, relative => relative.Length == 0
            ? Enumerable.Range(0, width).Select(index => File("file-" + index)).Append(Directory("empty")).Append(Directory("deep"))
            : relative == "empty" ? []
            : relative.Count(character => character == '/') >= depth ? [File("leaf")]
            : [Directory("next")]);
        var result = await new NeptuneBootstrapScanner(fixture.Store, source).ScanAsync("mapping", "client", fixture.Root, cancellationToken: Ct);
        Assert.Equal(width + 1, result.Files);
        Assert.Equal(depth + 2, result.Directories);
        Assert.Equal(4L * (width + 1), result.Bytes);
        long cursor = 0;
        var seen = 0;
        while (true)
        {
            var page = await fixture.Store.ListBootstrapEligibleEntriesAsync(result.ScanId, cursor, 37, Ct);
            Assert.InRange(page.Count, 0, 37);
            if (page.Count == 0) break;
            Assert.True(page.All(entry => entry.EntryId > cursor));
            cursor = page[^1].EntryId;
            seen += page.Count;
        }
        Assert.Equal(result.Files + result.Directories, seen);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => fixture.Store.ListBootstrapEligibleEntriesAsync(result.ScanId, limit: 257, cancellationToken: Ct));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_directories WHERE relative_path='empty' AND state=2"));
    }

    [Theory]
    [InlineData("Name", "name")]
    [InlineData("caf\u00e9", "cafe\u0301")]
    [InlineData(" spaced ", "spaced")]
    public async Task CanonicalSiblingCollisionAcrossBatchesPreventsAnyChildEligibility(string first, string second)
    {
        await using var fixture = await Fixture.CreateAsync();
        var source = new FakeSource(fixture.Root, _ => new[] { File(first) }
            .Concat(Enumerable.Range(0, 255).Select(index => File("unique-" + index))).Append(File(second)));
        var eligibleNotifications = 0;
        await Assert.ThrowsAsync<InvalidDataException>(() => new NeptuneBootstrapScanner(fixture.Store, source).ScanAsync("mapping", "client", fixture.Root,
            (_, _) => { eligibleNotifications++; return Task.CompletedTask; }, Ct));
        var failed = Assert.Single(await fixture.Store.ListBootstrapScansAsync("mapping", Ct));
        Assert.Equal(BootstrapScanStatus.Incomplete, failed.Status);
        Assert.Equal(0, eligibleNotifications);
        Assert.Empty(await fixture.Store.ListBootstrapEligibleEntriesAsync(failed.ScanId, cancellationToken: Ct));
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_entries WHERE eligible=1"));
        Assert.False(failed.DestructiveCleanupAllowed);
    }

    [Fact]
    public async Task AccessFailureBlocksFurtherEligibilityAndNeverCancelsAnUploadCheckpoint()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.CheckpointAsync();
        var source = new FakeSource(fixture.Root, relative => relative.Length == 0 ? [File("kept"), Directory("denied")] : throw new UnauthorizedAccessException("synthetic denied branch"));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => new NeptuneBootstrapScanner(fixture.Store, source).ScanAsync("mapping", "client", fixture.Root, cancellationToken: Ct));
        var scan = Assert.Single(await fixture.Store.ListBootstrapScansAsync("mapping", Ct));
        Assert.Equal(BootstrapScanStatus.Incomplete, scan.Status);
        Assert.Empty(await fixture.Store.ListBootstrapEligibleEntriesAsync(scan.ScanId, cancellationToken: Ct));
        Assert.False(scan.DestructiveCleanupAllowed);
        Assert.Equal("kept-upload", Assert.Single(await fixture.Store.ListSyncUploadCheckpointsAsync("scope", Ct)).UploadId);
    }

    [Fact]
    public async Task CancellationPersistsInterruptedAndRestartUsesFreshGenerationWithoutLosingCheckpoints()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.CheckpointAsync();
        var source = new FakeSource(fixture.Root, relative => relative.Length == 0 ? [File("one"), Directory("later")] : [File("two")]);
        using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new NeptuneBootstrapScanner(fixture.Store, source).ScanAsync("mapping", "client", fixture.Root,
            (_, _) => { cancelled.Cancel(); return Task.CompletedTask; }, cancelled.Token));
        var interrupted = Assert.Single(await fixture.Store.ListBootstrapScansAsync("mapping", Ct));
        Assert.Equal(BootstrapScanStatus.Interrupted, interrupted.Status);
        Assert.Empty(await fixture.Store.ListBootstrapEligibleEntriesAsync(interrupted.ScanId, cancellationToken: Ct));
        var restarted = new NeptuneStateStore(fixture.Database);
        var complete = await new NeptuneBootstrapScanner(restarted, source).ScanAsync("mapping", "client", fixture.Root, cancellationToken: Ct);
        Assert.NotEqual(interrupted.ScanId, complete.ScanId);
        Assert.Equal(2, complete.Files);
        Assert.Equal(2, (await restarted.ListBootstrapScansAsync("mapping", Ct)).Count);
        Assert.Equal("kept-upload", Assert.Single(await restarted.ListSyncUploadCheckpointsAsync("scope", Ct)).UploadId);
        Assert.False(complete.DestructiveCleanupAllowed);
    }

    [Fact]
    public async Task OneScannerOwnsMappingAcrossInstancesAndSourceCallsUseOneEnumerator()
    {
        await using var fixture = await Fixture.CreateAsync();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new FakeSource(fixture.Root, _ => [File("one")]);
        var first = new NeptuneBootstrapScanner(fixture.Store, source).ScanAsync("mapping", "client", fixture.Root, async (_, token) =>
        {
            entered.TrySetResult();
            await release.Task.WaitAsync(token);
        }, Ct);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        try
        {
            await Assert.ThrowsAsync<IOException>(() => new NeptuneBootstrapScanner(new NeptuneStateStore(fixture.Database), source).ScanAsync("mapping", "client", fixture.Root, cancellationToken: Ct));
            Assert.Single(await fixture.Store.ListBootstrapScansAsync("mapping", Ct));
        }
        finally { release.TrySetResult(); }
        Assert.Equal(BootstrapScanStatus.EnumerationComplete, (await first).Status);
        Assert.Equal(1, source.MaxEnumerators);
    }

    [Fact]
    public async Task StagedRowsAndParentEligibilityAreTransactionalIdempotentAndSurviveReopen()
    {
        await using var fixture = await Fixture.CreateAsync();
        var scan = await fixture.Store.BeginBootstrapScanAsync("scan", "mapping", "client", fixture.Root, "owner", Ct);
        var directory = (await fixture.Store.ClaimBootstrapDirectoryAsync("scan", "owner", Ct))!;
        await fixture.SqlAsync("CREATE TRIGGER interrupt_stage BEFORE INSERT ON bootstrap_scan_entries WHEN NEW.raw_name='reject' BEGIN SELECT RAISE(ABORT,'interrupt'); END");
        await Assert.ThrowsAsync<SqliteException>(() => fixture.Store.StageBootstrapEntriesAsync(directory, [File("first"), File("reject")], Ct));
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_entries"));
        await fixture.SqlAsync("DROP TRIGGER interrupt_stage");
        var restarted = new NeptuneStateStore(fixture.Database);
        Assert.Equal(scan, await restarted.BeginBootstrapScanAsync("scan", "mapping", "client", fixture.Root, "owner", Ct));
        await restarted.StageBootstrapEntriesAsync(directory, [File("first")], Ct);
        await restarted.StageBootstrapEntriesAsync(directory, [File("first")], Ct);
        Assert.Empty(await restarted.ListBootstrapEligibleEntriesAsync("scan", cancellationToken: Ct));
        await restarted.CompleteBootstrapDirectoryAsync(directory, Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => restarted.CompleteBootstrapDirectoryAsync(directory, Ct));
        Assert.Single(await restarted.ListBootstrapEligibleEntriesAsync("scan", cancellationToken: Ct));
        await restarted.RefillBootstrapFrontierAsync("scan", "owner", Ct);
        await restarted.FinishBootstrapScanAsync("scan", "owner", BootstrapScanStatus.EnumerationComplete, cancellationToken: Ct);
        Assert.Equal(1, (await restarted.GetBootstrapScanAsync("scan", Ct))!.Files);
    }

    [Fact]
    public async Task DeadOwnerRecoveryInterruptsOldGenerationAndRejectsItsLateWrites()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.Store.BeginBootstrapScanAsync("dead-scan", "mapping", "client", fixture.Root, "dead-owner", Ct);
        var old = (await fixture.Store.ClaimBootstrapDirectoryAsync("dead-scan", "dead-owner", Ct))!;
        await fixture.Store.StageBootstrapEntriesAsync(old, [File("staged")], Ct);
        var source = new FakeSource(fixture.Root, _ => [File("current")]);
        var fresh = await new NeptuneBootstrapScanner(new NeptuneStateStore(fixture.Database), source).ScanAsync("mapping", "client", fixture.Root, cancellationToken: Ct);
        Assert.Equal(BootstrapScanStatus.Interrupted, (await fixture.Store.GetBootstrapScanAsync("dead-scan", Ct))!.Status);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.CompleteBootstrapDirectoryAsync(old, Ct));
        Assert.Equal("current", Assert.Single(await fixture.Store.ListBootstrapEligibleEntriesAsync(fresh.ScanId, cancellationToken: Ct)).RelativePath);
    }

    [Fact]
    public async Task MutationOrMappingPauseDuringScanCannotProduceEnumerationComplete()
    {
        await using var fixture = await Fixture.CreateAsync();
        var calls = 0;
        var changing = new FakeSource(fixture.Root, _ => [File("one")], _ => new(At, At.AddSeconds(Interlocked.Increment(ref calls))));
        await Assert.ThrowsAsync<IOException>(() => new NeptuneBootstrapScanner(fixture.Store, changing).ScanAsync("mapping", "client", fixture.Root, cancellationToken: Ct));
        var changed = Assert.Single(await fixture.Store.ListBootstrapScansAsync("mapping", Ct));
        Assert.Equal(BootstrapScanStatus.Incomplete, changed.Status);
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_entries WHERE eligible=1"));
        var stable = new FakeSource(fixture.Root, _ => [Directory("next")]);
        await Assert.ThrowsAsync<InvalidOperationException>(() => new NeptuneBootstrapScanner(fixture.Store, stable).ScanAsync("mapping", "client", fixture.Root,
            async (_, token) => await fixture.Store.SetMappingEnabledAsync("mapping", "client", false, token), Ct));
        Assert.All(await fixture.Store.ListBootstrapScansAsync("mapping", Ct), scan => Assert.Equal(BootstrapScanStatus.Incomplete, scan.Status));
    }

    [Fact]
    public async Task WrongSourceOwnerBoundsAndUnfinishedQueueCannotBePromoted()
    {
        await using var fixture = await Fixture.CreateAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.BeginBootstrapScanAsync("wrong-source", "mapping", "client", Path.GetTempPath(), "owner", Ct));
        var scan = await fixture.Store.BeginBootstrapScanAsync("scan", "mapping", "client", fixture.Root, "owner", Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.BeginBootstrapScanAsync("scan", "mapping", "client", fixture.Root, "other-owner", Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.FinishBootstrapScanAsync("scan", "owner", BootstrapScanStatus.EnumerationComplete, cancellationToken: Ct));
        var directory = (await fixture.Store.ClaimBootstrapDirectoryAsync("scan", "owner", Ct))!;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => fixture.Store.StageBootstrapEntriesAsync(directory, Enumerable.Range(0, 257).Select(index => File("file-" + index)).ToArray(), Ct));
        Assert.False(scan.DestructiveCleanupAllowed);
    }

    [Fact]
    public async Task NativeFileSystemEnumeratorPreservesRealFileByteCountsAndEmptyDirectories()
    {
        await using var fixture = await Fixture.CreateAsync();
        await System.IO.File.WriteAllBytesAsync(Path.Combine(fixture.Root, "file.bin"), new byte[35], Ct);
        System.IO.Directory.CreateDirectory(Path.Combine(fixture.Root, "empty"));
        var result = await new NeptuneBootstrapScanner(fixture.Store).ScanAsync("mapping", "client", fixture.Root, cancellationToken: Ct);
        Assert.Equal(1, result.Files);
        Assert.Equal(1, result.Directories);
        Assert.Equal(35, result.Bytes);
    }

    [Fact]
    public async Task LatestGenerationOnlyIsEligibleAcrossRestartAndPruningRegardlessOfClockOrdering()
    {
        await using var fixture = await Fixture.CreateAsync();
        var source = new FakeSource(fixture.Root, _ => [File("one")]);
        var scanner = new NeptuneBootstrapScanner(fixture.Store, source);
        var first = await scanner.ScanAsync("mapping", "client", fixture.Root, cancellationToken: Ct);
        Assert.Single(await fixture.Store.ListBootstrapEligibleEntriesAsync(first.ScanId, cancellationToken: Ct));
        await fixture.SqlAsync("UPDATE bootstrap_scan_runs SET created_at='9999-01-01T00:00:00Z'");
        var second = await fixture.Store.BeginBootstrapScanAsync("second", "mapping", "client", fixture.Root, "second-owner", Ct);
        Assert.Equal(first.Generation + 1, second.Generation);
        Assert.Empty(await fixture.Store.ListBootstrapEligibleEntriesAsync(first.ScanId, cancellationToken: Ct));
        await fixture.Store.FinishBootstrapScanAsync(second.ScanId, second.OwnerToken, BootstrapScanStatus.Interrupted, cancellationToken: Ct);
        var third = await scanner.ScanAsync("mapping", "client", fixture.Root, cancellationToken: Ct);
        Assert.Equal(second.Generation + 1, third.Generation);
        Assert.Null(await fixture.Store.GetBootstrapScanAsync(first.ScanId, Ct));
        Assert.Equal(new[] { third.Generation, second.Generation }, (await fixture.Store.ListBootstrapScansAsync("mapping", Ct)).Select(scan => scan.Generation));
        Assert.Single(await fixture.Store.ListBootstrapEligibleEntriesAsync(third.ScanId, cancellationToken: Ct));
        Assert.False(third.DestructiveCleanupAllowed);
    }

    [Fact]
    public async Task EligibleReadsRejectPausedOrReboundMappingAndWrongClientCannotReuseGeneration()
    {
        await using var fixture = await Fixture.CreateAsync();
        var scan = await new NeptuneBootstrapScanner(fixture.Store, new FakeSource(fixture.Root, _ => [File("one")]))
            .ScanAsync("mapping", "client", fixture.Root, cancellationToken: Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.BeginBootstrapScanAsync(scan.ScanId, "mapping", "other-client", fixture.Root, scan.OwnerToken, Ct));
        await fixture.Store.SetMappingEnabledAsync("mapping", "client", false, Ct);
        Assert.Empty(await fixture.Store.ListBootstrapEligibleEntriesAsync(scan.ScanId, cancellationToken: Ct));
        await fixture.Store.SetMappingEnabledAsync("mapping", "client", true, Ct);
        Assert.Single(await fixture.Store.ListBootstrapEligibleEntriesAsync(scan.ScanId, cancellationToken: Ct));
        await fixture.SqlAsync("UPDATE sync_mappings SET local_path=local_path || '-rebound'");
        Assert.Empty(await fixture.Store.ListBootstrapEligibleEntriesAsync(scan.ScanId, cancellationToken: Ct));
    }

    [Fact]
    public async Task InterruptedParentPromotionRollsBackEligibilityQueueAndCountersTogether()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.Store.BeginBootstrapScanAsync("scan", "mapping", "client", fixture.Root, "owner", Ct);
        var directory = (await fixture.Store.ClaimBootstrapDirectoryAsync("scan", "owner", Ct))!;
        await fixture.Store.StageBootstrapEntriesAsync(directory, [File("one"), Directory("child")], Ct);
        await fixture.SqlAsync("CREATE TRIGGER interrupt_complete BEFORE UPDATE ON bootstrap_scan_runs BEGIN SELECT RAISE(ABORT,'interrupt'); END");
        await Assert.ThrowsAsync<SqliteException>(() => fixture.Store.CompleteBootstrapDirectoryAsync(directory, Ct));
        Assert.Empty(await fixture.Store.ListBootstrapEligibleEntriesAsync("scan", cancellationToken: Ct));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_directories"));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT state FROM bootstrap_scan_directories"));
        Assert.Equal(0, (await fixture.Store.GetBootstrapScanAsync("scan", Ct))!.Files);
        await fixture.SqlAsync("DROP TRIGGER interrupt_complete");
        await fixture.Store.CompleteBootstrapDirectoryAsync(directory, Ct);
        Assert.Equal(2, (await fixture.Store.ListBootstrapEligibleEntriesAsync("scan", cancellationToken: Ct)).Count);
    }

    [Theory]
    [InlineData("UPDATE bootstrap_scan_format SET version=3")]
    [InlineData("DELETE FROM bootstrap_scan_format")]
    [InlineData("DROP TABLE bootstrap_scan_format")]
    [InlineData("CREATE TABLE bootstrap_scan_unknown_future_state(value TEXT)")]
    [InlineData("ALTER TABLE bootstrap_scan_runs DROP COLUMN files")]
    [InlineData("DROP INDEX bootstrap_scan_one_active")]
    [InlineData("DROP INDEX bootstrap_scan_one_active; CREATE INDEX bootstrap_scan_one_active ON bootstrap_scan_runs(mapping_id) WHERE status=0")]
    [InlineData("DROP INDEX bootstrap_scan_one_active; CREATE UNIQUE INDEX bootstrap_scan_one_active ON bootstrap_scan_runs(mapping_id) WHERE status=1")]
    [InlineData("DROP INDEX bootstrap_scan_directory_queue; CREATE INDEX bootstrap_scan_directory_queue ON bootstrap_scan_directories(relative_path)")]
    [InlineData("DROP TABLE bootstrap_scan_entries; CREATE TABLE bootstrap_scan_entries(entry_id INTEGER PRIMARY KEY AUTOINCREMENT,scan_id TEXT NOT NULL,parent_directory_id INTEGER NOT NULL,raw_name TEXT NOT NULL,name_key TEXT NOT NULL,relative_path TEXT NOT NULL,kind INTEGER NOT NULL,size INTEGER NOT NULL,modified_at TEXT NOT NULL,eligible INTEGER NOT NULL,UNIQUE(scan_id,parent_directory_id,name_key),FOREIGN KEY(scan_id) REFERENCES bootstrap_scan_runs(scan_id) ON DELETE CASCADE); CREATE INDEX bootstrap_scan_eligible ON bootstrap_scan_entries(scan_id,eligible,entry_id)")]
    public async Task UnknownNewerOrBrokenBootstrapNamespaceIsRejectedBeforeWriterAndPreservesDatabaseAndWalBytes(string corrupt)
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.Store.BeginBootstrapScanAsync("scan", "mapping", "client", fixture.Root, "owner", Ct);
        await using var heldWriter = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = fixture.Database, Pooling = false }.ToString());
        await heldWriter.OpenAsync(Ct);
        await using (var command = heldWriter.CreateCommand())
        {
            command.CommandText = "PRAGMA wal_autocheckpoint=0; " + corrupt;
            await command.ExecuteNonQueryAsync(Ct);
        }
        var databaseBefore = await ReadSharedBytesAsync(fixture.Database);
        var walBefore = await ReadSharedBytesAsync(fixture.Database + "-wal");
        Assert.NotEmpty(walBefore);
        var reopened = new NeptuneStateStore(fixture.Database);
        await Assert.ThrowsAsync<InvalidDataException>(() => reopened.InitializeAsync(Ct));
        await Assert.ThrowsAsync<InvalidDataException>(() => reopened.BeginBootstrapScanAsync("new", "mapping", "client", fixture.Root, "new-owner", Ct));
        await Assert.ThrowsAsync<InvalidDataException>(() => reopened.GetBootstrapScanAsync("scan", Ct));
        await Assert.ThrowsAsync<InvalidDataException>(() => new NeptuneBootstrapScanner(reopened, new FakeSource(fixture.Root, _ => []))
            .ScanAsync("mapping", "client", fixture.Root, cancellationToken: Ct));
        Assert.Equal(databaseBefore, await ReadSharedBytesAsync(fixture.Database));
        Assert.Equal(walBefore, await ReadSharedBytesAsync(fixture.Database + "-wal"));
    }

    [Fact]
    public async Task OptionalBootstrapSchemaIsNotCreatedByReadDiagnostics()
    {
        await using var fixture = await Fixture.CreateAsync();
        Assert.Null(await fixture.Store.GetBootstrapScanAsync("missing", Ct));
        Assert.Empty(await fixture.Store.ListBootstrapScansAsync("mapping", Ct));
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM sqlite_master WHERE name GLOB 'bootstrap_scan_*'"));
    }

    private static BootstrapSourceEntry File(string name) => new(name, false, 4, At);
    private static BootstrapSourceEntry Directory(string name) => new(name, true, 0, At);

    private static async Task<byte[]> ReadSharedBytesAsync(string path)
    {
        await using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        using var output = new MemoryStream();
        await file.CopyToAsync(output, Ct);
        return output.ToArray();
    }

    private sealed class FakeSource(string root, Func<string, IEnumerable<BootstrapSourceEntry>> read, Func<string, BootstrapDirectoryStamp>? stamp = null) : IBootstrapScanSource
    {
        private int _active;
        public int MaxEnumerators { get; private set; }
        public BootstrapDirectoryStamp ReadDirectoryStamp(string fullPath) => stamp?.Invoke(fullPath) ?? new(At, At);
        public IEnumerable<BootstrapSourceEntry> EnumerateDirectory(string fullPath)
        {
            var count = Interlocked.Increment(ref _active);
            MaxEnumerators = Math.Max(MaxEnumerators, count);
            try
            {
                var relative = Path.GetRelativePath(root, fullPath).Replace('\\', '/');
                if (relative == ".") relative = "";
                foreach (var entry in read(relative)) yield return entry;
            }
            finally { Interlocked.Decrement(ref _active); }
        }
    }

    private sealed class Fixture : IAsyncDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "neptune-bootstrap-tests", Guid.NewGuid().ToString("N"));
        public string Root { get; }
        public string Database { get; }
        public NeptuneStateStore Store { get; }
        private Fixture()
        {
            System.IO.Directory.CreateDirectory(_directory);
            Root = Path.Combine(_directory, "source");
            System.IO.Directory.CreateDirectory(Root);
            Database = Path.Combine(_directory, "state.db");
            Store = new(Database);
        }
        public static async Task<Fixture> CreateAsync()
        {
            var fixture = new Fixture();
            await fixture.Store.InitializeAsync(Ct);
            await fixture.Store.AddMappingAsync(new("mapping", "client", fixture.Root, true, At), Ct);
            return fixture;
        }
        public Task CheckpointAsync() => Store.SaveSyncUploadCheckpointAsync(new("scope", "https://saturn.test/dav/sync/Existing/file", "key", "fingerprint", "kept-upload"), Ct);
        public async Task SqlAsync(string sql)
        {
            await using var connection = new SqliteConnection("Data Source=" + Database);
            await connection.OpenAsync(Ct);
            await using var command = connection.CreateCommand(); command.CommandText = sql;
            await command.ExecuteNonQueryAsync(Ct);
        }
        public async Task<object?> ScalarAsync(string sql)
        {
            await using var connection = new SqliteConnection("Data Source=" + Database);
            await connection.OpenAsync(Ct);
            await using var command = connection.CreateCommand(); command.CommandText = sql;
            return await command.ExecuteScalarAsync(Ct);
        }
        public ValueTask DisposeAsync()
        {
            SqliteConnection.ClearAllPools();
            if (!Path.GetFullPath(_directory).StartsWith(Path.GetFullPath(Path.Combine(Path.GetTempPath(), "neptune-bootstrap-tests")) + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)) throw new InvalidOperationException("Test cleanup escaped its owned directory.");
            System.IO.Directory.Delete(_directory, recursive: true);
            return ValueTask.CompletedTask;
        }
    }
}
