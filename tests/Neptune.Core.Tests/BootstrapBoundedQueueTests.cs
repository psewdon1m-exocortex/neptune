using System.Diagnostics;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class BootstrapBoundedQueueTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static DateTimeOffset At => DateTimeOffset.UnixEpoch;

    [Fact]
    public async Task StagingAccumulatesCountsIdempotentlyAndWidePromotionChangesOnlyTwoRows()
    {
        await using var fixture = await Fixture.CreateAsync();
        var parent = await fixture.BeginAsync("scan");
        for (var offset = 0; offset < 4096; offset += 256)
        {
            var batch = Enumerable.Range(offset, 256).Select(index => Entry("file-" + index, false, 3)).ToArray();
            await fixture.Store.StageBootstrapEntriesAsync(parent, batch, Ct);
            await fixture.Store.StageBootstrapEntriesAsync(parent, batch, Ct);
        }
        Assert.Empty(await fixture.Store.ListBootstrapEligibleEntriesAsync("scan", cancellationToken: Ct));
        await fixture.Store.CompleteBootstrapDirectoryAsync(parent, Ct);
        Assert.Equal(2, fixture.Store.GetBootstrapWriteMetrics().LastChangedRows);
        Assert.InRange(fixture.Store.GetBootstrapWriteMetrics().MaximumChangedRows, 1, 257);
        var state = (await fixture.Store.GetBootstrapScanAsync("scan", Ct))!;
        Assert.Equal(4096, state.Files); Assert.Equal(4096 * 3, state.Bytes);
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_entries WHERE eligible<>0"));
        Assert.Equal(256, (await fixture.Store.ListBootstrapEligibleEntriesAsync("scan", cancellationToken: Ct)).Count);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.FinishBootstrapScanAsync("scan", "owner", BootstrapScanStatus.EnumerationComplete, cancellationToken: Ct));
        var refill = await fixture.Store.RefillBootstrapFrontierAsync("scan", "owner", Ct);
        Assert.True(refill!.ParentComplete); Assert.Equal(0, refill.AddedDirectories);
        await fixture.Store.FinishBootstrapScanAsync("scan", "owner", BootstrapScanStatus.EnumerationComplete, cancellationToken: Ct);
        Assert.False((await fixture.Store.GetBootstrapScanAsync("scan", Ct))!.DestructiveCleanupAllowed);
    }

    [Fact]
    public async Task ChildRefillHasBoundedEffectsIndexedKeysetAndAtomicCursorAcrossFailureAndReopen()
    {
        await using var fixture = await Fixture.CreateAsync();
        var parent = await fixture.BeginAsync("scan");
        for (var offset = 0; offset < 4096; offset += 256)
            await fixture.Store.StageBootstrapEntriesAsync(parent, Enumerable.Range(offset, 256)
                .Select(index => Entry("entry-" + index, index % 4 == 0)).ToArray(), Ct);
        await fixture.Store.CompleteBootstrapDirectoryAsync(parent, Ct);
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_directories"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.ClaimBootstrapDirectoryAsync("scan", "owner", Ct));
        var first = (await fixture.Store.RefillBootstrapFrontierAsync("scan", "owner", Ct))!;
        Assert.Equal(256, first.AddedDirectories); Assert.False(first.ParentComplete);
        Assert.Equal(257, fixture.Store.GetBootstrapWriteMetrics().LastChangedRows);
        Assert.Equal(257L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_directories"));
        Assert.Contains("bootstrap_scan_child_refill", await fixture.ExplainAsync("SELECT entry_id,relative_path FROM bootstrap_scan_entries WHERE scan_id='scan' AND parent_directory_id=1 AND kind=1 AND entry_id>0 ORDER BY entry_id LIMIT 256"));
        await fixture.SqlAsync("CREATE TRIGGER interrupt_refill BEFORE UPDATE OF child_cursor ON bootstrap_scan_directories BEGIN SELECT RAISE(ABORT,'refill interruption'); END");
        await Assert.ThrowsAsync<SqliteException>(() => fixture.Store.RefillBootstrapFrontierAsync("scan", "owner", Ct));
        Assert.Equal(257L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_directories"));
        Assert.Equal(first.Cursor, await fixture.ScalarAsync("SELECT child_cursor FROM bootstrap_scan_directories WHERE directory_id=1"));
        await fixture.SqlAsync("DROP TRIGGER interrupt_refill");
        var reopened = new NeptuneStateStore(fixture.Database);
        var second = (await reopened.RefillBootstrapFrontierAsync("scan", "owner", Ct))!;
        Assert.Equal(256, second.AddedDirectories); Assert.True(second.Cursor > first.Cursor);
        Assert.Equal(513L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_directories"));
        await reopened.FinishBootstrapScanAsync("scan", "owner", BootstrapScanStatus.Interrupted, cancellationToken: Ct);
        var fresh = await new NeptuneBootstrapScanner(reopened, new OneFileSource()).ScanAsync("mapping", "client", fixture.Root, cancellationToken: Ct);
        Assert.Equal(1, fresh.Files);
        Assert.Empty(await reopened.ListBootstrapEligibleEntriesAsync("scan", cancellationToken: Ct));
        Assert.Equal(513L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_directories WHERE scan_id='scan'"));
    }

    [Fact]
    public async Task EligibilityWatermarkCannotSkipHiddenEarlierParentOrCrossAnIncompleteGeneration()
    {
        await using var fixture = await Fixture.CreateAsync();
        var root = await fixture.BeginAsync("scan");
        await fixture.Store.StageBootstrapEntriesAsync(root, [Entry("a", true), Entry("b", true)], Ct);
        await fixture.Store.CompleteBootstrapDirectoryAsync(root, Ct);
        await fixture.Store.RefillBootstrapFrontierAsync("scan", "owner", Ct);
        var prior = await fixture.Store.ListBootstrapEligibleEntriesAsync("scan", cancellationToken: Ct);
        var cursor = prior[^1].EntryId;
        var a = (await fixture.Store.ClaimBootstrapDirectoryAsync("scan", "owner", Ct))!;
        await fixture.Store.StageBootstrapEntriesAsync(a, [Entry("hidden-first")], Ct);
        Assert.Empty(await fixture.Store.ListBootstrapEligibleEntriesAsync("scan", cursor, cancellationToken: Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.ClaimBootstrapDirectoryAsync("scan", "owner", Ct));
        await fixture.Store.CompleteBootstrapDirectoryAsync(a, Ct);
        var afterA = Assert.Single(await fixture.Store.ListBootstrapEligibleEntriesAsync("scan", cursor, cancellationToken: Ct));
        Assert.Equal("a/hidden-first", afterA.RelativePath); Assert.True(afterA.EntryId > cursor);
        var b = (await fixture.Store.ClaimBootstrapDirectoryAsync("scan", "owner", Ct))!;
        await fixture.Store.StageBootstrapEntriesAsync(b, [Entry("hidden-second")], Ct);
        Assert.Empty(await fixture.Store.ListBootstrapEligibleEntriesAsync("scan", afterA.EntryId, cancellationToken: Ct));
        await fixture.Store.FinishBootstrapScanAsync("scan", "owner", BootstrapScanStatus.Incomplete, "synthetic inaccessible remainder", Ct);
        Assert.Empty(await fixture.Store.ListBootstrapEligibleEntriesAsync("scan", cancellationToken: Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.CompleteBootstrapDirectoryAsync(b, Ct));
        Assert.False((await fixture.Store.GetBootstrapScanAsync("scan", Ct))!.DestructiveCleanupAllowed);
    }

    [Fact]
    public async Task RetirementNeverCascadesAWholeGenerationAndPreservesCurrentOtherMappingAndCheckpoints()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.Store.SaveSyncUploadCheckpointAsync(new("scope", "https://saturn.test/dav/file", "key", "fingerprint", "upload"), Ct);
        var old = await fixture.BeginAsync("old");
        for (var offset = 0; offset < 2048; offset += 256)
            await fixture.Store.StageBootstrapEntriesAsync(old, Enumerable.Range(offset, 256).Select(index => Entry("f-" + index)).ToArray(), Ct);
        await fixture.Store.FinishBootstrapScanAsync("old", "owner", BootstrapScanStatus.Interrupted, cancellationToken: Ct);
        await fixture.Store.BeginBootstrapScanAsync("second", "mapping", "client", fixture.Root, "owner-2", Ct);
        await fixture.Store.FinishBootstrapScanAsync("second", "owner-2", BootstrapScanStatus.Interrupted, cancellationToken: Ct);
        await fixture.Store.BeginBootstrapScanAsync("current", "mapping", "client", fixture.Root, "owner-3", Ct);
        var otherRoot = Path.Combine(fixture.Directory, "other-source"); System.IO.Directory.CreateDirectory(otherRoot);
        await fixture.Store.AddMappingAsync(new("other", "client", otherRoot, true, At), Ct);
        for (var generation = 1; generation <= 3; generation++)
        {
            var id = "other-" + generation;
            await fixture.Store.BeginBootstrapScanAsync(id, "other", "client", otherRoot, "owner", Ct);
            if (generation == 1)
            {
                var directory = (await fixture.Store.ClaimBootstrapDirectoryAsync(id, "owner", Ct))!;
                await fixture.Store.StageBootstrapEntriesAsync(directory, [Entry("other-kept")], Ct);
            }
            await fixture.Store.FinishBootstrapScanAsync(id, "owner", BootstrapScanStatus.Interrupted, cancellationToken: Ct);
        }
        Assert.Null(await fixture.Store.GetBootstrapScanAsync("old", Ct));
        Assert.Equal(2048L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_entries WHERE scan_id='old'"));
        var batches = 0;
        while (true)
        {
            var batch = await fixture.Store.RetireBootstrapBatchAsync("mapping", Ct);
            if (batch.ScanId is null) break;
            batches++;
            Assert.InRange(batch.RemovedEntries + batch.RemovedDirectories + (batch.RemovedGeneration ? 1 : 0), 1, 256);
        }
        Assert.Equal(10, batches);
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_entries WHERE scan_id='old'"));
        Assert.NotNull(await fixture.Store.GetBootstrapScanAsync("current", Ct));
        Assert.NotNull(await fixture.Store.GetBootstrapScanAsync("second", Ct));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_entries WHERE scan_id='other-1'"));
        Assert.NotNull(await fixture.Store.GetBootstrapScanAsync("other-3", Ct));
        Assert.Equal("upload", Assert.Single(await fixture.Store.ListSyncUploadCheckpointsAsync("scope", Ct)).UploadId);
        Assert.InRange(fixture.Store.GetBootstrapWriteMetrics().MaximumChangedRows, 1, 257);
    }

    [Fact]
    public async Task LegacyModuleMigrationBacksUpCommittedWalPreservesPauseAndCheckpointsAndExportsVerifiedRollback()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.CreateLegacyModuleAsync();
        await using var writer = await fixture.OpenAsync(pooling: false);
        await fixture.SqlAsync(writer, "PRAGMA wal_autocheckpoint=0; INSERT INTO sync_upload_checkpoints(scope,target_uri,idempotency_key,fingerprint,upload_id) VALUES('wal','https://saturn.test/dav/file','key','fingerprint','wal-upload'); UPDATE sync_mappings SET enabled=0");
        await fixture.Store.InitializeAsync(Ct); // known inactive v1 is readable, not silently migrated
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT version FROM bootstrap_scan_format"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.BeginBootstrapScanAsync("new", "mapping", "client", fixture.Root, "owner", Ct));
        Assert.Equal(2L, await fixture.ScalarAsync("SELECT version FROM bootstrap_scan_format"));
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT enabled FROM sync_mappings"));
        Assert.Equal(BootstrapScanStatus.Interrupted, (await fixture.Store.GetBootstrapScanAsync("legacy", Ct))!.Status);
        Assert.Equal("wal-upload", Assert.Single(await fixture.Store.ListSyncUploadCheckpointsAsync("wal", Ct)).UploadId);
        var manifest = await File.ReadAllBytesAsync(fixture.Database + ".bootstrap-v2-backup.json", Ct);
        var rollback = await fixture.Store.CreateBootstrapV1RollbackCopyAsync(Path.Combine(fixture.Directory, "rollback.db"), Ct);
        await using var rollbackDb = new SqliteConnection("Data Source=" + rollback);
        await rollbackDb.OpenAsync(Ct);
        Assert.Equal(1L, await fixture.ScalarAsync(rollbackDb, "SELECT version FROM bootstrap_scan_format"));
        Assert.Equal("wal-upload", await fixture.ScalarAsync(rollbackDb, "SELECT upload_id FROM sync_upload_checkpoints WHERE scope='wal'"));
        Assert.Equal(0L, await fixture.ScalarAsync(rollbackDb, "SELECT enabled FROM sync_mappings"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.CreateBootstrapV1RollbackCopyAsync(fixture.Database, Ct));
        await fixture.Store.InitializeAsync(Ct);
        Assert.Equal(manifest, await File.ReadAllBytesAsync(fixture.Database + ".bootstrap-v2-backup.json", Ct));
    }

    [Fact]
    public async Task FailedModuleMigrationRollsBackAllDdlAndKeepsBackupBeforeExplicitRepair()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.CreateLegacyModuleAsync();
        await fixture.SqlAsync("CREATE TRIGGER interrupt_migration BEFORE UPDATE OF version ON bootstrap_scan_format BEGIN SELECT RAISE(ABORT,'migration interruption'); END");
        await Assert.ThrowsAsync<SqliteException>(() => fixture.Store.BeginBootstrapScanAsync("new", "mapping", "client", fixture.Root, "owner", Ct));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT version FROM bootstrap_scan_format"));
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM pragma_table_info('bootstrap_scan_runs') WHERE name='retired'"));
        Assert.True(File.Exists(fixture.Database + ".bootstrap-v2-backup.json"));
        await fixture.SqlAsync("DROP TRIGGER interrupt_migration");
        var next = await fixture.Store.BeginBootstrapScanAsync("new", "mapping", "client", fixture.Root, "owner", Ct);
        Assert.Equal(2, next.Generation);
        Assert.Equal(BootstrapScanStatus.Interrupted, (await fixture.Store.GetBootstrapScanAsync("legacy", Ct))!.Status);
    }

    [Fact]
    public async Task PassiveCheckpointDoesNotWaitForLongReaderAndBackpressurePreservesMetadataUntilRelease()
    {
        await using var fixture = await Fixture.CreateAsync();
        var parent = await fixture.BeginAsync("scan");
        await using var writer = await fixture.OpenAsync(pooling: false);
        await fixture.SqlAsync(writer, "PRAGMA wal_autocheckpoint=0; CREATE TABLE budget_probe(payload BLOB); INSERT INTO budget_probe VALUES(zeroblob(1048576))");
        await using var reader = await fixture.OpenAsync(pooling: false);
        using var snapshot = reader.BeginTransaction(deferred: true);
        await using (var pin = reader.CreateCommand()) { pin.Transaction = snapshot; pin.CommandText = "SELECT COUNT(*) FROM budget_probe"; await pin.ExecuteScalarAsync(Ct); }
        await fixture.SqlAsync(writer, "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<80) INSERT INTO budget_probe SELECT zeroblob(1048576) FROM n");
        var timer = Stopwatch.StartNew();
        var blocked = await fixture.Store.CheckBootstrapWalAsync(force: true, Ct);
        Assert.True(blocked.Attempted); Assert.True(blocked.Backpressure);
        Assert.True(blocked.UncheckpointedBytes > NeptuneStateStore.BootstrapWalAdmissionBytes);
        Assert.True(timer.Elapsed < TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<BootstrapStorageBackpressureException>(() => fixture.Store.StageBootstrapEntriesAsync(parent, [Entry("deferred")], Ct));
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_entries"));
        await snapshot.CommitAsync(Ct);
        var recovered = await fixture.Store.CheckBootstrapWalAsync(force: true, Ct);
        Assert.False(recovered.Backpressure); Assert.Equal(recovered.LogFrames, recovered.CheckpointedFrames);
        Assert.True(recovered.PhysicalWalBytes > NeptuneStateStore.BootstrapWalAdmissionBytes);
        Assert.False((await fixture.Store.CheckBootstrapWalAsync(cancellationToken: Ct)).Attempted);
        Assert.Equal(2, fixture.Store.GetBootstrapCheckpointMetrics().Attempts);
        Assert.Equal(1, fixture.Store.GetBootstrapCheckpointMetrics().AdmissionStops);
        await fixture.Store.StageBootstrapEntriesAsync(parent, [Entry("deferred")], Ct);
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_scan_entries"));
        Assert.Equal(2L, await fixture.ScalarAsync("PRAGMA synchronous"));
    }

    private static BootstrapSourceEntry Entry(string name, bool directory = false, long size = 1) => new(name, directory, directory ? 0 : size, At);
    private sealed class OneFileSource : IBootstrapScanSource
    {
        public BootstrapDirectoryStamp ReadDirectoryStamp(string path) => new(At, At);
        public IEnumerable<BootstrapSourceEntry> EnumerateDirectory(string path) { yield return Entry("current"); }
    }

    private sealed class Fixture : IAsyncDisposable
    {
        public string Directory { get; } = Path.Combine(Path.GetTempPath(), "neptune-bootstrap-bounded-tests", Guid.NewGuid().ToString("N"));
        public string Root { get; }
        public string Database { get; }
        public NeptuneStateStore Store { get; }
        private Fixture()
        {
            System.IO.Directory.CreateDirectory(Directory);
            Root = Path.Combine(Directory, "source"); System.IO.Directory.CreateDirectory(Root);
            Database = Path.Combine(Directory, "profile.db"); Store = new(Database);
        }
        public static async Task<Fixture> CreateAsync()
        {
            var fixture = new Fixture(); await fixture.Store.InitializeAsync(Ct);
            await fixture.Store.AddMappingAsync(new("mapping", "client", fixture.Root, true, At), Ct);
            return fixture;
        }
        public async Task<BootstrapDirectory> BeginAsync(string scan)
        {
            await Store.BeginBootstrapScanAsync(scan, "mapping", "client", Root, "owner", Ct);
            return (await Store.ClaimBootstrapDirectoryAsync(scan, "owner", Ct))!;
        }
        public async Task<SqliteConnection> OpenAsync(bool pooling = true)
        {
            var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = Database, Pooling = pooling }.ToString());
            await connection.OpenAsync(Ct); return connection;
        }
        public async Task SqlAsync(string sql) { await using var connection = await OpenAsync(); await SqlAsync(connection, sql); }
        public async Task SqlAsync(SqliteConnection connection, string sql) { await using var command = connection.CreateCommand(); command.CommandText = sql; await command.ExecuteNonQueryAsync(Ct); }
        public async Task<object?> ScalarAsync(string sql) { await using var connection = await OpenAsync(); return await ScalarAsync(connection, sql); }
        public async Task<object?> ScalarAsync(SqliteConnection connection, string sql) { await using var command = connection.CreateCommand(); command.CommandText = sql; return await command.ExecuteScalarAsync(Ct); }
        public async Task<string> ExplainAsync(string sql)
        {
            await using var connection = await OpenAsync(); await using var command = connection.CreateCommand(); command.CommandText = "EXPLAIN QUERY PLAN " + sql;
            await using var reader = await command.ExecuteReaderAsync(Ct); var lines = new List<string>();
            while (await reader.ReadAsync(Ct)) lines.Add(reader.GetString(3)); return string.Join("\n", lines);
        }
        public async Task CreateLegacyModuleAsync()
        {
            await using var connection = await OpenAsync(); await using var command = connection.CreateCommand();
            command.CommandText = """
                CREATE TABLE bootstrap_scan_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL);
                INSERT INTO bootstrap_scan_format VALUES(1,1);
                CREATE TABLE bootstrap_scan_runs(scan_id TEXT PRIMARY KEY,mapping_id TEXT NOT NULL,client_instance_id TEXT NOT NULL,source_root TEXT NOT NULL,owner_token TEXT NOT NULL,status INTEGER NOT NULL,created_at TEXT NOT NULL,generation INTEGER NOT NULL,files INTEGER NOT NULL DEFAULT 0,directories INTEGER NOT NULL DEFAULT 0,bytes INTEGER NOT NULL DEFAULT 0,error TEXT,UNIQUE(mapping_id,generation));
                CREATE UNIQUE INDEX bootstrap_scan_one_active ON bootstrap_scan_runs(mapping_id) WHERE status=0;
                CREATE TABLE bootstrap_scan_directories(directory_id INTEGER PRIMARY KEY AUTOINCREMENT,scan_id TEXT NOT NULL,relative_path TEXT NOT NULL,state INTEGER NOT NULL,UNIQUE(scan_id,relative_path),FOREIGN KEY(scan_id) REFERENCES bootstrap_scan_runs(scan_id) ON DELETE CASCADE);
                CREATE INDEX bootstrap_scan_directory_queue ON bootstrap_scan_directories(scan_id,state,directory_id);
                CREATE UNIQUE INDEX bootstrap_scan_directory_identity ON bootstrap_scan_directories(scan_id,directory_id);
                CREATE TABLE bootstrap_scan_entries(entry_id INTEGER PRIMARY KEY AUTOINCREMENT,scan_id TEXT NOT NULL,parent_directory_id INTEGER NOT NULL,raw_name TEXT NOT NULL,name_key TEXT NOT NULL,relative_path TEXT NOT NULL,kind INTEGER NOT NULL,size INTEGER NOT NULL,modified_at TEXT NOT NULL,eligible INTEGER NOT NULL,UNIQUE(scan_id,parent_directory_id,name_key),FOREIGN KEY(scan_id) REFERENCES bootstrap_scan_runs(scan_id) ON DELETE CASCADE,FOREIGN KEY(scan_id,parent_directory_id) REFERENCES bootstrap_scan_directories(scan_id,directory_id) ON DELETE CASCADE);
                CREATE INDEX bootstrap_scan_eligible ON bootstrap_scan_entries(scan_id,eligible,entry_id);
                INSERT INTO bootstrap_scan_runs VALUES('legacy','mapping','client',$source,'legacy-owner',1,'1970-01-01T00:00:00Z',1,0,0,0,NULL);
                INSERT INTO bootstrap_scan_directories(scan_id,relative_path,state) VALUES('legacy','',2);
                """;
            command.Parameters.AddWithValue("$source", Root); await command.ExecuteNonQueryAsync(Ct);
        }
        public ValueTask DisposeAsync()
        {
            SqliteConnection.ClearAllPools();
            if (!Path.GetFullPath(Directory).StartsWith(Path.GetFullPath(Path.Combine(Path.GetTempPath(), "neptune-bootstrap-bounded-tests")) + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)) throw new InvalidOperationException("Test cleanup escaped its owned directory.");
            System.IO.Directory.Delete(Directory, true); return ValueTask.CompletedTask;
        }
    }
}
