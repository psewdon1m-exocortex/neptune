using Microsoft.Data.Sqlite;
using System.Security.Cryptography;
using System.Text.Json;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class SyncStateMigrationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly string HashA = new('a', 64);
    private static readonly string HashB = new('b', 64);

    [Fact]
    public async Task ActualLegacySchemaMigrationPreservesPauseFilesBackupsAndCheckpointsWithoutInventingBaseline()
    {
        await using var fixture = await Fixture.CreateAsync(initialize: false);
        await fixture.SqlAsync("""
            CREATE TABLE sync_mappings(mapping_id TEXT PRIMARY KEY,client_instance_id TEXT NOT NULL,local_path TEXT NOT NULL,enabled INTEGER NOT NULL,created_at TEXT NOT NULL,UNIQUE(client_instance_id,local_path));
            CREATE TABLE sync_files(mapping_id TEXT NOT NULL,relative_path TEXT NOT NULL,local_size INTEGER NOT NULL,local_mtime_utc TEXT NOT NULL,sha256 TEXT NOT NULL,remote_etag TEXT,state TEXT NOT NULL,updated_at TEXT NOT NULL,error TEXT,PRIMARY KEY(mapping_id,relative_path),FOREIGN KEY(mapping_id) REFERENCES sync_mappings(mapping_id) ON DELETE CASCADE);
            CREATE TABLE sync_upload_checkpoints(scope TEXT NOT NULL,target_uri TEXT NOT NULL,idempotency_key TEXT NOT NULL,fingerprint TEXT NOT NULL,upload_id TEXT,PRIMARY KEY(scope,target_uri));
            CREATE TABLE backup_runs(run_id TEXT PRIMARY KEY,project_id TEXT NOT NULL,client_instance_id TEXT NOT NULL,state TEXT NOT NULL,spool_path TEXT,size INTEGER,sha256 TEXT,saturn_run_id TEXT,uploaded_bytes INTEGER NOT NULL DEFAULT 0,attempt INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,error TEXT);
            INSERT INTO sync_mappings VALUES('mapping-a','client-a','C:/paused-source',0,'2026-01-01');
            INSERT INTO sync_files VALUES('mapping-a','kept.txt',4,'2026-01-01','legacy-hash','legacy-etag','synced','2026-01-01',NULL);
            INSERT INTO sync_upload_checkpoints VALUES('opaque-scope','https://saturn.test/dav/sync/Existing/kept.txt','kept-key','kept-fingerprint','kept-upload');
            INSERT INTO backup_runs(run_id,project_id,client_instance_id,state,created_at,updated_at) VALUES('backup-kept','project','client-a','complete','2026-01-01','2026-01-01');
            """);
        await fixture.Store.InitializeAsync(Ct);
        await fixture.Store.InitializeAsync(Ct);
        Assert.False(Assert.Single(await fixture.Store.ListMappingsAsync(Ct)).Enabled);
        Assert.Equal("C:/paused-source", Assert.Single(await fixture.Store.ListMappingsAsync(Ct)).LocalPath);
        Assert.Equal("legacy-hash", (await fixture.Store.GetSyncFileAsync("mapping-a", "kept.txt", Ct))!.Sha256);
        Assert.Equal("kept-upload", Assert.Single(await fixture.Store.ListSyncUploadCheckpointsAsync("opaque-scope", Ct)).UploadId);
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT COUNT(*) FROM backup_runs WHERE run_id='backup-kept' AND export_generation IS NULL"));
        var root = await fixture.Store.GetSyncRootStateAsync("mapping-a", Ct);
        Assert.Null(root.Identity);
        Assert.True(root.NeedsReconciliation);
        Assert.False(root.MappingEnabled);
        Assert.Null((await fixture.Store.GetSyncEntryStateAsync("mapping-a", "kept.txt", Ct)).Baseline);
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM sync_state_baselines"));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT COUNT(*) FROM sync_state_format WHERE version=1"));
    }

    [Fact]
    public async Task MigrationFailureRollsBackAdditiveSchemaAndCanBeRetried()
    {
        await using var fixture = await Fixture.CreateAsync(initialize: false);
        await fixture.SqlAsync("CREATE TABLE sync_mappings(mapping_id TEXT PRIMARY KEY,client_instance_id TEXT NOT NULL,local_path TEXT NOT NULL,enabled INTEGER NOT NULL,created_at TEXT NOT NULL,UNIQUE(client_instance_id,local_path)); CREATE TABLE sync_state_entries(bad_column TEXT)");
        await Assert.ThrowsAsync<SqliteException>(() => fixture.Store.InitializeAsync(Ct));
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM sqlite_master WHERE name='sync_state_format'"));
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM sqlite_master WHERE name='sync_state_roots'"));
        await fixture.SqlAsync("DROP TABLE sync_state_entries");
        await fixture.Store.InitializeAsync(Ct);
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT COUNT(*) FROM sync_state_format WHERE version=1"));
    }

    [Fact]
    public async Task FutureFormatRefusesStateWritesAndInitializationWithoutChangingLegacyData()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.SqlAsync("UPDATE sync_state_format SET version=2");
        await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Store.InitializeAsync(Ct));
        await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Store.GetSyncRootStateAsync(fixture.MappingId, Ct));
        Assert.Equal(2L, await fixture.ScalarAsync("SELECT version FROM sync_state_format"));
        Assert.Single(await fixture.Store.ListMappingsAsync(Ct));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FutureFormatWithoutLegacyTablesRefusesBeforeAnyMutationIncludingCommittedWal(bool wal)
    {
        await using var fixture = await Fixture.CreateAsync(initialize: false);
        await using var keeper = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = fixture.Database, Pooling = false }.ToString());
        await keeper.OpenAsync(Ct);
        await using var seed = keeper.CreateCommand();
        seed.CommandText = (wal ? "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;" : "") +
            "CREATE TABLE sync_state_format(singleton INTEGER PRIMARY KEY,version INTEGER NOT NULL,migrated_at TEXT); INSERT INTO sync_state_format VALUES(1,2,'future'); CREATE TABLE future_payload(value TEXT); INSERT INTO future_payload VALUES('preserved');";
        await seed.ExecuteNonQueryAsync(Ct);
        var main = await ReadSharedBytesAsync(fixture.Database);
        var journal = wal ? await ReadSharedBytesAsync(fixture.Database + "-wal") : null;
        await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Store.InitializeAsync(Ct));
        Assert.Equal(main, await ReadSharedBytesAsync(fixture.Database));
        if (wal) Assert.Equal(journal, await ReadSharedBytesAsync(fixture.Database + "-wal"));
        Assert.Equal(2L, await fixture.ScalarAsync("SELECT COUNT(*) FROM sqlite_master WHERE type='table'"));
        Assert.Equal("preserved", await fixture.ScalarAsync("SELECT value FROM future_payload"));
        Assert.False(File.Exists(fixture.Database + ".sync-state-v1-backup.json"));
        Assert.False(File.Exists(fixture.Database + ".sync-state-migration.lock"));
    }

    [Fact]
    public async Task CorruptOrUnknownDatabaseRefusesWithoutChangingBytes()
    {
        await using var corrupt = await Fixture.CreateAsync(initialize: false);
        var bytes = System.Text.Encoding.UTF8.GetBytes("not a SQLite database, preserve this evidence");
        await File.WriteAllBytesAsync(corrupt.Database, bytes, Ct);
        await Assert.ThrowsAsync<InvalidDataException>(() => corrupt.Store.InitializeAsync(Ct));
        Assert.Equal(bytes, await File.ReadAllBytesAsync(corrupt.Database, Ct));
        await using var unknown = await Fixture.CreateAsync(initialize: false);
        await unknown.SqlAsync("CREATE TABLE unrelated(value TEXT); INSERT INTO unrelated VALUES('keep')");
        SqliteConnection.ClearAllPools();
        var before = await File.ReadAllBytesAsync(unknown.Database, Ct);
        await Assert.ThrowsAsync<InvalidDataException>(() => unknown.Store.InitializeAsync(Ct));
        Assert.Equal(before, await File.ReadAllBytesAsync(unknown.Database, Ct));
    }

    [Fact]
    public async Task LegacyBackupIncludesCommittedWalAndRollbackIsASeparateVerifiedIdempotentCopy()
    {
        await using var fixture = await Fixture.CreateAsync(initialize: false);
        await using var keeper = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = fixture.Database, Pooling = false }.ToString());
        await keeper.OpenAsync(Ct);
        await using var seed = keeper.CreateCommand();
        seed.CommandText = """
            PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
            CREATE TABLE sync_mappings(mapping_id TEXT PRIMARY KEY,client_instance_id TEXT NOT NULL,local_path TEXT NOT NULL,enabled INTEGER NOT NULL,created_at TEXT NOT NULL,UNIQUE(client_instance_id,local_path));
            INSERT INTO sync_mappings VALUES('mapping-a','client-a','C:/paused',0,'2026-01-01');
            """;
        await seed.ExecuteNonQueryAsync(Ct);
        Assert.True(new FileInfo(fixture.Database + "-wal").Length > 0);
        await fixture.Store.InitializeAsync(Ct);
        var manifestPath = fixture.Database + ".sync-state-v1-backup.json";
        var manifestBytes = await File.ReadAllBytesAsync(manifestPath, Ct);
        using var manifest = JsonDocument.Parse(manifestBytes);
        var backup = manifest.RootElement.GetProperty("backupPath").GetString()!;
        Assert.Equal(manifest.RootElement.GetProperty("sha256").GetString(), Convert.ToHexStringLower(SHA256.HashData(await File.ReadAllBytesAsync(backup, Ct))));
        await fixture.Store.InitializeAsync(Ct);
        Assert.Equal(manifestBytes, await File.ReadAllBytesAsync(manifestPath, Ct));
        var destination = fixture.Database + ".rollback.db";
        Assert.Equal(Path.GetFullPath(destination), await fixture.Store.CreateLegacySyncRollbackCopyAsync(destination, Ct));
        await using var rollback = new SqliteConnection("Data Source=" + destination + ";Mode=ReadOnly;Pooling=False");
        await rollback.OpenAsync(Ct);
        await using var query = rollback.CreateCommand();
        query.CommandText = "SELECT enabled FROM sync_mappings WHERE mapping_id='mapping-a'";
        Assert.Equal(0L, await query.ExecuteScalarAsync(Ct));
        query.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE name='sync_state_format'";
        Assert.Equal(0L, await query.ExecuteScalarAsync(Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.CreateLegacySyncRollbackCopyAsync(fixture.Database, Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.CreateLegacySyncRollbackCopyAsync(destination, Ct));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT version FROM sync_state_format"));
    }

    [Fact]
    public async Task BrokenKnownFormatSchemaRefusesBeforeRecreatingDeletedSafetyGuards()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.SqlAsync("DROP TRIGGER sync_state_legacy_file_update");
        SqliteConnection.ClearAllPools();
        var bytes = await File.ReadAllBytesAsync(fixture.Database, Ct);
        await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Store.InitializeAsync(Ct));
        Assert.Equal(bytes, await File.ReadAllBytesAsync(fixture.Database, Ct));
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM sqlite_master WHERE name='sync_state_legacy_file_update'"));
    }

    [Fact]
    public async Task ExplicitIntegrityCheckDetectsForeignKeyViolationWithoutReinitializingTheProfile()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.Store.VerifySyncIntegrityAsync(Ct);
        await using (var writer = new SqliteConnection("Data Source=" + fixture.Database + ";Foreign Keys=False;Pooling=False"))
        {
            await writer.OpenAsync(Ct);
            await using var insert = writer.CreateCommand();
            insert.CommandText = "INSERT INTO sync_state_entries(mapping_id,entry_id,name_key,local_generation,presence,local_json) VALUES('missing-root','orphan','orphan',1,1,'{}')";
            await insert.ExecuteNonQueryAsync(Ct);
        }
        await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Store.VerifySyncIntegrityAsync(Ct));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT COUNT(*) FROM sync_state_entries WHERE mapping_id='missing-root'"));
    }

    [Fact]
    public async Task ConcurrentInitializationAndRootBindingAreIdempotentButCannotRebindDeviceOrVolume()
    {
        await using var fixture = await Fixture.CreateAsync();
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() => new NeptuneStateStore(fixture.Database).InitializeAsync(Ct), Ct)));
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() => new NeptuneStateStore(fixture.Database).RegisterSyncRootAsync(fixture.Identity, Ct), Ct)));
        Assert.Equal(fixture.Identity, (await fixture.Store.GetSyncRootStateAsync(fixture.MappingId, Ct)).Identity);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.RegisterSyncRootAsync(fixture.Identity with { DeviceId = "another-device" }, Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.RegisterSyncRootAsync(fixture.Identity with { SourceVolumeIdentity = "another-volume" }, Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.RegisterSyncRootAsync(fixture.Identity with { NameCodec = "portable_v1" }, Ct));
    }

    [Fact]
    public async Task OneProfileCannotGiveTwoMappingsOwnershipOfOneServerRoot()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.Store.AddMappingAsync(new("mapping-b", "client-a", "C:/other-source", true, DateTimeOffset.UtcNow), Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.RegisterSyncRootAsync(fixture.Identity with { MappingId = "mapping-b" }, Ct));
        Assert.Null((await fixture.Store.GetSyncRootStateAsync("mapping-b", Ct)).Identity);
        await fixture.Store.RegisterSyncRootAsync(fixture.Identity with { MappingId = "mapping-b", ServerOrigin = new Uri("https://another-saturn.test/") }, Ct);
    }

    [Fact]
    public async Task CursorAndDirtyBatchAreAtomicAcrossAnInterruptedTransactionAndRestart()
    {
        await using var fixture = await Fixture.CreateAsync();
        var cursor = new SyncJournalCursor("journal-a", 10, true);
        await fixture.Store.CaptureSyncJournalAsync(fixture.MappingId, null, cursor, [], Ct);
        await fixture.SqlAsync("CREATE TRIGGER abort_dirty BEFORE INSERT ON sync_state_dirty WHEN NEW.entry_id='reject' BEGIN SELECT RAISE(ABORT,'injected interrupt'); END");
        var hints = new[] { Hint("first", 1), Hint("reject", 1) };
        await Assert.ThrowsAsync<SqliteException>(() => fixture.Store.CaptureSyncJournalAsync(fixture.MappingId, cursor, cursor with { Position = 20 }, hints, Ct));
        var reopened = new NeptuneStateStore(fixture.Database);
        Assert.Equal(cursor, await reopened.GetSyncJournalCursorAsync(fixture.MappingId, Ct));
        Assert.Empty(await reopened.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct));
        await fixture.SqlAsync("DROP TRIGGER abort_dirty");
        await reopened.CaptureSyncJournalAsync(fixture.MappingId, cursor, cursor with { Position = 20 }, hints, Ct);
        Assert.Equal(20, (await reopened.GetSyncJournalCursorAsync(fixture.MappingId, Ct))!.Position);
        Assert.Equal(2, (await reopened.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct)).Count);
    }

    [Fact]
    public async Task CursorCompareAndSwapPreservesFirstDirtyAndNewerObservedGeneration()
    {
        await using var fixture = await Fixture.CreateAsync();
        var first = Hint("entry-a", 1);
        var cursor = new SyncJournalCursor("journal-a", 10, true);
        await fixture.Store.CaptureSyncJournalAsync(fixture.MappingId, null, cursor, [first], Ct);
        var next = cursor with { Position = 20 };
        await fixture.Store.CaptureSyncJournalAsync(fixture.MappingId, cursor, next, [first with { ObservedGeneration = 2, FirstDirtyAt = first.FirstDirtyAt.AddSeconds(1), DueAt = first.DueAt.AddSeconds(1), Reasons = SyncDirtyReason.Rename }], Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.CaptureSyncJournalAsync(fixture.MappingId, cursor, cursor with { Position = 30 }, [Hint("lost", 3)], Ct));
        var dirty = Assert.Single(await fixture.Store.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct));
        Assert.Equal(2, dirty.ObservedGeneration);
        Assert.Equal(first.FirstDirtyAt.ToUnixTimeMilliseconds(), dirty.FirstDirtyAt.ToUnixTimeMilliseconds());
        Assert.Equal(SyncDirtyReason.LocalChange | SyncDirtyReason.Rename, dirty.Reasons);
        Assert.Equal(next, await fixture.Store.GetSyncJournalCursorAsync(fixture.MappingId, Ct));
    }

    [Fact]
    public async Task JournalResetPersistsUncertaintyInsteadOfPretendingContinuity()
    {
        await using var fixture = await Fixture.CreateAsync();
        var old = new SyncJournalCursor("journal-a", 10, true);
        await fixture.Store.CaptureSyncJournalAsync(fixture.MappingId, null, old, [], Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.CaptureSyncJournalAsync(fixture.MappingId, old, new("journal-b", 0, true), [], Ct));
        var reset = new SyncJournalCursor("journal-b", 0, false);
        await fixture.Store.CaptureSyncJournalAsync(fixture.MappingId, old, reset, [Hint("pending-path", 1) with { Reasons = SyncDirtyReason.JournalGap }], Ct);
        Assert.Equal(reset, await fixture.Store.GetSyncJournalCursorAsync(fixture.MappingId, Ct));
        Assert.True((await fixture.Store.GetSyncRootStateAsync(fixture.MappingId, Ct)).NeedsReconciliation);
        Assert.Equal(1, (await fixture.Store.GetSyncRootStateAsync(fixture.MappingId, Ct)).InvalidationGeneration);
    }

    [Fact]
    public async Task LegacyWritesInvalidateCursorAndPlannedWorkWithoutDeletingAgreedHistory()
    {
        await using var fixture = await Fixture.CreateAsync();
        var local = Entry();
        var first = await fixture.PlanAsync(local, "operation-a");
        await fixture.Store.BeginSyncOperationAsync(first.Intent.OperationId, "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(first), Ct);
        var newer = local with { LocalGeneration = 2, HashGeneration = 2, Sha256 = HashB };
        await fixture.Store.ObserveSyncLocalEntriesAsync(fixture.MappingId, [newer], Ct);
        var second = await fixture.Store.PlanSyncOperationAsync(Intent(fixture, newer, "operation-b", new(SyncExpectedKind.Version, "node-a", "version-1")), Ct);
        await fixture.Store.CaptureSyncJournalAsync(fixture.MappingId, null, new("journal", 10, true), [], Ct);
        await fixture.Store.UpsertSyncFileAsync(new(fixture.MappingId, "old-writer.txt", 4, DateTimeOffset.UtcNow, "legacy", "legacy-etag", "synced", DateTimeOffset.UtcNow, null), Ct);
        var root = await fixture.Store.GetSyncRootStateAsync(fixture.MappingId, Ct);
        Assert.True(root.NeedsReconciliation);
        Assert.Equal(1, root.LegacyRevision);
        Assert.False((await fixture.Store.GetSyncJournalCursorAsync(fixture.MappingId, Ct))!.Valid);
        Assert.Equal("version-1", (await fixture.Store.GetSyncEntryStateAsync(fixture.MappingId, local.EntryId, Ct)).Baseline!.RemoteVersion);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.BeginSyncOperationAsync(second.Intent.OperationId, "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.PlanSyncOperationAsync(second.Intent with { OperationId = "operation-c" }, Ct));
    }

    [Fact]
    public async Task OpaqueLegacyCheckpointWriteInvalidatesRootsButRetainsTheCheckpoint()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.Store.CaptureSyncJournalAsync(fixture.MappingId, null, new("journal", 1, true), [], Ct);
        var checkpoint = new SyncUploadCheckpoint("scope", "https://saturn.test/dav/sync/Legacy/file.bin", "legacy-key", "legacy-fingerprint", "upload-id");
        await fixture.Store.SaveSyncUploadCheckpointAsync(checkpoint, Ct);
        Assert.Equal(checkpoint, Assert.Single(await fixture.Store.ListSyncUploadCheckpointsAsync("scope", Ct)));
        Assert.Equal(1, (await fixture.Store.GetSyncRootStateAsync(fixture.MappingId, Ct)).LegacyRevision);
        Assert.False((await fixture.Store.GetSyncJournalCursorAsync(fixture.MappingId, Ct))!.Valid);
    }

    [Fact]
    public async Task LegacyMappingRemovalRetainsDurableOperationsAndBaselinesForRecovery()
    {
        await using var fixture = await Fixture.CreateAsync();
        var operation = await fixture.PlanAsync(Entry(), "operation");
        await fixture.Store.BeginSyncOperationAsync("operation", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(operation), Ct);
        await fixture.Store.RemoveMappingAsync(fixture.MappingId, "client-a", Ct);
        Assert.False((await fixture.Store.GetSyncRootStateAsync(fixture.MappingId, Ct)).MappingEnabled);
        Assert.NotNull(await fixture.Store.GetSyncOperationAsync("operation", Ct));
        Assert.NotNull((await fixture.Store.GetSyncEntryStateAsync(fixture.MappingId, "entry-a", Ct)).Baseline);
    }

    [Fact]
    public async Task PublicationAcknowledgesOnlyItsGenerationAndPreservesNewerLocalAndRemoteState()
    {
        await using var fixture = await Fixture.CreateAsync();
        var first = Entry();
        var operation = await fixture.PlanAsync(first, "operation-a");
        await fixture.Store.BeginSyncOperationAsync("operation-a", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.Store.ObserveSyncLocalEntriesAsync(fixture.MappingId, [first with { LocalGeneration = 2, HashGeneration = 2, Sha256 = HashB }], Ct);
        await fixture.Store.ApplySyncRemoteEntriesAsync(fixture.MappingId, "epoch-a", [Remote(first) with { Version = "peer-version", LastSequence = 20, Sha256 = HashB }], Ct);
        var receipt = Receipt(operation);
        await fixture.Store.ApplySyncPublishedReceiptAsync(receipt, Ct);
        await fixture.Store.ApplySyncPublishedReceiptAsync(receipt, Ct);
        var state = await new NeptuneStateStore(fixture.Database).GetSyncEntryStateAsync(fixture.MappingId, "entry-a", Ct);
        Assert.Equal(2, state.Local!.LocalGeneration);
        Assert.Equal(1, state.Baseline!.AgreedLocalGeneration);
        Assert.Equal("version-1", state.Baseline.RemoteVersion);
        Assert.Equal("peer-version", state.Remote!.Version);
        Assert.Equal(2, Assert.Single(await fixture.Store.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct)).ObservedGeneration);
        Assert.Equal(SyncOperationStage.ClientAcked, (await fixture.Store.GetSyncOperationAsync("operation-a", Ct))!.Stage);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.PlanSyncOperationAsync(Intent(fixture, state.Local, "overwrite-peer", new(SyncExpectedKind.Version, "node-a", "peer-version")), Ct));
    }

    [Fact]
    public async Task NewerRemoteRemainsDirtyWhenLateReceiptAcknowledgesTheSameLocalGeneration()
    {
        await using var fixture = await Fixture.CreateAsync();
        var initial = await fixture.PlanAsync(Entry(), "initial");
        await fixture.Store.BeginSyncOperationAsync("initial", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(initial), Ct);
        var operation = await fixture.Store.PlanSyncOperationAsync(Intent(fixture, Entry(), "later", new(SyncExpectedKind.Version, "node-a", "version-1")), Ct);
        await fixture.Store.BeginSyncOperationAsync("later", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.Store.ApplySyncRemoteEntriesAsync(fixture.MappingId, "epoch-a", [Remote(Entry()) with { Version = "peer-20", LastSequence = 20, Sha256 = HashB }], Ct);
        Assert.Equal(SyncDirtyReason.RemoteChange, Assert.Single(await fixture.Store.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct)).Reasons);
        var receipt = Receipt(operation) with { PublishedSequence = 10, Version = "version-10" };
        await fixture.Store.ApplySyncPublishedReceiptAsync(receipt, Ct);
        await fixture.Store.ObserveSyncLocalEntriesAsync(fixture.MappingId, [Entry()], Ct);
        var state = await fixture.Store.GetSyncEntryStateAsync(fixture.MappingId, "entry-a", Ct);
        Assert.Equal("version-10", state.Baseline!.RemoteVersion);
        Assert.Equal("peer-20", state.Remote!.Version);
        Assert.Equal(SyncDirtyReason.RemoteChange, Assert.Single(await fixture.Store.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct)).Reasons);
        await fixture.Store.ApplySyncPublishedReceiptAsync(receipt, Ct);
        Assert.Single(await fixture.Store.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct));
    }

    [Fact]
    public async Task UnsentIntentCannotBeAcknowledgedAndUnchangedObservationDoesNotRedirtyAcknowledgedEntry()
    {
        await using var fixture = await Fixture.CreateAsync();
        var planned = await fixture.PlanAsync(Entry(), "operation");
        await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(planned), Ct));
        await fixture.Store.BeginSyncOperationAsync("operation", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(planned), Ct);
        await fixture.Store.ObserveSyncLocalEntriesAsync(fixture.MappingId, [Entry()], Ct);
        Assert.Empty(await fixture.Store.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct));
    }

    [Fact]
    public async Task OutOfOrderReceiptsCannotRollBackAnAlreadyAgreedNewerGenerationOrCreateFalseDirty()
    {
        await using var fixture = await Fixture.CreateAsync();
        var old = await fixture.PlanAsync(Entry(), "old");
        await fixture.Store.BeginSyncOperationAsync("old", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        var second = Entry() with { LocalGeneration = 2, HashGeneration = 2, Sha256 = HashB };
        var newer = await fixture.PlanAsync(second, "newer");
        await fixture.Store.BeginSyncOperationAsync("newer", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(newer) with { Version = "version-20", PublishedSequence = 20 }, Ct);
        Assert.Empty(await fixture.Store.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct));
        await fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(old) with { PublishedSequence = 10 }, Ct);
        var state = await fixture.Store.GetSyncEntryStateAsync(fixture.MappingId, "entry-a", Ct);
        Assert.Equal(2, state.Baseline!.AgreedLocalGeneration);
        Assert.Equal("version-20", state.Remote!.Version);
        Assert.Empty(await fixture.Store.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct));
    }

    [Fact]
    public async Task RemoteDirtyUsesIndexedRootScopedNodeMatchingAndDuplicateFeedDoesNotRedirty()
    {
        await using var fixture = await Fixture.CreateAsync();
        var first = await fixture.PlanAsync(Entry(), "first");
        await fixture.Store.BeginSyncOperationAsync("first", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(first), Ct);
        await fixture.Store.AddMappingAsync(new("mapping-b", "client-a", "C:/other", true, DateTimeOffset.UtcNow), Ct);
        await fixture.Store.RegisterSyncRootAsync(fixture.Identity with { MappingId = "mapping-b", ServerRootId = "other-root" }, Ct);
        await fixture.Store.ObserveSyncLocalEntriesAsync("mapping-b", [Entry()], Ct);
        var other = await fixture.Store.PlanSyncOperationAsync(Intent(fixture, Entry(), "other", new(SyncExpectedKind.Absent)) with { MappingId = "mapping-b" }, Ct);
        await fixture.Store.BeginSyncOperationAsync("other", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(other) with { ServerRootId = "other-root" }, Ct);
        var changed = Remote(Entry()) with { LastSequence = 2, Version = "version-2" };
        await fixture.Store.ApplySyncRemoteEntriesAsync(fixture.MappingId, "epoch-a", [changed], Ct);
        Assert.Single(await fixture.Store.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct));
        Assert.Empty(await fixture.Store.ListSyncDirtyEntriesAsync("mapping-b", cancellationToken: Ct));
        Assert.Contains("sync_state_baselines_remote", await fixture.QueryPlanAsync("SELECT entry_id FROM sync_state_baselines WHERE mapping_id='mapping-a' AND remote_node_id='node-a'"));
        await fixture.SqlAsync("DELETE FROM sync_state_dirty WHERE mapping_id='mapping-a'");
        await fixture.Store.ApplySyncRemoteEntriesAsync(fixture.MappingId, "epoch-a", [changed], Ct);
        Assert.Empty(await fixture.Store.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct));
    }

    [Theory]
    [InlineData("missing")]
    [InlineData("absent")]
    [InlineData("file")]
    public async Task MissingOrAbsentOrFileParentsCannotBecomeLocalParents(string parentState)
    {
        await using var fixture = await Fixture.CreateAsync();
        var parent = Entry("parent", "parent") with { Kind = SyncEntryKind.Directory, Sha256 = null, HashGeneration = null, Size = 0 };
        if (parentState != "missing")
            await fixture.Store.ObserveSyncLocalEntriesAsync(fixture.MappingId, [parentState == "absent" ? parent with { Presence = SyncEntryPresence.Absent } : Entry("parent", "parent")], Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.ObserveSyncLocalEntriesAsync(fixture.MappingId, [Entry() with { ParentEntryId = "parent" }], Ct));
        Assert.Null((await fixture.Store.GetSyncEntryStateAsync(fixture.MappingId, "entry-a", Ct)).Local);
    }

    [Fact]
    public async Task FeedDoesNotCreateBaselineAndLateSnapshotDoesNotReplaceNewerRemoteDescriptor()
    {
        await using var fixture = await Fixture.CreateAsync();
        var local = Entry();
        await fixture.Store.ObserveSyncLocalEntriesAsync(fixture.MappingId, [local], Ct);
        await fixture.Store.ApplySyncRemoteEntriesAsync(fixture.MappingId, "epoch-a", [Remote(local) with { LastSequence = 20, Version = "version-20" }], Ct);
        await fixture.Store.ApplySyncRemoteEntriesAsync(fixture.MappingId, "epoch-a", [Remote(local)], Ct);
        Assert.Null((await fixture.Store.GetSyncEntryStateAsync(fixture.MappingId, "entry-a", Ct)).Baseline);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.PlanSyncOperationAsync(Intent(fixture, local, "unsafe", new(SyncExpectedKind.Version, "node-a", "version-20")), Ct));
        var adoption = await fixture.Store.PlanSyncOperationAsync(Intent(fixture, local, "adopt", new(SyncExpectedKind.Version, "node-a", "version-20")) with { Kind = SyncIntentKind.AdoptEqual }, Ct);
        await fixture.Store.BeginSyncOperationAsync("adopt", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(adoption) with { Version = "version-20", PublishedSequence = 20 }, Ct);
        Assert.Equal("version-20", (await fixture.Store.GetSyncEntryStateAsync(fixture.MappingId, "entry-a", Ct)).Remote!.Version);
    }

    [Fact]
    public async Task ReceiptAndBaselineDirtyAcknowledgementRollBackTogetherOnInterrupt()
    {
        await using var fixture = await Fixture.CreateAsync();
        var operation = await fixture.PlanAsync(Entry(), "operation-a");
        await fixture.Store.BeginSyncOperationAsync("operation-a", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.SqlAsync("CREATE TRIGGER abort_ack BEFORE UPDATE OF receipt_json ON sync_state_operations BEGIN SELECT RAISE(ABORT,'injected interrupt'); END");
        await Assert.ThrowsAsync<SqliteException>(() => fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(operation), Ct));
        var restarted = new NeptuneStateStore(fixture.Database);
        Assert.Null((await restarted.GetSyncEntryStateAsync(fixture.MappingId, "entry-a", Ct)).Baseline);
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM sync_state_remote"));
        Assert.Single(await restarted.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct));
        Assert.Equal(SyncOperationStage.Sending, (await restarted.GetSyncOperationAsync("operation-a", Ct))!.Stage);
        await fixture.SqlAsync("DROP TRIGGER abort_ack");
        await restarted.ApplySyncPublishedReceiptAsync(Receipt(operation), Ct);
        Assert.Empty(await restarted.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct));
        Assert.Equal(SyncOperationStage.ClientAcked, (await restarted.GetSyncOperationAsync("operation-a", Ct))!.Stage);
    }

    [Theory]
    [InlineData("root")]
    [InlineData("epoch")]
    [InlineData("fingerprint")]
    [InlineData("generation")]
    [InlineData("hash")]
    [InlineData("received")]
    [InlineData("durability")]
    public async Task ReceiptMustProveTheExactPublishedIntent(string defect)
    {
        await using var fixture = await Fixture.CreateAsync();
        var operation = await fixture.PlanAsync(Entry(), "operation");
        var valid = Receipt(operation);
        var wrong = defect switch
        {
            "root" => valid with { ServerRootId = "other-root" },
            "epoch" => valid with { RootEpoch = "other-epoch" },
            "fingerprint" => valid with { IntentFingerprint = HashB },
            "generation" => valid with { LocalGeneration = 2 },
            "hash" => valid with { Sha256 = HashB },
            "received" => valid with { Published = false },
            _ => valid with { Durability = "received" },
        };
        await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Store.ApplySyncPublishedReceiptAsync(wrong, Ct));
        Assert.Null((await fixture.Store.GetSyncEntryStateAsync(fixture.MappingId, "entry-a", Ct)).Baseline);
        Assert.Equal(SyncOperationStage.LocalPlanned, (await fixture.Store.GetSyncOperationAsync("operation", Ct))!.Stage);
    }

    [Fact]
    public async Task OperationIdsAreImmutableIdempotentAndDependenciesCannotCrossRootsOrCycle()
    {
        await using var fixture = await Fixture.CreateAsync();
        var directory = Entry("directory", "dir") with { Kind = SyncEntryKind.Directory, Size = 0, Sha256 = null, HashGeneration = null };
        await fixture.Store.ObserveSyncLocalEntriesAsync(fixture.MappingId, [directory, Entry()], Ct);
        var parent = await fixture.Store.PlanSyncOperationAsync(Intent(fixture, directory, "parent", new(SyncExpectedKind.Absent)) with { Kind = SyncIntentKind.EnsureDirectory, SourceSnapshotIdentity = null }, Ct);
        var childIntent = Intent(fixture, Entry(), "child", new(SyncExpectedKind.Absent)) with { Dependencies = new[] { "parent" } };
        var child = await fixture.Store.PlanSyncOperationAsync(childIntent, Ct);
        Assert.Equal(child.Fingerprint, (await fixture.Store.PlanSyncOperationAsync(childIntent, Ct)).Fingerprint);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.PlanSyncOperationAsync(childIntent with { SourceSnapshotIdentity = "another-snapshot" }, Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => fixture.Store.PlanSyncOperationAsync(childIntent with { Dependencies = new[] { "child" } }, Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.PlanSyncOperationAsync(childIntent with { OperationId = "missing-dependency", Dependencies = new[] { "unknown" } }, Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.BeginSyncOperationAsync("child", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct));
        await fixture.Store.BeginSyncOperationAsync("parent", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(parent) with { NodeId = "node-directory" }, Ct);
        await fixture.Store.BeginSyncOperationAsync("child", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
    }

    [Fact]
    public async Task ExpiredLeaseBecomesOutcomeUnknownAndRequiresStatusBeforeResending()
    {
        await using var fixture = await Fixture.CreateAsync();
        var planned = await fixture.PlanAsync(Entry(), "operation");
        var deadline = DateTimeOffset.UtcNow.AddSeconds(10);
        await fixture.Store.BeginSyncOperationAsync("operation", "first-worker", deadline, Ct);
        Assert.Equal(1, await new NeptuneStateStore(fixture.Database).RecoverExpiredSyncOperationsAsync(fixture.MappingId, deadline.AddSeconds(1), Ct));
        Assert.Equal(SyncOperationStage.OutcomeUnknown, (await fixture.Store.GetSyncOperationAsync("operation", Ct))!.Stage);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.BeginSyncOperationAsync("operation", "second-worker", deadline.AddMinutes(1), Ct));
        await fixture.Store.ConfirmSyncOperationAbsentAsync("operation", cancelled: false, Ct);
        var resumed = await fixture.Store.BeginSyncOperationAsync("operation", "second-worker", deadline.AddMinutes(1), Ct);
        Assert.Equal(2, resumed.Attempt);
        Assert.Equal(planned.Fingerprint, resumed.Fingerprint);
    }

    [Fact]
    public async Task PreviouslySentReadyOperationCanAcknowledgeLatePublicationAfterAnAbsenceStatus()
    {
        await using var fixture = await Fixture.CreateAsync();
        var planned = await fixture.PlanAsync(Entry(), "operation");
        await fixture.Store.BeginSyncOperationAsync("operation", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.Store.MarkSyncOperationOutcomeUnknownAsync("operation", Ct);
        await fixture.Store.ConfirmSyncOperationAbsentAsync("operation", cancelled: false, Ct);
        // Status was absent at its linearization point. An older delayed request
        // may subsequently arrive, so its exact publication proof still wins.
        await fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(planned), Ct);
        Assert.Equal(SyncOperationStage.ClientAcked, (await fixture.Store.GetSyncOperationAsync("operation", Ct))!.Stage);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.BeginSyncOperationAsync("operation", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct));
        Assert.Empty(await fixture.Store.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct));
    }

    [Fact]
    public async Task CancelledUnsentWorkRetainsDirtyAndSendingCancellationCanStillAcknowledgePublication()
    {
        await using var fixture = await Fixture.CreateAsync();
        var local = Entry();
        await fixture.PlanAsync(local, "unsent");
        await fixture.Store.CancelSyncOperationAsync("unsent", Ct);
        Assert.Equal(SyncOperationStage.Cancelled, (await fixture.Store.GetSyncOperationAsync("unsent", Ct))!.Stage);
        Assert.Single(await fixture.Store.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct));
        var sent = await fixture.Store.PlanSyncOperationAsync(Intent(fixture, local, "sent", new(SyncExpectedKind.Absent)), Ct);
        await fixture.Store.BeginSyncOperationAsync("sent", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.Store.CancelSyncOperationAsync("sent", Ct);
        Assert.Equal(SyncOperationStage.CancelRequested, (await fixture.Store.GetSyncOperationAsync("sent", Ct))!.Stage);
        await fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(sent), Ct);
        Assert.Equal(SyncOperationStage.ClientAcked, (await fixture.Store.GetSyncOperationAsync("sent", Ct))!.Stage);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.CancelSyncOperationAsync("sent", Ct));
    }

    [Fact]
    public async Task ObservedAbsenceCannotCancelSentWorkWithoutDurableRootScopedFence()
    {
        await using var fixture = await Fixture.CreateAsync();
        var planned = await fixture.PlanAsync(Entry(), "operation");
        await fixture.Store.BeginSyncOperationAsync("operation", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct);
        await fixture.Store.CancelSyncOperationAsync("operation", Ct);
        await fixture.Store.ConfirmSyncOperationAbsentAsync("operation", cancelled: false, Ct);
        Assert.Equal(SyncOperationStage.CancelRequested, (await fixture.Store.GetSyncOperationAsync("operation", Ct))!.Stage);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.ConfirmSyncOperationAbsentAsync("operation", cancelled: true, Ct));
        var fence = new SyncCancellationFence("operation", "server-root", "epoch-a", planned.Fingerprint, "durable-fence");
        await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Store.ConfirmSyncCancellationFenceAsync(fence with { RootEpoch = "other-epoch" }, Ct));
        await fixture.Store.ConfirmSyncCancellationFenceAsync(fence, Ct);
        await new NeptuneStateStore(fixture.Database).ConfirmSyncCancellationFenceAsync(fence, Ct);
        var cancelled = await fixture.Store.GetSyncOperationAsync("operation", Ct);
        Assert.Equal(SyncOperationStage.Cancelled, cancelled!.Stage);
        Assert.Equal(fence, cancelled.CancellationFence);
        Assert.Single(await fixture.Store.ListSyncDirtyEntriesAsync(fixture.MappingId, cancellationToken: Ct));
        await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Store.ApplySyncPublishedReceiptAsync(Receipt(planned), Ct));
    }

    [Fact]
    public async Task ConcurrentWorkersCannotSendOneIntentTwiceAndPausedMappingCannotSend()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.PlanAsync(Entry(), "operation");
        var succeeded = 0;
        await Task.WhenAll(Enumerable.Range(0, 8).Select(index => Task.Run(async () =>
        {
            try { await new NeptuneStateStore(fixture.Database).BeginSyncOperationAsync("operation", "worker-" + index, DateTimeOffset.UtcNow.AddMinutes(1), Ct); Interlocked.Increment(ref succeeded); }
            catch (InvalidOperationException) { }
        }, Ct)));
        Assert.Equal(1, succeeded);
        await fixture.Store.ObserveSyncLocalEntriesAsync(fixture.MappingId, [Entry("second", "second.txt")], Ct);
        await fixture.Store.PlanSyncOperationAsync(Intent(fixture, Entry("second", "second.txt"), "second", new(SyncExpectedKind.Absent)), Ct);
        await fixture.Store.SetMappingEnabledAsync(fixture.MappingId, "client-a", false, Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.BeginSyncOperationAsync("second", "worker", DateTimeOffset.UtcNow.AddMinutes(1), Ct));
        Assert.False(Assert.Single(await fixture.Store.ListMappingsAsync(Ct)).Enabled);
    }

    [Fact]
    public async Task HardLinksAreSeparateEntriesAndLocalCyclesOrStaleGenerationsAreRejectedAtomically()
    {
        await using var fixture = await Fixture.CreateAsync();
        var first = Entry("first", "first.txt") with { FileSystemId = "shared-file-id" };
        var second = Entry("second", "second.txt") with { FileSystemId = "shared-file-id" };
        await fixture.Store.ObserveSyncLocalEntriesAsync(fixture.MappingId, [first, second], Ct);
        Assert.NotNull((await fixture.Store.GetSyncEntryStateAsync(fixture.MappingId, "second", Ct)).Local);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.ObserveSyncLocalEntriesAsync(fixture.MappingId, [first with { Sha256 = HashB }], Ct));
        var a = Entry("a", "a") with { Kind = SyncEntryKind.Directory, Sha256 = null, HashGeneration = null, Size = 0, ParentEntryId = "b" };
        var b = a with { EntryId = "b", RawName = "b", NameKey = "b", ParentEntryId = "a" };
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.ObserveSyncLocalEntriesAsync(fixture.MappingId, [a, b], Ct));
        Assert.Null((await fixture.Store.GetSyncEntryStateAsync(fixture.MappingId, "a", Ct)).Local);
    }

    [Fact]
    public async Task BatchesAndCancelledCallsCannotLeavePartialState()
    {
        await using var fixture = await Fixture.CreateAsync();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => fixture.Store.ObserveSyncLocalEntriesAsync(fixture.MappingId, Enumerable.Range(0, 1001).Select(index => Entry("entry-" + index, "file-" + index)).ToArray(), Ct));
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.Store.ObserveSyncLocalEntriesAsync(fixture.MappingId, [Entry()], cancelled.Token));
        Assert.Null((await fixture.Store.GetSyncEntryStateAsync(fixture.MappingId, "entry-a", Ct)).Local);
    }

    private static SyncLocalEntry Entry(string id = "entry-a", string name = "file.txt") =>
        new(id, null, name, UnicodeNames.Key(name), SyncEntryKind.File, "file-id", 1, 4, DateTimeOffset.Parse("2026-10-12T00:00:00Z"), 1, HashA, 1, SyncEntryPresence.Present, "rules-a");
    private static SyncDirtyHint Hint(string id, long generation) => new(id, generation, SyncDirtyReason.LocalChange, DateTimeOffset.Parse("2026-10-12T00:00:00Z"), DateTimeOffset.Parse("2026-10-12T00:00:01Z"));
    private static SyncRemoteEntry Remote(SyncLocalEntry entry) => new("node-a", "parent-node", entry.RawName, entry.NameKey, entry.Kind, entry.Size, entry.Sha256, "version-1", 1, false);
    private static SyncOperationIntent Intent(Fixture fixture, SyncLocalEntry entry, string operationId, SyncExpectedDestination expected) =>
        new(operationId, fixture.MappingId, entry.EntryId, entry.LocalGeneration, "epoch-a", SyncIntentKind.PutFile, entry.RulesVersion, expected, "parent-node", entry.RawName, entry.NameKey, entry.Size, entry.Sha256, "snapshot-a", []);
    private static SyncPublicationReceipt Receipt(SyncDurableOperation operation) =>
        new(operation.Intent.OperationId, "server-root", "epoch-a", operation.Fingerprint, operation.Intent.LocalGeneration, "node-a", "version-1", 1, operation.Intent.Sha256, operation.Intent.Size, true, "verified_readback");

    private static async Task<byte[]> ReadSharedBytesAsync(string path)
    {
        await using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, 4096, useAsync: true);
        using var bytes = new MemoryStream();
        await file.CopyToAsync(bytes, Ct);
        return bytes.ToArray();
    }

    private sealed class Fixture : IAsyncDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "neptune-state-tests", Guid.NewGuid().ToString("N"));
        public string Database { get; }
        public string MappingId => "mapping-a";
        public NeptuneStateStore Store { get; }
        public SyncRootIdentity Identity => new(MappingId, "client-a", "profile-a", "device-a", new Uri("https://saturn.test/"), "server-root", "epoch-a", "volume-a", "root-file-id", "rules-a");
        private Fixture() { Directory.CreateDirectory(_directory); Database = Path.Combine(_directory, "state.db"); Store = new(Database); }
        public static async Task<Fixture> CreateAsync(bool initialize = true)
        {
            var fixture = new Fixture();
            if (initialize)
            {
                await fixture.Store.InitializeAsync(Ct);
                await fixture.Store.AddMappingAsync(new(fixture.MappingId, "client-a", "C:/source", true, DateTimeOffset.UtcNow), Ct);
                await fixture.Store.RegisterSyncRootAsync(fixture.Identity, Ct);
            }
            return fixture;
        }
        public async Task<SyncDurableOperation> PlanAsync(SyncLocalEntry local, string operationId)
        {
            await Store.ObserveSyncLocalEntriesAsync(MappingId, [local], Ct);
            return await Store.PlanSyncOperationAsync(Intent(this, local, operationId, new(SyncExpectedKind.Absent)), Ct);
        }
        public async Task SqlAsync(string sql)
        {
            await using var connection = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = Database, ForeignKeys = true }.ToString());
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
        public async Task<string> QueryPlanAsync(string sql)
        {
            await using var connection = new SqliteConnection("Data Source=" + Database);
            await connection.OpenAsync(Ct);
            await using var command = connection.CreateCommand(); command.CommandText = "EXPLAIN QUERY PLAN " + sql;
            await using var reader = await command.ExecuteReaderAsync(Ct);
            var details = new List<string>();
            while (await reader.ReadAsync(Ct)) details.Add(reader.GetString(3));
            return string.Join("\n", details);
        }
        public ValueTask DisposeAsync()
        {
            SqliteConnection.ClearAllPools();
            if (!Path.GetFullPath(_directory).StartsWith(Path.GetFullPath(Path.Combine(Path.GetTempPath(), "neptune-state-tests")) + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase)) throw new InvalidOperationException("Test cleanup escaped its owned directory.");
            Directory.Delete(_directory, recursive: true);
            return ValueTask.CompletedTask;
        }
    }
}
