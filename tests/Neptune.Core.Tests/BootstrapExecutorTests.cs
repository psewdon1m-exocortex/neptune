using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class BootstrapExecutorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task CorePipelineDoesNotRequireCallerSynchronizationContextToPump()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        await File.WriteAllTextAsync(Path.Combine(fixture.Source, "file"), "background engine", Ct);
        using var server = new Server(); var blocked = new PausedContext();
        var previous = SynchronizationContext.Current;
        Task<BootstrapExecutionResult> running;
        try { SynchronizationContext.SetSynchronizationContext(blocked); running = fixture.Executor(server).ScanAndTransferAsync(fixture.Request, cancellationToken: Ct); }
        finally { SynchronizationContext.SetSynchronizationContext(previous); }
        try
        {
            var result = await running.WaitAsync(TimeSpan.FromSeconds(5), Ct);
            Assert.Equal(1, result.PublishedFiles); Assert.Equal(0, blocked.Posted);
        }
        catch (TimeoutException)
        {
            var evidence = Environment.GetEnvironmentVariable("NEPTUNE_BOOTSTRAP_TEST_DIAGNOSTICS");
            if (evidence is not null)
            {
                Directory.CreateDirectory(evidence);
                await File.WriteAllTextAsync(Path.Combine(evidence, "caller-context-stall.json"), JsonSerializer.Serialize(new { blocked.Posted, TaskStatus = running.Status.ToString(), ServerRequests = server.Methods.ToArray(), Check = "caller synchronization context not pumped", Source = fixture.Source, Database = fixture.Database }), Ct);
                await using var input = new SqliteConnection("Data Source=" + fixture.Database + ";Mode=ReadOnly;Pooling=False"); await input.OpenAsync(Ct);
                await using var output = new SqliteConnection("Data Source=" + Path.Combine(evidence, "caller-context-stall.db") + ";Pooling=False"); await output.OpenAsync(Ct);
                input.BackupDatabase(output);
            }
            throw;
        }
        finally { blocked.Release(); await running; }
    }

    private sealed class PausedContext : SynchronizationContext
    {
        private readonly ConcurrentQueue<(SendOrPostCallback Action, object? State)> _pending = new();
        private bool _released;
        public int Posted;
        public override void Post(SendOrPostCallback action, object? state)
        { Interlocked.Increment(ref Posted); lock (_pending) { if (_released) ThreadPool.QueueUserWorkItem(_ => action(state)); else _pending.Enqueue((action, state)); } }
        public void Release()
        { lock (_pending) { _released = true; while (_pending.TryDequeue(out var callback)) ThreadPool.QueueUserWorkItem(_ => callback.Action(callback.State)); } }
    }

    [Fact]
    public async Task ActualWindowsCapturePublishesBeforeLateEnumerationCompletesAndPreservesEmptyDirectories()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } // qualified adapter is fail-closed elsewhere
        await using var fixture = await Fixture.CreateAsync();
        await File.WriteAllTextAsync(Path.Combine(fixture.Source, "early.txt"), "sealed first publication", Ct);
        Directory.CreateDirectory(Path.Combine(fixture.Source, "late"));
        Directory.CreateDirectory(Path.Combine(fixture.Source, "empty"));
        using var barrier = new ManualResetEventSlim();
        var source = new BarrierSource(fixture.Source, barrier);
        var observedBeforeScanEnd = false;
        using var server = new Server();
        var executor = fixture.Executor(server, source);
        var result = await executor.ScanAndTransferAsync(fixture.Request, (receipt, _) =>
        {
            Assert.False(receipt.ImmutablePublicationAuthority);
            if (!receipt.IsDirectory) { observedBeforeScanEnd = !source.LateEnumerationCompleted; barrier.Set(); }
            return Task.CompletedTask;
        }, Ct);
        var diagnostics = JsonSerializer.Serialize(result) + " " + JsonSerializer.Serialize(await fixture.Store.ListBootstrapWorkAsync(result.SessionId, cancellationToken: Ct));
        Assert.True(observedBeforeScanEnd, diagnostics); Assert.True(source.LateEnumerationCompleted, diagnostics);
        Assert.Equal(BootstrapScanStatus.EnumerationComplete, result.Scan!.Status);
        Assert.Null(result.ScanError); Assert.Equal(1, result.PublishedFiles); Assert.Equal(2, result.PublishedDirectories);
        Assert.Equal(0, result.Failures); Assert.Equal(0, result.Conflicts);
        Assert.False(result.DestructiveCleanupAllowed); Assert.Equal(1, result.MaximumSourceReaders);
        Assert.InRange(result.MaximumDataJobs, 1, 4); Assert.InRange(result.MaximumReadyQueue, 0, 16);
        Assert.Equal(server.Published.Single().Value, Encoding.UTF8.GetBytes("sealed first publication"));
        Assert.DoesNotContain(server.Methods, method => method == "DELETE" || method == "HEAD");
        Assert.All(server.CreateKeys, key => Assert.Equal(64, key.Length));
    }

    [Fact]
    public async Task CaptureAndDataBudgetsStayBoundedAcrossWideMixedFilesAndOversizeIsDeferred()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        for (var index = 0; index < 8; index++)
            await File.WriteAllBytesAsync(Path.Combine(fixture.Source, "large-" + index), new byte[1024 * 1024 + 1], Ct);
        for (var index = 0; index < 25; index++) await File.WriteAllTextAsync(Path.Combine(fixture.Source, "small-" + index), "x", Ct);
        await File.WriteAllBytesAsync(Path.Combine(fixture.Source, "too-large"), new byte[5 * 1024 * 1024], Ct);
        using var server = new Server { DelayMilliseconds = 20 };
        var request = fixture.Request with { Limits = new(LargeFileThreshold: 1024 * 1024, ReadyQueueCapacity: 4, SpoolBudgetBytes: 4 * 1024 * 1024, DiskFreeFloorBytes: 0) };
        var result = await fixture.Executor(server).ScanAndTransferAsync(request, cancellationToken: Ct);
        Assert.InRange(result.MaximumDataJobs, 1, 4); Assert.InRange(result.MaximumLargeJobs, 1, 2);
        Assert.Equal(1, result.MaximumSourceReaders); Assert.InRange(result.MaximumReadyQueue, 0, 4);
        Assert.True(result.MaximumSpoolBytes <= request.Limits!.SpoolBudgetBytes);
        Assert.Equal(1, result.Deferred); Assert.Equal(33, result.PublishedFiles);
        Assert.Equal(0, result.Failures); Assert.Equal(0, result.Conflicts);
    }

    [Fact]
    public async Task RestartResumesLostChunkWithSameSealedBytesAndIdentityAfterSourceMutates()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        var bytes = new byte[2 * 1024 * 1024 + 7]; RandomNumberGenerator.Fill(bytes);
        var path = Path.Combine(fixture.Source, "file.bin"); await File.WriteAllBytesAsync(path, bytes, Ct);
        using var server = new Server { LoseFirstChunkResponse = true };
        var first = await fixture.Executor(server).ScanAndTransferAsync(fixture.Request, cancellationToken: Ct);
        Assert.Equal(0, first.PublishedFiles); Assert.Equal(1, first.Failures);
        var before = Assert.Single(await fixture.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        Assert.NotNull(before.SpoolPath); Assert.True(File.Exists(before.SpoolPath)); Assert.Equal(1, before.Attempt);
        await File.WriteAllTextAsync(path, "new source version must remain unadopted", Ct);
        var reopened = new NeptuneStateStore(fixture.Database); await reopened.InitializeAsync(Ct);
        var resumed = await fixture.Executor(server, store: reopened).ResumeSessionAsync(first.SessionId, fixture.Request, cancellationToken: Ct);
        Assert.Equal(1, resumed.PublishedFiles); Assert.Equal(0, resumed.Failures);
        Assert.Equal(bytes, server.Published.Single().Value);
        Assert.Single(server.CreateKeys.Distinct());
        var after = Assert.Single(await reopened.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        Assert.Equal(before.OperationId, after.OperationId); Assert.Equal(before.Sha256, after.Sha256);
        Assert.Equal(2, after.Attempt); Assert.False(File.Exists(after.SpoolPath));
        Assert.Equal("new source version must remain unadopted", await File.ReadAllTextAsync(path, Ct));
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM sync_state_baselines"));
    }

    [Fact]
    public async Task UnknownExistingRemoteConflictsForSmallAndLargeAndNeverOverwrites()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        await File.WriteAllTextAsync(Path.Combine(fixture.Source, "small"), "mine", Ct);
        await File.WriteAllBytesAsync(Path.Combine(fixture.Source, "large"), new byte[1024 * 1024 + 1], Ct);
        using var server = new Server { ExistingTarget = true };
        var result = await fixture.Executor(server).ScanAndTransferAsync(fixture.Request, cancellationToken: Ct);
        Assert.Equal(2, result.Conflicts); Assert.Equal(0, result.PublishedFiles);
        Assert.Empty(server.Published); Assert.DoesNotContain("PUT", server.Methods); Assert.DoesNotContain("HEAD", server.Methods);
        Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM sync_state_baselines"));
    }

    [Fact]
    public async Task LostLegacyPutResponseIsObservedOnlyAndAbsenceNeverAuthorizesReplay()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        await File.WriteAllTextAsync(Path.Combine(fixture.Source, "small"), "mine", Ct);
        using var server = new Server { EndpointUnavailable = true, LosePutResponse = true };
        var first = await fixture.Executor(server, allowPut: true).ScanAndTransferAsync(fixture.Request, cancellationToken: Ct);
        Assert.Equal(1, first.Failures); Assert.Equal(1, server.Methods.Count(method => method == "PUT"));
        server.Published.Clear(); // absence does not fence the delayed PUT effect
        var second = await fixture.Executor(server, allowPut: true).ResumeSessionAsync(first.SessionId, fixture.Request, cancellationToken: Ct);
        Assert.Equal(1, second.Failures); Assert.Equal(0, second.PublishedFiles);
        Assert.Equal(1, server.Methods.Count(method => method == "PUT")); Assert.Contains("GET", server.Methods);
    }

    [Fact]
    public async Task CaseCollisionIsRejectedBeforeChildrenBecomeEligibleAndBeforeNetworkPublication()
    {
        await using var fixture = await Fixture.CreateAsync();
        using var server = new Server();
        var result = await fixture.Executor(server, new CollisionSource()).ScanAndTransferAsync(fixture.Request, cancellationToken: Ct);
        Assert.Contains("collide", result.ScanError); Assert.Equal(0, result.PublishedFiles); Assert.Empty(server.Methods);
    }

    [Fact]
    public async Task FutureWorkFormatRefusesCommonInitializeWithoutChangingDbOrWal()
    {
        await using var fixture = await Fixture.CreateAsync();
        await using var connection = new SqliteConnection("Data Source=" + fixture.Database + ";Pooling=False"); await connection.OpenAsync(Ct);
        await using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA wal_autocheckpoint=0; CREATE TABLE bootstrap_work_format(singleton INTEGER PRIMARY KEY,version INTEGER); INSERT INTO bootstrap_work_format VALUES(1,99)";
        await command.ExecuteNonQueryAsync(Ct);
        var db = await ReadSharedAsync(fixture.Database); var wal = await ReadSharedAsync(fixture.Database + "-wal");
        await Assert.ThrowsAsync<InvalidDataException>(() => new NeptuneStateStore(fixture.Database).InitializeAsync(Ct));
        Assert.Equal(db, await ReadSharedAsync(fixture.Database)); Assert.Equal(wal, await ReadSharedAsync(fixture.Database + "-wal"));
    }

    [Fact]
    public async Task TemporarySpoolSaturationWaitsForReleaseAndEventuallyPublishesBothFiles()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        await File.WriteAllBytesAsync(Path.Combine(fixture.Source, "a"), new byte[2 * 1024 * 1024], Ct);
        await File.WriteAllBytesAsync(Path.Combine(fixture.Source, "b"), new byte[2 * 1024 * 1024], Ct);
        using var server = new Server { DelayMilliseconds = 30 };
        var request = fixture.Request with { Limits = new(LargeFileThreshold: 1024 * 1024, SpoolBudgetBytes: 3 * 1024 * 1024, DiskFreeFloorBytes: 0) };
        var result = await fixture.Executor(server).ScanAndTransferAsync(request, cancellationToken: Ct);
        Assert.Equal(2, result.PublishedFiles); Assert.Equal(0, result.Deferred); Assert.Equal(0, result.Failures);
        Assert.InRange(result.MaximumSpoolBytes, 2 * 1024 * 1024, request.Limits!.SpoolBudgetBytes);
        Assert.InRange(result.MaximumSourceReaders, 1, 1);
    }

    [Fact]
    public async Task PostCommitCallbackAndBusyOwnedSpoolNeverDowngradeReceiptOrBreakWorkerJoin()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        await File.WriteAllTextAsync(Path.Combine(fixture.Source, "file"), "durable publication", Ct);
        using var server = new Server();
        using var holder = new HoldingTransport(fixture.Transport(server));
        var executor = new NeptuneBootstrapExecutor(fixture.Store, holder);
        var result = await executor.ScanAndTransferAsync(fixture.Request, (_, _) => throw new IOException("caller notification failed"), Ct);
        Assert.Equal(1, result.PublishedFiles); Assert.Equal(0, result.Failures); Assert.Equal(2, result.PostCommitDiagnostics);
        var work = Assert.Single(await fixture.Store.ListBootstrapWorkAsync(result.SessionId, cancellationToken: Ct));
        Assert.Equal(BootstrapWorkPhase.Published, work.Phase); Assert.NotNull(work.Receipt); Assert.Contains("post-commit", work.Error);
        Assert.True(File.Exists(work.SpoolPath));
        var effects = server.Methods.Count;
        var resumed = await executor.ResumeSessionAsync(result.SessionId, fixture.Request, cancellationToken: Ct);
        Assert.Equal(1, resumed.PublishedFiles); Assert.Equal(effects, server.Methods.Count);
    }

    [Fact]
    public async Task CancelAfterServerPublicationRetainsReceivedCheckpointThenRecoversSameSession()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        await File.WriteAllTextAsync(Path.Combine(fixture.Source, "file"), "cancel and recover", Ct);
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        using var server = new Server { BeforeComplete = () => { cancel.Cancel(); return Task.CompletedTask; } };
        var first = await fixture.Executor(server).ScanAndTransferAsync(fixture.Request, cancellationToken: cancel.Token);
        Assert.True(first.Cancelled); Assert.Equal(0, first.PublishedFiles);
        var work = Assert.Single(await fixture.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        Assert.Equal(BootstrapWorkPhase.Received, work.Phase); Assert.True(work.ReceivedBytes > 0); Assert.True(File.Exists(work.SpoolPath));
        server.BeforeComplete = null;
        var resumed = await fixture.Executor(server).ResumeSessionAsync(first.SessionId, fixture.Request, cancellationToken: Ct);
        Assert.Equal(1, resumed.PublishedFiles); Assert.Single(server.CreateKeys.Distinct());
        Assert.Equal(1, server.Methods.Count(method => method == "PATCH"));
    }

    [Fact]
    public async Task PauseDuringInFlightPublicationPreservesOutcomeAndCompletedReplayHasZeroEffects()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        await File.WriteAllTextAsync(Path.Combine(fixture.Source, "file"), "pause does not erase receipt", Ct);
        using var server = new Server { BeforeComplete = async () => { await fixture.ScalarAsync("UPDATE sync_mappings SET enabled=0 WHERE mapping_id='mapping'"); } };
        var first = await fixture.Executor(server).ScanAndTransferAsync(fixture.Request, cancellationToken: Ct);
        Assert.Equal(1, first.PublishedFiles);
        var work = Assert.Single(await fixture.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        Assert.Equal(BootstrapWorkPhase.Published, work.Phase); Assert.NotNull(work.Receipt);
        var effects = server.Methods.Count;
        var replay = await fixture.Executor(server).ResumeSessionAsync(first.SessionId, fixture.Request, cancellationToken: Ct);
        Assert.Equal(1, replay.PublishedFiles); Assert.Equal(effects, server.Methods.Count);
    }

    [Fact]
    public async Task PausedOrRemappedGenerationCannotReplayUnresolvedSealedIntent()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        await File.WriteAllTextAsync(Path.Combine(fixture.Source, "file"), "retain same bytes", Ct);
        using var server = new Server { LoseFirstChunkResponse = true };
        var first = await fixture.Executor(server).ScanAndTransferAsync(fixture.Request, cancellationToken: Ct);
        var effects = server.Methods.Count;
        await fixture.ScalarAsync("UPDATE sync_mappings SET enabled=0 WHERE mapping_id='mapping'");
        var paused = await fixture.Executor(server).ResumeSessionAsync(first.SessionId, fixture.Request, cancellationToken: Ct);
        Assert.Equal(0, paused.PublishedFiles); Assert.Equal(1, paused.Failures); Assert.Equal(effects, server.Methods.Count);
        var renamed = Path.Combine(fixture.DirectoryPath, "different-root"); Directory.CreateDirectory(renamed);
        await fixture.ScalarAsync("UPDATE sync_mappings SET enabled=1,local_path='" + renamed.Replace("'", "''") + "' WHERE mapping_id='mapping'");
        var rebound = await fixture.Executor(server).ResumeSessionAsync(first.SessionId, fixture.Request, cancellationToken: Ct);
        Assert.Equal(0, rebound.PublishedFiles); Assert.Equal(effects, server.Methods.Count);
        var stored = Assert.Single(await fixture.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        Assert.True(File.Exists(stored.SpoolPath)); Assert.Equal(1, stored.Attempt);
    }

    [Fact]
    public async Task RecoveredSpoolTamperingFailsBeforeNetworkAndInventoryAboveNewQuotaRefusesAdmission()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        await File.WriteAllTextAsync(Path.Combine(fixture.Source, "file"), "sealed payload", Ct);
        using var server = new Server { LoseFirstChunkResponse = true };
        var first = await fixture.Executor(server).ScanAndTransferAsync(fixture.Request, cancellationToken: Ct);
        var work = Assert.Single(await fixture.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        var effects = server.Methods.Count;
        await Assert.ThrowsAsync<BootstrapCaptureDeferredException>(() => fixture.Executor(server).ResumeSessionAsync(first.SessionId,
            fixture.Request with { Limits = new(SpoolBudgetBytes: 1, DiskFreeFloorBytes: 0) }, cancellationToken: Ct));
        var modified = await File.ReadAllBytesAsync(work.SpoolPath!, Ct); modified[0] ^= 1; await File.WriteAllBytesAsync(work.SpoolPath!, modified, Ct);
        var recovered = await fixture.Executor(server).ResumeSessionAsync(first.SessionId, fixture.Request, cancellationToken: Ct);
        Assert.Equal(1, recovered.Failures); Assert.Equal(0, recovered.PublishedFiles); Assert.Equal(effects, server.Methods.Count);
        Assert.Contains("hash changed", Assert.Single(await fixture.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct)).Error);
    }

    [Fact]
    public async Task InaccessibleLateBranchNeverCleansRemoteAndRetainsEarlyPublication()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        await File.WriteAllTextAsync(Path.Combine(fixture.Source, "early"), "publish first", Ct);
        Directory.CreateDirectory(Path.Combine(fixture.Source, "late"));
        using var barrier = new ManualResetEventSlim();
        using var server = new Server();
        var result = await fixture.Executor(server, new FailingLateSource(fixture.Source, barrier)).ScanAndTransferAsync(fixture.Request,
            (receipt, _) => { if (!receipt.IsDirectory) barrier.Set(); return Task.CompletedTask; }, Ct);
        Assert.Contains("inaccessible", result.ScanError); Assert.Equal(1, result.PublishedFiles); Assert.False(result.DestructiveCleanupAllowed);
        Assert.DoesNotContain("DELETE", server.Methods); Assert.Equal(0L, await fixture.ScalarAsync("SELECT COUNT(*) FROM sync_state_baselines"));
    }

    [Fact]
    public async Task MappingExecutionOwnerRejectsConcurrentExecutorBeforeSecondScan()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        await File.WriteAllTextAsync(Path.Combine(fixture.Source, "early"), "first", Ct);
        Directory.CreateDirectory(Path.Combine(fixture.Source, "late"));
        using var barrier = new ManualResetEventSlim(); using var entered = new ManualResetEventSlim();
        using var server = new Server();
        var first = fixture.Executor(server, new BarrierSource(fixture.Source, barrier, entered)).ScanAndTransferAsync(fixture.Request, cancellationToken: Ct);
        Assert.True(entered.Wait(TimeSpan.FromSeconds(10), Ct));
        await Assert.ThrowsAsync<IOException>(() => fixture.Executor(server).ScanAndTransferAsync(fixture.Request, cancellationToken: Ct));
        barrier.Set(); Assert.Equal(1, (await first).PublishedFiles);
    }

    [Fact]
    public async Task AdditiveExecutionMigrationBackupIncludesWalAndSeparateRollbackPreservesOldProfile()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        await using var connection = new SqliteConnection("Data Source=" + fixture.Database + ";Pooling=False"); await connection.OpenAsync(Ct);
        await using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA wal_autocheckpoint=0; CREATE TABLE user_wal_payload(value TEXT); INSERT INTO user_wal_payload VALUES('keep user WAL data')";
        await command.ExecuteNonQueryAsync(Ct);
        using var server = new Server();
        await fixture.Executor(server).ScanAndTransferAsync(fixture.Request, cancellationToken: Ct);
        var rollback = await fixture.Store.CreatePreBootstrapWorkRollbackCopyAsync(Path.Combine(fixture.DirectoryPath, "rollback.db"), Ct);
        await using var copy = new SqliteConnection("Data Source=" + rollback + ";Mode=ReadOnly;Pooling=False"); await copy.OpenAsync(Ct);
        await using var query = copy.CreateCommand(); query.CommandText = "SELECT value FROM user_wal_payload";
        Assert.Equal("keep user WAL data", await query.ExecuteScalarAsync(Ct));
        query.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE name='bootstrap_work_format'"; Assert.Equal(0L, await query.ExecuteScalarAsync(Ct));
        Assert.Equal(1L, await fixture.ScalarAsync("SELECT COUNT(*) FROM bootstrap_work_format"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Store.CreatePreBootstrapWorkRollbackCopyAsync(fixture.Database, Ct));
    }

    [Fact]
    public async Task NewScanGenerationAndDifferentClientCannotReplayOldSealedWork()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        await File.WriteAllTextAsync(Path.Combine(fixture.Source, "file"), "generation one", Ct);
        using var server = new Server { LoseFirstChunkResponse = true };
        var first = await fixture.Executor(server).ScanAndTransferAsync(fixture.Request, cancellationToken: Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.Executor(server).ResumeSessionAsync(first.SessionId, fixture.Request with { ClientInstanceId = "other-client" }, cancellationToken: Ct));
        await new NeptuneBootstrapScanner(fixture.Store).ScanAsync("mapping", "client", fixture.Source, cancellationToken: Ct);
        var effects = server.Methods.Count;
        var replay = await fixture.Executor(server).ResumeSessionAsync(first.SessionId, fixture.Request, cancellationToken: Ct);
        Assert.Equal(0, replay.PublishedFiles); Assert.Equal(1, replay.Failures); Assert.Equal(effects, server.Methods.Count);
        Assert.True(File.Exists(Assert.Single(await fixture.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct)).SpoolPath));
    }

    [Fact]
    public async Task DirectoryFileTypeConflictAndZeroLengthFileAreHandledIndependently()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        Directory.CreateDirectory(Path.Combine(fixture.Source, "empty"));
        await File.WriteAllBytesAsync(Path.Combine(fixture.Source, "zero"), [], Ct);
        using var server = new Server { DirectoryIsFile = true };
        var result = await fixture.Executor(server).ScanAndTransferAsync(fixture.Request, cancellationToken: Ct);
        Assert.Equal(1, result.Conflicts); Assert.Equal(1, result.PublishedFiles); Assert.Equal(0, result.PublishedDirectories);
        Assert.Equal(0, result.PublishedBytes); Assert.DoesNotContain("PUT", server.Methods); Assert.DoesNotContain("DELETE", server.Methods);
    }

    [Fact]
    public async Task DeepBranchesAndWideFilesRemainBoundedAndPreserveDirectories()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        var current = fixture.Source;
        for (var i = 0; i < 20; i++) { current = Path.Combine(current, "d" + i); Directory.CreateDirectory(current); }
        await File.WriteAllTextAsync(Path.Combine(current, "leaf"), "deep leaf", Ct);
        for (var i = 0; i < 300; i++) await File.WriteAllTextAsync(Path.Combine(fixture.Source, "wide-" + i), "x", Ct);
        using var server = new Server();
        var result = await fixture.Executor(server).ScanAndTransferAsync(fixture.Request with { Limits = new(ReadyQueueCapacity: 4, MaximumSpoolFiles: 4, DiskFreeFloorBytes: 0) }, cancellationToken: Ct);
        Assert.Equal(301, result.PublishedFiles); Assert.Equal(20, result.PublishedDirectories); Assert.Equal(0, result.Failures);
        Assert.InRange(result.MaximumReadyQueue, 0, 4); Assert.InRange(result.MaximumDataJobs, 1, 4); Assert.Equal(1, result.MaximumSourceReaders);
    }

    [Fact]
    public async Task NfcCollisionPreventsPublicationBeforeCanonicalParentValidation()
    {
        await using var fixture = await Fixture.CreateAsync(); using var server = new Server();
        var result = await fixture.Executor(server, new NfcCollisionSource()).ScanAndTransferAsync(fixture.Request, cancellationToken: Ct);
        Assert.Contains("collide", result.ScanError); Assert.Empty(server.Methods);
    }

    [Fact]
    public async Task ReparseSwapAfterScannerValidationIsRejectedBeforeFileTransport()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        var branch = Path.Combine(fixture.Source, "branch"); Directory.CreateDirectory(branch);
        await File.WriteAllTextAsync(Path.Combine(branch, "file"), "own source", Ct);
        var outside = Path.Combine(fixture.DirectoryPath, "outside"); Directory.CreateDirectory(outside);
        await File.WriteAllTextAsync(Path.Combine(outside, "file"), "different!", Ct);
        using var server = new Server();
        using var swapped = new ReparseSwapCapture(branch, outside);
        var executor = new NeptuneBootstrapExecutor(fixture.Store, fixture.Transport(server), captureSource: swapped);
        var result = await executor.ScanAndTransferAsync(fixture.Request, cancellationToken: Ct);
        Assert.Equal(0, result.PublishedFiles); Assert.True(result.Failures > 0); Assert.Empty(server.CreateKeys);
        var failure = (await fixture.Store.ListBootstrapWorkAsync(result.SessionId, cancellationToken: Ct)).Single(work => !work.IsDirectory).Error;
        Assert.True(swapped.Created, failure);
        Assert.Contains("reparse", failure);
        Assert.Equal("different!", await File.ReadAllTextAsync(Path.Combine(outside, "file"), Ct));
    }

    [Fact]
    public async Task ChangedSourceSizeFailsCaptureAndBrokenExecutionSchemaRefusesInitializeBeforeWrites()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = await Fixture.CreateAsync();
        await File.WriteAllTextAsync(Path.Combine(fixture.Source, "file"), "x", Ct);
        using var server = new Server();
        var executor = new NeptuneBootstrapExecutor(fixture.Store, fixture.Transport(server), captureSource: new GrowingSourceCapture());
        var result = await executor.ScanAndTransferAsync(fixture.Request, cancellationToken: Ct);
        Assert.Equal(0, result.PublishedFiles); Assert.Equal(1, result.Deferred); Assert.Empty(server.Methods);
        await using var connection = new SqliteConnection("Data Source=" + fixture.Database + ";Pooling=False"); await connection.OpenAsync(Ct);
        await using var command = connection.CreateCommand(); command.CommandText = "PRAGMA wal_autocheckpoint=0; DROP INDEX bootstrap_work_queue"; await command.ExecuteNonQueryAsync(Ct);
        var db = await ReadSharedAsync(fixture.Database); var wal = await ReadSharedAsync(fixture.Database + "-wal");
        await Assert.ThrowsAsync<InvalidDataException>(() => new NeptuneStateStore(fixture.Database).InitializeAsync(Ct));
        Assert.Equal(db, await ReadSharedAsync(fixture.Database)); Assert.Equal(wal, await ReadSharedAsync(fixture.Database + "-wal"));
    }

    private sealed class ReparseSwapCapture(string branch, string outside) : IBootstrapSourceCapture, IDisposable
    {
        private readonly WindowsBootstrapSourceCapture _actual = new();
        public bool Created;
        public Task ValidateDirectoryAsync(string root, string relative, CancellationToken ct) => _actual.ValidateDirectoryAsync(root, relative, ct);
        public async Task<BootstrapSealedSource> CaptureAsync(string root, string relative, string spool, long maximum, CancellationToken ct)
        {
            Directory.Move(branch, branch + "-original");
            var start = new ProcessStartInfo("cmd.exe") { UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true };
            start.ArgumentList.Add("/c"); start.ArgumentList.Add("mklink"); start.ArgumentList.Add("/J"); start.ArgumentList.Add(branch); start.ArgumentList.Add(outside);
            using var process = Process.Start(start)!; await process.WaitForExitAsync(ct);
            if (process.ExitCode != 0) throw new IOException("Owned test junction creation failed: " + await process.StandardError.ReadToEndAsync(ct));
            Created = true;
            return await _actual.CaptureAsync(root, relative, spool, maximum, ct);
        }
        public void Dispose()
        {
            // Remove only the exact junction created above, without recursively
            // traversing its target. Some Windows FS versions remove the link
            // and still return ERROR_INVALID_PARAMETER; qualify that outcome.
            if (Directory.Exists(branch) && (File.GetAttributes(branch) & FileAttributes.ReparsePoint) != 0)
            { try { Directory.Delete(branch, false); } catch (IOException) when (!Directory.Exists(branch)) { } }
        }
    }
    private sealed class GrowingSourceCapture : IBootstrapSourceCapture
    {
        private readonly WindowsBootstrapSourceCapture _actual = new();
        public Task ValidateDirectoryAsync(string root, string relative, CancellationToken ct) => _actual.ValidateDirectoryAsync(root, relative, ct);
        public async Task<BootstrapSealedSource> CaptureAsync(string root, string relative, string spool, long maximum, CancellationToken ct)
        { await File.AppendAllTextAsync(Path.Combine(root, relative), " grew", ct); return await _actual.CaptureAsync(root, relative, spool, maximum, ct); }
    }
    private sealed class NfcCollisionSource : IBootstrapScanSource
    {
        public BootstrapDirectoryStamp ReadDirectoryStamp(string path) => new(DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch);
        public IEnumerable<BootstrapSourceEntry> EnumerateDirectory(string path) { yield return new("e\u0301", false, 1, DateTimeOffset.UnixEpoch); yield return new("\u00e9", false, 1, DateTimeOffset.UnixEpoch); }
    }

    private sealed class HoldingTransport(ICreateOnlyBootstrapTransport actual) : ICreateOnlyBootstrapTransport, IDisposable
    {
        private FileStream? _held;
        public Uri TargetRoot => actual.TargetRoot; public string CredentialScope => actual.CredentialScope;
        public async Task<BootstrapPublicationReceipt> TransferAsync(BootstrapWorkRecord work, Func<BootstrapTransferCheckpoint, Task> checkpoint, CancellationToken ct)
        { var receipt = await actual.TransferAsync(work, checkpoint, ct); if (!work.IsDirectory) _held = new(work.SpoolPath!, FileMode.Open, FileAccess.Read, FileShare.Read); return receipt; }
        public void Dispose() => _held?.Dispose();
    }
    private sealed class FailingLateSource(string root, ManualResetEventSlim barrier) : IBootstrapScanSource
    {
        private readonly FileSystemBootstrapScanSource _actual = new();
        public BootstrapDirectoryStamp ReadDirectoryStamp(string path) => _actual.ReadDirectoryStamp(path);
        public IEnumerable<BootstrapSourceEntry> EnumerateDirectory(string path)
        { if (Path.GetRelativePath(root, path) == "late") { if (!barrier.Wait(TimeSpan.FromSeconds(15))) throw new IOException("No early publication"); throw new IOException("late branch inaccessible"); } return _actual.EnumerateDirectory(path); }
    }

    private static async Task<byte[]> ReadSharedAsync(string path)
    { await using var input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete); using var output = new MemoryStream(); await input.CopyToAsync(output, Ct); return output.ToArray(); }

    private sealed class CollisionSource : IBootstrapScanSource
    {
        public BootstrapDirectoryStamp ReadDirectoryStamp(string path) => new(DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch);
        public IEnumerable<BootstrapSourceEntry> EnumerateDirectory(string path) { yield return new("file", false, 1, DateTimeOffset.UnixEpoch); yield return new("FILE", false, 1, DateTimeOffset.UnixEpoch); }
    }
    private sealed class BarrierSource(string root, ManualResetEventSlim barrier, ManualResetEventSlim? entered = null) : IBootstrapScanSource
    {
        private readonly FileSystemBootstrapScanSource _actual = new();
        public bool LateEnumerationCompleted;
        public BootstrapDirectoryStamp ReadDirectoryStamp(string path) => _actual.ReadDirectoryStamp(path);
        public IEnumerable<BootstrapSourceEntry> EnumerateDirectory(string path)
        {
            if (Path.GetRelativePath(root, path) == "late")
            { entered?.Set(); if (!barrier.Wait(TimeSpan.FromSeconds(15))) throw new IOException("First publication did not unblock the late scan."); LateEnumerationCompleted = true; }
            return _actual.EnumerateDirectory(path);
        }
    }
    private sealed class Fixture : IAsyncDisposable
    {
        public string DirectoryPath { get; } = Path.Combine(Path.GetTempPath(), "neptune-bootstrap-executor-tests", Guid.NewGuid().ToString("N"));
        public string Source { get; }
        public string Database { get; }
        public NeptuneStateStore Store { get; }
        public BootstrapExecutionRequest Request => new("mapping", "client", Source, Path.Combine(DirectoryPath, "spool"), new(DiskFreeFloorBytes: 0));
        private Fixture() { Directory.CreateDirectory(DirectoryPath); Source = Path.Combine(DirectoryPath, "source"); Directory.CreateDirectory(Source); Database = Path.Combine(DirectoryPath, "profile.db"); Store = new(Database); }
        public static async Task<Fixture> CreateAsync() { var f = new Fixture(); await f.Store.InitializeAsync(Ct); await f.Store.AddMappingAsync(new("mapping", "client", f.Source, true, DateTimeOffset.UtcNow), Ct); return f; }
        public NeptuneBootstrapExecutor Executor(Server server, IBootstrapScanSource? source = null, NeptuneStateStore? store = null, bool allowPut = false)
            => new(store ?? Store, Transport(server, store, allowPut), source);
        public ICreateOnlyBootstrapTransport Transport(Server server, NeptuneStateStore? store = null, bool allowPut = false)
            => new CreateOnlyWebDavBootstrapTransport(new WebDavSyncClient(server.Http, store ?? Store), new Uri("https://saturn.test/dav/sync/Audit/Mapping/"), "audit-token", allowPut);
        public async Task<object?> ScalarAsync(string sql) { await using var c = new SqliteConnection("Data Source=" + Database); await c.OpenAsync(Ct); await using var q = c.CreateCommand(); q.CommandText = sql; return await q.ExecuteScalarAsync(Ct); }
        public ValueTask DisposeAsync() { SqliteConnection.ClearAllPools(); if (!Path.GetFullPath(DirectoryPath).StartsWith(Path.GetFullPath(Path.Combine(Path.GetTempPath(), "neptune-bootstrap-executor-tests")) + Path.DirectorySeparatorChar)) throw new InvalidOperationException("Cleanup escaped owned fixture."); Directory.Delete(DirectoryPath, true); return ValueTask.CompletedTask; }
    }
    private sealed class Server : HttpMessageHandler
    {
        public readonly HttpClient Http;
        public ConcurrentBag<string> Methods = [], CreateKeys = [];
        public ConcurrentDictionary<string, byte[]> Published = new();
        private readonly ConcurrentDictionary<string, Session> _sessions = new();
        private int _lostChunk;
        public bool ExistingTarget, EndpointUnavailable, LoseFirstChunkResponse, LosePutResponse, DirectoryIsFile;
        public int DelayMilliseconds;
        public Func<Task>? BeforeComplete;
        public Server() => Http = new(this, disposeHandler: false);
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Methods.Add(request.Method.Method);
            var path = request.RequestUri!.AbsolutePath;
            if (request.Method.Method == "PROPFIND") return new((HttpStatusCode)207) { Content = new StringContent($"<d:multistatus xmlns:d='DAV:'><d:response><d:href>{path}</d:href><d:propstat><d:prop><d:resourcetype>{(DirectoryIsFile && path.TrimEnd('/').EndsWith("/empty", StringComparison.Ordinal) ? "" : "<d:collection/>")}</d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>") };
            if (request.Method.Method == "MKCOL") return new(HttpStatusCode.Created);
            if (request.Method == HttpMethod.Get)
                return Published.TryGetValue(path, out var payload) ? new(HttpStatusCode.OK) { Content = new ByteArrayContent(payload) } : new(HttpStatusCode.NotFound);
            if (request.Method == HttpMethod.Put)
            {
                Assert.Equal("*", request.Headers.GetValues("If-None-Match").Single()); Assert.False(request.Headers.Contains("If-Match"));
                var data = await request.Content!.ReadAsByteArrayAsync(ct); Published[path] = data;
                if (LosePutResponse) throw new HttpRequestException("PUT response lost after publication");
                var response = new HttpResponseMessage(HttpStatusCode.Created); response.Headers.ETag = new($"\"sha256-{Convert.ToHexStringLower(SHA256.HashData(data))}\""); return response;
            }
            if (request.Method == HttpMethod.Post && path == "/api/v1/sync/uploads")
            {
                if (EndpointUnavailable) return new(HttpStatusCode.NotFound);
                using var json = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(ct)); var body = json.RootElement;
                Assert.Equal("*", body.GetProperty("ifNoneMatch").GetString()); Assert.False(body.TryGetProperty("ifMatch", out _));
                var key = body.GetProperty("idempotencyKey").GetString()!; CreateKeys.Add(key);
                if (ExistingTarget) return new(HttpStatusCode.PreconditionFailed);
                var s = _sessions.GetOrAdd(key, _ => new(Guid.NewGuid().ToString(), "/dav/" + body.GetProperty("path").GetString(), body.GetProperty("expectedSize").GetInt64(), body.GetProperty("expectedSha256").GetString()!));
                return s.Checkpoint();
            }
            var session = _sessions.Values.Single(value => path.Contains(value.Id, StringComparison.Ordinal));
            if (DelayMilliseconds > 0) await Task.Delay(DelayMilliseconds, ct);
            if (request.Method == HttpMethod.Patch)
            {
                Assert.Equal(session.Bytes.Length.ToString(), request.Headers.GetValues("Upload-Offset").Single());
                session.Bytes.Write(await request.Content!.ReadAsByteArrayAsync(ct));
                if (LoseFirstChunkResponse && Interlocked.Increment(ref _lostChunk) == 1) throw new HttpRequestException("chunk response lost after durable receipt");
                return session.Checkpoint();
            }
            Assert.EndsWith("/complete", path); Assert.Equal("*", request.Headers.GetValues("If-None-Match").Single());
            if (BeforeComplete is not null) await BeforeComplete();
            var bytes = session.Bytes.ToArray(); Assert.Equal(session.Size, bytes.Length); Assert.Equal(session.Hash, Convert.ToHexStringLower(SHA256.HashData(bytes)));
            session.Active = true; Published[session.Path] = bytes; return session.Checkpoint();
        }
        protected override void Dispose(bool disposing) { if (disposing) { Http.Dispose(); foreach (var session in _sessions.Values) session.Bytes.Dispose(); } base.Dispose(disposing); }
        private sealed class Session(string id, string path, long size, string hash)
        {
            public string Id = id, Path = path, Hash = hash; public long Size = size; public bool Active; public MemoryStream Bytes = new();
            public HttpResponseMessage Checkpoint() => new(HttpStatusCode.OK) { Content = new StringContent(JsonSerializer.Serialize(new { id = Id, status = Active ? "active" : "uploading", receivedSize = Bytes.Length, expectedSize = Size, etag = Active ? $"\"sha256-{Hash}\"" : null })) };
        }
    }
}
