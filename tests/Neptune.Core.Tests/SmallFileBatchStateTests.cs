using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    private static object BatchResources(WindowsFreshSyncEngine engine) => typeof(WindowsFreshSyncEngine).GetField("_resources", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(engine)!;
    private static Task<T> BatchSpoolCall<T>(string name, params object?[] args) => (Task<T>)typeof(NeptuneStateStore).Assembly.GetType("Neptune.Core.LocalSmallBatchSpool")!.GetMethod(name, BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, args)!;
    private static Task BatchSpoolCall(string name, params object?[] args) => (Task)typeof(NeptuneStateStore).Assembly.GetType("Neptune.Core.LocalSmallBatchSpool")!.GetMethod(name, BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, args)!;
    private static Task EnableBatch(LocalSourceJournalSession s, string scope) => SessionCall(s, "EnableSmallBatchesAsync", scope, Ct);
    private static Task<LocalSmallBatch> CreateBatch(LocalSourceJournalSession s, string scope, string batch, params string[] originals)
        => SessionCall<LocalSmallBatch>(s, "CreateSmallBatchAsync", scope, batch, originals, Ct);
    private static Task<LocalSmallBatch> ReadBatch(LocalSourceJournalSession s, string scope, string batch)
        => SessionCall<LocalSmallBatch>(s, "ReadSmallBatchAsync", scope, batch, Ct);
    private static async Task BatchPhase(LocalSourceJournalSession s, string scope, string batch, LocalSmallBatchPhase phase, string? hash = null)
    {
        // These state-only tests use an explicit synthetic incarnation; actual
        // handle ownership is exercised by the native aggregate spool tests.
        if (phase == LocalSmallBatchPhase.Ready) await SessionCall(s, "BindSmallBatchSpoolAsync", scope, batch, new string('1', 64), Ct);
        await SessionCall(s, "ChangeSmallBatchAsync", scope, batch, phase, hash, Ct);
    }
    private static Task ForgetBatch(LocalSourceJournalSession s, string scope, string batch) => SessionCall(s, "ForgetSmallBatchAsync", scope, batch, Ct);
    private static Task<LocalCycleWork> ReadWork(LocalSourceJournalSession s, string scope, string op)
        => SessionCall<LocalCycleWork>(s, "ReadCycleWorkAsync", scope, op, Ct);

    [Theory]
    [InlineData(4)][InlineData(5)][InlineData(6)]
    public async Task SmallBatchMigrationPreservesOriginalsAndCommittedWalWithSeparateRollback(int version)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } await using var f = await WindowsFreshCycleTests.Fixture.Create();
        File.WriteAllText(Path.Combine(f.Source, "retained"), "immutable source bytes");
        await Medium(async () =>
        {
            await using var e = await Open(f); f.Remote.LosePrepare = true; _ = await e.InitialSyncAsync(Ct); var s = Session(e);
            Assert.Equal(4L, await f.Scalar("SELECT version FROM local_cycle_format"));
            var admission = new LocalJournalAdmission(MinimumFreeDiskBytes: 10L << 30);
            if (version >= 5) await (Task)typeof(NeptuneStateStore).GetMethod("EnsureCycleReplacementAsync", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(f.Store, [admission, Ct])!;
            if (version == 6) await (Task)typeof(NeptuneStateStore).GetMethod("EnsureCycleSupersessionAsync", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(f.Store, [admission, Ct])!;
            var before = await Snapshot(f, e.ScopeId);
            await using var held = new SqliteConnection("Data Source=" + f.Database + ";Pooling=False;Cache=Private"); await held.OpenAsync(Ct);
            await using (var marker = held.CreateCommand()) { marker.CommandText = "CREATE TABLE batch_wal_marker(value TEXT NOT NULL);INSERT INTO batch_wal_marker VALUES('committed before v7')"; await marker.ExecuteNonQueryAsync(Ct); }
            await EnableBatch(s, e.ScopeId); Assert.Equal(7L, await f.Scalar("SELECT version FROM local_cycle_format"));
            Assert.Equal(before, await Snapshot(f, e.ScopeId)); Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_replacement_requests"));
            await EnableBatch(s, e.ScopeId); Assert.Single(Directory.GetFiles(f.Directory, "*.pre-local-cycle-v7-*.db"));
            var rollback = Path.Combine(f.Directory, "pre-batch.db"); await f.Store.CreateLocalCyclePreBatchRollbackCopyAsync(rollback, Ct);
            await using var copy = new SqliteConnection("Data Source=" + rollback + ";Mode=ReadOnly;Pooling=False"); await copy.OpenAsync(Ct);
            await using var q = copy.CreateCommand(); q.CommandText = "SELECT version FROM local_cycle_format"; Assert.Equal((long)version, await q.ExecuteScalarAsync(Ct));
            q.CommandText = "SELECT value FROM batch_wal_marker"; Assert.Equal("committed before v7", await q.ExecuteScalarAsync(Ct));
            q.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE name='local_cycle_batches'"; Assert.Equal(0L, await q.ExecuteScalarAsync(Ct));
            var evidence = Environment.GetEnvironmentVariable("NEPTUNE_FRESH_CYCLE_EVIDENCE_ROOT");
            if (evidence is not null)
            {
                Directory.CreateDirectory(evidence); var filename = Path.Combine(evidence, "batch-v7-from" + version + "-" + Path.GetFileName(f.Directory) + ".db");
                await using var destination = new SqliteConnection("Data Source=" + filename + ";Pooling=False"); await destination.OpenAsync(Ct); held.BackupDatabase(destination);
            }
            await f.Evidence("batch-migration-" + version, new { previous = version, current = 7, walMarkerPreserved = true, originalsPreserved = true }); return true;
        });
    }

    [Theory]
    [InlineData(0)][InlineData(262144)]
    public async Task SmallBatchReopenPinsExactOriginalSourceGenerationAndRebindsServerUuid(int size)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } await using var f = await WindowsFreshCycleTests.Fixture.Create();
        var bytes = Enumerable.Range(0, size).Select(n => (byte)n).ToArray(); File.WriteAllBytes(Path.Combine(f.Source, "café😀"), bytes);
        await Medium(async () =>
        {
            await using var e = await Open(f); f.Remote.LosePrepare = true; _ = await e.InitialSyncAsync(Ct); var s = Session(e);
            var op = (string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE input_json IS NOT NULL"))!;
            var batch = Guid.NewGuid().ToString("D"); await Assert.ThrowsAsync<InvalidOperationException>(() => CreateBatch(s, e.ScopeId, batch, op));
            await EnableBatch(s, e.ScopeId); var before = await Snapshot(f, e.ScopeId); var row = await CreateBatch(s, e.ScopeId, batch, op);
            var w = await ReadWork(s, e.ScopeId, op); Assert.Equal(before, await Snapshot(f, e.ScopeId));
            Assert.Equal((long)(await f.Scalar("SELECT work_no FROM local_cycle_work WHERE operation_id='" + op + "'"))!, row.Plan.Entries[0].SourceGeneration);
            Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(w.CaptureProof!))), row.Plan.Entries[0].SourceIdentity);
            Assert.Equal(0, row.Spans[0].Offset); Assert.Equal(size, row.Plan.DataBytes);
            await SessionCall(s, "MarkCycleHintsAsync", e.ScopeId, new[] { "café😀" }, Ct);
            Assert.Equal(row.Plan.EncodePlan(), (await ReadBatch(s, e.ScopeId, batch)).Plan.EncodePlan());
            await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.Ready, Convert.ToHexStringLower(SHA256.HashData(bytes)));
            await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.Attempted);
            var status = await f.Remote.UploadAsync("prepare", row.Plan.Entries[0].Original, null, w.IntentHash!, Ct);
            await SessionCall(s, "SaveCycleStatusAsync", e.ScopeId, op, status, Ct);
            var reopened = await ReadBatch(s, e.ScopeId, batch); Assert.Equal(status.UploadId, reopened.Plan.Entries[0].OriginalUploadId);
            Assert.Equal(row.Plan.EncodePlan(), reopened.Plan.EncodePlan()); Assert.Equal(LocalSmallBatchPhase.Attempted, reopened.Phase);
            await Assert.ThrowsAsync<InvalidOperationException>(() => BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.CleanupPending));
            await Assert.ThrowsAsync<InvalidOperationException>(() => ForgetBatch(s, e.ScopeId, batch));
            Assert.True(File.Exists(w.SpoolPath)); return true;
        });
    }

    [Theory]
    [InlineData("published")][InlineData("not_published")][InlineData("unknown")][InlineData("conflict")]
    public async Task SmallBatchCleanupRequiresEveryAttemptedOriginalTerminalReceipt(string outcome)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } await using var f = await WindowsFreshCycleTests.Fixture.Create(); var bytes = "payload"u8.ToArray();
        File.WriteAllBytes(Path.Combine(f.Source, "file"), bytes);
        await Medium(async () =>
        {
            await using var e = await Open(f); f.Remote.LosePrepare = true; _ = await e.InitialSyncAsync(Ct); var s = Session(e);
            var op = (string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE input_json IS NOT NULL"))!;
            await EnableBatch(s, e.ScopeId); var batch = Guid.NewGuid().ToString("D"); var row = await CreateBatch(s, e.ScopeId, batch, op); var w = await ReadWork(s, e.ScopeId, op);
            await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.Ready, Convert.ToHexStringLower(SHA256.HashData(bytes)));
            await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.Attempted);
            if (outcome is "published" or "not_published")
            {
                var original = row.Plan.Entries[0].Original; var status = await f.Remote.UploadAsync("prepare", original, null, w.IntentHash!, Ct);
                status = await f.Remote.AppendAsync(original, status.UploadId, w.IntentHash!, 0, bytes, Ct);
                f.Remote.CancelComplete = outcome == "not_published"; status = await f.Remote.UploadAsync("complete", original, status.UploadId, w.IntentHash!, Ct);
                await SessionCall(s, "SaveCycleStatusAsync", e.ScopeId, op, status, Ct);
            }
            else if (outcome == "conflict") await SessionCall(s, "FinishCycleWorkAsync", e.ScopeId, op, LocalCycleWorkPhase.Conflict, null, "uncertain CAS conflict", Ct);
            if (outcome is "unknown" or "conflict")
            {
                await Assert.ThrowsAsync<InvalidOperationException>(() => BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.CleanupPending));
                Assert.Equal(2L, await f.Scalar("SELECT phase FROM local_cycle_batches")); Assert.True(File.Exists(w.SpoolPath)); return true;
            }
            await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.CleanupPending);
            await Assert.ThrowsAsync<InvalidOperationException>(() => ForgetBatch(s, e.ScopeId, batch));
            var before = await Snapshot(f, e.ScopeId); await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.Cleaned); await ForgetBatch(s, e.ScopeId, batch);
            Assert.Equal(before, await Snapshot(f, e.ScopeId)); Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches"));
            Assert.True(File.Exists(w.SpoolPath)); Assert.Equal(0L, await f.Scalar("SELECT baseline_work FROM local_cycle_files WHERE kind=0")); return true;
        });
    }

    [Fact]
    public async Task SmallBatchDatabaseGuardsRetainImmutablePlanAndOffsetsUntilExplicitCleanup()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } await using var f = await WindowsFreshCycleTests.Fixture.Create(); File.WriteAllText(Path.Combine(f.Source, "file"), "bytes");
        await Medium(async () =>
        {
            await using var e = await Open(f); f.Remote.LosePrepare = true; _ = await e.InitialSyncAsync(Ct); var s = Session(e);
            var op = (string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE input_json IS NOT NULL"))!;
            await EnableBatch(s, e.ScopeId); var batch = Guid.NewGuid().ToString("D"); await CreateBatch(s, e.ScopeId, batch, op);
            foreach (var sql in new[] { "UPDATE local_cycle_batches SET plan_json='{}'", "UPDATE local_cycle_batches SET size=size+1", "UPDATE local_cycle_batches SET phase=2", "UPDATE local_cycle_batch_items SET offset=1", "DELETE FROM local_cycle_batch_items", "DELETE FROM local_cycle_batches", "PRAGMA foreign_keys=ON;DELETE FROM local_cycle_work" })
                await Assert.ThrowsAsync<SqliteException>(() => f.Scalar(sql));
            await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.CleanupPending); await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.Cleaned); await ForgetBatch(s, e.ScopeId, batch);
            Assert.Equal(1L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_work")); return true;
        });
    }

    [Theory]
    [InlineData("zero")][InlineData("bytes")][InlineData("corrupt")][InlineData("replace")][InlineData("cancel-before-copy")]
    public async Task SmallBatchNativeAggregateRetainsOriginalsAndRefusesChangedOrSubstitutedBytes(string scenario)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } await using var f = await WindowsFreshCycleTests.Fixture.Create();
        var bytes = scenario == "zero" ? [] : Enumerable.Range(0, 262144).Select(n => (byte)n).ToArray();
        File.WriteAllBytes(Path.Combine(f.Source, "sealed"), bytes);
        await Medium(async () =>
        {
            await using var e = await Open(f); f.Remote.LosePrepare = true; _ = await e.InitialSyncAsync(Ct); var s = Session(e);
            var op = (string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE input_json IS NOT NULL"))!;
            await EnableBatch(s, e.ScopeId); var batch = Guid.NewGuid().ToString("D"); var row = await CreateBatch(s, e.ScopeId, batch, op);
            var work = await ReadWork(s, e.ScopeId, op); var resources = BatchResources(e); var file = Path.Combine(f.Directory, "spool", batch + ".spool");
            Func<string, CancellationToken, Task<LocalCycleWork>> read = (original, token) => SessionCall<LocalCycleWork>(s, "ReadCycleWorkAsync", e.ScopeId, original, token);
            Func<string, CancellationToken, Task> bind = async (identity, token) =>
            {
                await SessionCall(s, "BindSmallBatchSpoolAsync", e.ScopeId, batch, identity, token);
                if (scenario == "cancel-before-copy") throw new OperationCanceledException("owned crash boundary after durable incarnation");
            };
            if (scenario == "cancel-before-copy")
            {
                await Assert.ThrowsAsync<OperationCanceledException>(() => BatchSpoolCall<string?>("TryBuildAsync", resources, row, read, bind, Ct));
                Assert.Equal(0, new FileInfo(file).Length); row = await ReadBatch(s, e.ScopeId, batch); Assert.NotNull(row.SpoolIdentity);
                Assert.Equal(LocalSmallBatchPhase.Building, row.Phase); await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.CleanupPending);
                await BatchSpoolCall("RemoveAsync", resources, await ReadBatch(s, e.ScopeId, batch), Ct);
                Assert.False(File.Exists(file)); Assert.Equal(bytes, await File.ReadAllBytesAsync(work.SpoolPath!, Ct)); return true;
            }
            var hash = await BatchSpoolCall<string?>("TryBuildAsync", resources, row, read, bind, Ct);
            Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(bytes)), hash); Assert.Equal(bytes, await File.ReadAllBytesAsync(file, Ct));
            await SessionCall(s, "ChangeSmallBatchAsync", e.ScopeId, batch, LocalSmallBatchPhase.Ready, hash, Ct); row = await ReadBatch(s, e.ScopeId, batch);
            if (scenario is "corrupt" or "replace")
            {
                if (scenario == "replace") { File.Move(file, file + ".retained-original"); File.WriteAllBytes(file, bytes); }
                else { using var writer = new FileStream(file, FileMode.Open, FileAccess.Write, FileShare.None); writer.WriteByte(99); writer.Flush(true); }
                await Assert.ThrowsAsync<InvalidDataException>(() => BatchSpoolCall<FileStream>("OpenAsync", resources, row, Ct));
                await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.CleanupPending); row = await ReadBatch(s, e.ScopeId, batch);
                await Assert.ThrowsAsync<InvalidDataException>(() => BatchSpoolCall("RemoveAsync", resources, row, Ct)); Assert.True(File.Exists(file));
                Assert.Equal(bytes, await File.ReadAllBytesAsync(work.SpoolPath!, Ct)); return true;
            }
            await using (var opened = await BatchSpoolCall<FileStream>("OpenAsync", resources, row, Ct))
            {
                var actual = new byte[bytes.Length]; await opened.ReadExactlyAsync(actual, Ct); Assert.Equal(bytes, actual);
                Assert.Throws<IOException>(() => new FileStream(file, FileMode.Open, FileAccess.Write, FileShare.ReadWrite));
                Assert.Throws<IOException>(() => File.Move(file, file + ".renamed"));
                await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.CleanupPending);
                await Assert.ThrowsAsync<System.ComponentModel.Win32Exception>(async () => await BatchSpoolCall("RemoveAsync", resources, await ReadBatch(s, e.ScopeId, batch), Ct));
            }
            row = await ReadBatch(s, e.ScopeId, batch); await BatchSpoolCall("RemoveAsync", resources, row, Ct);
            await BatchSpoolCall("RemoveAsync", resources, row, Ct); // crash after unlink, before phase4
            await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.Cleaned); await ForgetBatch(s, e.ScopeId, batch);
            Assert.False(File.Exists(file)); Assert.Equal(bytes, await File.ReadAllBytesAsync(work.SpoolPath!, Ct)); return true;
        });
    }

    [Fact]
    public async Task SmallBatchAggregateQuotaFallsBackWithoutWaitingForHeldOriginals()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } await using var f = await WindowsFreshCycleTests.Fixture.Create(); File.WriteAllBytes(Path.Combine(f.Source, "full"), new byte[1024]);
        await Medium(async () =>
        {
            await using var e = await WindowsFreshSyncEngine.OpenAsync(Options, f.Store, f.Source, Path.Combine(f.Directory, "spool"), Config(f), f.Remote,
                new(SpoolBytes: 1024, MinimumFreeDiskBytes: 10L << 30), cancellationToken: Ct);
            f.Remote.LosePrepare = true; _ = await e.InitialSyncAsync(Ct); var s = Session(e);
            var op = (string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE input_json IS NOT NULL"))!;
            await EnableBatch(s, e.ScopeId); var batch = Guid.NewGuid().ToString("D"); var row = await CreateBatch(s, e.ScopeId, batch, op);
            Func<string, CancellationToken, Task<LocalCycleWork>> read = (_, _) => throw new Xunit.Sdk.XunitException("Quota refusal must precede file reads");
            Func<string, CancellationToken, Task> bind = (_, _) => throw new Xunit.Sdk.XunitException("Quota refusal must precede file creation");
            var watch = System.Diagnostics.Stopwatch.StartNew(); Assert.Null(await BatchSpoolCall<string?>("TryBuildAsync", BatchResources(e), row, read, bind, Ct));
            Assert.True(watch.Elapsed < TimeSpan.FromSeconds(2)); Assert.Single(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
            Assert.Null((await ReadBatch(s, e.ScopeId, batch)).SpoolIdentity); return true;
        });
    }

    [Fact]
    public async Task SmallBatchRestoreRejectsOriginalBackendDriftEvenWhenIntentHashIsUnchanged()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } await using var f = await WindowsFreshCycleTests.Fixture.Create(); File.WriteAllText(Path.Combine(f.Source, "file"), "bytes");
        await Medium(async () =>
        {
            await using var e = await Open(f); f.Remote.LosePrepare = true; _ = await e.InitialSyncAsync(Ct); var s = Session(e);
            var op = (string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE input_json IS NOT NULL"))!;
            await EnableBatch(s, e.ScopeId); var batch = Guid.NewGuid().ToString("D"); await CreateBatch(s, e.ScopeId, batch, op);
            await f.Scalar("UPDATE local_cycle_work SET input_json=json_set(input_json,'$.backendIdentity','changed-backend')");
            await Assert.ThrowsAsync<InvalidDataException>(() => ReadBatch(s, e.ScopeId, batch)); return true;
        });
    }

    [Fact]
    public async Task SmallBatchPinRemainsRetirableAfterBoundedHistoryCursorWrap()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } await using var f = await WindowsFreshCycleTests.Fixture.Create(); File.WriteAllText(Path.Combine(f.Source, "file"), "bytes");
        await Medium(async () =>
        {
            await using var e = await Open(f); f.Remote.LosePrepare = true; _ = await e.InitialSyncAsync(Ct); var s = Session(e);
            var op = (string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE input_json IS NOT NULL"))!;
            await EnableBatch(s, e.ScopeId); var batch = Guid.NewGuid().ToString("D"); var row = await CreateBatch(s, e.ScopeId, batch, op); var w = await ReadWork(s, e.ScopeId, op);
            await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.Ready, w.Sha256); await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.Attempted);
            var original = row.Plan.Entries[0].Original; var status = await f.Remote.UploadAsync("prepare", original, null, w.IntentHash!, Ct);
            _ = await f.Remote.AppendAsync(original, status.UploadId, w.IntentHash!, 0, "bytes"u8.ToArray(), Ct);
            status = await f.Remote.UploadAsync("complete", original, status.UploadId, w.IntentHash!, Ct); await SessionCall(s, "SaveCycleStatusAsync", e.ScopeId, op, status, Ct);
            // Owned fixture represents an older fully acknowledged operation
            // with no latest-baseline reference. The batch is its only pin.
            await f.Scalar("UPDATE local_cycle_work SET active=NULL");
            async Task<LocalCycleHistoryRetirement> Retire()
            {
                var task = (Task)typeof(LocalSourceJournalSession).GetMethod("ReadCycleRetirableWorkAsync", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(s, [e.ScopeId, Ct])!;
                await task; var page = task.GetType().GetProperty("Result")!.GetValue(task)!;
                var records = (IReadOnlyList<LocalCycleWork>)page.GetType().GetProperty("Records")!.GetValue(page)!;
                return await SessionCall<LocalCycleHistoryRetirement>(s, "RetireCycleWorkAsync", e.ScopeId, page, records.Select(x => x.OperationId).ToArray(), Ct);
            }
            Assert.Equal(0, (await Retire()).Records); Assert.Equal(1L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_work"));
            await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.CleanupPending); await BatchPhase(s, e.ScopeId, batch, LocalSmallBatchPhase.Cleaned); await ForgetBatch(s, e.ScopeId, batch);
            // The existing bounded sweep wraps its cursor after an empty page.
            // Keeping this behavior avoids one pinned item starving later pages.
            Assert.Equal(0, (await Retire()).Records); Assert.Equal(0L, await f.Scalar("SELECT work_retire_after FROM local_cycle_scopes"));
            Assert.Equal(1, (await Retire()).Records); Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_work")); return true;
        });
    }
}
