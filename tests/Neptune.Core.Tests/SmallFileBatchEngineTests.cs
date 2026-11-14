using System.Reflection;
using System.Text.Json;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    private sealed class BatchRemote(WindowsFreshCycleTests.FakeRemote inner) : IWindowsCycleRemote, ISmallFileBatchRemote, IWindowsCycleExtensions
    {
        internal bool Available = true, LosePrepare, LoseData, ExecutionJobs, SingleRequest; internal int CutAfter = -1;
        internal bool LegacyAvailable;
        internal int LegacyCapabilityReads;
        internal Func<SmallFileBatchCapability, SmallFileBatchCapability>? LegacyCapabilityRewrite;
        internal int ParallelJobs = 1, LoseAfterSend = -1;
        internal int BatchPrepares, BatchSends, CapabilityReads, BatchStatusReads, IndividualPrepares;
        internal int ContentLookups;
        public int NamespaceRevision => inner.NamespaceRevision;
        public SyncNamespacePolicy? NamespacePolicy => inner.NamespacePolicy;
        public bool Supports(string feature) => inner.Supports(feature);
        public Task<SyncMutationReceipt> MutateAsync(SyncMutationRequest original,bool inspect,CancellationToken ct) => inner.MutateAsync(original,inspect,ct);
        public Task<SyncContentReference?> LookupContentAsync(long size,string sha256,CancellationToken ct)
        { Interlocked.Increment(ref ContentLookups); return inner.LookupContentAsync(size,sha256,ct); }
        public Task<SyncFeedPage> WaitForChangesAsync(string cursor,long after,CancellationToken ct) => inner.WaitForChangesAsync(cursor,after,ct);
        public Task<string> ApproveDeletePlanAsync(SyncDeletePlan original,CancellationToken ct) => inner.ApproveDeletePlanAsync(original,ct);
        internal bool MissingDeclaration, CorruptStatusOrder, ReturnPartialOutcome, WireJsonEscaping;
        internal Func<CycleUploadInput, Task>? AfterFirstPublication;
        internal Func<SmallFileBatchPlan, CancellationToken, Task>? BeforeSend;
        internal Func<SmallFileBatchPlan, int>? PublicationPrefix;
        internal Func<string, string>? ReceiptRewrite;
        internal Action? AfterPrepare;
        internal readonly System.Collections.Concurrent.ConcurrentQueue<int> BatchSizes = new();
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte[]> _plans = new(StringComparer.Ordinal);
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, CycleUploadStatus> _statuses = new(StringComparer.Ordinal);
        public SyncRootBinding Binding => inner.Binding;
        public Task<SmallFileBatchCapability?> ReadSmallBatchCapabilitiesAsync(CancellationToken ct)
        { CapabilityReads++; return Task.FromResult<SmallFileBatchCapability?>(Available ? new(Binding, inner.Device, "local-test", new string('a', 64), ExecutionJobs ? 16 : 500, 262144, ExecutionJobs ? 4194304 : 33554432, ExecutionJobs ? 1048576 : 4194304, ExecutionJobs, ParallelJobs, SingleRequest) : null); }
        public Task<SmallFileBatchCapability?> ReadLegacySmallBatchCapabilitiesAsync(CancellationToken ct)
        {
            Interlocked.Increment(ref LegacyCapabilityReads);
            SmallFileBatchCapability? cap = LegacyAvailable ? new(Binding, inner.Device, "local-test", new string('a', 64), 500, 262144, 33554432, 4194304) : null;
            return Task.FromResult(cap is null ? null : LegacyCapabilityRewrite?.Invoke(cap) ?? cap);
        }
        private SmallFileBatchOutcome Outcome(SmallFileBatchPlan plan) => new(plan.BatchId, plan.Entries.Select(e =>
        {
            var status = _statuses.GetValueOrDefault(e.Original.Intent.IdempotencyKey);
            if (WireJsonEscaping && status?.ResultJson is { } json)
                status = status with { ResultJson = JsonSerializer.Serialize(JsonSerializer.Deserialize<JsonElement>(json),
                    new JsonSerializerOptions { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping }) };
            if (ReceiptRewrite is { } rewrite && status?.ResultJson is { } receipt)
                status = status with { ResultJson = rewrite(receipt) };
            return new SmallFileBatchItemOutcome(e.Original.Intent.IdempotencyKey, e.SourceGeneration, e.SourceIdentity,
                status is null ? "MISSING" : status.Phase == "published" ? "PUBLISHED" : status.Phase == "not_published" ? "NOT_PUBLISHED" : "PENDING", status, null);
        }).ToArray());
        public Task<SmallFileBatchOutcome> PrepareSmallBatchAsync(SmallFileBatchPlan plan, CancellationToken ct)
        {
            Interlocked.Increment(ref BatchPrepares); if (_plans.TryGetValue(plan.BatchId, out var old)) Assert.Equal(old, plan.EncodePlan()); else Assert.True(_plans.TryAdd(plan.BatchId, plan.EncodePlan()));
            BatchSizes.Enqueue(plan.Entries.Count); AfterPrepare?.Invoke();
            if (LosePrepare) { LosePrepare = false; throw new IOException("lost batch declaration acknowledgement"); }
            return Task.FromResult(Outcome(plan));
        }
        public Task<SmallFileBatchOutcome> ReadSmallBatchAsync(SmallFileBatchPlan plan, CancellationToken ct)
        {
            BatchStatusReads++;
            if (MissingDeclaration) throw new GuardedCycleRemoteException(System.Net.HttpStatusCode.NotFound, "batch_missing", true);
            Assert.Equal(_plans[plan.BatchId], plan.EncodePlan()); var result = Outcome(plan);
            return Task.FromResult(CorruptStatusOrder ? result with { Items = result.Items.Reverse().ToArray() } : result);
        }
        public async Task<SmallFileBatchOutcome> ExecuteSmallJobAsync(SmallFileBatchPlan plan, Stream bytes, CancellationToken ct)
        {
            Assert.True(SingleRequest); Assert.True(plan.ExecutionJob);
            if (_plans.TryGetValue(plan.BatchId, out var old)) Assert.Equal(old, plan.EncodePlan()); else Assert.True(_plans.TryAdd(plan.BatchId, plan.EncodePlan()));
            BatchSizes.Enqueue(plan.Entries.Count);
            // The real single-request SQL accepts every original before the
            // first body part, including those beyond a later transport cut.
            foreach (var entry in plan.Entries)
            {
                var input = entry.Original;
                var hash = (string)typeof(NeptuneStateStore).Assembly.GetType("Neptune.Core.LocalCycleWire")!.GetMethod("IntentHash", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [input])!;
                _statuses[input.Intent.IdempotencyKey] = await inner.UploadAsync("prepare", input, null, hash, ct);
            }
            return await SendSmallBatchAsync(plan, bytes, ct);
        }
        public async Task<SmallFileBatchOutcome> SendSmallBatchAsync(SmallFileBatchPlan plan, Stream bytes, CancellationToken ct)
        {
            var sendNumber = Interlocked.Increment(ref BatchSends); Assert.Equal(_plans[plan.BatchId], plan.EncodePlan());
            if (BeforeSend is { } before) await before(plan, ct);
            var prefix = PublicationPrefix?.Invoke(plan) ?? -1;
            for (var n = 0; n < plan.Entries.Count; n++)
            {
                if (n == prefix) return Outcome(plan);
                if (CutAfter == n) { CutAfter = -1; if (ReturnPartialOutcome) return Outcome(plan); throw new IOException("connection cut before next original"); }
                var input = plan.Entries[n].Original; var payload = new byte[int.Parse(input.Intent.SizeBytes, System.Globalization.CultureInfo.InvariantCulture)];
                await bytes.ReadExactlyAsync(payload, ct); Assert.Equal(input.Intent.Sha256, Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(payload)));
                var hash = (string)typeof(NeptuneStateStore).Assembly.GetType("Neptune.Core.LocalCycleWire")!.GetMethod("IntentHash", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [input])!;
                var status = SingleRequest && _statuses.TryGetValue(input.Intent.IdempotencyKey, out var admitted)
                    ? admitted : await inner.UploadAsync("prepare", input, null, hash, ct);
                if (status.Phase != "published")
                {
                    if (payload.Length > 0) status = await inner.AppendAsync(input, status.UploadId, hash, 0, payload, ct);
                    status = await inner.UploadAsync("complete", input, status.UploadId, hash, ct);
                }
                _statuses[input.Intent.IdempotencyKey] = status;
                if (n == 0 && AfterFirstPublication is { } callback) { AfterFirstPublication = null; await callback(input); }
            }
            Assert.Equal(bytes.Length, bytes.Position);
            if (LoseData || LoseAfterSend == sendNumber) { LoseData = false; throw new IOException("lost batch data acknowledgement after publications"); }
            return Outcome(plan);
        }
        public Task<byte[]> StartSnapshotAsync(string id, CancellationToken ct) => inner.StartSnapshotAsync(id, ct);
        public Task<byte[]> StepSnapshotAsync(string id, CancellationToken ct) => inner.StepSnapshotAsync(id, ct);
        public Task<SyncSnapshotReady> ReadySnapshotAsync(string id, CancellationToken ct) => inner.ReadySnapshotAsync(id, ct);
        public Task<SyncSnapshotEntriesPage> ReadEntriesAsync(SyncSnapshotReady ready, string cursor, string? after, CancellationToken ct) => inner.ReadEntriesAsync(ready, cursor, after, ct);
        public Task<SyncFeedPage> ReadChangesAsync(string cursor, long after, CancellationToken ct) => inner.ReadChangesAsync(cursor, after, ct);
        public Task<CycleFolderOutcome> EnsureDirectoryAsync(CycleFolderInput input, CancellationToken ct) => inner.EnsureDirectoryAsync(input, ct);
        public Task<CycleUploadStatus> UploadAsync(string command, CycleUploadInput input, string? upload, string hash, CancellationToken ct)
        { if (command == "prepare") IndividualPrepares++; return inner.UploadAsync(command, input, upload, hash, ct); }
        public Task<CycleUploadStatus> AppendAsync(CycleUploadInput input, string upload, string hash, long offset, ReadOnlyMemory<byte> bytes, CancellationToken ct) => inner.AppendAsync(input, upload, hash, offset, bytes, ct);
    }
    private static Task<WindowsFreshSyncEngine> OpenBatchEngine(WindowsFreshCycleTests.Fixture f, BatchRemote remote, WindowsCycleBudget? budget = null, bool resume = false)
        => resume ? WindowsFreshSyncEngine.ResumeOrOpenAsync(Options, f.Store, f.Source, Path.Combine(f.Directory, "spool"), Config(f), remote,
            budget ?? new(MinimumFreeDiskBytes: 10L << 30), cancellationToken: Ct)
        : WindowsFreshSyncEngine.OpenAsync(Options, f.Store, f.Source, Path.Combine(f.Directory, "spool"), Config(f), remote,
            budget ?? new(MinimumFreeDiskBytes: 10L << 30), cancellationToken: Ct);
    private static void BatchFiles(WindowsFreshCycleTests.Fixture f, int count = 4, int size = 1024)
    { for (var n = 0; n < count; n++) File.WriteAllBytes(Path.Combine(f.Source, "file-" + n), Enumerable.Repeat((byte)n, size).ToArray()); }

    [Fact]
    public async Task SmallBatchNativeEnginePublishesFourOriginalsAndFreshCycleReadsNoSourceBytes()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock()); BatchFiles(f);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote); await using var e = await OpenBatchEngine(f, remote); Assert.True(await e.EnableSmallFileBatchQualificationAsync(Ct));
            var initial = await e.InitialSyncAsync(Ct); Assert.Null(initial.Error); Assert.Equal("NoWorkAtAppliedEventBoundary", initial.Decision);
            Assert.Equal(4, initial.CapturedFiles); Assert.Equal(4, initial.PublishedFiles); Assert.Equal(4096, initial.CapturedBytes);
            Assert.Equal(1, remote.BatchPrepares); Assert.Equal(1, remote.BatchSends); Assert.Equal(4, f.Remote.Prepares);
            Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches")); Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
            var repeat = await f.FreshWhenDueAsync(e,Ct); Assert.Null(repeat.Error); Assert.Equal(0, repeat.CapturedBytes); Assert.Equal(0, repeat.PublishedFiles);
            Assert.Equal(1, remote.BatchSends); Assert.InRange(e.ResourceMetrics.MaximumSourceReaders, 1, 1); Assert.InRange(e.ResourceMetrics.MaximumDataJobs, 1, 4);
            await f.Evidence("batch-engine-initial-repeat", new { initial, repeat, remote.BatchPrepares, remote.BatchSends, e.ResourceMetrics }); return true;
        });
    }

    [Theory]
    [InlineData("lost-prepare", 4)][InlineData("lost-data", 4)][InlineData("partial", 4)]
    [InlineData("lost-prepare", 16)][InlineData("lost-data", 16)][InlineData("partial", 16)]
    public async Task SmallBatchNativeEngineRecoversSameOriginalsAfterResponseLossAndEngineReopen(string failure, int files = 4)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } var clock=new Stage3Clock();await using var f = await WindowsFreshCycleTests.Fixture.Create(clock); BatchFiles(f, files);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { LosePrepare = failure == "lost-prepare", LoseData = failure == "lost-data", CutAfter = failure == "partial" ? 1 : -1 };
            string scope; string originals;
            await using (var e = await OpenBatchEngine(f, remote))
            {
                scope = e.ScopeId; Assert.True(await e.EnableSmallFileBatchQualificationAsync(Ct)); var first = await e.InitialSyncAsync(Ct);
                Assert.Equal("PendingOrUncertain", first.Decision); Assert.Equal(1L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches WHERE phase=2"));
                originals = (string)(await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"))!;
            }
            await Stage3ChatBTests.AdvancePastDue(f,clock);await using var resumed = await OpenBatchEngine(f, remote, resume: true); Assert.Equal(scope, resumed.ScopeId);
            // No batch enable call: retained originals recover individually in
            // the standard engine even if optional transport is unavailable.
            remote.Available = false; var recovered = await f.FreshWhenDueAsync(resumed,Ct);
            Assert.Null(recovered.Error); Assert.Equal("NoWorkAtAppliedEventBoundary", recovered.Decision); Assert.Equal(0, recovered.CapturedBytes);
            Assert.Equal(originals, await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"));
            Assert.Equal(files, f.Remote.Prepares); Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches"));
            Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
            await f.Evidence("batch-engine-recovery-" + failure, new { recovered, scope, originalKeysPreserved = true, remote.BatchPrepares, remote.BatchSends }); return true;
        });
    }

    [Theory]
    [InlineData("lost-prepare", 4)][InlineData("lost-data", 0)][InlineData("partial", 3)]
    public async Task SmallBatchNativeEngineReadsOneStatusAndDispatchesOnlyMissingOriginals(string failure, int expectedPrepares)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock()); BatchFiles(f);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { LosePrepare = failure == "lost-prepare", LoseData = failure == "lost-data", CutAfter = failure == "partial" ? 1 : -1 };
            await using (var engine = await OpenBatchEngine(f, remote))
            { Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct)); Assert.NotNull((await engine.InitialSyncAsync(Ct)).Error); }
            var originals = await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)");
            await using var resumed = await OpenBatchEngine(f, remote, resume: true);
            var recovered = await f.FreshWhenDueAsync(resumed,Ct);
            Assert.Null(recovered.Error); Assert.Equal("NoWorkAtAppliedEventBoundary", recovered.Decision); Assert.Equal(0, recovered.CapturedFiles);
            Assert.Equal(1, remote.BatchStatusReads); Assert.Equal(expectedPrepares, remote.IndividualPrepares);
            Assert.Equal(4, f.Remote.Prepares); Assert.Equal(originals, await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"));
            Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches")); Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
            var repeat = await f.FreshWhenDueAsync(resumed,Ct); Assert.Null(repeat.Error); Assert.Equal(0, repeat.CapturedBytes); Assert.Equal(0, repeat.PublishedFiles); Assert.Equal(1, remote.BatchStatusReads);
            await f.Evidence("batch-status-recovery-" + failure, new { recovered, repeat, remote.BatchStatusReads, remote.IndividualPrepares }); return true;
        });
    }

    [Theory]
    [InlineData(false)][InlineData(true)]
    public async Task SmallBatchNativeEngineMissingDeclarationFallsBackButChangedOrderRetainsOriginals(bool corrupt)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock()); BatchFiles(f);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { LoseData = true };
            await using (var engine = await OpenBatchEngine(f, remote))
            { Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct)); Assert.NotNull((await engine.InitialSyncAsync(Ct)).Error); }
            var originals = await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)");
            remote.MissingDeclaration = !corrupt; remote.CorruptStatusOrder = corrupt;
            await using var resumed = await OpenBatchEngine(f, remote, resume: true); var recovered = await f.FreshWhenDueAsync(resumed,Ct);
            Assert.Equal(1, remote.BatchStatusReads); Assert.Equal(0, recovered.CapturedFiles);
            Assert.Equal(originals, await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"));
            if (corrupt)
            {
                Assert.NotNull(recovered.Error); Assert.Equal(0, remote.IndividualPrepares); Assert.Equal(1L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches"));
                Assert.Equal(4L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE result_json IS NULL"));
                remote.CorruptStatusOrder = false; var retry = await f.FreshWhenDueAsync(resumed,Ct); Assert.Null(retry.Error); Assert.Equal(2, remote.BatchStatusReads);
            }
            else { Assert.Null(recovered.Error); Assert.Equal(4, remote.IndividualPrepares); }
            Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches")); Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool")); return true;
        });
    }

    [Theory]
    [InlineData("disabled")][InlineData("unsupported")][InlineData("quota")][InlineData("singleton")]
    public async Task SmallBatchNativeEngineIndividualFallbackPreservesExistingPublicationPath(string mode)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } await using var f = await WindowsFreshCycleTests.Fixture.Create(); BatchFiles(f, mode == "singleton" ? 1 : 4, 256);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { Available = mode != "unsupported" };
            await using var e = await OpenBatchEngine(f, remote, new(SpoolBytes: mode == "quota" ? 1024 : 1024 * 1024, MinimumFreeDiskBytes: 10L << 30));
            if (mode != "disabled") Assert.Equal(mode != "unsupported", await e.EnableSmallFileBatchQualificationAsync(Ct));
            var initial = await e.InitialSyncAsync(Ct); Assert.Null(initial.Error); Assert.Equal(mode == "singleton" ? 1 : 4, initial.PublishedFiles);
            Assert.Equal(0, remote.BatchSends); Assert.Equal(0, remote.BatchPrepares); Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
            Assert.Equal(mode is "disabled" or "unsupported" ? 4L : 7L, await f.Scalar("SELECT version FROM local_cycle_format")); return true;
        });
    }

    [Fact]
    public async Task SmallBatchNativeEnginePublicationCannotAcknowledgeNewerDirtyGeneration()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock()); BatchFiles(f);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote); remote.AfterFirstPublication = input => { File.WriteAllText(Path.Combine(f.Source, input.Intent.Name), "new generation while old batch publishes"); return Task.CompletedTask; };
            await using var e = await OpenBatchEngine(f, remote); Assert.True(await e.EnableSmallFileBatchQualificationAsync(Ct));
            var result = await f.InitialWhenDueAsync(e,Ct); Assert.Null(result.Error); Assert.Equal("NoWorkAtAppliedEventBoundary", result.Decision);
            Assert.Equal(5, result.CapturedFiles); Assert.Equal(5, result.PublishedFiles); Assert.Equal(5, f.Remote.Prepares);
            Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE active=1")); Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches"));
            Assert.Equal(0, (await f.FreshWhenDueAsync(e,Ct)).CapturedBytes); return true;
        });
    }
}
