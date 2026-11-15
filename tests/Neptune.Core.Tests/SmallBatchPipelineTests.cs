using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    [Fact]
    public async Task QualifiedTwoJobPipelineOverlapsWhileOldCapabilityRemainsSerial()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(); BatchFiles(f, 32);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { ExecutionJobs = true, ParallelJobs = 2 };
            var two = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            remote.AfterPrepare = () => { if (Volatile.Read(ref remote.BatchPrepares) == 2) two.TrySetResult(); };
            remote.BeforeSend = (_, ct) => two.Task.WaitAsync(TimeSpan.FromSeconds(8), ct);
            await using var engine = await OpenBatchEngine(f, remote);
            Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
            var result = await engine.InitialSyncAsync(Ct);
            Assert.True(two.Task.IsCompletedSuccessfully, result.Error); Assert.Null(result.Error);
            Assert.Equal(32, result.PublishedFiles); Assert.Equal(2, remote.BatchPrepares); Assert.Equal(2, remote.BatchSends);
            Assert.InRange(engine.ResourceMetrics.MaximumDataJobs, 1, 4);
            Assert.Equal(0L, await f.Scalar("SELECT count(*) FROM local_cycle_batches"));
            return true;
        });
    }
    [Fact]
    public async Task ExecutionJobPipelineWaitsForPriorRootJobBeforeNextAdmission()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(); BatchFiles(f, 32);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { ExecutionJobs = true };
            remote.BeforeSend = async (plan, ct) =>
            {
                Assert.True(plan.ExecutionJob); var accepted = Volatile.Read(ref remote.BatchPrepares);
                await Task.Delay(250, ct);
                Assert.Equal(accepted, Volatile.Read(ref remote.BatchPrepares));
            };
            await using var engine = await OpenBatchEngine(f, remote);
            Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
            var result = await engine.InitialSyncAsync(Ct); Assert.Null(result.Error);
            Assert.Equal(2, remote.BatchPrepares); Assert.Equal(2, remote.BatchSends);
            Assert.Equal(0, remote.IndividualPrepares); return true;
        });
    }
    [Fact]
    public async Task SmallBatchPipelineConcurrentLostResponseReopensSameOriginalsWithoutRecapture()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());
        BatchFiles(f, 32);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { LoseData = true };
            var both = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            remote.AfterPrepare = () => { if (Volatile.Read(ref remote.BatchPrepares) == 2) both.TrySetResult(); };
            remote.BeforeSend = (_, ct) => both.Task.WaitAsync(TimeSpan.FromSeconds(8), ct);
            string scope; object? originals;
            await using (var engine = await OpenBatchEngine(f, remote))
            {
                scope = engine.ScopeId;
                Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
                var interrupted = await engine.InitialSyncAsync(Ct);
                Assert.NotNull(interrupted.Error);
                Assert.Equal(32, interrupted.CapturedFiles);
                Assert.Equal(2, remote.BatchPrepares);
                Assert.Equal(32, f.Remote.Prepares);
                originals = await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)");
            }
            remote.BeforeSend = null;
            await using var resumed = await OpenBatchEngine(f, remote, resume: true);
            Assert.Equal(scope, resumed.ScopeId);
            var recovered = await f.FreshWhenDueAsync(resumed, Ct);
            Assert.True(recovered.Error is null, recovered.Error);
            Assert.Equal("NoWorkAtAppliedEventBoundary", recovered.Decision);
            Assert.Equal(0, recovered.CapturedBytes);
            Assert.Equal(32, f.Remote.Prepares);
            Assert.Equal(originals, await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"));
            Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches"));
            Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
            Assert.Equal(32, Directory.GetFiles(f.Source).Length);
            return true;
        });
    }

    [Fact]
    public async Task SmallBatchPipelineSpoolPressureDefersNewCaptureInsteadOfHoldingAllSlots()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create();
        using var resources = new LocalCycleResources(Path.Combine(f.Directory, "bounded-spool"), new(SpoolBytes: 1024, MinimumFreeDiskBytes: 0));
        resources.DeferCaptureOnSpoolPressure = true;
        var retained = Guid.NewGuid().ToString("D");
        await resources.ReserveAsync(retained, 1024, Ct);
        try
        {
            var pending = resources.ReserveAsync(Guid.NewGuid().ToString("D"), 1, Ct);
            await Assert.ThrowsAsync<CycleSpoolAdmissionException>(() => pending.WaitAsync(TimeSpan.FromSeconds(2), Ct));
            Assert.False(resources.TryReserveAggregate(Guid.NewGuid().ToString("D"), 1));
        }
        finally { resources.Release(retained, true); }
        var next = Guid.NewGuid().ToString("D");
        await resources.ReserveAsync(next, 1024, Ct); resources.Release(next, true);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SmallBatchPipelineOverlapsNextDeclarationAndRetainsAllOriginalChecks(bool fresh)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());
        if (!fresh) BatchFiles(f, 48);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote);
            var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            remote.AfterPrepare = () => { if (Volatile.Read(ref remote.BatchPrepares) >= 2) second.TrySetResult(); };
            var sends = 0;
            remote.BeforeSend = async (_, ct) =>
            {
                if (Interlocked.Increment(ref sends) == 1)
                    await second.Task.WaitAsync(TimeSpan.FromSeconds(8), ct);
            };
            await using var engine = await OpenBatchEngine(f, remote);
            Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
            if (fresh)
            {
                var empty = await engine.InitialSyncAsync(Ct);
                Assert.Null(empty.Error); Assert.Equal(0, empty.PublishedFiles);
                BatchFiles(f, 48);
            }
            var initial = fresh ? await f.FreshWhenDueAsync(engine, Ct) : await engine.InitialSyncAsync(Ct);
            Assert.True(initial.Error is null, initial.Error);
            Assert.True(second.Task.IsCompletedSuccessfully);
            // Capture speed can legitimately trigger the age limit before a
            // full group forms, with an aged singleton using individual send.
            Assert.InRange(remote.BatchSizes.Count, 2, 24);
            Assert.Equal(48, remote.BatchSizes.Sum() + remote.IndividualPrepares);
            Assert.All(remote.BatchSizes, size => Assert.InRange(size, 2, 16));
            Assert.Equal(48, initial.PublishedFiles);
            Assert.Equal(48, initial.CapturedFiles);
            Assert.Equal(48, f.Remote.Prepares);
            Assert.Equal(48, Directory.GetFiles(f.Source).Length);
            Assert.InRange(engine.ResourceMetrics.MaximumDataJobs, 1, 4);
            Assert.Equal(1, engine.ResourceMetrics.MaximumSourceReaders);
            Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
            Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches"));
            var repeat = await f.FreshWhenDueAsync(engine, Ct);
            Assert.Null(repeat.Error); Assert.Equal(0, repeat.CapturedBytes);
            Assert.Equal(0, repeat.PublishedFiles);
            await f.Evidence("batch-pipeline-overlap", new { initial, repeat, remote.BatchSizes, engine.ResourceMetrics });
            return true;
        });
    }
}
