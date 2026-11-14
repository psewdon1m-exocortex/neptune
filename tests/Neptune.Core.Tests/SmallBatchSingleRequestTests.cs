using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    [Theory]
    [InlineData(false, -1)]
    [InlineData(true, -1)]
    [InlineData(false, 5)]
    public async Task SingleRequestPersistsOriginalsBeforeSendAndRecoversLostResponse(bool loseResponse, int cutAfter)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock()); BatchFiles(f, 16);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { ExecutionJobs = true, ParallelJobs = 2, SingleRequest = true, LoseData = loseResponse, CutAfter = cutAfter };
            remote.BeforeSend = async (plan, ct) =>
            {
                Assert.Equal(16L, await f.Scalar("SELECT count(*) FROM local_cycle_work WHERE input_json IS NOT NULL AND capture_proof IS NOT NULL"));
                Assert.Equal(1L, await f.Scalar("SELECT count(*) FROM local_cycle_batches"));
            };
            object? originals; string scope;
            await using (var engine = await OpenBatchEngine(f, remote))
            {
                scope = engine.ScopeId; Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
                var first = await engine.InitialSyncAsync(Ct);
                if (loseResponse || cutAfter >= 0) Assert.NotNull(first.Error); else Assert.True(first.Error is null, first.Error);
                Assert.Equal(0, remote.BatchPrepares); Assert.Equal(1, remote.BatchSends);
                originals = await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)");
            }
            remote.BeforeSend = null;
            await using var resumed = await OpenBatchEngine(f, remote, resume: true);
            Assert.Equal(scope, resumed.ScopeId); var recovered = await f.FreshWhenDueAsync(resumed, Ct);
            Assert.True(recovered.Error is null, recovered.Error); Assert.Equal(0, recovered.CapturedBytes);
            Assert.Equal(16, f.Remote.Prepares); Assert.Equal(1, remote.BatchSends); Assert.Equal(0, remote.BatchPrepares);
            Assert.Equal(originals, await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"));
            Assert.Equal(0L, await f.Scalar("SELECT count(*) FROM local_cycle_batches"));
            Assert.Equal(16, Directory.GetFiles(f.Source).Length);
            return true;
        });
    }
}
