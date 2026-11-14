using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    [Fact]
    public async Task PartialBatchDoesNotStopIndependentFilesInSamePass()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Requires Windows native filesystem fixture");
        var clock = new Stage3Clock();
        await using var f = await WindowsFreshCycleTests.Fixture.Create(clock);
        BatchFiles(f, 40);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { CutAfter = 1, ReturnPartialOutcome = true };
            await using var engine = await OpenBatchEngine(f, remote);
            Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
            var partial = await engine.InitialSyncAsync(Ct);
            await f.Evidence("partial-batch-independent-files", new { partial, remote.BatchSends, remote.BatchSizes });
            Assert.Equal(40, partial.CapturedFiles);
            Assert.Equal(25, partial.PublishedFiles);
            Assert.Equal(15L, await f.Scalar("SELECT COUNT(*) FROM client_operation_schedule WHERE reason='outcome_pending'"));
            Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM client_operation_schedule WHERE operation_id='cycle'"));
            var originals = await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)");
            await Stage3ChatBTests.AdvancePastDue(f, clock);
            var recovered = await f.FreshWhenDueAsync(engine, Ct);
            Assert.Null(recovered.Error);
            Assert.Equal("NoWorkAtAppliedEventBoundary", recovered.Decision);
            Assert.Equal(0, recovered.CapturedFiles);
            Assert.Equal(40, f.Remote.Completes);
            Assert.Equal(originals, await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"));
            Assert.Equal(40, Directory.GetFiles(f.Source).Length);
            return true;
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task PartialBatchNormalResponseRetainsPerOriginalRetryAndEquivalentWireReceipts(bool checkRetryOwnership)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Requires Windows native filesystem fixture");
        var clock = new Stage3Clock();
        await using var f = await WindowsFreshCycleTests.Fixture.Create(clock);
        BatchFiles(f);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { CutAfter = 1, ReturnPartialOutcome = true, WireJsonEscaping = true };
            string originalInputs;
            await using (var engine = await OpenBatchEngine(f, remote))
            {
                Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
                var partial = await engine.InitialSyncAsync(Ct);
                Assert.Equal(1, partial.PublishedFiles);
                Assert.Equal(1L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches WHERE phase=2"));
                Assert.Equal(3L, await f.Scalar("SELECT COUNT(*) FROM client_operation_schedule WHERE reason='outcome_pending'"));
                originalInputs = (string)(await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"))!;
                if (checkRetryOwnership)
                {
                    Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM client_operation_schedule WHERE operation_id='cycle'"));
                    Assert.DoesNotContain("CycleSpoolAdmissionException", partial.Error ?? "");
                }
            }
            await Stage3ChatBTests.AdvancePastDue(f, clock);
            await using var resumed = await OpenBatchEngine(f, remote, resume: true);
            var recovered = await f.FreshWhenDueAsync(resumed, Ct);
            Assert.Null(recovered.Error);
            Assert.Equal("NoWorkAtAppliedEventBoundary", recovered.Decision);
            Assert.Equal(0, recovered.CapturedFiles);
            Assert.Equal(4, f.Remote.Prepares);
            Assert.Equal(4, f.Remote.Completes);
            Assert.Equal(originalInputs, await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"));
            Assert.Equal(4, Directory.GetFiles(f.Source).Length);
            Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches"));
            await f.Evidence("partial-batch-receipt-and-retry", new { checkRetryOwnership, recovered });
            return true;
        });
    }
}
