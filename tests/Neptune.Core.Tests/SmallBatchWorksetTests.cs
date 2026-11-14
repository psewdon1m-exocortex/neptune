using Neptune.Core;
using System.Reflection;
using System.Text.Json;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    [Fact]
    public async Task CombinedSealRollsBackBytesWhenOriginalIntentBindingIsInvalid()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock()); BatchFiles(f, 2);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { ExecutionJobs = true, ParallelJobs = 2 };
            await using var engine = await OpenBatchEngine(f, remote);
            Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
            Assert.Null((await engine.InitialSyncAsync(Ct)).Error);
            var session = Session(engine);
            var operation = (string)(await f.Scalar("SELECT operation_id FROM local_cycle_work ORDER BY work_no LIMIT 1"))!;
            var work = await session.ReadCycleWorkAsync(engine.ScopeId, operation, Ct);
            var input = JsonSerializer.Deserialize<CycleUploadInput>(work.InputJson!, LocalCycleWire.Json)!;
            var capture = (WindowsNativeCycleCapture)typeof(WindowsFreshSyncEngine).GetField("_capture", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(engine)!;
            // Rewind only the owned test ledger to the pre-seal boundary. No
            // network action is allowed after this deliberate fixture mutation.
            await f.Scalar("UPDATE local_cycle_work SET phase=0,active=1,spool_path=NULL,size=NULL,sha256=NULL,capture_proof=NULL,input_json=NULL,intent_hash=NULL,upload_id=NULL,result_json=NULL,received=0 WHERE operation_id='" + operation + "'");
            var before = await f.Scalar("SELECT bytes FROM local_cycle_format");
            var commits = f.Store.GetLocalCycleMetrics().Transactions;
            var seal = new LocalCycleSeal(work.SpoolPath!, work.Size!.Value, work.Sha256!, work.CaptureProof!);
            await Assert.ThrowsAsync<InvalidDataException>(() => session.TrySealCycleInputAsync(engine.ScopeId, operation, seal, capture.RootProof, input with { BackendIdentity = "different-backend" }, Ct));
            var unchanged = await session.ReadCycleWorkAsync(engine.ScopeId, operation, Ct);
            Assert.Equal(LocalCycleWorkPhase.Queued, unchanged.Phase); Assert.Null(unchanged.CaptureProof); Assert.Null(unchanged.InputJson);
            Assert.Equal(before, await f.Scalar("SELECT bytes FROM local_cycle_format"));
            Assert.Equal(commits, f.Store.GetLocalCycleMetrics().Transactions);
            return true;
        });
    }
    [Fact]
    public async Task TwoNetworkJobsLeaveOneBoundedJobOfPreparedOriginalsAhead()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock()); BatchFiles(f, 48);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { ExecutionJobs = true, ParallelJobs = 2 };
            var sends = 0; var preparedAhead = false; var sendsAtReady = 0;
            var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            remote.BeforeSend = async (_, ct) =>
            {
                if (Interlocked.Increment(ref sends) == 1)
                {
                    var until = DateTime.UtcNow.AddSeconds(4);
                    while (DateTime.UtcNow < until)
                    {
                        if (Convert.ToInt64(await f.Scalar("SELECT count(*) FROM local_cycle_work WHERE input_json IS NOT NULL")) == 48)
                        { preparedAhead = true; sendsAtReady = Volatile.Read(ref sends); break; }
                        await Task.Delay(10, ct);
                    }
                    ready.TrySetResult();
                }
                await ready.Task.WaitAsync(ct);
            };
            await using var engine = await OpenBatchEngine(f, remote);
            Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
            var result = await engine.InitialSyncAsync(Ct); Assert.Null(result.Error); Assert.Equal(48, result.PublishedFiles);
            Assert.True(preparedAhead, "All preparation workers waited on two network jobs; no next originals were prepared.");
            Assert.InRange(sendsAtReady, 1, 2);
            Assert.InRange(engine.ResourceMetrics.MaximumDataJobs, 1, 4);
            Assert.InRange(engine.ResourceMetrics.MaximumSourceReaders, 1, 1);
            Assert.InRange(engine.ResourceMetrics.MaximumReadyQueue, 0, 32);
            return true;
        });
    }
    [Fact]
    public async Task BoundedAcknowledgementRollsBackEarlierMembersWhenLastReceiptIsInvalid()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock()); BatchFiles(f, 16);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { ExecutionJobs = true, ParallelJobs = 2 };
            await using var engine = await OpenBatchEngine(f, remote);
            Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
            var first = await engine.InitialSyncAsync(Ct); Assert.Null(first.Error); Assert.Equal(16, first.PublishedFiles);
            var operations = JsonSerializer.Deserialize<string[]>((string)(await f.Scalar("SELECT json_group_array(operation_id) FROM(SELECT operation_id FROM local_cycle_work ORDER BY work_no)"))!)!;
            var original = (string)(await f.Scalar("SELECT result_json FROM local_cycle_work ORDER BY work_no DESC LIMIT 1"))!;
            // Deliberately corrupt the last retained outcome in this owned fixture.
            await f.Scalar("UPDATE local_cycle_work SET active=1; UPDATE local_cycle_work SET result_json='{}' WHERE work_no=(SELECT MAX(work_no) FROM local_cycle_work)");
            var session = (LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(engine)!;
            var capture = (WindowsNativeCycleCapture)typeof(WindowsFreshSyncEngine).GetField("_capture", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(engine)!;
            var commits = f.Store.GetLocalCycleMetrics().Transactions;
            await Assert.ThrowsAnyAsync<Exception>(() => session.AcknowledgeCycleWorksetAsync(engine.ScopeId, operations, capture.RootProof, Ct));
            Assert.Equal(16L, await f.Scalar("SELECT count(*) FROM local_cycle_work WHERE active=1"));
            Assert.Equal(commits, f.Store.GetLocalCycleMetrics().Transactions);
            await f.Scalar("UPDATE local_cycle_work SET result_json='" + original.Replace("'", "''") + "' WHERE work_no=(SELECT MAX(work_no) FROM local_cycle_work)");
            Assert.All(await session.AcknowledgeCycleWorksetAsync(engine.ScopeId, operations, capture.RootProof, Ct), Assert.True);
            Assert.Equal(commits + 1, f.Store.GetLocalCycleMetrics().Transactions);
            return true;
        });
    }
    [Fact]
    public async Task BoundedDispatchAvoidsPerFileClaimRoundtripsAndRetainsOriginalsAcrossReplay()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock()); BatchFiles(f, 32);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { ExecutionJobs = true, ParallelJobs = 2 };
            await using var engine = await OpenBatchEngine(f, remote);
            Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
            var first = await engine.InitialSyncAsync(Ct); Assert.Null(first.Error); Assert.Equal(32, first.PublishedFiles);
            var calls = engine.Diagnostics.LocalHelpers.Where(h => h.Operation is "DispatchCycleWorkAsync" or "ClaimCycleWorkAsync" or "DispatchCycleWorksetAsync").Sum(h => h.Calls);
            Assert.InRange(calls, 1, 24);
            Assert.Equal(0, engine.Diagnostics.LocalHelpers.Where(h => h.Operation is "SealCycleWorkAsync" or "SaveCycleInputAsync").Sum(h => h.Calls));
            var acknowledgements = engine.Diagnostics.LocalHelpers.Where(h => h.Operation is "AcknowledgeCycleAsync" or "AcknowledgeCycleWorksetAsync").Sum(h => h.Calls);
            Assert.InRange(acknowledgements, 1, 4);
            Assert.Equal(0L, await f.Scalar("SELECT count(*) FROM client_operation_schedule WHERE kind='work'"));
            var originals = await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)");
            var replay = await f.FreshWhenDueAsync(engine, Ct); Assert.Null(replay.Error); Assert.Equal(0, replay.CapturedFiles);
            Assert.Equal(32, f.Remote.Completes);
            Assert.Equal(originals, await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"));
            return true;
        });
    }
}
