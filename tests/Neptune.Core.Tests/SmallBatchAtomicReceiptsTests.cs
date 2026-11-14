using System.Reflection;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    [Fact]
    public async Task BatchReceiptsRollbackLateInvalidOriginalAndCommitValidSetOnce()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(); BatchFiles(f, 16);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { ExecutionJobs = true };
            await using var engine = await OpenBatchEngine(f, remote, new(MinimumFreeDiskBytes: 0));
            var session = (LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(engine)!;
            var save = typeof(WindowsFreshSyncEngine).GetMethod("SaveBatchOutcomes", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var exercised = false;
            remote.BeforeSend = async (plan, ct) =>
            {
                var row = await session.ReadSmallBatchAsync(engine.ScopeId, plan.BatchId, ct);
                var items = new List<SmallFileBatchItemOutcome>();
                foreach (var entry in plan.Entries)
                {
                    var status = await f.Remote.UploadAsync("prepare", entry.Original, null, LocalCycleWire.IntentHash(entry.Original), ct);
                    items.Add(new(entry.Original.Intent.IdempotencyKey, entry.SourceGeneration, entry.SourceIdentity, "PENDING", status, null));
                }
                var valid = new SmallFileBatchOutcome(plan.BatchId, items.ToArray());
                var bad = items.ToArray(); bad[^1] = bad[^1] with { Status = bad[^1].Status! with { ReceivedBytes = -1 } };
                var before = f.Store.GetLocalCycleMetrics().Transactions;
                await Assert.ThrowsAnyAsync<InvalidDataException>(() => (Task)save.Invoke(engine, [row, new SmallFileBatchOutcome(plan.BatchId, bad), ct])!);
                Assert.Equal(0L, await f.Scalar("SELECT count(*) FROM local_cycle_work WHERE upload_id IS NOT NULL"));
                Assert.Equal(before, f.Store.GetLocalCycleMetrics().Transactions);
                await (Task)save.Invoke(engine, [row, valid, ct])!;
                Assert.Equal(16L, await f.Scalar("SELECT count(*) FROM local_cycle_work WHERE upload_id IS NOT NULL"));
                Assert.Equal(before + 1, f.Store.GetLocalCycleMetrics().Transactions);
                exercised = true;
            };
            Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
            var result = await engine.InitialSyncAsync(Ct);
            Assert.True(exercised, result.Error); Assert.Null(result.Error); Assert.Equal(16, result.PublishedFiles);
            return true;
        });
    }
}
