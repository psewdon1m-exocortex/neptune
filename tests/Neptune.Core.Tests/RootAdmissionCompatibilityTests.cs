using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RootAdmissionConcurrentJobLostReplyRecoversSameOriginals(bool timeout)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock()); BatchFiles(f, 32);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { ExecutionJobs = true, ParallelJobs = 2, SingleRequest = true, LoseAfterSend = timeout ? -1 : 1 };
            var both = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var started = 0;
            remote.BeforeSend = async (_, ct) =>
            {
                var number = Interlocked.Increment(ref started);
                if (number == 2) both.TrySetResult();
                await both.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
                if (number == 1 && timeout) throw new TimeoutException("root admission network timeout");
                if (number == 2) await Task.Delay(250, ct);
            };
            string scope; object? originals;
            await using (var engine = await OpenBatchEngine(f, remote))
            {
                scope = engine.ScopeId; Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
                var interrupted = await engine.InitialSyncAsync(Ct);
                Assert.NotNull(interrupted.Error); Assert.Equal(2, started);
                if (timeout) Assert.Contains("root admission network timeout", interrupted.Error);
                Assert.Equal(32, interrupted.CapturedFiles); Assert.Equal(32, f.Remote.Prepares);
                originals = await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)");
            }
            remote.BeforeSend = null;
            await using var resumed = await OpenBatchEngine(f, remote, resume: true);
            Assert.Equal(scope, resumed.ScopeId);
            var recovered = await f.FreshWhenDueAsync(resumed, Ct);
            Assert.True(recovered.Error is null, recovered.Error); Assert.Equal(0, recovered.CapturedBytes);
            Assert.Equal(32, f.Remote.Prepares); Assert.Equal(2, remote.BatchSends);
            Assert.Equal(originals, await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"));
            Assert.Equal(0L, await f.Scalar("SELECT count(*) FROM local_cycle_batches"));
            Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
            return true;
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FreshBatchAdmissionKeepsCasAndDifferentParentsOutOfOverlappingJobs(bool differentParents)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());
        if (differentParents)
        {
            Directory.CreateDirectory(Path.Combine(f.Source, "a"));
            Directory.CreateDirectory(Path.Combine(f.Source, "b"));
        }
        else BatchFiles(f, 32);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { ExecutionJobs = true, ParallelJobs = 2, SingleRequest = true, LegacyAvailable = true };
            await using var engine = await OpenBatchEngine(f, remote);
            Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
            Assert.Null((await engine.InitialSyncAsync(Ct)).Error);
            for (var n = 0; n < 32; n++)
            {
                var first = differentParents ? Path.Combine("a", $"file-{n}") : $"file-{n}";
                var second = differentParents ? Path.Combine("b", $"file-{n}") : $"new-{n}";
                await File.WriteAllTextAsync(Path.Combine(f.Source, first), $"replacement-a:{n}", Ct);
                await File.WriteAllTextAsync(Path.Combine(f.Source, second), $"replacement-b:{n}", Ct);
            }
            var active = new Dictionary<string, (bool Job, string Parent)>();
            var gate = new object(); var incompatible = 0; var observed = 0;
            remote.BeforeSend = async (plan, ct) =>
            {
                var parent = Assert.Single(plan.Entries.Select(e => e.Original.Intent.ParentId).Distinct());
                lock (gate)
                {
                    if (active.Values.Any(other => !plan.ExecutionJob || !other.Job || other.Parent != parent)) incompatible++;
                    active.Add(plan.BatchId, (plan.ExecutionJob, parent)); observed++;
                }
                try { await Task.Delay(150, ct); }
                finally { lock (gate) active.Remove(plan.BatchId); }
            };
            var changed = await f.FreshWhenDueAsync(engine, Ct);
            Assert.True(changed.Error is null, changed.Error);
            Assert.Equal(64, changed.PublishedFiles);
            Assert.True(observed >= 4, "The fixture must exercise multiple concurrent candidates.");
            Assert.Equal(0, incompatible);
            Assert.Equal(0L, await f.Scalar("SELECT count(*) FROM local_cycle_batches"));
            Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
            return true;
        });
    }
}
