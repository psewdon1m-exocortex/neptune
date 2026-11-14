using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MixedFreshWorkKeepsNewFilesOnExecutionJobs(bool overwrite)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());
        if (overwrite) BatchFiles(f, 8);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { ExecutionJobs = true, ParallelJobs = 2, SingleRequest = true };
            await using var engine = await OpenBatchEngine(f, remote);
            Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
            Assert.Null((await engine.InitialSyncAsync(Ct)).Error);
            remote.BatchSizes.Clear(); remote.IndividualPrepares = 0;
            for (var i = 0; i < 8; i++)
            {
                await File.WriteAllTextAsync(Path.Combine(f.Source, $"file-{i}"), overwrite ? $"changed:{i}" : "", Ct);
                await File.WriteAllTextAsync(Path.Combine(f.Source, $"new-{i}"), $"new:{i}", Ct);
            }
            remote.BeforeSend = (plan, _) =>
            {
                Assert.All(plan.Entries, entry =>
                {
                    Assert.StartsWith("new-", entry.Original.Intent.Name);
                    Assert.True(entry.Original.Intent.RequireAbsent);
                    Assert.Null(entry.Original.Intent.TargetId);
                    Assert.Null(entry.Original.Intent.ExpectedVersionId);
                });
                return Task.CompletedTask;
            };
            var changed = await f.FreshWhenDueAsync(engine, Ct);
            Assert.True(changed.Error is null, changed.Error);
            Assert.Equal(16, changed.PublishedFiles);
            Assert.Equal(8, remote.BatchSizes.Sum());
            Assert.Equal(8, remote.IndividualPrepares);
            Assert.Equal(0L, await f.Scalar("SELECT count(*) FROM local_cycle_batches"));
            Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
            var unchanged = await f.FreshWhenDueAsync(engine, Ct);
            Assert.Null(unchanged.Error); Assert.Equal(0, unchanged.CapturedBytes); Assert.Equal(0, unchanged.PublishedFiles);
            return true;
        });
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task ExecutionJobsRetainAccelerationAcrossIndependentParents(int loseAfterJob)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());
        Directory.CreateDirectory(Path.Combine(f.Source, "a")); Directory.CreateDirectory(Path.Combine(f.Source, "b"));
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { ExecutionJobs = true, ParallelJobs = 2, SingleRequest = true };
            object? originals; string scope;
            await using (var engine = await OpenBatchEngine(f, remote))
            {
                scope = engine.ScopeId;
                Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
                var initial = await engine.InitialSyncAsync(Ct); Assert.Null(initial.Error);
                foreach (var parent in new[] { "a", "b" })
                    for (var i = 0; i < 8; i++)
                        await File.WriteAllTextAsync(Path.Combine(f.Source, parent, $"file-{i}"), $"{parent}:{i}:new content", Ct);
                remote.LoseAfterSend = loseAfterJob;
                remote.BeforeSend = (plan, _) =>
                {
                    Assert.True(plan.ExecutionJob);
                    Assert.Single(plan.Entries.Select(e => e.Original.Intent.ParentId).Distinct());
                    return Task.CompletedTask;
                };
                var changed = await f.FreshWhenDueAsync(engine, Ct);
                if (loseAfterJob > 0) Assert.NotNull(changed.Error); else Assert.True(changed.Error is null, changed.Error);
                Assert.Equal(16, changed.CapturedFiles);
                Assert.Equal(0, remote.IndividualPrepares);
                Assert.NotEmpty(remote.BatchSizes);
                Assert.All(remote.BatchSizes, size => Assert.InRange(size, 1, 8));
                Assert.InRange(engine.ResourceMetrics.MaximumDataJobs, 1, 4);
                originals = await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work WHERE input_json IS NOT NULL ORDER BY work_no)");
            }
            remote.BeforeSend = null;
            await using var resumed = await OpenBatchEngine(f, remote, resume: true);
            Assert.Equal(scope, resumed.ScopeId);
            var recovered = await f.FreshWhenDueAsync(resumed, Ct); Assert.True(recovered.Error is null, recovered.Error);
            Assert.Equal(0, recovered.CapturedBytes);
            Assert.Equal(16, f.Remote.Prepares);
            // Fresh directory observation may enqueue new work without an
            // original. Every already sealed operation must remain identical.
            Assert.Equal(originals, await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work WHERE input_json IS NOT NULL ORDER BY work_no)"));
            Assert.Equal(0L, await f.Scalar("SELECT count(*) FROM local_cycle_batches"));
            Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
            Assert.Equal(16, Directory.GetFiles(f.Source, "*", SearchOption.AllDirectories).Length);
            return true;
        });
    }
}
