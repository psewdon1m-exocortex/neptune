using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    [Theory]
    [InlineData("missing")]
    [InlineData("receipt")]
    [InlineData("scope")]
    [InlineData("protocol")]
    public async Task LegacyFallbackRequiresItsOwnCurrentQualifiedCapability(string failure)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());
        BatchFiles(f, 8);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { ExecutionJobs = true, ParallelJobs = 2, SingleRequest = true, LegacyAvailable = failure != "missing" };
            remote.LegacyCapabilityRewrite = cap => failure switch
            {
                "receipt" => cap with { QualificationReceiptSha256 = new string('b', 64) },
                "scope" => cap with { BackendIdentity = "another-backend" },
                "protocol" => cap with { ExecutionJob = true },
                _ => cap
            };
            await using var engine = await OpenBatchEngine(f, remote);
            Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
            Assert.Null((await engine.InitialSyncAsync(Ct)).Error);
            remote.IndividualPrepares = 0; remote.BatchSends = 0;
            for (var i = 0; i < 8; i++) await File.WriteAllTextAsync(Path.Combine(f.Source, $"file-{i}"), $"replacement:{i}", Ct);
            var changed = await f.FreshWhenDueAsync(engine, Ct);
            Assert.Equal(1, remote.LegacyCapabilityReads);
            Assert.Equal(0, remote.BatchSends);
            if (failure is "scope" or "protocol")
            {
                Assert.NotNull(changed.Error); Assert.Equal(0, remote.IndividualPrepares);
            }
            else
            {
                Assert.True(changed.Error is null, changed.Error); Assert.Equal(8, remote.IndividualPrepares);
                for (var i = 0; i < 8; i++) await File.WriteAllTextAsync(Path.Combine(f.Source, $"file-{i}"), $"next:{i}", Ct);
                Assert.Null((await f.FreshWhenDueAsync(engine, Ct)).Error);
                Assert.Equal(2, remote.LegacyCapabilityReads);
            }
            return true;
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FreshCasUsesSeparatelyAdvertisedBatchAndRecoversItsOriginalProtocol(bool loseCasReply)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());
        BatchFiles(f, 8);
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote) { ExecutionJobs = true, ParallelJobs = 2, SingleRequest = true, LegacyAvailable = true };
            object? originals; string scope;
            await using (var engine = await OpenBatchEngine(f, remote))
            {
                scope = engine.ScopeId;
                Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
                Assert.Null((await engine.InitialSyncAsync(Ct)).Error);
                remote.BatchSizes.Clear(); remote.IndividualPrepares = 0;
                for (var i = 0; i < 8; i++)
                {
                    await File.WriteAllTextAsync(Path.Combine(f.Source, $"file-{i}"), $"replacement:{i}", Ct);
                    await File.WriteAllTextAsync(Path.Combine(f.Source, $"new-{i}"), $"new:{i}", Ct);
                }
                var sawCas = false; var injected = false;
                remote.BeforeSend = (plan, _) =>
                {
                    if (plan.ExecutionJob) Assert.All(plan.Entries, e => Assert.True(e.Original.Intent.RequireAbsent));
                    else
                    {
                        sawCas = true;
                        Assert.All(plan.Entries, e => { Assert.False(e.Original.Intent.RequireAbsent); Assert.NotNull(e.Original.Intent.TargetId); Assert.NotNull(e.Original.Intent.ExpectedVersionId); });
                        if (loseCasReply && !injected) { injected = true; remote.LoseData = true; }
                    }
                    return Task.CompletedTask;
                };
                var changed = await f.FreshWhenDueAsync(engine, Ct);
                Assert.True(sawCas, "Existing-file originals must use the separately advertised legacy batch.");
                if (loseCasReply) Assert.NotNull(changed.Error); else Assert.True(changed.Error is null, changed.Error);
                Assert.Equal(0, remote.IndividualPrepares);
                originals = await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work WHERE input_json IS NOT NULL ORDER BY work_no)");
            }
            remote.BeforeSend = null; remote.LegacyAvailable = false;
            await using var resumed = await OpenBatchEngine(f, remote, resume: true);
            Assert.Equal(scope, resumed.ScopeId);
            var recovered = await f.FreshWhenDueAsync(resumed, Ct); Assert.True(recovered.Error is null, recovered.Error);
            Assert.Equal(0, recovered.CapturedBytes);
            Assert.Equal(originals, await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work WHERE input_json IS NOT NULL ORDER BY work_no)"));
            Assert.Equal(0L, await f.Scalar("SELECT count(*) FROM local_cycle_batches"));
            Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
            if (loseCasReply) Assert.True(remote.BatchStatusReads > 0);
            return true;
        });
    }
}
