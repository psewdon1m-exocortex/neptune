using System.Net;
using System.Reflection;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsFreshCycleTests
{
    private static LocalSourceJournalSession RetrySession(WindowsFreshSyncEngine engine) =>
        (LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(engine)!;

    [Theory]
    [InlineData(429, 3)]
    [InlineData(503, 180)]
    [InlineData(407, 0)]
    public async Task NativeOperationRetryDoesNotInheritOldCycleFailures(int status, int retrySeconds)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Requires Windows native filesystem fixture");
        var clock = new Stage3Clock(); await using var f = await Fixture.Create(clock);
        File.WriteAllText(Path.Combine(f.Source, "one"), "first original");
        File.WriteAllText(Path.Combine(f.Source, "two"), "second original");
        var remote = new AdmissionRemote(f.Remote) { RejectAfterPublications = 1, RejectsRemaining = 1,
            RejectionStatus = (HttpStatusCode)status, RetryAfter = TimeSpan.FromSeconds(retrySeconds) };
        await using var engine = await WindowsFreshSyncEngine.OpenAsync(f.Store, f.Source, Path.Combine(f.Directory, "spool"),
            AnchoredConfiguration(f), remote, new(DataJobs: 1, LargeJobs: 1, MinimumFreeDiskBytes: 0), cancellationToken: Ct);
        var session = RetrySession(engine);
        Assert.True(await session.ScheduleReadyAsync(engine.ScopeId, "cycle", "cycle", Ct));
        for (var i = 0; i < 9; i++) await session.SaveOperationWaitAsync(engine.ScopeId, "cycle", new("RETRY_WAIT", "old_failure"), Ct);
        await Stage3ChatBTests.AdvancePastDue(f, clock);
        var first = await engine.InitialSyncAsync(Ct);
        Assert.Equal(1, first.PublishedFiles); Assert.NotNull(first.Error);
        var original = (string)(await f.Scalar("SELECT input_json FROM local_cycle_work WHERE phase=4"))!;
        var due = Convert.ToInt64(await f.Scalar("SELECT not_before FROM client_operation_schedule WHERE kind='work'"));
        Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM client_operation_schedule WHERE operation_id='cycle'"));
        Assert.Equal(1L, await f.Scalar("SELECT attempts FROM client_operation_schedule WHERE kind='work'"));
        Assert.DoesNotContain("CycleSpoolAdmissionException", first.Error!);
        if (status == 407)
        {
            Assert.Equal("AUTH_REQUIRED", first.RetryState); Assert.Equal(long.MaxValue, due);
            clock.Advance(TimeSpan.FromHours(1));
            await engine.FreshSyncAsync(Ct);
            Assert.Equal(2, remote.Attempts); // No periodic replay without renewed authorization.
        }
        else
        {
            Assert.Equal(status == 429 ? "RETRY_WAIT" : "WAIT_CONNECTIVITY", first.RetryState);
            Assert.Equal(clock.GetUtcNow().ToUnixTimeMilliseconds() + retrySeconds * 1000, due);
            await engine.FreshSyncAsync(Ct); Assert.Equal(2, remote.Attempts);
            await Stage3ChatBTests.AdvancePastDue(f, clock);
            var recovered = await f.FreshWhenDueAsync(engine, Ct);
            Assert.Null(recovered.Error); Assert.Equal("NoWorkAtAppliedEventBoundary", recovered.Decision);
            Assert.Equal(0, recovered.CapturedFiles); Assert.Equal(2, f.Remote.Prepares); Assert.Equal(2, f.Remote.Completes);
            Assert.Equal(original, await f.Scalar("SELECT input_json FROM local_cycle_work WHERE input_json IS NOT NULL ORDER BY work_no DESC LIMIT 1"));
            Assert.Equal(remote.Inputs[1], remote.Inputs[2]);
            Assert.Equal(2, Directory.GetFiles(f.Source).Length);
            Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
        }
        await f.Evidence("retry-owner-" + status, new { first, due, remote.Attempts, remote.Inputs });
    }

    [Fact]
    public async Task NativePersistentOperationFailureKeepsItsOwnDegradedBackoff()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Requires Windows native filesystem fixture");
        var clock = new Stage3Clock(); await using var f = await Fixture.Create(clock);
        File.WriteAllText(Path.Combine(f.Source, "file"), "retained original");
        var remote = new AdmissionRemote(f.Remote) { RejectsRemaining = int.MaxValue };
        await using var engine = await OpenBackpressure(f, remote);
        var result = await engine.InitialSyncAsync(Ct);
        for (var i = 1; i < 8; i++)
        {
            await Stage3ChatBTests.AdvancePastDue(f, clock);
            result = await engine.FreshSyncAsync(Ct);
            Assert.Equal(0, result.CapturedFiles);
        }
        Assert.Equal("DEGRADED", result.RetryState); Assert.Equal(8, remote.Attempts);
        Assert.Equal(8L, await f.Scalar("SELECT attempts FROM client_operation_schedule WHERE kind='work'"));
        var due = Convert.ToInt64(await f.Scalar("SELECT not_before FROM client_operation_schedule WHERE kind='work'"));
        Assert.InRange(due - clock.GetUtcNow().ToUnixTimeMilliseconds(), 300000, 360000);
        Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM client_operation_schedule WHERE operation_id='cycle'"));
        Assert.Single(remote.Inputs.Distinct()); Assert.Equal(0, f.Remote.Prepares);
        Assert.Single(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
    }
}
