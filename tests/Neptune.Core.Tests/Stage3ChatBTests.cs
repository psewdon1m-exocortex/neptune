using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

internal sealed class Stage3Clock : TimeProvider
{
    private long _ticks = DateTimeOffset.UtcNow.UtcTicks;
    public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _ticks), TimeSpan.Zero);
    public override long GetTimestamp() => Interlocked.Read(ref _ticks);
    public override long TimestampFrequency => TimeSpan.TicksPerSecond;
    internal void Advance(TimeSpan elapsed) => Interlocked.Add(ref _ticks, elapsed.Ticks);
}

[Collection("Native fresh cycle")]
public sealed partial class Stage3ChatBTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static WindowsCycleConfiguration Configuration(WindowsFreshCycleTests.Fixture f) => new("mapping","client","audit","rules-v1",new Uri("https://test.invalid/"),f.Remote.Device,"audit-token","local-test",f.Remote.Binding);
    private static Task<WindowsFreshSyncEngine> Open(WindowsFreshCycleTests.Fixture f) => WindowsFreshSyncEngine.ResumeOrOpenAsync(f.Store,f.Source,Path.Combine(f.Directory,"spool"),Configuration(f),f.Remote,new(MinimumFreeDiskBytes:0),cancellationToken:Ct);
    private static LocalSourceJournalSession Session(WindowsFreshSyncEngine engine) =>
        (LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(engine)!;
    internal static async Task AwaitHint(WindowsFreshCycleTests.Fixture f)
    {
        var timer = Stopwatch.StartNew();
        while (Convert.ToInt64(await f.Scalar("SELECT COUNT(*) FROM client_event_inbox")) == 0)
        {
            Assert.True(timer.Elapsed < TimeSpan.FromSeconds(10), "Native watcher did not durably deliver its fixture hint.");
            await Task.Delay(10, Ct);
        }
    }
    internal static async Task AdvancePastDue(WindowsFreshCycleTests.Fixture f, Stage3Clock clock)
    {
        var due = Convert.ToInt64(await f.Scalar("SELECT COALESCE(MAX(not_before),0) FROM client_operation_schedule"));
        clock.Advance(TimeSpan.FromMilliseconds(Math.Max(1000, due - clock.GetUtcNow().ToUnixTimeMilliseconds() + 1)));
    }
    private static void RequireWindows() => Assert.SkipUnless(OperatingSystem.IsWindows(), "Requires Windows/NTFS; Linux skip is not native qualification");

    [Fact]
    public async Task B02_BurstQuietMaxAgeGenerationAndCrashPersistence()
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);
        await using(var engine=await Open(f))
        {
            var s=Session(engine);var now=clock.GetUtcNow().ToUnixTimeMilliseconds();
            for(var i=0;i<100;i++)await s.SaveEventHintsAsync(["hint"],false,Ct);
            var pending=await s.ReadEventHintsAsync(Ct);Assert.Equal(100,Assert.Single(pending.Hints).Generation);Assert.Equal(now+750,pending.NextDue);
            Assert.Empty((await s.ReadEventHintsAsync(Ct,true)).Hints);clock.Advance(TimeSpan.FromMilliseconds(749));Assert.Empty((await s.ReadEventHintsAsync(Ct,true)).Hints);
            clock.Advance(TimeSpan.FromMilliseconds(1));var g=Assert.Single((await s.ReadEventHintsAsync(Ct,true)).Hints);
            await s.SaveEventHintsAsync(["hint"],false,Ct);await s.AckEventHintsAsync([g],Ct);
            Assert.Equal(101,Assert.Single((await s.ReadEventHintsAsync(Ct)).Hints).Generation);
            for(var i=0;i<10;i++){clock.Advance(TimeSpan.FromMilliseconds(500));await s.SaveEventHintsAsync(["hint"],false,Ct);}
            Assert.Equal(now+5000,(await s.ReadEventHintsAsync(Ct)).NextDue);Assert.Single((await s.ReadEventHintsAsync(Ct,true)).Hints);
        }
        f.Store=new(f.Database,clock);await using var reopened=await f.Open();Assert.Equal(111,Assert.Single((await Session(reopened).ReadEventHintsAsync(Ct)).Hints).Generation);
    }

    [Fact]
    public async Task B02_CapacityOverflowAndGapGenerationCas()
    {
        RequireWindows();await using var f=await WindowsFreshCycleTests.Fixture.Create();await using var engine=await f.Open();var s=Session(engine);
        for(var i=0;i<32;i++)await s.SaveEventHintsAsync(Enumerable.Range(i*128,128).Select(n=>"hint-"+n).ToArray(),false,Ct);
        Assert.Equal(4096L,await f.Scalar("SELECT COUNT(*) FROM client_event_inbox"));Assert.False((await s.ReadEventHintsAsync(Ct)).Gap);
        await s.SaveEventHintsAsync(["overflow"],false,Ct);var gap=(await s.ReadEventHintsAsync(Ct)).GapGeneration;Assert.True(gap>0);
        await s.SaveEventHintsAsync([],true,Ct);await s.AckEventGapAsync(gap,Ct);Assert.Equal(gap+1,(await s.ReadEventHintsAsync(Ct)).GapGeneration);
        Assert.Equal(4096L,await f.Scalar("SELECT COUNT(*) FROM client_event_inbox"));await s.AckEventGapAsync(gap+1,Ct);Assert.False((await s.ReadEventHintsAsync(Ct)).Gap);
    }

    [Fact]
    public async Task B11_DurableDeadlineRestartAndEighthFailureDegraded()
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);string scope;
        await using(var engine=await f.Open())
        {
            Assert.Null((await engine.InitialSyncAsync(Ct)).Error);scope=engine.ScopeId;var s=Session(engine);
            Assert.True(await s.ScheduleReadyAsync(scope,"cycle","cycle",Ct));
            await s.SaveOperationWaitAsync(scope,"cycle",new("RETRY_WAIT","fixture",TimeSpan.FromSeconds(30)),Ct);
            Assert.False(await s.ScheduleReadyAsync(scope,"cycle","cycle",Ct));
            Assert.Equal(clock.GetUtcNow().ToUnixTimeMilliseconds()+30000,(await s.ReadOperationScheduleAsync(scope,Ct)).NextAttemptUnixMilliseconds);
        }
        f.Store=new(f.Database,clock);await using var resumed=await Open(f);Assert.Equal(scope,resumed.ScopeId);var session=Session(resumed);
        Assert.False(await session.ScheduleReadyAsync(scope,"cycle","cycle",Ct));clock.Advance(TimeSpan.FromSeconds(30));Assert.True(await session.ScheduleReadyAsync(scope,"cycle","cycle",Ct));
        for(var n=1;n<8;n++){await session.SaveOperationWaitAsync(scope,"cycle",new("RETRY_WAIT","fixture"),Ct);await AdvancePastDue(f,clock);}
        var state=await session.ReadOperationScheduleAsync(scope,Ct);Assert.Equal("DEGRADED",state.State);
        Assert.Equal(8L,await f.Scalar("SELECT attempts FROM client_operation_schedule WHERE operation_id='cycle'"));
        Assert.True(await session.ScheduleReadyAsync(scope,"cycle","cycle",Ct));await session.ClearOperationWaitAsync(scope,"cycle",Ct);Assert.Equal(0,(await session.ReadOperationScheduleAsync(scope,Ct)).Waiting);
    }
}
