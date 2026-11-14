using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsFreshCycleTests
{
    [Fact]
    public async Task ProgressGuardedRetainedSessionUsesNewSubscriberAndStopsPollingAfterEachRun()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"note.md"),"one");
        var configuration=new WindowsCycleConfiguration("mapping","client","audit","rules-v1",new Uri("https://test.invalid/"),f.Remote.Device,"audit-token","local-test",f.Remote.Binding);
        var engine=await f.Open();
        var transport=new SaturnGuardedCycleTransport(configuration,_=>Task.FromResult("Bearer "+new string('b',43)));
        var lease=Neptune.Windows.WindowsGuardedStateLease.Open(Path.Combine(f.Directory,"lease"),true);
        await using var session=(Neptune.Windows.WindowsGuardedMappingSession)Activator.CreateInstance(typeof(Neptune.Windows.WindowsGuardedMappingSession),System.Reflection.BindingFlags.Instance|System.Reflection.BindingFlags.NonPublic,null,
            [engine,transport,"fixture",new string('b',43),lease,f.Directory],null)!;
        var first=new System.Collections.Concurrent.ConcurrentQueue<Neptune.Windows.WindowsSyncProgress>();
        var second=new System.Collections.Concurrent.ConcurrentQueue<Neptune.Windows.WindowsSyncProgress>();
        var firstSink=new Neptune.Windows.WindowsSyncProgressReporter(null,new Neptune.Windows.SyncProgressCallback<Neptune.Windows.WindowsSyncProgress>(first.Enqueue),"notes",1,1);
        var secondSink=new Neptune.Windows.WindowsSyncProgressReporter(null,new Neptune.Windows.SyncProgressCallback<Neptune.Windows.WindowsSyncProgress>(second.Enqueue),"notes",1,1);
        var initial=await session.SyncAsync("mapping",Ct,firstSink);Assert.Equal("NoWorkAtAppliedEventBoundary",initial.Result.Decision);
        Assert.Equal(SyncProgressStage.Completed,first.Last().Work.Stage);var firstCount=first.Count;Assert.False(engine.ProgressEnabled);
        var repeat=await session.SyncAsync("mapping",Ct,secondSink);Assert.Equal(0,repeat.Result.CapturedFiles);Assert.Equal(SyncProgressStage.Completed,second.Last().Work.Stage);
        Assert.True(second.Last().Work.Revision>first.Last().Work.Revision);Assert.Equal(firstCount,first.Count);Assert.False(engine.ProgressEnabled);
    }
    [Fact]
    public async Task ProgressTracksValidatedStreamAcksThroughPublication()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await Fixture.Create();f.Remote.StreamEnabled=true;
        var bytes=new byte[12*1024*1024+17];new Random(93).NextBytes(bytes);File.WriteAllBytes(Path.Combine(f.Source,"stream.bin"),bytes);
        var configuration=new WindowsCycleConfiguration("mapping","client","audit","rules-v1",new Uri("https://test.invalid/"),f.Remote.Device,"audit-token","local-test",f.Remote.Binding);
        await using var e=await WindowsFreshSyncEngine.ResumeOrOpenAsync(f.Store,f.Source,Path.Combine(f.Directory,"spool"),configuration,f.Remote,new(LargeFileThreshold:1024*1024,SpoolBytes:16777216,MinimumFreeDiskBytes:0),cancellationToken:Ct);
        e.ProgressEnabled=true;var result=await e.InitialSyncAsync(Ct);await f.Evidence("progress-stream",new{result,e.WorkProgress});
        Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(1,result.PublishedFiles);
        Assert.Equal(new[]{0,1,2,3},f.Remote.StreamSent);Assert.Equal(bytes.Length,e.WorkProgress.TransferredBytes);
        Assert.Equal(1,e.WorkProgress.CompletedItems);Assert.Equal(SyncProgressStage.Completed,e.WorkProgress.Stage);
    }
    [Fact]
    public async Task ProgressTracksActualPublicationAndUnchangedCycleDoesNotRereadFiles()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await Fixture.Create();File.WriteAllBytes(Path.Combine(f.Source,"large.dat"),new byte[3*1024*1024]);
        File.WriteAllText(Path.Combine(f.Source,"empty.md"),"");Directory.CreateDirectory(Path.Combine(f.Source,"folder"));
        var observations=new List<SyncWorkProgressSnapshot>();WindowsFreshSyncEngine? engine=null;
        await using var e=await f.Open((o,ct)=>{if(engine is not null)lock(observations)observations.Add(engine.WorkProgress);return Task.CompletedTask;});engine=e;e.ProgressEnabled=true;
        var initial=await e.InitialSyncAsync(Ct);Assert.Null(initial.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",initial.Decision);
        var completed=e.WorkProgress;Assert.Equal(SyncProgressStage.Completed,completed.Stage);Assert.Equal(3,completed.CompletedItems);Assert.Equal(3*1024*1024,completed.TransferredBytes);
        Assert.Contains(observations,o=>o.CompletedItems>0);Assert.Contains(observations,o=>o.Stage==SyncProgressStage.Scanning);
        var next=await e.FreshSyncAsync(Ct);Assert.Null(next.Error);Assert.Equal(0,next.CapturedFiles);Assert.Equal(0,next.CapturedBytes);
        Assert.Equal(SyncProgressStage.Completed,e.WorkProgress.Stage);Assert.Equal(0,e.WorkProgress.CompletedItems);Assert.Equal(0,e.WorkProgress.TransferredBytes);Assert.True(e.WorkProgress.Revision>completed.Revision);
    }
    [Fact]
    public async Task ProgressLeavesBusySourcePendingThenCompletesAfterRetry()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        var clock=new Stage3Clock();await using var f=await Fixture.Create(clock);var path=Path.Combine(f.Source,"busy.md");File.WriteAllText(path,"old");
        var writer=new FileStream(path,FileMode.Open,FileAccess.Write,FileShare.ReadWrite|FileShare.Delete);writer.WriteByte(42);writer.Flush(true);
        await using var e=await f.Open();e.ProgressEnabled=true;
        using(writer)
        {
            var first=await e.InitialSyncAsync(Ct);await f.Evidence("progress-busy",new{first,e.WorkProgress});
            Assert.True(first.Deferred>0,System.Text.Json.JsonSerializer.Serialize(first));Assert.Equal(SyncProgressStage.Waiting,e.WorkProgress.Stage);Assert.Equal(0,e.WorkProgress.CompletedItems);
        }
        // Closing a handle need not raise a FileSystemWatcher path event. The
        // persisted source-busy retry is admitted by its scheduled deadline.
        await Stage3ChatBTests.AdvancePastDue(f,clock);
        await e.RetryDeferredAsync(Ct);var retry=await e.FreshSyncAsync(Ct);await f.Evidence("progress-retry",new{retry,e.WorkProgress});Assert.Null(retry.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",retry.Decision);
        Assert.Equal(SyncProgressStage.Completed,e.WorkProgress.Stage);
        Assert.Equal(Convert.ToInt64(await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE phase IN (5,6)")),e.WorkProgress.CompletedItems);
        Assert.Equal(e.WorkProgress.CompletedItems,e.WorkProgress.TotalItems);Assert.Equal(1,retry.PublishedFiles);Assert.Equal(2,f.Remote.Count);
    }
}
