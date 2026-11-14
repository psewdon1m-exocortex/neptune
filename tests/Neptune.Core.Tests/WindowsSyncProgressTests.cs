using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class WindowsSyncProgressTests
{
    private sealed class Clock:TimeProvider
    {
        private long _ticks;
        public override long TimestampFrequency=>TimeSpan.TicksPerSecond;
        public override long GetTimestamp()=>_ticks;
        internal void Advance(double seconds)=>_ticks+=TimeSpan.FromSeconds(seconds).Ticks;
    }
    private static SyncWorkProgressSnapshot Work(double units=0,long total=100,long revision=1)
        =>new(SyncProgressStage.Syncing,(long)units,total,units,Revision:revision);

    [Fact]
    public void AcknowledgedBytesAdvanceSingleLargeFileButCannotClaimPublication()
    {
        var tracker=new SyncWorkProgressTracker{Enabled=true};tracker.Begin();tracker.Plan(1);tracker.Start("large","note.md");
        tracker.TransferStart("large",1000);tracker.Acknowledge("large",500);
        var snapshot=tracker.Snapshot();Assert.Equal(.475,snapshot.CompletedUnits,8);Assert.Equal(500,snapshot.TransferredBytes);Assert.Equal(0,snapshot.CompletedItems);
        tracker.Acknowledge("large",1000);Assert.Equal(.95,tracker.Snapshot().CompletedUnits,8);
        tracker.Complete("large");tracker.Complete("large");Assert.Equal(1,tracker.Snapshot().CompletedItems);
        Assert.Equal(99,new SyncProgressEstimator().Update(tracker.Snapshot()).Percent);
        tracker.Stage(SyncProgressStage.Verifying);Assert.Null(new SyncProgressEstimator().Update(tracker.Snapshot()).Percent);
        tracker.Stage(SyncProgressStage.Completed);Assert.Equal(100,new SyncProgressEstimator().Update(tracker.Snapshot()).Percent);
    }
    [Fact]
    public void ResumeAndDuplicateAcksDoNotInflateTransferredBytesAndDeferredItemsStayPending()
    {
        var t=new SyncWorkProgressTracker{Enabled=true};t.Begin();t.Plan(2);t.Start("a","a");t.TransferStart("a",1000,400);
        t.Acknowledge("a",400);t.Acknowledge("a",200);Assert.Equal(0,t.Snapshot().TransferredBytes);
        t.Acknowledge("a",600);t.Acknowledge("a",600);Assert.Equal(200,t.Snapshot().TransferredBytes);
        t.Release("a");Assert.Equal(0,t.Snapshot().CompletedUnits);Assert.Equal(2,t.Snapshot().TotalItems);
        t.Start("zero","zero");t.TransferStart("zero",0);Assert.Equal(0,t.Snapshot().CompletedUnits);
        t.Complete("zero");Assert.Equal(1,t.Snapshot().CompletedItems);Assert.Null(t.Snapshot().CurrentPath);
    }
    [Fact]
    public void ParallelCompletionsAreExactAndNewCycleDiscardsOldProgress()
    {
        var t=new SyncWorkProgressTracker{Enabled=true};t.Begin();t.Plan(400);
        Parallel.For(0,400,i=>{var key=i.ToString();t.Start(key,key);t.TransferStart(key,100);t.Acknowledge(key,100);t.Complete(key);t.Release(key);});
        var previous=t.Snapshot();Assert.Equal(400,previous.CompletedItems);Assert.Equal(40000,previous.TransferredBytes);
        t.Begin();var next=t.Snapshot();Assert.True(next.Revision>previous.Revision);Assert.Null(next.TotalItems);Assert.Equal(0,next.CompletedItems);Assert.Equal(0,next.TransferredBytes);
    }
    [Fact]
    public void DisabledTrackerDoesNotAddWork()
    {
        var t=new SyncWorkProgressTracker();t.Begin();t.Plan(1);t.Start("a","a");t.Complete("a");
        Assert.Null(t.Snapshot().TotalItems);Assert.Equal(0,t.Snapshot().CompletedUnits);
    }
    [Fact]
    public void EtaRequiresSamplesAndUsesObservedRate()
    {
        var clock=new Clock();var eta=new SyncProgressEstimator(clock);Assert.Null(eta.Update(Work()).Remaining);
        for(var i=1;i<=2;i++){clock.Advance(1);Assert.Null(eta.Update(Work(i*10)).Remaining);}
        clock.Advance(1);var value=eta.Update(Work(30));Assert.Equal(30,value.Percent);Assert.Equal(TimeSpan.FromSeconds(7),value.Remaining);
    }
    [Fact]
    public void StalledEtaDisappearsAndRequiresNewSamplesAfterRecovery()
    {
        var clock=new Clock();var eta=new SyncProgressEstimator(clock);eta.Update(Work());
        for(var i=1;i<=3;i++){clock.Advance(1);eta.Update(Work(i*10));}
        clock.Advance(11);Assert.Null(eta.Update(Work(30)).Remaining);
        clock.Advance(1);Assert.Null(eta.Update(Work(40)).Remaining);
        clock.Advance(1);Assert.Null(eta.Update(Work(50)).Remaining);
        clock.Advance(1);Assert.NotNull(eta.Update(Work(60)).Remaining);
    }
    [Theory]
    [InlineData(SyncProgressStage.Preparing)]
    [InlineData(SyncProgressStage.Scanning)]
    [InlineData(SyncProgressStage.Verifying)]
    [InlineData(SyncProgressStage.Waiting)]
    [InlineData(SyncProgressStage.Failed)]
    [InlineData(SyncProgressStage.Cancelled)]
    public void NonTransferStagesNeverShowNumericEtaOrOneHundredPercent(SyncProgressStage stage)
    {
        var eta=new SyncProgressEstimator();var result=eta.Update(Work(100) with{Stage=stage});Assert.Null(result.Percent);Assert.Null(result.Remaining);
    }
    [Fact]
    public void ChangedPlanAndRegressedRetryRestartEtaWarmup()
    {
        var clock=new Clock();var eta=new SyncProgressEstimator(clock);eta.Update(Work());
        for(var i=1;i<=3;i++){clock.Advance(1);eta.Update(Work(i*10));}
        Assert.Null(eta.Update(Work(30,200,2)).Remaining);
        clock.Advance(1);Assert.Null(eta.Update(Work(10,200,2)).Remaining);
        Assert.Null(eta.Update(Work(0,0)).Percent);
    }
    [Fact]
    public void ElapsedIncludesWaitingFreezesAtEndAndResetsForNewRun()
    {
        var clock=new Clock();var display=new WindowsSyncProgressDisplay(clock);display.Begin();clock.Advance(125);
        var value=display.Update(new("vault",1,2,Work()));Assert.Equal("Elapsed 02:05",value.Elapsed);
        clock.Advance(10);value=display.Update(new("vault",1,2,new(SyncProgressStage.Waiting)));
        Assert.Equal("Elapsed 02:15",value.Elapsed);Assert.Equal("ETA ∞",value.Eta);Assert.False(value.Indeterminate);Assert.Empty(value.PercentText);
        display.Stop();clock.Advance(60);Assert.Equal("Elapsed 02:15",display.Update(new(null,0,0,new(SyncProgressStage.Cancelled))).Elapsed);
        Assert.Empty(display.Update(new(null,0,0,new(SyncProgressStage.Cancelled))).Eta);
        display.Begin();Assert.Equal("Elapsed 00:00",display.Update(new(null,0,0,new(SyncProgressStage.Preparing))).Elapsed);
        clock.Advance(3661);Assert.Equal("Elapsed 1:01:01",display.Update(new(null,0,0,Work())).Elapsed);
    }
    [Fact]
    public void MappingChangeAndNetworkRecoveryNeverReuseOldEta()
    {
        var clock=new Clock();var display=new WindowsSyncProgressDisplay(clock);display.Begin();display.Update(new("first",1,2,Work()));
        SyncProgressDisplay? result=null;
        for(var i=1;i<=3;i++){clock.Advance(1);result=display.Update(new("first",1,2,Work(i*10)));}
        Assert.Contains("≈ 7 s",result!.Eta);Assert.Contains("(1/2)",result.Title);
        result=display.Update(new("second",2,2,Work(30)));Assert.Equal("ETA estimating…",result.Eta);Assert.Contains("(2/2)",result.Title);
        Assert.Equal("ETA ∞",display.Update(new("second",2,2,new(SyncProgressStage.Waiting))).Eta);
        Assert.Equal("ETA estimating…",display.Update(new("second",2,2,Work(30))).Eta);
    }
    [Fact]
    public void ReporterKeepsMappingContextDeliversStageChangesAndContainsObserverFailures()
    {
        var received=new List<WindowsSyncProgress>();var root=new WindowsSyncProgressReporter(null,new SyncProgressCallback<WindowsSyncProgress>(received.Add));
        var mapping=WindowsSyncProgressReporter.ForMapping(root,"notes",2,3);
        WindowsSyncProgressReporter.Send(mapping,new(SyncProgressStage.Scanning));
        WindowsSyncProgressReporter.Send(mapping,new(SyncProgressStage.Syncing));
        WindowsSyncProgressReporter.Send(mapping,new(SyncProgressStage.Completed));
        Assert.Equal(3,received.Count);Assert.All(received,p=>{Assert.Equal("notes",p.MappingName);Assert.Equal(2,p.MappingIndex);Assert.Equal(3,p.MappingCount);});
        var throwing=new WindowsSyncProgressReporter(null,new SyncProgressCallback<WindowsSyncProgress>(_=>throw new IOException("UI observer")));
        WindowsSyncProgressReporter.Send(throwing,new(SyncProgressStage.Completed));
    }
}
