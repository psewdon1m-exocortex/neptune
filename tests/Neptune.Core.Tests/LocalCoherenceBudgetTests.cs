using System.Diagnostics;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

[CollectionDefinition("LocalCoherenceQualification",DisableParallelization=true)]
public sealed class LocalCoherenceQualificationCollection;

public sealed partial class LocalCoherenceTests
{
    [Fact]
    public async Task ActualFourThousandOutsideFilesHaveNoInsideDirtyAndNoRepeatTreeEnumeration()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();var enumerations=0;
        await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct,(o,ct)=>{enumerations++;return Task.CompletedTask;});await using var s=await f.Open(c);var anchor=await c.BuildAnchorAsync(s,cancellationToken:Ct);var initial=enumerations;
        var timer=Stopwatch.StartNew();for(var i=0;i<4000;i++)System.IO.File.WriteAllText(Path.Combine(f.Outside,"outside-"+i),"outside");var created=timer.Elapsed;
        var cycle=Stopwatch.StartNew();var observed=await c.ObserveChangesAsync(s,anchor.CoherenceId,Ct);
        Assert.Equal(LocalCoherenceStatus.Anchored,observed.Status);Assert.Equal(0,observed.DirtyNodes);Assert.True(observed.OutsideRecords>=4000);Assert.Equal(initial,enumerations);Assert.Equal(0,observed.Unresolved);
        await f.Evidence("actual-4000-outside",new{dataset="owned NTFS sibling files; actual OS journal",files=4000,createdMilliseconds=created.TotalMilliseconds,cycleMilliseconds=cycle.Elapsed.TotalMilliseconds,enumerationsBefore=initial,enumerationsAfter=enumerations,anchor,observed,journal=f.Store.GetLocalJournalMetrics(),membership=f.Store.GetLocalCoherenceMetrics()});
    }
    [Theory]
    [InlineData(100000)][InlineData(500000)]
    public async Task SyntheticOutsidePrefixScaleHasFixedHotPagesAndRowEffects(int count)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);var anchor=await c.BuildAnchorAsync(s,cancellationToken:Ct);
        // Drain real records captured beyond the native anchor cut before
        // establishing synthetic counters. They must not spill into its last
        // partial page and be mistaken for extra synthetic input.
        anchor=await c.ObserveChangesAsync(s,anchor.CoherenceId,Ct);Assert.Equal(LocalCoherenceStatus.Anchored,anchor.Status);
        var current=anchor.AppliedUsn;var baseline=anchor.OutsideRecords;var baselineRecords=anchor.ProcessedRecords;var process=Process.GetCurrentProcess();var retainedBefore=GC.GetTotalMemory(true);var timer=Stopwatch.StartNew();long rss=0,heap=0,wal=0,db=0,maxRows=0;var batches=0;var maxCapture=TimeSpan.Zero;var maxApply=TimeSpan.Zero;var maxRetire=TimeSpan.Zero;
        for(var offset=0;offset<count;offset+=256)
        {
            var cursor=(await s.ReadStateAsync(Ct)).Cursor;var take=Math.Min(256,count-offset);var page=UsnRecordParser.Parse(cursor.JournalIdHex,cursor.NextReadUsn,SyntheticV3(cursor.NextReadUsn,take,"ffffffffffffffffffffffffffffffff","eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",1));
            var stage=Stopwatch.StartNew();await s.CapturePageAsync(cursor,page,Ct);if(stage.Elapsed>maxCapture)maxCapture=stage.Elapsed;
            stage.Restart();var applied=await s.ApplyNextCoherenceAsync(anchor.CoherenceId,current,Ct);if(stage.Elapsed>maxApply)maxApply=stage.Elapsed;current=applied.AppliedUsn;maxRows=Math.Max(maxRows,applied.ChangedRows);
            Assert.Equal(take,applied.Records);Assert.Equal(take,applied.Outside);Assert.Equal(0,applied.Dirty);Assert.Equal(0,applied.Unresolved);
            // Full raw/disposition retirement is separately bounded and keeps
            // replay counters; this dataset never fabricates a native OS cut.
            while(true){stage.Restart();var retired=await s.RetireCoherenceInboxBatchAsync(anchor.CoherenceId,Ct);if(stage.Elapsed>maxRetire)maxRetire=stage.Elapsed;maxRows=Math.Max(maxRows,retired.ChangedRows);if(retired.Rows==0)break;}
            batches++;process.Refresh();rss=Math.Max(rss,process.WorkingSet64);heap=Math.Max(heap,GC.GetTotalMemory(false));wal=Math.Max(wal,System.IO.File.Exists(f.Database+"-wal")?new FileInfo(f.Database+"-wal").Length:0);db=Math.Max(db,new FileInfo(f.Database).Length);
        }
        var result=await s.ReadCoherenceAsync(anchor.CoherenceId,Ct);Assert.Equal(baseline+count,result.OutsideRecords);Assert.Equal(baselineRecords+count,result.ProcessedRecords);Assert.Equal(0,result.DirtyNodes);Assert.Equal(0,result.Unresolved);Assert.InRange(maxRows,1,260);
        var retainedAfter=GC.GetTotalMemory(true);Assert.True(retainedAfter-retainedBefore<16L*1024*1024,"Retained managed state must not grow with the event catalogue.");
        await f.Evidence("synthetic-scale-"+count,new{dataset="lazy synthetic V3 pages after actual owned native anchor; not 100k/500k real filesystem objects or an OS continuity claim",count,batches,maximumInputRecords=256,maximumInputBytes=24584,maximumDerivedRows=maxRows,totalMilliseconds=timer.Elapsed.TotalMilliseconds,maximumCaptureMilliseconds=maxCapture.TotalMilliseconds,maximumApplyMilliseconds=maxApply.TotalMilliseconds,maximumRetirementMilliseconds=maxRetire.TotalMilliseconds,peakProcessRss=rss,peakManagedHeap=heap,retainedBefore,retainedAfter,retainedDelta=retainedAfter-retainedBefore,peakPhysicalWal=wal,peakDatabase=db,result,journal=f.Store.GetLocalJournalMetrics(),membership=f.Store.GetLocalCoherenceMetrics()});
    }
    [Fact]
    public async Task SyntheticPinnedReaderStopsBeforePredictedWalBudgetAndDoesNotAdvanceRefusedCursor()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);var anchor=await c.BuildAnchorAsync(s,cancellationToken:Ct);
        var current=anchor.AppliedUsn;
        await using var held=await f.Connection();using var tx=held.BeginTransaction(deferred:true);await using var query=held.CreateCommand();query.Transaction=tx;query.CommandText="SELECT bytes FROM local_coherence_format";await query.ExecuteScalarAsync(Ct);
        var iterations=0;LocalSourceGapException? stop=null;LocalSourceContinuityState? refusedBefore=null;LocalCoherenceState? memberBefore=null;var stage="";
        while(iterations<5000)
        {
            var source=await s.ReadStateAsync(Ct);var page=UsnRecordParser.Parse(source.Cursor.JournalIdHex,source.Cursor.NextReadUsn,SyntheticV3(source.Cursor.NextReadUsn,256,"ffffffffffffffffffffffffffffffff","eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",1));
            refusedBefore=source;memberBefore=await s.ReadCoherenceAsync(anchor.CoherenceId,Ct);
            try{stage="capture";await s.CapturePageAsync(source.Cursor,page,Ct);refusedBefore=await s.ReadStateAsync(Ct);stage="apply";var applied=await s.ApplyNextCoherenceAsync(anchor.CoherenceId,current,Ct);current=applied.AppliedUsn;iterations++;}
            catch(LocalSourceGapException e){stop=e;break;}
        }
        Assert.NotNull(stop);Assert.Equal("WalAdmission",stop.Code);var after=await s.ReadStateAsync(Ct);var memberAfter=await s.ReadCoherenceAsync(anchor.CoherenceId,Ct);Assert.Equal(refusedBefore!.Cursor.NextReadUsn,after.Cursor.NextReadUsn);Assert.Equal(memberBefore!.AppliedUsn,memberAfter.AppliedUsn);
        var metrics=f.Store.GetLocalJournalMetrics();var physical=new FileInfo(f.Database+"-wal").Length;Assert.InRange(physical,1,64L*1024*1024);
        await tx.RollbackAsync(Ct);
        // Use the public measured checkpoint API so the admission cache is
        // refreshed from actual frames, not from speculative physical length.
        var released=await f.Store.InspectLocalJournalWalAsync(force:true,cancellationToken:Ct);Assert.Equal(0,released.UncheckpointedWalBytes);
        var physicalAfter=new FileInfo(f.Database+"-wal").Length;
        if(stage=="capture"){var cursor=(await s.ReadStateAsync(Ct)).Cursor;await s.CapturePageAsync(cursor,UsnRecordParser.Parse(cursor.JournalIdHex,cursor.NextReadUsn,SyntheticV3(cursor.NextReadUsn,256,"ffffffffffffffffffffffffffffffff","eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",1)),Ct);}
        var recovery=await s.ApplyNextCoherenceAsync(anchor.CoherenceId,current,Ct);Assert.True(recovery.AppliedUsn>current);
        await f.Evidence("pinned-reader",new{dataset="actual held SQLite read transaction; synthetic bounded V3 journal pages",iterations,refusedStage=stage,code=stop.Code,physicalWal=physical,physicalWalAfterPassive=physicalAfter,configuredOutstandingWalCap=64L*1024*1024,refusedCursor=after.Cursor.NextReadUsn,refusedApplied=memberAfter.AppliedUsn,metrics,released,recovery});
    }
}
