using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class Stage3ChatBTests
{
    [Fact]
    public async Task B11_ThreeMappingsShareHardLimitsAndCancelledWaitersRelease()
    {
        using var cancel=new CancellationTokenSource();
        var scheduler=new SyncResourceScheduler();var held=new List<IDisposable>();
        try
        {
            for(var n=0;n<4;n++)held.Add(await scheduler.AcquireAsync((SyncWorkLane)(n%3),1024,false,Ct));
            var pending=scheduler.AcquireAsync(SyncWorkLane.Live,1,false,cancel.Token);Assert.False(pending.IsCompleted);Assert.Equal(4,scheduler.Snapshot().ActiveData);
            cancel.Cancel();await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>pending);Assert.Equal(0,scheduler.Snapshot().Queued);
            Assert.Equal(0,scheduler.Snapshot().PublishedBytes);scheduler.Published(100,TimeSpan.FromMilliseconds(1));Assert.Equal(100,scheduler.Snapshot().PublishedBytes);
        }
        finally{foreach(var lease in held)lease.Dispose();}Assert.Equal(0,scheduler.Snapshot().ActiveData);
        using var a=new SharedCycleGate(1,SyncResourceScheduler.SourceReader);using var b=new SharedCycleGate(1,SyncResourceScheduler.SourceReader);using var c=new SharedCycleGate(1,SyncResourceScheduler.SourceReader);
        await a.WaitAsync(Ct);using var queued=new CancellationTokenSource();var waiting=b.WaitAsync(queued.Token);Assert.False(waiting.IsCompleted);queued.Cancel();await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>waiting);a.Release();await c.WaitAsync(Ct);c.Release();
        using var largeA=new SharedCycleGate(2,SyncResourceScheduler.LargeJobs);using var largeB=new SharedCycleGate(2,SyncResourceScheduler.LargeJobs);
        await largeA.WaitAsync(Ct);await largeB.WaitAsync(Ct);using var largeCancel=new CancellationTokenSource();var third=largeA.WaitAsync(largeCancel.Token);Assert.False(third.IsCompleted);largeCancel.Cancel();await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>third);largeA.Release();largeB.Release();
        using var cpuA=await SyncResourceScheduler.CpuSlot(Ct);using var cpuB=await SyncResourceScheduler.CpuSlot(Ct);using var cpuCancel=new CancellationTokenSource();var cpuThird=SyncResourceScheduler.CpuSlot(cpuCancel.Token);Assert.False(cpuThird.IsCompleted);cpuCancel.Cancel();await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>cpuThird);
    }
    [Fact]
    public async Task B11_WeightedByteQueueAgingAndCooldownUseControlledClock()
    {
        var clock=new Stage3Clock();var scheduler=new SyncResourceScheduler(clock);var held=new List<IDisposable>();var granted=new List<IDisposable>();
        try
        {
            for(var i=0;i<4;i++)held.Add(await scheduler.AcquireAsync(SyncWorkLane.Live,1,false,Ct));
            var bootstrap=scheduler.AcquireAsync(SyncWorkLane.Bootstrap,400,false,Ct);var recovery=scheduler.AcquireAsync(SyncWorkLane.Recovery,400,false,Ct);var live=scheduler.AcquireAsync(SyncWorkLane.Live,400,false,Ct);
            held[0].Dispose();granted.Add(await live);Assert.False(recovery.IsCompleted);Assert.False(bootstrap.IsCompleted);
            granted[0].Dispose();granted.Add(await recovery);Assert.False(bootstrap.IsCompleted);granted[1].Dispose();granted.Add(await bootstrap);
            var old=scheduler.AcquireAsync(SyncWorkLane.Bootstrap,32*1024*1024,false,Ct);clock.Advance(TimeSpan.FromSeconds(31));var fresh=scheduler.AcquireAsync(SyncWorkLane.Live,1,false,Ct);
            granted[2].Dispose();granted.Add(await old);Assert.False(fresh.IsCompleted);granted[3].Dispose();granted.Add(await fresh);
            scheduler.Pressure();Assert.Equal(2,scheduler.Snapshot().DataLimit);scheduler.Published(1024,TimeSpan.FromSeconds(1));Assert.Equal(2,scheduler.Snapshot().DataLimit);
            var queue=scheduler.AcquireAsync(SyncWorkLane.Recovery,1,false,Ct);Assert.False(queue.IsCompleted);clock.Advance(TimeSpan.FromSeconds(31));scheduler.Published(2048,TimeSpan.FromSeconds(1));Assert.Equal(3,scheduler.Snapshot().DataLimit);
            foreach(var h in held)h.Dispose();foreach(var h in granted)h.Dispose();using var final=await queue;Assert.InRange(scheduler.Snapshot().DataLimit,1,4);
        }
        finally{foreach(var h in held)h.Dispose();foreach(var h in granted)h.Dispose();}
    }
    [Fact]
    public async Task B11_RetainedDisabledSpoolsConsumeAggregateByteAndFileBudget()
    {
        RequireWindows();await using var f=await WindowsFreshCycleTests.Fixture.Create();var roots=Enumerable.Range(0,3).Select(i=>Path.Combine(f.Directory,"spool-"+i)).ToArray();foreach(var root in roots)Directory.CreateDirectory(root);
        var paths=new List<string>();
        try
        {
            for(var i=0;i<64;i++){var path=Path.Combine(roots[i%3],Guid.NewGuid().ToString("D")+".spool");paths.Add(path);await File.WriteAllBytesAsync(path,[],Ct);}
            foreach(var root in roots)WindowsCycleSharedBudget.ObserveSpoolDirectory(root);
            Assert.False(WindowsCycleSharedBudget.Reserve(Path.Combine(roots[0],Guid.NewGuid().ToString("D")+".spool"),1));
            foreach(var path in paths)WindowsCycleSharedBudget.Forget(path);paths.Clear();
            var one=Path.Combine(roots[0],Guid.NewGuid().ToString("D")+".spool");paths.Add(one);Assert.True(WindowsCycleSharedBudget.Reserve(one,1024L*1024*1024));
            Assert.False(WindowsCycleSharedBudget.Reserve(Path.Combine(roots[2],Guid.NewGuid().ToString("D")+".spool"),1));
            Assert.True(WindowsCycleSharedBudget.Reserve(one,1024L*1024*1024));Assert.False(WindowsCycleSharedBudget.Reserve(one,1));
        }
        finally{foreach(var path in paths)WindowsCycleSharedBudget.Forget(path);}
    }
}
