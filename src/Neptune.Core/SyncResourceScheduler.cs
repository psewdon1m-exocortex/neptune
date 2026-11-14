using System.Diagnostics;

namespace Neptune.Core;

internal enum SyncWorkLane { Live, Recovery, Bootstrap }
public sealed record SyncSchedulerSnapshot(int DataLimit,int ActiveData,int ActiveTransfers,int Queued,long PoolWaitMilliseconds,long PublishedBytes,double PublishedBytesPerSecond);

// One client-process budget, shared by all mappings. Per-mapping limits can
// narrow it. No adaptation can enlarge the fixed reader/large/buffer ceilings.
internal sealed class SyncResourceScheduler(TimeProvider? timeProvider = null,int maximum=4)
{
    private readonly TimeProvider _clock=timeProvider ?? TimeProvider.System;
    internal static readonly SyncResourceScheduler Shared=new(maximum:8);
    internal static readonly SemaphoreSlim SourceReader=new(1,1),LargeJobs=new(2,2);
    private static readonly SemaphoreSlim Cpu=new(2,2);
    internal static async Task<IDisposable> CpuSlot(CancellationToken ct)
    {await Cpu.WaitAsync(ct);return new CpuLease();}
    private sealed class CpuLease:IDisposable
    {private int _disposed;public void Dispose(){if(Interlocked.Exchange(ref _disposed,1)==0)Cpu.Release();}}
    private sealed class Request(SyncWorkLane lane,long bytes,bool transfer,long enqueued)
    {
        internal readonly SyncWorkLane Lane=lane;
        internal readonly long Bytes=bytes,Enqueued=enqueued;
        internal readonly bool Transfer=transfer;
        internal readonly TaskCompletionSource<IDisposable> Completion=new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal double Finish;
    }
    private readonly object _gate=new();
    private readonly List<Request> _pending=[];
    private readonly double[] _finish=new double[3];
    private int _activeData,_activeTransfers,_limit=maximum;
    private readonly int _maximum=maximum;
    private long _waitTicks,_published,_windowBytes,_windowStart=(timeProvider ?? TimeProvider.System).GetTimestamp(),_cooldown;
    private double _virtual,_rate,_previousRate,_latency,_previousLatency;
    internal Task<IDisposable> AcquireAsync(SyncWorkLane lane,long bytes,bool transfer,CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();var request=new Request(lane,Math.Max(1,bytes),transfer,_clock.GetTimestamp());
        lock(_gate)
        {
            if(_pending.Count>=512)throw new CycleSpoolAdmissionException("Shared scheduler queue is full.");
            // Weighted fair byte service, with a 30 s aging override. Idle
            // classes reserve no slots; another class can borrow the capacity.
            var weight=lane==SyncWorkLane.Live?4:lane==SyncWorkLane.Recovery?2:1;
            request.Finish=Math.Max(_virtual,_finish[(int)lane])+Math.Min(request.Bytes,32L*1024*1024)/(double)weight;
            _finish[(int)lane]=request.Finish;_pending.Add(request);Pump();
        }
        return Wait(request,ct);
    }
    private async Task<IDisposable> Wait(Request request,CancellationToken ct)
    {
        using var cancellation=ct.Register(()=>
        {
            lock(_gate){if(_pending.Remove(request)){request.Completion.TrySetCanceled(ct);Pump();}}
        });
        var lease=await request.Completion.Task;
        if(ct.IsCancellationRequested){lease.Dispose();ct.ThrowIfCancellationRequested();}return lease;
    }
    private void Pump()
    {
        while(true)
        {
            var now=_clock.GetTimestamp();Request? next=null;
            foreach(var item in _pending)
            {
                if(item.Transfer?_activeTransfers>=_limit:_activeData>=_limit)continue;
                if(next is null||((now-item.Enqueued)>30*_clock.TimestampFrequency&&(now-next.Enqueued)<=30*_clock.TimestampFrequency)
                    ||((now-item.Enqueued)>30*_clock.TimestampFrequency&&(now-next.Enqueued)>30*_clock.TimestampFrequency&&item.Enqueued<next.Enqueued)
                    ||((now-item.Enqueued)<=30*_clock.TimestampFrequency&&(now-next.Enqueued)<=30*_clock.TimestampFrequency&&item.Finish<next.Finish))next=item;
            }
            if(next is null)return;
            _pending.Remove(next);_virtual=Math.Max(_virtual,next.Finish);_waitTicks+=now-next.Enqueued;
            if(next.Transfer)_activeTransfers++;else _activeData++;
            next.Completion.TrySetResult(new Lease(this,next.Transfer));
        }
    }
    internal void Pressure()
    {lock(_gate){_limit=Math.Max(1,_limit/2);_cooldown=_clock.GetTimestamp()+30*_clock.TimestampFrequency;}}
    internal void Published(long bytes,TimeSpan latency)
    {
        lock(_gate)
        {
            _published+=Math.Max(0,bytes);_windowBytes+=Math.Max(0,bytes);_latency=_latency==0?latency.TotalSeconds:0.8*_latency+0.2*latency.TotalSeconds;
            var now=_clock.GetTimestamp();var elapsed=(now-_windowStart)/(double)_clock.TimestampFrequency;if(elapsed<30)return;
            _rate=_windowBytes/elapsed;
            // Increase only on useful publication, stable latency and queued
            // demand. Backpressure immediately reduces concurrency instead.
            if(now>=_cooldown&&_windowBytes>0&&_pending.Count>0&&(_previousLatency==0||_latency<=_previousLatency*1.2)&&(_previousRate==0||_rate>=_previousRate*0.9))
            {_limit=Math.Min(_maximum,_limit+1);_cooldown=now+30*_clock.TimestampFrequency;}
            _previousRate=_rate;_previousLatency=_latency;_windowBytes=0;_windowStart=now;Pump();
        }
    }
    internal SyncSchedulerSnapshot Snapshot()
    {lock(_gate)return new(_limit,_activeData,_activeTransfers,_pending.Count,(long)(_waitTicks*1000d/_clock.TimestampFrequency),_published,_rate);}
    private sealed class Lease(SyncResourceScheduler owner,bool transfer):IDisposable
    {
        private int _disposed;
        public void Dispose(){if(Interlocked.Exchange(ref _disposed,1)!=0)return;lock(owner._gate){if(transfer)owner._activeTransfers--;else owner._activeData--;owner.Pump();}}
    }
}

internal sealed class SharedCycleGate(int localLimit,SemaphoreSlim shared):IDisposable
{
    private readonly SemaphoreSlim _local=new(localLimit,localLimit);
    internal async Task WaitAsync(CancellationToken ct)
    {await _local.WaitAsync(ct);try{await shared.WaitAsync(ct);}catch{_local.Release();throw;}}
    internal void Release(){shared.Release();_local.Release();}
    public void Dispose()=>_local.Dispose();
}
