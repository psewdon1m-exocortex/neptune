namespace Neptune.Core;

public enum SyncProgressStage { Preparing, Scanning, Syncing, Verifying, Waiting, Completed, Cancelled, Failed }

// A current-mirror estimate, never a server durability or whole-profile claim.
public sealed record SyncWorkProgressSnapshot(SyncProgressStage Stage, long CompletedItems = 0,
    long? TotalItems = null, double CompletedUnits = 0, long TransferredBytes = 0,
    string? CurrentPath = null, long Revision = 0);

// Only active jobs are retained. There is no per-file history or filesystem/SQL
// read for a UI refresh. Byte ACKs contribute at most 95% of an unfinished job.
public sealed class SyncWorkProgressTracker
{
    private sealed class Job(string path) { internal string Path=path; internal long Size,Offset; }
    private readonly object _gate=new();
    private readonly Dictionary<string,Job> _active=new(StringComparer.Ordinal);
    private long _done,_bytes,_revision; private long? _total;
    private SyncProgressStage _stage=SyncProgressStage.Preparing;
    public bool Enabled { get; set; }
    public void Begin()
    { lock(_gate){_done=_bytes=0;_total=null;_active.Clear();_stage=SyncProgressStage.Preparing;_revision++;} }
    public void Stage(SyncProgressStage stage) { if(Enabled)lock(_gate)_stage=stage; }
    public void Plan(long remaining)
    {
        if(!Enabled)return;lock(_gate){var total=checked(_done+Math.Max(0,remaining));if(total!=_total)_revision++;_total=total;_stage=SyncProgressStage.Syncing;}
    }
    public void Start(string id,string path)
    { if(Enabled)lock(_gate){if(_active.Count<512)_active.TryAdd(id,new(path));} }
    public void TransferStart(string id,long size,long acknowledged=0)
    { if(Enabled)lock(_gate)if(_active.TryGetValue(id,out var job)){job.Size=Math.Max(0,size);job.Offset=Math.Clamp(acknowledged,0,job.Size);} }
    public void Acknowledge(string id,long offset)
    {
        if(!Enabled)return;lock(_gate)if(_active.TryGetValue(id,out var job))
        {var next=Math.Clamp(offset,job.Offset,job.Size);_bytes=checked(_bytes+next-job.Offset);job.Offset=next;}
    }
    public void Complete(string id)
    { if(Enabled)lock(_gate)if(_active.Remove(id))_done++; }
    public void Release(string id) { if(Enabled)lock(_gate)_active.Remove(id); }
    public SyncWorkProgressSnapshot Snapshot()
    {
        lock(_gate)
        {
            var fraction=_active.Values.Sum(j=>j.Size>0?.95*j.Offset/j.Size:0);
            return new(_stage,_done,_total,_done+fraction,_bytes,_active.Values.FirstOrDefault()?.Path,_revision);
        }
    }
}

public sealed record SyncProgressEstimate(double? Percent,TimeSpan? Remaining);

// Bounded EWMA of current-mirror work. Reset across plans/retries; never let
// idle wall time or an old connection advertise a steadily falling ETA.
public sealed class SyncProgressEstimator(TimeProvider? clock=null)
{
    private readonly TimeProvider _clock=clock??TimeProvider.System;
    private long _revision=long.MinValue,_started,_sampleAt,_movedAt;
    private long? _total; private double _units,_rate; private int _samples;
    private SyncProgressStage _stage;
    public void Reset(){_revision=long.MinValue;_samples=0;_rate=0;}
    public SyncProgressEstimate Update(SyncWorkProgressSnapshot progress)
    {
        if(progress.Stage==SyncProgressStage.Completed){Reset();return new(100,null);}
        if(progress.Stage!=SyncProgressStage.Syncing||progress.TotalItems is not >0)
        {Reset();return new(null,null);}
        var now=_clock.GetTimestamp();var units=Math.Clamp(progress.CompletedUnits,0,progress.TotalItems.Value);
        var percent=Math.Min(99,100*units/progress.TotalItems.Value);
        if(_revision!=progress.Revision||_total!=progress.TotalItems||_stage!=progress.Stage||units<_units)
        { _revision=progress.Revision;_total=progress.TotalItems;_stage=progress.Stage;_started=_sampleAt=_movedAt=now;_units=units;_rate=0;_samples=0;return new(percent,null); }
        var elapsed=_clock.GetElapsedTime(_sampleAt,now).TotalSeconds;
        if(units>_units&&elapsed>=.25)
        {
            var rate=(units-_units)/elapsed;_rate=_samples==0?rate:.3*rate+.7*_rate;
            _units=units;_sampleAt=_movedAt=now;_samples++;
        }
        var stalled=_clock.GetElapsedTime(_movedAt,now)>TimeSpan.FromSeconds(10);
        if(stalled){_rate=0;_samples=0;_started=_sampleAt=now;return new(percent,null);}
        if(_samples<3||_clock.GetElapsedTime(_started,now)<TimeSpan.FromSeconds(3)||_rate<=0||units>=progress.TotalItems)return new(percent,null);
        var seconds=(progress.TotalItems.Value-units)/_rate;
        return new(percent,double.IsFinite(seconds)&&seconds>0&&seconds<=TimeSpan.FromDays(365).TotalSeconds?TimeSpan.FromSeconds(Math.Ceiling(seconds)):null);
    }
}
