using Neptune.Core;

namespace Neptune.Windows;

internal sealed record SyncProgressDisplay(string Title,string PercentText,string Detail,string Eta,string Elapsed,double? Percent,bool Indeterminate);

internal sealed class WindowsSyncProgressDisplay(TimeProvider? clock=null)
{
    private readonly SyncProgressEstimator _estimator=new(clock);
    private readonly TimeProvider _clock=clock??TimeProvider.System;
    private long? _startedAt;
    private TimeSpan _elapsed;
    private (string?,int,int) _mapping;
    internal void Reset(){_estimator.Reset();_mapping=default;}
    internal void Begin(){Reset();_elapsed=TimeSpan.Zero;_startedAt=_clock.GetTimestamp();}
    internal void Stop(){if(_startedAt is { } start)_elapsed=_clock.GetElapsedTime(start);_startedAt=null;}
    internal SyncProgressDisplay Update(WindowsSyncProgress progress)
    {
        var mapping=(progress.MappingName,progress.MappingIndex,progress.MappingCount);
        if(_mapping!=mapping){_estimator.Reset();_mapping=mapping;}
        var work=progress.Work;var estimate=_estimator.Update(work);
        var stage=work.Stage switch
        {
            SyncProgressStage.Preparing=>"Preparing",SyncProgressStage.Scanning=>"Scanning files",
            SyncProgressStage.Syncing=>"Synchronizing",SyncProgressStage.Verifying=>"Verifying changes",
            SyncProgressStage.Waiting=>"Waiting to retry",SyncProgressStage.Completed=>"Synchronized",
            SyncProgressStage.Cancelled=>"Stopped",_=>"Needs attention"
        };
        var title=progress.MappingName is { } name?$"{stage} · {name} ({progress.MappingIndex}/{progress.MappingCount})":stage;
        var detail=work.TotalItems is { } total?$"{Math.Min(work.CompletedItems,total):N0} / {total:N0} operations":string.Empty;
        if(work.Stage==SyncProgressStage.Completed&&progress.MappingName is null)detail="All enabled mirrors";
        var eta=work.Stage==SyncProgressStage.Waiting?"ETA ∞"
            :estimate.Remaining is { } remaining?$"ETA ≈ {FormatRemaining(remaining)} · this mirror"
            :work.Stage==SyncProgressStage.Syncing?"ETA estimating…":string.Empty;
        var indeterminate=estimate.Percent is null&&work.Stage is SyncProgressStage.Preparing or SyncProgressStage.Scanning or SyncProgressStage.Syncing or SyncProgressStage.Verifying;
        var elapsed=_startedAt is { } started?_clock.GetElapsedTime(started):_elapsed;
        return new(title,estimate.Percent is { } percent?$"{Math.Floor(percent):0}%":string.Empty,detail,eta,$"Elapsed {FormatElapsed(elapsed)}",estimate.Percent,indeterminate);
    }
    internal static string FormatElapsed(TimeSpan value)=>value.TotalHours>=1?$"{(int)value.TotalHours}:{value.Minutes:00}:{value.Seconds:00}":$"{value.Minutes:00}:{value.Seconds:00}";
    internal static string FormatRemaining(TimeSpan value)=>value.TotalHours>=1?$"{(int)value.TotalHours} h {value.Minutes} min"
        :value.TotalMinutes>=1?$"{(int)value.TotalMinutes} min {value.Seconds} s":$"{Math.Max(1,(int)Math.Ceiling(value.TotalSeconds))} s";
}
