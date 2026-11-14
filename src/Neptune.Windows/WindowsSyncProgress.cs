using System.Diagnostics;
using Neptune.Core;

namespace Neptune.Windows;

public sealed record WindowsSyncProgress(string? MappingName,int MappingIndex,int MappingCount,SyncWorkProgressSnapshot Work);

internal sealed class WindowsSyncProgressReporter(IProgress<string>? text,IProgress<WindowsSyncProgress> target,
    string? mapping=null,int index=0,int count=0):IProgress<string>,IProgress<SyncWorkProgressSnapshot>
{
    private readonly IProgress<string>? _text=text;
    private readonly IProgress<WindowsSyncProgress> _target=target;
    private readonly object _gate=new();
    private long _lastWork,_lastText;private SyncProgressStage? _stage;
    public void Report(string value)
    {
        lock(_gate){var now=Stopwatch.GetTimestamp();if(_lastText!=0&&Stopwatch.GetElapsedTime(_lastText,now)<TimeSpan.FromMilliseconds(250))return;_lastText=now;}
        try{_text?.Report(value);}catch{}
    }
    public void Report(SyncWorkProgressSnapshot value)
    {
        lock(_gate){var now=Stopwatch.GetTimestamp();if(_stage==value.Stage&&_lastWork!=0&&Stopwatch.GetElapsedTime(_lastWork,now)<TimeSpan.FromMilliseconds(250))return;_lastWork=now;_stage=value.Stage;}
        try{_target.Report(new(mapping,index,count,value));}catch{}
    }
    internal static IProgress<string>? ForMapping(IProgress<string>? progress,string name,int index,int count)
        =>progress is WindowsSyncProgressReporter p?new WindowsSyncProgressReporter(p._text,p._target,name,index,count):progress;
    internal static void Send(IProgress<string>? progress,SyncWorkProgressSnapshot value)
    {if(progress is IProgress<SyncWorkProgressSnapshot> detailed)try{detailed.Report(value);}catch{}}
}

internal sealed class SyncProgressCallback<T>(Action<T> callback):IProgress<T>
{ public void Report(T value)=>callback(value); }
