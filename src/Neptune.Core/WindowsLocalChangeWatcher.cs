using System.Threading.Channels;

namespace Neptune.Core;

// FileSystemWatcher uses ReadDirectoryChangesW on Windows. Hints complement
// USN coalescing; they do not replace ordered journal/membership evidence.
internal sealed class WindowsLocalChangeWatcher : IDisposable, IAsyncDisposable
{
    private readonly FileSystemWatcher _watcher;
    private readonly Channel<string> _paths=Channel.CreateBounded<string>(new BoundedChannelOptions(256){SingleReader=true,SingleWriter=false,FullMode=BoundedChannelFullMode.Wait});
    private string? _gap;
    private readonly CancellationTokenSource _shutdown=new();
    private readonly SemaphoreSlim _flush=new(1,1);
    private LocalSourceJournalSession? _session;
    private Task? _pump;
    private long _due=long.MaxValue;
    private long _retryDue=long.MaxValue;
    private bool _gapSaved;
    internal event Action? WorkAvailable;
    internal async Task StartAsync(LocalSourceJournalSession session,CancellationToken ct)
    {
        await session.InitializeEventInboxAsync(ct);_session=session;
        var retained=await session.ReadEventHintsAsync(ct);
        _due=retained.Gap?0:retained.NextDue??long.MaxValue;
        _pump=Task.Run(PumpAsync);
    }
    private async Task PumpAsync()
    {
        try
        {
            while(!_shutdown.IsCancellationRequested)
            {
                await FlushAsync(_shutdown.Token);
                var due=Interlocked.Read(ref _due);
                if(_session!.Clock.GetUtcNow().ToUnixTimeMilliseconds()>=due&&Interlocked.CompareExchange(ref _due,long.MaxValue,due)==due)WorkAvailable?.Invoke();
                var retry=Interlocked.Read(ref _retryDue);
                if(_session!.Clock.GetUtcNow().ToUnixTimeMilliseconds()>=retry&&Interlocked.CompareExchange(ref _retryDue,long.MaxValue,retry)==retry)WorkAvailable?.Invoke();
                await Task.Delay(50,_shutdown.Token);
            }
        }
        catch(OperationCanceledException)when(_shutdown.IsCancellationRequested){}
        catch(Exception e){Interlocked.Exchange(ref _gap,"Durable watcher ingress failed: "+e.GetType().Name);WorkAvailable?.Invoke();}
    }
    internal async Task FlushAsync(CancellationToken ct)
    {
        if(_session is null)return;
        await _flush.WaitAsync(ct);
        try
        {
            for(var batch=0;batch<2;batch++)
            {
                var paths=Drain();var saveGap=Gap is not null&&!_gapSaved;
                if(paths.Count==0&&!saveGap)return;
                var due=await _session.SaveEventHintsAsync(paths,saveGap,ct);
                _gapSaved|=saveGap;
                if(due is not null)Interlocked.Exchange(ref _due,due.Value);
            }
        }
        catch { Interlocked.CompareExchange(ref _gap,"Durable watcher persistence gap.",null);throw; }
        finally{_flush.Release();}
    }
    internal WindowsLocalChangeWatcher(string root)
    {
        if(!OperatingSystem.IsWindows())throw new PlatformNotSupportedException();
        _watcher=new(root){IncludeSubdirectories=true,InternalBufferSize=64*1024,
            NotifyFilter=NotifyFilters.FileName|NotifyFilters.DirectoryName|NotifyFilters.LastWrite|NotifyFilters.Size};
        _watcher.Changed+=(_,e)=>Add(e.Name);_watcher.Created+=(_,e)=>Add(e.Name);
        _watcher.Deleted+=(_,e)=>Add(e.Name);_watcher.Renamed+=(_,e)=>{Add(e.OldName);Add(e.Name);};
        _watcher.Error+=(_,e)=>Interlocked.CompareExchange(ref _gap,"Native watcher overflow/error requires source reconciliation: "+e.GetException().GetType().Name,null);
        _watcher.EnableRaisingEvents=true;
    }
    private void Add(string? name)
    {
        if(name is null||name.Length is <1 or >4096||Path.IsPathRooted(name)||name.Split(['\\','/']).Any(p=>p is "" or "." or "..")
            ||!_paths.Writer.TryWrite(name.Replace('\\','/')))
            Interlocked.CompareExchange(ref _gap,"Native watcher bounded inbox/alias framing gap.",null);
    }
    internal string? Gap=>Volatile.Read(ref _gap);
    internal void ScheduleRemaining(long? due){if(due is not null)Interlocked.Exchange(ref _due,due.Value);}
    internal void ScheduleRetry(long? due)=>Interlocked.Exchange(ref _retryDue,due??long.MaxValue);
    internal async Task BeginGapRecoveryAsync(CancellationToken ct)
    {await _flush.WaitAsync(ct);try{Interlocked.Exchange(ref _gap,null);_gapSaved=false;}finally{_flush.Release();}}
    internal IReadOnlyList<string> Drain()
    {
        var result=new List<string>(128);while(result.Count<128&&_paths.Reader.TryRead(out var path))result.Add(path);return result;
    }
    public void Dispose(){_watcher.EnableRaisingEvents=false;_watcher.Dispose();_paths.Writer.TryComplete();}
    public async ValueTask DisposeAsync()
    {
        Dispose();_shutdown.Cancel();if(_pump is not null)await _pump;
        // Persist the final bounded ingress before the profile owner is released.
        await FlushAsync(CancellationToken.None);_shutdown.Dispose();
    }
}
