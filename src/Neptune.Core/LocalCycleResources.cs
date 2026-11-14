using System.Security.Cryptography;

namespace Neptune.Core;

public sealed record WindowsCycleResourceMetrics(int MaximumDataJobs,int MaximumLargeJobs,int MaximumSourceReaders,int MaximumReadyQueue,long MaximumSpoolBytes,long PostCommitDiagnostics);
internal sealed class LocalCycleResources : IDisposable
{
    internal readonly SharedCycleGate Reader=new(1,SyncResourceScheduler.SourceReader),Large;
    internal readonly SemaphoreSlim Data;
    internal bool DeferCaptureOnSpoolPressure;
    internal readonly string Root;internal readonly WindowsCycleBudget Budget;
    private readonly Dictionary<string,long> _occupied=new(StringComparer.Ordinal);
    private readonly HashSet<string> _active=new(StringComparer.Ordinal);private readonly object _gate=new();
    private readonly IDisposable _ancestry;private readonly FileStream _ownership;
    private TaskCompletionSource _changed=Signal();private long _bytes,_maximum,_diagnostics;private int _data,_large,_readers,_maxData,_maxLarge,_maxReaders,_maxReady;
    private static TaskCompletionSource Signal()=>new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal LocalCycleResources(string root,WindowsCycleBudget budget)
    {
        Root=Path.GetFullPath(root);Budget=budget;Directory.CreateDirectory(Root);
        Data=new(budget.DataJobs,budget.DataJobs);
        if((File.GetAttributes(Root)&FileAttributes.ReparsePoint)!=0)throw new IOException("Cycle spool root is a reparse point.");
        _ancestry=WindowsBootstrapSourceCapture.HoldDirectoryPath(Root);
        Large=new(budget.LargeJobs,SyncResourceScheduler.LargeJobs);
        try
        {
        var ownership=Path.Combine(Root,".cycle-spool-owner.lock");
        if(File.Exists(ownership)&&(File.GetAttributes(ownership)&FileAttributes.ReparsePoint)!=0)throw new IOException("Spool ownership file is a reparse point.");
        _ownership=new FileStream(ownership,FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None);
        WindowsBootstrapSourceCapture.RequireHandlePath(_ownership.SafeFileHandle,ownership);
        foreach(var f in new DirectoryInfo(Root).EnumerateFiles())
        {
            if(f.Name==".cycle-spool-owner.lock")continue;
            if(_occupied.Count>=64||f.Name.Length!=42||!f.Name.EndsWith(".spool",StringComparison.Ordinal)||!LocalCycleWire.Uuid(f.Name[..36])||(f.Attributes&FileAttributes.ReparsePoint)!=0)
                throw new BootstrapCaptureDeferredException("Cycle spool has unknown files or exceeds its fixed inventory.");
            _occupied.Add(f.Name[..36],f.Length);_bytes=checked(_bytes+f.Length);
        }
        _maximum=_bytes;if(_bytes>budget.SpoolBytes||Free<budget.MinimumFreeDiskBytes)throw new BootstrapCaptureDeferredException("Retained cycle spool exceeds budget/free floor.");
        WindowsCycleSharedBudget.ObserveSpoolDirectory(Root);
        }
        catch{_ownership?.Dispose();_ancestry.Dispose();Large.Dispose();Reader.Dispose();Data.Dispose();throw;}
    }
    private long Free=>new DriveInfo(Path.GetPathRoot(Root)!).AvailableFreeSpace;
    internal string PathFor(string operation)=>Path.Combine(Root,operation+".spool");
    // Never wait while callers already hold their original spools:
    // an aggregate competes for the same quota and falls back individually.
    internal bool TryReserveAggregate(string operation,long bytes)
    {
        if(!LocalCycleWire.Uuid(operation)||bytes is <0 or >SmallFileBatchPlan.MaximumDataBytes)throw new ArgumentException("Invalid aggregate reservation.");
        lock(_gate)
        {
            var free=Free;
            if(_occupied.ContainsKey(operation)||_occupied.Count>=64||bytes>Budget.SpoolBytes-_bytes||free<Budget.MinimumFreeDiskBytes||bytes>free-Budget.MinimumFreeDiskBytes)return false;
            if(!WindowsCycleSharedBudget.Reserve(PathFor(operation),bytes))return false;
            _occupied.Add(operation,bytes);_active.Add(operation);_bytes+=bytes;_maximum=Math.Max(_maximum,_bytes);return true;
        }
    }
    internal void ConfirmAggregateRemoved(string operation)
    {
        lock(_gate)
        {
            if(File.Exists(PathFor(operation)))throw new IOException("Aggregate path still exists after exact handle deletion.");
            if(_occupied.Remove(operation,out var bytes))_bytes-=bytes;
            WindowsCycleSharedBudget.Forget(PathFor(operation));
            _active.Remove(operation);var signal=_changed;_changed=Signal();signal.TrySetResult();
        }
    }
    internal async Task ReserveAsync(string operation,long bytes,CancellationToken ct)
    {
        while(true)
        {
            Task wait;lock(_gate)
            {
                if(bytes<0||bytes>Budget.SpoolBytes)throw new BootstrapCaptureDeferredException("File exceeds explicit 1GiB capture quota.");
                if(_occupied.TryGetValue(operation,out var old)){if(old!=bytes)throw new InvalidDataException("Original spool size changed.");if(!WindowsCycleSharedBudget.Reserve(PathFor(operation),bytes))throw new CycleSpoolAdmissionException("Shared retained spool reservation is unavailable.");_active.Add(operation);return;}
                var free=Free;if(free<Budget.MinimumFreeDiskBytes)throw new CycleSpoolAdmissionException("Spool free floor unavailable.");
                if(bytes<=Budget.SpoolBytes-_bytes&&_occupied.Count<64&&bytes<=free-Budget.MinimumFreeDiskBytes)
                {if(!WindowsCycleSharedBudget.Reserve(PathFor(operation),bytes))throw new CycleSpoolAdmissionException("Shared retained spool quota prevents new capture.");_occupied.Add(operation,bytes);_active.Add(operation);_bytes+=bytes;_maximum=Math.Max(_maximum,_bytes);return;}
                // A pipeline may already hold sealed originals awaiting a
                // transport slot. Never occupy every capture slot waiting for
                // their quota: defer new work so those originals can finish.
                if(DeferCaptureOnSpoolPressure||_active.Count==0)throw new CycleSpoolAdmissionException("Retained spools prevent new capture admission.");wait=_changed.Task;
            }
            await wait.WaitAsync(ct);
        }
    }
    internal async Task<FileStream> OpenAsync(LocalCycleWork w,bool verify,CancellationToken ct)
    {
        var path=PathFor(w.OperationId);if(path!=w.SpoolPath||w.Size is null||w.Sha256 is null||!File.Exists(path)||(File.GetAttributes(path)&FileAttributes.ReparsePoint)!=0)throw new InvalidDataException("Original owned spool unavailable.");
        var f=new FileStream(path,FileMode.Open,FileAccess.Read,FileShare.Read,128*1024,FileOptions.Asynchronous|FileOptions.SequentialScan);
        try{WindowsBootstrapSourceCapture.RequireHandlePath(f.SafeFileHandle,path);if(f.Length!=w.Size)throw new InvalidDataException("Original spool size changed.");if(verify){using var cpu=await SyncResourceScheduler.CpuSlot(ct);var hash=Convert.ToHexStringLower(await SHA256.HashDataAsync(f,ct));if(hash!=w.Sha256)throw new InvalidDataException("Original spool digest changed.");f.Position=0;}return f;}catch{await f.DisposeAsync();throw;}
    }
    internal void Release(string operation,bool delete)
    {
        lock(_gate)
        {
            if(delete){var path=PathFor(operation);if(File.Exists(path)){if((File.GetAttributes(path)&FileAttributes.ReparsePoint)!=0)throw new IOException("Spool became a reparse point.");File.Delete(path);}if(_occupied.Remove(operation,out var bytes))_bytes-=bytes;WindowsCycleSharedBudget.Forget(path);}
            _active.Remove(operation);var signal=_changed;_changed=Signal();signal.TrySetResult();
        }
    }
    internal void StartData(){lock(_gate){_maxData=Math.Max(_maxData,++_data);}}internal void EndData(){lock(_gate){_data--;}}
    internal void StartLarge(){lock(_gate){_maxLarge=Math.Max(_maxLarge,++_large);}}internal void EndLarge(){lock(_gate){_large--;}}
    internal void StartReader(){lock(_gate){_maxReaders=Math.Max(_maxReaders,++_readers);}}internal void EndReader(){lock(_gate){_readers--;}}
    internal void Queue(int count){lock(_gate){_maxReady=Math.Max(_maxReady,count);}}
    internal void Diagnostic()=>Interlocked.Increment(ref _diagnostics);
    internal WindowsCycleResourceMetrics Metrics{get{lock(_gate)return new(_maxData,_maxLarge,_maxReaders,_maxReady,_maximum,Interlocked.Read(ref _diagnostics));}}
    public void Dispose(){_ownership.Dispose();_ancestry.Dispose();Reader.Dispose();Large.Dispose();Data.Dispose();}
}
// Global backpressure differs from one source file that cannot yet be sealed.
internal sealed class CycleSpoolAdmissionException(string message):IOException(message);
