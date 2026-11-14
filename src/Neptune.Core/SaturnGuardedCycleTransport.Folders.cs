using System.Threading.Channels;

namespace Neptune.Core;

public sealed partial class SaturnGuardedCycleTransport
{
    private sealed record FolderCall(CycleFolderInput Input,CancellationToken Cancellation,TaskCompletionSource<CycleFolderOutcome> Completion);
    private readonly Channel<FolderCall> _folders=Channel.CreateBounded<FolderCall>(new BoundedChannelOptions(500){SingleReader=true,FullMode=BoundedChannelFullMode.Wait});
    private readonly CancellationTokenSource _folderShutdown=new();
    private readonly object _folderGate=new();
    private Task? _folderPump;
    private sealed class DirectoryFlight{internal readonly SemaphoreSlim Gate=new(1,1);internal int References;}
    private sealed record DirectoryResult(string Input,CycleFolderOutcome? Outcome,GuardedCycleRemoteException? Failure,long Expires);
    private readonly SemaphoreSlim _directoryAdmission=new(500,500);
    private readonly Dictionary<string,DirectoryFlight> _directoryFlights=new(StringComparer.Ordinal);
    private readonly Dictionary<string,DirectoryResult> _directoryResults=new(StringComparer.Ordinal);
    private long _directoryFailureGeneration;
    private void InvalidateDirectoryFailures()
    {
        lock(_folderGate)
        {
            _directoryFailureGeneration++;
            foreach(var key in _directoryResults.Where(pair=>pair.Value.Failure is not null).Select(pair=>pair.Key).ToArray())_directoryResults.Remove(key);
        }
    }
    private Sync.Protocol.SyncFeedPage ObserveDirectoryFeed(Sync.Protocol.SyncFeedPage page)
    {
        if(page.Items.Count>0)InvalidateDirectoryFailures();
        return page;
    }
    public async Task<CycleFolderOutcome> EnsureDirectoryAsync(CycleFolderInput original,CancellationToken ct)
    {
        LocalCycleWire.FolderInput(original);
        if(NamespacePolicy is {} policy)_=Sync.Protocol.SyncNameCodec.Key(original.Name,policy.Codec);
        if(original.RootId!=Binding.RootId||original.Epoch!=Binding.ServerEpoch||original.BindingVersion!=Binding.BindingVersion)throw new InvalidDataException("Folder ensure cannot borrow another namespace binding.");
        await _directoryAdmission.WaitAsync(ct);var key=original.ParentId+"\0"+original.Name;DirectoryFlight flight;
        lock(_folderGate){if(!_directoryFlights.TryGetValue(key,out flight!))_directoryFlights.Add(key,flight=new());flight.References++;}
        var taken=false;
        try
        {
            await flight.Gate.WaitAsync(ct);taken=true;
            var identity=System.Text.Json.JsonSerializer.Serialize(LocalCycleWire.FolderObject(original));
            DirectoryResult? cached;long failureGeneration;lock(_folderGate){_directoryResults.TryGetValue(original.OperationId,out cached);failureGeneration=_directoryFailureGeneration;}
            if(cached is not null)
            {
                if(cached.Input!=identity)throw new InvalidDataException("Folder original changed within the same transport binding.");
                if(cached.Expires>Environment.TickCount64){if(cached.Failure is not null)throw cached.Failure;return cached.Outcome!;}
            }
            void Remember(DirectoryResult result){lock(_folderGate){if(result.Failure is not null&&failureGeneration!=_directoryFailureGeneration)return;if(_directoryResults.Count>=256)_directoryResults.Clear();_directoryResults[original.OperationId]=result;}}
            try{var outcome=await DispatchDirectory(original,ct);Remember(new(identity,outcome,null,Environment.TickCount64+30_000));return outcome;}
            catch(GuardedCycleRemoteException error)when(error.StructuredWireCode&&error.Status is System.Net.HttpStatusCode.NotFound or System.Net.HttpStatusCode.PreconditionFailed)
            {Remember(new(identity,null,error,Environment.TickCount64+1000));throw;}
        }
        finally
        {
            if(taken)flight.Gate.Release();
            lock(_folderGate)if(--flight.References==0){_directoryFlights.Remove(key);flight.Gate.Dispose();}
            _directoryAdmission.Release();
        }
    }
    private async Task<CycleFolderOutcome> DispatchDirectory(CycleFolderInput original,CancellationToken ct)
    {
        if(!Supports("folder-batch-v1"))return await EnsureDirectoryIndividualAsync(original,ct);
        var call=new FolderCall(original,ct,new(TaskCreationOptions.RunContinuationsAsynchronously));
        lock(_folderGate){ObjectDisposedException.ThrowIf(_disposeTask is not null,this);_folderPump??=Task.Run(PumpFolders);}
        await _folders.Writer.WriteAsync(call,ct);
        return await call.Completion.Task.WaitAsync(ct);
    }
    private async Task PumpFolders()
    {
        try
        {
            while(await _folders.Reader.WaitToReadAsync(_folderShutdown.Token))
            {
                // The engine has already persisted each individual original.
                // Different parents can share one request; parent dependencies
                // remain in the durable engine planner and never in this queue.
                await Task.Delay(20,_folderShutdown.Token);
                var batch=new List<FolderCall>();var bytes=1024;
                while(batch.Count<500&&_folders.Reader.TryPeek(out var next))
                {
                    var length=System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(LocalCycleWire.FolderObject(next.Input)).Length+64;
                    if(batch.Count>0&&bytes+length>4*1024*1024)break;
                    if(!_folders.Reader.TryRead(out var call))break;
                    if(call.Cancellation.IsCancellationRequested){call.Completion.TrySetCanceled(call.Cancellation);continue;}
                    batch.Add(call);bytes+=length;
                }
                if(batch.Count==0)continue;
                try
                {
                    IReadOnlyList<CycleFolderOutcome?> outcomes;
                    try{outcomes=await EnsureDirectoriesAsync(batch.Select(x=>x.Input).ToArray(),_folderShutdown.Token);}
                    catch(GuardedCycleRemoteException e)when(e.StructuredWireCode&&e.Status is System.Net.HttpStatusCode.NotFound or System.Net.HttpStatusCode.NotImplemented)
                    {outcomes=new CycleFolderOutcome?[batch.Count];}
                    for(var i=0;i<batch.Count;i++)
                    {
                        try{batch[i].Completion.TrySetResult(outcomes[i]??await EnsureDirectoryIndividualAsync(batch[i].Input,_folderShutdown.Token));}
                        catch(Exception e){batch[i].Completion.TrySetException(e);}
                    }
                }
                catch(Exception e){foreach(var call in batch)call.Completion.TrySetException(e);}
            }
        }
        catch(OperationCanceledException)when(_folderShutdown.IsCancellationRequested){}
        finally{while(_folders.Reader.TryRead(out var call))call.Completion.TrySetException(new ObjectDisposedException(nameof(SaturnGuardedCycleTransport)));}
    }
}
