using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;
public sealed partial class SaturnGuardedCycleTransport
{
    private sealed record MutationCall(SyncMutationRequest Original,CancellationToken Cancellation,TaskCompletionSource<SyncMutationReceipt> Completion);
    private readonly Channel<MutationCall> _mutations=Channel.CreateBounded<MutationCall>(new BoundedChannelOptions(500){SingleReader=true,FullMode=BoundedChannelFullMode.Wait});
    private Task? _mutationPump;
    private async Task<SyncMutationReceipt> QueueMetadataMutation(SyncMutationRequest original,CancellationToken ct)
    {
        var call=new MutationCall(original,ct,new(TaskCreationOptions.RunContinuationsAsynchronously));
        lock(_folderGate){ObjectDisposedException.ThrowIf(_disposeTask is not null,this);_mutationPump??=Task.Run(PumpMetadata);}
        await _mutations.Writer.WriteAsync(call,ct);return await call.Completion.Task.WaitAsync(ct);
    }
    private async Task PumpMetadata()
    {
        try
        {
            while(await _mutations.Reader.WaitToReadAsync(_folderShutdown.Token))
            {
                await Task.Delay(20,_folderShutdown.Token);
                var batch=new List<MutationCall>();var operations=new HashSet<string>(StringComparer.Ordinal);var bytes=1024;
                while(batch.Count<500&&_mutations.Reader.TryPeek(out var next))
                {
                    var length=Encoding.UTF8.GetByteCount(next.Original.Input.GetRawText())+128;
                    if(batch.Count>0&&(bytes+length>4*1024*1024||operations.Contains(next.Original.OperationId)))break;
                    if(!_mutations.Reader.TryRead(out var call))break;
                    if(call.Cancellation.IsCancellationRequested){call.Completion.TrySetCanceled(call.Cancellation);continue;}
                    operations.Add(call.Original.OperationId);batch.Add(call);bytes+=length;
                }
                if(batch.Count==0)continue;
                try
                {
                    var policy=batch[0].Original.Namespace;
                    if(batch.Any(item=>item.Original.Namespace!=policy))throw new InvalidDataException("Metadata batch mixed namespace policies.");
                    var raw=await Send("mutations/batch",HttpMethod.Post,Json(new{binding=LocalCycleWire.BindingObject(Binding),@namespace=new{codec=policy.Codec,policyHash=policy.PolicyHash},items=batch.Select((item,index)=>new{@ref=index.ToString(System.Globalization.CultureInfo.InvariantCulture),input=item.Original.Input})},4*1024*1024),4*1024*1024,_folderShutdown.Token);
                    using var doc=SyncProtocolReader.Parse(raw);var outcomes=doc.RootElement.GetProperty("outcomes");
                    if(outcomes.ValueKind!=JsonValueKind.Array||outcomes.GetArrayLength()!=batch.Count)throw new InvalidDataException("Metadata batch membership changed.");
                    for(var i=0;i<batch.Count;i++)
                    {
                        var item=outcomes[i];if(item.GetProperty("ref").GetString()!=i.ToString(System.Globalization.CultureInfo.InvariantCulture))throw new InvalidDataException("Metadata batch order changed.");
                        if(item.TryGetProperty("outcome",out var outcome))batch[i].Completion.TrySetResult(SyncMutationWire.Read(Encoding.UTF8.GetBytes(outcome.GetRawText()),batch[i].Original));
                        else
                        {
                            var code=item.GetProperty("code").GetString()??throw new InvalidDataException("Metadata failure code missing.");
                            var status=code switch{"precondition_failed"=>HttpStatusCode.PreconditionFailed,"unauthorized"=>HttpStatusCode.Unauthorized,"forbidden"=>HttpStatusCode.Forbidden,"admission_full" or "capacity"=>HttpStatusCode.TooManyRequests,"backend_busy" or "backend_draining"=>HttpStatusCode.ServiceUnavailable,_=>HttpStatusCode.Conflict};
                            batch[i].Completion.TrySetException(new GuardedCycleRemoteException(status,code,true));
                        }
                    }
                }
                catch(Exception error){foreach(var item in batch)item.Completion.TrySetException(error);}
            }
        }
        catch(OperationCanceledException)when(_folderShutdown.IsCancellationRequested){}
        finally{while(_mutations.Reader.TryRead(out var call))call.Completion.TrySetException(new ObjectDisposedException(nameof(SaturnGuardedCycleTransport)));}
    }
}
