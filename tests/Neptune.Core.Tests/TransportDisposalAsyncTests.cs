using System.Net;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class TransportDisposalTests
{
    [Fact]
    public async Task AsyncDisposeJoinsBothPumpsAndConcurrentDisposeCalls()
    {
        using var handler=new HeldCancellationHandler();using var http=new HttpClient(handler);
        var config=Config();var transport=Transport(http,config);Configure(transport);using var first=new CancellationTokenSource();using var second=new CancellationTokenSource();
        var folder=Start(transport,config,false,first.Token);var folderGate=await handler.Next(Ct);
        var metadata=Start(transport,config,true,second.Token);var metadataGate=await handler.Next(Ct);
        Task? a=null,b=null,sync=null;
        try
        {
            first.Cancel();second.Cancel();await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>folder);await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>metadata);
            // Cleanup must run even after caller cancellation; all disposal APIs join it.
            a=transport.DisposeAsync().AsTask();b=transport.DisposeAsync().AsTask();sync=Task.Run(transport.Dispose,CancellationToken.None);
            await Task.WhenAll(folderGate.Cancelled.Task,metadataGate.Cancelled.Task).WaitAsync(Bound,Ct);
            folderGate.Release.TrySetResult();await folderGate.Exited.Task.WaitAsync(Bound,Ct);
            await Assert.ThrowsAsync<TimeoutException>(()=>Task.WhenAll(a,b,sync).WaitAsync(TimeSpan.FromMilliseconds(200),Ct));
            metadataGate.Release.TrySetResult();await Task.WhenAll(a,b,sync).WaitAsync(Bound,Ct);
            Assert.True(Pump(transport,false).IsCompleted);Assert.True(Pump(transport,true).IsCompleted);
            await transport.DisposeAsync();transport.Dispose();
        }
        finally
        {
            folderGate.Release.TrySetResult();metadataGate.Release.TrySetResult();
            if(a is null)transport.Dispose();else await Task.WhenAll(a,b!,sync!).WaitAsync(Bound,Ct);
        }
    }

    [Fact]
    public async Task ClosedTransportCannotStartPreviouslyAbsentPumps()
    {
        using var handler=new HeldCancellationHandler();using var http=new HttpClient(handler);
        var config=Config();var transport=Transport(http,config);Configure(transport);
        await transport.DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(()=>Start(transport,config,false,Ct));
        await Assert.ThrowsAsync<ObjectDisposedException>(()=>Start(transport,config,true,Ct));
        Assert.Equal(0,handler.EnteredCount);transport.Dispose();await transport.DisposeAsync();
    }

    [Fact]
    public async Task CompletedFolderBatchKeepsExactOriginalAndNormalResultAcrossAsyncDispose()
    {
        var config=Config();var original=Folder(config);var calls=0;
        using var http=new HttpClient(new ReplyHandler(async request=>
        {
            Interlocked.Increment(ref calls);using var document=JsonDocument.Parse(await request.Content!.ReadAsByteArrayAsync(Ct));
            var row=document.RootElement.GetProperty("items")[0];var input=row.GetProperty("input");
            Assert.Equal(original.OperationId,input.GetProperty("operationId").GetString());Assert.Equal(original.IdempotencyKey,input.GetProperty("idempotencyKey").GetString());Assert.Equal(9,input.EnumerateObject().Count());
            return JsonSerializer.SerializeToUtf8Bytes(new{schema="saturn.folder-batch-result.v1",items=new[]{new{@ref="0",outcome=new{replayed=false,result=new{schema="saturn.catalog-folder-result.v2",operationId=original.OperationId,intentHash=new string('b',64),rootId=original.RootId,status="CREATED",description=new{node_id=Guid.NewGuid().ToString("D"),parent_id=original.ParentId,name=original.Name,type="folder",size_bytes="0",content_sha256=(string?)null,content_version_id=(string?)null,version=Guid.NewGuid().ToString("D"),availability="AVAILABLE"}}}}}});
        }));
        var transport=Transport(http,config);Configure(transport);var result=await transport.EnsureDirectoryAsync(original,Ct);
        Assert.Equal(original.Name,result.Description.Name);await transport.DisposeAsync();Assert.Equal(1,calls);Assert.True(Pump(transport,false).IsCompleted);
    }

}
