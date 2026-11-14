using System.IO.Compression;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class CompletionBodyTests
{
    [Fact]
    public async Task TwelveTruncatedBodyCyclesPerPathAndCancellationAfterCommit()
    {
        var ct=TestContext.Current.CancellationToken;
        using var port=new TcpListener(IPAddress.Loopback,0);port.Start();var number=((IPEndPoint)port.LocalEndpoint).Port;port.Stop();
        using var listener=new HttpListener();listener.Prefixes.Add($"http://127.0.0.1:{number}/");listener.Start();
        using var stop=CancellationTokenSource.CreateLinkedTokenSource(ct);var mode=0;var calls=0;var committed=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var compressed=new MemoryStream();using(var gzip=new GZipStream(compressed,CompressionLevel.Fastest,true))gzip.Write(Encoding.UTF8.GetBytes("{\"accepted\":true}"));var payload=compressed.ToArray();
        var server=Task.Run(async()=>
        {
            try{while(!stop.IsCancellationRequested)
            {
                var context=await listener.GetContextAsync().WaitAsync(stop.Token);await context.Request.InputStream.CopyToAsync(Stream.Null,stop.Token);Interlocked.Increment(ref calls);
                var response=context.Response;response.StatusCode=200;response.ContentType="application/json";
                if(Volatile.Read(ref mode)==2){response.ContentLength64=100;await response.OutputStream.WriteAsync(new byte[]{(byte)'{'},stop.Token);committed.TrySetResult();await Task.Delay(Timeout.Infinite,stop.Token);}
                else if(Volatile.Read(ref mode)==0){response.Headers["Content-Encoding"]="gzip";response.ContentLength64=payload.Length;await response.OutputStream.WriteAsync(payload.AsMemory(0,payload.Length-4),stop.Token);response.Abort();}
                else{response.ContentLength64=2;await response.OutputStream.WriteAsync(Encoding.UTF8.GetBytes("{}"),stop.Token);response.Close();}
            }}catch(OperationCanceledException)when(stop.IsCancellationRequested){}
        },ct);
        var remote=new WindowsFreshCycleTests.FakeRemote();var origin=new Uri($"http://127.0.0.1:{number}/");
        var config=new WindowsCycleConfiguration("mapping","client","audit","rules-v1",origin,remote.Device,"completion-canary","local-test",remote.Binding);
        using var transport=new SaturnGuardedCycleTransport(config,_=>Task.FromResult("Bearer completion-canary"),TimeSpan.FromSeconds(3));
        var send=typeof(SaturnGuardedCycleTransport).GetMethod("Send",BindingFlags.Instance|BindingFlags.NonPublic)!;
        Task<byte[]> Request(string path,CancellationToken token)=>(Task<byte[]>)send.Invoke(transport,[path,HttpMethod.Post,new StringContent("{}",Encoding.UTF8,"application/json"),4096,token,false])!;
        try
        {
            var paths=new[]{"snapshots/start","catalog-uploads/prepare","catalog-uploads/append","catalog-uploads/complete","mutations/batch","stream-uploads/chunk","small-batches/data"};
            foreach(var path in paths)for(var cycle=0;cycle<12;cycle++)
            {
                Volatile.Write(ref mode,0);var failure=await Record.ExceptionAsync(()=>Request(path,ct));Assert.NotNull(failure);Assert.Equal("WAIT_CONNECTIVITY",SyncRetryController.Classify(failure)?.State);
                Volatile.Write(ref mode,1);Assert.Equal("{}",Encoding.UTF8.GetString(await Request(path,ct)));
            }
            Assert.Equal(paths.Length*24,calls);
            Volatile.Write(ref mode,2);using var cancel=CancellationTokenSource.CreateLinkedTokenSource(ct);var task=Request("catalog-uploads/complete",cancel.Token);await committed.Task.WaitAsync(TimeSpan.FromSeconds(3),ct);cancel.Cancel();await Assert.ThrowsAnyAsync<OperationCanceledException>(async()=>await task);Assert.Equal(paths.Length*24+1,calls);
        }
        finally{stop.Cancel();listener.Stop();await server;}
    }
}
