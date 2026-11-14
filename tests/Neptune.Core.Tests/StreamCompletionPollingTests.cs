using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class StreamCompletionPollingTests
{
    [Fact]
    public async Task WindowsMappingForwardsTheOriginalPagedStreamCursor()
    {
        var remote=new WindowsFreshCycleTests.FakeRemote();
        var anchor=new WindowsMappingAnchorRemote(remote,Guid.NewGuid().ToString("D"),remote.Binding.RootId,"mapping");
        var id=Guid.NewGuid().ToString("D");
        var input=new SyncStreamInput(id,"1",new string('a',64),JsonSerializer.SerializeToElement(new{schema="saturn.stream-upload.v2",operationId=id,prepare=new{intent=new{sizeBytes="0"}}}));
        var result=await ((IWindowsStreamRemote)anchor).StreamStatusPageAsync(input,255,TestContext.Current.CancellationToken);
        Assert.True(result.Parallel);Assert.Equal(255,result.AfterIndex);Assert.Equal("RECEIVING",result.State);
    }
    [Theory]
    [InlineData("published")]
    [InlineData("cancel")]
    [InlineData("identity")]
    [InlineData("sha")]
    [InlineData("reconciliation")]
    public async Task PendingCompletionUsesOriginalAndBoundedRequestsWithoutNewUploads(string scenario)
    {
        var ct = TestContext.Current.CancellationToken;
        using var port = new TcpListener(IPAddress.Loopback, 0); port.Start();
        var number = ((IPEndPoint)port.LocalEndpoint).Port; port.Stop();
        using var listener = new HttpListener(); listener.Prefixes.Add($"http://127.0.0.1:{number}/"); listener.Start();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var id = Guid.NewGuid().ToString("D"); var hash = new string('a',64);
        var input = new SyncStreamInput(id,"1",hash,JsonSerializer.SerializeToElement(new{schema="saturn.stream-upload.v1",operationId=id,prepare=new{intent=new{sizeBytes="1"}}}));
        var requests = new List<string>();
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var server = Task.Run(async () => {
            try { while (!stop.IsCancellationRequested) {
                var context = await listener.GetContextAsync().WaitAsync(stop.Token);
                using var body = await JsonDocument.ParseAsync(context.Request.InputStream,cancellationToken:stop.Token);
                Assert.Equal(input.Input.GetRawText(),body.RootElement.GetProperty("input").GetRawText());
                var route = context.Request.Url!.AbsolutePath; requests.Add(route);
                Assert.EndsWith(requests.Count==1?"/seal":"/complete",route);
                var failed = scenario=="reconciliation" && requests.Count==2;
                var done = requests.Count>=3;
                var json = failed ? JsonSerializer.Serialize(new{code="reconciliation_required"}) : JsonSerializer.Serialize(new {
                    schema="saturn.stream-upload-status.v1",operationId=scenario=="identity"&&requests.Count==2?Guid.NewGuid().ToString("D"):id,
                    state=done?"PUBLISHED":"PUBLISHING",receivedSizeBytes="1",
                    chunks=new[]{new{index=0,offset="0",length="1",sha256=hash}},pendingChunk=(object?)null,
                    finalSha256=scenario=="sha"&&requests.Count==2?new string('b',64):hash,
                    publication=done?(object)new{session=new{phase="published"}}:null
                });
                var bytes=Encoding.UTF8.GetBytes(json);context.Response.StatusCode=failed?409:200;
                context.Response.ContentType="application/json";context.Response.ContentLength64=bytes.Length;
                await context.Response.OutputStream.WriteAsync(bytes,stop.Token);context.Response.Close();first.TrySetResult();
            }} catch(OperationCanceledException)when(stop.IsCancellationRequested){}
        },ct);
        var remote = new WindowsFreshCycleTests.FakeRemote();
        var config = new WindowsCycleConfiguration("mapping","client","audit","rules-v1",new Uri($"http://127.0.0.1:{number}/"),remote.Device,"fixture","local-test",remote.Binding);
        // Two one-second polling intervals outlive the per-request 500ms budget.
        using var transport = new SaturnGuardedCycleTransport(config,_=>Task.FromResult("Bearer fixture"),TimeSpan.FromMilliseconds(500));
        transport.ConfigureExtensions(new HashSet<string>{"stream-upload-v1"},null);
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(ct);
        try {
            var completion=transport.StreamCommandAsync("seal",input,hash,caller.Token);
            if(scenario=="cancel") { await first.Task.WaitAsync(TimeSpan.FromSeconds(5),ct);caller.Cancel();await Assert.ThrowsAnyAsync<OperationCanceledException>(async()=>await completion);Assert.Single(requests); }
            else if(scenario is "identity" or "sha") { await Assert.ThrowsAsync<InvalidDataException>(async()=>await completion);Assert.Equal(2,requests.Count); }
            else if(scenario=="reconciliation") { var error=await Assert.ThrowsAsync<GuardedCycleRemoteException>(async()=>await completion);Assert.Equal("reconciliation_required",error.Code);Assert.Equal(2,requests.Count); }
            else { Assert.Equal("PUBLISHED",(await completion).State);Assert.Equal(3,requests.Count); }
        } finally { stop.Cancel();await server;listener.Stop(); }
    }
}
