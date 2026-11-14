using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class CompletionOracleReadbackTests
{
    [Fact]
    public async Task StalledHeadersAreCancelledAndNextConnectionReturnsRealMetadata()
    {
        await using var server = new StallServer(1);
        using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        try
        {
            var result = await CompletionOracleReadback.ReadMetadataAsync(http, server.Origin,
                "windows", "active-vault", cancel.Token, TimeSpan.FromMilliseconds(100), 2)
                .WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            Assert.Equal(2, server.Requests);
            Assert.Equal("actual-response", result.GetProperty("proof").GetString());
        }
        finally { await cancel.CancelAsync(); }
    }

    [Fact]
    public async Task PersistentHeaderStallFailsAfterBoundedAttempts()
    {
        await using var server = new StallServer(int.MaxValue);
        using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        try
        {
            await Assert.ThrowsAsync<TimeoutException>(async () =>
                await CompletionOracleReadback.ReadMetadataAsync(http, server.Origin,
                    "windows", "active-vault", cancel.Token, TimeSpan.FromMilliseconds(100), 2)
                    .WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken));
            Assert.Equal(2, server.Requests);
        }
        finally { await cancel.CancelAsync(); }
    }

    [Fact]
    public async Task CallerCancellationDoesNotRetry()
    {
        await using var server = new StallServer(int.MaxValue);
        using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var pending = CompletionOracleReadback.ReadMetadataAsync(http, server.Origin,
            "windows", "active-vault", cancel.Token, TimeSpan.FromMinutes(2), 3);
        await server.FirstRequest.Task.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        await cancel.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending);
        Assert.Equal(1, server.Requests);
    }

    [Fact]
    public async Task AggregateBodyStallIsBoundedAfterHeadersAndDoesNotBecomeCompletion()
    {
        await using var server = new StallServer(int.MaxValue, headersBeforeStall: true);
        using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
        await Assert.ThrowsAsync<TimeoutException>(() => CompletionOracleReadback.ReadProgressAsync(http,
            server.Origin, "windows", "vault", "root", TestContext.Current.CancellationToken,
            TimeSpan.FromMilliseconds(100), 2));
        Assert.Equal(2, server.Requests);
    }

    [Theory]
    [InlineData("wrong-root", "vault", "2", "1", "6", true)]
    [InlineData("root", "wrong-folder", "2", "1", "6", true)]
    [InlineData("root", "vault", "2", "1", "6", false)]
    [InlineData("root", "vault", "0", "0", "0", true)]
    public async Task AggregateScopeAndInconsistentCountsAreRejected(string rootId, string folder,
        string files, string folders, string bytes, bool found)
    {
        var body=System.Text.Json.JsonSerializer.Serialize(new { schema="saturn.completion-progress.v1",rootId,folder,files,folders,bytes,found });
        await using var server=new StallServer(0,body:body);
        using var http=new HttpClient { Timeout=Timeout.InfiniteTimeSpan };
        await Assert.ThrowsAsync<InvalidDataException>(()=>CompletionOracleReadback.ReadProgressAsync(http,
            server.Origin,"windows","vault","root",TestContext.Current.CancellationToken));
        Assert.Equal(1,server.Requests);
    }

    [Fact]
    public async Task AggregateIsSmallAndExactButHasNoCompletionVerdict()
    {
        await using var server = new StallServer(0, body:"{\"schema\":\"saturn.completion-progress.v1\",\"rootId\":\"root\",\"folder\":\"vault\",\"files\":\"2\",\"folders\":\"1\",\"bytes\":\"9007199254741000\",\"found\":true}");
        using var http = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
        var result = await CompletionOracleReadback.ReadProgressAsync(http,server.Origin,"windows","vault","root",TestContext.Current.CancellationToken);
        Assert.Equal(2,result.Files);
        Assert.Equal(9007199254741000L,result.Bytes);
    }

    [Fact]
    public async Task OversizedAggregateIsRejectedWithoutReadingAnEntireCatalog()
    {
        await using var server=new StallServer(0,body:new string(' ',8192));
        using var http=new HttpClient { Timeout=Timeout.InfiniteTimeSpan };
        await Assert.ThrowsAsync<InvalidDataException>(()=>CompletionOracleReadback.ReadProgressAsync(http,
            server.Origin,"windows","vault","root",TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task MetadataBodyAndFinalPhysicalReadbackHaveDeadlines()
    {
        await using var server=new StallServer(int.MaxValue,headersBeforeStall:true);
        using var http=new HttpClient { Timeout=Timeout.InfiniteTimeSpan };
        await Assert.ThrowsAsync<TimeoutException>(()=>CompletionOracleReadback.ReadMetadataAsync(http,
            server.Origin,"windows","vault",TestContext.Current.CancellationToken,TimeSpan.FromMilliseconds(100),1));
        // Exercise each body deadline on its own connection pool. Draining the
        // cancelled metadata response must not consume the physical request's
        // entire budget before that independent observation reaches the server.
        using var physicalHttp=new HttpClient { Timeout=Timeout.InfiniteTimeSpan };
        await Assert.ThrowsAsync<TimeoutException>(()=>CompletionOracleReadback.ReadPhysicalAsync(physicalHttp,
            server.Origin,"windows","vault",TestContext.Current.CancellationToken,TimeSpan.FromMilliseconds(100)));
        Assert.Equal(2,server.Requests);
    }

    [Fact]
    public async Task OldReceiverFailsClearlyWithoutFallbackOrRetry()
    {
        await using var server=new StallServer(0,statusCode:404);
        using var http=new HttpClient { Timeout=Timeout.InfiniteTimeSpan };
        var error=await Assert.ThrowsAsync<HttpRequestException>(()=>CompletionOracleReadback.ReadProgressAsync(http,
            server.Origin,"windows","vault","root",TestContext.Current.CancellationToken));
        Assert.Equal(HttpStatusCode.NotFound,error.StatusCode);
        Assert.Equal(1,server.Requests);
    }

    [Fact]
    public async Task TruncatedMetadataBodyRetriesTheIndependentObservation()
    {
        await using var server=new StallServer(1,truncateBody:true);
        using var http=new HttpClient { Timeout=Timeout.InfiniteTimeSpan };
        var result=await CompletionOracleReadback.ReadMetadataAsync(http,server.Origin,"windows","vault",TestContext.Current.CancellationToken);
        Assert.Equal("actual-response",result.GetProperty("proof").GetString());
        Assert.Equal(2,server.Requests);
    }

    private sealed class StallServer : IAsyncDisposable
    {
        private readonly TcpListener listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource stop = new();
        private readonly ConcurrentBag<Task> connections = new();
        private readonly Task accepting;
        private readonly int stalls;
        private readonly bool headersBeforeStall;
        private readonly string body;
        private readonly int statusCode;
        private readonly bool truncateBody;
        private int requests;
        internal int Requests => Volatile.Read(ref requests);
        internal TaskCompletionSource FirstRequest { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal Uri Origin { get; }

        internal StallServer(int stalls, bool headersBeforeStall = false, string body = "{\"proof\":\"actual-response\",\"files\":[],\"folders\":[]}", int statusCode = 200, bool truncateBody = false)
        {
            this.stalls = stalls;
            this.headersBeforeStall = headersBeforeStall;
            this.body = body;
            this.statusCode = statusCode;
            this.truncateBody = truncateBody;
            listener.Start();
            Origin = new Uri($"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}/");
            accepting = Accept();
        }

        private async Task Accept()
        {
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    var client = await listener.AcceptTcpClientAsync(stop.Token);
                    connections.Add(Respond(client));
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        }

        private async Task Respond(TcpClient client)
        {
            using (client)
            {
                var stream = client.GetStream();
                var buffer = new byte[4096];
                if (await stream.ReadAsync(buffer, stop.Token) == 0) return;
                var number = Interlocked.Increment(ref requests);
                FirstRequest.TrySetResult();
                if (number <= stalls)
                {
                    if (headersBeforeStall||truncateBody) await stream.WriteAsync(Encoding.UTF8.GetBytes("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{"),stop.Token);
                    if(truncateBody)return;
                    await Task.Delay(Timeout.InfiniteTimeSpan, stop.Token);
                }
                var reply = Encoding.UTF8.GetBytes($"HTTP/1.1 {statusCode} Test\r\nContent-Type: application/json\r\nContent-Length: {Encoding.UTF8.GetByteCount(body)}\r\nConnection: close\r\n\r\n{body}");
                await stream.WriteAsync(reply, stop.Token);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await stop.CancelAsync();
            await accepting;
            listener.Stop();
            try { await Task.WhenAll(connections); }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
            stop.Dispose();
        }
    }
}
