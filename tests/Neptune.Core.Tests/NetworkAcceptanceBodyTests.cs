using System.Net;
using System.Net.Sockets;
using System.Text;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class NetworkAcceptanceTests
{
    [Theory]
    [InlineData(429)] [InlineData(503)] [InlineData(407)]
    public async Task N06_LegacyMutationPreservesRetryAfter(int status)
    {
        var path = Path.Combine(Path.GetTempPath(), "network-retry-" + Guid.NewGuid().ToString("N"));
        var ct = TestContext.Current.CancellationToken;
        await File.WriteAllTextAsync(path, "original", ct);
        try
        {
            using var http = new HttpClient(new RetryHeaderHandler(status));
            var error = await Record.ExceptionAsync(() => new WebDavSyncClient(http).UploadAsync(new Uri("https://saturn.test/dav/"), "fixture", "file", path, ct));
            Assert.IsType<GuardedCycleRemoteException>(error);
            Assert.Equal(TimeSpan.FromSeconds(2), ((GuardedCycleRemoteException)error).RetryAfter);
            Assert.Equal(status, (int)((GuardedCycleRemoteException)error).Status);
        }
        finally { File.Delete(path); }
    }
    private sealed class RetryHeaderHandler(int status) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var response = new HttpResponseMessage(request.Method == HttpMethod.Head ? HttpStatusCode.NotFound : (HttpStatusCode)status);
            response.Headers.TryAddWithoutValidation("Retry-After", "2");
            return Task.FromResult(response);
        }
    }
    [Fact]
    public void N06_RetryAfterDateAndInvalidValues()
    {
        using var response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
        response.Headers.TryAddWithoutValidation("Retry-After", "2"); Assert.Equal(TimeSpan.FromSeconds(2), NetworkFailure.RetryAfter(response));
        response.Headers.Remove("Retry-After"); response.Headers.TryAddWithoutValidation("Retry-After", DateTimeOffset.UtcNow.AddSeconds(10).ToString("R"));
        Assert.InRange(NetworkFailure.RetryAfter(response)!.Value.TotalSeconds, 8, 10);
        response.Headers.Remove("Retry-After"); response.Headers.TryAddWithoutValidation("Retry-After", DateTimeOffset.UtcNow.AddDays(-1).ToString("R")); Assert.Equal(TimeSpan.Zero, NetworkFailure.RetryAfter(response));
        response.Headers.Remove("Retry-After"); response.Headers.TryAddWithoutValidation("Retry-After", "invalid"); Assert.Null(NetworkFailure.RetryAfter(response));
    }

    [Fact]
    public void N02_PrematureBodyEndIsConnectivityButMalformedProtocolIsNot()
    {
        var error = new HttpIOException(HttpRequestError.ResponseEnded, "premature EOF");
        Assert.Equal("Waiting for network", NetworkFailure.State(error));
        Assert.Equal("WAIT_CONNECTIVITY", SyncRetryController.Classify(error)?.State);
        Assert.Null(NetworkFailure.State(new InvalidDataException("complete but malformed JSON")));
    }

    [Fact]
    public async Task N13_TwelveSaturatedOriginRecoveriesCancelNinthAdmission()
    {
        var ct = TestContext.Current.CancellationToken;
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
        var peers = new List<Task>(); var accepted = 0;
        var server = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    var peer = await listener.AcceptTcpClientAsync(stop.Token); Interlocked.Increment(ref accepted);
                    peers.Add(Task.Run(async () =>
                    {
                        using (peer)
                        {
                            try
                            {
                                var stream = peer.GetStream(); var buffer = new byte[8192];
                                Assert.True(await stream.ReadAsync(buffer, stop.Token) > 0);
                                await stream.WriteAsync(Encoding.ASCII.GetBytes("HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{"), stop.Token);
                                while (await stream.ReadAsync(buffer, stop.Token) > 0) { }
                            }
                            catch (IOException) { }
                            catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
                        }
                    }, ct));
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        }, ct);
        try
        {
            var remote = new WindowsFreshCycleTests.FakeRemote();
            var config = new WindowsCycleConfiguration("mapping", "client", "audit", "rules-v1", new Uri($"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}/"), remote.Device, "fixture", "local-test", remote.Binding);
            using var transport = new SaturnGuardedCycleTransport(config, _ => Task.FromResult("Bearer fixture"), TimeSpan.FromSeconds(4));
            for (var cycle = 0; cycle < 12; cycle++)
            {
                using var batch = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var tasks = Enumerable.Range(0, 8).Select(_ => transport.StartSnapshotAsync(Guid.NewGuid().ToString("D"), batch.Token)).ToArray();
                using var barrier = CancellationTokenSource.CreateLinkedTokenSource(ct); barrier.CancelAfter(TimeSpan.FromSeconds(3));
                while (Volatile.Read(ref accepted) < (cycle + 1) * 8) await Task.Delay(5, barrier.Token);
                using var ninth = CancellationTokenSource.CreateLinkedTokenSource(ct); ninth.CancelAfter(20);
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => transport.StartSnapshotAsync(Guid.NewGuid().ToString("D"), ninth.Token));
                Assert.Equal((cycle + 1) * 8, Volatile.Read(ref accepted));
                batch.Cancel();
                foreach (var task in tasks) await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await task);
            }
        }
        finally { stop.Cancel(); await server; await Task.WhenAll(peers); }
    }

    [Theory]
    [InlineData(401, false)] [InlineData(403, false)] [InlineData(407, false)] [InlineData(429, false)] [InlineData(503, false)]
    [InlineData(401, true)] [InlineData(403, true)] [InlineData(407, true)] [InlineData(429, true)] [InlineData(503, true)]
    public async Task N06_GuardedErrorStatusAndRetryAfterSurviveBrokenBody(int status, bool oversized)
    {
        var ct = TestContext.Current.CancellationToken;
        using var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
        var server = Task.Run(async () =>
        {
            using var peer = await listener.AcceptTcpClientAsync(ct);
            var stream = peer.GetStream(); var buffer = new byte[8192];
            Assert.True(await stream.ReadAsync(buffer, ct) > 0);
            await stream.WriteAsync(Encoding.ASCII.GetBytes($"HTTP/1.1 {status} Error\r\nContent-Length: {(oversized ? 32768 : 100)}\r\nRetry-After: 2\r\nConnection: close\r\n\r\n<html>"), ct);
        }, ct);
        var remote = new WindowsFreshCycleTests.FakeRemote();
        var config = new WindowsCycleConfiguration("mapping", "client", "audit", "rules-v1", new Uri($"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}/"), remote.Device, "fixture", "local-test", remote.Binding);
        using var transport = new SaturnGuardedCycleTransport(config, _ => Task.FromResult("Bearer fixture"), TimeSpan.FromSeconds(2));
        var error = await Assert.ThrowsAsync<GuardedCycleRemoteException>(() => transport.StartSnapshotAsync(Guid.NewGuid().ToString("D"), ct));
        Assert.Equal(status, (int)error.Status); Assert.Equal(TimeSpan.FromSeconds(2), error.RetryAfter); Assert.False(error.StructuredWireCode);
        await server;
    }
}
