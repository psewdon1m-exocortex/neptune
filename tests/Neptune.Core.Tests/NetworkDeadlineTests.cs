using System.Net;
using System.Net.Sockets;
using System.Text;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class NetworkDeadlineTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task HeadersWithoutBodyReleaseGuardedOriginSlots()
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        using var listener = new TcpListener(IPAddress.Loopback, 0); listener.Start();
        var origin = new Uri($"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}/");
        var headersSent = 0;
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // Keep the deliberately stalled loopback peer off the shared pool.
        // The short client deadline must test a response body, not whether a
        // queued Task.Run gets scheduled before an already-cancelled socket.
        var server = Task.Factory.StartNew(() =>
        {
            ready.SetResult();
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    using var peer = listener.AcceptTcpClient();
                    var stream = peer.GetStream();
                    var buffer = new byte[8192];
                    if (stream.Read(buffer) == 0) continue;
                    stream.Write(Encoding.ASCII.GetBytes("HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{"));
                    Interlocked.Increment(ref headersSent);
                    try { while (stream.Read(buffer) != 0) { } } catch (IOException) { }
                }
            }
            catch (SocketException) when (stop.IsCancellationRequested) { }
            catch (ObjectDisposedException) when (stop.IsCancellationRequested) { }
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        await ready.Task;
        try
        {
            var remote = new WindowsFreshCycleTests.FakeRemote();
            var config = new WindowsCycleConfiguration("mapping", "client", "audit", "rules-v1", origin, remote.Device, "test-only-credential", "local-test", remote.Binding);
            using var transport = new SaturnGuardedCycleTransport(config, _ => Task.FromResult("Bearer test"), TimeSpan.FromMilliseconds(200));
            // More than all eight origin slots: each timed out body must release its slot.
            for (var i = 0; i < 9; i++)
                await Assert.ThrowsAsync<TimeoutException>(() => transport.StartSnapshotAsync(Guid.NewGuid().ToString("D"), Ct));
        }
        finally
        {
            stop.Cancel(); listener.Stop(); await server;
        }
        Assert.Equal(9, Volatile.Read(ref headersSent));
    }

    [Fact]
    public async Task ProgressingTransferOutlivesControlBudget()
    {
        using var http = new HttpClient(new Handler((request, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
        { RequestMessage = request, Content = new StreamContent(new SlowBody(12, TimeSpan.FromMilliseconds(30))) }))) { Timeout = Timeout.InfiniteTimeSpan };
        using var request = new HttpRequestMessage(HttpMethod.Put, "https://saturn.test/dav/file") { Content = new ByteArrayContent([1]) };
        using var response = await SyncHttpPolicy.SendAsync(http, request, HttpCompletionOption.ResponseHeadersRead, Ct,
            controlTimeout: TimeSpan.FromMilliseconds(80), idleTimeout: TimeSpan.FromMilliseconds(200));
        Assert.Equal(12, (await response.Content.ReadAsByteArrayAsync(Ct)).Length);
    }

    [Fact]
    public async Task StalledUploadTimesOutWithoutReplay()
    {
        var calls = 0;
        using var http = new HttpClient(new Handler(async (request, ct) =>
        {
            calls++;
            await request.Content!.CopyToAsync(new BlockedWriter(), ct);
            return new(HttpStatusCode.OK);
        })) { Timeout = Timeout.InfiniteTimeSpan };
        using var request = new HttpRequestMessage(HttpMethod.Put, "https://saturn.test/dav/file") { Content = new ByteArrayContent(new byte[65536]) };
        await Assert.ThrowsAsync<TimeoutException>(() => SyncHttpPolicy.SendAsync(http, request, HttpCompletionOption.ResponseHeadersRead, Ct, idleTimeout: TimeSpan.FromMilliseconds(100)));
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task CallerCancellationRemainsCancellation()
    {
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        using var http = new HttpClient(new Handler(async (_, ct) => { await Task.Delay(Timeout.InfiniteTimeSpan, ct); return new(HttpStatusCode.OK); }));
        using var request = new HttpRequestMessage(HttpMethod.Get, "https://saturn.test/control");
        caller.CancelAfter(50);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => SyncHttpPolicy.SendAsync(http, request, HttpCompletionOption.ResponseHeadersRead, caller.Token));
    }

    private sealed class Handler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send) : HttpMessageHandler
    { protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct) => send(request, ct); }
    private sealed class SlowBody(int remaining, TimeSpan delay) : MemoryStream
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        { if (remaining-- <= 0) return 0; await Task.Delay(delay, ct); buffer.Span[0] = 1; return 1; }
    }
    private sealed class BlockedWriter : MemoryStream
    {
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
            => await Task.Delay(Timeout.InfiniteTimeSpan, ct);
    }
}
