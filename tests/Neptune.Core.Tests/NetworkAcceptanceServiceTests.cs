using System.Net;
using System.Net.Http.Json;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class NetworkAcceptanceTests
{
    [Fact]
    public async Task N06_ServiceConnectPreservesHeartbeatRetryAfterAcrossRouteHints()
    {
        var ct = TestContext.Current.CancellationToken;
        var directory = Path.Combine(Path.GetTempPath(), "network-service-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var handler = new RecoveryServiceHandler();
            using var http = new HttpClient(handler);
            await using var service = new WindowsSyncService(new("network-test", directory, false), http);
            var wake = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            service.MappingWorkAvailable += _ => wake.TrySetResult();
            var connection = new WindowsConnection(new Uri("https://kernel.test/"), "fixture-kernel", new string('b', 43), "Assigned", WindowsKernelCredentialKind.NeptuneOutdoorToken);
            var error = await Assert.ThrowsAsync<GuardedCycleRemoteException>(() => service.ConnectAsync("client", connection, ct));
            Assert.Equal(HttpStatusCode.ProxyAuthenticationRequired, error.Status);
            Assert.Equal("Proxy authorization required", service.NetworkState);
            for (var i = 0; i < 100; i++) service.NotifyNetworkChanged();
            await Task.Delay(1100, ct);
            Assert.Equal(1, handler.Heartbeats);
            await wake.Task.WaitAsync(TimeSpan.FromSeconds(4), ct);
            Assert.Equal(2, handler.Heartbeats);
            Assert.Equal("Recovering operations", service.NetworkState);
        }
        finally { Directory.Delete(directory, true); }
    }

    private sealed class RecoveryServiceHandler : HttpMessageHandler
    {
        public int Heartbeats;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            if (request.RequestUri!.AbsolutePath == "/api/v1/outdoor/resolve")
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = JsonContent.Create(new { schema = "exocortex.register.resolution.v1", register_revision = "register-1", values = new Dictionary<string, object> {
                    ["services.saturn.sni"] = new { value = "saturn.test", secret = false }, ["services.saturn.port"] = new { value = "443", secret = false },
                    ["services.saturn.paths.sync"] = new { value = "/webdav/sync/", secret = false }, ["services.saturn.paths.sync_preferences"] = new { value = "/preferences", secret = false }
                } }) });
            Assert.Equal("/api/v1/device-session/heartbeat", request.RequestUri.AbsolutePath);
            Assert.Equal(new string('b', 43), request.Headers.Authorization?.Parameter);
            if (Interlocked.Increment(ref Heartbeats) == 1)
            {
                var refused = new HttpResponseMessage(HttpStatusCode.ProxyAuthenticationRequired) { Content = new StringContent("proxy access required") };
                refused.Headers.RetryAfter = new(TimeSpan.FromSeconds(2));
                return Task.FromResult(refused);
            }
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = JsonContent.Create(new { id = "82b44ad9-6360-45cc-9a0c-a36cce13b559", syncRootId = "d3eeae64-79d8-492b-b4b7-3b535ebd2a48", syncFolderName = "Assigned" }) });
        }
    }
}
