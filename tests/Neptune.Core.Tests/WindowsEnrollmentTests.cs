using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class WindowsEnrollmentTests
{
    [Theory]
    [InlineData("Office PC")]
    [InlineData("Домашний ПК")]
    public async Task UsesServerAssignedFolderForEnrollmentAndEveryDavRequest(string folder)
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-windows-enrollment-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var mappingDirectory = Path.Combine(directory, "Documents"); Directory.CreateDirectory(mappingDirectory);
        using var handler = new Handler(folder);
        using var http = new HttpClient(handler);
        var service = new WindowsSyncService(new WindowsProfileContext("test", directory, false), http);
        var connection = new WindowsConnection(new Uri("https://kernel.test"), "kernel-fixture", new string('a', 32), "caller-selected-folder");
        try
        {
            var result = await service.ConnectAsync("client-id", connection, TestContext.Current.CancellationToken);
            Assert.Equal(folder, result.Connection.RemoteFolder);
            Assert.Equal(new string('b', 43), result.Connection.SaturnToken);
            Assert.Empty(handler.DavRequests);
            var mapping = new SyncMapping("mapping-id", "client-id", mappingDirectory, true, DateTimeOffset.UtcNow);
            await service.SyncAsync("client-id", [mapping], result.Connection, cancellationToken: TestContext.Current.CancellationToken);
            Assert.NotEmpty(handler.DavRequests);
            Assert.All(handler.DavRequests, uri => Assert.StartsWith("/webdav/sync/" + Uri.EscapeDataString(folder) + "/Documents/", uri.AbsolutePath));
        }
        finally { Directory.Delete(directory, true); }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("../other")]
    [InlineData("sync/other")]
    [InlineData("..")]
    public async Task FailsBeforeDavWhenServerHasNoSafeFolderAssignment(string? folder)
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-windows-unbound-" + Guid.NewGuid().ToString("N"));
        using var handler = new Handler(folder); using var http = new HttpClient(handler);
        var service = new WindowsSyncService(new WindowsProfileContext("test", directory, false), http);
        try
        {
            await Assert.ThrowsAsync<InvalidDataException>(() => service.ConnectAsync("client", new WindowsConnection(new Uri("https://kernel.test"), "fixture", new string('b', 43), "old-folder"), TestContext.Current.CancellationToken));
            Assert.Empty(handler.DavRequests);
        }
        finally { if (Directory.Exists(directory)) Directory.Delete(directory, true); }
    }

    [Fact]
    public async Task UsesBoundedMetadataCacheDuringKernelOutageButStopsOnAuthorizationRejection()
    {
        var directory=Path.Combine(Path.GetTempPath(),"neptune-cache-"+Guid.NewGuid().ToString("N"));
        using var handler=new Handler("Assigned");using var http=new HttpClient(handler);
        var service=new WindowsSyncService(new WindowsProfileContext("test",directory,false),http);
        var connection=new WindowsConnection(new Uri("https://kernel.test"),"fixture",new string('b',43),"Assigned");
        try {
            await service.ConnectAsync("client",connection,TestContext.Current.CancellationToken);
            handler.KernelFailure=HttpStatusCode.ServiceUnavailable;
            Assert.Equal("Assigned",(await service.ConnectAsync("client",connection,TestContext.Current.CancellationToken)).Connection.RemoteFolder);
            await Assert.ThrowsAsync<HttpRequestException>(()=>service.ConnectAsync("client",connection with{KernelToken="different"},TestContext.Current.CancellationToken));
            handler.KernelFailure=HttpStatusCode.Forbidden;
            await Assert.ThrowsAsync<HttpRequestException>(()=>service.ConnectAsync("client",connection,TestContext.Current.CancellationToken));
            handler.KernelFailure=HttpStatusCode.ServiceUnavailable;
            await Assert.ThrowsAsync<HttpRequestException>(()=>service.ConnectAsync("client",connection,TestContext.Current.CancellationToken));
        } finally {if(Directory.Exists(directory))Directory.Delete(directory,true);}
    }

    private sealed class Handler(string? folder) : HttpMessageHandler
    {
        public HttpStatusCode? KernelFailure {get;set;}
        public List<Uri> DavRequests { get; } = [];
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var route = request.RequestUri!.AbsolutePath;
            if (request.RequestUri.Host=="kernel.test" && KernelFailure is { } failure) return Task.FromResult(new HttpResponseMessage(failure));
            HttpResponseMessage Json(object body) => new(HttpStatusCode.OK) { Content = JsonContent.Create(body) };
            if (route == "/api/v1/register/snapshot")
            {
                using var values = JsonDocument.Parse("""{"services":{"saturn":{"sni":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/1","port":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/2","paths":{"sync":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/3","sync_preferences":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/4"}}}}""");
                return Task.FromResult(Json(new { schema = "exocortex.register.snapshot.v1", revision = 1, checksum = CanonicalJson.ComputeRegisterChecksum(values.RootElement), values = values.RootElement.Clone() }));
            }
            if (route == "/api/v1/register/resolve") return Task.FromResult(Json(new { schema = "exocortex.register.resolution.v1", values = new Dictionary<string, object> {
                ["services.saturn.sni"] = new { value = "saturn.test" }, ["services.saturn.port"] = new { value = "443" },
                ["services.saturn.paths.sync"] = new { value = "/webdav/sync/" }, ["services.saturn.paths.sync_preferences"] = new { value = "/preferences" }
            } }));
            if (route == "/api/v1/device-enrollments/redeem") return Task.FromResult(Json(new { token = new string('b', 43) }));
            if (route == "/api/v1/device-session/heartbeat")
            {
                Assert.Equal(new string('b', 43), request.Headers.Authorization?.Parameter);
                return Task.FromResult(Json(new { syncRootId = Guid.NewGuid().ToString(), syncFolderName = folder }));
            }
            if (route == "/preferences") return Task.FromResult(Json(new { accentColor = "#00A8FF" }));
            if (route.StartsWith("/webdav/", StringComparison.Ordinal))
            {
                DavRequests.Add(request.RequestUri);
                Assert.Equal(new string('b', 43), request.Headers.Authorization?.Parameter);
                return Task.FromResult(request.Method.Method == "PROPFIND"
                    ? new HttpResponseMessage((HttpStatusCode)207) { Content = new StringContent("<d:multistatus xmlns:d=\"DAV:\" />") }
                    : new HttpResponseMessage(HttpStatusCode.Created));
            }
            throw new InvalidOperationException("Unexpected request: " + request);
        }
    }
}
