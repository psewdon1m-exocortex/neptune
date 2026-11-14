using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class WindowsGuardedBridgeTests
{
    private const string Device = "82b44ad9-6360-45cc-9a0c-a36cce13b559";
    private const string Root = "d3eeae64-79d8-492b-b4b7-3b535ebd2a48";
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static JsonObject Capability() => JsonNode.Parse("""
        {"schema":"saturn.sync-capabilities.v1","binding":{"protocol":"neptune-sync/1","descriptor_schema":"saturn.current-namespace.v1","server_identity":"own-server","root_id":"d3eeae64-79d8-492b-b4b7-3b535ebd2a48","server_epoch":"own-epoch","policy_version":"own-policy","binding_version":"1d3b6597-ff5b-4261-9a4f-baa1c6203243"},"device_id":"82b44ad9-6360-45cc-9a0c-a36cce13b559","backend_identity":"own-backend","qualification_receipt_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","coverage":"append-only-directories/file-cas","features":["snapshot-v1","changes-v1","folder-v2","catalog-upload-v2"],"limits":{"data_jobs":4,"large_jobs":2,"spool_bytes":"1073741824","maximum_file_bytes":"1073741824"}}
        """)!.AsObject();
    [Fact]
    public void ExactCapabilitiesBindAssignedScopeAndConservativeLocalCaptureLimit()
    {
        var json = Capability(); json["limits"]!["maximum_file_bytes"] = "524288";
        var result = WindowsGuardedCapabilities.Parse(Encoding.UTF8.GetBytes(json.ToJsonString()), Device, Root);
        Assert.Equal(Root, result.Binding.RootId); Assert.Equal(4, result.Budget.DataJobs);
        Assert.Equal(2, result.Budget.LargeJobs); Assert.Equal(1073741824, result.Budget.SpoolBytes);
        Assert.Equal(524288, result.Budget.MaximumNewFileBytes);
        Assert.Equal(524288, result.MaximumFileBytes);
    }
    [Theory]
    [InlineData("root")]
    [InlineData("device")]
    [InlineData("coverage")]
    [InlineData("jobs")]
    [InlineData("large")]
    [InlineData("spool")]
    [InlineData("file")]
    [InlineData("feature")]
    [InlineData("extra")]
    public void RefusesRebindUnsupportedFeaturesAndUnboundedResourceLimits(string change)
    {
        var json = Capability();
        switch (change)
        {
            case "root": json["binding"]!["root_id"] = Device; break;
            case "device": json["device_id"] = Root; break;
            case "coverage": json["coverage"] = "mirror-all"; break;
            case "jobs": json["limits"]!["data_jobs"] = 5; break;
            case "large": json["limits"]!["large_jobs"] = 3; break;
            case "spool": json["limits"]!["spool_bytes"] = "1073741825"; break;
            case "file": json["limits"]!["maximum_file_bytes"] = "1073741825"; break;
            case "feature": json["features"]![3] = "folder-v2"; break;
            case "extra": json["trusted"] = true; break;
        }
        Assert.ThrowsAny<Exception>(() => WindowsGuardedCapabilities.Parse(Encoding.UTF8.GetBytes(json.ToJsonString()), Device, Root));
    }
    [Fact]
    public void ScaledCapabilityDecouplesLargeFilesFromBoundedSpoolAndRequiresMatchingProtocol()
    {
        var json=Capability();json["features"]!.AsArray().Add("stream-upload-v1");json["features"]!.AsArray().Add("scale-transfer-v2");
        json["extensions"]=JsonNode.Parse("""
            {"revisions":{"K1":2,"K2":2,"K3":2,"K4":1},"namespace":{"codec":"portable_v1","policyHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"minimum_writer":"saturn.namespace-mutation.v1","backend_policy":"managed-exclusive","limits":{"metadata_items":500,"metadata_bytes":4194304,"long_poll_ms":25000,"gzip_threshold_bytes":16384,"stream_chunk_bytes":4194304,"stream_maximum_chunks":2147483648}}
            """);
        json["limits"]!["data_jobs"]=8;json["limits"]!["maximum_file_bytes"]="68719476736";
        var result=WindowsGuardedCapabilities.Parse(Encoding.UTF8.GetBytes(json.ToJsonString()),Device,Root);
        Assert.Equal(8,result.Budget.DataJobs);Assert.Equal(1073741824,result.Budget.SpoolBytes);Assert.Equal(68719476736,result.MaximumFileBytes);
        json["extensions"]!["limits"]!["stream_maximum_chunks"]=256;
        Assert.Throws<InvalidDataException>(()=>WindowsGuardedCapabilities.Parse(Encoding.UTF8.GetBytes(json.ToJsonString()),Device,Root));
    }
    [Fact]
    public void RefusesDuplicateJsonPropertiesAndOversizeNegotiation()
    {
        var json = Capability().ToJsonString();
        Assert.ThrowsAny<Exception>(() => WindowsGuardedCapabilities.Parse(Encoding.UTF8.GetBytes(json[..^1] + ",\"schema\":\"saturn.sync-capabilities.v1\"}"), Device, Root));
        Assert.Throws<InvalidDataException>(() => WindowsGuardedCapabilities.Parse(new byte[16385], Device, Root));
    }
    [Fact]
    public async Task OnlyPreEnrollment404AllowsLegacyAndDurableMarkerSurvivesDowngrade()
    {
        await using var fixture = new Fixture();
        fixture.Handler.Status = HttpStatusCode.NotFound;
        Assert.Null(await fixture.Negotiate());
        Assert.False(Directory.Exists(Path.Combine(fixture.Directory, "guarded-sync")));
        fixture.Handler.Status = HttpStatusCode.OK;
        Assert.NotNull(await fixture.Negotiate());
        var marker = Path.Combine(fixture.Directory, "guarded-sync", WindowsGuardedCapabilities.NamespaceKey(fixture.Origin, Device, Root) + ".json");
        var before = await File.ReadAllBytesAsync(marker, Ct);
        fixture.Handler.Status = HttpStatusCode.NotFound;
        await Assert.ThrowsAsync<HttpRequestException>(() => fixture.Negotiate());
        Assert.Equal(before, await File.ReadAllBytesAsync(marker, Ct));
    }
    [Fact]
    public async Task MissingMarkerWithRetainedEvidenceRefusesBeforeNetwork()
    {
        await using var fixture = new Fixture();
        Directory.CreateDirectory(Path.Combine(fixture.Directory, "guarded-sync", WindowsGuardedCapabilities.NamespaceKey(fixture.Origin, Device, Root), "owned-mapping"));
        await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Negotiate());
        Assert.Equal(0, fixture.Handler.Requests);
    }
    [Theory]
    [InlineData(403)]
    [InlineData(409)]
    [InlineData(503)]
    [InlineData(307)]
    [InlineData(308)]
    public async Task RejectedAdmissionAndRedirectNeverEnableLegacy(int status)
    {
        await using var fixture = new Fixture(); fixture.Handler.Status = (HttpStatusCode)status;
        await Assert.ThrowsAnyAsync<Exception>(() => fixture.Negotiate());
        Assert.Equal(1, fixture.Handler.Requests);
        Assert.False(Directory.Exists(Path.Combine(fixture.Directory, "guarded-sync")));
    }
    [Fact]
    public async Task ChangedResponseRouteRefusesBeforePersistingEnrollment()
    {
        await using var fixture = new Fixture(); fixture.Handler.ChangeResponseUri = true;
        await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Negotiate());
        Assert.False(Directory.Exists(Path.Combine(fixture.Directory, "guarded-sync")));
    }
    [Fact]
    public async Task ActualWindowsProfileReplacementIsDetectedBeforeEnrollmentWrites()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var fixture = new Fixture();
        fixture.Handler.BeforeResponse = () =>
        {
            System.IO.Directory.Move(fixture.Directory, fixture.Directory + "-moved");
            System.IO.Directory.CreateDirectory(fixture.Directory);
        };
        await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Negotiate());
        Assert.False(System.IO.Directory.Exists(Path.Combine(fixture.Directory, "guarded-sync")));
        Assert.False(System.IO.Directory.Exists(Path.Combine(fixture.Directory + "-moved", "guarded-sync")));
    }
    private sealed class Fixture : IAsyncDisposable
    {
        internal string Directory { get; } = Path.Combine(Path.GetTempPath(), "neptune-guarded-bridge-" + Guid.NewGuid().ToString("N"));
        internal Uri Origin { get; } = new("https://owned-saturn.invalid");
        internal Handler Handler { get; } = new();
        private readonly HttpClient _http;
        internal Fixture() { System.IO.Directory.CreateDirectory(Directory); _http = new(Handler); }
        internal Task<WindowsGuardedCapability?> Negotiate() => WindowsGuardedCapabilities.NegotiateAsync(_http, Origin, new string('b', 43), Device, Root, Directory, Ct);
        public ValueTask DisposeAsync()
        {
            _http.Dispose();
            foreach (var own in new[] { Directory, Directory + "-moved" })
            {
                if (Path.GetDirectoryName(Path.GetFullPath(own)) != Path.TrimEndingDirectorySeparator(Path.GetFullPath(Path.GetTempPath())))
                    throw new InvalidOperationException("Own test cleanup escaped its temporary parent.");
                if (System.IO.Directory.Exists(own)) System.IO.Directory.Delete(own, true);
            }
            return ValueTask.CompletedTask;
        }
    }
    private sealed class Handler : HttpMessageHandler
    {
        internal HttpStatusCode Status = HttpStatusCode.OK;
        internal bool ChangeResponseUri;
        internal Action? BeforeResponse;
        internal int Requests;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests++; Assert.Equal(HttpMethod.Get, request.Method);
            Assert.Equal("/sync/v1/roots/" + Root + "/capabilities", request.RequestUri!.AbsolutePath);
            Assert.Equal(new string('b', 43), request.Headers.Authorization?.Parameter);
            BeforeResponse?.Invoke();
            return Task.FromResult(new HttpResponseMessage(Status)
            { RequestMessage = ChangeResponseUri ? new(HttpMethod.Get, "https://owned-saturn.invalid/unapproved-route") : request,
                Content = new StringContent(Capability().ToJsonString(), Encoding.UTF8, "application/json") });
        }
    }
}
