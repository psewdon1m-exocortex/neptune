using Microsoft.Extensions.Logging.Abstractions;
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using Neptune.Linux;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class PipelineSelectionTests
{
    [Fact]
    public async Task MirrorOnlyProjectCannotStartResumeOrEnableAnArchive()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-pipeline-selection-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var cancel = TestContext.Current.CancellationToken;
        try
        {
            var registry = new ProjectRegistry(Path.Combine(directory, "projects.json"));
            var project = new ProjectRegistration("volt", new Uri("http://127.0.0.1/backup"), "control", "export", "producer", "", false, 24,
                SaturnSlug: "volt", Mirror: new MirrorRegistration(new Uri("http://127.0.0.1/mirror"), "volt", "mirror", "single-file", "personal.volt", false, 60), ArchiveAvailable: false);
            await registry.UpsertAsync(project, cancel);
            await registry.ApplyRemoteDesiredAsync("volt", 1, true, 24, true, 60, cancel);
            var applied = await registry.FindAsync("volt", cancel) ?? throw new InvalidDataException();
            Assert.False(applied.Enabled); Assert.Null(applied.NextRunAt); Assert.True(applied.Mirror!.Enabled);
            await registry.UpdateScheduleAsync("volt", true, 48, cancel);
            Assert.False((await registry.FindAsync("volt", cancel))!.Enabled);
            var options = new LinuxOptions { StateDirectory = directory, KernelOrigin = new Uri("https://kernel.test") };
            var state = new NeptuneStateStore(Path.Combine(directory, "state.db"));
            var worker = new BackupWorker(options, registry, state, new RejectHttpFactory(), NullLogger<BackupWorker>.Instance);
            Assert.False(worker.Start(applied));
            await Assert.ThrowsAsync<InvalidOperationException>(() => worker.RunCommandAsync(applied, "unexpected-command", cancel));
            await registry.UpsertAsync(project with { Enabled = true }, cancel, preservePolicy: true);
            Assert.False((await registry.FindAsync("volt", cancel))!.Enabled);
        }
        finally { Directory.Delete(directory, true); }
    }

    private sealed class RejectHttpFactory : IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => throw new InvalidOperationException("Disabled archive accessed the network");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MirrorOnlyResumeChecksItsMirrorAndRejectsCapabilityMismatch(bool mismatch)
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-mirror-resume-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var cancel = TestContext.Current.CancellationToken;
        var tokenFile = Path.Combine(directory, "token"); await File.WriteAllTextAsync(tokenFile, "fixture", cancel);
        using var handler = new ResumeHandler(mismatch);
        try
        {
            var registry = new ProjectRegistry(Path.Combine(directory, "projects.json"));
            var project = new ProjectRegistration("volt", new Uri("http://127.0.0.1/backup"), tokenFile, tokenFile, tokenFile, "", false, 24,
                SaturnSlug: "volt", Mirror: new MirrorRegistration(new Uri("http://127.0.0.1/mirror"), "volt", tokenFile, "single-file", "personal.volt", false, 60), ArchiveAvailable: false);
            await registry.UpsertAsync(project, cancel);
            var client = new ServicePolicyClient(new LinuxOptions { StateDirectory = directory, KernelOrigin = new Uri("https://kernel.test"), KernelTokenFile = tokenFile }, registry, handler);
            var body = JsonNode.Parse("""{"kind":"resume","expectedRevision":1}""");
            if (mismatch) await Assert.ThrowsAsync<InvalidDataException>(() => client.RequestAsync(project, HttpMethod.Put, body: body, cancellationToken: cancel));
            else
            {
                await client.RequestAsync(project, HttpMethod.Put, body: body, cancellationToken: cancel);
                var applied = (await registry.FindAsync("volt", cancel))!;
                Assert.False(applied.Enabled); Assert.True(applied.Mirror!.Enabled);
            }
            Assert.Equal(1, handler.MirrorChecks); Assert.Equal(1, handler.DestinationChecks);
        }
        finally { Directory.Delete(directory, true); }
    }

    private sealed class ResumeHandler(bool mismatch) : HttpMessageHandler, IHttpClientFactory
    {
        public int MirrorChecks, DestinationChecks;
        public HttpClient CreateClient(string name) => new(this, false);
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var route = request.RequestUri!.AbsolutePath;
            HttpResponseMessage Json(object value) => new(HttpStatusCode.OK) { Content = JsonContent.Create(value) };
            if (route == "/backup") throw new InvalidOperationException("Unselected archive was verified");
            if (route == "/mirror")
            {
                MirrorChecks++; Assert.Equal(HttpMethod.Head, request.Method);
                var ready = new HttpResponseMessage(HttpStatusCode.OK); ready.Headers.Add("X-Neptune-Ready", "1"); return Task.FromResult(ready);
            }
            if (route == "/dav/volt/") { DestinationChecks++; return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)); }
            if (route == "/api/v1/register/snapshot")
            {
                using var values = JsonDocument.Parse("""{"services":{"saturn":{"sni":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/1","port":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/2"}}}""");
                return Task.FromResult(Json(new { schema = "exocortex.register.snapshot.v1", revision = 1, checksum = CanonicalJson.ComputeRegisterChecksum(values.RootElement), values = values.RootElement.Clone() }));
            }
            if (route == "/api/v1/register/resolve") return Task.FromResult(Json(new { schema = "exocortex.register.resolution.v1", values = new Dictionary<string, object> {
                ["services.saturn.sni"] = new { value = "saturn.test" }, ["services.saturn.port"] = new { value = "443" } } }));
            if (route == "/api/v1/neptune/agent/policy") return Task.FromResult(Json(new { schema = "exocortex.backup.policy.v1", revision = 2, paused = false,
                archive = new { available = mismatch, enabled = false, intervalHours = 24 }, mirror = new { enabled = true, intervalMinutes = 60 } }));
            throw new InvalidOperationException("Unexpected request " + request);
        }
    }
}
