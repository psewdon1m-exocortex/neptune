using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Neptune.Core;
using Neptune.Linux;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class ServicePolicyTests
{
    [Fact]
    public async Task MissingSaturnPolicyEndpointRequiresCoordinatedUpgrade()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-policy-protocol-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var kernelToken = Path.Combine(directory, "kernel.token");
        var producerToken = Path.Combine(directory, "producer.token");
        await File.WriteAllTextAsync(kernelToken, "kernel-fixture", TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(producerToken, "producer-fixture", TestContext.Current.CancellationToken);
        try
        {
            var options = new LinuxOptions { StateDirectory = directory, KernelOrigin = new Uri("https://kernel.test"), KernelTokenFile = kernelToken };
            var registry = new ProjectRegistry(Path.Combine(directory, "projects.json"));
            var project = new ProjectRegistration("kernel", new Uri("http://127.0.0.1:19020/backup"), producerToken,
                producerToken, producerToken, "services.kernel.backup.saturn_slug", true, 24, SaturnSlug: "kernel");
            var error = await Assert.ThrowsAsync<PolicyProtocolException>(() =>
                new ServicePolicyClient(options, registry, new MissingPolicyHandler()).RequestAsync(project, HttpMethod.Get,
                    cancellationToken: TestContext.Current.CancellationToken));
            Assert.Equal(426, error.StatusCode);
            Assert.Contains("Update Saturn before Neptune", error.Message);
        }
        finally { Directory.Delete(directory, true); }
    }

    [Fact]
    public async Task RepairPreservesPolicyAndOldRunCannotReplaceNewDue()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-policy-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var file = Path.Combine(directory, "projects.json");
        var cancellation = TestContext.Current.CancellationToken;
        try
        {
            var due = DateTimeOffset.UtcNow.AddHours(7);
            var project = new ProjectRegistration("volt", new Uri("http://127.0.0.1:19010/backup"), "control", "export", "producer",
                "services.volt.backup.saturn_slug", true, 7, due, "volt",
                new MirrorRegistration(new Uri("http://127.0.0.1:19010/mirror"), "volt", "mirror-token", "single-file", "personal.volt", true, 5, due),
                ControlRevision: 12);
            var first = new ProjectRegistry(file);
            await first.UpsertAsync(project, cancellation);
            await new ProjectRegistry(file).UpsertAsync(project with { Enabled = false, IntervalHours = 24, NextRunAt = null,
                ControlRevision = 0, Mirror = project.Mirror! with { Enabled = false, IntervalMinutes = 1440 } }, cancellation, preservePolicy: true);
            var repaired = (await first.FindAsync("volt", cancellation))!;
            Assert.True(repaired.Enabled);
            Assert.Equal(7, repaired.IntervalHours);
            Assert.Equal(5, repaired.Mirror!.IntervalMinutes);
            Assert.Equal(due, repaired.NextRunAt);
            Assert.Equal(12, repaired.ControlRevision);
            await first.ApplyRemoteDesiredAsync("volt", 13, true, 9, true, 60, cancellation);
            var changed = (await first.FindAsync("volt", cancellation))!;
            await first.UpdateNextRunAsync("volt", due.AddDays(1), cancellation, expectedRevision: 12);
            await first.UpdateMirrorNextRunAsync("volt", due.AddDays(1), cancellation, expectedRevision: 12);
            var afterOldRun = (await first.FindAsync("volt", cancellation))!;
            Assert.Equal(changed.NextRunAt, afterOldRun.NextRunAt);
            Assert.Equal(changed.Mirror!.NextRunAt, afterOldRun.Mirror!.NextRunAt);
        }
        finally { Directory.Delete(directory, true); }
    }

    [Fact]
    public async Task RestorePauseIsSafeForLegacyReadersAndStalePolicyCannotResume()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-policy-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var cancellation = TestContext.Current.CancellationToken;
        try
        {
            var registry = new ProjectRegistry(Path.Combine(directory, "projects.json"));
            await registry.UpsertAsync(new ProjectRegistration("chronos", new Uri("http://127.0.0.1:18880/backup"),
                "control", "export", "producer", "services.chronos.backup.saturn_slug", true, 7), cancellation);
            await registry.ApplyRemoteDesiredAsync("chronos", 9, true, 7, false, 1440, cancellation, paused: true);
            var paused = (await registry.FindAsync("chronos", cancellation))!;
            Assert.True(paused.PolicyPaused);
            Assert.False(paused.Enabled);
            Assert.Equal(7, paused.IntervalHours);
            await registry.ApplyRemoteDesiredAsync("chronos", 8, true, 24, false, 1440, cancellation);
            Assert.Equal(paused, await registry.FindAsync("chronos", cancellation));
            await registry.ApplyRemoteDesiredAsync("chronos", 10, true, 7, false, 1440, cancellation);
            var resumed = (await registry.FindAsync("chronos", cancellation))!;
            Assert.True(resumed.Enabled);
            Assert.False(resumed.PolicyPaused);
            Assert.True(resumed.NextRunAt > DateTimeOffset.UtcNow.AddHours(6));
        }
        finally { Directory.Delete(directory, true); }
    }

    [Fact]
    public async Task UnlinkDrainsOneProjectAndLeavesOtherRegistrationsIntact()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-unlink-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var cancellation = TestContext.Current.CancellationToken;
            var registry = new ProjectRegistry(Path.Combine(directory, "projects.json"));
            var export = new Uri("http://127.0.0.1:18880/backup");
            await registry.UpsertAsync(new ProjectRegistration("volt", export, "control", "export", "producer",
                "services.volt.backup.saturn_slug", true, 3,
                Mirror: new MirrorRegistration(export, "volt", "mirror", "single-file", "personal.volt", true, 30)), cancellation);
            await registry.UpsertAsync(new ProjectRegistration("chronos", export, "control", "export", "producer",
                "services.chronos.backup.saturn_slug", true, 24), cancellation);
            await registry.PrepareUnlinkAsync("volt", cancellation);
            await registry.ApplyRemoteDesiredAsync("volt", 99, true, 1, true, 5, cancellation);
            var paused = (await registry.FindAsync("volt", cancellation))!;
            Assert.True(paused.Unlinking);
            Assert.False(paused.Enabled);
            Assert.False(paused.Mirror!.Enabled);
            Assert.Null(paused.NextRunAt);
            await registry.MarkRemoteDisconnectedAsync("volt", cancellation);
            await registry.RemoveAsync("volt", cancellation);
            Assert.Null(await registry.FindAsync("volt", cancellation));
            Assert.True((await registry.FindAsync("chronos", cancellation))!.Enabled);
        }
        finally { Directory.Delete(directory, true); }
    }

    private sealed class MissingPolicyHandler : HttpMessageHandler, IHttpClientFactory
    {
        public HttpClient CreateClient(string name) => new(this, false);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var route = request.RequestUri!.AbsolutePath;
            if (route == "/api/v1/register/snapshot")
            {
                using var values = JsonDocument.Parse("""{"services":{"saturn":{"sni":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/1","port":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/2"}}}""");
                return Task.FromResult(Json(new { schema = "exocortex.register.snapshot.v1", revision = 1,
                    checksum = CanonicalJson.ComputeRegisterChecksum(values.RootElement), published_at = DateTimeOffset.UtcNow,
                    values = values.RootElement.Clone() }));
            }
            if (route == "/api/v1/register/resolve") return Task.FromResult(Json(new {
                schema = "exocortex.register.resolution.v1",
                values = new Dictionary<string, object> {
                    ["services.saturn.sni"] = new { value = "saturn.test" },
                    ["services.saturn.port"] = new { value = "443" },
                }
            }));
            if (route == "/api/v1/neptune/agent/policy")
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound) { Content = new StringContent("") });
            throw new InvalidOperationException("Unexpected test route: " + route);
        }

        private static HttpResponseMessage Json(object value) => new(HttpStatusCode.OK) { Content = JsonContent.Create(value) };
    }
}
