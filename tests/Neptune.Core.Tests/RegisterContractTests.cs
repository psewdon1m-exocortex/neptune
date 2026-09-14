using System.Net;
using System.Text;
using System.Text.Json;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class RegisterContractTests
{
    [Fact]
    public async Task ResolvesOnlyRequestedMetadataInOneBatchAndKeepsOptionalKeysAbsent()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-scope-" + Guid.NewGuid().ToString("N"));
        var reference = "volt://019926ad-1034-7000-8000-c38a837740ab/4";
        var privateValues = Enumerable.Range(0, 100).ToDictionary(index => "secret" + index, _ => reference);
        using var values = JsonDocument.Parse(JsonSerializer.Serialize(new { services = new { saturn = new { sni = reference, port = reference } }, credentials = privateValues }));
        var snapshot = JsonSerializer.Serialize(new { schema = "exocortex.register.snapshot.v1", revision = "scope-1", checksum = CanonicalJson.ComputeRegisterChecksum(values.RootElement), values = values.RootElement });
        var handler = new ScopedHandler(snapshot);
        using var http = new HttpClient(handler);
        try
        {
            using var resolved = await new KernelRegisterClient(http, Path.Combine(directory, "register.json"))
                .GetSnapshotAsync(new Uri("https://kernel.audit.invalid"), "audit-only-token", TestContext.Current.CancellationToken,
                    ["services.saturn.sni", "services.saturn.port", "services.saturn.paths.sync_preferences"]);
            Assert.Equal(1, handler.ResolveCalls);
            var result = resolved.RootElement.GetProperty("values");
            Assert.False(result.TryGetProperty("credentials", out _));
            Assert.Equal("https://saturn-audit-canary.invalid/", RegisterValues.HttpsOrigin(result, "saturn").AbsoluteUri);
            Assert.Throws<InvalidDataException>(() => RegisterValues.RequiredString(result, "services.saturn.paths.sync_preferences"));
            Assert.DoesNotContain("saturn-audit-canary", await File.ReadAllTextAsync(Path.Combine(directory, "register.json"), TestContext.Current.CancellationToken));
        }
        finally { if (Directory.Exists(directory)) Directory.Delete(directory, true); }
    }

    private sealed class ScopedHandler(string snapshot) : HttpMessageHandler
    {
        public int ResolveCalls { get; private set; }
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Assert.Equal("audit-only-token", request.Headers.Authorization?.Parameter);
            if (request.Method == HttpMethod.Get)
                return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(snapshot) };
            ResolveCalls++;
            using var payload = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(cancellationToken));
            Assert.Equal(new[] { "services.saturn.port", "services.saturn.sni" }, payload.RootElement.GetProperty("keys").EnumerateArray().Select(item => item.GetString()));
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{\"schema\":\"exocortex.register.resolution.v1\",\"values\":{\"services.saturn.port\":{\"value\":\"443\"},\"services.saturn.sni\":{\"value\":\"saturn-audit-canary.invalid\"}}}") };
        }
    }

    [Theory]
    [InlineData("1", true)]
    [InlineData("4", true)]
    [InlineData("5", true)]
    [InlineData("0", false)]
    [InlineData("6", false)]
    [InlineData("04", false)]
    [InlineData("3518462b-bb66-459a-a1ab-c38a837740ab", false)]
    public async Task ResolvesNumericPositionsWithoutPersistingResolvedValues(string position, bool valid)
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-contract-" + Guid.NewGuid().ToString("N"));
        var cache = Path.Combine(directory, "register.json");
        var reference = "volt://019926ad-1034-7000-8000-c38a837740ab/" + position;
        using var values = JsonDocument.Parse(JsonSerializer.Serialize(new { services = new { saturn = new { sni = reference } } }));
        var snapshot = JsonSerializer.Serialize(new { schema = "exocortex.register.snapshot.v1", revision = "audit-1", checksum = CanonicalJson.ComputeRegisterChecksum(values.RootElement), values = values.RootElement });
        using var http = new HttpClient(new Handler(snapshot));
        try
        {
            var task = new KernelRegisterClient(http, cache).GetSnapshotAsync(new Uri("https://kernel.audit.invalid"), "audit-only-token", TestContext.Current.CancellationToken);
            if (!valid) { await Assert.ThrowsAsync<InvalidDataException>(() => task); return; }
            using var resolved = await task;
            Assert.Equal("saturn-audit-canary.invalid", RegisterValues.RequiredString(resolved.RootElement.GetProperty("values"), "services.saturn.sni"));
            var cached = await File.ReadAllTextAsync(cache, TestContext.Current.CancellationToken);
            Assert.Contains(reference, cached);
            Assert.DoesNotContain("saturn-audit-canary", cached);
        }
        finally { if (Directory.Exists(directory)) Directory.Delete(directory, true); }
    }

    private sealed class Handler(string snapshot) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Assert.Equal("audit-only-token", request.Headers.Authorization?.Parameter);
            var body = request.Method == HttpMethod.Get ? snapshot : "{\"schema\":\"exocortex.register.resolution.v1\",\"values\":{\"services.saturn.sni\":{\"value\":\"saturn-audit-canary.invalid\"}}}";
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") });
        }
    }
}
