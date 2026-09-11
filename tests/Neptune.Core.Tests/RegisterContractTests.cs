using System.Net;
using System.Text;
using System.Text.Json;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class RegisterContractTests
{
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
