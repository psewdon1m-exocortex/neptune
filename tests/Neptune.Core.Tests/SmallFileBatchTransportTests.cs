using System.Net;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class SmallFileBatchTransportTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static (WindowsFreshCycleTests.FakeRemote Remote, SmallFileBatchPlan Plan, byte[] Data) Fixture()
    {
        var remote = new WindowsFreshCycleTests.FakeRemote(); var parts = new[] { Array.Empty<byte>(), new byte[] { 17 }, Enumerable.Range(0, 262144).Select(n => (byte)n).ToArray() };
        var entries = parts.Select((bytes, n) => new SmallFileBatchEntry(new(remote.Binding.ServerEpoch, remote.Binding.BindingVersion, "local-test",
            new("saturn.upload-intent.v2", "windows_sync", remote.Device, remote.Binding.RootId, "/café😀-" + n, "neptune-cycle-" + Guid.NewGuid().ToString("D"), remote.Binding.RootId,
                "café😀-" + n, null, bytes.Length.ToString(System.Globalization.CultureInfo.InvariantCulture), Convert.ToHexStringLower(SHA256.HashData(bytes)), null, "*", true, null,
                new("absent"), [new(remote.Binding.RootId, null, "root")])), 99 + n, "native-capture-" + n)).ToArray();
        return (remote, SmallFileBatchPlan.Create(remote.Binding, Guid.NewGuid().ToString("D"), "rules-v1", entries), parts.SelectMany(p => p).ToArray());
    }
    private static byte[] Missing(SmallFileBatchPlan plan)
    {
        var body = JsonNode.Parse(plan.EncodeManifest())!.AsObject(); body["schema"] = "saturn.small-file-batch-result.v1"; body.Remove("rules_version");
        body["items"] = new JsonArray(plan.Entries.Select(e => (JsonNode)new JsonObject { ["operation_id"] = e.Original.Intent.IdempotencyKey,
            ["source_generation"] = e.SourceGeneration.ToString(System.Globalization.CultureInfo.InvariantCulture), ["source_identity"] = e.SourceIdentity, ["state"] = "MISSING" }).ToArray());
        return Encoding.UTF8.GetBytes(body.ToJsonString());
    }
    private static SaturnGuardedCycleTransport Make(HttpClient http, WindowsFreshCycleTests.FakeRemote remote, Func<CancellationToken, Task<string>>? authorization = null)
    {
        var configuration = new WindowsCycleConfiguration("mapping", "client", "audit", "rules-v1", new Uri("http://localhost/"), remote.Device, "test-only", "local-test", remote.Binding);
        return (SaturnGuardedCycleTransport)typeof(SaturnGuardedCycleTransport).GetConstructor(BindingFlags.Instance | BindingFlags.NonPublic, null,
            [typeof(HttpClient), typeof(WindowsCycleConfiguration), typeof(Func<CancellationToken, Task<string>>)], null)!.Invoke([http, configuration, authorization ?? (_ => Task.FromResult("Bearer batch-test"))]);
    }
    private sealed class Handler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> action) : HttpMessageHandler
    { protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct) => action(request, ct); }
    private static HttpResponseMessage Response(HttpRequestMessage request, byte[] raw, HttpStatusCode status = HttpStatusCode.Accepted)
        => new(status) { RequestMessage = request, Content = new ByteArrayContent(raw) };
    private sealed class ReadBudget(byte[] bytes) : MemoryStream(bytes, false)
    {
        internal int MaximumRead;
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        { MaximumRead = Math.Max(MaximumRead, buffer.Length); return base.ReadAsync(buffer, ct); }
    }

    [Fact]
    public async Task BatchHttpUsesStrictPlanStatusAndBoundedMultipartIncludingZeroAndUnicode()
    {
        var f = Fixture(); var commands = new List<string>(); var authorizations = 0;
        using var http = new HttpClient(new Handler(async (request, ct) =>
        {
            Assert.Equal(HttpMethod.Post, request.Method); Assert.Equal("Bearer batch-test-" + commands.Count, request.Headers.Authorization!.ToString());
            var command = request.RequestUri!.Segments[^1]; commands.Add(command); var raw = await request.Content!.ReadAsByteArrayAsync(ct);
            if (command == "prepare") Assert.Equal(f.Plan.EncodePlan(), raw);
            else if (command == "status") Assert.Equal(f.Plan.EncodeStatusRequest(), raw);
            else
            {
                Assert.Equal("data", command); Assert.Equal("multipart/related", request.Content.Headers.ContentType!.MediaType);
                var boundary = request.Content.Headers.ContentType.Parameters.Single(p => p.Name == "boundary").Value!;
                Assert.InRange(boundary.Length, 1, 70); Assert.Equal(raw.Length, request.Content.Headers.ContentLength);
                var opening = Encoding.ASCII.GetBytes("--" + boundary + "\r\nContent-Type: application/json\r\nContent-Length: " + f.Plan.EncodeManifest().Length + "\r\n\r\n");
                Assert.Equal(opening, raw.AsSpan(0, opening.Length).ToArray()); Assert.Equal(f.Plan.EncodeManifest(), raw.AsSpan(opening.Length, f.Plan.EncodeManifest().Length).ToArray());
                Assert.True(raw.AsSpan().EndsWith(Encoding.ASCII.GetBytes("\r\n--" + boundary + "--\r\n")));
                var file = Environment.GetEnvironmentVariable("NEPTUNE_BATCH_HTTP_FIXTURE");
                if (file is not null)
                {
                    await using (var output = new FileStream(file + ".bin", FileMode.CreateNew, FileAccess.Write)) await output.WriteAsync(raw, ct);
                    await File.WriteAllTextAsync(file + ".json", JsonSerializer.Serialize(new { contentType = request.Content.Headers.ContentType.ToString(),
                        plan = JsonSerializer.Deserialize<JsonElement>(f.Plan.EncodePlan()), byteLength = raw.Length, sha256 = Convert.ToHexStringLower(SHA256.HashData(raw)) }), ct);
                }
            }
            return Response(request, Missing(f.Plan), command == "status" ? HttpStatusCode.OK : HttpStatusCode.Accepted);
        }));
        using var transport = Make(http, f.Remote, _ => Task.FromResult("Bearer batch-test-" + authorizations++));
        Assert.Equal(3, (await transport.PrepareSmallBatchAsync(f.Plan, Ct)).Items.Count);
        Assert.Equal(3, (await transport.ReadSmallBatchAsync(f.Plan, Ct)).Items.Count);
        using var spool = new ReadBudget(f.Data); Assert.Equal(3, (await transport.SendSmallBatchAsync(f.Plan, spool, Ct)).Items.Count);
        Assert.True(spool.CanRead); Assert.InRange(spool.MaximumRead, 1, 65536); Assert.Equal(new[] { "prepare", "status", "data" }, commands);
        Assert.Equal(3, authorizations); Assert.Equal(3, transport.RequestCounts.Values.Sum());
    }

    [Fact]
    public async Task SingleRequestCapabilityUsesExactPlanAndOneExecuteRoute()
    {
        var f = Fixture(); var plan = SmallFileBatchPlan.Create(f.Plan.Binding, f.Plan.BatchId, f.Plan.RulesVersion, f.Plan.Entries.Skip(1).ToArray()).AsExecutionJob();
        var calls = new List<string>();
        using var http = new HttpClient(new Handler(async (request, ct) =>
        {
            var command = request.RequestUri!.Segments[^1]; calls.Add(command);
            if (command == "single-request-capabilities")
            {
                var body = CapabilityBody(plan, f.Remote); body["schema"] = "saturn.small-execution-job-single-request-capabilities.v1"; body["transport"] = "saturn.small-execution-job.v1";
                body["limits"]!["items"] = 16; body["limits"]!["data_bytes"] = "4194304"; body["limits"]!["manifest_bytes"] = "1048576"; body["limits"]!["jobs"] = 2;
                return Response(request, Encoding.UTF8.GetBytes(body.ToJsonString()), HttpStatusCode.OK);
            }
            Assert.Equal("execute", command); var raw = await request.Content!.ReadAsByteArrayAsync(ct);
            Assert.True(raw.AsSpan().IndexOf(plan.EncodePlan()) >= 0); return Response(request, Missing(plan));
        }));
        using var transport = Make(http, f.Remote); var cap = await transport.ReadSmallBatchCapabilitiesAsync(Ct);
        Assert.True(cap!.SingleRequest); Assert.True(cap.Admits(plan));
        using var data = new MemoryStream(f.Data); await transport.ExecuteSmallJobAsync(plan, data, Ct);
        Assert.Equal(new[] { "single-request-capabilities", "execute" }, calls);
    }
    [Theory]
    [InlineData("digest")][InlineData("offset")][InlineData("size")][InlineData("binding")]
    public async Task BatchHttpRejectsChangedPayloadOrBindingWithoutPublishingFollowingDelimiter(string changed)
    {
        var f = Fixture(); using var captured = new MemoryStream(); var requests = 0;
        using var http = new HttpClient(new Handler(async (request, ct) =>
        { requests++; await request.Content!.CopyToAsync(captured, ct); return Response(request, Missing(f.Plan)); }));
        using var transport = Make(http, f.Remote); var bytes = (byte[])f.Data.Clone(); if (changed == "digest") bytes[0] ^= 99;
        using var source = new ReadBudget(changed == "size" ? bytes[..^1] : bytes); if (changed == "offset") source.Position = 1;
        var plan = changed == "binding" ? SmallFileBatchPlan.Create(f.Plan.Binding with { PolicyVersion = "other-policy" }, f.Plan.BatchId, f.Plan.RulesVersion, f.Plan.Entries) : f.Plan;
        await Assert.ThrowsAnyAsync<Exception>(() => transport.SendSmallBatchAsync(plan, source, Ct));
        Assert.True(source.CanRead); Assert.Equal(changed == "digest" ? 1 : 0, requests);
        if (changed == "digest")
        {
            // Item zero is independently complete; the corrupt one-byte item
            // never receives its following delimiter or next item's headers.
            var text = Encoding.UTF8.GetString(captured.ToArray()); Assert.Contains(f.Plan.Entries[1].Original.Intent.IdempotencyKey, text);
            Assert.DoesNotContain("Content-ID: <" + f.Plan.Entries[2].Original.Intent.IdempotencyKey + ">", text);
        }
    }

    [Theory]
    [InlineData(307)][InlineData(429)][InlineData(503)]
    public async Task BatchHttpSurfacesRedirectAndAdmissionWithoutImplicitRetry(int code)
    {
        var f = Fixture(); var requests = 0;
        using var http = new HttpClient(new Handler((request, _) =>
        {
            requests++; var response = Response(request, "{\"code\":\"admission_full\"}"u8.ToArray(), (HttpStatusCode)code);
            response.Headers.Location = new("https://elsewhere.invalid/"); response.Headers.RetryAfter = new(TimeSpan.FromSeconds(1)); return Task.FromResult(response);
        }));
        using var transport = Make(http, f.Remote); var error = await Assert.ThrowsAsync<GuardedCycleRemoteException>(() => transport.PrepareSmallBatchAsync(f.Plan, Ct));
        Assert.Equal((HttpStatusCode)code, error.Status); Assert.Equal(1, requests);
    }

    [Fact]
    public async Task BatchHttpKnownOriginalUuidCannotBeReplacedByMissingStatus()
    {
        var f = Fixture(); var known = f.Plan.WithOriginalUploadIds(new Dictionary<string, string> { [f.Plan.Entries[0].Original.Intent.IdempotencyKey] = Guid.NewGuid().ToString("D") });
        using var http = new HttpClient(new Handler((request, _) => Task.FromResult(Response(request, Missing(f.Plan)))));
        using var transport = Make(http, f.Remote); await Assert.ThrowsAsync<InvalidDataException>(() => transport.ReadSmallBatchAsync(known, Ct));
    }

    private static JsonObject CapabilityBody(SmallFileBatchPlan plan, WindowsFreshCycleTests.FakeRemote remote)
    {
        var binding = JsonNode.Parse(plan.EncodeManifest())!.AsObject(); foreach (var key in new[] { "schema", "batch_id", "rules_version", "items" }) binding.Remove(key);
        return new JsonObject { ["schema"] = "saturn.small-file-batch-capabilities.v1", ["transport"] = "saturn.small-file-batch.v1", ["binding"] = binding,
            ["device_id"] = remote.Device, ["backend_identity"] = "local-test", ["qualification_receipt_sha256"] = new string('a', 64),
            ["limits"] = new JsonObject { ["items"] = 500, ["file_bytes"] = "262144", ["data_bytes"] = "33554432", ["manifest_bytes"] = "4194304" } };
    }
    [Fact]
    public async Task SmallBatchCapabilityUsesSeparateAuthenticatedGetAndDoesNotChangeV1Negotiation()
    {
        var f = Fixture(); using var http = new HttpClient(new Handler(async (request, ct) =>
        {
            Assert.Equal(HttpMethod.Get, request.Method);
            Assert.Equal("Bearer batch-test", request.Headers.Authorization!.ToString()); Assert.Empty(await request.Content!.ReadAsByteArrayAsync(ct));
            if (request.RequestUri!.AbsolutePath.Contains("/small-jobs/", StringComparison.Ordinal))
                return Response(request, "{\"code\":\"batch_unavailable\"}"u8.ToArray(), HttpStatusCode.NotFound);
            Assert.EndsWith("/small-batches/capabilities", request.RequestUri.AbsolutePath);
            return Response(request, Encoding.UTF8.GetBytes(CapabilityBody(f.Plan, f.Remote).ToJsonString()), HttpStatusCode.OK);
        }));
        using var transport = Make(http, f.Remote); var capability = await transport.ReadSmallBatchCapabilitiesAsync(Ct);
        Assert.NotNull(capability); Assert.Equal(f.Plan.Binding, capability.Binding); Assert.True(capability.Admits(f.Plan));
    }
    [Fact]
    public async Task SmallBatchCapabilityAbsentEndpointMeansIndividualTransportRemainsAvailable()
    {
        var f = Fixture(); using var http = new HttpClient(new Handler((request, _) => Task.FromResult(Response(request, "{\"code\":\"batch_unavailable\"}"u8.ToArray(), HttpStatusCode.NotFound))));
        using var transport = Make(http, f.Remote); Assert.Null(await transport.ReadSmallBatchCapabilitiesAsync(Ct));
    }
    [Fact]
    public async Task LegacyFallbackCapabilityUsesItsOwnAuthenticatedEndpoint()
    {
        var f = Fixture(); var calls = 0;
        using var http = new HttpClient(new Handler((request, _) =>
        {
            calls++; Assert.Equal(HttpMethod.Get, request.Method);
            Assert.Equal("Bearer batch-test", request.Headers.Authorization!.ToString());
            Assert.EndsWith("/small-batches/capabilities", request.RequestUri!.AbsolutePath);
            return Task.FromResult(Response(request, Encoding.UTF8.GetBytes(CapabilityBody(f.Plan, f.Remote).ToJsonString()), HttpStatusCode.OK));
        }));
        using var transport = Make(http, f.Remote); var capability = await transport.ReadLegacySmallBatchCapabilitiesAsync(Ct);
        Assert.NotNull(capability); Assert.False(capability.ExecutionJob); Assert.True(capability.Admits(f.Plan)); Assert.Equal(1, calls);
    }
    [Theory]
    [InlineData(HttpStatusCode.NotFound)]
    [InlineData(HttpStatusCode.ProxyAuthenticationRequired)]
    public async Task LegacyFallbackAbsenceDiffersFromProxyAuthenticationFailure(HttpStatusCode status)
    {
        var f = Fixture(); using var http = new HttpClient(new Handler((request, _) => Task.FromResult(Response(request, "{}"u8.ToArray(), status))));
        using var transport = Make(http, f.Remote);
        if (status == HttpStatusCode.NotFound) Assert.Null(await transport.ReadLegacySmallBatchCapabilitiesAsync(Ct));
        else Assert.Equal(status, (await Assert.ThrowsAsync<GuardedCycleRemoteException>(() => transport.ReadLegacySmallBatchCapabilitiesAsync(Ct))).Status);
    }
    [Fact]
    public async Task ExecutionJobCapabilityPinsSeparateRoutesAndExactPlanInEveryAttempt()
    {
        var f = Fixture(); var plan = SmallFileBatchPlan.Create(f.Plan.Binding, f.Plan.BatchId, f.Plan.RulesVersion, f.Plan.Entries.Skip(1).ToArray()).AsExecutionJob();
        var calls = new List<string>();
        using var http = new HttpClient(new Handler(async (request, ct) =>
        {
            Assert.Contains("/small-jobs/", request.RequestUri!.AbsolutePath); var command = request.RequestUri.Segments[^1]; calls.Add(command);
            Assert.Equal("Bearer batch-test", request.Headers.Authorization!.ToString());
            if (command is "pipeline-capabilities" or "single-request-capabilities") return Response(request, "{\"code\":\"batch_unavailable\"}"u8.ToArray(), HttpStatusCode.NotFound);
            if (command == "capabilities")
            {
                var body = CapabilityBody(plan, f.Remote); body["schema"] = "saturn.small-execution-job-capabilities.v1"; body["transport"] = "saturn.small-execution-job.v1";
                body["limits"]!["items"] = 16; body["limits"]!["data_bytes"] = "4194304"; body["limits"]!["manifest_bytes"] = "1048576";
                return Response(request, Encoding.UTF8.GetBytes(body.ToJsonString()), HttpStatusCode.OK);
            }
            var raw = await request.Content!.ReadAsByteArrayAsync(ct);
            if (command == "data") Assert.True(raw.AsSpan().IndexOf(plan.EncodePlan()) >= 0);
            else Assert.Equal(plan.EncodePlan(), raw);
            return Response(request, Missing(plan));
        }));
        using var transport = Make(http, f.Remote); var cap = await transport.ReadSmallBatchCapabilitiesAsync(Ct);
        Assert.True(cap!.ExecutionJob); Assert.True(cap.Admits(plan)); Assert.False(cap.Admits(f.Plan));
        await transport.PrepareSmallBatchAsync(plan, Ct); await transport.ReadSmallBatchAsync(plan, Ct);
        using var data = new MemoryStream(f.Data); await transport.SendSmallBatchAsync(plan, data, Ct);
        Assert.Equal(new[] { "single-request-capabilities", "pipeline-capabilities", "capabilities", "prepare", "status", "data" }, calls);
    }
    [Theory]
    [InlineData("device")][InlineData("epoch")][InlineData("backend")][InlineData("receipt")]
    [InlineData("items")][InlineData("file")][InlineData("metadata")][InlineData("extra")][InlineData("schema")]
    public async Task SmallBatchCapabilityRejectsUnknownBindingAndUnsupportedCeilings(string changed)
    {
        var f = Fixture(); var body = CapabilityBody(f.Plan, f.Remote);
        switch (changed)
        {
            case "device": body["device_id"] = Guid.NewGuid().ToString("D"); break;
            case "epoch": body["binding"]!["server_epoch"] = Guid.NewGuid().ToString("D"); break;
            case "backend": body["backend_identity"] = "other"; break;
            case "receipt": body["qualification_receipt_sha256"] = new string('A', 64); break;
            case "items": body["limits"]!["items"] = 501; break;
            case "file": body["limits"]!["file_bytes"] = "262145"; break;
            case "metadata": body["limits"]!["manifest_bytes"] = "4194305"; break;
            case "extra": body["auto_enable"] = true; break;
            case "schema": body["schema"] = "future.v2"; break;
        }
        using var http = new HttpClient(new Handler((request, _) => Task.FromResult(Response(request, Encoding.UTF8.GetBytes(body.ToJsonString()), HttpStatusCode.OK))));
        using var transport = Make(http, f.Remote); await Assert.ThrowsAnyAsync<Exception>(() => transport.ReadSmallBatchCapabilitiesAsync(Ct));
    }
}
