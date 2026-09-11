using System.Net;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Neptune.Linux;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class CommandCompletionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ArchiveCommandWaitsForReceiptAndReplayUsesDurableRun(bool rejectReceipt)
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-command-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var tokenFile = Path.Combine(directory, "token");
        await File.WriteAllTextAsync(tokenFile, "synthetic-control-token", TestContext.Current.CancellationToken);
        var state = new NeptuneStateStore(Path.Combine(directory, "state.db"));
        var registry = new ProjectRegistry(Path.Combine(directory, "projects.json"));
        var options = new LinuxOptions { StateDirectory = directory, KernelOrigin = new Uri("https://kernel.test"), KernelTokenFile = tokenFile };
        var project = new ProjectRegistration("volt", new Uri("http://127.0.0.1/export"), tokenFile, tokenFile, tokenFile, "", false, 24, SaturnSlug: "volt");
        await registry.UpsertAsync(project, TestContext.Current.CancellationToken);
        using var handler = new PipelineHandler(rejectReceipt);
        var worker = new BackupWorker(options, registry, state, handler, NullLogger<BackupWorker>.Instance);
        try
        {
            await worker.StartAsync(TestContext.Current.CancellationToken);
            while (worker.ClientInstanceId == "initializing")
                await Task.Delay(10, TestContext.Current.CancellationToken);
        }
        catch (TimeoutException) { /* The disabled schedule must not start an export. */ }
        try
        {
            await state.InitializeAsync(TestContext.Current.CancellationToken);
            var command = worker.RunCommandAsync(project, "command-1", TestContext.Current.CancellationToken);
            var reached = await Task.WhenAny(command, handler.Completing.Task).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            if (reached == command)
                Assert.Fail((await state.GetProjectRunSummaryAsync(worker.ClientInstanceId, "volt", TestContext.Current.CancellationToken)).Latest?.Error ?? "Export failed before a run was journaled");
            await handler.Completing.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.False(command.IsCompleted);
            var concurrentReplay = rejectReceipt ? null : worker.RunCommandAsync(project, "command-1", TestContext.Current.CancellationToken);
            if (concurrentReplay is not null) Assert.False(concurrentReplay.IsCompleted);
            handler.Release.SetResult();
            if (rejectReceipt)
            {
                await Assert.ThrowsAsync<InvalidOperationException>(() => command);
                Assert.Equal("retry-wait", (await state.GetProjectRunSummaryAsync(worker.ClientInstanceId, "volt", TestContext.Current.CancellationToken)).Latest?.State);
            }
            else
            {
                await command;
                await concurrentReplay!;
                await worker.RunCommandAsync(project, "command-1", TestContext.Current.CancellationToken);
                Assert.Equal(1, handler.Exports);
                Assert.Equal(1, handler.Completions);
                Assert.Equal("complete", (await state.GetProjectRunSummaryAsync(worker.ClientInstanceId, "volt", TestContext.Current.CancellationToken)).Latest?.State);
            }
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
            worker.Dispose();
            SqliteConnection.ClearAllPools();
            Directory.Delete(directory, true);
        }
    }

    private sealed class PipelineHandler(bool rejectReceipt) : HttpMessageHandler, IHttpClientFactory
    {
        public readonly TaskCompletionSource Started = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly TaskCompletionSource Completing = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Exports;
        public int Completions;
        private readonly byte[] _bytes = "synthetic-zip-content"u8.ToArray();
        public HttpClient CreateClient(string name) => new(this, false);
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var route = request.RequestUri!.AbsolutePath;
            if (route == "/export") {
                Exports++; Started.TrySetResult();
                var content = new ByteArrayContent(_bytes);
                content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/zip");
                return new(HttpStatusCode.OK) { Content = content };
            }
            if (route == "/api/v1/register/snapshot")
            {
                using var values = JsonDocument.Parse("""{"services":{"saturn":{"sni":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/1","port":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/2","paths":{"backup_ingest":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/3"}}}}""");
                return Json(new { schema = "exocortex.register.snapshot.v1", revision = 1, checksum = CanonicalJson.ComputeRegisterChecksum(values.RootElement), published_at = DateTimeOffset.UtcNow, values = values.RootElement.Clone() });
            }
            if (route == "/api/v1/register/resolve") return Json(new {
                schema = "exocortex.register.resolution.v1",
                values = new Dictionary<string, object> {
                    ["services.saturn.sni"] = new { value = "saturn.test" },
                    ["services.saturn.port"] = new { value = "443" },
                    ["services.saturn.paths.backup_ingest"] = new { value = "/backups" },
                }
            });
            if (route.EndsWith("/capabilities", StringComparison.Ordinal)) return Json(new { schema = "saturn.backup-ingest.capabilities.v1", protocolVersion = 1, resumable = true, checksum = "sha256", maxChunkBytes = 4096 });
            if (route.EndsWith("/runs", StringComparison.Ordinal)) return Json(new { id = "run-1", state = "uploading", receivedSize = 0 });
            if (route.EndsWith("/upload", StringComparison.Ordinal))
            {
                var response = new HttpResponseMessage(HttpStatusCode.NoContent);
                response.Headers.Add("Upload-Offset", request.Method == HttpMethod.Head ? "0" : _bytes.Length.ToString());
                return response;
            }
            if (route.EndsWith("/complete", StringComparison.Ordinal))
            {
                Completions++;
                Completing.SetResult();
                await Release.Task.WaitAsync(cancellationToken);
                return Json(new { receipt = new { schema = "saturn.backup.receipt.v1", runId = "run-1", serviceId = "volt", serviceSlug = "volt", logicalPath = "/backups/volt/test.zip", sizeBytes = _bytes.Length, sha256 = rejectReceipt ? "wrong-hash" : Convert.ToHexStringLower(SHA256.HashData(_bytes)), committedAt = DateTimeOffset.UtcNow } });
            }
            throw new InvalidOperationException("Unexpected test route: " + route);
        }
        private static HttpResponseMessage Json(object value) => new(HttpStatusCode.OK) { Content = JsonContent.Create(value) };
    }
}
