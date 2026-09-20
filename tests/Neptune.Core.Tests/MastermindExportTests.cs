using System.IO.Compression;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class MastermindExportTests
{
    private sealed class Handler(Func<HttpRequestMessage, HttpResponseMessage> respond) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken token)
            => Task.FromResult(respond(request));
    }

    [Theory]
    [InlineData("ok")]
    [InlineData("wrong-size")]
    [InlineData("wrong-hash")]
    [InlineData("missing-generation")]
    [InlineData("negative-generation")]
    [InlineData("encoding")]
    public async Task StreamValidatesSizeHashGenerationAndPurpose(string fault)
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-export-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var bytes = RandomNumberGenerator.GetBytes(2 * 1024 * 1024);
            var hash = Convert.ToHexStringLower(SHA256.HashData(bytes));
            using var http = new HttpClient(new Handler(request =>
            {
                Assert.Equal("archive", request.Headers.GetValues("X-Neptune-Purpose").Single());
                Assert.Equal("capability", request.Headers.Authorization?.Parameter);
                var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(bytes) };
                response.Content.Headers.ContentType = new MediaTypeHeaderValue("application/zip");
                response.Content.Headers.ContentLength = bytes.Length + (fault == "wrong-size" ? 1 : 0);
                if (fault != "missing-generation") response.Headers.Add("X-Mastermind-Generation", fault == "negative-generation" ? "-1" : "42");
                response.Headers.Add("X-Content-SHA256", fault == "wrong-hash" ? new string('0', 64) : hash);
                if (fault == "encoding") response.Content.Headers.ContentEncoding.Add("gzip");
                return response;
            }));
            var exporter = new ProjectBackupExporter(http);
            var path = Path.Combine(directory, "spool.zip");
            if (fault == "ok")
            {
                var receipt = await exporter.ExportAsync(new Uri("http://127.0.0.1:18390/backup"), "capability", path, TestContext.Current.CancellationToken, "archive");
                Assert.Equal(new ExportArtifactReceipt(42, bytes.Length, hash), receipt);
                Assert.Equal(bytes, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            }
            else await Assert.ThrowsAsync<InvalidDataException>(() => exporter.ExportAsync(new Uri("http://127.0.0.1:18390/backup"), "capability", path, TestContext.Current.CancellationToken, "archive"));
        }
        finally { Directory.Delete(directory, true); }
    }

    [Fact]
    public async Task DurableGenerationSurvivesRestartAndRetry()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-export-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var token = TestContext.Current.CancellationToken;
            var store = new NeptuneStateStore(Path.Combine(directory, "state.db"));
            await store.InitializeAsync(token);
            var registration = new ProjectRegistration("mastermind", new Uri("http://127.0.0.1:18390/backup"), "control", "export", "saturn", "slug", true, 24);
            var content = Encoding.UTF8.GetBytes("durable test archive");
            var receipt = new ExportArtifactReceipt(47, content.Length, Convert.ToHexStringLower(SHA256.HashData(content)));
            var run = await new BackupCoordinator(store, "client", directory).ExportAsync(registration,
                (path, ct) => File.WriteAllBytesAsync(path, content, ct), token, exportedReceipt: () => receipt);
            var restarted = new NeptuneStateStore(Path.Combine(directory, "state.db"));
            await restarted.InitializeAsync(token);
            Assert.Equal(47L, (await restarted.GetRunAsync("client", run.RunId, token))!.ExportGeneration);
            await restarted.UpdateRunAsync(run with { State = "retry-wait", Attempt = 1 }, token);
            Assert.Equal(47L, (await restarted.ListRecoverableRunsAsync("client", token)).Single().ExportGeneration);
            Assert.Equal(47L, (await restarted.GetProjectRunSummaryAsync("client", "mastermind", token)).Latest!.ExportGeneration);
        }
        finally { SqliteConnection.ClearAllPools(); Directory.Delete(directory, true); }
    }

    [Theory]
    [InlineData("../outside")]
    [InlineData("/absolute")]
    [InlineData("a\\b")]
    [InlineData("a:b")]
    [InlineData("duplicate")]
    [InlineData("file-directory")]
    [InlineData("symlink")]
    [InlineData("expansion")]
    public void HostileMirrorZipCannotCreateAnyPayload(string fault)
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-export-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var path = Path.Combine(directory, "archive.zip");
            using (var archive = ZipFile.Open(path, ZipArchiveMode.Create))
            {
                var name = fault is "duplicate" or "file-directory" or "symlink" or "expansion" ? "file" : fault;
                var entry = archive.CreateEntry(name, CompressionLevel.SmallestSize);
                if (fault == "symlink") entry.ExternalAttributes = 0xA1FF << 16;
                using (var stream = entry.Open()) stream.Write(new byte[fault == "expansion" ? 2 * 1024 * 1024 : 1]);
                if (fault == "duplicate") archive.CreateEntry("FILE");
                if (fault == "file-directory") archive.CreateEntry("file/child");
            }
            var output = Path.Combine(directory, "tree");
            Directory.CreateDirectory(output);
            Assert.Throws<InvalidDataException>(() => MirrorArchive.Extract(path, output, TestContext.Current.CancellationToken));
            Assert.Empty(Directory.GetFileSystemEntries(output));
        }
        finally { Directory.Delete(directory, true); }
    }

    [Fact]
    public async Task UnknownLengthJsonStillHasABound()
    {
        using var content = new StreamContent(new MemoryStream(new byte[4097]));
        await Assert.ThrowsAsync<InvalidDataException>(() => BoundedJson.ReadAsync(content, 4096, TestContext.Current.CancellationToken));
    }
}
