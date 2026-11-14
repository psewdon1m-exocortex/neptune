using System.Net;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class NetworkAcceptanceTests
{
    [Theory]
    [InlineData(false)] [InlineData(true)]
    public async Task N07_CorruptOrMissingSpoolIsRetainedForReview(bool missing)
    {
        var ct = TestContext.Current.CancellationToken;
        var directory = Path.Combine(Path.GetTempPath(), "network-spool-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        try
        {
            var source = Path.Combine(directory, "source"); await File.WriteAllTextAsync(source, "sealed", ct);
            var spool = Path.Combine(directory, "spool"); var target = new Uri("https://saturn.test/dav/file");
            await using (var original = await RetainedLegacyUpload.OpenAsync(spool, "scope", target, source, null, ct)) await original.BindAsync("\"old\"", ct);
            var manifest = Directory.GetFiles(spool, "*.json").Single(); var before = await File.ReadAllBytesAsync(manifest, ct);
            var bytes = Directory.GetFiles(spool, "*.bin").Single();
            if (missing) File.Delete(bytes); else await File.WriteAllTextAsync(bytes, "broken", ct);
            var failure = await Record.ExceptionAsync(async () => { await using var refused = await RetainedLegacyUpload.OpenAsync(spool, "scope", target, source, null, ct); });
            if (missing) Assert.IsType<FileNotFoundException>(failure); else Assert.IsType<InvalidDataException>(failure);
            Assert.Equal(before, await File.ReadAllBytesAsync(manifest, ct)); Assert.Equal(!missing, File.Exists(bytes));
        }
        finally { Directory.Delete(directory, true); }
    }

    [Fact]
    public async Task N07_FullSpoolRefusesNewCaptureWithoutDiscardingOriginals()
    {
        var ct = TestContext.Current.CancellationToken;
        var directory = Path.Combine(Path.GetTempPath(), "network-quota-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        try
        {
            for (var i = 0; i < 512; i++) await File.WriteAllTextAsync(Path.Combine(directory, i + ".bin"), "retained", ct);
            var source = Path.Combine(directory, "new-source"); await File.WriteAllTextAsync(source, "new", ct);
            await Assert.ThrowsAsync<IOException>(async () => { await using var refused = await RetainedLegacyUpload.OpenAsync(directory, "scope", new Uri("https://saturn.test/dav/file"), source, null, ct); });
            Assert.Equal(512, Directory.GetFiles(directory, "*.bin").Length);
            Assert.Empty(Directory.GetFiles(directory, "*.json"));
        }
        finally { Directory.Delete(directory, true); }
    }

    [Theory]
    [InlineData(404)] [InlineData(409)]
    public async Task N07_MissingEndpointOrCASRefusalCannotReplaceOriginal(int status)
    {
        var ct = TestContext.Current.CancellationToken;
        var directory = Path.Combine(Path.GetTempPath(), "network-endpoint-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        try
        {
            var source = Path.Combine(directory, "file"); await File.WriteAllBytesAsync(source, new byte[1024 * 1024], ct);
            var handler = new EndpointHandler(status); using var http = new HttpClient(handler);
            var store = new NeptuneStateStore(Path.Combine(directory, "state.db")); await store.InitializeAsync(ct);
            var spool = Path.Combine(directory, "spool"); var root = new Uri("https://saturn.test/dav/sync/test/");
            var client = new WebDavSyncClient(http, store) { RetainedOriginalsDirectory = spool };
            await Assert.ThrowsAsync<HttpRequestException>(() => client.UploadAsync(root, "fixture", "file", source, ct));
            var manifest = Directory.GetFiles(spool, "*.json").Single(); var before = await File.ReadAllBytesAsync(manifest, ct);
            handler.Recovering = true; File.Delete(source);
            await Assert.ThrowsAsync<InvalidDataException>(() => client.RecoverRetainedUploadsAsync(root, "fixture", ct));
            Assert.Equal(0, handler.Puts); Assert.Equal(before, await File.ReadAllBytesAsync(manifest, ct)); Assert.Single(Directory.GetFiles(spool, "*.bin"));
        }
        finally { Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools(); Directory.Delete(directory, true); }
    }
    private sealed class EndpointHandler(int status) : HttpMessageHandler
    {
        public bool Recovering; public int Puts;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (request.Method == HttpMethod.Head) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound));
            if (request.Method == HttpMethod.Put) Puts++;
            if (!Recovering) throw new HttpRequestException("lost prepare ACK");
            return Task.FromResult(new HttpResponseMessage((HttpStatusCode)status));
        }
    }
}
