using System.Text.Json;
using System.Net;
using System.Net.Http.Json;
using System.Text;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class CoreTests
{
    [Fact]
    public void RegisterChecksumSortsObjectKeysButKeepsArrayOrder()
    {
        using var values = JsonDocument.Parse("""{"z":[2,1],"a":{"y":true,"x":"ok"}}""");
        var left = CanonicalJson.ComputeRegisterChecksum(values.RootElement);
        using var reordered = JsonDocument.Parse("""{"a":{"x":"ok","y":true},"z":[2,1]}""");
        var right = CanonicalJson.ComputeRegisterChecksum(reordered.RootElement);
        Assert.Equal(left, right);
    }

    [Fact]
    public async Task ClientIdentityIsStableInsideOneStateDirectory()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-tests", Guid.NewGuid().ToString("N"));
        try
        {
            var store = new ClientIdentityStore(directory);
            var cancellationToken = TestContext.Current.CancellationToken;
            Assert.Equal(await store.GetOrCreateAsync(cancellationToken), await store.GetOrCreateAsync(cancellationToken));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task ConcurrentStartupConvergesOnOneClientIdentity()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-tests", Guid.NewGuid().ToString("N"));
        try
        {
            var cancellationToken = TestContext.Current.CancellationToken;
            var identities = await Task.WhenAll(Enumerable.Range(0, 24)
                .Select(_ => new ClientIdentityStore(directory).GetOrCreateAsync(cancellationToken)));
            Assert.Single(identities.Distinct(StringComparer.Ordinal));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void IdempotencyKeySeparatesClientsAndProjects()
    {
        var now = DateTimeOffset.UtcNow;
        var run = new BackupRun("run", "saturn", "client-a", "spooled", null, 10, "hash", null, 0, 0, now, now, null);
        var coordinator = new BackupCoordinator(null!, "client-a", "unused");
        var key = coordinator.CreateIdempotencyKey(run);
        Assert.StartsWith("neptune-", key);
        Assert.Equal(72, key.Length);
        Assert.NotEqual(key, coordinator.CreateIdempotencyKey(run with { ClientInstanceId = "client-b" }));
        Assert.NotEqual(key, coordinator.CreateIdempotencyKey(run with { ProjectId = "kernel" }));
    }

    [Fact]
    public async Task JournalKeepsRecoverableRunsIsolatedByClient()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-tests", Guid.NewGuid().ToString("N"));
        try
        {
            var store = new NeptuneStateStore(Path.Combine(directory, "state.db"));
            var cancellationToken = TestContext.Current.CancellationToken;
            await store.InitializeAsync(cancellationToken);
            var now = DateTimeOffset.UtcNow;
            await store.AddRunAsync(new BackupRun("one", "kernel", "client-a", "spooled", "one.zip", 1, "a", null, 0, 0, now, now, null), cancellationToken);
            await store.AddRunAsync(new BackupRun("two", "kernel", "client-b", "spooled", "two.zip", 1, "b", null, 0, 0, now, now, null), cancellationToken);
            var recovered = await store.ListRecoverableRunsAsync("client-a", cancellationToken);
            Assert.Single(recovered);
            Assert.Equal("one", recovered[0].RunId);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void WebDavPathSeparatesClientAndMappingAndEscapesSegments()
    {
        var uri = WebDavSyncClient.TargetUri(new Uri("https://saturn.example/dav/sync/"), "client-a", "mapping-b", "folder/a b.txt");
        Assert.Equal("https://saturn.example/dav/sync/client-a/mapping-b/folder/a%20b.txt", uri.AbsoluteUri);
    }

    [Fact]
    public async Task WebDavNamespaceIsClaimedAndAccentComesFromSaturn()
    {
        var handler = new NamespaceProtocolHandler();
        var client = new WebDavSyncClient(new HttpClient(handler));
        var cancellationToken = TestContext.Current.CancellationToken;

        var root = await client.ClaimNamespaceAsync(
            new Uri("https://saturn.example/dav/sync/"), "token", "User PC", "client-a", cancellationToken);
        var accent = await client.ReadAccentAsync(
            new Uri("https://saturn.example/api/v1/sync/preferences"), "token", cancellationToken);

        Assert.Equal("https://saturn.example/dav/sync/User%20PC/", root.AbsoluteUri);
        Assert.Equal("client-a", handler.Owner);
        Assert.Equal("#7357FF", accent);
    }

    [Fact]
    public async Task WebDavMirrorDeletesEntriesMissingLocally()
    {
        var handler = new MirrorProtocolHandler();
        var client = new WebDavSyncClient(new HttpClient(handler));

        await client.MirrorAsync(
            new Uri("https://saturn.example/dav/sync/User%20PC/Project/"),
            "token",
            new HashSet<string>(["keep.txt"], StringComparer.Ordinal),
            new HashSet<string>(StringComparer.Ordinal),
            TestContext.Current.CancellationToken);

        Assert.Contains("/dav/sync/User%20PC/Project/stale.txt", handler.Deleted);
        Assert.Contains("/dav/sync/User%20PC/Project/old/", handler.Deleted);
        Assert.DoesNotContain("/dav/sync/User%20PC/Project/keep.txt", handler.Deleted);
    }

    [Fact]
    public async Task SaturnUploadUsesCapabilitiesAndPreservesExactBytes()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var spool = Path.Combine(directory, "archive.zip");
        var payload = Encoding.UTF8.GetBytes("exact archive bytes");
        await File.WriteAllBytesAsync(spool, payload, TestContext.Current.CancellationToken);
        try
        {
            var handler = new BackupProtocolHandler(payload);
            var now = DateTimeOffset.UtcNow;
            var run = new BackupRun("run", "kernel", "client-a", "spooled", spool, payload.Length,
                Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(payload)), null, 0, 0, now, now, null);
            var receipt = await new SaturnBackupClient(new HttpClient(handler), chunkSizeBytes: 1024).UploadAsync(
                new Uri("https://saturn.example/api/v1/backups/"), "kernel", "token", run, "neptune-idempotency", "0.1.0", TestContext.Current.CancellationToken);
            Assert.Equal(payload, handler.Uploaded.ToArray());
            Assert.Equal(run.Sha256, receipt.Sha256);
            Assert.True(handler.PatchCount > 1);
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, recursive: true);
        }
    }

    private sealed class BackupProtocolHandler(byte[] expected) : HttpMessageHandler
    {
        public MemoryStream Uploaded { get; } = new();
        public int PatchCount { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var path = request.RequestUri!.AbsolutePath;
            if (request.Method == HttpMethod.Get && path.EndsWith("/capabilities"))
                return Json(new { schema = "saturn.backup-ingest.capabilities.v1", protocolVersion = 1, resumable = true, checksum = "sha256", maxChunkBytes = 5, archiveEncryptionDeclaredPerRun = true });
            if (request.Method == HttpMethod.Post && path.EndsWith("/runs"))
                return Json(new { id = "saturn-run", state = "pending", receivedSize = 0 });
            if (request.Method == HttpMethod.Head)
            {
                var response = new HttpResponseMessage(HttpStatusCode.NoContent);
                response.Headers.TryAddWithoutValidation("Upload-Offset", Uploaded.Length.ToString());
                return response;
            }
            if (request.Method == HttpMethod.Patch)
            {
                Assert.Equal(Uploaded.Length.ToString(), request.Headers.GetValues("Upload-Offset").Single());
                var chunk = await request.Content!.ReadAsByteArrayAsync(cancellationToken);
                await Uploaded.WriteAsync(chunk, cancellationToken);
                PatchCount++;
                var response = new HttpResponseMessage(HttpStatusCode.NoContent);
                response.Headers.TryAddWithoutValidation("Upload-Offset", Uploaded.Length.ToString());
                return response;
            }
            if (request.Method == HttpMethod.Post && path.EndsWith("/complete"))
                return Json(new { receipt = new { schema = "vault.service-backup-receipt.v1", runId = "saturn-run", serviceId = "service-id", serviceSlug = "kernel", logicalPath = "/backups/kernel/archive.zip", sizeBytes = expected.Length, sha256 = Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(expected)), committedAt = DateTimeOffset.UtcNow } });
            return new HttpResponseMessage(HttpStatusCode.NotFound);
        }

        private static HttpResponseMessage Json<T>(T value) => new(HttpStatusCode.OK) { Content = JsonContent.Create(value) };
    }

    private sealed class NamespaceProtocolHandler : HttpMessageHandler
    {
        public string? Owner { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Method.Method == "PROPFIND") return new HttpResponseMessage(HttpStatusCode.NotFound);
            if (request.Method.Method == "MKCOL") return new HttpResponseMessage(HttpStatusCode.Created);
            if (request.Method == HttpMethod.Put && request.RequestUri!.AbsolutePath.EndsWith("/_neptune-owner", StringComparison.Ordinal))
            {
                Owner = await request.Content!.ReadAsStringAsync(cancellationToken);
                return new HttpResponseMessage(HttpStatusCode.Created);
            }
            if (request.Method == HttpMethod.Get && request.RequestUri!.AbsolutePath.EndsWith("/api/v1/sync/preferences", StringComparison.Ordinal))
                return new HttpResponseMessage(HttpStatusCode.OK) { Content = JsonContent.Create(new { accentColor = "#7357FF" }) };
            return new HttpResponseMessage(HttpStatusCode.NotFound);
        }
    }

    private sealed class MirrorProtocolHandler : HttpMessageHandler
    {
        public List<string> Deleted { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var path = request.RequestUri!.AbsolutePath;
            if (request.Method.Method == "PROPFIND")
            {
                var children = path.EndsWith("/old/", StringComparison.Ordinal)
                    ? ""
                    : """
                      <d:response><d:href>/dav/sync/User%20PC/Project/keep.txt</d:href><d:propstat><d:prop><d:displayname>keep.txt</d:displayname><d:getcontentlength>1</d:getcontentlength></d:prop></d:propstat></d:response>
                      <d:response><d:href>/dav/sync/User%20PC/Project/stale.txt</d:href><d:propstat><d:prop><d:displayname>stale.txt</d:displayname><d:getcontentlength>1</d:getcontentlength></d:prop></d:propstat></d:response>
                      <d:response><d:href>/dav/sync/User%20PC/Project/old/</d:href><d:propstat><d:prop><d:displayname>old</d:displayname><d:resourcetype><d:collection /></d:resourcetype></d:prop></d:propstat></d:response>
                      """;
                var xml = $"""
                    <?xml version="1.0" encoding="utf-8"?>
                    <d:multistatus xmlns:d="DAV:">
                      <d:response><d:href>{path}</d:href><d:propstat><d:prop><d:displayname>Project</d:displayname><d:resourcetype><d:collection /></d:resourcetype></d:prop></d:propstat></d:response>
                      {children}
                    </d:multistatus>
                    """;
                return Task.FromResult(new HttpResponseMessage((HttpStatusCode)207) { Content = new StringContent(xml, Encoding.UTF8, "application/xml") });
            }
            if (request.Method == HttpMethod.Delete)
            {
                Deleted.Add(path);
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NoContent));
            }
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound));
        }
    }
}
