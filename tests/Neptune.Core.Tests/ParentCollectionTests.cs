using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Linq;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class ParentCollectionTests
{
    private static readonly Uri Root = new("https://saturn.test/dav/sync/Fixture/");
    private static CancellationToken TestToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task OneHundredConcurrentFilesPrepareEachParentOnceAndKeepChecksumReceipts()
    {
        var file = Path.GetTempFileName();
        var bytes = Encoding.UTF8.GetBytes("singleflight-file-payload");
        await File.WriteAllBytesAsync(file, bytes, TestToken);
        var digest = Convert.ToHexStringLower(SHA256.HashData(bytes));
        var created = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var heads = 0;
        var puts = 0;
        using var http = new HttpClient(new Handler(async (request, cancellationToken) =>
        {
            if (request.Method.Method == "MKCOL")
            {
                created.AddOrUpdate(request.RequestUri!.AbsolutePath, 1, (_, count) => count + 1);
                await Task.Delay(10, cancellationToken);
                return new(HttpStatusCode.Created);
            }
            if (request.Method == HttpMethod.Head) { Interlocked.Increment(ref heads); return new(HttpStatusCode.NotFound); }
            Assert.Equal(HttpMethod.Put, request.Method);
            Assert.Equal("*", request.Headers.GetValues("If-None-Match").Single());
            Assert.Equal(digest, request.Headers.GetValues("X-Content-Sha256").Single());
            Assert.Equal(bytes, await request.Content!.ReadAsByteArrayAsync(cancellationToken));
            Interlocked.Increment(ref puts);
            return Receipt(digest);
        }));
        try
        {
            var client = new WebDavSyncClient(http);
            var results = await Task.WhenAll(Enumerable.Range(0, 100).Select(index =>
                client.UploadAsync(Root, "fixture", $"parent/child/file-{index}.txt", file, digest, TestToken)));
            Assert.All(results, value => Assert.Equal($"\"sha256-{digest}\"", value));
            Assert.Equal(2, created.Count);
            Assert.All(created.Values, count => Assert.Equal(1, count));
            Assert.Equal(100, heads);
            Assert.Equal(100, puts);
        }
        finally { File.Delete(file); }
    }

    [Fact]
    public async Task ConcurrentDistinctParentsHaveBoundedMetadataRequests()
    {
        var active = 0;
        var maximum = 0;
        var requests = 0;
        using var http = new HttpClient(new Handler(async (request, cancellationToken) =>
        {
            Assert.Equal("MKCOL", request.Method.Method);
            var count = Interlocked.Increment(ref active);
            int observed;
            do { observed = Volatile.Read(ref maximum); }
            while (count > observed && Interlocked.CompareExchange(ref maximum, count, observed) != observed);
            Interlocked.Increment(ref requests);
            try { await Task.Delay(10, cancellationToken); return new(HttpStatusCode.Created); }
            finally { Interlocked.Decrement(ref active); }
        }));
        var client = new WebDavSyncClient(http);
        await Task.WhenAll(Enumerable.Range(0, 100).Select(index => client.EnsureDirectoryAsync(Root, "fixture", $"dir-{index}", TestToken)));
        Assert.InRange(maximum, 1, 4);
        Assert.Equal(100, requests);
        Assert.Equal(0, active);
    }

    [Theory]
    [InlineData(405)]
    [InlineData(409)]
    public async Task ExistingDirectoryRequiresMatchingCollectionMetadataAndIsCached(int status)
    {
        var requests = new List<string>();
        using var http = new HttpClient(new Handler((request, _) =>
        {
            requests.Add(request.Method.Method);
            if (request.Method.Method == "MKCOL") return Task.FromResult(new HttpResponseMessage((HttpStatusCode)status));
            Assert.Equal("0", request.Headers.GetValues("Depth").Single());
            return Task.FromResult(Metadata(request.RequestUri!.AbsolutePath));
        }));
        var client = new WebDavSyncClient(http);
        await client.EnsureDirectoryAsync(Root, "fixture", "parent", TestToken);
        await client.EnsureDirectoryAsync(Root, "fixture", "parent", TestToken);
        Assert.Equal(["MKCOL", "PROPFIND"], requests);
    }

    [Fact]
    public async Task LegacyWebdavRouteAcceptsOnlyItsExactCanonicalDavHref()
    {
        using var http = new HttpClient(new Handler((request, _) => Task.FromResult(
            request.Method.Method == "MKCOL" ? new HttpResponseMessage(HttpStatusCode.Conflict)
                : Metadata("/dav/sync/Fixture/parent/"))));
        await new WebDavSyncClient(http).EnsureDirectoryAsync(new Uri("https://saturn.test/webdav/sync/Fixture/"), "fixture", "parent", TestToken);
    }

    [Theory]
    [InlineData("https://other.test/dav/sync/Fixture/parent/")]
    [InlineData("/dav/sync/Other/parent/")]
    [InlineData("/dav/sync/Fixture/Parent/")]
    [InlineData("/dav/sync/Fixture/parent%2Fchild/")]
    [InlineData("/dav/sync/Fixture/parent/?query=1")]
    public async Task MetadataForAnotherOriginRootPathOrCaseIsRejected(string href)
    {
        using var http = new HttpClient(new Handler((request, _) => Task.FromResult(
            request.Method.Method == "MKCOL" ? new HttpResponseMessage(HttpStatusCode.Conflict) : Metadata(href))));
        await Assert.ThrowsAsync<InvalidDataException>(() => new WebDavSyncClient(http).EnsureDirectoryAsync(Root, "fixture", "parent", TestToken));
    }

    [Theory]
    [InlineData(405)]
    [InlineData(409)]
    public async Task FileAtRequiredDirectoryIsRejectedAndDoesNotPoisonCache(int status)
    {
        var collection = false;
        var creates = 0;
        var probes = 0;
        using var http = new HttpClient(new Handler((request, _) =>
        {
            if (request.Method.Method == "MKCOL") { creates++; return Task.FromResult(new HttpResponseMessage((HttpStatusCode)status)); }
            probes++;
            return Task.FromResult(Metadata(request.RequestUri!.AbsolutePath, collection));
        }));
        var client = new WebDavSyncClient(http);
        await Assert.ThrowsAsync<InvalidDataException>(() => client.EnsureDirectoryAsync(Root, "fixture", "parent", TestToken));
        collection = true;
        await client.EnsureDirectoryAsync(Root, "fixture", "parent", TestToken);
        Assert.Equal(2, creates);
        Assert.Equal(2, probes);
    }

    [Fact]
    public async Task UnexplainedConflictNeverSucceedsOrCreatesTheCallerRoot()
    {
        var requests = new List<string>();
        using var http = new HttpClient(new Handler((request, _) =>
        {
            requests.Add(request.Method.Method + " " + request.RequestUri!.AbsolutePath);
            return Task.FromResult(new HttpResponseMessage(request.Method.Method == "MKCOL" ? HttpStatusCode.Conflict : HttpStatusCode.NotFound));
        }));
        var error = await Assert.ThrowsAsync<HttpRequestException>(() => new WebDavSyncClient(http).EnsureDirectoryAsync(Root, "fixture", "parent", TestToken));
        Assert.Equal(HttpStatusCode.NotFound, error.StatusCode);
        Assert.Equal(4, requests.Count);
        Assert.DoesNotContain(requests, value => value == "MKCOL " + Root.AbsolutePath);
    }

    [Fact]
    public async Task MissingCachedAncestorRepreparesOnlyItsBranchOnce()
    {
        var directories = new HashSet<string>(StringComparer.Ordinal) { Root.AbsolutePath };
        var creates = new Dictionary<string, int>(StringComparer.Ordinal);
        using var http = new HttpClient(new Handler((request, _) =>
        {
            var path = request.RequestUri!.AbsolutePath;
            if (request.Method.Method == "PROPFIND") return Task.FromResult(directories.Contains(path) ? Metadata(path) : new HttpResponseMessage(HttpStatusCode.NotFound));
            creates[path] = creates.GetValueOrDefault(path) + 1;
            if (directories.Contains(path)) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.Conflict));
            var parent = new Uri(request.RequestUri, "../").AbsolutePath;
            if (!directories.Contains(parent)) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.Conflict));
            directories.Add(path);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.Created));
        }));
        var client = new WebDavSyncClient(http);
        await client.EnsureDirectoryAsync(Root, "fixture", "a/b", TestToken);
        await client.EnsureDirectoryAsync(Root, "fixture", "unrelated", TestToken);
        directories.Remove(Root.AbsolutePath + "a/");
        directories.Remove(Root.AbsolutePath + "a/b/");
        await client.EnsureDirectoryAsync(Root, "fixture", "a/b/c", TestToken);
        await client.EnsureDirectoryAsync(Root, "fixture", "unrelated", TestToken);
        Assert.Contains(Root.AbsolutePath + "a/b/c/", directories);
        Assert.Equal(2, creates[Root.AbsolutePath + "a/"]);
        Assert.Equal(2, creates[Root.AbsolutePath + "a/b/"]);
        Assert.Equal(2, creates[Root.AbsolutePath + "a/b/c/"]);
        Assert.Equal(1, creates[Root.AbsolutePath + "unrelated/"]);
    }

    [Theory]
    [InlineData(404)]
    [InlineData(409)]
    public async Task FailedUploadInvalidatesParentsWithoutBlindPublicationReplay(int status)
    {
        var file = Path.GetTempFileName();
        var digest = Convert.ToHexStringLower(SHA256.HashData([]));
        var creates = 0;
        var puts = 0;
        using var http = new HttpClient(new Handler((request, _) =>
        {
            if (request.Method.Method == "MKCOL") { creates++; return Task.FromResult(new HttpResponseMessage(HttpStatusCode.Created)); }
            if (request.Method == HttpMethod.Head) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound));
            Assert.Equal(HttpMethod.Put, request.Method);
            puts++;
            return Task.FromResult(puts == 1 ? new HttpResponseMessage((HttpStatusCode)status) : Receipt(digest));
        }));
        try
        {
            var client = new WebDavSyncClient(http);
            await Assert.ThrowsAsync<HttpRequestException>(() => client.UploadAsync(Root, "fixture", "parent/file", file, digest, TestToken));
            Assert.Equal(1, puts);
            Assert.Equal($"\"sha256-{digest}\"", await client.UploadAsync(Root, "fixture", "parent/file", file, digest, TestToken));
            Assert.Equal(2, creates);
            Assert.Equal(2, puts);
        }
        finally { File.Delete(file); }
    }

    [Fact]
    public async Task CacheScopeSeparatesCredentialsRootsAndOrigins()
    {
        var requests = 0;
        using var http = new HttpClient(new Handler((_, _) => { requests++; return Task.FromResult(new HttpResponseMessage(HttpStatusCode.Created)); }));
        var client = new WebDavSyncClient(http);
        await client.EnsureDirectoryAsync(Root, "first", "parent", TestToken);
        await client.EnsureDirectoryAsync(Root, "second", "parent", TestToken);
        await client.EnsureDirectoryAsync(new Uri("https://saturn.test/dav/sync/Other/"), "first", "parent", TestToken);
        await client.EnsureDirectoryAsync(new Uri("https://other.test/dav/sync/Fixture/"), "first", "parent", TestToken);
        await client.EnsureDirectoryAsync(Root, " first ", "parent", TestToken);
        Assert.Equal(4, requests);
    }

    [Fact]
    public async Task ExpiredCacheRequiresNewConfirmationAndCannotHideAChangedType()
    {
        var clock = new Clock();
        var first = true;
        var requests = 0;
        using var http = new HttpClient(new Handler((request, _) =>
        {
            requests++;
            if (first) { first = false; return Task.FromResult(new HttpResponseMessage(HttpStatusCode.Created)); }
            return Task.FromResult(request.Method.Method == "MKCOL" ? new HttpResponseMessage(HttpStatusCode.Conflict) : Metadata(request.RequestUri!.AbsolutePath, false));
        }));
        var client = new WebDavSyncClient(http, state: null, timeProvider: clock);
        await client.EnsureDirectoryAsync(Root, "fixture", "parent", TestToken);
        await client.EnsureDirectoryAsync(Root, "fixture", "parent", TestToken);
        Assert.Equal(1, requests);
        clock.Advance(TimeSpan.FromMinutes(3));
        await Assert.ThrowsAsync<InvalidDataException>(() => client.EnsureDirectoryAsync(Root, "fixture", "parent", TestToken));
        Assert.Equal(3, requests);
    }

    [Fact]
    public async Task LeastRecentlyUsedCacheIsBoundedAndEvictionIsSafe()
    {
        var requests = 0;
        using var http = new HttpClient(new Handler((_, _) => { requests++; return Task.FromResult(new HttpResponseMessage(HttpStatusCode.Created)); }));
        var client = new WebDavSyncClient(http);
        for (var index = 0; index < 4097; index++) await client.EnsureDirectoryAsync(Root, "fixture", $"parent-{index}", TestToken);
        await client.EnsureDirectoryAsync(Root, "fixture", "parent-4096", TestToken);
        Assert.Equal(4097, requests);
        await client.EnsureDirectoryAsync(Root, "fixture", "parent-0", TestToken);
        Assert.Equal(4098, requests);
    }

    [Fact]
    public async Task CancelledEnsureReleasesItsLocksAndDoesNotCacheAnUnconfirmedParent()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var requests = 0;
        using var http = new HttpClient(new Handler(async (_, cancellationToken) =>
        {
            if (Interlocked.Increment(ref requests) == 1)
            {
                entered.SetResult();
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            return new(HttpStatusCode.Created);
        }));
        var client = new WebDavSyncClient(http);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestToken);
        var first = client.EnsureDirectoryAsync(Root, "fixture", "parent", cancellation.Token);
        await entered.Task.WaitAsync(TestToken);
        var second = client.EnsureDirectoryAsync(Root, "fixture", "parent", TestToken);
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        await second;
        await client.EnsureDirectoryAsync(Root, "fixture", "parent", TestToken);
        Assert.Equal(2, requests);
    }

    [Fact]
    public async Task MirrorDeletionInvalidatesItsPreviouslyConfirmedDirectory()
    {
        var creates = 0;
        using var http = new HttpClient(new Handler((request, _) =>
        {
            if (request.Method.Method == "MKCOL") { creates++; return Task.FromResult(new HttpResponseMessage(HttpStatusCode.Created)); }
            if (request.Method == HttpMethod.Delete) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NoContent));
            var document = XDocument.Parse(MetadataText(Root.AbsolutePath, "Fixture"));
            if (request.RequestUri!.AbsolutePath == Root.AbsolutePath)
                document.Root!.Add(XDocument.Parse(MetadataText(Root.AbsolutePath + "removed/", "removed")).Root!.Elements());
            else document = XDocument.Parse(MetadataText(request.RequestUri.AbsolutePath, "removed"));
            return Task.FromResult(new HttpResponseMessage((HttpStatusCode)207) { Content = new StringContent(document.ToString(), Encoding.UTF8, "application/xml") });
        }));
        var client = new WebDavSyncClient(http);
        await client.EnsureDirectoryAsync(Root, "fixture", "removed", TestToken);
        Assert.Equal(1, await client.MirrorAsync(Root, "fixture", new HashSet<string>(), new HashSet<string>(), cancellationToken: TestToken));
        await client.EnsureDirectoryAsync(Root, "fixture", "removed", TestToken);
        Assert.Equal(2, creates);
    }

    [Theory]
    [InlineData("<d:multistatus xmlns:d=\"DAV:\" />")]
    [InlineData("<d:multistatus xmlns:d=\"DAV:\"><d:response><d:href>/dav/sync/Fixture/parent/</d:href><d:propstat><d:prop><d:resourcetype><d:collection /></d:resourcetype></d:prop><d:status>HTTP/1.1 404 Not Found</d:status></d:propstat></d:response></d:multistatus>")]
    public async Task EmptyOrFailedMetadataCannotConfirmACollection(string xml)
    {
        using var http = new HttpClient(new Handler((request, _) => Task.FromResult(request.Method.Method == "MKCOL"
            ? new HttpResponseMessage(HttpStatusCode.Conflict)
            : new HttpResponseMessage((HttpStatusCode)207) { Content = new StringContent(xml, Encoding.UTF8, "application/xml") })));
        await Assert.ThrowsAsync<InvalidDataException>(() => new WebDavSyncClient(http).EnsureDirectoryAsync(Root, "fixture", "parent", TestToken));
    }

    [Fact]
    public async Task OversizedMetadataIsRejectedBeforeItCanEnterTheCache()
    {
        using var http = new HttpClient(new Handler((request, _) => Task.FromResult(request.Method.Method == "MKCOL"
            ? new HttpResponseMessage(HttpStatusCode.Conflict)
            : new HttpResponseMessage((HttpStatusCode)207) { Content = new StringContent(new string('x', 65537), Encoding.UTF8, "application/xml") })));
        await Assert.ThrowsAsync<InvalidDataException>(() => new WebDavSyncClient(http).EnsureDirectoryAsync(Root, "fixture", "parent", TestToken));
    }

    private static HttpResponseMessage Receipt(string digest)
    {
        var response = new HttpResponseMessage(HttpStatusCode.Created);
        response.Headers.TryAddWithoutValidation("ETag", $"\"sha256-{digest}\"");
        return response;
    }

    private static HttpResponseMessage Metadata(string href, bool collection = true)
    {
        XNamespace dav = "DAV:";
        var document = new XDocument(new XElement(dav + "multistatus", new XElement(dav + "response",
            new XElement(dav + "href", href), new XElement(dav + "propstat",
                new XElement(dav + "prop", new XElement(dav + "resourcetype", collection ? new XElement(dav + "collection") : null)),
                new XElement(dav + "status", "HTTP/1.1 200 OK")))));
        return new HttpResponseMessage((HttpStatusCode)207) { Content = new StringContent(document.ToString(), Encoding.UTF8, "application/xml") };
    }

    private static string MetadataText(string href, string displayName)
    {
        XNamespace dav = "DAV:";
        return new XDocument(new XElement(dav + "multistatus", new XElement(dav + "response", new XElement(dav + "href", href),
            new XElement(dav + "propstat", new XElement(dav + "prop", new XElement(dav + "displayname", displayName),
                new XElement(dav + "resourcetype", new XElement(dav + "collection"))), new XElement(dav + "status", "HTTP/1.1 200 OK"))))).ToString();
    }

    private sealed class Handler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> action) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) => action(request, cancellationToken);
    }

    private sealed class Clock : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.UtcNow;
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan elapsed) => _now += elapsed;
    }
}
