using System.IO.Compression;
using System.Net;
using System.Text;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class MastermindBoundaryTests
{
    [Theory]
    [InlineData("https://outside.test/webdav/mastermind/gone.md", "gone.md")]
    [InlineData("/webdav/sibling/gone.md", "gone.md")]
    [InlineData("/webdav/mastermind/other.md", "gone.md")]
    [InlineData("/webdav/mastermind/folder%2Fgone.md", "folder/gone.md")]
    public async Task UntrustedDavHrefCannotSendCredentialsOrDelete(string href, string name)
    {
        var requests = new List<string>();
        using var http = new HttpClient(new Handler(request =>
        {
            requests.Add(request.Method.Method + " " + request.RequestUri!.Host);
            Assert.Equal("saturn.test", request.RequestUri.Host);
            return Xml(Row(href, name));
        }));
        await Assert.ThrowsAsync<InvalidDataException>(() => new WebDavSyncClient(http).MirrorAsync(
            new Uri("https://saturn.test/webdav/mastermind/"), "fixture", new HashSet<string>(), new HashSet<string>(),
            cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(["PROPFIND saturn.test"], requests);
    }

    [Fact]
    public async Task DavSelfMayBeAbsentAndOnlyTheExactMissingChildIsDeleted()
    {
        var requests = new List<string>();
        using var http = new HttpClient(new Handler(request =>
        {
            requests.Add(request.Method.Method + " " + request.RequestUri!.AbsolutePath);
            return request.Method.Method == "PROPFIND"
                ? Xml(Row("/webdav/mastermind/keep.md", "keep.md") + Row("/webdav/mastermind/gone.md", "gone.md"))
                : new HttpResponseMessage(HttpStatusCode.NoContent);
        }));
        var count = await new WebDavSyncClient(http).MirrorAsync(new Uri("https://saturn.test/webdav/mastermind/"), "fixture",
            new HashSet<string> { "keep.md" }, new HashSet<string>(), cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, count);
        Assert.Equal(["PROPFIND /webdav/mastermind/", "DELETE /webdav/mastermind/gone.md"], requests);
    }

    [Fact]
    public async Task OversizeDavMetadataIsRejectedBeforeDeletingAnything()
    {
        var requests = 0;
        using var http = new HttpClient(new Handler(_ =>
        {
            requests++;
            var response = Xml("");
            response.Content.Headers.ContentLength = 8 * 1024 * 1024 + 1;
            return response;
        }));
        await Assert.ThrowsAsync<InvalidDataException>(() => new WebDavSyncClient(http).MirrorAsync(
            new Uri("https://saturn.test/webdav/mastermind/"), "fixture", new HashSet<string>(), new HashSet<string>(),
            cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(1, requests);
    }

    [Theory]
    [InlineData("/webdav/mastermind/Fixture%20(2).md", "Fixture (2).md", "Fixture%20%282%29.md")]
    [InlineData("/webdav/mastermind/caf%c3%a9.md", "café.md", "caf%C3%A9.md")]
    public async Task EquivalentDavEncodingUsesCanonicalSafeRequest(string href, string name, string suffix)
    {
        var requests = new List<string>();
        using var http = new HttpClient(new Handler(request =>
        {
            requests.Add(request.Method.Method + " " + request.RequestUri!.AbsolutePath);
            return request.Method.Method == "PROPFIND" ? Xml(Row(href, name)) : new HttpResponseMessage(HttpStatusCode.NoContent);
        }));
        Assert.Equal(1, await new WebDavSyncClient(http).MirrorAsync(new Uri("https://saturn.test/webdav/mastermind/"), "fixture",
            new HashSet<string> { "local-kept.md" }, new HashSet<string>(), cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal("DELETE /webdav/mastermind/" + suffix, requests[1]);
    }

    [Fact]
    public void ImplicitDirectoryCaseCollisionRejectedBeforeExtraction()
    {
        var directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        try
        {
            var file = Path.Combine(directory, "source.zip");var output = Path.Combine(directory, "out");Directory.CreateDirectory(output);
            using (var zip = ZipFile.Open(file, ZipArchiveMode.Create)) { zip.CreateEntry("Folder/a.md");zip.CreateEntry("folder/b.md"); }
            Assert.Throws<InvalidDataException>(() => MirrorArchive.Extract(file, output, TestContext.Current.CancellationToken));
            Assert.Empty(Directory.GetFileSystemEntries(output));
        }
        finally { Directory.Delete(directory, true); }
    }

    [Fact]
    public async Task FailedExportReplacesStaleSuccessAndCanRetryTheSameCommand()
    {
        var directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        try
        {
            var ct = TestContext.Current.CancellationToken;
            var store = new NeptuneStateStore(Path.Combine(directory, "state.db"));await store.InitializeAsync(ct);
            var registration = new ProjectRegistration("kernel", new Uri("http://127.0.0.1/backup"), "control", "export", "saturn", "slug", true, 24);
            var coordinator = new BackupCoordinator(store, "client", directory);
            await Assert.ThrowsAsync<IOException>(() => coordinator.ExportAsync(registration, (_, _) => throw new IOException("fixture"), ct, "samecommand"));
            var failed = (await store.GetProjectRunSummaryAsync("client", "kernel", ct)).Latest!;
            Assert.Equal("export-failed", failed.State);Assert.Equal("EXPORT_NOT_COMPLETED", failed.Error);Assert.Null(failed.SpoolPath);
            var completed = await coordinator.ExportAsync(registration, (p, token) => File.WriteAllTextAsync(p, "archive", token), ct, "samecommand");
            Assert.Equal("spooled", completed.State);Assert.Equal(2, completed.Attempt);
            Assert.Equal("samecommand", completed.RunId);
        }
        finally { SqliteConnection.ClearAllPools();Directory.Delete(directory, true); }
    }

    [Fact]
    public void FullUnicodeCollisionRejectedAndUnambiguousDecomposedNamePreserved()
    {
        var directory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        try
        {
            var file = Path.Combine(directory, "source.zip");var output = Path.Combine(directory, "out");Directory.CreateDirectory(output);
            using (var zip = ZipFile.Open(file, ZipArchiveMode.Create)) { zip.CreateEntry("Straße.md");zip.CreateEntry("STRASSE.md"); }
            Assert.Throws<InvalidDataException>(() => MirrorArchive.Extract(file, output, TestContext.Current.CancellationToken));Assert.Empty(Directory.GetFiles(output));File.Delete(file);
            using (var zip = ZipFile.Open(file, ZipArchiveMode.Create)) zip.CreateEntry("cafe\u0301.md");
            MirrorArchive.Extract(file, output, TestContext.Current.CancellationToken);Assert.True(File.Exists(Path.Combine(output, "cafe\u0301.md")));
        }
        finally { Directory.Delete(directory, true); }
    }

    [Fact]
    public async Task CredentialReadRejectsOversizeBeforeDecoding()
    {
        var path = Path.GetTempFileName();
        try
        {
            using (var stream = File.OpenWrite(path)) stream.SetLength(128 * 1024 + 1);
            await Assert.ThrowsAsync<InvalidDataException>(() => CredentialFile.ReadAsync(path, TestContext.Current.CancellationToken));
            await File.WriteAllTextAsync(path, " fixture \n", TestContext.Current.CancellationToken);
            Assert.Equal("fixture", await CredentialFile.ReadAsync(path, TestContext.Current.CancellationToken));
        }
        finally { File.Delete(path); }
    }

    private static string Row(string href, string name) => $"<d:response><d:href>{href}</d:href><d:propstat><d:prop><d:displayname>{name}</d:displayname></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>";
    private static HttpResponseMessage Xml(string rows) => new((HttpStatusCode)207) { Content = new StringContent("<d:multistatus xmlns:d=\"DAV:\">" + rows + "</d:multistatus>", Encoding.UTF8, "application/xml") };
    private sealed class Handler(Func<HttpRequestMessage, HttpResponseMessage> send) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) => Task.FromResult(send(request));
    }
}
