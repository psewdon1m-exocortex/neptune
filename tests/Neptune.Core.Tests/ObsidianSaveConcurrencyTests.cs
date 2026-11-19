using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Net.Http.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

[CollectionDefinition("Obsidian save concurrency", DisableParallelization = true)]
public sealed class ObsidianSaveConcurrencyCollection { }

[Collection("Obsidian save concurrency")]
public sealed class ObsidianSaveConcurrencyTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private const string Name = "Агентность, допуск контроля, инициативность.md";
    private static readonly Uri Root = new("https://saturn.test/webdav/sync/Fixture/");
    private static string Hash(byte[] bytes) => Convert.ToHexStringLower(SHA256.HashData(bytes));

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(true, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, true, true)]
    public async Task SavingDuringUploadKeepsOneLocalNameAndUploadsTheNextVersion(bool replace, bool large, bool resumable)
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-obsidian-save-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var file = Path.Combine(directory, Name);
        var original = Encoding.UTF8.GetBytes(new string('a', large ? 1024 * 1024 + 31 : 1024));
        var edited = Encoding.UTF8.GetBytes(new string('b', original.Length));
        await File.WriteAllBytesAsync(file, original, Ct);
        byte[]? remote = null;
        var editPerformed = false;
        var published = 0;
        var uploadId = Guid.NewGuid().ToString("D");
        using var received = new MemoryStream();
        string? uploadHash = null;
        HttpResponseMessage Checkpoint(string status = "uploading") => new(HttpStatusCode.OK)
        {
            Content = JsonContent.Create(new { id = uploadId, status, receivedSize = received.Length,
                expectedSize = original.Length, etag = status == "active" ? $"\"sha256-{uploadHash}\"" : null })
        };
        using var http = new HttpClient(new Handler(async request =>
        {
            if (request.Method == HttpMethod.Head)
            {
                // Upload has opened/captured the source, but has not sent bytes.
                // This must not deny an editor's write or atomic replacement.
                if (!editPerformed)
                {
                    editPerformed = true;
                    if (replace)
                    {
                        var temp = Path.Combine(directory, "editor.tmp");
                        await File.WriteAllBytesAsync(temp, edited, Ct);
                        File.Move(temp, file, overwrite: true);
                    }
                    else await File.WriteAllBytesAsync(file, edited, Ct);
                }
                return remote is null ? new(HttpStatusCode.NotFound) : WithEtag(HttpStatusCode.OK, remote);
            }
            // Also cover a release server without resumable upload support.
            if (request.Method == HttpMethod.Post && request.RequestUri!.AbsolutePath == "/api/v1/sync/uploads")
            {
                if (!resumable) return new(HttpStatusCode.NotFound);
                using var body = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(Ct));
                uploadHash = body.RootElement.GetProperty("expectedSha256").GetString();
                Assert.Equal(remote is null ? "*" : $"\"sha256-{Hash(remote)}\"",
                    body.RootElement.GetProperty(remote is null ? "ifNoneMatch" : "ifMatch").GetString());
                received.SetLength(0);
                return Checkpoint();
            }
            if (request.Method == HttpMethod.Patch)
            {
                Assert.Equal(received.Length.ToString(), request.Headers.GetValues("Upload-Offset").Single());
                received.Write(await request.Content!.ReadAsByteArrayAsync(Ct));
                return Checkpoint();
            }
            if (request.Method == HttpMethod.Post && request.RequestUri!.AbsolutePath.EndsWith("/complete", StringComparison.Ordinal))
            {
                remote = received.ToArray();
                Assert.Equal(uploadHash, Hash(remote));
                Assert.Equal(published == 0 ? original : edited, remote);
                published++;
                return Checkpoint("active");
            }
            Assert.Equal(HttpMethod.Put, request.Method);
            Assert.Equal(WebDavSyncClient.RelativeTargetUri(Root, Name), request.RequestUri);
            Assert.Equal(remote is null ? "*" : $"\"sha256-{Hash(remote)}\"",
                request.Headers.GetValues(remote is null ? "If-None-Match" : "If-Match").Single());
            var payload = await request.Content!.ReadAsByteArrayAsync(Ct);
            Assert.Equal(Hash(payload), request.Headers.GetValues("X-Content-Sha256").Single());
            Assert.Equal(published == 0 ? original : edited, payload);
            remote = payload;
            published++;
            return WithEtag(HttpStatusCode.Created, payload);
        }));
        try
        {
            var client = new WebDavSyncClient(http);
            Assert.Equal($"\"sha256-{Hash(original)}\"", await client.UploadAsync(Root, "fixture", Name, file, Hash(original), Ct));
            Assert.Equal(edited, await File.ReadAllBytesAsync(file, Ct));
            Assert.Equal([Name], Directory.GetFiles(directory).Select(Path.GetFileName));
            Assert.Equal($"\"sha256-{Hash(edited)}\"", await client.UploadAsync(Root, "fixture", Name, file, Hash(edited), Ct));
            Assert.Equal(edited, remote);
            Assert.Equal(2, published);
            // Replaying the same version must not create a second name or PUT.
            await client.UploadAsync(Root, "fixture", Name, file, Hash(edited), Ct);
            Assert.Equal(2, published);
            Assert.Equal([Name], Directory.GetFiles(directory).Select(Path.GetFileName));
        }
        finally { Directory.Delete(directory, recursive: true); }
    }

    [Fact]
    public async Task StaleScanDigestCannotPublishReplacementBytesEvenWhenSizeAndTimestampMatch()
    {
        var file = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(file, "old", Ct);
            var stamp = File.GetLastWriteTimeUtc(file);
            var oldHash = Hash(await File.ReadAllBytesAsync(file, Ct));
            await File.WriteAllTextAsync(file, "new", Ct);
            File.SetLastWriteTimeUtc(file, stamp);
            using var http = new HttpClient(new Handler(_ => throw new InvalidOperationException("Changed source must not reach HTTP.")));
            await Assert.ThrowsAsync<LocalSourceChangedException>(() =>
                new WebDavSyncClient(http).UploadAsync(Root, "fixture", Name, file, oldHash, Ct));
            Assert.Equal("new", await File.ReadAllTextAsync(file, Ct));
        }
        finally { File.Delete(file); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedOrCancelledUploadDeletesItsPrivateCopyAndLeavesTheEditorFree(bool cancel)
    {
        var file = Path.GetTempFileName();
        string? spool = null;
        var priorSpools = Directory.GetFiles(Path.GetTempPath(), "neptune-upload-*.tmp").ToHashSet(StringComparer.OrdinalIgnoreCase);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await File.WriteAllTextAsync(file, "valuable note", Ct);
        using var http = new HttpClient(new Handler(async request =>
        {
            if (request.Method == HttpMethod.Head) return new(HttpStatusCode.NotFound);
            await request.Content!.ReadAsByteArrayAsync(Ct);
            spool = Assert.Single(Directory.GetFiles(Path.GetTempPath(), "neptune-upload-*.tmp"), path => !priorSpools.Contains(path));
            Assert.NotEqual(file, spool);
            if (cancel) { cancellation.Cancel(); throw new OperationCanceledException(cancellation.Token); }
            throw new HttpRequestException("Simulated offline server");
        }));
        try
        {
            var client = new WebDavSyncClient(http);
            if (cancel) await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.UploadAsync(Root, "fixture", Name, file, cancellation.Token));
            else await Assert.ThrowsAsync<HttpRequestException>(() => client.UploadAsync(Root, "fixture", Name, file, cancellation.Token));
            Assert.NotNull(spool);
            Assert.False(File.Exists(spool));
            await File.WriteAllTextAsync(file, "editor can save", Ct);
        }
        finally { File.Delete(file); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FullServiceRetriesOrDefersMissingNoteWithoutCleanupAndConvergesOnNextRun(bool returns)
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-obsidian-service-" + Guid.NewGuid().ToString("N"));
        var source = Path.Combine(directory, "obsidian");
        Directory.CreateDirectory(source);
        var file = Path.Combine(source, Name);
        await File.WriteAllTextAsync(file, "old", Ct);
        await File.WriteAllTextAsync(Path.Combine(source, "sibling.md"), "unaffected", Ct);
        var remote = new Dictionary<string, byte[]> { [Name] = Encoding.UTF8.GetBytes("old") };
        var cleanupScans = 0;
        var cancelledUploads = 0;
        var replaced = false;
        var missingReads = 0; var restored = 0; Exception? replacementError = null;
        // Returning on a wall-clock delay can precede the first native read
        // on a slow runner, so it does not prove the intended missing-source
        // observation. Restore only after this exact FileInfo read has thrown.
        EventHandler<System.Runtime.ExceptionServices.FirstChanceExceptionEventArgs> restoreAfterMissing = (_, observed) =>
        {
            if (observed.Exception is not FileNotFoundException missing
                || !string.Equals(missing.FileName, file, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal)) return;
            Interlocked.Increment(ref missingReads);
            if (returns && Interlocked.CompareExchange(ref restored, 1, 0) == 0)
            {
                try { File.WriteAllText(file, "new contents"); }
                catch (Exception failure) { replacementError = failure; }
            }
        };
        using var http = new HttpClient(ServiceHandler(async request =>
        {
            if (request.RequestUri!.AbsolutePath == "/api/v1/sync/uploads/cancel")
            {
                cancelledUploads++;
                return new(HttpStatusCode.OK) { Content = JsonContent.Create(new { status = "absent" }) };
            }
            var name = Uri.UnescapeDataString(request.RequestUri!.AbsolutePath.Split('/').Last());
            if (request.Method.Method == "MKCOL")
            {
                if (!replaced && request.RequestUri.AbsolutePath.EndsWith("/obsidian/", StringComparison.Ordinal))
                {
                    replaced = true;
                    File.Delete(file);

                }
                return new(HttpStatusCode.Created);
            }
            if (request.Method == HttpMethod.Head)
                return remote.TryGetValue(name, out var existing) ? WithEtag(HttpStatusCode.OK, existing) : new(HttpStatusCode.NotFound);
            if (request.Method == HttpMethod.Put)
            {
                var bytes = await request.Content!.ReadAsByteArrayAsync(Ct);
                Assert.Equal(Hash(bytes), request.Headers.GetValues("X-Content-Sha256").Single());
                remote[name] = bytes;
                return WithEtag(HttpStatusCode.Created, bytes);
            }
            Assert.Equal("PROPFIND", request.Method.Method);
            cleanupScans++;
            return new((HttpStatusCode)207) { Content = new StringContent("<d:multistatus xmlns:d=\"DAV:\" />") };
        }));
        await using var service = new WindowsSyncService(new WindowsProfileContext("test", directory, false), http);
        var mapping = new SyncMapping("mapping", "client", source, true, DateTimeOffset.UtcNow);
        var connection = new WindowsConnection(new Uri("https://kernel.test"), "fixture", new string('b', 43), "Assigned");
        try
        {
            var state = new NeptuneStateStore(Path.Combine(directory, "neptune.db"));
            await state.InitializeAsync(Ct);
            await state.AddMappingAsync(mapping, Ct);
            var scope = Hash(Encoding.UTF8.GetBytes("https://saturn.test\n" + connection.SaturnToken));
            await state.SaveSyncUploadCheckpointAsync(new SyncUploadCheckpoint(scope,
                "https://saturn.test/webdav/sync/Assigned/obsidian/temporarily-missing.md", new string('a', 64), "retained", null), Ct);
            WindowsSyncResult result;
            AppDomain.CurrentDomain.FirstChanceException += restoreAfterMissing;
            try { result = await service.SyncAsync("client", [mapping], connection, cancellationToken: Ct); }
            finally { AppDomain.CurrentDomain.FirstChanceException -= restoreAfterMissing; }
            Assert.True(Volatile.Read(ref missingReads) > 0, "The product must actually observe this missing source before the editor returns it.");
            Assert.Null(replacementError);
            Assert.Equal(returns ? 1 : 0, Volatile.Read(ref restored));
            Assert.Equal(returns ? 2 : 1, result.UploadedFiles);
            Assert.Equal(returns ? 0 : 1, result.DeferredFiles);
            Assert.Equal(0, cleanupScans);
            Assert.Equal(0, cancelledUploads);
            Assert.Single(await state.ListSyncUploadCheckpointsAsync(scope, Ct));
            Assert.Empty(result.Mirrors);
            Assert.Equal("unaffected", Encoding.UTF8.GetString(remote["sibling.md"]));
            Assert.Equal(returns ? "new contents" : "old", Encoding.UTF8.GetString(remote[Name]));
            if (!returns) await File.WriteAllTextAsync(file, "new contents", Ct);
            var settled = await service.SyncAsync("client", [mapping], connection, cancellationToken: Ct);
            Assert.Equal(0, settled.DeferredFiles);
            Assert.Equal("new contents", Encoding.UTF8.GetString(remote[Name]));
            Assert.Equal(2, remote.Count);
            Assert.Equal(2, Directory.GetFiles(source).Length);
            Assert.True(cleanupScans > 0);
            Assert.Equal(1, cancelledUploads);
            Assert.Empty(await state.ListSyncUploadCheckpointsAsync(scope, Ct));
            Assert.Single(settled.Mirrors);
        }
        finally { SqliteConnection.ClearAllPools(); Directory.Delete(directory, recursive: true); }
    }

    [Fact]
    public async Task FullServiceDoesNotRejectAnOpenEditorWriteHandleDuringHashing()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-obsidian-writer-" + Guid.NewGuid().ToString("N"));
        var source = Path.Combine(directory, "obsidian");
        Directory.CreateDirectory(source);
        var file = Path.Combine(source, Name);
        await File.WriteAllTextAsync(file, "editor contents", Ct);
        using var http = new HttpClient(ServiceHandler(async request =>
        {
            if (request.Method.Method == "MKCOL") return new(HttpStatusCode.Created);
            if (request.Method == HttpMethod.Head) return new(HttpStatusCode.NotFound);
            if (request.Method == HttpMethod.Put)
            {
                var bytes = await request.Content!.ReadAsByteArrayAsync(Ct);
                Assert.Equal("editor contents", Encoding.UTF8.GetString(bytes));
                return WithEtag(HttpStatusCode.Created, bytes);
            }
            Assert.Equal("PROPFIND", request.Method.Method);
            return new((HttpStatusCode)207) { Content = new StringContent("<d:multistatus xmlns:d=\"DAV:\" />") };
        }));
        await using var service = new WindowsSyncService(new WindowsProfileContext("test", directory, false), http);
        try
        {
            using var editor = new FileStream(file, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
            var mapping = new SyncMapping("mapping", "client", source, true, DateTimeOffset.UtcNow);
            var state = new NeptuneStateStore(Path.Combine(directory, "neptune.db"));
            await state.InitializeAsync(Ct);
            await state.AddMappingAsync(mapping, Ct);
            var result = await service.SyncAsync("client", [mapping],
                new WindowsConnection(new Uri("https://kernel.test"), "fixture", new string('b', 43), "Assigned"), cancellationToken: Ct);
            Assert.Equal(1, result.UploadedFiles);
            Assert.Equal(0, result.DeferredFiles);
        }
        finally { SqliteConnection.ClearAllPools(); Directory.Delete(directory, recursive: true); }
    }

    private static Handler ServiceHandler(Func<HttpRequestMessage, Task<HttpResponseMessage>> dav) => new(async request =>
    {
        var route = request.RequestUri!.AbsolutePath;
        if (route == "/api/v1/register/snapshot")
        {
            using var values = JsonDocument.Parse("""{"services":{"saturn":{"sni":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/1","port":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/2","paths":{"sync":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/3","sync_preferences":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/4"}}}}""");
            return new(HttpStatusCode.OK) { Content = JsonContent.Create(new { schema = "exocortex.register.snapshot.v1", revision = 1,
                checksum = CanonicalJson.ComputeRegisterChecksum(values.RootElement), values = values.RootElement.Clone() }) };
        }
        if (route == "/api/v1/register/resolve") return new(HttpStatusCode.OK) { Content = JsonContent.Create(new { schema = "exocortex.register.resolution.v1", values = new Dictionary<string, object> {
            ["services.saturn.sni"] = new { value = "saturn.test" }, ["services.saturn.port"] = new { value = "443" },
            ["services.saturn.paths.sync"] = new { value = "/webdav/sync/" }, ["services.saturn.paths.sync_preferences"] = new { value = "/preferences" }
        } }) };
        if (route == "/api/v1/device-session/heartbeat") return new(HttpStatusCode.OK) { Content = JsonContent.Create(new {
            id = "82b44ad9-6360-45cc-9a0c-a36cce13b559", syncRootId = "d3eeae64-79d8-492b-b4b7-3b535ebd2a48", syncFolderName = "Assigned" }) };
        if (route.EndsWith("/capabilities", StringComparison.Ordinal) || route == "/preferences") return new(HttpStatusCode.NotFound) { RequestMessage = request };
        return await dav(request);
    });

    private static HttpResponseMessage WithEtag(HttpStatusCode status, byte[] content)
    {
        var response = new HttpResponseMessage(status);
        response.Headers.ETag = new EntityTagHeaderValue($"\"sha256-{Hash(content)}\"");
        return response;
    }

    private sealed class Handler(Func<HttpRequestMessage, Task<HttpResponseMessage>> action) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) => action(request);
    }
}
