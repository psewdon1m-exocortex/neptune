using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class RetainedLegacyUploadTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SourceRemainsEditableWhileRetainedUploadWaitsForNetwork(bool replace)
    {
        var ct=TestContext.Current.CancellationToken;
        var directory=Path.Combine(Path.GetTempPath(),"neptune-editor-network-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        var reached=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resume=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            var file=Path.Combine(directory,"note.md");var original=Encoding.UTF8.GetBytes("original version");
            await File.WriteAllBytesAsync(file,original,ct);
            var digest=Convert.ToHexStringLower(SHA256.HashData(original));
            using var http=new HttpClient(new Handler(async request=>
            {
                if(request.Method==HttpMethod.Head){reached.TrySetResult();await resume.Task.WaitAsync(ct);return new(HttpStatusCode.NotFound);}
                Assert.Equal(HttpMethod.Put,request.Method);
                Assert.Equal(original,await request.Content!.ReadAsByteArrayAsync(ct));
                var response=new HttpResponseMessage(HttpStatusCode.Created);response.Headers.ETag=new($"\"sha256-{digest}\"");return response;
            }));
            var store=new NeptuneStateStore(Path.Combine(directory,"state.db"));await store.InitializeAsync(ct);
            var client=new WebDavSyncClient(http,store){RetainedOriginalsDirectory=Path.Combine(directory,"originals")};
            var upload=client.UploadAsync(new Uri("https://saturn.test/dav/sync/fixture/"),"fixture","note.md",file,digest,ct);
            try
            {
                await reached.Task.WaitAsync(TimeSpan.FromSeconds(10),ct);
                if(replace){var temp=Path.Combine(directory,"editor.tmp");await File.WriteAllTextAsync(temp,"new editor bytes",ct);File.Move(temp,file,true);}
                else await File.WriteAllTextAsync(file,"new editor bytes",ct);
                Assert.False(upload.IsCompleted);
            }
            finally{resume.TrySetResult();await upload;}
            Assert.Equal("new editor bytes",await File.ReadAllTextAsync(file,ct));
            Assert.Single(Directory.GetFiles(directory,"*.md"));
            Assert.Empty(Directory.GetFiles(Path.Combine(directory,"originals"),"*.bin"));
        }
        finally{resume.TrySetResult();Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();Directory.Delete(directory,true);}
    }

    [Theory]
    [InlineData("prepare", false, false)] [InlineData("append", false, false)] [InlineData("complete", false, false)]
    [InlineData("prepare", true, false)] [InlineData("append", true, false)] [InlineData("complete", true, false)]
    [InlineData("prepare", false, true)] [InlineData("append", false, true)] [InlineData("complete", false, true)]
    public async Task RestartRecoversSealedOriginalBeforeChangedOrDeletedSource(string lostPhase, bool deleted, bool unchanged)
    {
        var ct = TestContext.Current.CancellationToken;
        var directory = Path.Combine(Path.GetTempPath(), "neptune-network-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        try
        {
            var source = Path.Combine(directory, "data.bin"); var database = Path.Combine(directory, "state.db"); var spool = Path.Combine(directory, "originals");
            var original = new byte[1024 * 1024 + 13]; RandomNumberGenerator.Fill(original); await File.WriteAllBytesAsync(source, original, ct);
            var hash = Convert.ToHexStringLower(SHA256.HashData(original)); var etag = "\"sha256-" + hash + "\"";
            var id = Guid.NewGuid().ToString("D"); string? key = null, remoteEtag = null; long offset = 0; var lost = false; var publications = 0;
            using var accepted = new MemoryStream();
            HttpResponseMessage Status() => new(HttpStatusCode.OK) { Content = new StringContent(JsonSerializer.Serialize(new
            { id, status = remoteEtag is null ? "uploading" : "active", receivedSize = offset, expectedSize = original.Length, etag = remoteEtag }), Encoding.UTF8, "application/json") };
            void Lose(string phase) { if (!lost && lostPhase == phase) { lost = true; throw new HttpRequestException("ACK lost after durable server write"); } }
            using var http = new HttpClient(new Handler(async request =>
            {
                if (request.Method == HttpMethod.Head)
                { var response = new HttpResponseMessage(remoteEtag is null ? HttpStatusCode.NotFound : HttpStatusCode.OK); if (remoteEtag is not null) response.Headers.ETag = new(remoteEtag); return response; }
                if (request.RequestUri!.AbsolutePath.EndsWith("/cancel"))
                    return new(HttpStatusCode.OK) { Content = new StringContent("{\"status\":\"active\"}", Encoding.UTF8, "application/json") };
                if (request.RequestUri.AbsolutePath.EndsWith("/complete"))
                { Assert.Equal(original, accepted.ToArray()); publications++; remoteEtag = etag; Lose("complete"); return Status(); }
                if (request.Method == HttpMethod.Post)
                {
                    using var json = JsonDocument.Parse(await request.Content!.ReadAsByteArrayAsync(ct)); var body = json.RootElement;
                    var currentKey = body.GetProperty("idempotencyKey").GetString(); key ??= currentKey; Assert.Equal(key, currentKey);
                    Assert.Equal(hash, body.GetProperty("expectedSha256").GetString()); Assert.Equal("*", body.GetProperty("ifNoneMatch").GetString());
                    Lose("prepare"); return Status();
                }
                Assert.Equal(HttpMethod.Patch, request.Method);
                Assert.Equal(offset.ToString(), request.Headers.GetValues("Upload-Offset").Single());
                var bytes = await request.Content!.ReadAsByteArrayAsync(ct); accepted.Write(bytes); offset += bytes.Length; Lose("append"); return Status();
            }));
            var store = new NeptuneStateStore(database); await store.InitializeAsync(ct);
            var root = new Uri("https://saturn.test/dav/sync/fixture/");
            var client = new WebDavSyncClient(http, store) { RetainedOriginalsDirectory = spool };
            await Assert.ThrowsAsync<HttpRequestException>(() => client.UploadAsync(root, "fixture", "data.bin", source, ct));
            Assert.Single(Directory.GetFiles(spool, "*.bin"));
            if (deleted) File.Delete(source); else if (!unchanged) await File.WriteAllTextAsync(source, "new generation must wait", ct);
            var restarted = new NeptuneStateStore(database); await restarted.InitializeAsync(ct);
            await new WebDavSyncClient(http, restarted) { RetainedOriginalsDirectory = spool }.RecoverRetainedUploadsAsync(root, "fixture", ct);
            Assert.Equal(1, publications); Assert.Equal(original, accepted.ToArray());
            Assert.Empty(Directory.GetFiles(spool, "*.bin")); Assert.Empty(Directory.GetFiles(spool, "*.json"));
            if (unchanged) Assert.Equal(original, await File.ReadAllBytesAsync(source, ct));
            else if (!deleted) Assert.Equal("new generation must wait", await File.ReadAllTextAsync(source, ct));
        }
        finally { Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools(); Directory.Delete(directory, recursive: true); }
    }
    private sealed class Handler(Func<HttpRequestMessage, Task<HttpResponseMessage>> action) : HttpMessageHandler
    { protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct) => action(request); }
}
