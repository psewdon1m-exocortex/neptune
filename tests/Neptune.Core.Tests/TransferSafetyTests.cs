using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class TransferSafetyTests
{
    private sealed class Handler(Func<HttpRequestMessage, Task<HttpResponseMessage>> action) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) => action(request);
    }

    [Theory]
    [InlineData("/dav/sync/Fixture/")]
    [InlineData("/webdav/sync/Fixture/")]
    public async Task ResumesFromDurableOffsetAfterLostChunkAcknowledgement(string davPath)
    {
        var file = Path.GetTempFileName();
        var content = new byte[2 * 1024 * 1024 + 13]; RandomNumberGenerator.Fill(content);
        await File.WriteAllBytesAsync(file, content, TestContext.Current.CancellationToken);
        var digest = Convert.ToHexStringLower(SHA256.HashData(content));
        var id = Guid.NewGuid().ToString("D"); long offset = 0; var patches = 0; string? key = null;
        var accepted = new MemoryStream();
        HttpResponseMessage Checkpoint(string status = "uploading") => new(HttpStatusCode.OK) { Content = new StringContent(JsonSerializer.Serialize(new { id, status, receivedSize = offset, expectedSize = content.Length, etag = status == "active" ? $"\"sha256-{digest}\"" : null }), Encoding.UTF8, "application/json") };
        using var http = new HttpClient(new Handler(async request =>
        {
            if (request.Method == HttpMethod.Head) return new(HttpStatusCode.NotFound);
            if (request.Method.Method == "MKCOL") return new(HttpStatusCode.Created);
            if (request.Method == HttpMethod.Post && request.RequestUri!.AbsolutePath == "/api/v1/sync/uploads")
            {
                using var value = JsonDocument.Parse(await request.Content!.ReadAsStringAsync());
                var nextKey = value.RootElement.GetProperty("idempotencyKey").GetString();
                if (key is not null) Assert.Equal(key, nextKey); key = nextKey;
                Assert.Equal("sync/Fixture/data.bin", value.RootElement.GetProperty("path").GetString());
                return Checkpoint();
            }
            if (request.Method == HttpMethod.Patch)
            {
                Assert.Equal(offset.ToString(), request.Headers.GetValues("Upload-Offset").Single());
                var bytes = await request.Content!.ReadAsByteArrayAsync(); accepted.Write(bytes); offset += bytes.Length; patches++;
                if (patches == 1) throw new HttpRequestException("response lost after durable commit");
                return Checkpoint();
            }
            Assert.Equal(HttpMethod.Post, request.Method); Assert.EndsWith("/complete", request.RequestUri!.AbsolutePath);
            Assert.Equal(content, accepted.ToArray()); return Checkpoint("active");
        }));
        try
        {
            var client = new WebDavSyncClient(http); var root = new Uri("https://saturn.test" + davPath);
            await Assert.ThrowsAsync<HttpRequestException>(() => client.UploadAsync(root, "fixture", "data.bin", file, TestContext.Current.CancellationToken));
            Assert.Equal(1024 * 1024, offset);
            Assert.Equal($"\"sha256-{digest}\"", await client.UploadAsync(root, "fixture", "data.bin", file, TestContext.Current.CancellationToken));
            Assert.Equal(3, patches);
        }
        finally { File.Delete(file); accepted.Dispose(); }
    }

    [Fact]
    public async Task RejectsIncorrectUploadChecksumReceipt()
    {
        var file = Path.GetTempFileName(); await File.WriteAllTextAsync(file, "valuable", TestContext.Current.CancellationToken);
        using var http = new HttpClient(new Handler(request =>
        {
            var response = new HttpResponseMessage(request.Method == HttpMethod.Head ? HttpStatusCode.NotFound : HttpStatusCode.Created);
            if (request.Method == HttpMethod.Put) response.Headers.TryAddWithoutValidation("ETag", "\"sha256-" + new string('0',64) + "\"");
            return Task.FromResult(response);
        }));
        try { await Assert.ThrowsAsync<InvalidDataException>(() => new WebDavSyncClient(http).UploadAsync(new Uri("https://saturn.test/webdav/sync/Fixture/"), "fixture", "config.txt", file, TestContext.Current.CancellationToken)); }
        finally { File.Delete(file); }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CancelsDurableAttemptAfterRestartWhenSourceChangesOrDisappears(bool changed)
    {
        var directory=Path.Combine(Path.GetTempPath(),"neptune-checkpoint-"+Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
        var file=Path.Combine(directory,"data.bin"); var database=Path.Combine(directory,"state.db");
        await File.WriteAllBytesAsync(file,new byte[1024*1024+13],TestContext.Current.CancellationToken);
        var store=new NeptuneStateStore(database); await store.InitializeAsync(TestContext.Current.CancellationToken);
        string? oldKey=null;var cancels=0;var creates=0;var failCancel=true;
        using var http=new HttpClient(new Handler(async request=>
        {
            if(request.Method==HttpMethod.Head) return new(HttpStatusCode.NotFound);
            if(request.RequestUri!.AbsolutePath.EndsWith("/cancel")) {
                using var json=JsonDocument.Parse(await request.Content!.ReadAsStringAsync());
                Assert.Equal(oldKey,json.RootElement.GetProperty("idempotencyKey").GetString());cancels++;
                if(failCancel) throw new HttpRequestException("cancellation response lost");
                return new(HttpStatusCode.OK){Content=new StringContent("{\"status\":\"abandoned\"}",Encoding.UTF8,"application/json")};
            }
            if(request.Method==HttpMethod.Post) {
                creates++;using var json=JsonDocument.Parse(await request.Content!.ReadAsStringAsync());oldKey=json.RootElement.GetProperty("idempotencyKey").GetString();
                throw new HttpRequestException("create acknowledgement lost");
            }
            if(request.Method==HttpMethod.Put) return new(HttpStatusCode.Created){Headers={ETag=new System.Net.Http.Headers.EntityTagHeaderValue("\"sha256-"+Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes("changed")))+"\"")}};
            throw new InvalidOperationException("Unexpected request");
        }));
        try {
            var root=new Uri("https://saturn.test/dav/sync/Fixture/");var token="fixture";
            await Assert.ThrowsAsync<HttpRequestException>(()=>new WebDavSyncClient(http,store).UploadAsync(root,token,"data.bin",file,TestContext.Current.CancellationToken));
            var restarted=new NeptuneStateStore(database); await restarted.InitializeAsync(TestContext.Current.CancellationToken);
            var client=new WebDavSyncClient(http,restarted);
            if(changed) await File.WriteAllTextAsync(file,"changed",TestContext.Current.CancellationToken);else File.Delete(file);
            async Task Act() { if(changed) await client.UploadAsync(root,token,"data.bin",file,TestContext.Current.CancellationToken);else await client.CancelMissingUploadsAsync(root,token,new HashSet<string>(),TestContext.Current.CancellationToken); }
            await Assert.ThrowsAsync<HttpRequestException>(Act);
            Assert.Equal(1,creates);Assert.Equal(1,cancels);
            var scope=Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes("https://saturn.test\n"+token)));
            Assert.Single(await restarted.ListSyncUploadCheckpointsAsync(scope,TestContext.Current.CancellationToken));
            failCancel=false;await Act();Assert.Equal(2,cancels);Assert.Empty(await restarted.ListSyncUploadCheckpointsAsync(scope,TestContext.Current.CancellationToken));
        } finally { SqliteConnection.ClearAllPools(); Directory.Delete(directory,true); }
    }

    [Fact]
    public async Task EmptyLocalSnapshotCannotDeleteEvenOneRemoteFile()
    {
        var deletes = 0;
        using var http = new HttpClient(new Handler(request =>
        {
            if (request.Method == HttpMethod.Delete) deletes++;
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.MultiStatus) { Content = new StringContent("<d:multistatus xmlns:d='DAV:'><d:response><d:href>/webdav/sync/Fixture/config.txt</d:href><d:propstat><d:prop><d:displayname>config.txt</d:displayname><d:resourcetype/></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>", Encoding.UTF8, "application/xml") });
        }));
        await Assert.ThrowsAsync<InvalidOperationException>(() => new WebDavSyncClient(http).MirrorAsync(new Uri("https://saturn.test/webdav/sync/Fixture/"), "fixture", new HashSet<string>(), new HashSet<string>(), cancellationToken: TestContext.Current.CancellationToken));
        Assert.Equal(0, deletes);
    }
}
