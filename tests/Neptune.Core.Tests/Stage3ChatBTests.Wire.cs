using System.IO.Compression;
using System.Net;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class Stage3ChatBTests
{
    private sealed class WireHandler(Func<HttpRequestMessage,CancellationToken,Task<byte[]>> action):HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken ct) =>
            new(HttpStatusCode.OK){RequestMessage=request,Content=new ByteArrayContent(await action(request,ct))};
    }
    private static SaturnGuardedCycleTransport Transport(HttpClient http,WindowsFreshCycleTests.FakeRemote remote) =>
        (SaturnGuardedCycleTransport)typeof(SaturnGuardedCycleTransport).GetConstructor(BindingFlags.NonPublic|BindingFlags.Instance,null,
            [typeof(HttpClient),typeof(WindowsCycleConfiguration),typeof(Func<CancellationToken,Task<string>>)],null)!.Invoke(
            [http,new WindowsCycleConfiguration("mapping","client","audit","rules-v1",new Uri("https://fixture.invalid/"),remote.Device,"canary-credential-DO-NOT-EXPORT","local-test",remote.Binding),(Func<CancellationToken,Task<string>>)(_=>Task.FromResult("Bearer canary-credential-DO-NOT-EXPORT"))]);
    private static Task<byte[]> SendMetadata(SaturnGuardedCycleTransport remote,HttpContent content,int maximum=4*1024*1024) =>
        (Task<byte[]>)typeof(SaturnGuardedCycleTransport).GetMethod("Send",BindingFlags.NonPublic|BindingFlags.Instance)!.Invoke(remote,["fixture",HttpMethod.Post,content,maximum,Ct,false])!;
    private static string RepositoryFile(string path)
    {
        for(var at=new DirectoryInfo(AppContext.BaseDirectory);at is not null;at=at.Parent)
        {var candidate=Path.Combine(at.FullName,path);if(File.Exists(candidate))return candidate;}
        var shipped=Path.Combine(AppContext.BaseDirectory,"TestData",Path.GetFileName(path));
        if(File.Exists(shipped))return shipped;
        throw new FileNotFoundException("Shared contract fixture missing",path);
    }
    private static Task WireEvidence(string name,object value)
    {
        var root=Environment.GetEnvironmentVariable("NEPTUNE_FRESH_CYCLE_EVIDENCE_ROOT")??Path.Combine(AppContext.BaseDirectory,"TestResults","stage3-wire");
        Directory.CreateDirectory(root);return File.WriteAllTextAsync(Path.Combine(root,name+".json"),JsonSerializer.Serialize(value,new JsonSerializerOptions{WriteIndented=true}),Ct);
    }

    [Fact]
    public async Task B15_AllSharedCodecVectorsAndUnicodeBoundaries()
    {
        var raw=await File.ReadAllBytesAsync(RepositoryFile("saturn/packages/contracts/fixtures/name-codec-v1.json"),Ct);
        using var fixture=JsonDocument.Parse(raw);var count=0;
        foreach(var vector in fixture.RootElement.GetProperty("vectors").EnumerateArray())
        {
            var accepted=vector.GetProperty("accepted").GetBoolean();var codec=vector.GetProperty("codec").GetString()!;
            if(accepted)Assert.Equal(vector.GetProperty("key").GetString(),SyncNameCodec.Key(vector.GetProperty("name").GetString()!,codec));
            else Assert.NotNull(Record.Exception(()=>SyncNameCodec.Key(vector.GetProperty("name").GetString()!,codec)));
            count++;
        }
        Assert.Equal(28,count);Assert.Equal(new string('x',255),SyncNameCodec.Key(new string('x',255),"portable_v1"));
        Assert.Throws<InvalidDataException>(()=>SyncNameCodec.Key(new string('x',256),"portable_v1"));
        Assert.Throws<InvalidDataException>(()=>SyncNameCodec.Key("\ud800","legacy"));Assert.Throws<InvalidDataException>(()=>SyncNameCodec.Key("\udc00","legacy"));
        Assert.NotEqual(SyncNameCodec.Key("İ","portable_v1"),SyncNameCodec.Key("i","portable_v1"));
        await WireEvidence("B15-codec",new{count,fixtureSha256=Convert.ToHexStringLower(SHA256.HashData(raw))});
    }

    [Theory]
    [InlineData(16384,true,false)][InlineData(16385,true,true)][InlineData(16385,false,false)]
    public async Task B15_GzipThresholdAndCanonicalBytes(int size,bool negotiated,bool expectedGzip)
    {
        var original=Encoding.UTF8.GetBytes("\""+new string('x',size-2)+"\"");var remote=new WindowsFreshCycleTests.FakeRemote();byte[]? wire=null;
        using var http=new HttpClient(new WireHandler(async(request,ct)=>
        {
            wire=await request.Content!.ReadAsByteArrayAsync(ct);Assert.Equal(expectedGzip,request.Content.Headers.ContentEncoding.Contains("gzip"));
            if(expectedGzip){using var gzip=new GZipStream(new MemoryStream(wire),CompressionMode.Decompress);using var decoded=new MemoryStream();await gzip.CopyToAsync(decoded,ct);Assert.Equal(original,decoded.ToArray());}
            else Assert.Equal(original,wire);return "{}"u8.ToArray();
        }));using var transport=Transport(http,remote);transport.ConfigureExtensions(negotiated?new HashSet<string>{"metadata-gzip-v1"}:new HashSet<string>(),null);
        using var content=new ByteArrayContent(original);content.Headers.ContentType=new("application/json");Assert.Equal("{}"u8.ToArray(),await SendMetadata(transport,content));
        await WireEvidence($"B15-gzip-{size}-{negotiated}",new{size,negotiated,expectedGzip,originalSha256=Convert.ToHexStringLower(SHA256.HashData(original)),wireBytes=wire!.Length});
    }

    [Fact]
    public async Task B15_MetadataBoundsBinaryAndDiagnosticsRedaction()
    {
        var calls=0;var remote=new WindowsFreshCycleTests.FakeRemote();var binary=new byte[]{0,255,31,139,0,5};
        using var http=new HttpClient(new WireHandler(async(request,ct)=>{calls++;Assert.Empty(request.Content!.Headers.ContentEncoding);Assert.Equal(binary,await request.Content.ReadAsByteArrayAsync(ct));return "{}"u8.ToArray();}));
        using var transport=Transport(http,remote);transport.ConfigureExtensions(new HashSet<string>{"metadata-gzip-v1"},null);
        using(var tooLarge=new ByteArrayContent(new byte[4*1024*1024+1])){tooLarge.Headers.ContentType=new("application/json");await Assert.ThrowsAsync<InvalidDataException>(()=>SendMetadata(transport,tooLarge));}Assert.Equal(0,calls);
        using(var content=new ByteArrayContent(binary)){content.Headers.ContentType=new("application/vnd.saturn.sync-stream.v1");await SendMetadata(transport,content);}Assert.Equal(1,calls);
        var diagnostics=new SyncDiagnostics();using(diagnostics.Measure(SyncPhase.Capture)){diagnostics.Items(SyncPhase.Capture,12);SyncDiagnostics.SqlCommandCreated();}
        diagnostics.StreamRead(12,true);diagnostics.Retry("WAIT_CONNECTIVITY");diagnostics.Backlog(3,10);var snapshot=diagnostics.Snapshot() with{CapturedBytes=12,ReceivedBytes=12};
        Assert.Equal(0,snapshot.PublishedFiles);Assert.Null(snapshot.BackendWrittenBytes);Assert.Null(snapshot.BackendVerifiedBytes);Assert.Equal(12,snapshot.StreamRecoveryReadBytes);
        var exported=JsonSerializer.Serialize(new{snapshot,requests=transport.RequestCounts});Assert.DoesNotContain("canary-credential",exported);Assert.DoesNotContain(Convert.ToBase64String(binary),exported);
        await WireEvidence("B15-diagnostics",new{snapshot,requests=transport.RequestCounts});
    }

    [Theory]
    [InlineData("missing-seal")][InlineData("offset")][InlineData("hash")][InlineData("pending")][InlineData("operation")]
    public void B10_StreamStatusRejectsUnprovenPublicationAndBrokenAckMap(string defect)
    {
        var id=Guid.NewGuid().ToString("D");var input=new SyncStreamInput(id,"1:1",new string('a',64),JsonSerializer.SerializeToElement(new{prepare=new{intent=new{sizeBytes="4194304"}}}));
        var chunk=new SyncStreamChunk(0,defect=="offset"?"1":"0","4194304",defect=="hash"?"bad":new string('b',64));
        var raw=JsonSerializer.SerializeToUtf8Bytes(new{schema="saturn.stream-upload-status.v1",operationId=defect=="operation"?Guid.NewGuid().ToString("D"):id,state=defect=="missing-seal"?"PUBLISHED":"RECEIVING",receivedSizeBytes="4194304",chunks=new[]{chunk},pendingChunk=defect=="pending"?chunk:null,finalSha256=(string?)null,publication=(object?)null},LocalCycleWire.Json);
        Assert.Throws<InvalidDataException>(()=>SaturnGuardedCycleTransport.ReadStream(raw,input));
    }
}
