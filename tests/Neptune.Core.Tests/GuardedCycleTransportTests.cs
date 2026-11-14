using System.Net;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class GuardedCycleTransportTests
{
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    private static SaturnGuardedCycleTransport Make(HttpClient http,WindowsCycleConfiguration configuration)=>(SaturnGuardedCycleTransport)typeof(SaturnGuardedCycleTransport).GetConstructor(BindingFlags.Instance|BindingFlags.NonPublic,null,[typeof(HttpClient),typeof(WindowsCycleConfiguration),typeof(Func<CancellationToken,Task<string>>)],null)!.Invoke([http,configuration,(Func<CancellationToken,Task<string>>)(_=>Task.FromResult("Bearer test-only-token"))]);
    private static CycleUploadInput Input(WindowsFreshCycleTests.FakeRemote remote)=>new(remote.Binding.ServerEpoch,remote.Binding.BindingVersion,"local-test",new("saturn.upload-intent.v2","windows_sync",remote.Device,remote.Binding.RootId,"/café😀","neptune-cycle-"+Guid.NewGuid().ToString("D"),remote.Binding.RootId,"café😀",null,"3",new string('a',64),null,"*",true,null,new("absent"),[new(remote.Binding.RootId,null,"root")]));
    private static string Hash(CycleUploadInput i)=>(string)typeof(NeptuneStateStore).Assembly.GetType("Neptune.Core.LocalCycleWire")!.GetMethod("IntentHash",BindingFlags.Static|BindingFlags.NonPublic)!.Invoke(null,[i])!;
    private static WindowsCycleConfiguration Config(WindowsFreshCycleTests.FakeRemote remote)=>new("mapping","client","audit","rules-v1",new Uri("http://localhost/"),remote.Device,"test-only-credential","local-test",remote.Binding);
    private static object Body(CycleUploadStatus status)
    {
        var session=new{uploadId=status.UploadId,operationId=status.OperationId,rootId=status.RootId,intentHash=status.IntentHash,expectedSizeBytes=status.ExpectedBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),receivedSizeBytes=status.ReceivedBytes.ToString(System.Globalization.CultureInfo.InvariantCulture),phase=status.Phase,transferPending=status.TransferPending};
        return status.ResultJson is null?new{session,replayed=status.Replayed}:(object)new{session,result=JsonSerializer.Deserialize<JsonElement>(status.ResultJson),replayed=status.Replayed};
    }
    [Fact]
    public async Task AppendUsesExactBigEndianFramingAndOriginalBindingWithoutBase64Copy()
    {
        var remote=new WindowsFreshCycleTests.FakeRemote();var i=Input(remote);var hash=Hash(i);var status=await remote.UploadAsync("prepare",i,null,hash,Ct);var handler=new Handler(async request=>
        {
            Assert.Equal("application/vnd.saturn.sync-upload.v1",request.Content!.Headers.ContentType!.MediaType);var raw=await request.Content.ReadAsByteArrayAsync(Ct);var length=System.Buffers.Binary.BinaryPrimitives.ReadUInt32BigEndian(raw);Assert.InRange(length,1u,40u*1024);using var frame=JsonDocument.Parse(raw.AsMemory(4,(int)length));Assert.Equal(status.UploadId,frame.RootElement.GetProperty("upload_id").GetString());Assert.Equal("0",frame.RootElement.GetProperty("offset").GetString());Assert.Equal("café😀",frame.RootElement.GetProperty("input").GetProperty("intent").GetProperty("name").GetString());Assert.Equal(new byte[]{4,5,6},raw.AsSpan(4+(int)length).ToArray());return JsonSerializer.SerializeToUtf8Bytes(Body(status with{ReceivedBytes=3}));
        });using var http=new HttpClient(handler);var transport=Make(http,Config(remote));var ack=await transport.AppendAsync(i,status.UploadId,hash,0,new byte[]{4,5,6},Ct);Assert.Equal(3,ack.ReceivedBytes);
    }
    [Theory]
    [InlineData("root")][InlineData("hash")][InlineData("upload")][InlineData("receipt-op")][InlineData("size")][InlineData("version")][InlineData("missing")][InlineData("extra")][InlineData("descriptor-extra")][InlineData("schema")][InlineData("receipt-time")]
    public async Task RecognizedHistoricalReceiptWithWrongBindingOrFieldsFailsClosed(string mutation)
    {
        var remote=new WindowsFreshCycleTests.FakeRemote();var i=Input(remote);var hash=Hash(i);var create=await remote.UploadAsync("prepare",i,null,hash,Ct);await remote.AppendAsync(i,create.UploadId,hash,0,new byte[]{1,2,3},Ct);var published=await remote.UploadAsync("complete",i,create.UploadId,hash,Ct);var body=JsonNode.Parse(JsonSerializer.Serialize(Body(published)))!;var receipt=body["result"]!["publicationReceipt"]!;
        if(mutation=="root")receipt["rootId"]=Guid.NewGuid().ToString("D");else if(mutation=="hash")receipt["intentHash"]=new string('b',64);else if(mutation=="upload")body["session"]!["uploadId"]=Guid.NewGuid().ToString("D");else if(mutation=="receipt-op")receipt["operationId"]=i.Intent.IdempotencyKey;else if(mutation=="size")receipt["sizeBytes"]="03";else if(mutation=="version")receipt["versionId"]=Guid.NewGuid().ToString("D");else if(mutation=="missing")body["result"]!.AsObject().Remove("publicationReceipt");else if(mutation=="extra")receipt["unrecognized"]=1;else if(mutation=="descriptor-extra")body["result"]!["publishedDescription"]!["unexpected"]=1;else if(mutation=="schema")receipt["schema"]="future.v100";else receipt["publishedAt"]="2026-10-13T00:00:00Z";
        using var http=new HttpClient(new Handler(_=>Task.FromResult(Encoding.UTF8.GetBytes(body.ToJsonString()))));var transport=Make(http,Config(remote));await Assert.ThrowsAnyAsync<Exception>(()=>transport.UploadAsync("inspect",i,create.UploadId,hash,Ct));
    }
    [Theory]
    [InlineData("duplicate")][InlineData("overflow")][InlineData("bad-utf8")][InlineData("depth")]
    public async Task RawStatusBudgetAndJsonFramingNeverBecomeSyntheticSuccess(string input)
    {
        var remote=new WindowsFreshCycleTests.FakeRemote();var i=Input(remote);var raw=input=="duplicate"?Encoding.UTF8.GetBytes("{\"session\":{},\"session\":{},\"replayed\":false}"):input=="overflow"?new byte[40*1024+1]:input=="bad-utf8"?new byte[]{123,34,255,34,58,49,125}:Encoding.UTF8.GetBytes(new string('[',64)+new string(']',64));using var http=new HttpClient(new Handler(_=>Task.FromResult(raw)));var transport=Make(http,Config(remote));await Assert.ThrowsAnyAsync<Exception>(()=>transport.UploadAsync("prepare",i,null,Hash(i),Ct));
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GuardedFolderV2SendsOriginalNineFieldsAndRejectsRawAliasRebinding(bool retainedMappingAnchor)
    {
        var remote=new WindowsFreshCycleTests.FakeRemote();var input=new CycleFolderInput(Guid.NewGuid().ToString("D"),"",remote.Binding.RootId,remote.Binding.ServerEpoch,remote.Binding.BindingVersion,remote.Binding.RootId,"café","ensure",[new(remote.Binding.RootId,null,"root")]);input=input with{IdempotencyKey=retainedMappingAnchor?input.OperationId:"neptune-cycle-"+input.OperationId};var outcome=await remote.EnsureDirectoryAsync(input,Ct);
        using var http=new HttpClient(new Handler(async request=>{Assert.EndsWith("/directories/ensure-v2",request.RequestUri!.AbsolutePath);using var body=JsonDocument.Parse(await request.Content!.ReadAsByteArrayAsync(Ct));var original=body.RootElement.GetProperty("input");Assert.Equal(9,original.EnumerateObject().Count());Assert.Equal(input.OperationId,original.GetProperty("operationId").GetString());Assert.Equal(input.IdempotencyKey,original.GetProperty("idempotencyKey").GetString());return Encoding.UTF8.GetBytes(outcome.RawJson);}));var t=Make(http,Config(remote));Assert.Equal(outcome.Description,(await t.EnsureDirectoryAsync(input,Ct)).Description);
        var wrong=JsonNode.Parse(outcome.RawJson)!;wrong["result"]!["description"]!["name"]="cafe\u0301";using var wrongHttp=new HttpClient(new Handler(_=>Task.FromResult(Encoding.UTF8.GetBytes(wrong.ToJsonString()))));await Assert.ThrowsAsync<InvalidDataException>(()=>Make(wrongHttp,Config(remote)).EnsureDirectoryAsync(input,Ct));
    }
    [Theory]
    [InlineData("unrelated-key")]
    [InlineData("create")]
    [InlineData("nested")]
    public async Task MappingAnchorCompatibilityCannotAdmitOtherBareKeyIntents(string change)
    {
        var remote = new WindowsFreshCycleTests.FakeRemote();var id = Guid.NewGuid().ToString("D");
        var input = new CycleFolderInput(id, id, remote.Binding.RootId, remote.Binding.ServerEpoch,
            remote.Binding.BindingVersion, remote.Binding.RootId, "mapping", "ensure", [new(remote.Binding.RootId, null, "root")]);
        if(change=="unrelated-key")input=input with{IdempotencyKey=Guid.NewGuid().ToString("D")};
        if(change=="create")input=input with{Mode="create"};
        if(change=="nested") {var parent=Guid.NewGuid().ToString("D");input=input with{ParentId=parent,ParentTopology=[new(parent,input.RootId,"child"),new(input.RootId,null,"root")]};}
        var requests=0;using var http=new HttpClient(new Handler(_=>{requests++;throw new InvalidOperationException("Must refuse before HTTP");}));
        await Assert.ThrowsAsync<InvalidDataException>(()=>Make(http,Config(remote)).EnsureDirectoryAsync(input,Ct));Assert.Equal(0,requests);
    }
    private sealed class Handler(Func<HttpRequestMessage,Task<byte[]>> response):HttpMessageHandler
    {protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken cancellationToken)=>new(HttpStatusCode.OK){Content=new ByteArrayContent(await response(request)),RequestMessage=request};}
}
