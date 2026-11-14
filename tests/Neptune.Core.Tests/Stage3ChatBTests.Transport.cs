using System.Text;
using System.Text.Json;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class Stage3ChatBTests
{
    private static CycleFolderInput Folder(WindowsFreshCycleTests.FakeRemote remote,string name,string? parent=null,IReadOnlyList<CatalogParentTopology>? topology=null)
    {var id=Guid.NewGuid().ToString("D");return new(id,"neptune-cycle-"+id,remote.Binding.RootId,remote.Binding.ServerEpoch,remote.Binding.BindingVersion,parent??remote.Binding.RootId,name,"ensure",topology??[new(remote.Binding.RootId,null,"root")]);}
    [Fact]
    public async Task B07_FiveHundredOneDirectoriesThreeLevelsSingleflightAndImmutableCache()
    {
        var remote=new WindowsFreshCycleTests.FakeRemote();var sizes=new List<int>();var requests=0;var examples=new List<object>();
        using var http=new HttpClient(new WireHandler(async(request,ct)=>
        {
            Interlocked.Increment(ref requests);var bytes=await request.Content!.ReadAsByteArrayAsync(ct);Assert.InRange(bytes.Length,1,4*1024*1024);
            using var document=JsonDocument.Parse(bytes);var rows=document.RootElement.GetProperty("items");lock(sizes)sizes.Add(rows.GetArrayLength());Assert.InRange(rows.GetArrayLength(),1,500);
            var items=new List<object>();foreach(var row in rows.EnumerateArray())
            {var original=JsonSerializer.Deserialize<CycleFolderInput>(row.GetProperty("input"),LocalCycleWire.Json)!;var result=await remote.EnsureDirectoryAsync(original,ct);items.Add(new{@ref=row.GetProperty("ref").GetString(),outcome=JsonSerializer.Deserialize<JsonElement>(result.RawJson)});}
            var response=JsonSerializer.SerializeToUtf8Bytes(new{schema="saturn.folder-batch-result.v1",items});lock(examples)if(examples.Count==0)examples.Add(new{route=request.RequestUri!.AbsolutePath,request=JsonSerializer.Deserialize<JsonElement>(bytes),response=JsonSerializer.Deserialize<JsonElement>(response)});return response;
        }));using var transport=Transport(http,remote);transport.ConfigureExtensions(new HashSet<string>{"folder-batch-v1"},null);
        var roots=Enumerable.Range(0,167).Select(i=>Folder(remote,"parent-"+i)).ToArray();var first=await Task.WhenAll(roots.Select(i=>transport.EnsureDirectoryAsync(i,Ct)));
        var secondInput=first.Select((p,i)=>Folder(remote,"child-"+i,p.Description.NodeId,[new(p.Description.NodeId,remote.Binding.RootId,p.Description.Name),new(remote.Binding.RootId,null,"root")])).ToArray();
        var second=await Task.WhenAll(secondInput.Select(i=>transport.EnsureDirectoryAsync(i,Ct)));
        var thirdInput=second.Select((p,i)=>Folder(remote,"leaf-"+i,p.Description.NodeId,[new(p.Description.NodeId,first[i].Description.NodeId,p.Description.Name),new(first[i].Description.NodeId,remote.Binding.RootId,first[i].Description.Name),new(remote.Binding.RootId,null,"root")])).ToArray();
        await Task.WhenAll(thirdInput.Select(i=>transport.EnsureDirectoryAsync(i,Ct)));Assert.Equal(502,remote.Count);Assert.Equal(501,sizes.Sum());
        var repeated=Folder(remote,"singleflight");var before=requests;var all=await Task.WhenAll(Enumerable.Range(0,20).Select(_=>transport.EnsureDirectoryAsync(repeated,Ct)));
        Assert.Equal(before+1,requests);Assert.Single(all.Select(x=>x.Description.NodeId).Distinct());
        await Assert.ThrowsAsync<InvalidDataException>(()=>transport.EnsureDirectoryAsync(repeated with{Name="changed-original"},Ct));Assert.Equal(before+1,requests);
        await WireEvidence("B07-parent-batches",new{sizes,requests,nodes=remote.Count,original=repeated});
        await WireEvidence("B07-wire-examples",new{fixture=true,credentialsOmitted=true,examples});
    }
    [Fact]
    public async Task B07_CapabilityOffUsesIndividualOriginalAndWaitCancellationIsJoined()
    {
        var remote=new WindowsFreshCycleTests.FakeRemote();var input=Folder(remote,"fallback");var started=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);var calls=0;
        using var http=new HttpClient(new WireHandler(async(request,ct)=>
        {
            calls++;if(request.RequestUri!.AbsolutePath.EndsWith("/changes")){started.TrySetResult();await Task.Delay(Timeout.Infinite,ct);return [];}
            Assert.EndsWith("/directories/ensure-v2",request.RequestUri.AbsolutePath);return Encoding.UTF8.GetBytes((await remote.EnsureDirectoryAsync(input,ct)).RawJson);
        }));using var transport=Transport(http,remote);Assert.Equal("fallback",(await transport.EnsureDirectoryAsync(input,Ct)).Description.Name);
        using var cancel=new CancellationTokenSource();var waiting=transport.WaitForChangesAsync("cursor",0,cancel.Token);await started.Task;cancel.Cancel();await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>waiting);Assert.Equal(2,calls);
    }
    [Fact] public async Task B08_LostPrepareRecovery(){RequireWindows();await new JournalGapReplacementTests().SmallBatchNativeEngineRecoversSameOriginalsAfterResponseLossAndEngineReopen("lost-prepare");}
    [Fact] public async Task B08_LostDataRecovery(){RequireWindows();await new JournalGapReplacementTests().SmallBatchNativeEngineRecoversSameOriginalsAfterResponseLossAndEngineReopen("lost-data");}
    [Fact] public async Task B08_PartialRecovery(){RequireWindows();await new JournalGapReplacementTests().SmallBatchNativeEngineRecoversSameOriginalsAfterResponseLossAndEngineReopen("partial");}
    [Fact]
    public async Task B07_RemoteFeedInvalidatesNegativeDirectoryCache()
    {
        var remote=new WindowsFreshCycleTests.FakeRemote();var input=Folder(remote,"became-available");var attempts=0;var changed=false;
        using var http=new HttpClient(new WireHandler(async(request,ct)=>
        {
            if(request.RequestUri!.AbsolutePath.EndsWith("/changes"))
            {
                changed=true;var outcome=await remote.EnsureDirectoryAsync(input,ct);var body=System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(LocalCycleWire.BindingObject(remote.Binding)))!.AsObject();body["from_exclusive"]="0";body["to_inclusive"]="1";body["next_cursor"]="next";body["has_more"]=false;
                var item=System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(WindowsFreshCycleTests.FakeRemote.Wire(outcome.Description)))!.AsObject();item["seq"]="1";item["kind"]="upsert";item["operation_id"]=input.OperationId;body["items"]=new System.Text.Json.Nodes.JsonArray(item);return JsonSerializer.SerializeToUtf8Bytes(body);
            }
            attempts++;if(!changed)throw new GuardedCycleRemoteException(System.Net.HttpStatusCode.PreconditionFailed,"precondition_failed",true);return Encoding.UTF8.GetBytes((await remote.EnsureDirectoryAsync(input,ct)).RawJson);
        }));using var transport=Transport(http,remote);
        await Assert.ThrowsAsync<GuardedCycleRemoteException>(()=>transport.EnsureDirectoryAsync(input,Ct));await Assert.ThrowsAsync<GuardedCycleRemoteException>(()=>transport.EnsureDirectoryAsync(input,Ct));Assert.Equal(1,attempts);
        var feed=await transport.ReadChangesAsync("before",0,Ct);Assert.Single(feed.Items);var ready=await transport.EnsureDirectoryAsync(input,Ct);Assert.Equal(input.Name,ready.Description.Name);Assert.Equal(2,attempts);
    }
    [Fact]
    public async Task B07_NamespacePolicyInvalidatesFailureButPreservesOriginalReceipt()
    {
        var remote=new WindowsFreshCycleTests.FakeRemote();var accepted=Folder(remote,"accepted");var missing=Folder(remote,"missing");var changed=false;var calls=0;
        using var http=new HttpClient(new WireHandler(async(request,ct)=>
        {
            calls++;using var body=JsonDocument.Parse(await request.Content!.ReadAsByteArrayAsync(ct));var input=JsonSerializer.Deserialize<CycleFolderInput>(body.RootElement.GetProperty("input"),LocalCycleWire.Json)!;
            if(input.OperationId==missing.OperationId&&!changed)throw new GuardedCycleRemoteException(System.Net.HttpStatusCode.PreconditionFailed,"precondition_failed",true);
            return Encoding.UTF8.GetBytes((await remote.EnsureDirectoryAsync(input,ct)).RawJson);
        }));using var transport=Transport(http,remote);transport.ConfigureExtensions(new HashSet<string>(),new("legacy",new string('a',64)));
        var original=await transport.EnsureDirectoryAsync(accepted,Ct);await Assert.ThrowsAsync<GuardedCycleRemoteException>(()=>transport.EnsureDirectoryAsync(missing,Ct));Assert.Equal(2,calls);
        changed=true;transport.ConfigureExtensions(new HashSet<string>(),new("legacy",new string('b',64)));Assert.Equal(original,await transport.EnsureDirectoryAsync(accepted,Ct));Assert.Equal(2,calls);
        Assert.Equal("missing",(await transport.EnsureDirectoryAsync(missing,Ct)).Description.Name);Assert.Equal(3,calls);
    }
}
