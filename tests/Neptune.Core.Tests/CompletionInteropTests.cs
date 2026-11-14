using System.Diagnostics;
using System.Net;
using System.Net.Http.Json;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

[Trait("Lane", "Live")]
public sealed class CompletionInteropTests
{
    [Fact]
    public async Task ActualExtendedCapabilityAcceptedWithoutWeakeningValidation()
    {
        var ct=TestContext.Current.CancellationToken;
        var directory=Environment.GetEnvironmentVariable("COMPLETION_EVIDENCE")??throw new InvalidOperationException("Own API required");
        using var config=JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(directory,"clients.private.json"),ct));var c=config.RootElement[0];
        using var http=SyncHttpPolicy.CreateClient();var root=c.GetProperty("binding").GetProperty("root_id").GetString()!;var device=c.GetProperty("deviceId").GetString()!;
        using var request=new HttpRequestMessage(HttpMethod.Get,new Uri(new Uri(c.GetProperty("origin").GetString()!),"sync/v1/roots/"+root+"/capabilities"));request.Headers.TryAddWithoutValidation("Authorization",c.GetProperty("authorization").GetString());
        using var response=await http.SendAsync(request,ct);response.EnsureSuccessStatusCode();var raw=await response.Content.ReadAsByteArrayAsync(ct);
        var parsed=WindowsGuardedCapabilities.Parse(raw,device,root);Assert.Equal(2,parsed.NamespaceRevision);Assert.Contains("namespace-mutations-v1",parsed.ExtensionFeatures);
        var node=System.Text.Json.Nodes.JsonNode.Parse(raw)!.AsObject();node["coverage"]="anything";Assert.Throws<InvalidDataException>(()=>WindowsGuardedCapabilities.Parse(System.Text.Encoding.UTF8.GetBytes(node.ToJsonString()),device,root));
        node["coverage"]="namespace-mutations-v1";node.Remove("extensions");Assert.Throws<InvalidDataException>(()=>WindowsGuardedCapabilities.Parse(System.Text.Encoding.UTF8.GetBytes(node.ToJsonString()),device,root));
    }
    [Fact]
    public async Task ActualSaturnMetadataStreamAndFreshFeedAcrossTwoHops()
    {
        var ct=TestContext.Current.CancellationToken;
        var directory=Environment.GetEnvironmentVariable("COMPLETION_EVIDENCE")??throw new InvalidOperationException("Own API fixture required");
        using var config=JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(directory,"clients.private.json"),ct));
        var c=config.RootElement[0]; var b=c.GetProperty("binding");
        string B(string key)=>b.GetProperty(key).GetString()!;
        var binding=new SyncRootBinding(B("protocol"),B("descriptor_schema"),B("server_identity"),B("root_id"),B("server_epoch"),B("policy_version"),B("binding_version"));
        var policy=new SyncNamespacePolicy(c.GetProperty("namespace").GetProperty("codec").GetString()!,c.GetProperty("namespace").GetProperty("policyHash").GetString()!);
        var authorization=c.GetProperty("authorization").GetString()!;
        using var http=SyncHttpPolicy.CreateClient();
        var cfg=new WindowsCycleConfiguration("completion","completion","completion","rules-v1",new(c.GetProperty("origin").GetString()!),c.GetProperty("deviceId").GetString()!,"fixture",c.GetProperty("backendIdentity").GetString()!,binding);
        using var remote=(SaturnGuardedCycleTransport)typeof(SaturnGuardedCycleTransport).GetConstructor(BindingFlags.Instance|BindingFlags.NonPublic,null,[typeof(HttpClient),typeof(WindowsCycleConfiguration),typeof(Func<CancellationToken,Task<string>>)],null)!.Invoke([http,cfg,(Func<CancellationToken,Task<string>>)(_=>Task.FromResult(authorization))]);
        remote.ConfigureExtensions(new HashSet<string>{"namespace-mutations-v1","metadata-gzip-v1","stream-upload-v1","changes-wait-v1","folder-batch-v1"},policy,2);
        var prefix="neptune-"+Guid.NewGuid().ToString("N");
        async Task Fault(string tail,string kind,int milliseconds=0)
        {using var response=await http.PostAsJsonAsync(new Uri(new Uri(c.GetProperty("faultOrigin").GetString()!),"__completion/fault"),new{tail,kind,milliseconds},ct);response.EnsureSuccessStatusCode();}
        var mutations=Enumerable.Range(0,40).Select(i=>{
            var id=Guid.NewGuid().ToString("D");return new SyncMutationRequest(id,"create-empty",JsonSerializer.SerializeToElement(new{schema="saturn.namespace-mutation.v1",operationId=id,idempotencyKey=id,rootId=binding.RootId,epoch=binding.ServerEpoch,bindingVersion=binding.BindingVersion,kind="create-empty",expected=new{kind="absent"},parentId=binding.RootId,name=prefix+"-"+i+".md"}),policy);
        }).ToArray();
        // Persist originals before I/O so an interrupted run can verify these
        // exact operations instead of silently generating replacement IDs.
        await File.WriteAllTextAsync(Path.Combine(directory,"neptune-interop-originals.json"),JsonSerializer.Serialize(mutations.Select(m=>m.Input)),ct);
        await Fault("mutations/batch","lost-ack");
        await Assert.ThrowsAnyAsync<Exception>(()=>Task.WhenAll(mutations.Select(m=>remote.MutateAsync(m,false,ct))));
        // Losing the HTTP acknowledgement does not imply that the whole batch
        // has finished. The shared-backend outage may outlive the HTTP timeout.
        // Observe the same IDs; never reserve a substitute operation here.
        var publicationWait=Stopwatch.StartNew();
        async Task<SyncMutationReceipt> AwaitPublication(SyncMutationRequest original)
        {
            string last="not_observed";
            while(publicationWait.Elapsed<TimeSpan.FromMinutes(3))
            {
                try
                {
                    var receipt=await remote.MutateAsync(original,true,ct);
                    if(receipt.State=="PUBLISHED")return receipt;
                    last=receipt.State;
                }
                catch(GuardedCycleRemoteException e)when(e.StructuredWireCode&&e.Status==HttpStatusCode.Conflict&&e.Code=="operation_reused")
                {last="not_yet_admitted_or_original_mismatch";}
                await Task.Delay(250,ct);
            }
            throw new TimeoutException($"Original metadata operation {original.OperationId} did not publish: {last}");
        }
        var receipts=await Task.WhenAll(mutations.Select(AwaitPublication));
        publicationWait.Stop();
        Assert.All(receipts,r=>Assert.Equal("PUBLISHED",r.State));
        Assert.Equal(40,receipts.Select(r=>r.Description!.NodeId).Distinct().Count());
        var replay=await Task.WhenAll(mutations.Select(m=>remote.MutateAsync(m,false,ct)));
        Assert.Equal(receipts.Select(r=>r.RawJson),replay.Select(r=>r.RawJson));
        foreach(var kind in new[]{"407","429","close"})
        {
            await Fault("mutations/status",kind);
            var error=await Assert.ThrowsAnyAsync<Exception>(()=>remote.MutateAsync(mutations[0],true,ct));
            if(kind!="close")Assert.Equal((HttpStatusCode)int.Parse(kind),Assert.IsType<GuardedCycleRemoteException>(error).Status);
            Assert.Equal(receipts[0].RawJson,(await remote.MutateAsync(mutations[0],true,ct)).RawJson);
        }
        var data=new byte[4194304+17];new Random(314159).NextBytes(data);
        var streamId=Guid.NewGuid().ToString("D");var witness=new string('a',64);var generation="1:1";
        var input=JsonSerializer.SerializeToElement(new{schema="saturn.stream-upload.v1",operationId=streamId,source=new{generation,witnessSha256=witness},chunkBytes=4194304,
            prepare=new{epoch=binding.ServerEpoch,bindingVersion=binding.BindingVersion,backendIdentity=cfg.BackendIdentity,intent=new{schema="saturn.upload-intent.v2",deviceKind="windows_sync",deviceId=cfg.DeviceId,rootId=binding.RootId,requestPath="/completion/"+prefix,idempotencyKey=streamId,parentId=binding.RootId,name=prefix+"-stream.bin",targetId=(string?)null,sizeBytes=data.Length.ToString(),sha256=(string?)null,ifMatch=(string?)null,ifNoneMatch="*",requireAbsent=true,expectedVersionId=(string?)null,condition=new{kind="absent"},parentTopology=new[]{new{nodeId=binding.RootId,parentId=(string?)null,name=c.GetProperty("rootName").GetString()!}}}}});
        var stream=new SyncStreamInput(streamId,generation,witness,input);
        await Fault("stream-uploads/prepare","lost-ack");
        await Assert.ThrowsAnyAsync<Exception>(()=>remote.StreamCommandAsync("prepare",stream,null,ct));
        Assert.Equal(0,(await remote.StreamCommandAsync("status",stream,null,ct)).Received);
        for(var index=0;index<2;index++)
        {
            var chunk=data.AsMemory(index*4194304,Math.Min(4194304,data.Length-index*4194304));
            var meta=new SyncStreamChunk(index,(index*4194304L).ToString(),chunk.Length.ToString(),Convert.ToHexStringLower(SHA256.HashData(chunk.Span)));
            await Fault("stream-uploads/chunk","lost-ack");
            await Assert.ThrowsAnyAsync<Exception>(()=>remote.StreamChunkAsync(stream,meta,chunk,ct));
            var status=await remote.StreamCommandAsync("status",stream,null,ct);
            Assert.Equal(index+1,status.Chunks.Count);
            Assert.Equal(meta,status.Chunks[index]);
        }
        var hash=Convert.ToHexStringLower(SHA256.HashData(data));
        await remote.StreamCommandAsync("seal",stream,hash,ct);
        await Fault("stream-uploads/complete","lost-ack");
        await Assert.ThrowsAnyAsync<Exception>(()=>remote.StreamCommandAsync("complete",stream,null,ct));
        var done=await remote.StreamCommandAsync("status",stream,null,ct);
        Assert.Equal("PUBLISHED",done.State);Assert.Equal(hash,done.FinalSha256);
        var snapshot=Guid.NewGuid().ToString("D");await remote.StartSnapshotAsync(snapshot,ct);
        for(var n=0;n<100;n++){using var step=JsonDocument.Parse(await remote.StepSnapshotAsync(snapshot,ct));if(step.RootElement.GetProperty("state").GetString()=="READY")break;Assert.True(n<99);}
        var ready=await remote.ReadySnapshotAsync(snapshot,ct);
        var page=await remote.ReadEntriesAsync(ready,ready.EntriesCursor,null,ct);
        Assert.True(page.Items.Count>=41);
        var sw=Stopwatch.StartNew();var feed=await remote.WaitForChangesAsync(ready.ChangesCursor,long.Parse(ready.CutSeq),ct);
        Assert.InRange(sw.Elapsed.TotalSeconds,24,44);Assert.Empty(feed.Items);
        await File.WriteAllTextAsync(Path.Combine(directory,"neptune-interop.json"),JsonSerializer.Serialize(new{prefix,metadata=receipts.Length,operationIds=mutations.Select(m=>m.OperationId),nodeIds=receipts.Select(r=>r.Description!.NodeId),publicationWaitSeconds=publicationWait.Elapsed.TotalSeconds,stream=done.State,hash,requests=remote.RequestCounts,longPollSeconds=sw.Elapsed.TotalSeconds}),ct);
    }
}
