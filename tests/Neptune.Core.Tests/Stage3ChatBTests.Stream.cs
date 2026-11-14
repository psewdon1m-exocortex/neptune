using System.Security.Cryptography;
using System.Text.Json;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class WindowsFreshCycleTests
{
    internal sealed partial class FakeRemote:IWindowsStreamRemote
    {
        internal bool StreamEnabled,StreamLoseSecondAck,StreamWrongSeal;
        internal readonly List<int> StreamSent=[];
        private readonly List<SyncStreamChunk> _streamChunks=[];
        private readonly MemoryStream _streamBytes=new();
        private SyncStreamInput? _streamInput;
        private string? _streamHash;
        private JsonElement? _streamPublication;
        private SyncStreamStatus StreamStatus(SyncStreamInput original)
        {
            if(ParallelStreamEnabled)return ParallelStatus(original,-1);
            var raw=JsonSerializer.SerializeToUtf8Bytes(new{schema="saturn.stream-upload-status.v1",operationId=original.OperationId,state=_streamPublication is not null?"PUBLISHED":_streamHash is not null?"SEALED":"RECEIVING",receivedSizeBytes=_streamBytes.Length.ToString(),chunks=_streamChunks,pendingChunk=(SyncStreamChunk?)null,finalSha256=_streamHash,publication=_streamPublication},LocalCycleWire.Json);
            return SaturnGuardedCycleTransport.ReadStream(raw,original);
        }
        public Task<SyncStreamStatus> StreamChunkAsync(SyncStreamInput original,SyncStreamChunk chunk,ReadOnlyMemory<byte> bytes,CancellationToken ct)
        {
            if(ParallelStreamEnabled)return ParallelChunk(original,chunk,bytes,ct);
            Assert.Equal(_streamChunks.Count,chunk.Index);Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(bytes.Span)),chunk.Sha256);StreamSent.Add(chunk.Index);_streamBytes.Write(bytes.Span);_streamChunks.Add(chunk);
            if(chunk.Index==1&&StreamLoseSecondAck){StreamLoseSecondAck=false;throw new IOException("fixture response lost after second chunk commit");}return Task.FromResult(StreamStatus(original));
        }
        public Task<SyncStreamStatus> StreamCommandAsync(string command,SyncStreamInput original,string? wholeHash,CancellationToken ct)
        {
            if(_streamInput is null)_streamInput=original;else Assert.Equal(JsonSerializer.Serialize(_streamInput),JsonSerializer.Serialize(original));
            if(command=="seal"){
                string hash;if(ParallelDestination is null)hash=Convert.ToHexStringLower(SHA256.HashData(_streamBytes.ToArray()));else{using var stream=File.OpenRead(ParallelDestination);hash=Convert.ToHexStringLower(SHA256.HashData(stream));}
                Assert.Equal(hash,wholeHash);_streamHash=StreamWrongSeal?new string('0',64):wholeHash;
            }
            if(command=="complete")
            {
                Assert.False(StreamWrongSeal,"Client must reject the wrong final seal before complete.");var input=JsonSerializer.Deserialize<CycleUploadInput>(original.Input.GetProperty("prepare"),LocalCycleWire.Json)!;input=input with{Intent=input.Intent with{Sha256=_streamHash!}};var hash=LocalCycleWire.IntentHash(input);var id=original.OperationId;
                var d=new SyncDescriptor(input.Intent.TargetId??Guid.NewGuid().ToString("D"),input.Intent.ParentId,input.Intent.Name,"file",input.Intent.SizeBytes,_streamHash,Guid.NewGuid().ToString("D"),Guid.NewGuid().ToString("D"),"AVAILABLE");_nodes[d.NodeId]=d;Event(d,id);Completes++;
                var result=new{schema="saturn.catalog-upload-result.v2",status="PUBLISHED",uploadId=id,operationId=id,rootId=Binding.RootId,intentHash=hash,publicationReceipt=new{schema="saturn.upload-publication.v1",uploadId=id,operationId=id,intentHash=hash,rootId=Binding.RootId,resourceId=d.NodeId,versionId=d.ContentVersionId,parentId=d.ParentId,name=d.Name,sizeBytes=d.SizeBytes,sha256=d.ContentSha256,etag="\"sha256-"+d.ContentSha256+"\"",publishedAt="2026-10-14T00:00:00.000Z"},publishedDescription=Wire(d),firstSeq=_events.Count.ToString(),lastSeq=_events.Count.ToString()};
                _streamPublication=JsonSerializer.SerializeToElement(new{session=new{uploadId=id,operationId=id,rootId=Binding.RootId,intentHash=hash,expectedSizeBytes=d.SizeBytes,receivedSizeBytes=d.SizeBytes,phase="published",transferPending=false},result,replayed=false});
            }
            return Task.FromResult(StreamStatus(original));
        }
    }
}
public sealed partial class Stage3ChatBTests
{
    [Theory]
    [InlineData("resume")][InlineData("modified")][InlineData("deleted")][InlineData("replaced")][InlineData("wrong-seal")][InlineData("quota")]
    public async Task B10_StreamRestartRetainsAckMapAndRefusesUnprovenFinalBytes(string scenario)
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);f.Remote.StreamEnabled=true;f.Remote.StreamLoseSecondAck=scenario is not("wrong-seal" or "quota");f.Remote.StreamWrongSeal=scenario=="wrong-seal";
        var data=new byte[12*1024*1024+17];new Random(42).NextBytes(data);var path=Path.Combine(f.Source,"large");await File.WriteAllBytesAsync(path,data,Ct);
        Task<WindowsFreshSyncEngine> StreamEngine()=>WindowsFreshSyncEngine.ResumeOrOpenAsync(f.Store,f.Source,Path.Combine(f.Directory,"spool"),Configuration(f),f.Remote,new(LargeFileThreshold:1024*1024,SpoolBytes:scenario=="quota"?4194303:16777216,MinimumFreeDiskBytes:0),cancellationToken:Ct);
        var engine=await StreamEngine();var first=await engine.InitialSyncAsync(Ct);await f.Evidence("B10-first-"+scenario,new{first,f.Remote.StreamSent});Assert.Equal(0,first.PublishedFiles);Assert.Equal(1,f.Remote.Count);
        if(scenario=="quota"){Assert.Empty(f.Remote.StreamSent);Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM client_stream_operations"));await engine.DisposeAsync();return;}
        var original=await f.Scalar("SELECT input_json FROM client_stream_operations");Assert.NotNull(original);
        if(scenario=="wrong-seal"){Assert.Equal(0,f.Remote.Completes);await engine.DisposeAsync();return;}
        Assert.Equal(new[]{0,1},f.Remote.StreamSent);await engine.DisposeAsync();
        if(scenario=="resume")await VerifyAcceptedProfileBackup(f,"stream");
        if(scenario=="modified")File.WriteAllText(path,"changed");if(scenario=="deleted")File.Delete(path);if(scenario=="replaced"){File.Move(path,Path.Combine(f.Directory,"old-version"));File.WriteAllBytes(path,data);}
        await AdvancePastDue(f,clock);f.Store=new(f.Database,clock);await using var resumed=await StreamEngine();var result=await f.FreshWhenDueAsync(resumed,Ct);
        await f.Evidence("B10-recovery-"+scenario,new{result,f.Remote.StreamSent,diagnostics=resumed.Diagnostics});Assert.Equal(original,await f.Scalar("SELECT input_json FROM client_stream_operations"));
        if(scenario=="resume"){Assert.Null(result.Error);Assert.Equal(new[]{0,1,2,3},f.Remote.StreamSent);Assert.Equal(1,result.PublishedFiles);Assert.Equal(data.Length,resumed.Diagnostics.StreamRecoveryReadBytes);Assert.Equal(2,f.Remote.Count);Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(data)),await f.Scalar("SELECT sha256 FROM local_cycle_work WHERE phase=5"));}
        else{Assert.Equal(new[]{0,1},f.Remote.StreamSent);Assert.Equal(0,result.PublishedFiles);Assert.Equal(1,f.Remote.Count);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM client_stream_operations WHERE seal_json IS NULL"));}
    }
}
