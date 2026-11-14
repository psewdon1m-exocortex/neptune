using System.Text.Json;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;
public sealed class ParallelStreamWireTests
{
    private static readonly string Op=Guid.NewGuid().ToString("D");
    private static SyncStreamInput Original=>new(Op,"1",new string('a',64),JsonSerializer.SerializeToElement(new{schema="saturn.stream-upload.v2",prepare=new{intent=new{sizeBytes="68719476736"}}}));
    private static byte[] Status(object[] chunks,int after=-1,int? next=null,long received=0)=>JsonSerializer.SerializeToUtf8Bytes(new{schema="saturn.stream-upload-status.v2",operationId=Op,state="RECEIVING",receivedSizeBytes=received.ToString(),chunks,pendingChunks=Array.Empty<object>(),afterIndex=after,nextIndex=next,finalSha256=(string?)null,publication=(object?)null});
    private static object Part(int index)=>new{index,offset=((long)index*4194304).ToString(),length="4194304",sha256=new string('a',64)};
    [Fact]
    public void PagesBeyondOld256CapKeepExactIndicesAndBoundedWire()
    {
        var first=SaturnGuardedCycleTransport.ReadStream(Status(Enumerable.Range(0,256).Select(Part).ToArray(),next:255,received:261L*4194304),Original);
        var second=SaturnGuardedCycleTransport.ReadStream(Status(Enumerable.Range(256,5).Select(Part).ToArray(),after:255,received:261L*4194304),Original);
        Assert.True(first.Parallel);Assert.Equal(255,first.NextIndex);Assert.Equal(256,second.Chunks[0].Index);Assert.Null(second.NextIndex);
    }
    [Fact]
    public void InvalidPagesAndChangedReceiptsNeverBecomeAcknowledged()
    {
        foreach(var raw in new[]{Status([new{index=0,offset="0",length="4194304",sha256=new string('a',64),unknown=true}],received:4194304),Status([Part(1),Part(1)],received:8388608),Status([Part(0)],after:0,received:4194304),Status([Part(0)],next:0,received:4194304),Status([Part(0)],received:1),Status(Enumerable.Range(0,257).Select(Part).ToArray(),received:257L*4194304)})
            Assert.Throws<InvalidDataException>(()=>SaturnGuardedCycleTransport.ReadStream(raw,Original));
        Assert.True(LocalCycleWire.SameResult("{\"name\":\"a\",\"n\":1}","{\"n\":1,\"name\":\"\\u0061\"}"));
        Assert.False(LocalCycleWire.SameResult("{\"hash\":\"a\"}","{\"hash\":\"b\"}"));
        Assert.Throws<SyncWireException>(()=>LocalCycleWire.SameResult("{\"x\":1}","{\"x\":1,\"x\":1}"));
    }
}
