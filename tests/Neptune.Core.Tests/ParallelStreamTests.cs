using System.Security.Cryptography;
using System.Text.Json;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class WindowsFreshCycleTests
{
    internal sealed partial class FakeRemote
    {
        internal bool ParallelStreamEnabled;
        internal string? ParallelDestination;
        internal int ParallelStreamPeak;
        private int _parallelActive;
        private readonly TaskCompletionSource _fourParts=new(TaskCreationOptions.RunContinuationsAsynchronously);
        private SyncStreamStatus ParallelStatus(SyncStreamInput original,int after)
        {
            var chunks=_streamChunks.OrderBy(c=>c.Index).Where(c=>c.Index>after).Take(256).ToArray();
            int? next=chunks.Length==256&&_streamChunks.Any(c=>c.Index>chunks[^1].Index)?chunks[^1].Index:null;
            return SaturnGuardedCycleTransport.ReadStream(JsonSerializer.SerializeToUtf8Bytes(new{
                schema="saturn.stream-upload-status.v2",operationId=original.OperationId,
                state=_streamPublication is not null?"PUBLISHED":_streamHash is not null?"SEALED":"RECEIVING",
                receivedSizeBytes=_streamChunks.Sum(c=>long.Parse(c.Length)).ToString(),chunks,pendingChunks=Array.Empty<SyncStreamChunk>(),afterIndex=after,nextIndex=next,
                finalSha256=_streamHash,publication=_streamPublication},LocalCycleWire.Json),original);
        }
        public Task<SyncStreamStatus> StreamStatusPageAsync(SyncStreamInput input,int afterIndex,CancellationToken ct)
        {lock(_gate)return Task.FromResult(ParallelStatus(input,afterIndex));}
        private async Task<SyncStreamStatus> ParallelChunk(SyncStreamInput original,SyncStreamChunk chunk,ReadOnlyMemory<byte> bytes,CancellationToken ct)
        {
            var active=Interlocked.Increment(ref _parallelActive);
            lock(_gate){ParallelStreamPeak=Math.Max(ParallelStreamPeak,active);if(active==4)_fourParts.TrySetResult();}
            try{
                if(chunk.Index is >=1 and <=4)await _fourParts.Task.WaitAsync(TimeSpan.FromSeconds(10),ct);
                Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(bytes.Span)),chunk.Sha256);
                SyncStreamStatus status;bool lose;
                lock(_gate){
                    Assert.DoesNotContain(_streamChunks,c=>c.Index==chunk.Index);
                    StreamSent.Add(chunk.Index);
                    if(ParallelDestination is null){_streamBytes.Position=long.Parse(chunk.Offset);_streamBytes.Write(bytes.Span);}
                    else{using var destination=new FileStream(ParallelDestination,FileMode.OpenOrCreate,FileAccess.Write,FileShare.Read);destination.Position=long.Parse(chunk.Offset);destination.Write(bytes.Span);}
                    _streamChunks.Add(chunk);
                    status=ParallelStatus(original,chunk.Index-1);lose=chunk.Index==1&&StreamLoseSecondAck;if(lose)StreamLoseSecondAck=false;
                }
                // Replies deliberately arrive out of order, after durable ACK.
                await Task.Delay(chunk.Index%2==0?1:15,CancellationToken.None);
                if(lose)throw new IOException("lost v2 ACK after durable write");
                return status;
            }finally{Interlocked.Decrement(ref _parallelActive);}
        }
    }
}

public sealed partial class Stage3ChatBTests
{
    [Fact]
    public async Task ParallelNativeFileAboveTwoGiBUses513PartsAndExactReadback()
    {
        RequireWindows();await using var f=await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());
        f.Remote.StreamEnabled=true;f.Remote.ParallelStreamEnabled=true;f.Remote.ParallelDestination=Path.Combine(f.Directory,"remote-large-fixture");
        var path=Path.Combine(f.Source,"native-2GiB");const long length=2L*1024*1024*1024+17;
        using(var source=new FileStream(path,FileMode.CreateNew,FileAccess.Write,FileShare.None)){source.SetLength(length);source.Flush(true);}
        await using var engine=await WindowsFreshSyncEngine.ResumeOrOpenAsync(f.Store,f.Source,Path.Combine(f.Directory,"spool"),Configuration(f),f.Remote,new(LargeFileThreshold:1024*1024,SpoolBytes:32*1024*1024,MinimumFreeDiskBytes:0),cancellationToken:Ct);
        var result=await engine.InitialSyncAsync(Ct);await f.Evidence("native-2GiB",new{result,f.Remote.ParallelStreamPeak,parts=f.Remote.StreamSent.Count});
        Assert.Null(result.Error);Assert.Equal(1,result.PublishedFiles);Assert.Equal(length,result.ReceivedBytes);Assert.Equal(4,f.Remote.ParallelStreamPeak);
        Assert.Equal(Enumerable.Range(0,513),f.Remote.StreamSent.Order());Assert.Equal(513L,await f.Scalar("SELECT count(*) FROM client_stream_parts"));
        await using var a=File.OpenRead(path);await using var b=File.OpenRead(f.Remote.ParallelDestination);
        Assert.Equal(length,b.Length);Assert.Equal(await SHA256.HashDataAsync(a,Ct),await SHA256.HashDataAsync(b,Ct));
        Assert.Equal(new[]{path},Directory.GetFiles(f.Source));
    }
    [Theory]
    [InlineData("normal")][InlineData("resume")][InlineData("modified")][InlineData("wrong-seal")]
    public async Task ParallelStreamPinsSourceDrainsPartsAndResumesOnlyUnconfirmed(string mode)
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);
        f.Remote.StreamEnabled=true;f.Remote.ParallelStreamEnabled=true;f.Remote.StreamLoseSecondAck=mode is "resume" or "modified";f.Remote.StreamWrongSeal=mode=="wrong-seal";
        var data=new byte[32*1024*1024+17];new Random(51).NextBytes(data);var path=Path.Combine(f.Source,"large-v2");await File.WriteAllBytesAsync(path,data,Ct);
        Task<WindowsFreshSyncEngine> OpenStream()=>WindowsFreshSyncEngine.ResumeOrOpenAsync(f.Store,f.Source,Path.Combine(f.Directory,"spool"),Configuration(f),f.Remote,new(LargeFileThreshold:1024*1024,SpoolBytes:32*1024*1024,MinimumFreeDiskBytes:0),cancellationToken:Ct);
        var engine=await OpenStream();var first=await engine.InitialSyncAsync(Ct);await f.Evidence("parallel-first-"+mode,new{first,f.Remote.StreamSent,f.Remote.ParallelStreamPeak});
        Assert.Equal(4,f.Remote.ParallelStreamPeak);
        var original=await f.Scalar("SELECT input_json FROM client_stream_operations");Assert.NotNull(original);
        Assert.Equal("neptune.client-evolution.v2",await f.Scalar("SELECT minimum_writer FROM client_evolution_format"));
        await engine.DisposeAsync();
        if(mode=="wrong-seal"){Assert.Equal(0,first.PublishedFiles);Assert.Equal(0,f.Remote.Completes);return;}
        if(mode=="normal"){Assert.Null(first.Error);Assert.Equal(1,first.PublishedFiles);}
        else{
            Assert.Equal(0,first.PublishedFiles);var sent=f.Remote.StreamSent.ToArray();
            if(mode=="modified")File.WriteAllText(path,"edited after disconnect");
            await AdvancePastDue(f,clock);f.Store=new(f.Database,clock);await using var resumed=await OpenStream();var result=await f.FreshWhenDueAsync(resumed,Ct);
            await f.Evidence("parallel-recovery-"+mode,new{result,f.Remote.StreamSent});
            Assert.Equal(original,await f.Scalar("SELECT input_json FROM client_stream_operations"));
            if(mode=="modified"){Assert.Equal(0,result.PublishedFiles);Assert.Equal(sent,f.Remote.StreamSent);return;}
            Assert.Null(result.Error);Assert.Equal(1,result.PublishedFiles);
        }
        Assert.Equal(Enumerable.Range(0,9),f.Remote.StreamSent.Order());
        Assert.Equal(9L,await f.Scalar("SELECT count(*) FROM client_stream_parts"));
        Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(data)),await f.Scalar("SELECT sha256 FROM local_cycle_work WHERE phase=5"));
        Assert.Equal(new[]{path},Directory.GetFiles(f.Source));
    }
}
