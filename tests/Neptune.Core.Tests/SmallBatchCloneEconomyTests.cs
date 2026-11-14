using System.Text.Json;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    // These native regressions must run on the task's isolated NTFS volume.
    private static void RequireCloneEconomyVolume()
    {
        var manifest=Environment.GetEnvironmentVariable("SMALL_FILE_NATIVE_VOLUME")??throw new InvalidOperationException("Owned native volume required");
        using var document=JsonDocument.Parse(File.ReadAllBytes(manifest));var m=document.RootElement;
        Assert.Equal("ready",m.GetProperty("state").GetString());
        var root=Path.GetFullPath(Environment.GetEnvironmentVariable("NEPTUNE_FRESH_CYCLE_TEST_ROOT")??throw new InvalidOperationException("Owned fixture directory required"));
        Assert.Equal(Path.Combine(m.GetProperty("nativePath").GetString()!,"clone-economy-tests"),root);
        Assert.NotEqual(Path.GetPathRoot(manifest),Path.GetPathRoot(root));
    }

    [Theory]
    [Trait("Lane", "Live")]
    [InlineData(true,1024,0,true)]
    [InlineData(true,262144,0,true)]
    [InlineData(false,1024,4,true)]
    [InlineData(true,262145,4,true)]
    [InlineData(true,1024,4,false)]
    public async Task CloneEconomyOnlySkipsNewLookupForQualifiedSmallBatches(bool batches,int size,int expectedLookups,bool available)
    {
        RequireCloneEconomyVolume();await using var f=await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());
        BatchFiles(f,4,size);f.Remote.ExtensionsEnabled=true;f.Remote.CloneEnabled=true;
        await Medium(async()=>{
            var remote=new BatchRemote(f.Remote){Available=available};await using var engine=await OpenBatchEngine(f,remote,new(MinimumFreeDiskBytes:1L<<30));
            if(batches)Assert.Equal(available,await engine.EnableSmallFileBatchQualificationAsync(Ct));
            var result=await f.InitialWhenDueAsync(engine,Ct);
            await f.Evidence("clone-economy-"+batches+"-"+size,new{result,remote.ContentLookups,remote.BatchSends,expectedLookups});
            Assert.Null(result.Error);Assert.Equal(4,result.PublishedFiles);Assert.Equal(expectedLookups,remote.ContentLookups);
            Assert.Equal(batches&&available&&size<=262144?1:0,remote.BatchSends);
            Assert.Equal(4,Directory.GetFiles(f.Source).Length);return true;
        });
    }

    [Fact]
    [Trait("Lane", "Live")]
    public async Task CloneEconomyKeepsEmptyFileMutationWithBatches()
    {
        RequireCloneEconomyVolume();await using var f=await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());
        File.WriteAllBytes(Path.Combine(f.Source,"empty"),[]);f.Remote.ExtensionsEnabled=true;f.Remote.CloneEnabled=true;
        await Medium(async()=>{
            var remote=new BatchRemote(f.Remote);await using var engine=await OpenBatchEngine(f,remote,new(MinimumFreeDiskBytes:1L<<30));
            Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
            var result=await f.InitialWhenDueAsync(engine,Ct);
            Assert.Null(result.Error);Assert.Equal(1,result.PublishedFiles);Assert.Equal(0,remote.ContentLookups);
            Assert.Equal("create-empty",Assert.Single(f.Remote.Mutations).Kind);Assert.Equal(0,remote.BatchSends);return true;
        });
    }

    [Fact]
    [Trait("Lane", "Live")]
    public async Task CloneEconomyKeepsRecordedCloneBeforeNewBatchesAfterLostAcknowledgement()
    {
        RequireCloneEconomyVolume();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);
        f.Remote.ExtensionsEnabled=true;f.Remote.CloneEnabled=true;f.Remote.AddExisting("known","clone bytes");
        var source=f.Remote.CurrentFixtureNode("known");File.WriteAllText(Path.Combine(f.Source,"target"),"clone bytes");
        f.Remote.LoseMutationReply=true;
        await Medium(async()=>{
            var remote=new BatchRemote(f.Remote);string original;
            await using(var engine=await OpenBatchEngine(f,remote,new(MinimumFreeDiskBytes:1L<<30))){
                var first=await engine.InitialSyncAsync(Ct);Assert.True(first.PendingEntries>0);
                original=(string)(await f.Scalar("SELECT request_json FROM client_namespace_operations WHERE kind='clone-content'"))!;
                Assert.Equal(1,remote.ContentLookups);Assert.Single(f.Remote.Mutations);
            }
            await Stage3ChatBTests.AdvancePastDue(f,clock);
            await using var resumed=await OpenBatchEngine(f,remote,new(MinimumFreeDiskBytes:1L<<30),resume:true);
            Assert.True(await resumed.EnableSmallFileBatchQualificationAsync(Ct));
            var recovered=await f.FreshWhenDueAsync(resumed,Ct);
            await f.Evidence("clone-economy-recorded-clone",new{recovered,remote.ContentLookups,remote.BatchSends,f.Remote.MutationStatusReads});
            Assert.Null(recovered.Error);Assert.Equal(0,recovered.PendingEntries);Assert.Equal(0,recovered.CapturedBytes);
            Assert.Equal(1,remote.ContentLookups);Assert.Single(f.Remote.Mutations);Assert.True(f.Remote.MutationStatusReads>0);
            Assert.Equal(0,remote.BatchSends);Assert.Equal(0,f.Remote.Prepares);
            Assert.Equal(original,await f.Scalar("SELECT request_json FROM client_namespace_operations WHERE kind='clone-content'"));
            Assert.Equal(source,f.Remote.CurrentFixtureNode("known"));
            Assert.NotEqual(source.NodeId,f.Remote.CurrentFixtureNode("target").NodeId);return true;
        });
    }
}
