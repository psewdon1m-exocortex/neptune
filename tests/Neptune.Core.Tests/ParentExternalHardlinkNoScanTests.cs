using System.Reflection;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class ParentReconciliationTests
{
    [Fact]
    public async Task FirstOutsideHardlinkUsesNativeWitnessWithoutFullScan()
    {
        if(!OperatingSystem.IsWindows()){Assert.Skip("Native Windows required");return;}
        await using var f=await WindowsFreshCycleTests.Fixture.Create();
        Directory.CreateDirectory(Path.Combine(f.Source,"deep","branch"));
        File.WriteAllText(Path.Combine(f.Source,"deep","branch","original"),"baseline");
        var outside=Path.Combine(f.Directory,"outside");File.WriteAllText(outside,"outside bytes");
        await Medium(async()=>
        {
            var branches=0;await using var e=await Open(f,observer:(o,t)=>{if(o.Stage==WindowsCycleStage.BranchEnumerated)branches++;return Task.CompletedTask;});
            Assert.Null((await Initial(e)).Error);var before=branches;
            var original=await f.Scalar("SELECT group_concat(input_json||intent_hash) FROM local_cycle_work WHERE input_json IS NOT NULL");
            var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'");
            Assert.True(CreateHardLinkW(Path.Combine(f.Directory,"outside-alias"),outside,nint.Zero));
            var result=await Converge(e,f);await f.Evidence("first-outside-no-scan",new{before,branches,result});
            Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);
            Assert.Equal(before,branches);Assert.Equal(0,result.CapturedFiles);Assert.Equal(0,result.PublishedFiles);
            Assert.Equal(0L,await f.Scalar("SELECT count(*) FROM local_parent_reconcile_plans"));
            Assert.True(Convert.ToInt64(await f.Scalar("SELECT count(*) FROM local_parent_reconcile_external_cuts"))>0);
            Assert.Equal(original,await f.Scalar("SELECT group_concat(input_json||intent_hash) FROM local_cycle_work WHERE input_json IS NOT NULL"));
            Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));
            Assert.Single(Directory.GetFiles(f.Source,"*",SearchOption.AllDirectories));
            var scope=e.ScopeId;await e.DisposeAsync();
            await using var resumed=await Open(f,true,observer:(o,t)=>{if(o.Stage==WindowsCycleStage.BranchEnumerated)branches++;return Task.CompletedTask;});
            Assert.Equal(scope,resumed.ScopeId);var repeat=await Converge(resumed,f);Assert.Null(repeat.Error);Assert.Equal(before,branches);Assert.Equal(0,repeat.CapturedFiles);
            var cut=Convert.ToInt64(await f.Scalar("SELECT max(cut_usn) FROM local_parent_reconcile_external_cuts"));
            File.WriteAllText(Path.Combine(f.Source,"deep","branch","new"),"inside change");
            var changed=await Converge(resumed,f);Assert.Null(changed.Error);Assert.Equal(1,changed.PublishedFiles);
            var triggers=await FullProofTriggersAfter(resumed,cut);
            var full=Convert.ToInt64(await f.Scalar("SELECT full_membership FROM local_parent_reconcile_plans WHERE status=3 ORDER BY rowid DESC LIMIT 1"));
            await f.Evidence("outside-followed-by-inside",new{before,branches,changed,cut,full,triggers});
            if(triggers.Count==0)Assert.Equal(0,full);
            Assert.Equal(before+(full==1?3:1),branches);return true;
        });
    }

    [Fact]
    public async Task UnstableOutsideWitnessDefersWithoutFullScanThenRecovers()
    {
        if(!OperatingSystem.IsWindows()){Assert.Skip("Native Windows required");return;}
        await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"baseline");
        var outside=Path.Combine(f.Directory,"outside");File.WriteAllText(outside,"outside bytes");
        await Medium(async()=>
        {
            var branches=0;await using var e=await Open(f,observer:(o,t)=>{if(o.Stage==WindowsCycleStage.BranchEnumerated)branches++;return Task.CompletedTask;});
            Assert.Null((await Initial(e)).Error);var before=branches;var effects=f.Remote.Prepares;
            var original=await f.Scalar("SELECT group_concat(input_json||intent_hash) FROM local_cycle_work WHERE input_json IS NOT NULL");
            var coordinator=(WindowsLocalCoherenceCoordinator)typeof(WindowsFreshSyncEngine).GetField("_coordinator",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(e)!;
            var hook=typeof(WindowsLocalCoherenceCoordinator).GetProperty("ExternalHardlinkProofOpened",BindingFlags.Instance|BindingFlags.NonPublic);
            Assert.NotNull(hook);var changes=0;
            hook.SetValue(coordinator,(Func<CancellationToken,Task>)(ct=>{Assert.True(CreateHardLinkW(Path.Combine(f.Directory,"racing-alias-"+(++changes)),outside,nint.Zero));return Task.CompletedTask;}));
            Assert.True(CreateHardLinkW(Path.Combine(f.Directory,"outside-alias"),outside,nint.Zero));
            var deferred=await e.FreshSyncAsync(Ct);await f.Evidence("unstable-outside-no-scan",new{changes,before,branches,deferred});
            Assert.NotNull(deferred.Error);Assert.InRange(changes,1,3);Assert.Equal(before,branches);Assert.Equal(effects,f.Remote.Prepares);
            Assert.Equal(original,await f.Scalar("SELECT group_concat(input_json||intent_hash) FROM local_cycle_work WHERE input_json IS NOT NULL"));
            Assert.Equal(0L,await f.Scalar("SELECT count(*) FROM local_parent_reconcile_plans"));
            hook.SetValue(coordinator,null);await e.RetryDeferredAsync(Ct);
            var recovered=await Converge(e,f);Assert.Null(recovered.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",recovered.Decision);Assert.Equal(before,branches);
            Assert.Equal(effects,f.Remote.Prepares);Assert.Single(Directory.GetFiles(f.Source));return true;
        });
    }

    private static async Task<List<object>> FullProofTriggersAfter(WindowsFreshSyncEngine engine,long cut)
    {
        var session=(LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(engine)!;
        var coherence=(string)typeof(WindowsFreshSyncEngine).GetField("_coherence",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(engine)!;
        var result=new List<object>();var after=cut-1;
        for(var n=0;n<1000;n++)
        {
            var page=await session.ReadCoherenceUnresolvedPageAsync(coherence,after,Ct);if(page.Count==0)return result;
            foreach(var item in page)if((item.Record.Reason&0x13200)!=0)result.Add(new{item.Usn,item.Code,item.Record.Reason});
            after=page[^1].Usn;
        }
        throw new InvalidOperationException("Test retained evidence exceeded observation budget.");
    }
}
