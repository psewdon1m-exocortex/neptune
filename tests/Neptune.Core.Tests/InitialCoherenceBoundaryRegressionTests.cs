using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class ParentReconciliationTests
{
    [Fact]
    public async Task InitialCutWithUnrelatedNativeEventsReconcilesSameCoherenceBeforeFinalBinding()
    {
        if(!OperatingSystem.IsWindows()){Assert.Skip("Native Windows required");return;}
        await using var f=await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());
        File.WriteAllText(Path.Combine(f.Source,"original"),"original immutable bytes");
        Directory.CreateDirectory(Path.Combine(f.Source,"empty"));
        var outside=Path.Combine(f.Directory,"unrelated");Directory.CreateDirectory(outside);
        await Medium(async()=>
        {
            var branches=0;
            await using var engine=await Open(f,observer:(o,ct)=>
            {
                if(o.Stage==WindowsCycleStage.BranchEnumerated && ++branches==3)
                    for(var i=0;i<32;i++)
                    {
                        var file=Path.Combine(outside,"external-"+i);File.WriteAllText(file,"outside event "+i);
                        Assert.True(CreateHardLinkW(Path.Combine(outside,"alias-"+i),file,nint.Zero));
                    }
                return Task.CompletedTask;
            });
            var first=await Initial(engine);
            await f.Evidence("initial-coherence-external-cut",new{first,branches});
            Assert.Null(first.Error);
            Assert.Equal("NoWorkAtAppliedEventBoundary",first.Decision);
            Assert.Equal(1,f.Remote.Prepares);
            Assert.Equal(1L,await f.Scalar("SELECT count(*) FROM local_cycle_scopes"));
            Assert.Equal(1L,await f.Scalar("SELECT count(*) FROM local_coherence_roots"));
            var original=await f.Scalar("SELECT group_concat(operation_id||input_json) FROM local_cycle_work WHERE input_json IS NOT NULL");
            var repeat=await Converge(engine,f);
            Assert.Null(repeat.Error);Assert.Equal(0,repeat.PublishedFiles);
            Assert.Equal(original,await f.Scalar("SELECT group_concat(operation_id||input_json) FROM local_cycle_work WHERE input_json IS NOT NULL"));
            return true;
        });
    }
}
