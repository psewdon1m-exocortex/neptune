using System.Reflection;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsFreshCycleTests
{
    [Fact]
    public async Task ActualUnsealedSpoolFromInterruptedCaptureRetriesSameWork()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await Fixture.Create(new Stage3Clock());
        File.WriteAllText(Path.Combine(f.Source,"file.md"),"closed original source");
        string? operation=null;
        await using(var engine=await Resume(f))
        {
            var capture=(WindowsNativeCycleCapture)typeof(WindowsFreshSyncEngine).GetField("_capture",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(engine)!;
            Func<NativeCaptureStage,CancellationToken,Task> interrupted=async(stage,ct)=>
            {
                if(stage!=NativeCaptureStage.GuardOpened||operation is not null)return;
                operation=(string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE phase=0 AND active=1"))!;
                // The previous process died after creating bytes but before Seal.
                // No capture proof, upload intent or remote operation exists.
                await File.WriteAllTextAsync(Path.Combine(f.Directory,"spool",operation+".spool"),"partial old capture",ct);
            };
            typeof(WindowsNativeCycleCapture).GetField("_observer",BindingFlags.Instance|BindingFlags.NonPublic)!.SetValue(capture,interrupted);
            var first=await engine.InitialSyncAsync(Ct);
            Assert.Equal(1,first.Deferred);
            Assert.Equal(0,first.Conflicts);
            Assert.Equal(0,f.Remote.Prepares);
            Assert.Equal(8L,await f.Scalar("SELECT phase FROM local_cycle_work WHERE active=1"));
        }
        await using var resumed=await Resume(f);
        await resumed.RetryDeferredAsync(Ct);
        var result=await f.FreshWhenDueAsync(resumed,Ct);
        Assert.Null(result.Error);
        Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);
        Assert.Equal(operation,await f.Scalar("SELECT operation_id FROM local_cycle_work"));
        Assert.Equal(1,f.Remote.Prepares);
        Assert.Equal(1,f.Remote.Completes);
        Assert.Equal("closed original source",File.ReadAllText(Path.Combine(f.Source,"file.md")));
        Assert.Single(Directory.GetFiles(f.Source));
        Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory,"spool"),"*.spool"));
    }
}
