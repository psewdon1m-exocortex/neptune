using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsFreshCycleTests
{
    [Theory]
    [InlineData(false)][InlineData(true)]
    public async Task NativeJournalAdvancesDuringHeldTransferAndPumpJoinsOnExit(bool cancel)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await Fixture.Create(new Stage3Clock());var file=Path.Combine(f.Source,"file");File.WriteAllText(file,"first");
        var held=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);var release=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);var armed=0;
        await using var engine=await f.Open(async(o,ct)=>{if(o.Stage==WindowsCycleStage.FileCaptured&&Interlocked.CompareExchange(ref armed,2,1)==1){held.SetResult();await release.Task.WaitAsync(ct);}});
        Assert.Null((await engine.InitialSyncAsync(Ct)).Error);var original=await f.Scalar("SELECT result_json FROM local_cycle_work WHERE phase=5 ORDER BY work_no LIMIT 1");
        File.WriteAllText(file,"second");armed=1;using var stop=CancellationTokenSource.CreateLinkedTokenSource(Ct);var running=f.FreshWhenDueAsync(engine,stop.Token);var advanced=false;
        try
        {
            await held.Task.WaitAsync(TimeSpan.FromSeconds(15),Ct);var before=(long)(await f.Scalar("SELECT applied_usn FROM local_coherence_roots WHERE status=1"))!;
            // A later generation while the earlier sealed bytes are held.
            File.WriteAllText(file,"third generation while transfer is held");
            var until=DateTime.UtcNow.AddSeconds(8);
            while(DateTime.UtcNow<until){if((long)(await f.Scalar("SELECT applied_usn FROM local_coherence_roots WHERE status=1"))!>before){advanced=true;break;}await Task.Delay(100,Ct);}
        }
        finally{if(cancel)stop.Cancel();release.TrySetResult();}
        var result=await running.WaitAsync(TimeSpan.FromSeconds(20),Ct);Assert.True(advanced,"Native journal must advance before a blocked transfer is released.");
        Assert.Equal(original,await f.Scalar("SELECT result_json FROM local_cycle_work WHERE phase=5 ORDER BY work_no LIMIT 1"));
        if(cancel){Assert.NotNull(result.Error);Assert.Equal("PendingOrUncertain",result.Decision);}
        else{Assert.Null(result.Error);var settled=await f.FreshWhenDueAsync(engine,Ct);Assert.Null(settled.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",settled.Decision);Assert.Equal(2,result.PublishedFiles+settled.PublishedFiles);Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files f JOIN local_coherence_dirty d ON d.node_id=f.node_id WHERE f.ack_journal<d.generation"));}
        var cursor=await f.Scalar("SELECT cursor FROM local_source_roots WHERE valid=1");File.WriteAllText(Path.Combine(f.Directory,"outside-after-join"),"outside");await Task.Delay(1200,Ct);
        Assert.Equal(cursor,await f.Scalar("SELECT cursor FROM local_source_roots WHERE valid=1"));
        await f.Evidence("continuous-native-journal-"+cancel,new{advanced,result,joined=true});
    }
}
