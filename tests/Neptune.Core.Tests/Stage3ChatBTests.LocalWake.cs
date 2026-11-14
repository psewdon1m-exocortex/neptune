using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class Stage3ChatBTests
{
    [Fact]
    public async Task B07_NativeLocalWorkCancelsPollAndBeforeSubscribeSurvivesRestart()
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);f.Remote.WaitEnabled=true;
        var entered=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);var joined=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);var calls=0;
        f.Remote.WaitHandler=async(_,_,ct)=>{Interlocked.Increment(ref calls);entered.TrySetResult();try{await Task.Delay(Timeout.Infinite,ct);throw new InvalidOperationException("Unreachable fixture wait.");}finally{joined.TrySetResult();}};
        await using(var engine=await Open(f))
        {
            Assert.Null((await engine.InitialSyncAsync(Ct)).Error);var session=Session(engine);var before=await session.ReadCycleRemoteProgressAsync(engine.ScopeId,Ct);
            using var shutdown=CancellationTokenSource.CreateLinkedTokenSource(Ct);var waiting=engine.WaitForRemoteHintAsync(shutdown.Token);
            try
            {
                await entered.Task.WaitAsync(TimeSpan.FromSeconds(5),Ct);File.WriteAllText(Path.Combine(f.Source,"local"),"pending");await AwaitHint(f);clock.Advance(TimeSpan.FromSeconds(1));
                Assert.True(await waiting.WaitAsync(TimeSpan.FromSeconds(5),Ct));Assert.True(joined.Task.IsCompleted);Assert.Equal(1,calls);
                Assert.Equal(before,await session.ReadCycleRemoteProgressAsync(engine.ScopeId,Ct));Assert.NotEmpty((await session.ReadEventHintsAsync(Ct)).Hints);
                Assert.True(await engine.WaitForRemoteHintAsync(Ct).WaitAsync(TimeSpan.FromSeconds(5),Ct));Assert.Equal(1,calls);
            }
            finally{shutdown.Cancel();try{await waiting;}catch(OperationCanceledException){}}
        }
        f.Store=new(f.Database,clock);await using var restored=await Open(f);Assert.True(await restored.WaitForRemoteHintAsync(Ct).WaitAsync(TimeSpan.FromSeconds(5),Ct));Assert.Equal(1,calls);
        Assert.NotEmpty((await Session(restored).ReadEventHintsAsync(Ct)).Hints);await f.Evidence("B07-local-wake",new{calls,joined=joined.Task.IsCompleted,beforeSubscribe=true,restarted=true});
    }
}
