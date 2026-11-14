using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class Stage3ChatBTests
{
    [Theory]
    [InlineData(LocalJournalReadMode.Named)]
    [InlineData(LocalJournalReadMode.IdentityOnly)]
    public async Task DelayedStructuralHintAfterInstalledRenameReprovesMembership(LocalJournalReadMode mode)
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);
        f.Remote.ExtensionsEnabled=true;Directory.CreateDirectory(Path.Combine(f.Source,"before"));
        File.WriteAllText(Path.Combine(f.Source,"before","leaf"),"retained bytes");
        await using var engine=await WindowsFreshSyncEngine.ResumeOrOpenAsync(new WindowsCycleNativeOptions(mode,true),f.Store,f.Source,Path.Combine(f.Directory,"spool"),Configuration(f),f.Remote,new(MinimumFreeDiskBytes:0),cancellationToken:Ct);
        await ManagedBaseline(engine,f,clock,2);var remote=await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE kind=0");
        Directory.Move(Path.Combine(f.Source,"before"),Path.Combine(f.Source,"after"));await AwaitHint(f);clock.Advance(TimeSpan.FromSeconds(1));
        Assert.Null((await engine.FreshSyncAsync(Ct)).Error);Assert.Single(f.Remote.Mutations,x=>x.Kind=="move");
        // A durable watcher delivery can lag the USN cut which already installed
        // the rename. Its obsolete path then has no unconsumed USN parent.
        await Session(engine).MarkParentCycleHintsAsync(engine.ScopeId,["before"],Ct);
        var result=await engine.FreshSyncAsync(Ct);await f.Evidence("delayed-structural-"+mode,new{result,state=await NamespaceState(f)});
        Assert.Null(result.Error);Assert.Equal(0,result.PendingEntries);Assert.Equal(0,result.CapturedBytes);
        Assert.Equal(remote,await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE kind=0"));
        Assert.Equal("after/leaf",await f.Scalar("SELECT path FROM local_cycle_files WHERE kind=0"));
        Assert.Single(f.Remote.Mutations,x=>x.Kind=="move");Assert.DoesNotContain(f.Remote.Mutations,x=>x.Kind=="quarantine-delete");
        Assert.Equal("retained bytes",File.ReadAllText(Path.Combine(f.Source,"after","leaf")));Assert.Single(Directory.GetFiles(f.Source,"*",SearchOption.AllDirectories));
    }
}
