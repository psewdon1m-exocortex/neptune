using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class Stage3ChatBTests
{
    private static async Task<WindowsCycleResult> SettleNativeChange(WindowsFreshSyncEngine engine,WindowsFreshCycleTests.Fixture f,Stage3Clock clock)
    {
        await AwaitHint(f);WindowsCycleResult result=null!;
        for(var i=0;i<12;i++)
        {
            await AdvancePastDue(f,clock);result=await engine.FreshSyncAsync(Ct);Assert.Null(result.Error);
            if(result.PendingEntries==0)return result;
            await Task.Delay(100,Ct);
        }
        Assert.Equal(0,result.PendingEntries);return result;
    }
    [Fact]
    public async Task AtomicIncarnationSupportsRenameAndConservativeMetadataRecheck()
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);f.Remote.ExtensionsEnabled=true;
        var path=Path.Combine(f.Source,"before.md");File.WriteAllText(path,"original");
        await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,1);
        var resource=await f.Scalar("SELECT remote_node FROM local_cycle_files");
        var temporary=Path.Combine(f.Directory,"save.tmp");File.WriteAllText(temporary,"latest atomic bytes");File.Replace(temporary,path,null);
        await SettleNativeChange(engine,f,clock);
        Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_incarnations"));
        var prepares=f.Remote.Prepares;var baseline=f.Remote.CurrentFixtureNode("before.md");
        var renamed=Path.Combine(f.Source,"after.md");File.Move(path,renamed);await SettleNativeChange(engine,f,clock);
        Assert.Equal(resource,await f.Scalar("SELECT remote_node FROM local_cycle_files"));
        Assert.Equal("after.md",await f.Scalar("SELECT path FROM local_cycle_files"));Assert.Equal(prepares,f.Remote.Prepares);
        Assert.Single(f.Remote.Mutations,m=>m.Kind=="move");
        File.SetLastWriteTimeUtc(renamed,DateTime.UtcNow.AddMinutes(2));var metadata=await SettleNativeChange(engine,f,clock);
        await f.Evidence("atomic-metadata-observed",new{resource,preparesBefore=prepares,preparesAfter=f.Remote.Prepares,f.Remote.Mutations,state=await NamespaceState(f)});
        // The retained cumulative native reasons include the earlier rename.
        // Conservative recapture is required; the identical hash must not upload
        // or create a content version merely to acknowledge the newer hint.
        Assert.Equal(19,metadata.CapturedBytes);Assert.Equal(prepares,f.Remote.Prepares);Assert.Single(f.Remote.Mutations);
        Assert.Equal(baseline.ContentVersionId,f.Remote.CurrentFixtureNode("after.md").ContentVersionId);
        Assert.Equal(2,f.Remote.Count);Assert.Equal(new[]{"after.md"},Directory.GetFiles(f.Source).Select(Path.GetFileName));
        await f.Evidence("atomic-rename-metadata",new{resource,prepares,f.Remote.Mutations,state=await NamespaceState(f)});
    }
    [Theory]
    [InlineData("missing")][InlineData("replaced")][InlineData("locked")]
    public async Task StreamDefersAtomicRaceBeforeAnyRemoteBegin(string change)
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);
        var path=Path.Combine(f.Source,"stream");File.WriteAllText(path,"original");
        await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,1);
        var op=(string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE phase=5 ORDER BY work_no DESC LIMIT 1"))!;
        var work=await Session(engine).ReadCycleWorkAsync(engine.ScopeId,op,Ct);
        using var writer=change=="locked"?new FileStream(path,FileMode.Open,FileAccess.ReadWrite,FileShare.None):null;
        if(change=="missing")File.Move(path,Path.Combine(f.Directory,"retained"));
        if(change=="replaced"){var temp=Path.Combine(f.Directory,"save.tmp");File.WriteAllText(temp,"different");File.Replace(temp,path,null);}
        await using var capture=await WindowsNativeCycleCapture.OpenAsync(f.Source,Ct);var began=false;var chunks=0;
        await Assert.ThrowsAsync<BootstrapCaptureDeferredException>(()=>capture.CaptureStreamAsync(work,Path.Combine(f.Directory,"chunk"),
            (w,s,ct)=>{began=true;return Task.CompletedTask;},(i,b,ct)=>{chunks++;return Task.CompletedTask;},ct=>Task.FromResult<IDisposable>(new NoopLease()),Ct));
        Assert.False(began);Assert.Equal(0,chunks);
    }
    [Fact]
    public async Task AtomicIncarnationRetainsOwnershipAfterQualifiedJournalReplacement()
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);
        var path=Path.Combine(f.Source,"note.md");File.WriteAllText(path,"original");string old;object? resource;
        await using(var engine=await NamespaceEngine(f))
        {
            await ManagedBaseline(engine,f,clock,1);old=engine.ScopeId;resource=await f.Scalar("SELECT remote_node FROM local_cycle_files");
            var temp=Path.Combine(f.Directory,"save.tmp");File.WriteAllText(temp,"atomic latest");File.Replace(temp,path,null);await SettleNativeChange(engine,f,clock);
            await Session(engine).MarkGapAsync("JournalGap",Ct);
        }
        var prepares=f.Remote.Prepares;var originals=await f.Scalar("SELECT group_concat(entry_json||input_json||capture_proof) FROM local_cycle_work WHERE scope_id='"+old+"'");
        await using var replacement=await WindowsFreshSyncEngine.OpenJournalReplacementAsync(new WindowsCycleNativeOptions(LocalJournalReadMode.Named,true),new(Guid.NewGuid().ToString("N"),old),f.Store,f.Source,Path.Combine(f.Directory,"spool"),Configuration(f),f.Remote,new(MinimumFreeDiskBytes:0),cancellationToken:Ct);
        await replacement.QualifyJournalReplacementAsync(Ct);WindowsCycleResult result=null!;
        for(var i=0;i<12;i++){await AdvancePastDue(f,clock);result=await replacement.FreshSyncAsync(Ct);Assert.Null(result.Error);if(result.PendingEntries==0)break;await Task.Delay(100,Ct);}
        await f.Evidence("atomic-journal-replacement",new{result,old,replacement.ScopeId,preparesBefore=prepares,preparesAfter=f.Remote.Prepares,state=await NamespaceState(f)});
        Assert.Equal(0,result.PendingEntries);Assert.Equal(0,result.Conflicts);Assert.Equal(prepares,f.Remote.Prepares);
        Assert.Equal(resource,await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE scope_id='"+replacement.ScopeId+"'"));
        Assert.Equal(originals,await f.Scalar("SELECT group_concat(entry_json||input_json||capture_proof) FROM local_cycle_work WHERE scope_id='"+old+"'"));Assert.Equal(2,f.Remote.Count);
        File.WriteAllText(path,"in-place after journal recovery");await SettleNativeChange(replacement,f,clock);
        Assert.Equal(resource,await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE scope_id='"+replacement.ScopeId+"'"));Assert.Equal(2,f.Remote.Count);
    }
}
