using Neptune.Core;
using Microsoft.Data.Sqlite;
using System.Reflection;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsFreshCycleTests
{
    [Theory]
    [InlineData(false,"eligible")][InlineData(true,"eligible")]
    [InlineData(false,"policy-disabled")][InlineData(true,"policy-disabled")]
    [InlineData(false,"captured")][InlineData(true,"captured")]
    [InlineData(false,"input")][InlineData(true,"input")]
    [InlineData(false,"retained-stream")][InlineData(true,"retained-stream")]
    public async Task LegacyStreamConflictReconsiderationPreservesTerminalRowsAndAttemptedOriginals(bool parallel,string kind)
    {
        if(!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture");return; }
        await using var f=await Fixture.Create();f.Remote.StreamEnabled=true;f.Remote.ParallelStreamEnabled=parallel;
        var text=new string('L',65537);var path=Path.Combine(f.Source,"legacy-stream.bin");
        File.WriteAllText(path,text);f.Remote.AddExisting("legacy-stream.bin",text);
        var original=f.Remote.CurrentFixtureNode("legacy-stream.bin");
        // A policy-disabled native pass constructs a real pre-input conflict.
        // The selector is then tested with an explicit policy input. Synthetic
        // retained-field controls below exist only in this owned unit fixture.
        await using var engine=await WindowsFreshSyncEngine.ResumeOrOpenAsync(f.Store,f.Source,
            Path.Combine(f.Directory,"spool"),AnchoredConfiguration(f),f.Remote,
            new(LargeFileThreshold:4096,SpoolBytes:32*1024*1024,MinimumFreeDiskBytes:0),cancellationToken:Ct);
        Assert.Equal(1,(await engine.InitialSyncAsync(Ct)).Conflicts);
        if(kind=="captured")await ExecuteSql(f,"UPDATE local_cycle_work SET capture_proof='retained fixture proof'");
        if(kind=="input")await ExecuteSql(f,"UPDATE local_cycle_work SET input_json='{\"retained\":true}'");
        if(kind=="retained-stream")await ExecuteSql(f,"INSERT INTO client_stream_operations(operation_id,scope_id,input_json) SELECT operation_id,scope_id,'{}' FROM local_cycle_work");
        const string workSnapshot="SELECT json_object('operation',operation_id,'scope',scope_id,'entry',entry_json,'phase',phase,'active',active,'spool',spool_path,'size',size,'sha',sha256,'proof',capture_proof,'input',input_json,'intent',intent_hash,'upload',upload_id,'received',received,'result',result_json,'error',error,'dispatch',dispatch_id,'bytes',bytes) FROM local_cycle_work";
        var before=await f.Scalar(workSnapshot);
        var session=(LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(engine)!;
        var scope=await session.ReadCycleScopeAsync(engine.ScopeId,Ct);
        scope=scope with{Configuration=scope.Configuration with{AllowVerifiedEqualAdoption=kind!="policy-disabled"}};
        await using(var c=new SqliteConnection(new SqliteConnectionStringBuilder{DataSource=NeptuneStateStore.NativeDatabasePath(f.Database),Pooling=false}.ToString()))
        {
            await c.OpenAsync(Ct);using var transaction=c.BeginTransaction(deferred:false);
            var method=typeof(NeptuneStateStore).GetMethod("ReconsiderUnattemptedLegacyStreamConflicts",BindingFlags.Static|BindingFlags.NonPublic)!;
            var count=await(Task<int>)method.Invoke(null,new object[]{c,transaction,scope,Ct})!;
            Assert.Equal(kind=="eligible"?1:0,count);await transaction.CommitAsync(Ct);
        }
        Assert.Equal(before,await f.Scalar(workSnapshot));
        Assert.Equal(kind=="eligible"?0L:2L,await f.Scalar("SELECT phase FROM local_cycle_files WHERE kind=0"));
        Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work"));
        Assert.Equal(original,f.Remote.CurrentFixtureNode("legacy-stream.bin"));
        Assert.Equal(0,f.Remote.Prepares);Assert.Empty(f.Remote.StreamSent);
        Assert.Equal(text,File.ReadAllText(path));
    }

    [Theory]
    [InlineData(false, "equal", true)]
    [InlineData(true, "equal", true)]
    [InlineData(false, "equal", false)]
    [InlineData(true, "equal", false)]
    [InlineData(false, "different", true)]
    [InlineData(true, "different", true)]
    [InlineData(false, "different-size", true)]
    [InlineData(true, "different-size", true)]
    public async Task StreamSizedExistingTargetRequiresVerifiedBytesAndExplicitAdoption(
        bool parallel, string content, bool allowAdoption)
    {
        if(!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture");return; }
        await using var f=await Fixture.Create();
        f.Remote.StreamEnabled=true;f.Remote.ParallelStreamEnabled=parallel;
        var local=new string('L',65537);
        var remote=content=="equal"?local:new string('R',content=="different-size"?65538:65537);
        var path=Path.Combine(f.Source,"existing-stream.bin");
        await File.WriteAllTextAsync(path,local,Ct);
        f.Remote.AddExisting("existing-stream.bin",remote);
        var original=f.Remote.CurrentFixtureNode("existing-stream.bin");
        var configuration=AnchoredConfiguration(f) with {AllowVerifiedEqualAdoption=allowAdoption};
        var budget=new WindowsCycleBudget(LargeFileThreshold:4096,SpoolBytes:32*1024*1024,MinimumFreeDiskBytes:0);
        var scope="";
        await using(var engine=await WindowsFreshSyncEngine.ResumeOrOpenAsync(f.Store,f.Source,
            Path.Combine(f.Directory,"spool"),configuration,f.Remote,budget,cancellationToken:Ct))
        {
            scope=engine.ScopeId;
            var result=await engine.InitialSyncAsync(Ct);
            await f.Evidence("existing-stream-"+parallel+"-"+content+"-"+allowAdoption,result);
            Assert.Null(result.Error);Assert.Equal(0,result.PublishedFiles);
            if(content=="equal"&&allowAdoption)
            {
                Assert.Equal(0,result.PendingEntries);Assert.Equal(0,result.Conflicts);
                Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);
                Assert.Equal(new WindowsCycleConfirmedTotals(1,65537),await engine.ReadConfirmedTotalsAsync(Ct));
            }
            else
            {
                Assert.Equal(1,result.Conflicts);Assert.Equal(1,result.PendingEntries);
                Assert.Equal(new WindowsCycleConfirmedTotals(0,0),await engine.ReadConfirmedTotalsAsync(Ct));
            }
        }
        Assert.Equal(original,f.Remote.CurrentFixtureNode("existing-stream.bin"));
        Assert.Equal(0,f.Remote.Prepares);Assert.Equal(0,f.Remote.Completes);Assert.Empty(f.Remote.StreamSent);
        Assert.Equal(local,await File.ReadAllTextAsync(path,Ct));
        if(content=="equal"&&allowAdoption)
        {
            await using var reopened=await WindowsFreshSyncEngine.ResumeOrOpenAsync(f.Store,f.Source,
                Path.Combine(f.Directory,"spool"),configuration,f.Remote,budget,cancellationToken:Ct);
            Assert.Equal(scope,reopened.ScopeId);
            var noop=await reopened.FreshSyncAsync(Ct);
            Assert.Null(noop.Error);Assert.Equal(0,noop.PendingEntries);Assert.Equal(0,noop.CapturedFiles);
            Assert.Equal(0,noop.PublishedFiles);Assert.Equal(original,f.Remote.CurrentFixtureNode("existing-stream.bin"));
        }
    }
}
