using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class Stage3ChatBTests
{
    [Theory]
    [InlineData("project2/private.bin","project2",true,0)]
    [InlineData("PROJECT2/private.bin","project2",true,0)]
    [InlineData("cafe\u0301/secret.bin","café",true,0)]
    [InlineData("project20/keep.bin","project2",false,0)]
    [InlineData("visible/file.bin","project2",false,0)]
    [InlineData("project2/private.bin","project2",false,2)]
    [InlineData("project2/private.bin","project2",false,3)]
    public async Task BindingAdoptionHonorsExcludedPendingPathsWithoutHidingIncludedGenerations(string relative,string excluded,bool allowed,int phase)
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);
        File.WriteAllText(Path.Combine(f.Source,"healthy.bin"),"accepted original");
        await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,1);
        var initialPolicy=new MappingSyncPolicy("legacy",[relative.Split('/')[0]]);
        var preview=await engine.PreviewPolicyAsync(initialPolicy,Ct);await engine.ApplyPolicyAsync(initialPolicy,preview.Digest,Ct);
        var local=Path.Combine(f.Source,relative.Replace('/',Path.DirectorySeparatorChar));Directory.CreateDirectory(Path.GetDirectoryName(local)!);File.WriteAllText(local,"excluded source must stay local");
        var settled=await SettleNativeChange(engine,f,clock);Assert.Null(settled.Error);Assert.Equal(0,settled.PendingEntries);
        var policy=new MappingSyncPolicy("legacy",[excluded]);preview=await engine.PreviewPolicyAsync(policy,Ct);await engine.ApplyPolicyAsync(policy,preview.Digest,Ct);
        Assert.True(Convert.ToInt64(await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE phase=0"))>0);
        Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE active=1"));
        var original=engine.ScopeId;await engine.DisposeAsync();
        // Isolated fixture control: an excluded non-candidate phase must still block.
        if(phase!=0)await Sql(f,"UPDATE local_cycle_files SET phase=$phase WHERE scope_id=$scope AND path=$path",("$phase",phase),("$scope",original),("$path",relative));
        var files=await f.Scalar("SELECT json_group_array(json_array(node_id,parent_id,path,phase,baseline,remote_node)) FROM(SELECT * FROM local_cycle_files WHERE scope_id='"+original+"' ORDER BY node_id)");
        var history=await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)");
        var binding=f.Remote.Binding with{BindingVersion=Guid.NewGuid().ToString("D")};
        var artifact=JsonSerializer.Serialize(new{schema="saturn.sync-adoption.v1",planId=Guid.NewGuid().ToString("D"),planSha256=new string('a',64),treeSha256=new string('b',64),binding=LocalCycleWire.BindingObject(binding),@namespace=new{codec="legacy",policyHash=new string('c',64)},device_id=f.Remote.Device,backend_identity="local-test",qualification_receipt_sha256=new string('d',64)});
        var plan=await f.Store.PreviewServerBindingAsync(artifact,Ct);
        if(allowed)
        {
            Assert.Empty(plan.Blockers);await f.Store.ApplyServerBindingAsync(plan,Ct);
            Assert.Equal(policy.Hash,plan.Policy.Hash);Assert.True(plan.Policy.Excludes(relative));
            Assert.Equal("QUALIFYING",await f.Scalar("SELECT state FROM client_binding_migrations"));
        }
        else
        {
            Assert.Contains("Resolve pending local generations/conflicts before adoption.",plan.Blockers);
            await Assert.ThrowsAsync<InvalidDataException>(()=>f.Store.ApplyServerBindingAsync(plan,Ct));
        }
        Assert.Equal(files,await f.Scalar("SELECT json_group_array(json_array(node_id,parent_id,path,phase,baseline,remote_node)) FROM(SELECT * FROM local_cycle_files WHERE scope_id='"+original+"' ORDER BY node_id)"));
        Assert.Equal(history,await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"));
        Assert.Equal("excluded source must stay local",File.ReadAllText(local));
        Assert.Equal("accepted original",File.ReadAllText(Path.Combine(f.Source,"healthy.bin")));
    }

    [Fact]
    public async Task B12_BindingAdoptionPageCrashReopenIdentityAndFreshFeedConflict()
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);
        for(var i=0;i<251;i++)File.WriteAllText(Path.Combine(f.Source,"file-"+i),"x");
        await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,251);var original=engine.ScopeId;
        var save=Path.Combine(f.Directory,"atomic-save.tmp");File.WriteAllText(save,"latest");File.Replace(save,Path.Combine(f.Source,"file-0"),null);
        await SettleNativeChange(engine,f,clock);var native=await f.Scalar("SELECT native_id FROM local_parent_reconcile_incarnations WHERE scope_id='"+original+"'");Assert.NotNull(native);await engine.DisposeAsync();
        var old=f.Remote.Binding;var binding=old with{BindingVersion=Guid.NewGuid().ToString("D")};
        var artifact=JsonSerializer.Serialize(new{schema="saturn.sync-adoption.v1",planId=Guid.NewGuid().ToString("D"),planSha256=new string('a',64),treeSha256=new string('b',64),binding=LocalCycleWire.BindingObject(binding),@namespace=new{codec="legacy",policyHash=new string('c',64)},device_id=f.Remote.Device,backend_identity="local-test",qualification_receipt_sha256=new string('d',64)});
        var originals=await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)");
        var plan=await f.Store.PreviewServerBindingAsync(artifact,Ct);Assert.Empty(plan.Blockers);
        await Assert.ThrowsAsync<InvalidDataException>(()=>f.Store.ApplyServerBindingAsync(plan with{ArtifactSha256=new string('0',64)},Ct));
        var wrong=JsonNode.Parse(artifact)!.AsObject();wrong["backend_identity"]="another-backend";await Assert.ThrowsAsync<InvalidDataException>(()=>f.Store.PreviewServerBindingAsync(wrong.ToJsonString(),Ct));
        // Interrupt after the real first page transaction has committed.
        await Assert.ThrowsAsync<IOException>(()=>f.Store.ApplyServerBindingAsync(plan,()=>Task.FromException(new IOException("stage3 committed page interruption")),Ct));
        // Each page reserves room for both file rows and their native-incarnation rows.
        Assert.Equal(120L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE scope_id='"+plan.NextScope+"'"));Assert.Equal("COPYING",await f.Scalar("SELECT state FROM client_binding_migrations"));
        await Assert.ThrowsAsync<InvalidOperationException>(()=>NamespaceEngine(f));
        f.Store=new(f.Database,clock);await f.Store.ApplyServerBindingAsync(plan,Ct);Assert.Equal("QUALIFYING",await f.Scalar("SELECT state FROM client_binding_migrations"));
        Assert.Equal(251L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE scope_id='"+original+"'"));Assert.Equal(251L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE scope_id='"+plan.NextScope+"'"));
        Assert.Equal(native,await f.Scalar("SELECT native_id FROM local_parent_reconcile_incarnations WHERE scope_id='"+plan.NextScope+"'"));
        Assert.Equal(originals,await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"));
        await Assert.ThrowsAsync<InvalidDataException>(()=>f.Store.ReadAdoptedAnchorAsync(binding,plan.Namespace,new string('0',64),Ct));await f.Store.ReadAdoptedAnchorAsync(binding,plan.Namespace,new string('d',64),Ct);
        f.Remote.AdoptFixtureBinding(binding);var remoteId=(string)(await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE scope_id='"+plan.NextScope+"' AND path='file-0'"))!;f.Remote.RenameFolder(remoteId,"foreign-renamed");
        await using var resumed=await WindowsFreshSyncEngine.ResumeOrOpenAsync(new WindowsCycleNativeOptions(LocalJournalReadMode.Named,true),f.Store,f.Source,Path.Combine(f.Directory,"spool"),plan.Configuration,f.Remote,new(MinimumFreeDiskBytes:0),cancellationToken:Ct);
        var result=await resumed.FreshSyncAsync(Ct);await f.Evidence("B12-adoption",new{result,plan.OriginalScope,plan.NextScope,oldPreserved=251,firstCommittedPage=120,state=await f.Scalar("SELECT state FROM client_binding_migrations")});
        Assert.Equal(plan.NextScope,resumed.ScopeId);Assert.Null(result.Error);Assert.True(result.Conflicts>0);Assert.Equal(0,result.CapturedBytes);Assert.Equal("APPLIED",await f.Scalar("SELECT state FROM client_binding_migrations"));
    }
}
public sealed partial class WindowsFreshCycleTests
{
    internal sealed partial class FakeRemote
    {
        internal void AdoptFixtureBinding(Neptune.Core.Sync.Protocol.SyncRootBinding binding)=>Binding=binding;
    }
}
