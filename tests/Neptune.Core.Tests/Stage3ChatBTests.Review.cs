using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class Stage3ChatBTests
{
    [Theory][InlineData(false)][InlineData(true)]
    public async Task B04_NativeTypeReplacementCannotBorrowManagedIdentity(bool directoryFirst)
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);f.Remote.ExtensionsEnabled=true;var path=Path.Combine(f.Source,"entry");if(directoryFirst)Directory.CreateDirectory(path);else File.WriteAllText(path,"managed");
        await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,1);var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files");var remote=f.Remote.CurrentFixtureNode("entry");
        if(directoryFirst){Directory.Delete(path);File.WriteAllText(path,"replacement");}else{File.Delete(path);Directory.CreateDirectory(path);}
        await AwaitHint(f);clock.Advance(TimeSpan.FromSeconds(1));var result=await engine.FreshSyncAsync(Ct);await f.Evidence("B04-type-"+directoryFirst,new{result,remote});Assert.True(result.PendingEntries>0||result.Conflicts>0||result.Error is not null);Assert.Empty(f.Remote.Mutations);Assert.Equal(remote,f.Remote.CurrentFixtureNode("entry"));Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE baseline IS NOT NULL"));
    }
    [Fact]
    public async Task B05_OldDeleteReceiptCannotAcknowledgeNewLocalHint()
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);f.Remote.ExtensionsEnabled=true;var path=Path.Combine(f.Source,"entry");File.WriteAllText(path,"managed");
        await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,1);File.Delete(path);await AwaitHint(f);clock.Advance(TimeSpan.FromSeconds(1));Assert.Null((await engine.FreshSyncAsync(Ct)).Error);var plan=await engine.PreviewDeletesAsync(Ct);Assert.Single(plan.Operations);await engine.ApproveDeletesAsync(plan.Digest,Ct);
        f.Remote.BeforeMutation=async _=>{f.Remote.BeforeMutation=null;File.WriteAllText(path,"new generation");await Session(engine).SaveEventHintsAsync(["entry"],false,Ct);};
        var first=await engine.FreshSyncAsync(Ct);var hints=await Session(engine).ReadEventHintsAsync(Ct);Assert.Contains(hints.Hints,h=>h.Path=="entry");Assert.Equal("new generation",File.ReadAllText(path));Assert.Single(f.Remote.Mutations);
        await f.Evidence("B05-late-generation",new{first,hints,oldReceipt=true,newSourcePreserved=true});
    }
    [Fact]
    public async Task B06_PortablePreviewRejectsDecomposedNativeNameWithoutRenaming()
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);var name="e\u0301.txt";File.WriteAllText(Path.Combine(f.Source,name),"original");
        await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,1);var original=await f.Scalar("SELECT input_json FROM local_cycle_work");var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files");
        var proposed=new MappingSyncPolicy("portable_v1",[]);var preview=await engine.PreviewPolicyAsync(proposed,Ct);Assert.Equal(1,preview.Collisions);Assert.Equal(proposed.Hash,preview.ProposedHash);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>engine.ApplyPolicyAsync(proposed,preview.Digest,Ct));Assert.Equal("legacy",(await engine.ReadMappingPolicyAsync(Ct)).Codec);Assert.True(File.Exists(Path.Combine(f.Source,name)));Assert.False(File.Exists(Path.Combine(f.Source,"é.txt")));
        Assert.Equal(original,await f.Scalar("SELECT input_json FROM local_cycle_work"));Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files"));Assert.Equal(name,f.Remote.CurrentFixtureNode(name).Name);await f.Evidence("B06-codec-preview",new{preview,unchangedOriginal=true,unchangedNativeName=true});
    }
    [Fact]
    public async Task B05_ChangedDeletePlanRejectsPreviouslyReviewedDigest()
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);f.Remote.ExtensionsEnabled=true;foreach(var name in new[]{"one","two"})File.WriteAllText(Path.Combine(f.Source,name),"managed");
        await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,2);File.Delete(Path.Combine(f.Source,"one"));await AwaitHint(f);clock.Advance(TimeSpan.FromSeconds(1));Assert.Null((await engine.FreshSyncAsync(Ct)).Error);var reviewed=await engine.PreviewDeletesAsync(Ct);Assert.Single(reviewed.Operations);
        File.Delete(Path.Combine(f.Source,"two"));await AwaitHint(f);clock.Advance(TimeSpan.FromSeconds(1));Assert.Null((await engine.FreshSyncAsync(Ct)).Error);var changed=await engine.PreviewDeletesAsync(Ct);Assert.Equal(2,changed.Operations.Count);Assert.NotEqual(reviewed.Digest,changed.Digest);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>engine.ApproveDeletesAsync(reviewed.Digest,Ct));Assert.Empty(f.Remote.Approvals);Assert.Empty(f.Remote.Mutations);Assert.Equal(3,f.Remote.Count);
        await engine.ApproveDeletesAsync(changed.Digest,Ct);Assert.Null((await engine.FreshSyncAsync(Ct)).Error);Assert.Single(f.Remote.Approvals);Assert.Equal(2,f.Remote.Mutations.Count);Assert.Equal(1,f.Remote.Count);await f.Evidence("B05-changed-plan",new{reviewed,changed,approved=f.Remote.Approvals[0].PlanId});
    }
}
