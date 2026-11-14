using System.Security.AccessControl;
using System.Security.Principal;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class Stage3ChatBTests
{
    [Fact]
    public async Task B04_NativeRenameWithDurableGapCannotBorrowPartialProof()
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);f.Remote.ExtensionsEnabled=true;File.WriteAllText(Path.Combine(f.Source,"old"),"original");await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,1);var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files");File.Move(Path.Combine(f.Source,"old"),Path.Combine(f.Source,"new"));await Session(engine).MarkGapAsync("Stage3RenameHalfGap",Ct);
        var result=await engine.FreshSyncAsync(Ct);await f.Evidence("B04-rename-gap",new{result,injectedDurableGap=true});Assert.NotNull(result.Error);Assert.Empty(f.Remote.Mutations);Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files"));
    }
    [Fact]
    public async Task B02_LaterGapDuringActualFullProofSurvivesOldGenerationAck()
    {
        RequireWindows();await using var f=await WindowsFreshCycleTests.Fixture.Create();await using var engine=await NamespaceEngine(f);Assert.Null((await engine.InitialSyncAsync(Ct)).Error);var s=Session(engine);await s.SaveEventHintsAsync([],true,Ct);var gap=(await s.ReadEventHintsAsync(Ct)).GapGeneration;Assert.True(await s.RequestFullParentProofAsync(engine.ScopeId,Ct));
        var coordinator=(WindowsLocalCoherenceCoordinator)typeof(WindowsFreshSyncEngine).GetField("_coordinator",System.Reflection.BindingFlags.Instance|System.Reflection.BindingFlags.NonPublic)!.GetValue(engine)!;var scope=await s.ReadCycleScopeAsync(engine.ScopeId,Ct);var injected=false;
        var proof=await coordinator.ReconcileParentsAsync(s,engine.ScopeId,scope.CoherenceId!,Ct,async(_,ct)=>{if(!injected){injected=true;await s.SaveEventHintsAsync([],true,ct);}});Assert.True(injected);Assert.Equal(LocalCoherenceStatus.Anchored,proof.Status);await s.AckEventGapAsync(gap,Ct);Assert.Equal(gap+1,(await s.ReadEventHintsAsync(Ct)).GapGeneration);
        var recovered=await engine.FreshSyncAsync(Ct);Assert.Null(recovered.Error);Assert.False((await s.ReadEventHintsAsync(Ct)).Gap);await f.Evidence("B02-full-gap",new{gap,proof,recovered});
    }
    [Theory][InlineData(410,"cursor_expired")][InlineData(409,"journal_gap")]
    public async Task B07_ExpiredRemoteCursorPreservesBaselineAndRequalifies(int status,string code){RequireWindows();await new WindowsFreshCycleTests().ActualQualifiedCursorGapInstallsSeparateRWithoutCaptureOrBaselineReset(status,code);}
    [Fact] public async Task B04_HardlinkAliasRetainsOriginalAndBlocksMutation(){RequireWindows();await new ParentReconciliationTests().ActualMediumHardlinkRemainsUncertainAndPreservesBaseline();}
    [Theory][InlineData("inside")][InlineData("during-cut")][InlineData("root-change-during-cut")]
    public async Task B05_IncompleteNativeMembershipNeverAuthorizesEffects(string kind){RequireWindows();await new ParentReconciliationTests().FullMembershipRefusesInsideOrChangingEvidenceWithoutPublishing(kind);}
    [Fact] public async Task B05_NamespaceMutationDuringScanNeverActivatesPartialProof(){RequireWindows();await new ParentReconciliationTests().NamespaceMutationDuringActualParentPassCannotActivatePartialVersion();}
    [Fact]
    public async Task B05_NativeAclDenialCannotBecomeDeleteApproval()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires native Windows ACL"); return; }
        var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);f.Remote.ExtensionsEnabled=true;var branch=Path.Combine(f.Source,"branch");Directory.CreateDirectory(branch);File.WriteAllText(Path.Combine(branch,"file"),"retained");
        await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,2);File.Delete(Path.Combine(branch,"file"));
        var directory=new DirectoryInfo(branch);var before=directory.GetAccessControl();var denied=new DirectorySecurity();denied.SetSecurityDescriptorBinaryForm(before.GetSecurityDescriptorBinaryForm());denied.AddAccessRule(new FileSystemAccessRule(WindowsIdentity.GetCurrent().User!,FileSystemRights.ListDirectory|FileSystemRights.ReadAttributes,AccessControlType.Deny));
        try
        {
            directory.SetAccessControl(denied);await AwaitHint(f);clock.Advance(TimeSpan.FromSeconds(1));var result=await engine.FreshSyncAsync(Ct);var preview=await engine.PreviewDeletesAsync(Ct);
            await f.Evidence("B05-acl-denied",new{result,preview});Assert.NotNull(result.Error);Assert.Empty(preview.Operations);Assert.Empty(f.Remote.Mutations);Assert.Equal(3,f.Remote.Count);
        }
        finally{var restored=new DirectorySecurity();restored.SetSecurityDescriptorBinaryForm(before.GetSecurityDescriptorBinaryForm());directory.SetAccessControl(restored);}
    }
}
