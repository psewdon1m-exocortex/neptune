using System.Text.Json;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsFreshCycleTests
{
    [Fact]
    public async Task ActualAuthoritativeNotPublishedBecomesConflictWithoutLosingSealedEvidence()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"bytes");f.Remote.CancelComplete=true;await using var e=await f.Open();var first=await e.InitialSyncAsync(Ct);Assert.Null(first.Error);Assert.Equal(1,first.Conflicts);Assert.Equal(0,first.PublishedFiles);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE phase=7 AND active IS NULL AND capture_proof IS NOT NULL AND result_json IS NOT NULL"));Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE baseline IS NOT NULL"));Assert.Single(Directory.GetFiles(Path.Combine(f.Directory,"spool"),"*.spool"));var repeat=await e.FreshSyncAsync(Ct);Assert.Equal(1,repeat.Conflicts);Assert.Equal(0,repeat.CapturedFiles);Assert.Equal(1,f.Remote.Prepares);
    }
    [Fact]
    public async Task ActualWritableMappedSourceRemainsDeferredUntilMappingCloses()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.Create(new Stage3Clock());
        var path = Path.Combine(f.Source, "file"); File.WriteAllBytes(path, new byte[4096]);
        using var map = System.IO.MemoryMappedFiles.MemoryMappedFile.CreateFromFile(path, FileMode.Open, null, 0, System.IO.MemoryMappedFiles.MemoryMappedFileAccess.ReadWrite);
        using var view = map.CreateViewAccessor(); view.Write(0, (byte)9);
        await using var engine = await f.Open();
        var first = await engine.InitialSyncAsync(Ct);
        Assert.Equal("PendingOrUncertain", first.Decision); Assert.Equal(1, first.Deferred); Assert.Equal(0, f.Remote.Prepares);
        view.Dispose(); map.Dispose(); await engine.RetryDeferredAsync(Ct);
        // Closing the mapping does not bypass the durable SOURCE_BUSY retry
        // deadline. Advance the fixture clock like other native writer tests.
        var after = await f.FreshWhenDueAsync(engine, Ct);
        await f.Evidence("mapped-closed-due", new { first, after });
        Assert.Null(after.Error); Assert.Equal("NoWorkAtAppliedEventBoundary", after.Decision); Assert.Equal(1, after.PublishedFiles);
    }
    private static WindowsCycleConfiguration AnchoredConfiguration(Fixture f,string? anchor=null)=>new("mapping","client","audit","rules-v1",new Uri("https://test.invalid/"),f.Remote.Device,"audit-token","local-test",f.Remote.Binding,RemoteAnchorId:anchor);
    private static Task<WindowsFreshSyncEngine> OpenAnchored(Fixture f,string? anchor=null,string? spool=null)=>WindowsFreshSyncEngine.OpenAsync(f.Store,f.Source,spool??Path.Combine(f.Directory,"spool"),AnchoredConfiguration(f,anchor),f.Remote,new(MinimumFreeDiskBytes:0),cancellationToken:Ct);
    [Fact]
    public async Task ActualNestedMappingAnchorTargetsEnrolledRootAndRenameRefusesFurtherEffects()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();var parent=f.Remote.AddFolder("parent");var anchor=f.Remote.AddFolder("mapping",parent);File.WriteAllText(Path.Combine(f.Source,"file"),"bytes");
        await using var e=await OpenAnchored(f,anchor);var first=await e.InitialSyncAsync(Ct);await f.Evidence("remote-anchor",new{first,anchor,input=f.Remote.LastInput});Assert.Null(first.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",first.Decision);Assert.Equal(anchor,f.Remote.LastInput!.Intent.ParentId);Assert.Equal(f.Remote.Binding.RootId,f.Remote.LastInput.Intent.RootId);Assert.Equal(new[]{anchor,parent,f.Remote.Binding.RootId},f.Remote.LastInput.Intent.ParentTopology.Select(p=>p.NodeId));
        f.Remote.RenameFolder(anchor,"renamed");var later=await e.FreshSyncAsync(Ct);Assert.NotNull(later.Error);Assert.Equal("PendingOrUncertain",later.Decision);Assert.Equal(1,f.Remote.Prepares);Assert.Equal(0,later.CapturedFiles);
    }
    [Theory]
    [InlineData("missing")][InlineData("file")][InlineData("outside")]
    public async Task ActualUnqualifiedRemoteAnchorNeverStartsSourceOrUpload(string kind)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"bytes");var anchor=kind=="file"?f.Remote.AddAnchorFile():kind=="outside"?f.Remote.AddFolder("outside",Guid.NewGuid().ToString("D")):Guid.NewGuid().ToString("D");await using var e=await OpenAnchored(f,anchor);var result=await e.InitialSyncAsync(Ct);Assert.NotNull(result.Error);Assert.Equal("PendingOrUncertain",result.Decision);Assert.Equal(0,result.CapturedFiles);Assert.Equal(0,f.Remote.Prepares);Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files"));
    }
    [Theory]
    [InlineData("prepare")][InlineData("complete")]
    public async Task ActualNewOwnerRecoversOriginalOperationAndReanchorsWithoutUnknownOverwrite(string lost)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"sealed original");f.Remote.LosePrepare=lost=="prepare";f.Remote.LoseComplete=lost=="complete";string scope,operation;CycleUploadInput original;
        await using(var first=await f.Open()){scope=first.ScopeId;Assert.Equal("PendingOrUncertain",(await first.InitialSyncAsync(Ct)).Decision);operation=(string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE input_json IS NOT NULL"))!;original=f.Remote.LastInput!;}
        await using var restored=await f.Open();var recovered=await restored.RecoverOriginalWorkAsync(scope,operation,Ct);await f.Evidence("new-owner-recovery",new{lost,recovered,original,current=f.Remote.LastInput});Assert.Null(recovered.Error);Assert.Equal("published",recovered.Status);Assert.Equal("NotAdopted",recovered.Baseline);Assert.Equal("Preserved",recovered.Dirty);Assert.Equal(JsonSerializer.Serialize(original),JsonSerializer.Serialize(f.Remote.LastInput));Assert.Equal(1,f.Remote.Prepares);Assert.Equal(1,f.Remote.Completes);Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE baseline IS NOT NULL"));
        Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory,"spool"),"*.spool"));var freshAnchor=await restored.InitialSyncAsync(Ct);Assert.Null(freshAnchor.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",freshAnchor.Decision);Assert.Equal(1,freshAnchor.CapturedFiles);Assert.Equal(0,freshAnchor.PublishedFiles);Assert.Equal(1,f.Remote.Prepares);Assert.Equal(0,(await restored.FreshSyncAsync(Ct)).CapturedFiles);
    }
    [Fact]
    public async Task ActualSpoolAncestorCannotMoveAndAnotherOwnerCannotBorrowSpool()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();var spool=Path.Combine(f.Directory,"spool");await using var e=await f.Open();Assert.Throws<IOException>(()=>Directory.Move(spool,spool+"-moved"));await using var other=await Fixture.Create();await Assert.ThrowsAsync<IOException>(()=>WindowsFreshSyncEngine.OpenAsync(other.Store,other.Source,spool,AnchoredConfiguration(other),other.Remote,new(MinimumFreeDiskBytes:0),cancellationToken:Ct));Assert.Equal("NoWorkAtAppliedEventBoundary",(await e.InitialSyncAsync(Ct)).Decision);
    }
}
