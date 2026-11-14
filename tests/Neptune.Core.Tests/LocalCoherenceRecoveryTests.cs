using System.Buffers.Binary;
using System.Text;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class LocalCoherenceTests
{
    [Fact]
    public async Task ActualUnknownHardlinkWithinInitialWindowBlocksHistoricalOutsideCertification()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();var file=Path.Combine(f.Outside,"original");System.IO.File.WriteAllText(file,"x");var done=false;
        await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct,(o,ct)=>{if(!done&&o.Stage==LocalCoherenceStage.BeforeCut){Assert.True(Native.CreateHardLink(Path.Combine(f.Outside,"unknown-link"),file,IntPtr.Zero));System.IO.File.WriteAllText(Path.Combine(f.Outside,"after-link"),"x");done=true;}return Task.CompletedTask;});await using var s=await f.Open(c);var anchor=await c.BuildAnchorAsync(s,cancellationToken:Ct);
        Assert.True(done);Assert.Equal(LocalCoherenceStatus.Uncertain,anchor.Status);Assert.Equal(0,anchor.OutsideRecords);Assert.True(anchor.Unresolved>0);Assert.Contains(await s.ReadCoherenceUnresolvedPageAsync(anchor.CoherenceId,cancellationToken:Ct),x=>x.Code=="HardlinkMembershipUnresolved");
    }
    [Fact]
    public async Task ActualInitialOutsideEventsAreRetainedUntilCompleteWindowThenQualifiedHistorically()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();var done=false;
        await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct,(o,ct)=>{if(!done&&o.Stage==LocalCoherenceStage.BeforeCut){for(var i=0;i<20;i++)System.IO.File.WriteAllText(Path.Combine(f.Outside,"before-cut-"+i),"x");done=true;}return Task.CompletedTask;});await using var s=await f.Open(c);var anchor=await c.BuildAnchorAsync(s,cancellationToken:Ct);
        Assert.True(done);Assert.Equal(LocalCoherenceStatus.Anchored,anchor.Status);Assert.True(anchor.OutsideRecords>=20);Assert.Equal(0,anchor.Unresolved);Assert.Equal(0,anchor.DirtyNodes);Assert.Equal("QualifiedAtAppliedBoundary",anchor.NamespaceCoverage);
    }
    [Fact]
    public async Task ActualCaseSensitiveParentCollisionNeverAliasesTwoChildren()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();
        // Microsoft requires an empty directory and create/write/delete-child
        // rights when setting this owned fixture's case-sensitivity flag.
        System.IO.File.Delete(f.File);
        using(var directory=Native.CreateFile(f.Source,0x40000000,3,IntPtr.Zero,3,0x02000000|0x00200000,IntPtr.Zero)){Assert.False(directory.IsInvalid);Assert.True(Native.SetFileInformationByHandle(directory,23,new byte[]{1,0,0,0},4),"Owned case flag error "+System.Runtime.InteropServices.Marshal.GetLastWin32Error());}
        System.IO.File.WriteAllText(f.File,"original");
        System.IO.File.WriteAllText(Path.Combine(f.Source,"Case"),"a");System.IO.File.WriteAllText(Path.Combine(f.Source,"CASE"),"b");
        await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);Assert.True(c.RootProof.Handle.CaseSensitive);await using var s=await f.Open(c);await Assert.ThrowsAsync<InvalidDataException>(()=>c.BuildAnchorAsync(s,cancellationToken:Ct));Assert.Equal(2L,await f.Scalar("SELECT status FROM local_coherence_roots"));
    }
    [Fact]
    public async Task NativeLifetimeRootHandlePreventsRootRenameAndReplacement()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);await Assert.ThrowsAsync<IOException>(()=>Task.Run(()=>Directory.Move(f.Source,Path.Combine(f.Directory,"swapped")),Ct));
        var anchor=await c.BuildAnchorAsync(s,cancellationToken:Ct);Assert.Equal(LocalCoherenceStatus.Anchored,anchor.Status);Assert.Equal(1,anchor.MatchedEntries);
    }
    [Fact]
    public async Task NativeRepeatCoordinatorCannotUseAnotherRootOnTheSameVolume()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);var anchor=await c.BuildAnchorAsync(s,cancellationToken:Ct);var cursor=(await s.ReadStateAsync(Ct)).Cursor;
        await using var wrong=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Outside,Ct);await Assert.ThrowsAsync<InvalidDataException>(()=>wrong.ObserveChangesAsync(s,anchor.CoherenceId,Ct));Assert.Equal(cursor,(await s.ReadStateAsync(Ct)).Cursor);Assert.Equal(anchor.AppliedUsn,(await s.ReadCoherenceAsync(anchor.CoherenceId,Ct)).AppliedUsn);
    }
    [Fact]
    public async Task ActualKnownInsideFileWrittenThroughOutsideHardlinkStillDirties()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();var alias=Path.Combine(f.Outside,"alias");
        Assert.True(Native.CreateHardLink(alias,f.File,IntPtr.Zero));
        await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);var before=await c.BuildAnchorAsync(s,cancellationToken:Ct);
        System.IO.File.WriteAllText(alias,"outside alias changed inside bytes");var after=await c.ObserveChangesAsync(s,before.CoherenceId,Ct);
        Assert.Equal(LocalCoherenceStatus.Anchored,after.Status);Assert.Equal(1,after.DirtyNodes);Assert.Single(await s.ReadCoherenceDirtyPageAsync(before.CoherenceId,cancellationToken:Ct));Assert.Equal("Unknown",after.SourceCycle);
    }
    [Fact]
    public async Task ActualUnknownHardlinkAfterAnchorIsNeverDiscardedAsOutside()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();var file=Path.Combine(f.Outside,"original");System.IO.File.WriteAllText(file,"x");
        await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);var before=await c.BuildAnchorAsync(s,cancellationToken:Ct);
        Assert.True(Native.CreateHardLink(Path.Combine(f.Outside,"new-link"),file,IntPtr.Zero));var after=await c.ObserveChangesAsync(s,before.CoherenceId,Ct);
        Assert.Equal(LocalCoherenceStatus.Uncertain,after.Status);Assert.Contains(await s.ReadCoherenceUnresolvedPageAsync(before.CoherenceId,cancellationToken:Ct),x=>x.Code=="HardlinkMembershipUnresolved");
    }
    [Fact]
    public async Task ActualRepeatedHardlinkDuringOpenWriterCannotQualifyMovedInUnknownAlias()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();var file=Path.Combine(f.Outside,"original");System.IO.File.WriteAllText(file,"x");
        using var held=new FileStream(file,FileMode.Open,FileAccess.ReadWrite,FileShare.ReadWrite|FileShare.Delete);Assert.True(Native.CreateHardLink(Path.Combine(f.Outside,"prior-link"),file,IntPtr.Zero));
        await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);var before=await c.BuildAnchorAsync(s,cancellationToken:Ct);
        Assert.True(Native.CreateHardLink(Path.Combine(f.Source,"entered-alias"),file,IntPtr.Zero));held.Position=0;held.WriteByte(7);held.Flush(true);var after=await c.ObserveChangesAsync(s,before.CoherenceId,Ct);
        await f.Evidence("open-hardlink-cycle",new{before,after,unresolved=(await s.ReadCoherenceUnresolvedPageAsync(before.CoherenceId,cancellationToken:Ct)).Select(x=>new{x.Usn,x.Code,x.Record.Reason,x.Record.MajorVersion})});
        Assert.Equal(LocalCoherenceStatus.Uncertain,after.Status);Assert.True(after.Unresolved>0);
    }
    [Fact]
    public async Task ActualRepeatedOpenWriterDataDoesNotTurnNamespaceAnchorIntoByteAgreement()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();using var held=new FileStream(f.File,FileMode.Open,FileAccess.Write,FileShare.ReadWrite|FileShare.Delete);held.WriteByte(1);held.Flush(true);
        await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);var before=await c.BuildAnchorAsync(s,cancellationToken:Ct);
        held.Position=0;held.WriteByte(2);held.Flush(true);var after=await c.ObserveChangesAsync(s,before.CoherenceId,Ct);
        Assert.Equal("Unknown",after.SourceCycle);Assert.Equal("Unknown",after.Baseline);Assert.Equal("QualifiedAtAppliedBoundary",after.NamespaceCoverage);
        await f.Evidence("open-data-cycle",new{before,after});
    }
    [Fact]
    public async Task NativeNfcCollisionFailsBeforeCoherenceParentIsQualified()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();System.IO.File.WriteAllText(Path.Combine(f.Source,"café"),"a");System.IO.File.WriteAllText(Path.Combine(f.Source,"cafe\u0301"),"b");
        await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);await Assert.ThrowsAsync<InvalidDataException>(()=>c.BuildAnchorAsync(s,cancellationToken:Ct));Assert.Equal(2L,await f.Scalar("SELECT status FROM local_coherence_roots"));
    }
    [Fact]
    public async Task NativeReparseBranchDoesNotProduceCompleteMembership()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();Directory.CreateSymbolicLink(Path.Combine(f.Source,"link"),f.Outside);
        await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);await Assert.ThrowsAsync<InvalidDataException>(()=>c.BuildAnchorAsync(s,cancellationToken:Ct));Assert.Equal(2L,await f.Scalar("SELECT status FROM local_coherence_roots"));
    }
    [Fact]
    public async Task SyntheticMixedPrefixSqlAbortAndCancelledApplyRollBackAllDerivedState()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);var anchor=await c.BuildAnchorAsync(s,cancellationToken:Ct);
        var cursor=(await s.ReadStateAsync(Ct)).Cursor;var raw=SyntheticV3(cursor.NextReadUsn,3,NativeFileIds(f.File).Id,c.RootProof.Handle.FileId128,1);await s.CapturePageAsync(cursor,UsnRecordParser.Parse(cursor.JournalIdHex,cursor.NextReadUsn,raw),Ct);
        // A future duplicate disposition forces SQL failure after the first
        // dirty update; arbitrary triggers are correctly rejected at preflight.
        await f.Sql("INSERT INTO local_coherence_dispositions VALUES('"+anchor.CoherenceId+"',"+cursor.NextReadUsn+",'owned-conflicting-evidence',0,96)");
        await Assert.ThrowsAsync<SqliteException>(()=>s.ApplyNextCoherenceAsync(anchor.CoherenceId,anchor.AppliedUsn,Ct));var unchanged=await s.ReadCoherenceAsync(anchor.CoherenceId,Ct);Assert.Equal(anchor.AppliedUsn,unchanged.AppliedUsn);Assert.Equal(0,unchanged.DirtyNodes);
        await f.Sql("DELETE FROM local_coherence_dispositions WHERE coherence_id='"+anchor.CoherenceId+"' AND usn="+cursor.NextReadUsn);using var cancel=new CancellationTokenSource();cancel.Cancel();await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>s.ApplyNextCoherenceAsync(anchor.CoherenceId,anchor.AppliedUsn,cancel.Token));
        var applied=await s.ApplyNextCoherenceAsync(anchor.CoherenceId,anchor.AppliedUsn,Ct);Assert.Equal(3,applied.Records);Assert.Equal(1,applied.Dirty);Assert.Equal(3,Assert.Single(await s.ReadCoherenceDirtyPageAsync(anchor.CoherenceId,cancellationToken:Ct)).Generation);
        var replay=await s.ApplyNextCoherenceAsync(anchor.CoherenceId,anchor.AppliedUsn,Ct);Assert.True(replay.Replayed);Assert.Equal(0,replay.ChangedRows);Assert.Equal(applied.AppliedUsn,replay.AppliedUsn);
    }
    [Fact]
    public async Task SyntheticUnappliedPrefixPinsOtherRetirementAndProofRemainsAfterOwnRetirement()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);var anchor=await c.BuildAnchorAsync(s,cancellationToken:Ct);var cursor=(await s.ReadStateAsync(Ct)).Cursor;
        await s.CapturePageAsync(cursor,UsnRecordParser.Parse(cursor.JournalIdHex,cursor.NextReadUsn,SyntheticV3(cursor.NextReadUsn,3,NativeFileIds(f.File).Id,c.RootProof.Handle.FileId128,1)),Ct);
        await s.MarkGapAsync("OwnedGap",Ct);await Assert.ThrowsAsync<InvalidOperationException>(()=>s.RetireGappedInboxBatchAsync(cursor.NextReadUsn+3*96,Ct));
        Assert.Equal(LocalCoherenceStatus.Uncertain,(await s.ReadCoherenceAsync(anchor.CoherenceId,Ct)).Status);Assert.Equal(anchor.AppliedUsn,(await s.ReadCoherenceAsync(anchor.CoherenceId,Ct)).AppliedUsn);
    }
    [Fact]
    public async Task NewOwnerDoesNotReusePriorAnchorAndLegacyRevisionCannotBeInvented()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);string id;
        await using(var s=await f.Open(c)){id=(await c.BuildAnchorAsync(s,cancellationToken:Ct)).CoherenceId;await Assert.ThrowsAsync<IOException>(()=>f.Open(c));}
        await using var next=await f.Open(c);await Assert.ThrowsAsync<InvalidOperationException>(()=>next.ReadCoherenceAsync(id,Ct));Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM sync_state_roots WHERE identity_json IS NOT NULL OR server_root_id IS NOT NULL"));
    }
    [Fact]
    public async Task SyntheticQuotaRefusalPreservesInboxCursorAndDerivedPrefix()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);var anchor=await c.BuildAnchorAsync(s,cancellationToken:Ct);var cursor=(await s.ReadStateAsync(Ct)).Cursor;
        await f.Sql("UPDATE local_coherence_roots SET max_bytes=bytes+128 WHERE coherence_id='"+anchor.CoherenceId+"'");
        await s.CapturePageAsync(cursor,UsnRecordParser.Parse(cursor.JournalIdHex,cursor.NextReadUsn,SyntheticV3(cursor.NextReadUsn,20,"ffffffffffffffffffffffffffffffff","eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",1)),Ct);
        await Assert.ThrowsAsync<LocalSourceGapException>(()=>s.ApplyNextCoherenceAsync(anchor.CoherenceId,anchor.AppliedUsn,Ct));Assert.Equal(anchor.AppliedUsn,(await s.ReadCoherenceAsync(anchor.CoherenceId,Ct)).AppliedUsn);Assert.Equal(cursor.NextReadUsn+20*96,(await s.ReadStateAsync(Ct)).Cursor.NextReadUsn);
    }
    private static byte[] SyntheticV3(long start,int count,string node,string parent,uint reason)
    {
        const int size=96;var bytes=new byte[8+count*size];BinaryPrimitives.WriteInt64LittleEndian(bytes,start+count*size);
        for(var i=0;i<count;i++){var s=bytes.AsSpan(8+i*size,size);BinaryPrimitives.WriteUInt32LittleEndian(s,size);BinaryPrimitives.WriteUInt16LittleEndian(s[4..],3);Convert.FromHexString(node).CopyTo(s[8..]);Convert.FromHexString(parent).CopyTo(s[24..]);BinaryPrimitives.WriteInt64LittleEndian(s[40..],start+i*size);BinaryPrimitives.WriteUInt32LittleEndian(s[56..],reason);BinaryPrimitives.WriteUInt16LittleEndian(s[72..],8);BinaryPrimitives.WriteUInt16LittleEndian(s[74..],76);Encoding.Unicode.GetBytes("test").CopyTo(s[76..]);}return bytes;
    }
}
