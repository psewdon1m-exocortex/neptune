using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class LocalCoherenceTests
{
    [Fact]
    public async Task AppliedInboxRetirementBoundsOutsideHistoryAndPreservesUnknownRawReplay()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await Fixture.Create();await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);
        var anchor=await c.BuildAnchorAsync(s,cancellationToken:Ct);anchor=await c.ObserveChangesAsync(s,anchor.CoherenceId,Ct);
        var begin=(await s.ReadStateAsync(Ct)).Cursor.NextReadUsn;
        for(var page=0;page<8;page++)
        {
            var cursor=(await s.ReadStateAsync(Ct)).Cursor;
            await s.CapturePageAsync(cursor,UsnRecordParser.Parse(cursor.JournalIdHex,cursor.NextReadUsn,SyntheticV3(cursor.NextReadUsn,256,"ffffffffffffffffffffffffffffffff","eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",1)),Ct);
            while((await s.ReadCoherenceAsync(anchor.CoherenceId,Ct)).AppliedUsn<(await s.ReadStateAsync(Ct)).Cursor.NextReadUsn)
            {var state=await s.ReadCoherenceAsync(anchor.CoherenceId,Ct);await s.ApplyNextCoherenceAsync(anchor.CoherenceId,state.AppliedUsn,Ct);}
            while(true){var retired=await s.RetireCoherenceInboxBatchAsync(anchor.CoherenceId,Ct);Assert.InRange(retired.ChangedRows,0,260);if(retired.Rows==0)break;}
        }
        Assert.InRange((long)(await f.Scalar("SELECT COUNT(*) FROM local_coherence_dispositions WHERE usn>="+begin))!,0,256);
        var unknown=(await s.ReadStateAsync(Ct)).Cursor;
        await s.CapturePageAsync(unknown,UsnRecordParser.Parse(unknown.JournalIdHex,unknown.NextReadUsn,SyntheticV3(unknown.NextReadUsn,256,"ffffffffffffffffffffffffffffffff","eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",0x10000)),Ct);
        var before=(await s.ReadCoherenceAsync(anchor.CoherenceId,Ct)).AppliedUsn;
        var applied=await s.ApplyNextCoherenceAsync(anchor.CoherenceId,before,Ct);
        var proof=await f.Scalar("SELECT group_concat(hex(payload)||':'||sha256,';') FROM (SELECT payload,sha256 FROM local_coherence_unresolved ORDER BY usn)");
        while((await s.RetireCoherenceInboxBatchAsync(anchor.CoherenceId,Ct)).Rows>0){}
        Assert.Equal(proof,await f.Scalar("SELECT group_concat(hex(payload)||':'||sha256,';') FROM (SELECT payload,sha256 FROM local_coherence_unresolved ORDER BY usn)"));
        var replay=await s.ApplyNextCoherenceAsync(anchor.CoherenceId,before,Ct);Assert.True(replay.Replayed);Assert.Equal(applied.AppliedUsn,replay.AppliedUsn);Assert.Equal(applied.Unresolved,replay.Unresolved);
        Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_source_inbox WHERE usn>="+begin+" AND usn<"+applied.AppliedUsn));
        Assert.Equal(LocalCoherenceStatus.Uncertain,(await s.ReadCoherenceAsync(anchor.CoherenceId,Ct)).Status);
    }

    [Fact]
    public async Task ObservedExpiredNativeBoundaryPersistsTypedGapWithoutSkippingCursor()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await Fixture.Create();await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);
        var anchor=await c.BuildAnchorAsync(s,cancellationToken:Ct);var bounds=await c.QueryJournalAsync(Ct);Assert.True(Math.Max(bounds.FirstUsn,bounds.LowestValidUsn)>0);
        // Inject an expired cursor only into this disposable owned fixture.
        await f.Sql("UPDATE local_source_roots SET cursor=0");var before=await s.ReadStateAsync(Ct);
        var failure=await Assert.ThrowsAsync<LocalSourceGapException>(()=>c.ObserveChangesAsync(s,anchor.CoherenceId,Ct));Assert.Equal("JournalGap",failure.Code);
        var after=await s.ReadStateAsync(Ct);Assert.False(after.Cursor.Valid);Assert.Equal(0,after.Cursor.NextReadUsn);Assert.Equal(before.GapEpoch+1,after.GapEpoch);Assert.Equal("JournalGap",await f.Scalar("SELECT gap_code FROM local_source_roots"));Assert.Equal(before.InboxRecords,after.InboxRecords);
    }

    [Theory]
    [InlineData("payload")][InlineData("sha256")]
    public async Task UnknownRawRetirementRefusesCorruptRetainedCopyAtomically(string field)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await Fixture.Create();await using var c=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var s=await f.Open(c);
        var anchor=await c.BuildAnchorAsync(s,cancellationToken:Ct);anchor=await c.ObserveChangesAsync(s,anchor.CoherenceId,Ct);
        var cursor=(await s.ReadStateAsync(Ct)).Cursor;
        await s.CapturePageAsync(cursor,UsnRecordParser.Parse(cursor.JournalIdHex,cursor.NextReadUsn,SyntheticV3(cursor.NextReadUsn,1,"ffffffffffffffffffffffffffffffff","eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",0x10000)),Ct);
        await s.ApplyNextCoherenceAsync(anchor.CoherenceId,anchor.AppliedUsn,Ct);
        await f.Sql("UPDATE local_coherence_unresolved SET "+field+"="+(field=="payload"?"X'00'":"'corrupt'")+" WHERE usn="+cursor.NextReadUsn);
        var source=await s.ReadStateAsync(Ct);
        await Assert.ThrowsAsync<InvalidDataException>(async()=>{while((await s.RetireCoherenceInboxBatchAsync(anchor.CoherenceId,Ct)).Rows>0){}});
        Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_source_inbox WHERE usn="+cursor.NextReadUsn));
        Assert.Equal(source.Cursor,(await s.ReadStateAsync(Ct)).Cursor);
    }
}
