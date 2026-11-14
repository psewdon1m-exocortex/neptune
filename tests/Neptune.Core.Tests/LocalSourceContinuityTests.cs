using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

// Pinned-reader/WAL resource qualification requires its own bounded CPU/IO
// window. This does not repair or weaken protected executor/barrier tests.
[Collection("LC1 resource qualification")]
public sealed class LocalSourceContinuityTests
{
    private const string Journal="01dc6ff2f7335b65";
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    private static readonly LocalJournalBounds Bounds=new(Journal,100,0,100000000,2,4,false);
    private static byte[] SharedHash(string path)
    { using var file=new FileStream(path,FileMode.Open,FileAccess.Read,FileShare.ReadWrite|FileShare.Delete);return SHA256.HashData(file); }

    public static byte[] Frame(long start,int count=3,string name="file.dat",uint reason=0x80000100)
    {
        var unicode=Encoding.Unicode.GetBytes(name); var length=(60+unicode.Length+7)/8*8; var frame=new byte[8+count*length];
        BinaryPrimitives.WriteInt64LittleEndian(frame,start+count*length);
        for(var i=0;i<count;i++)
        {
            var s=frame.AsSpan(8+i*length,length); BinaryPrimitives.WriteUInt32LittleEndian(s,(uint)length);
            BinaryPrimitives.WriteUInt16LittleEndian(s[4..],2); BinaryPrimitives.WriteUInt64LittleEndian(s[8..],(ulong)(i+1));
            BinaryPrimitives.WriteUInt64LittleEndian(s[16..],17); BinaryPrimitives.WriteInt64LittleEndian(s[24..],start+i*length);
            BinaryPrimitives.WriteUInt32LittleEndian(s[40..],reason); BinaryPrimitives.WriteUInt16LittleEndian(s[56..],(ushort)unicode.Length);
            BinaryPrimitives.WriteUInt16LittleEndian(s[58..],60); unicode.CopyTo(s[60..]);
        }
        return frame;
    }
    [Fact]
    public void PhysicalPageIsSubdividedAtRealRecordBoundaryAndRetainsOnly256()
    {
        var raw=Frame(100,400); var page=UsnRecordParser.Parse(Journal,100,raw);
        Assert.Equal(256,page.Records.Count); var length=BinaryPrimitives.ReadInt32LittleEndian(raw.AsSpan(8));
        Assert.Equal(100+256*length,page.NextReadUsn); Assert.NotEqual(page.Records[^1].Usn+1,page.NextReadUsn);
        var continuation=new byte[8+raw.Length-8-256*length];
        raw.AsSpan(0,8).CopyTo(continuation); raw.AsSpan(8+256*length).CopyTo(continuation.AsSpan(8));
        var last=UsnRecordParser.Parse(Journal,page.NextReadUsn,continuation);
        Assert.Equal(144,last.Records.Count); Assert.Equal(BinaryPrimitives.ReadInt64LittleEndian(raw),last.NextReadUsn);
    }
    [Theory]
    [InlineData("Truncated")][InlineData("Length")][InlineData("Alignment")][InlineData("Version")]
    [InlineData("Minor")][InlineData("UnknownReason")][InlineData("ReasonZero")][InlineData("Source")]
    [InlineData("NameOffset")][InlineData("NameOdd")][InlineData("Unicode")][InlineData("Slash")]
    [InlineData("UsnOrder")][InlineData("Next")][InlineData("TrailingVersion")]
    public void MalformedOrUnknownRecordsNeverYieldAnAdvance(string mutation)
    {
        var raw=Frame(100,mutation=="TrailingVersion"?300:3); var offset=mutation=="TrailingVersion"?8+299*80:8;
        switch(mutation)
        {
            case "Truncated":raw=raw[..^1];break;
            case "Length":BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(8),uint.MaxValue);break;
            case "Alignment":BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(8),79);break;
            case "Version":case "TrailingVersion":BinaryPrimitives.WriteUInt16LittleEndian(raw.AsSpan(offset+4),4);break;
            case "Minor":BinaryPrimitives.WriteUInt16LittleEndian(raw.AsSpan(14),1);break;
            case "UnknownReason":BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(48),0x40000000);break;
            case "ReasonZero":BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(48),0);break;
            case "Source":BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(52),0x80000000);break;
            case "NameOffset":BinaryPrimitives.WriteUInt16LittleEndian(raw.AsSpan(66),58);break;
            case "NameOdd":BinaryPrimitives.WriteUInt16LittleEndian(raw.AsSpan(64),3);break;
            case "Unicode":BinaryPrimitives.WriteUInt16LittleEndian(raw.AsSpan(68),0xd800);break;
            case "Slash":BinaryPrimitives.WriteUInt16LittleEndian(raw.AsSpan(68),'/');break;
            case "UsnOrder":BinaryPrimitives.WriteInt64LittleEndian(raw.AsSpan(8+80+24),100);break;
            case "Next":BinaryPrimitives.WriteInt64LittleEndian(raw,100);break;
        }
        Assert.Throws<LocalSourceGapException>(()=>UsnRecordParser.Parse(Journal,100,raw));
    }
    [Fact]
    public void EmptyReadUsesExactOsNextAndUnsignedJournalIdentity()
    {
        var raw=new byte[8]; BinaryPrimitives.WriteInt64LittleEndian(raw,960);
        var page=UsnRecordParser.Parse("ffffffffffffffff",100,raw); Assert.Empty(page.Records); Assert.Equal(960,page.NextReadUsn);
        Assert.Throws<ArgumentException>(()=>UsnRecordParser.Parse("FFFFFFFFFFFFFFFF",100,raw));
        Assert.Throws<LocalSourceGapException>(()=>UsnRecordParser.Parse(Journal,100,new byte[1024*1024+1]));
    }
    [Fact]
    public void V3Opaque128BitIdsRemainIntactAndCannotBeTruncatedToV2()
    {
        var v2=Frame(100,1);var raw=new byte[104];BinaryPrimitives.WriteInt64LittleEndian(raw,196);var s=raw.AsSpan(8);
        BinaryPrimitives.WriteInt32LittleEndian(s,96);BinaryPrimitives.WriteUInt16LittleEndian(s[4..],3);
        for(var i=0;i<32;i++)s[8+i]=(byte)(i+1);BinaryPrimitives.WriteInt64LittleEndian(s[40..],100);
        v2.AsSpan(48,16).CopyTo(s[56..]);BinaryPrimitives.WriteUInt16LittleEndian(s[72..],16);BinaryPrimitives.WriteUInt16LittleEndian(s[74..],76);
        v2.AsSpan(68,16).CopyTo(s[76..]);var parsed=Assert.Single(UsnRecordParser.Parse(Journal,100,raw).Records);
        Assert.Equal((ushort)3,parsed.MajorVersion);Assert.Equal("0102030405060708090a0b0c0d0e0f10",parsed.FileIdHex);
        Assert.Equal("1112131415161718191a1b1c1d1e1f20",parsed.ParentIdHex);
        BinaryPrimitives.WriteUInt16LittleEndian(s[74..],60);Assert.Throws<LocalSourceGapException>(()=>UsnRecordParser.Parse(Journal,100,raw));
    }
    [Fact]
    public async Task DifferentPhysicalVolumeBecomesGapEvenWithSameJournalId()
    {
        await using var f=await Fixture.CreateAsync();await using var session=await f.OpenAsync();
        var fake=new FakeReader{Volume=new("e8481792-21dd-422b-b116-35d025404382","50532e73","NTFS")};
        var error=await Assert.ThrowsAsync<LocalSourceGapException>(()=>session.CaptureNextAsync(fake,cancellationToken:Ct));
        Assert.Equal("VolumeBinding",error.Code);Assert.Equal("VolumeBinding",(await session.ReadStateAsync(Ct)).GapCode);
    }
    [Fact]
    public async Task AtomicInboxCursorDuplicateRestartAndUnknownCompleteness()
    {
        await using var f=await Fixture.CreateAsync(); await using(var session=await f.OpenAsync())
        {
            var state=await session.ReadStateAsync(Ct); var page=UsnRecordParser.Parse(Journal,100,Frame(100,256));
            var result=await session.CapturePageAsync(state.Cursor,page,Ct); Assert.Equal(258,result.ChangedRows);
            var duplicate=await session.CapturePageAsync(state.Cursor,page,Ct); Assert.True(duplicate.Duplicate);
            var after=await session.ReadStateAsync(Ct); Assert.Equal(256,after.InboxRecords); Assert.Equal(page.NextReadUsn,after.Cursor.NextReadUsn);
            Assert.Equal("Unknown",after.LocalCompleteness); Assert.Equal("Unknown",after.SourceCycle);
            Assert.Equal(256,(await session.ReadInboxAsync(cancellationToken:Ct)).Count);
            Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM sync_state_roots WHERE server_root_id IS NOT NULL"));
            Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM sync_state_baselines"));
        }
        await f.Store.InitializeAsync(Ct); await using var reopened=await f.OpenAsync();
        var resumed=await reopened.ReadStateAsync(Ct); Assert.True(resumed.Cursor.Valid); Assert.Equal(256,resumed.InboxRecords); Assert.Equal(2,resumed.Cursor.OwnerEpoch);
    }
    [Fact]
    public async Task MidPageSqlAbortRollsBackEveryRecordAndCursorThenSamePageSucceeds()
    {
        await using var f=await Fixture.CreateAsync(); await using var session=await f.OpenAsync();
        var state=await session.ReadStateAsync(Ct); var page=UsnRecordParser.Parse(Journal,100,Frame(100));
        // A controlled already-present second key causes a mid-page constraint
        // failure. Do not weaken the production refusal of unknown SQL triggers.
        await using(var c=await f.Connection())await using(var q=c.CreateCommand())
        {
            q.CommandText="INSERT INTO local_source_inbox(root_key,journal_id,usn,payload,byte_count) SELECT root_key,journal_id,180,$payload,144 FROM local_source_roots; UPDATE local_source_roots SET inbox_records=1,inbox_bytes=144;UPDATE local_source_format SET inbox_records=1,inbox_bytes=144";
            q.Parameters.AddWithValue("$payload",Frame(180,1).AsSpan(8).ToArray());await q.ExecuteNonQueryAsync(Ct);
        }
        await Assert.ThrowsAsync<SqliteException>(()=>session.CapturePageAsync(state.Cursor,page,Ct));
        Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_source_inbox")); Assert.Equal(100,(await session.ReadStateAsync(Ct)).Cursor.NextReadUsn);
        await f.Sql("DELETE FROM local_source_inbox;UPDATE local_source_roots SET inbox_records=0,inbox_bytes=0;UPDATE local_source_format SET inbox_records=0,inbox_bytes=0"); await session.CapturePageAsync(state.Cursor,page,Ct);
        Assert.Equal(3,(await session.ReadStateAsync(Ct)).InboxRecords);
    }
    [Fact]
    public async Task CancellationHasNoAdvanceOrRecordLossAndSessionCanContinue()
    {
        await using var f=await Fixture.CreateAsync(); await using var session=await f.OpenAsync(); var state=await session.ReadStateAsync(Ct);
        using var cancel=new CancellationTokenSource(); cancel.Cancel(); var page=UsnRecordParser.Parse(Journal,100,Frame(100));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>session.CapturePageAsync(state.Cursor,page,cancel.Token));
        Assert.Equal(0,(await session.ReadStateAsync(Ct)).InboxRecords); await session.CapturePageAsync(state.Cursor,page,Ct);
        Assert.Equal(3,(await session.ReadStateAsync(Ct)).InboxRecords);
    }
    [Fact]
    public async Task FullLifetimeOsOwnerAndEpochFenceAcrossStoreInstances()
    {
        await using var f=await Fixture.CreateAsync(); var session=await f.OpenAsync(); var original=(await session.ReadStateAsync(Ct)).Cursor;
        await Assert.ThrowsAsync<IOException>(()=>new NeptuneStateStore(f.Database).OpenLocalJournalSessionAsync(f.Binding,Bounds,100,cancellationToken:Ct));
        await session.DisposeAsync(); await using var second=await f.OpenAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(()=>second.CapturePageAsync(original,UsnRecordParser.Parse(Journal,100,Frame(100)),Ct));
        await Assert.ThrowsAsync<ObjectDisposedException>(()=>session.ReadStateAsync(Ct));
    }
    [Theory]
    [InlineData("Version")][InlineData("Reason")][InlineData("Tracking")][InlineData("Journal")][InlineData("Retention")]
    public async Task ReadErrorsProduceDurableGapWithoutSkipping(string cause)
    {
        await using var f=await Fixture.CreateAsync(); await using var session=await f.OpenAsync(); var fake=new FakeReader();
        if(cause=="Tracking") fake.Bounds=Bounds with { RangeTrackingEnabled=true };
        if(cause=="Journal") fake.Bounds=Bounds with { JournalIdHex="ffffffffffffffff" };
        if(cause=="Retention") fake.Bounds=Bounds with { FirstUsn=101 };
        if(cause is "Version" or "Reason") fake.Failure=new LocalSourceGapException("Unsupported"+cause,"Synthetic unknown frame.");
        await Assert.ThrowsAsync<LocalSourceGapException>(()=>session.CaptureNextAsync(fake,cancellationToken:Ct));
        var after=await session.ReadStateAsync(Ct); Assert.False(after.Cursor.Valid); Assert.Equal(100,after.Cursor.NextReadUsn); Assert.Equal(1,after.GapEpoch);
        await session.MarkGapAsync("Repeated",Ct); Assert.Equal(1,(await session.ReadStateAsync(Ct)).GapEpoch);
    }
    [Theory]
    [InlineData("file")][InlineData("checkpoint")][InlineData("mapping")][InlineData("pause")][InlineData("delete")]
    public async Task LegacyWritesInvalidateWithoutAnyQualifiedServerRoot(string operation)
    {
        await using var f=await Fixture.CreateAsync(); await using var session=await f.OpenAsync();
        switch(operation)
        {
            case "file":await f.Store.UpsertSyncFileAsync(new("mapping","legacy.txt",5,DateTimeOffset.UtcNow,new string('a',64),null,"synced",DateTimeOffset.UtcNow,null),Ct);break;
            case "checkpoint":await f.Store.SaveSyncUploadCheckpointAsync(new("legacy","https://saturn.test/dav/file","key","fingerprint","upload"),Ct);break;
            case "mapping":await f.Sql("UPDATE sync_mappings SET local_path=local_path||'other' WHERE mapping_id='mapping'");break;
            case "pause":await f.Sql("UPDATE sync_mappings SET enabled=0 WHERE mapping_id='mapping'");break;
            case "delete":await f.Sql("DELETE FROM sync_mappings WHERE mapping_id='mapping'");break;
        }
        var state=await session.ReadStateAsync(Ct); Assert.False(state.Cursor.Valid); Assert.Equal("LegacyWrite",state.GapCode);
        Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM sync_state_roots WHERE server_root_id IS NOT NULL")); Assert.Equal(100,state.Cursor.NextReadUsn);
    }
    [Fact]
    public async Task ExplicitNewSubscriptionCannotAccidentallyRestorePreviousGapOrAliasRoot()
    {
        await using var f=await Fixture.CreateAsync(); await using(var s=await f.OpenAsync()) await s.MarkGapAsync("ExplicitRecovery",Ct);
        await using(var reopened=await f.OpenAsync()) Assert.False((await reopened.ReadStateAsync(Ct)).Cursor.Valid);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>f.Store.OpenLocalJournalSessionAsync(f.Binding with {RootFileId=new string('2',32)},Bounds,100,cancellationToken:Ct));
        await using var changed=await f.Store.OpenLocalJournalSessionAsync(f.Binding with {RootFileId=new string('2',32),RootIncarnation=2,SubscriptionGeneration=2},Bounds,Bounds.NextUsn,cancellationToken:Ct);
        var state=await changed.ReadStateAsync(Ct); Assert.True(state.Cursor.Valid); Assert.Equal(Bounds.NextUsn,state.Cursor.NextReadUsn);
        Assert.Equal("Unknown",state.LocalCompleteness); Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_source_roots"));
    }
    [Fact]
    public async Task InboxBackpressurePreservesHistoryAndExplicitGapRetirementIsBounded()
    {
        await using var f=await Fixture.CreateAsync(); await using var session=await f.OpenAsync(new(MaximumInboxBytes:40000));
        var state=await session.ReadStateAsync(Ct); var page=UsnRecordParser.Parse(Journal,100,Frame(100,256)); await session.CapturePageAsync(state.Cursor,page,Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>session.RetireGappedInboxBatchAsync(page.NextReadUsn,Ct));
        var expected=(await session.ReadStateAsync(Ct)).Cursor;
        await Assert.ThrowsAsync<LocalSourceGapException>(()=>session.CapturePageAsync(expected,UsnRecordParser.Parse(Journal,expected.NextReadUsn,Frame(expected.NextReadUsn,256)),Ct));
        var refused=await session.ReadStateAsync(Ct);Assert.Equal(256,refused.InboxRecords);Assert.True(refused.Cursor.Valid);Assert.Equal(expected,refused.Cursor);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>session.RetireGappedInboxBatchAsync(page.NextReadUsn,Ct));
        await session.MarkGapAsync("ExplicitTestGap",Ct);var gap=await session.ReadStateAsync(Ct);Assert.False(gap.Cursor.Valid);
        var retired=await session.RetireGappedInboxBatchAsync(gap.Cursor.NextReadUsn,Ct); Assert.Equal(256,retired.Records); Assert.Equal(258,retired.ChangedRows);
        Assert.Equal(0,(await session.ReadStateAsync(Ct)).InboxRecords); Assert.Equal(0L,await f.Scalar("SELECT inbox_bytes FROM local_source_format"));
    }
    [Theory]
    [InlineData(false)][InlineData(true)]
    public async Task InboxAdmissionRefusalPreservesContinuityAndReplaysExactPageAfterReopen(bool reader)
    {
        await using var f=await Fixture.CreateAsync();await using var session=await f.OpenAsync(new(MaximumInboxBytes:37000));
        var first=UsnRecordParser.Parse(Journal,100,Frame(100,256));await session.CapturePageAsync((await session.ReadStateAsync(Ct)).Cursor,first,Ct);
        var before=await session.ReadStateAsync(Ct);var next=UsnRecordParser.Parse(Journal,before.Cursor.NextReadUsn,Frame(before.Cursor.NextReadUsn));
        var failure=await Assert.ThrowsAsync<LocalSourceGapException>(()=>reader?session.CaptureNextAsync(new FakeReader(),cancellationToken:Ct):session.CapturePageAsync(before.Cursor,next,Ct));
        Assert.Equal("InboxCapacity",failure.Code);var after=await session.ReadStateAsync(Ct);
        Assert.True(after.Cursor.Valid);Assert.Equal(before.Cursor,after.Cursor);Assert.Equal(before.GapEpoch,after.GapEpoch);Assert.Null(after.GapCode);Assert.Equal(before.InboxBytes,after.InboxBytes);
        await session.DisposeAsync();await using var reopened=await f.OpenAsync();var retained=await reopened.ReadStateAsync(Ct);
        Assert.True(retained.Cursor.Valid);Assert.Equal(before.Binding,retained.Binding);Assert.Equal(before.Cursor.NextReadUsn,retained.Cursor.NextReadUsn);
        var written=await reopened.CapturePageAsync(retained.Cursor,next,Ct);Assert.Equal(next.NextReadUsn,written.NextReadUsn);Assert.Equal(259,(await reopened.ReadStateAsync(Ct)).InboxRecords);
    }
    [Fact]
    public async Task SupersededRetirementIsBoundedFencedAndCannotDeleteActiveOrAnotherRoot()
    {
        await using var f=await Fixture.CreateAsync();await using(var old=await f.OpenAsync())
        {
            var cursor=(await old.ReadStateAsync(Ct)).Cursor;
            for(var i=0;i<3;i++){var page=UsnRecordParser.Parse(Journal,cursor.NextReadUsn,Frame(cursor.NextReadUsn,256));var written=await old.CapturePageAsync(cursor,page,Ct);cursor=cursor with {NextReadUsn=written.NextReadUsn};}
        }
        var binding=f.Binding with {SubscriptionGeneration=2};await using var current=await f.Store.OpenLocalJournalSessionAsync(binding,Bounds,Bounds.NextUsn,cancellationToken:Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>current.RetireSupersededInboxBatchAsync(binding,Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(()=>current.RetireSupersededInboxBatchAsync(f.Binding with {MappingId="other"},Ct));
        var first=await current.RetireSupersededInboxBatchAsync(f.Binding,Ct);Assert.Equal(256,first.Records);Assert.Equal(258,first.ChangedRows);Assert.False(first.SubscriptionRemoved);
        await current.RetireSupersededInboxBatchAsync(f.Binding,Ct);
        var last=await current.RetireSupersededInboxBatchAsync(f.Binding,Ct);Assert.Equal(256,last.Records);Assert.Equal(259,last.ChangedRows);Assert.True(last.SubscriptionRemoved);
        Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_source_roots"));Assert.Equal(0L,await f.Scalar("SELECT inbox_bytes FROM local_source_format"));
        Assert.True((await current.ReadStateAsync(Ct)).Cursor.Valid);Assert.Equal("Unknown",(await current.ReadStateAsync(Ct)).LocalCompleteness);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>current.RetireSupersededInboxBatchAsync(f.Binding,Ct));
    }
    [Fact]
    public async Task PhysicalRollbackBackupsConsumeAdmissionQuota()
    {
        await using var f=await Fixture.CreateAsync();await using(var existing=await f.OpenAsync()){}
        await File.WriteAllBytesAsync(f.Database+".pre-audit-quota.db",new byte[2*1024*1024],Ct);
        var error=await Assert.ThrowsAsync<LocalSourceGapException>(()=>f.OpenAsync(new(MaximumDatabaseBytes:2*1024*1024)));
        Assert.Equal("DiskAdmission",error.Code);
    }
    [Theory]
    [InlineData(512)][InlineData(4096)][InlineData(65536)]
    public async Task LegalPageSizesHavePositiveAdmissionAccounting(int pageSize)
    {
        await using var f=await Fixture.CreateAsync(pageSize);await using var session=await f.OpenAsync();
        Assert.Equal((long)pageSize,await f.Scalar("PRAGMA page_size"));var cursor=(await session.ReadStateAsync(Ct)).Cursor;
        await session.CapturePageAsync(cursor,UsnRecordParser.Parse(Journal,100,Frame(100)),Ct);
        var observed=await f.Store.InspectLocalJournalWalAsync(force:true,cancellationToken:Ct);
        Assert.True(observed.UncheckpointedWalBytes>=0);Assert.Equal(3,(await session.ReadStateAsync(Ct)).InboxRecords);
    }
    [Fact]
    public async Task DiskFloorFailsClosedButDuplicateDoesNotNeedNewAdmission()
    {
        await using var f=await Fixture.CreateAsync(); await using(var s=await f.OpenAsync())
        { var st=await s.ReadStateAsync(Ct); await s.CapturePageAsync(st.Cursor,UsnRecordParser.Parse(Journal,100,Frame(100)),Ct); }
        await Assert.ThrowsAsync<LocalSourceGapException>(()=>f.OpenAsync(new(MinimumFreeDiskBytes:long.MaxValue)));
        await using var low=await f.OpenAsync(new(MaximumDatabaseBytes:1024*1024));
        var cursor=(await low.ReadStateAsync(Ct)).Cursor;
        var duplicate=await low.CapturePageAsync(cursor with { NextReadUsn=100 },UsnRecordParser.Parse(Journal,100,Frame(100)),Ct); Assert.True(duplicate.Duplicate);
        await f.Sql("CREATE TABLE audit_padding(payload BLOB); INSERT INTO audit_padding VALUES(zeroblob(2097152))");
        await Assert.ThrowsAsync<LocalSourceGapException>(()=>low.CapturePageAsync(cursor,UsnRecordParser.Parse(Journal,cursor.NextReadUsn,Frame(cursor.NextReadUsn)),Ct));
        Assert.Equal("DiskAdmission",(await low.ReadStateAsync(Ct)).GapCode);
    }
    [Fact]
    public async Task ConcurrentDuplicateCallbacksSerializeAndDoNotMultiplyInboxRows()
    {
        await using var f=await Fixture.CreateAsync();await using var session=await f.OpenAsync();var cursor=(await session.ReadStateAsync(Ct)).Cursor;
        var page=UsnRecordParser.Parse(Journal,100,Frame(100));var results=await Task.WhenAll(Enumerable.Range(0,32).Select(_=>session.CapturePageAsync(cursor,page,Ct)));
        Assert.Single(results,r=>!r.Duplicate);Assert.Equal(31,results.Count(r=>r.Duplicate));Assert.Equal(3,(await session.ReadStateAsync(Ct)).InboxRecords);
    }
    [Fact]
    public async Task OccupiedQuotaCannotBeSilentlyLoweredAfterRestart()
    {
        await using var f=await Fixture.CreateAsync();await using(var s=await f.OpenAsync())
        {var st=await s.ReadStateAsync(Ct);await s.CapturePageAsync(st.Cursor,UsnRecordParser.Parse(Journal,100,Frame(100,256)),Ct);}
        var error=await Assert.ThrowsAsync<LocalSourceGapException>(()=>f.OpenAsync(new(MaximumInboxBytes:1024)));
        Assert.Equal("InboxCapacity",error.Code);await using var reopened=await f.OpenAsync();Assert.Equal(256,(await reopened.ReadStateAsync(Ct)).InboxRecords);
    }
    [Theory][InlineData(true)][InlineData(false)]
    public async Task LongReaderCausesRarePassiveBackpressureAndReleaseDoesNotPermanentlyPoisonAdmission(bool explicitObservation)
    {
        await using var f=await Fixture.CreateAsync();await using var session=await f.OpenAsync();await using var held=await f.Connection();
        await using(var seed=held.CreateCommand()){seed.CommandText="PRAGMA wal_autocheckpoint=0;CREATE TABLE audit_snapshot_seed(value BLOB);INSERT INTO audit_snapshot_seed VALUES(zeroblob(1048576))";await seed.ExecuteNonQueryAsync(Ct);}
        using var snapshot=held.BeginTransaction(deferred:true);await using(var query=held.CreateCommand())
        {query.Transaction=snapshot;query.CommandText="SELECT COUNT(*) FROM audit_snapshot_seed";await query.ExecuteScalarAsync(Ct);}
        LocalSourceGapException? gap=null;var cursor=(await session.ReadStateAsync(Ct)).Cursor;var admissionClock=System.Diagnostics.Stopwatch.StartNew();
        for(var i=0;i<2000;i++)
        {
            try {var page=UsnRecordParser.Parse(Journal,cursor.NextReadUsn,Frame(cursor.NextReadUsn,256));var result=await session.CapturePageAsync(cursor,page,Ct);cursor=cursor with {NextReadUsn=result.NextReadUsn};}
            catch(LocalSourceGapException error){gap=error;break;}
        }
        Assert.True(gap is not null,"Pinned-reader metrics: "+System.Text.Json.JsonSerializer.Serialize(f.Store.GetLocalJournalMetrics())+"; WAL bytes="+(File.Exists(f.Database+"-wal")?new FileInfo(f.Database+"-wal").Length:0));Assert.Equal("WalAdmission",gap!.Code);var stopped=await session.ReadStateAsync(Ct);
        Assert.True(stopped.Cursor.Valid);Assert.Null(stopped.GapCode);Assert.Equal(cursor.NextReadUsn,stopped.Cursor.NextReadUsn);
        // Bounded pressure waits extend elapsed time; preserve the one-second
        // throttle instead of the old total for an immediate-refusal loop.
        var metrics=f.Store.GetLocalJournalMetrics();Assert.InRange(metrics.CheckpointAttempts,1,2+(long)Math.Ceiling(admissionClock.Elapsed.TotalSeconds));
        Assert.InRange(metrics.UncheckpointedWalBytes,50*1024*1024,64*1024*1024);Assert.Equal(258,metrics.MaximumChangedRows);
        Assert.True(metrics.PhysicalWalBytes<=64*1024*1024,"WAL admission must stop before predictable physical overshoot: "+System.Text.Json.JsonSerializer.Serialize(metrics));
        await snapshot.RollbackAsync(Ct);
        if(explicitObservation)Assert.Equal(0,(await f.Store.InspectLocalJournalWalAsync(force:true,cancellationToken:Ct)).UncheckpointedWalBytes);
        // Retry the same bounded page size that was refused, not a smaller
        // page which could fit under the stale conservative WAL estimate.
        await session.CapturePageAsync(stopped.Cursor,UsnRecordParser.Parse(Journal,stopped.Cursor.NextReadUsn,Frame(stopped.Cursor.NextReadUsn,256)),Ct);
        var current=await session.ReadStateAsync(Ct);Assert.Equal(stopped.InboxRecords+256,current.InboxRecords);Assert.True(current.Cursor.Valid);Assert.Equal(stopped.Binding,current.Binding);
        // Once the journal really loses the retained cursor, the same reader
        // path still records a durable gap. Releasing WAL is not proof of USN
        // continuity and cannot silently skip a later missing interval.
        var expired=new FakeReader{Bounds=Bounds with{FirstUsn=current.Cursor.NextReadUsn+1}};
        Assert.Equal("JournalGap",(await Assert.ThrowsAsync<LocalSourceGapException>(()=>session.CaptureNextAsync(expired,cancellationToken:Ct))).Code);
        Assert.False((await session.ReadStateAsync(Ct)).Cursor.Valid);
    }
    [Fact]
    public async Task ReaderWalBackpressureDoesNotPersistAnUnobservedHistoryGap()
    {
        await using var f=await Fixture.CreateAsync();await using var session=await f.OpenAsync();var before=await session.ReadStateAsync(Ct);
        var reader=new FakeReader{Failure=new LocalSourceGapException("WalAdmission","Transient WAL admission")};
        Assert.Equal("WalAdmission",(await Assert.ThrowsAsync<LocalSourceGapException>(()=>session.CaptureNextAsync(reader,cancellationToken:Ct))).Code);
        Assert.Equal(before,await session.ReadStateAsync(Ct));reader.Failure=null;
        await session.CaptureNextAsync(reader,cancellationToken:Ct);Assert.Equal(3,(await session.ReadStateAsync(Ct)).InboxRecords);
    }
    [Fact]
    public async Task BackupIncludesWalPausedMappingsAndLegacyCheckpointsAndIdempotentInit()
    {
        await using var f=await Fixture.CreateAsync();
        await f.Store.AddMappingAsync(new("paused","client",Path.Combine(f.Source,"paused"),false,DateTimeOffset.UtcNow),Ct);
        await f.Store.SaveSyncUploadCheckpointAsync(new("resume","https://saturn.test/dav/checkpoint","stable","fingerprint","original-id"),Ct);
        await using var held=await f.Connection(); await using(var session=await f.OpenAsync()) { }
        await f.Store.InitializeAsync(Ct); await f.Store.InitializeAsync(Ct);
        var rollback=await f.Store.CreateLocalSourceRollbackCopyAsync(Path.Combine(f.Directory,"rollback.db"),Ct);
        await using var backup=new SqliteConnection("Data Source="+rollback+";Mode=ReadOnly;Pooling=False"); await backup.OpenAsync(Ct);
        await using var query=backup.CreateCommand(); query.CommandText="SELECT enabled FROM sync_mappings WHERE mapping_id='paused'"; Assert.Equal(0L,await query.ExecuteScalarAsync(Ct));
        query.CommandText="SELECT upload_id FROM sync_upload_checkpoints WHERE scope='resume'"; Assert.Equal("original-id",await query.ExecuteScalarAsync(Ct));
        query.CommandText="SELECT COUNT(*) FROM sqlite_master WHERE name='local_source_format'"; Assert.Equal(0L,await query.ExecuteScalarAsync(Ct));
        Assert.Single(System.IO.Directory.GetFiles(f.Directory,"*.local-source-v2-backup.json"));
    }
    [Theory]
    [InlineData("Future")][InlineData("Trigger")][InlineData("Table")][InlineData("Orphan")]
    public async Task UnknownOrBrokenOptionalSchemaRefusesBeforeWritesAndPreservesDbWal(string mode)
    {
        await using var f=await Fixture.CreateAsync(); await using(var s=await f.OpenAsync()) { }
        await using var held=await f.Connection();
        await f.Sql(mode switch {
            "Future"=>"UPDATE local_source_format SET version=99",
            "Trigger"=>"DROP TRIGGER local_source_legacy_sync_files_insert",
            "Table"=>"ALTER TABLE local_source_inbox RENAME TO incompatible_inbox",
            _=>"DROP TABLE local_source_format" });
        var before=f.Hashes(); await Assert.ThrowsAsync<InvalidDataException>(()=>new NeptuneStateStore(f.Database).InitializeAsync(Ct));
        Assert.Equal(before,f.Hashes());
    }
    [Fact]
    public async Task NewerOptionalOnlyDatabaseIsUnchangedAndNoLegacyTablesAreCreated()
    {
        await using var f=await Fixture.CreateAsync(); var other=Path.Combine(f.Directory,"future.db");
        await using var c=new SqliteConnection("Data Source="+other+";Pooling=False"); await c.OpenAsync(Ct);
        await using var q=c.CreateCommand(); q.CommandText="PRAGMA journal_mode=WAL; CREATE TABLE local_source_format(singleton INTEGER PRIMARY KEY,version INTEGER); INSERT INTO local_source_format VALUES(1,99)"; await q.ExecuteNonQueryAsync(Ct);
        var db=SharedHash(other); var wal=SharedHash(other+"-wal");
        await Assert.ThrowsAsync<InvalidDataException>(()=>new NeptuneStateStore(other).InitializeAsync(Ct));
        Assert.Equal(db,SharedHash(other)); Assert.Equal(wal,SharedHash(other+"-wal"));
        q.CommandText="SELECT COUNT(*) FROM sqlite_master WHERE name='sync_mappings'"; Assert.Equal(0L,await q.ExecuteScalarAsync(Ct));
    }
    [Fact]
    public async Task PausedMappingCannotStartJournalOwnerAndReaderIsOptInReadOnly()
    {
        await using var f=await Fixture.CreateAsync(); await f.Sql("UPDATE sync_mappings SET enabled=0");
        var error=await Assert.ThrowsAsync<LocalSourceGapException>(()=>f.OpenAsync()); Assert.Equal("MappingChanged",error.Code);
        if(OperatingSystem.IsWindows()) Assert.Throws<ArgumentException>(()=>new WindowsUsnJournalReader("C:"));
        else Assert.Throws<PlatformNotSupportedException>(()=>new WindowsUsnJournalReader("C:"));
        await Assert.ThrowsAsync<ArgumentException>(()=>f.Store.OpenLocalJournalSessionAsync(f.Binding,Bounds,101,cancellationToken:Ct));
    }
    private sealed class FakeReader:ILocalJournalReader
    {
        public LocalJournalVolumeIdentity Volume {get;set;}=new("d8481792-21dd-422b-b116-35d025404382","50532e73","NTFS");
        public LocalJournalBounds Bounds=LocalSourceContinuityTests.Bounds; public LocalSourceGapException? Failure;
        public ValueTask<LocalJournalBounds> QueryAsync(CancellationToken cancellationToken=default)=>ValueTask.FromResult(Bounds);
        public ValueTask<LocalJournalPage> ReadAsync(LocalJournalCursor cursor,int maximumBufferBytes=256*1024,CancellationToken cancellationToken=default)
        { if(Failure is not null)throw Failure;return ValueTask.FromResult(UsnRecordParser.Parse(Journal,cursor.NextReadUsn,Frame(cursor.NextReadUsn))); }
        public ValueTask DisposeAsync()=>ValueTask.CompletedTask;
    }
    private sealed class Fixture:IAsyncDisposable
    {
        public string Directory=Path.Combine(Path.GetTempPath(),"neptune-lc1-tests-"+Guid.NewGuid().ToString("N"));
        public string Database=>Path.Combine(Directory,"state.db"); public string Source=>Path.Combine(Directory,"source"); public NeptuneStateStore Store=null!;
        public LocalSourceBinding Binding=>new("audit","client","mapping",Source,"d8481792-21dd-422b-b116-35d025404382","50532e73","NTFS",new string('1',32),1,1,"rules-v1","nfc-casefold-v1");
        public static async Task<Fixture> CreateAsync(int pageSize=4096)
        { var f=new Fixture();System.IO.Directory.CreateDirectory(f.Source);f.Store=new(f.Database);if(pageSize!=4096)await f.Sql("PRAGMA page_size="+pageSize+";VACUUM");await f.Store.InitializeAsync(Ct);await f.Store.AddMappingAsync(new("mapping","client",f.Source,true,DateTimeOffset.UtcNow),Ct);return f; }
        public Task<LocalSourceJournalSession> OpenAsync(LocalJournalAdmission? admission=null)=>Store.OpenLocalJournalSessionAsync(Binding,Bounds,100,admission,Ct);
        public async Task<SqliteConnection> Connection()
        { var c=new SqliteConnection(new SqliteConnectionStringBuilder{DataSource=Database,Pooling=false,Cache=SqliteCacheMode.Private}.ToString());await c.OpenAsync(Ct);return c; }
        public async Task Sql(string sql) {await using var c=await Connection();await using var q=c.CreateCommand();q.CommandText=sql;await q.ExecuteNonQueryAsync(Ct);}
        public async Task<object?> Scalar(string sql) {await using var c=await Connection();await using var q=c.CreateCommand();q.CommandText=sql;return await q.ExecuteScalarAsync(Ct);}
        public string Hashes()=>string.Join(";",new[]{Database,Database+"-wal"}.Select(p=>File.Exists(p)?Convert.ToHexString(SharedHash(p)):"missing"));
        public ValueTask DisposeAsync()
        {
            SqliteConnection.ClearAllPools();
            if(!Path.GetFullPath(Directory).StartsWith(Path.Combine(Path.GetTempPath(),"neptune-lc1-tests-"),StringComparison.OrdinalIgnoreCase))throw new InvalidOperationException("Owned test path escaped.");
            if(System.IO.Directory.Exists(Directory))System.IO.Directory.Delete(Directory,true);return ValueTask.CompletedTask;
        }
    }
}

[CollectionDefinition("LC1 resource qualification",DisableParallelization=true)]
public sealed class LocalSourceResourceQualificationCollection;
