using System.Buffers.Binary;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("LC1 resource qualification")]
public sealed class LocalProjectionTests
{
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    private const string Journal="01dc6ff2f7335b65",RootId="01000000000000000300000000000000",Serial="aabbccdd50532e73";
    private static string Id(int n)=>n.ToString("x32");
    private static LocalNamespaceHandleProof Handle(string node=RootId,long created=10,uint attrs=0x10,string legacy="1122334455667788")=>new(node,legacy,Serial,0x50532e73,created,attrs,false);
    private static LocalProjectionMetadataProof MockProof(LocalProjectionMetadataClaim claim,string? node=null,long created=10,uint attrs=0x20,long links=1,string legacy="0100000000000000")
        =>(LocalProjectionMetadataProof)Activator.CreateInstance(typeof(LocalProjectionMetadataProof),BindingFlags.Instance|BindingFlags.NonPublic,null,[claim,Handle(node??claim.Record.FileIdHex,created,attrs,legacy),links],null)!;
    private sealed record Event(long Usn,string File,string Parent,string Name,uint Reason=0x80000001,uint Attributes=0x20,ushort Major=3);
    private static byte[] Frame(long end,params Event[] records)
    {
        var arrays=new List<byte[]>();foreach(var e in records)
        {
            var shift=e.Major==3?16:0;var bytes=Encoding.Unicode.GetBytes(e.Name);var raw=new byte[(60+shift+bytes.Length+7)/8*8];
            BinaryPrimitives.WriteUInt32LittleEndian(raw,(uint)raw.Length);BinaryPrimitives.WriteUInt16LittleEndian(raw.AsSpan(4),e.Major);
            Convert.FromHexString(e.File).CopyTo(raw,8);Convert.FromHexString(e.Parent).CopyTo(raw,8+(e.Major==3?16:8));
            BinaryPrimitives.WriteInt64LittleEndian(raw.AsSpan(24+shift),e.Usn);BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(40+shift),e.Reason);BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(52+shift),e.Attributes);
            BinaryPrimitives.WriteUInt16LittleEndian(raw.AsSpan(56+shift),(ushort)bytes.Length);BinaryPrimitives.WriteUInt16LittleEndian(raw.AsSpan(58+shift),(ushort)(60+shift));bytes.CopyTo(raw,60+shift);arrays.Add(raw);
        }
        var result=new byte[8+arrays.Sum(x=>x.Length)];BinaryPrimitives.WriteInt64LittleEndian(result,end);var at=8;foreach(var a in arrays){a.CopyTo(result,at);at+=a.Length;}return result;
    }
    [Fact]
    public async Task PrefixUsesLookaheadBoundaryAndLostResponseReplayHasZeroWrites()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var p=await f.Candidate(s);
        var projection=await s.BeginLocalProjectionAsync(p,new(MinimumFreeDiskBytes:0),Ct);
        var events=Enumerable.Range(0,70).Select(i=>new Event(100+i*100,Id(1),RootId,"file")).ToArray();await f.Ingest(s,10000,events);
        var first=await s.ApplyNextLocalInboxAsync(projection.ProjectionId,100,Ct);Assert.Equal(32,first.Records);Assert.Equal(3300,first.AppliedUsn);Assert.NotEqual(events[31].Usn+1,first.AppliedUsn);Assert.Equal(32,first.Unresolved);Assert.InRange(first.ChangedRows,1,260);
        var duplicate=await s.ApplyNextLocalInboxAsync(projection.ProjectionId,100,Ct);Assert.True(duplicate.Duplicate);Assert.Equal(first.AppliedUsn,duplicate.AppliedUsn);Assert.Equal(0,duplicate.ChangedRows);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>s.ApplyNextLocalInboxAsync(projection.ProjectionId,101,Ct));
        var second=await s.ApplyNextLocalInboxAsync(projection.ProjectionId,3300,Ct);Assert.Equal(6500,second.AppliedUsn);var third=await s.ApplyNextLocalInboxAsync(projection.ProjectionId,6500,Ct);Assert.Equal(10000,third.AppliedUsn);Assert.Equal(6,third.Records);
        Assert.Equal(70,(await s.ReadLocalUnresolvedPageAsync(projection.ProjectionId,cancellationToken:Ct)).Count);Assert.Equal("Unknown",(await s.ReadLocalProjectionAsync(projection.ProjectionId,Ct)).LocalCompleteness);
        var retired=await s.RetireAppliedInboxBatchAsync(projection.ProjectionId,Ct);Assert.Equal(70,retired.Rows);Assert.InRange(retired.ChangedRows,70,74);Assert.Empty(await s.ReadInboxAsync(cancellationToken:Ct));
        Assert.True((await s.ApplyNextLocalInboxAsync(projection.ProjectionId,6500,Ct)).Duplicate);Assert.Equal(70,(await s.ReadLocalProjectionAsync(projection.ProjectionId,Ct)).Unresolved);
    }
    [Fact]
    public async Task EmptyValidatedTailAdvancesOnlyToPersistedIngestBoundary()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);
        await f.Ingest(s,999);var result=await s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct);Assert.Equal(0,result.Records);Assert.Equal(999,result.AppliedUsn);Assert.Equal(2,result.ChangedRows);Assert.True((await s.ApplyNextLocalInboxAsync(p.ProjectionId,999,Ct)).Duplicate);
    }
    [Theory]
    [InlineData(0x80000200u,"HistoricalDeleteUnresolved")][InlineData(0x80001000u,"HistoricalRenameUnresolved")][InlineData(0x80002000u,"HistoricalRenameUnresolved")]
    [InlineData(0x80010000u,"IncarnationOrPolicyUnresolved")][InlineData(0x80000100u,"IncarnationOrPolicyUnresolved")]
    public async Task KnownRootHistoricalEventsRemainRawAndDirectoryDirty(uint reason,string unused)
    {
        _=unused;await using var f=await Fixture.Create();await using var s=await f.Open();var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);
        await f.Ingest(s,1000,new Event(100,RootId,Id(999),"root",reason,0x10));await s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct);
        var dirty=Assert.Single(await s.ReadLocalDirtyPageAsync(p.ProjectionId,cancellationToken:Ct));Assert.True(dirty.Directory);Assert.Equal(1,dirty.SubtreeEpoch);Assert.Equal(1,dirty.Generation);Assert.Single(await s.ReadLocalUnresolvedPageAsync(p.ProjectionId,cancellationToken:Ct));Assert.Null(await s.ReadLocalMetadataClaimAsync(p.ProjectionId,100,Ct));
    }
    [Theory]
    [InlineData(0x80000001u,1,true)][InlineData(0x80000100u,1,false)][InlineData(0x80001000u,1,false)][InlineData(0x80000001u,2,false)]
    public async Task PresentNativeMetadataOnlyQualifiesOrdinarySingleLinkEvents(uint reason,int links,bool qualified)
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);await f.Ingest(s,1000,new Event(100,Id(1),RootId,"file",reason));await s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct);
        var claim=(await s.ReadLocalMetadataClaimAsync(p.ProjectionId,100,Ct))!;Assert.NotNull(claim);var proof=MockProof(claim,links:links);Assert.Equal(qualified,await s.CommitLocalMetadataProofAsync(proof,Ct));
        var before=await s.ReadLocalProjectionAsync(p.ProjectionId,Ct);var dirty=Assert.Single(await s.ReadLocalDirtyPageAsync(p.ProjectionId,cancellationToken:Ct));Assert.Equal(1,dirty.Generation);Assert.Equal(qualified?0:1,before.Unresolved);
        Assert.Equal(qualified,await s.CommitLocalMetadataProofAsync(proof,Ct));Assert.Equal(before,await s.ReadLocalProjectionAsync(p.ProjectionId,Ct));Assert.Equal(dirty,Assert.Single(await s.ReadLocalDirtyPageAsync(p.ProjectionId,cancellationToken:Ct)));
        var raw=Assert.Single(await s.ReadLocalUnresolvedPageAsync(p.ProjectionId,cancellationToken:Ct));Assert.Equal(qualified,raw.PresentQualified);Assert.Null(await s.ReadLocalMetadataClaimAsync(p.ProjectionId,100,Ct));Assert.Equal("Unknown",before.SourceCycle);
    }
    [Fact]
    public async Task BoundAliasMakesLaterDataDirtyAndKnownDeleteLocalTombstoneOnly()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);await f.Ingest(s,1000,new Event(100,Id(1),RootId,"file"));await s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct);await s.CommitLocalMetadataProofAsync(MockProof((await s.ReadLocalMetadataClaimAsync(p.ProjectionId,100,Ct))!),Ct);
        await f.Ingest(s,2000,new Event(1000,Id(1),RootId,"file"),new Event(1100,Id(1),RootId,"file",0x80000200));var applied=await s.ApplyNextLocalInboxAsync(p.ProjectionId,1000,Ct);Assert.Equal(2,applied.Known);Assert.Equal(0,applied.Unresolved);
        var dirty=Assert.Single(await s.ReadLocalDirtyPageAsync(p.ProjectionId,cancellationToken:Ct));Assert.Equal(3,dirty.Generation);Assert.Equal(1100,dirty.LastUsn);Assert.Equal(1L,await f.Scalar("SELECT action FROM local_projection_edges"));Assert.Single(await s.ReadLocalUnresolvedPageAsync(p.ProjectionId,cancellationToken:Ct));
    }
    [Theory]
    [InlineData("WrongId")][InlineData("WrongType")][InlineData("Reparse")][InlineData("WrongSerial")][InlineData("Creation")][InlineData("Later")]
    public async Task MetadataMismatchRetainsCursorAndRawEvidence(string mutation)
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);await f.Ingest(s,1000,new Event(100,Id(1),RootId,"file"));await s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct);var claim=(await s.ReadLocalMetadataClaimAsync(p.ProjectionId,100,Ct))!;
        var h=Handle(Id(1),attrs:0x20,legacy:"0100000000000000");h=mutation switch{"WrongId"=>h with{FileId128=Id(3)},"WrongType"=>h with{Attributes=0x10},"Reparse"=>h with{Attributes=0x420},"WrongSerial"=>h with{VolumeSerial64="0000000000000001"},"Creation"=>h with{CreationTime=11},_=>h};
        if(mutation=="Later")await f.Sql("INSERT INTO local_projection_nodes VALUES('"+p.ProjectionId+"','"+Id(1)+"',10,0,1,999,1,0,256)");
        var proof=(LocalProjectionMetadataProof)Activator.CreateInstance(typeof(LocalProjectionMetadataProof),BindingFlags.Instance|BindingFlags.NonPublic,null,[claim,h,1L],null)!;
        await Assert.ThrowsAnyAsync<Exception>(()=>s.CommitLocalMetadataProofAsync(proof,Ct));Assert.Equal(1000,(await s.ReadLocalProjectionAsync(p.ProjectionId,Ct)).AppliedUsn);Assert.False(Assert.Single(await s.ReadLocalUnresolvedPageAsync(p.ProjectionId,cancellationToken:Ct)).PresentQualified);
    }
    [Theory]
    [InlineData("FILE")][InlineData("e\u0301")]
    public async Task CanonicalSiblingConflictRollsBackEveryDerivedEffect(string name)
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var candidate=await f.Candidate(s,name=="FILE"?"file":"é");var p=await s.BeginLocalProjectionAsync(candidate,new(MinimumFreeDiskBytes:0),Ct);await f.Ingest(s,1000,new Event(100,Id(2),RootId,name));await s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct);var claim=(await s.ReadLocalMetadataClaimAsync(p.ProjectionId,100,Ct))!;var before=await s.ReadLocalProjectionAsync(p.ProjectionId,Ct);
        await Assert.ThrowsAsync<LocalSourceGapException>(()=>s.CommitLocalMetadataProofAsync(MockProof(claim),Ct));Assert.Equal(before,await s.ReadLocalProjectionAsync(p.ProjectionId,Ct));Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_projection_nodes"));Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_projection_aliases"));
    }
    [Fact]
    public async Task RenameHintIsBoundedButEveryRawEventSurvivesAcrossBatches()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);await f.Ingest(s,10000,Enumerable.Range(0,70).Select(i=>new Event(100+i*100,Id(1),RootId,"old-"+i,0x80001000)).ToArray());
        var cursor=100L;while(cursor<10000)cursor=(await s.ApplyNextLocalInboxAsync(p.ProjectionId,cursor,Ct)).AppliedUsn;Assert.Equal(16L,await f.Scalar("SELECT COUNT(*) FROM local_projection_rename"));Assert.Equal(70,(await s.ReadLocalUnresolvedPageAsync(p.ProjectionId,cancellationToken:Ct)).Count);Assert.Equal(70,(await s.ReadLocalProjectionAsync(p.ProjectionId,Ct)).Unresolved);
    }
    [Fact]
    public async Task CancellationAndRawCorruptionDoNotAdvanceAppliedCursor()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);await f.Ingest(s,1000,new Event(100,Id(1),RootId,"file"));using var canceled=new CancellationTokenSource();canceled.Cancel();await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>s.ApplyNextLocalInboxAsync(p.ProjectionId,100,canceled.Token));Assert.Equal(100,(await s.ReadLocalProjectionAsync(p.ProjectionId,Ct)).AppliedUsn);
        await f.Sql("UPDATE local_source_inbox SET payload=x'0102',byte_count=66");await Assert.ThrowsAsync<LocalSourceGapException>(()=>s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct));Assert.Equal(100,(await s.ReadLocalProjectionAsync(p.ProjectionId,Ct)).AppliedUsn);
    }
    [Fact]
    public async Task ConcurrentReplayIsSerializedAndOwnerRestartRequiresFreshCandidate()
    {
        await using var f=await Fixture.Create();var s=await f.Open();var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);await f.Ingest(s,1000,new Event(100,Id(1),RootId,"file"));var results=await Task.WhenAll(Enumerable.Range(0,16).Select(_=>s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct)));Assert.Single(results,x=>!x.Duplicate);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_projection_unresolved"));await Assert.ThrowsAsync<IOException>(()=>f.Open());await s.DisposeAsync();await using var next=await f.Open();await Assert.ThrowsAsync<InvalidOperationException>(()=>next.ApplyNextLocalInboxAsync(p.ProjectionId,1000,Ct));var fresh=await next.BeginLocalProjectionAsync(await f.Candidate(next),new(MinimumFreeDiskBytes:0),Ct);Assert.NotEqual(p.ProjectionId,fresh.ProjectionId);Assert.Equal("Unknown",fresh.LocalCompleteness);
    }
    [Theory]
    [InlineData("Pause")][InlineData("Legacy")][InlineData("Remap")]
    public async Task MappingAndOldClientWritesInvalidateLocalOnlyProjection(string change)
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);
        if(change=="Legacy")await f.Store.UpsertSyncFileAsync(new("mapping","old",1,DateTimeOffset.UtcNow,new string('a',64),null,"synced",DateTimeOffset.UtcNow,null),Ct);else await f.Sql(change=="Pause"?"UPDATE sync_mappings SET enabled=0":"UPDATE sync_mappings SET local_path=local_path||'-different'");
        await Assert.ThrowsAnyAsync<Exception>(()=>s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct));Assert.Equal(100,(await s.ReadLocalProjectionAsync(p.ProjectionId,Ct)).AppliedUsn);
    }
    [Fact]
    public async Task BaseHistoryIsPinnedBeforeDeletionAndProjectionRetiresInBoundedPhases()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var candidate=await f.Candidate(s);var p=await s.BeginLocalProjectionAsync(candidate,new(MinimumFreeDiskBytes:0),Ct);await f.Ingest(s,1000,new Event(100,Id(1),RootId,"file"));await s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct);var successor=await f.Candidate(s);
        var count=await f.Scalar("SELECT COUNT(*) FROM local_namespace_entries WHERE candidate_id='"+candidate+"'");await Assert.ThrowsAsync<InvalidOperationException>(()=>s.RetireNamespaceBatchAsync(candidate,Ct));Assert.Equal(count,await f.Scalar("SELECT COUNT(*) FROM local_namespace_entries WHERE candidate_id='"+candidate+"'"));await s.BeginLocalProjectionAsync(successor,new(MinimumFreeDiskBytes:0),Ct);
        var removed=false;for(var i=0;i<12;i++){var r=await s.RetireLocalProjectionBatchAsync(p.ProjectionId,Ct);Assert.InRange(r.ChangedRows,0,258);if(r.Removed){removed=true;break;}}Assert.True(removed);Assert.True((await s.RetireNamespaceBatchAsync(candidate,Ct)).Rows>0);
    }
    [Theory]
    [InlineData(4096)][InlineData(65536)]
    public async Task PageSizeReducesDerivedWriteCapacityAndPreservesBudget(int size)
    {
        await using var f=await Fixture.Create(size);await using var s=await f.Open();var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);await f.Ingest(s,10000,Enumerable.Range(0,70).Select(i=>new Event(100+i*100,Id(1),RootId,"file")).ToArray());var r=await s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct);Assert.InRange(r.Records,1,32);if(size==65536)Assert.True(r.Records<32);Assert.InRange(r.ChangedRows,1,260);
    }
    [Fact]
    public async Task LogicalQuotaStopsAtomicallyWithoutSkippingInbox()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(1024,1024L*1024*1024,0),Ct);await f.Ingest(s,1000,new Event(100,Id(1),RootId,"file"));var e=await Assert.ThrowsAsync<LocalSourceGapException>(()=>s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct));Assert.Equal("ProjectionCapacity",e.Code);var current=await s.ReadLocalProjectionAsync(p.ProjectionId,Ct);Assert.Equal(100,current.AppliedUsn);Assert.Equal(LocalProjectionStatus.Gapped,current.Status);Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_projection_unresolved"));Assert.Single(await s.ReadInboxAsync(cancellationToken:Ct));
    }
    [Theory]
    [InlineData("Future")][InlineData("Broken")][InlineData("Extra")]
    public async Task OptionalFutureOrBrokenModuleRefusesCommonInitializeBeforeAnyWrite(string kind)
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);await using var held=await f.Connection();await f.Sql(kind switch{"Future"=>"UPDATE local_projection_format SET version=99","Broken"=>"DROP INDEX local_projection_pins",_=>"CREATE INDEX local_projection_alien ON local_projection_roots(status)"});var before=f.Hashes();await Assert.ThrowsAsync<InvalidDataException>(()=>new NeptuneStateStore(f.Database).InitializeAsync(Ct));Assert.Equal(before,f.Hashes());
    }
    [Fact]
    public async Task WalAwareBackupPreservesPausedAndCheckpointDataAndRollbackRejectsTampering()
    {
        await using var f=await Fixture.Create();await f.Store.AddMappingAsync(new("paused","client",f.Source+"-paused",false,DateTimeOffset.UtcNow),Ct);await f.Store.SaveSyncUploadCheckpointAsync(new("resume","https://test/dav/file","key","hash","upload"),Ct);await using var held=await f.Connection();await using var s=await f.Open();await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);
        var rollback=Path.Combine(f.Directory,"rollback.db");await f.Store.CreateLocalProjectionRollbackCopyAsync(rollback,Ct);await using(var c=new SqliteConnection("Data Source="+rollback+";Pooling=False")){await c.OpenAsync(Ct);await using var q=c.CreateCommand();q.CommandText="SELECT COUNT(*) FROM sync_mappings WHERE enabled=0";Assert.Equal(1L,await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT COUNT(*) FROM sync_upload_checkpoints";Assert.Equal(1L,await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT COUNT(*) FROM sqlite_master WHERE name='local_projection_format'";Assert.Equal(0L,await q.ExecuteScalarAsync(Ct));}
        await Assert.ThrowsAsync<InvalidOperationException>(()=>f.Store.CreateLocalProjectionRollbackCopyAsync(rollback,Ct));var manifest=f.Database+".local-projection-v1-backup.json";var json=await File.ReadAllTextAsync(manifest,Ct);await File.WriteAllTextAsync(manifest,json.Replace("\"targetFormat\":1","\"targetFormat\":2"),Ct);await Assert.ThrowsAsync<InvalidDataException>(()=>f.Store.CreateLocalProjectionRollbackCopyAsync(Path.Combine(f.Directory,"bad.db"),Ct));
    }
    [Fact]
    public async Task IdempotentBeginApplyAndProofDoNotRequireNewDiskAdmission()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var candidate=await f.Candidate(s);var p=await s.BeginLocalProjectionAsync(candidate,new(MinimumFreeDiskBytes:0),Ct);await f.Ingest(s,1000,new Event(100,Id(1),RootId,"file"));await s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct);var proof=MockProof((await s.ReadLocalMetadataClaimAsync(p.ProjectionId,100,Ct))!);await s.CommitLocalMetadataProofAsync(proof,Ct);
        await f.Sql("UPDATE local_projection_roots SET free_floor=9223372036854775807");Assert.Equal(p.ProjectionId,(await s.BeginLocalProjectionAsync(candidate,new(MinimumFreeDiskBytes:long.MaxValue),Ct)).ProjectionId);Assert.True((await s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct)).Duplicate);Assert.True(await s.CommitLocalMetadataProofAsync(proof,Ct));Assert.Equal(1,Assert.Single(await s.ReadLocalDirtyPageAsync(p.ProjectionId,cancellationToken:Ct)).Generation);
    }
    [Fact]
    public async Task FutureModuleWithoutLegacyTablesIsUntouchedIncludingPinnedWal()
    {
        await using var f=await Fixture.Create();var future=Path.Combine(f.Directory,"future.db");await using var c=new SqliteConnection("Data Source="+future+";Pooling=False");await c.OpenAsync(Ct);await using var q=c.CreateCommand();q.CommandText="PRAGMA journal_mode=WAL;CREATE TABLE local_projection_format(singleton INTEGER PRIMARY KEY,version INTEGER);INSERT INTO local_projection_format VALUES(1,99)";await q.ExecuteNonQueryAsync(Ct);var original=Fixture.FileHash(future)+Fixture.FileHash(future+"-wal");await Assert.ThrowsAsync<InvalidDataException>(()=>new NeptuneStateStore(future).InitializeAsync(Ct));Assert.Equal(original,Fixture.FileHash(future)+Fixture.FileHash(future+"-wal"));q.CommandText="SELECT COUNT(*) FROM sqlite_master WHERE name='sync_mappings'";Assert.Equal(0L,await q.ExecuteScalarAsync(Ct));
    }
    [Fact]
    public async Task EarlierUnqualifiedInboxCannotBeRetiredByFreshProjection()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();await f.Ingest(s,1000,new Event(100,Id(1),RootId,"file"));var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);Assert.Equal(1000,p.StartUsn);await Assert.ThrowsAsync<LocalSourceGapException>(()=>s.RetireAppliedInboxBatchAsync(p.ProjectionId,Ct));Assert.Single(await s.ReadInboxAsync(cancellationToken:Ct));Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_projection_retired"));
    }
    [Fact]
    public async Task DirectoryGenerationOverflowCannotBecomeSqliteRealOrSkipEvent()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);await f.Ingest(s,1000,new Event(100,RootId,Id(9),"root",Attributes:0x10));await s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct);await f.Sql("UPDATE local_projection_nodes SET subtree_epoch=9223372036854775807");await f.Ingest(s,2000,new Event(1000,RootId,Id(9),"root",Attributes:0x10));await Assert.ThrowsAsync<LocalSourceGapException>(()=>s.ApplyNextLocalInboxAsync(p.ProjectionId,1000,Ct));Assert.Equal(1000,(await s.ReadLocalProjectionAsync(p.ProjectionId,Ct)).AppliedUsn);Assert.Equal("integer",await f.Scalar("SELECT typeof(subtree_epoch) FROM local_projection_nodes"));
    }
    [Fact]
    public async Task AlreadyQualifiedMetadataCannotBeReboundByDuplicateProof()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);await f.Ingest(s,1000,new Event(100,Id(1),RootId,"file"));await s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct);var claim=(await s.ReadLocalMetadataClaimAsync(p.ProjectionId,100,Ct))!;await s.CommitLocalMetadataProofAsync(MockProof(claim),Ct);var before=await s.ReadLocalProjectionAsync(p.ProjectionId,Ct);await Assert.ThrowsAsync<InvalidOperationException>(()=>s.CommitLocalMetadataProofAsync(MockProof(claim,created:11),Ct));Assert.Equal(before,await s.ReadLocalProjectionAsync(p.ProjectionId,Ct));
    }
    [Fact]
    public async Task NewNamespaceCandidateFencesOldProjectionBeforeEffectsOrNativeResolution()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var p=await s.BeginLocalProjectionAsync(await f.Candidate(s),new(MinimumFreeDiskBytes:0),Ct);await f.Ingest(s,1000,new Event(100,Id(1),RootId,"file"));await s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct);var claim=(await s.ReadLocalMetadataClaimAsync(p.ProjectionId,100,Ct))!;await s.BeginNamespaceCandidateAsync(f.Root,new(MinimumFreeDiskBytes:0),Ct);await Assert.ThrowsAsync<InvalidOperationException>(()=>s.ApplyNextLocalInboxAsync(p.ProjectionId,100,Ct));await Assert.ThrowsAsync<InvalidOperationException>(()=>s.CommitLocalMetadataProofAsync(MockProof(claim),Ct));await Assert.ThrowsAsync<InvalidOperationException>(()=>s.ReadLocalMetadataClaimAsync(p.ProjectionId,100,Ct));Assert.Equal(1000,(await s.ReadLocalProjectionAsync(p.ProjectionId,Ct)).AppliedUsn);
    }
    private sealed class Fixture:IAsyncDisposable
    {
        public string Directory=Path.Combine(Path.GetTempPath(),"neptune-lc2b-tests-"+Guid.NewGuid().ToString("N"));public string Source=>Path.Combine(Directory,"source");public string Database=>Path.Combine(Directory,"state.db");public NeptuneStateStore Store=null!;
        public LocalNamespaceRootProof Root=>new(Source,"d8481792-21dd-422b-b116-35d025404382","NTFS",Handle());public LocalSourceBinding Binding=>new("audit","client","mapping",Source,Root.VolumeGuid,"50532e73","NTFS",RootId,1,1,"rules-v1","nfc-casefold-v1");
        public static async Task<Fixture> Create(int size=4096){var f=new Fixture();System.IO.Directory.CreateDirectory(f.Source);f.Store=new(f.Database);if(size!=4096)await f.Sql("PRAGMA page_size="+size+";VACUUM");await f.Store.InitializeAsync(Ct);await f.Store.AddMappingAsync(new("mapping","client",f.Source,true,DateTimeOffset.UtcNow),Ct);return f;}
        public Task<LocalSourceJournalSession> Open()=>Store.OpenLocalJournalSessionAsync(Binding,new(Journal,100,0,100000000,2,4,false),100,new(MinimumFreeDiskBytes:0),Ct);
        public async Task<string> Candidate(LocalSourceJournalSession s,string name="file")
        {var c=await s.BeginNamespaceCandidateAsync(Root,new(MinimumFreeDiskBytes:0),Ct);var p=(await s.ClaimNamespaceDirectoryAsync(c.CandidateId,Ct))!;await s.RecordNamespaceHandleAsync(p,Handle(),Ct);await s.StageNamespaceEntriesAsync(p,[new(name,Id(1),10,20,30,42,0x20,0)],Ct);await s.CompleteNamespaceDirectoryAsync(p,Ct);while(await s.RefillNamespaceFrontierAsync(c.CandidateId,Ct)is not null){}await s.FinishNamespaceAsync(c.CandidateId,LocalNamespaceStatus.EnumerationComplete,cancellationToken:Ct);return c.CandidateId;}
        public async Task Ingest(LocalSourceJournalSession s,long end,params Event[] events){var cursor=(await s.ReadStateAsync(Ct)).Cursor;await s.CapturePageAsync(cursor,UsnRecordParser.Parse(Journal,cursor.NextReadUsn,Frame(end,events)),Ct);}
        public async Task<SqliteConnection> Connection(){var c=new SqliteConnection("Data Source="+Database+";Pooling=False;Cache=Private");await c.OpenAsync(Ct);return c;}
        public async Task Sql(string sql){await using var c=await Connection();await using var q=c.CreateCommand();q.CommandText=sql;await q.ExecuteNonQueryAsync(Ct);}
        public async Task<object?> Scalar(string sql){await using var c=await Connection();await using var q=c.CreateCommand();q.CommandText=sql;return await q.ExecuteScalarAsync(Ct);}
        public static string FileHash(string p){if(!File.Exists(p))return "missing";using var file=new FileStream(p,FileMode.Open,FileAccess.Read,FileShare.ReadWrite|FileShare.Delete);return Convert.ToHexString(SHA256.HashData(file));}
        public string Hashes()=>FileHash(Database)+FileHash(Database+"-wal");
        public ValueTask DisposeAsync(){SqliteConnection.ClearAllPools();if(!Path.GetFullPath(Directory).StartsWith(Path.Combine(Path.GetTempPath(),"neptune-lc2b-tests-"),StringComparison.OrdinalIgnoreCase))throw new InvalidOperationException();System.IO.Directory.Delete(Directory,true);return ValueTask.CompletedTask;}
    }
}
