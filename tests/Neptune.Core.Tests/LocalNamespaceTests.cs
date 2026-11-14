using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("LC1 resource qualification")]
public sealed class LocalNamespaceTests
{
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    private const string RootId="01000000000000000300000000000000",Serial64="aabbccdd50532e73";
    private static LocalNamespaceEntry Entry(int id,string? name=null,bool directory=false)=>new(name??"file-"+id,id.ToString("x32"),10,20,30,directory?0:42,directory?0x10u:0x20u,0);
    private static LocalNamespaceHandleProof Proof(string node=RootId,long created=10,bool sensitive=false)=>new(node,"1122334455667788",Serial64,0x50532e73,created,0x10,sensitive);
    private static byte[] Raw(params LocalNamespaceEntry[] entries)
    {
        var lengths=entries.Select(e=>(88+Encoding.Unicode.GetByteCount(e.Name)+7)/8*8).ToArray();var bytes=new byte[lengths.Sum()];var at=0;
        for(var i=0;i<entries.Length;i++)
        {var e=entries[i];var s=bytes.AsSpan(at,lengths[i]);BinaryPrimitives.WriteUInt32LittleEndian(s,i==entries.Length-1?0u:(uint)lengths[i]);BinaryPrimitives.WriteInt64LittleEndian(s[8..],e.CreationTime);BinaryPrimitives.WriteInt64LittleEndian(s[24..],e.LastWriteTime);BinaryPrimitives.WriteInt64LittleEndian(s[32..],e.ChangeTime);BinaryPrimitives.WriteInt64LittleEndian(s[40..],e.Size);BinaryPrimitives.WriteUInt32LittleEndian(s[56..],e.Attributes);BinaryPrimitives.WriteUInt32LittleEndian(s[60..],(uint)e.Name.Length*2);BinaryPrimitives.WriteUInt32LittleEndian(s[68..],e.ReparseTag);Convert.FromHexString(e.FileId128).CopyTo(s[72..]);Encoding.Unicode.GetBytes(e.Name).CopyTo(s[88..]);at+=lengths[i];}
        return bytes;
    }
    [Fact]
    public void NativeFramesKeepExactHigh128IdsAndDoNotUseFileIndexOrUndefinedTag()
    {
        var input=Entry(1,"abc") with{FileId128="0123456789abcdef1020304050607080"};var raw=Raw(input);BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(4),uint.MaxValue);BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(68),uint.MaxValue);
        var actual=Assert.Single(WindowsDirectoryPageParser.Parse(raw));Assert.Equal(input,actual);Assert.Equal(input.FileId128,actual.FileId128);
    }
    [Theory]
    [InlineData("Header")][InlineData("Offset")][InlineData("Alignment")][InlineData("Length")][InlineData("Odd")]
    [InlineData("Unicode")][InlineData("Nul")][InlineData("Slash")][InlineData("ZeroId")][InlineData("Size")][InlineData("Device")][InlineData("Tail")][InlineData("TooMany")]
    public void MalformedNativeFramesFailClosed(string mutation)
    {
        var raw=mutation=="TooMany"?Raw(Enumerable.Range(1,257).Select(i=>Entry(i,"x")).ToArray()):Raw(Entry(1,"abc"),Entry(2,"def"));
        switch(mutation)
        {
            case "Header":raw=raw[..80];break;case "Offset":BinaryPrimitives.WriteUInt32LittleEndian(raw,88);break;case "Alignment":BinaryPrimitives.WriteUInt32LittleEndian(raw,95);break;
            case "Length":BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(60),512);break;case "Odd":BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(60),3);break;
            case "Unicode":BinaryPrimitives.WriteUInt16LittleEndian(raw.AsSpan(88),0xd800);break;case "Nul":raw[88]=raw[89]=0;break;case "Slash":BinaryPrimitives.WriteUInt16LittleEndian(raw.AsSpan(88),'/');break;
            case "ZeroId":raw.AsSpan(72,16).Clear();break;case "Size":BinaryPrimitives.WriteInt64LittleEndian(raw.AsSpan(40),-1);break;case "Device":BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(56),0x40);break;case "Tail":raw[^1]=1;break;
        }
        Assert.Throws<InvalidDataException>(()=>WindowsDirectoryPageParser.Parse(raw));
    }
    [Fact]
    public void ProtocolDotRecordsAreExcludedButReparseMetadataIsPreserved()
    {
        var reparse=Entry(3,"link",true) with{Attributes=0x410,ReparseTag=0xa0000003};var result=WindowsDirectoryPageParser.Parse(Raw(Entry(1,"."),Entry(2,".."),reparse));var e=Assert.Single(result);Assert.True(e.Excluded);Assert.Equal(reparse,e);
    }
    [Fact]
    public async Task CandidatePagesBecomeEligibleOnlyAfterAllSiblingValidationAndHandleProof()
    {
        await using var f=await Fixture.Create();await using var session=await f.Open();var c=await session.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct);
        var parent=(await session.ClaimNamespaceDirectoryAsync(c.CandidateId,Ct))!;
        await Assert.ThrowsAsync<InvalidOperationException>(()=>session.CompleteNamespaceDirectoryAsync(parent,Ct));
        await session.RecordNamespaceHandleAsync(parent,Proof(),Ct);var written=await session.StageNamespaceEntriesAsync(parent,[Entry(1),Entry(2)],Ct);
        Assert.Equal(2,written.Entries);Assert.Empty(await session.ReadNamespaceEligibleEntriesAsync(c.CandidateId,cancellationToken:Ct));
        Assert.Equal(2,await session.CompleteNamespaceDirectoryAsync(parent,Ct));Assert.Equal(2,(await session.ReadNamespaceEligibleEntriesAsync(c.CandidateId,cancellationToken:Ct)).Count);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>session.FinishNamespaceAsync(c.CandidateId,LocalNamespaceStatus.EnumerationComplete,cancellationToken:Ct));
        Assert.True((await session.RefillNamespaceFrontierAsync(c.CandidateId,Ct))!.Complete);await session.FinishNamespaceAsync(c.CandidateId,LocalNamespaceStatus.EnumerationComplete,cancellationToken:Ct);
        var result=await session.ReadNamespaceCandidateAsync(c.CandidateId,Ct);Assert.Equal("Unknown",result.LocalCompleteness);Assert.Equal("Unknown",result.SourceCycle);
        Assert.Empty(await session.ReadNamespaceEligibleEntriesAsync(c.CandidateId,afterEntryId:result.EligibleThrough,cancellationToken:Ct));
    }
    [Theory]
    [InlineData("Case")][InlineData("Nfc")][InlineData("Rebind")][InlineData("Self")][InlineData("DirectoryAlias")][InlineData("BadUnicode")]
    public async Task ParentCollisionsOrIdentityAliasesRollbackWithoutEligibility(string mode)
    {
        await using var f=await Fixture.Create();await using var session=await f.Open();var c=await session.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct);var p=(await session.ClaimNamespaceDirectoryAsync(c.CandidateId,Ct))!;await session.RecordNamespaceHandleAsync(p,Proof(sensitive:true),Ct);
        var entries=mode switch{"Case"=>new[]{Entry(1,"Readme"),Entry(2,"README")},"Nfc"=>new[]{Entry(1,"é"),Entry(2,"e\u0301")},"Rebind"=>new[]{Entry(1,"a"),Entry(1,"b") with{CreationTime=11}},"Self"=>new[]{Entry(1) with{FileId128=RootId}},"DirectoryAlias"=>new[]{Entry(1,"a",true),Entry(1,"b",true)},_=>new[]{Entry(1,"\ud800")}};
        await Assert.ThrowsAnyAsync<Exception>(()=>session.StageNamespaceEntriesAsync(p,entries,Ct));Assert.Empty(await session.ReadNamespaceEligibleEntriesAsync(c.CandidateId,cancellationToken:Ct));Assert.Equal(0,(await session.ReadNamespaceCandidateAsync(c.CandidateId,Ct)).Entries);
        await session.FinishNamespaceAsync(c.CandidateId,LocalNamespaceStatus.Incomplete,"collision",Ct);await Assert.ThrowsAsync<InvalidOperationException>(()=>session.ReadNamespaceEligibleEntriesAsync(c.CandidateId,cancellationToken:Ct));
    }
    [Fact]
    public async Task HardlinkFilesRetainTwoEdgesAndUnprovenWireAliasesAreNotInvented()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var c=await s.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct);var p=(await s.ClaimNamespaceDirectoryAsync(c.CandidateId,Ct))!;await s.RecordNamespaceHandleAsync(p,Proof(),Ct);
        await s.StageNamespaceEntriesAsync(p,[Entry(1,"a"),Entry(1,"b")],Ct);await s.CompleteNamespaceDirectoryAsync(p,Ct);Assert.Equal(2,(await s.ReadNamespaceEligibleEntriesAsync(c.CandidateId,cancellationToken:Ct)).Count);
        Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_namespace_nodes"));Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_namespace_aliases"));
        Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_namespace_aliases WHERE major=2 AND wire_id='1122334455667788'"));Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_namespace_aliases WHERE node_id='"+1.ToString("x32")+"'"));
    }
    [Fact]
    public async Task WidePromotionAndFrontierHaveConstantRowEffectsAndResumeNeedsFreshCandidate()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var c=await s.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct);var p=(await s.ClaimNamespaceDirectoryAsync(c.CandidateId,Ct))!;await s.RecordNamespaceHandleAsync(p,Proof(),Ct);
        for(var at=1;at<=1024;at+=128){var result=await s.StageNamespaceEntriesAsync(p,Enumerable.Range(at,128).Select(i=>Entry(i,directory:true)).ToArray(),Ct);Assert.Equal(259,result.ChangedRows);}
        var duplicate=await s.StageNamespaceEntriesAsync(p,Enumerable.Range(1,128).Select(i=>Entry(i,directory:true)).ToArray(),Ct);Assert.Equal(0,duplicate.ChangedRows);
        Assert.Equal(2,await s.CompleteNamespaceDirectoryAsync(p,Ct));var refill=await s.RefillNamespaceFrontierAsync(c.CandidateId,Ct);Assert.Equal(256,refill!.Added);Assert.Equal(257,refill.ChangedRows);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>s.ClaimNamespaceDirectoryAsync(c.CandidateId,Ct));var next=await s.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct);
        Assert.Equal(LocalNamespaceStatus.Superseded,(await s.ReadNamespaceCandidateAsync(c.CandidateId,Ct)).Status);await Assert.ThrowsAsync<InvalidOperationException>(()=>s.StageNamespaceEntriesAsync(p,[Entry(3000)],Ct));
        var removed=0;for(var i=0;i<20;i++){var r=await s.RetireNamespaceBatchAsync(c.CandidateId,Ct);Assert.InRange(r.ChangedRows,0,258);removed+=r.Rows;if(r.Removed)break;}
        Assert.True(removed>=1024);Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_namespace_candidates WHERE candidate_id='"+c.CandidateId+"'"));Assert.Equal(1024,(await s.ReadNamespaceCandidateAsync(next.CandidateId,Ct)).Bytes);
    }
    [Fact]
    public async Task CancelledBatchRollsBackAndNextGenerationNeverPromotesOldPrefix()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var c=await s.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct);var p=(await s.ClaimNamespaceDirectoryAsync(c.CandidateId,Ct))!;await s.RecordNamespaceHandleAsync(p,Proof(),Ct);
        using var cancelled=new CancellationTokenSource();cancelled.Cancel();await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>s.StageNamespaceEntriesAsync(p,[Entry(1)],cancelled.Token));Assert.Equal(0,(await s.ReadNamespaceCandidateAsync(c.CandidateId,Ct)).Entries);
        var next=await s.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct);Assert.Empty(await s.ReadNamespaceEligibleEntriesAsync(next.CandidateId,cancellationToken:Ct));
    }
    [Fact]
    public async Task OwnerRestartStaleCandidateMappingPauseAndLegacyWritesRemainFenced()
    {
        await using var f=await Fixture.Create();var first=await f.Open();var c=await first.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct);await Assert.ThrowsAsync<IOException>(()=>f.Open());await first.DisposeAsync();
        await using var second=await f.Open();await Assert.ThrowsAsync<InvalidOperationException>(()=>second.ReadNamespaceCandidateAsync(c.CandidateId,Ct));var next=await second.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct);
        await f.Store.UpsertSyncFileAsync(new("mapping","legacy.txt",1,DateTimeOffset.UtcNow,new string('a',64),null,"synced",DateTimeOffset.UtcNow,null),Ct);await Assert.ThrowsAsync<LocalSourceGapException>(()=>second.ReadNamespaceEligibleEntriesAsync(next.CandidateId,cancellationToken:Ct));
        Assert.Equal(LocalNamespaceStatus.Incomplete,(await second.ReadNamespaceCandidateAsync(next.CandidateId,Ct)).Status);Assert.Equal("LegacyWrite",(await second.ReadStateAsync(Ct)).GapCode);
        await f.Sql("UPDATE sync_mappings SET enabled=0");await Assert.ThrowsAsync<LocalSourceGapException>(()=>second.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct));
    }
    [Theory]
    [InlineData("Id")][InlineData("Volume")][InlineData("Root")][InlineData("Serial")]
    public async Task RootProofCannotBeReboundByPathOrTruncatedId(string kind)
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var proof=kind switch{"Id"=>f.Root with{Handle=Proof(node:"01000000000000000000000000000000")},"Volume"=>f.Root with{VolumeGuid=Guid.NewGuid().ToString("D")},"Root"=>f.Root with{RootPath=f.Source+"-other"},_=>f.Root with{Handle=Proof() with{LegacyVolumeSerial=1}}};
        await Assert.ThrowsAsync<InvalidDataException>(()=>s.BeginNamespaceCandidateAsync(proof,cancellationToken:Ct));Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM sqlite_master WHERE name='local_namespace_format'"));
    }
    [Theory]
    [InlineData("Future")][InlineData("Index")][InlineData("Orphan")][InlineData("Extra")]
    public async Task UnsupportedSchemaRefusesBeforeWritableInitOrJournalAndPreservesBytes(string mode)
    {
        await using var f=await Fixture.Create();await using(var s=await f.Open()){await s.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct);}
        await using var held=await f.Connection();await f.Sql(mode switch{"Future"=>"UPDATE local_namespace_format SET version=99","Index"=>"DROP INDEX local_namespace_children","Orphan"=>"DROP TABLE local_namespace_format",_=>"CREATE INDEX unexpected_namespace ON local_namespace_nodes(kind)"});
        var before=f.Hashes();await Assert.ThrowsAsync<InvalidDataException>(()=>new NeptuneStateStore(f.Database).InitializeAsync(Ct));Assert.Equal(before,f.Hashes());await Assert.ThrowsAsync<InvalidDataException>(()=>f.Open());Assert.Equal(before,f.Hashes());
    }
    [Fact]
    public async Task NamespaceOnlyFutureProfileDoesNotGetLegacyDdl()
    {
        await using var f=await Fixture.Create();var path=Path.Combine(f.Directory,"future.db");await using var c=new SqliteConnection("Data Source="+path+";Pooling=False");await c.OpenAsync(Ct);await using var q=c.CreateCommand();q.CommandText="PRAGMA journal_mode=WAL;CREATE TABLE local_namespace_format(singleton INTEGER PRIMARY KEY,version INTEGER);INSERT INTO local_namespace_format VALUES(1,99)";await q.ExecuteNonQueryAsync(Ct);
        var before=Fixture.Hash(path)+Fixture.Hash(path+"-wal");await Assert.ThrowsAsync<InvalidDataException>(()=>new NeptuneStateStore(path).InitializeAsync(Ct));Assert.Equal(before,Fixture.Hash(path)+Fixture.Hash(path+"-wal"));q.CommandText="SELECT COUNT(*) FROM sqlite_master WHERE name='sync_mappings'";Assert.Equal(0L,await q.ExecuteScalarAsync(Ct));
    }
    [Fact]
    public async Task MigrationBackupIncludesWalPausedDataCheckpointsAndSeparateRollback()
    {
        await using var f=await Fixture.Create();await f.Store.AddMappingAsync(new("paused","client",f.Source+"-paused",false,DateTimeOffset.UtcNow),Ct);await f.Store.SaveSyncUploadCheckpointAsync(new("resume","https://test/dav/file","key","hash","upload"),Ct);
        await using var held=await f.Connection();await using(var s=await f.Open()){await s.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct);}
        await f.Store.InitializeAsync(Ct);await f.Store.InitializeAsync(Ct);var copy=await f.Store.CreateLocalNamespaceRollbackCopyAsync(Path.Combine(f.Directory,"rollback.db"),Ct);
        await using var c=new SqliteConnection("Data Source="+copy+";Mode=ReadOnly;Pooling=False");await c.OpenAsync(Ct);await using var q=c.CreateCommand();q.CommandText="SELECT enabled FROM sync_mappings WHERE mapping_id='paused'";Assert.Equal(0L,await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT upload_id FROM sync_upload_checkpoints WHERE scope='resume'";Assert.Equal("upload",await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT COUNT(*) FROM sqlite_master WHERE name='local_namespace_format'";Assert.Equal(0L,await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT version FROM local_source_format";Assert.Equal(2L,await q.ExecuteScalarAsync(Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(()=>f.Store.CreateLocalNamespaceRollbackCopyAsync(f.Database,Ct));
    }
    [Fact]
    public async Task DiskAndLogicalQuotaRefusalDoesNotPromoteAParentOrCursor()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var state=await s.ReadStateAsync(Ct);
        await Assert.ThrowsAsync<LocalSourceGapException>(()=>s.BeginNamespaceCandidateAsync(f.Root,new(1024*1024,1024*1024,long.MaxValue),Ct));Assert.Equal(state.Cursor,(await s.ReadStateAsync(Ct)).Cursor);
        var c=await s.BeginNamespaceCandidateAsync(f.Root,new(1024,1024L*1024*1024,0),Ct);var p=(await s.ClaimNamespaceDirectoryAsync(c.CandidateId,Ct))!;await s.RecordNamespaceHandleAsync(p,Proof(),Ct);await Assert.ThrowsAsync<LocalSourceGapException>(()=>s.StageNamespaceEntriesAsync(p,[Entry(1)],Ct));Assert.Equal(0,(await s.ReadNamespaceCandidateAsync(c.CandidateId,Ct)).Entries);Assert.Empty(await s.ReadNamespaceEligibleEntriesAsync(c.CandidateId,cancellationToken:Ct));
    }
    [Theory]
    [InlineData(512)][InlineData(4096)][InlineData(65536)]
    public async Task LegalPageSizesAdaptKnownWriteReserveAndBoundFrontier(int size)
    {
        await using var f=await Fixture.Create(size);await using var s=await f.Open();var c=await s.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct);var p=(await s.ClaimNamespaceDirectoryAsync(c.CandidateId,Ct))!;await s.RecordNamespaceHandleAsync(p,Proof(),Ct);
        Assert.InRange(s.NamespaceStageCapacity,1,128);if(size==65536)Assert.True(s.NamespaceStageCapacity<128);
        for(var at=1;at<=256;at+=s.NamespaceStageCapacity)await s.StageNamespaceEntriesAsync(p,Enumerable.Range(at,Math.Min(s.NamespaceStageCapacity,257-at)).Select(i=>Entry(i,directory:true)).ToArray(),Ct);
        Assert.Equal(2,await s.CompleteNamespaceDirectoryAsync(p,Ct));var total=0;while(await s.RefillNamespaceFrontierAsync(c.CandidateId,Ct)is{} refill){Assert.InRange(refill.Added,0,256);Assert.InRange(refill.ChangedRows,1,257);total+=refill.Added;}
        Assert.Equal(256,total);Assert.InRange(f.Store.GetLocalNamespaceMetrics().MaximumChangedRows,1,259);
    }
    [Fact]
    public async Task PinnedReaderStopsBeforeNextReservationAndReleaseDoesNotInventAZeroPendingEstimate()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();var c=await s.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct);var p=(await s.ClaimNamespaceDirectoryAsync(c.CandidateId,Ct))!;await s.RecordNamespaceHandleAsync(p,Proof(),Ct);
        await using var held=await f.Connection();using var pin=held.BeginTransaction(deferred:true);await using(var q=held.CreateCommand()){q.Transaction=pin;q.CommandText="SELECT COUNT(*) FROM local_namespace_entries";await q.ExecuteScalarAsync(Ct);}
        string? stop=null;long prefix=0;
        for(var at=1;at<200000;at+=128)
        {try{await s.StageNamespaceEntriesAsync(p,Enumerable.Range(at,128).Select(i=>Entry(i)).ToArray(),Ct);prefix+=128;}catch(LocalSourceGapException e){stop=e.Code;break;}}
        Assert.Equal("WalAdmission",stop);var before=f.Store.GetLocalJournalMetrics();Assert.InRange(before.UncheckpointedWalBytes,1,64L*1024*1024);Assert.InRange(before.PhysicalWalBytes,1,64L*1024*1024);Assert.InRange(before.CheckpointAttempts,1,4);
        Assert.Equal(prefix,(await s.ReadNamespaceCandidateAsync(c.CandidateId,Ct)).Entries);Assert.Empty(await s.ReadNamespaceEligibleEntriesAsync(c.CandidateId,cancellationToken:Ct));Assert.Equal(100,(await s.ReadStateAsync(Ct)).Cursor.NextReadUsn);
        pin.Dispose();var released=await f.Store.InspectLocalJournalWalAsync(true,Ct);Assert.Equal(0,released.UncheckpointedWalBytes);Assert.True(released.PhysicalWalBytes>=before.PhysicalWalBytes);await s.FinishNamespaceAsync(c.CandidateId,LocalNamespaceStatus.Incomplete,"wal budget",Ct);
    }
    [Theory]
    [InlineData("DeepWide")][InlineData("Mutation")][InlineData("Denied")][InlineData("Cancel")]
    public async Task ScannerKeepsCompleteCandidateSeparateFromContinuousProofAndErrors(string scenario)
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();await using var source=new FakeSource(f.Root,scenario);using var cancel=new CancellationTokenSource();source.Cancel=cancel;
        if(scenario=="DeepWide")
        {var c=await new LocalNamespaceScanner(source).ScanAsync(s,cancellationToken:Ct);Assert.Equal(LocalNamespaceStatus.EnumerationComplete,c.Status);Assert.Equal(321,c.Entries);Assert.Equal(21,c.ValidatedParents);Assert.Equal("Unknown",c.LocalCompleteness);Assert.Equal("Unknown",c.SourceCycle);}
        else
        {await Assert.ThrowsAnyAsync<Exception>(()=>new LocalNamespaceScanner(source).ScanAsync(s,cancellationToken:scenario=="Cancel"?cancel.Token:Ct));Assert.Equal(2L,await f.Scalar("SELECT status FROM local_namespace_candidates ORDER BY rowid DESC LIMIT 1"));Assert.Equal(100,(await s.ReadStateAsync(Ct)).Cursor.NextReadUsn);}
    }
    [Fact]
    public async Task RawRollbackManifestRejectsDepthDuplicateCasingAndNonintegerFormatBeforeCopy()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open();await s.BeginNamespaceCandidateAsync(f.Root,cancellationToken:Ct);var path=f.Database+".local-namespace-v1-backup.json";var original=await File.ReadAllTextAsync(path,Ct);
        foreach(var malformed in new[]{original.Replace("\"targetFormat\":1","\"targetFormat\":1.0"),original.Replace("\"sourcePath\"","\"SOURCEPATH\""),original[..^1]+",\"targetFormat\":1}","{\"x\":"+new string('[',9)+"0"+new string(']',9)+"}"})
        {await File.WriteAllTextAsync(path,malformed,Ct);await Assert.ThrowsAnyAsync<Exception>(()=>f.Store.CreateLocalNamespaceRollbackCopyAsync(Path.Combine(f.Directory,"bad.db"),Ct));Assert.False(File.Exists(Path.Combine(f.Directory,"bad.db")));}
        await File.WriteAllTextAsync(path,original,Ct);
    }
    private sealed class FakeSource(LocalNamespaceRootProof proof,string scenario):ILocalNamespaceSource
    {
        public string Scenario=>scenario;
        public LocalNamespaceRootProof RootProof=>proof;public CancellationTokenSource? Cancel;
        public ValueTask DisposeAsync()=>ValueTask.CompletedTask;
        public ValueTask<ILocalNamespaceDirectoryReader> OpenDirectoryAsync(LocalNamespaceDirectory directory,CancellationToken cancellationToken=default)
        {if(scenario=="Denied"&&directory.RelativePath!="")throw new UnauthorizedAccessException("owned synthetic denied");return ValueTask.FromResult<ILocalNamespaceDirectoryReader>(new FakeReader(this,directory));}
        private sealed class FakeReader(FakeSource source,LocalNamespaceDirectory parent):ILocalNamespaceDirectoryReader
        {
            private int _page;public LocalNamespaceHandleProof Proof=>parent.RelativePath==""?source.RootProof.Handle:LocalNamespaceTests.Proof(parent.FileId128,parent.CreationTime) with{LegacyFileId64=(90000+parent.RelativePath.Length).ToString("x16")};
            public ValueTask<LocalNamespacePage> ReadPageAsync(CancellationToken cancellationToken=default)
            {
                cancellationToken.ThrowIfCancellationRequested();if(source.Scenario=="Cancel"){source.Cancel!.Cancel();cancellationToken.ThrowIfCancellationRequested();}
                if(parent.RelativePath=="")
                {if(_page++==0)return ValueTask.FromResult(new LocalNamespacePage([Entry(1001,"d",true)],false));if(source.Scenario!="DeepWide"||_page>4)return ValueTask.FromResult(new LocalNamespacePage([],true));var start=(_page-2)*128+1;return ValueTask.FromResult(new LocalNamespacePage(Enumerable.Range(start,Math.Min(128,301-start)).Select(i=>Entry(i)).ToArray(),false));}
                var depth=parent.RelativePath.Split('/').Length;if(_page++>0||depth>=20)return ValueTask.FromResult(new LocalNamespacePage(depth>=20&&_page==1?[Entry(2001,"leaf")]:[],true));return ValueTask.FromResult(new LocalNamespacePage([Entry(1001+depth,"d",true)],true));
            }
            public ValueTask ValidateUnchangedAsync(CancellationToken cancellationToken=default){if(source.Scenario=="Mutation")throw new IOException("owned synthetic metadata changed");return ValueTask.CompletedTask;}
            public ValueTask DisposeAsync()=>ValueTask.CompletedTask;
        }
    }
    private sealed class Fixture:IAsyncDisposable
    {
        public string Directory=Path.Combine(Path.GetTempPath(),"neptune-lc2-tests-"+Guid.NewGuid().ToString("N"));public string Source=>Path.Combine(Directory,"source");public string Database=>Path.Combine(Directory,"state.db");public NeptuneStateStore Store=null!;
        public LocalNamespaceRootProof Root=>new(Source,"d8481792-21dd-422b-b116-35d025404382","NTFS",Proof());
        public LocalSourceBinding Binding=>new("audit","client","mapping",Source,Root.VolumeGuid,"50532e73","NTFS",RootId,1,1,"rules-v1","nfc-casefold-v1");
        public static async Task<Fixture> Create(int pageSize=4096){var f=new Fixture();System.IO.Directory.CreateDirectory(f.Source);f.Store=new(f.Database);if(pageSize!=4096)await f.Sql("PRAGMA page_size="+pageSize+";VACUUM");await f.Store.InitializeAsync(Ct);await f.Store.AddMappingAsync(new("mapping","client",f.Source,true,DateTimeOffset.UtcNow),Ct);return f;}
        public Task<LocalSourceJournalSession> Open()=>Store.OpenLocalJournalSessionAsync(Binding,new("01dc6ff2f7335b65",100,0,100000000,2,4,false),100,new(MinimumFreeDiskBytes:0),Ct);
        public async Task<SqliteConnection> Connection(){var c=new SqliteConnection("Data Source="+Database+";Pooling=False;Cache=Private");await c.OpenAsync(Ct);return c;}
        public async Task Sql(string sql){await using var c=await Connection();await using var q=c.CreateCommand();q.CommandText=sql;await q.ExecuteNonQueryAsync(Ct);}
        public async Task<object?> Scalar(string sql){await using var c=await Connection();await using var q=c.CreateCommand();q.CommandText=sql;return await q.ExecuteScalarAsync(Ct);}
        public static string Hash(string p){if(!File.Exists(p))return "missing";using var f=new FileStream(p,FileMode.Open,FileAccess.Read,FileShare.ReadWrite|FileShare.Delete);return Convert.ToHexString(SHA256.HashData(f));}
        public string Hashes()=>Hash(Database)+Hash(Database+"-wal");
        public ValueTask DisposeAsync(){SqliteConnection.ClearAllPools();if(!Path.GetFullPath(Directory).StartsWith(Path.Combine(Path.GetTempPath(),"neptune-lc2-tests-"),StringComparison.OrdinalIgnoreCase))throw new InvalidOperationException();System.IO.Directory.Delete(Directory,true);return ValueTask.CompletedTask;}
    }
}
