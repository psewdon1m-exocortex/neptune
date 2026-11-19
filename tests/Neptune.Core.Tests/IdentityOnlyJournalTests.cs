using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Reflection;
using System.Text;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("LC1 resource qualification")]
public sealed class IdentityOnlyJournalTests
{
    internal const string Journal="01dc6ff2f7335b65";
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    internal static byte[] Frame(long start,int count=3,ushort major=3,uint reason=0x80000002)
    {
        var shift=major==3?16:0;var length=64+shift;var b=new byte[8+count*length];BinaryPrimitives.WriteInt64LittleEndian(b,start+count*length);
        for(var i=0;i<count;i++)
        {
            var r=b.AsSpan(8+i*length,length);BinaryPrimitives.WriteInt32LittleEndian(r,length);BinaryPrimitives.WriteUInt16LittleEndian(r[4..],major);
            BinaryPrimitives.WriteUInt64LittleEndian(r[8..],(ulong)(i+1));BinaryPrimitives.WriteUInt64LittleEndian(r[(major==3?24:16)..],17);
            BinaryPrimitives.WriteInt64LittleEndian(r[(24+shift)..],start+i*length);BinaryPrimitives.WriteUInt32LittleEndian(r[(40+shift)..],reason);
            BinaryPrimitives.WriteUInt16LittleEndian(r[(58+shift)..],(ushort)(60+shift));
        }
        return b;
    }
    [Theory][InlineData(2)][InlineData(3)]
    public void NamedDefaultRejectsAndIdentityOnlyRetainsExplicitRealSubpageBoundary(int version)
    {
        var raw=Frame(100,400,(ushort)version);Assert.Throws<LocalSourceGapException>(()=>UsnRecordParser.Parse(Journal,100,raw));
        var page=UsnRecordParser.ParseIdentityOnly(Journal,100,raw);Assert.Equal(LocalJournalReadMode.IdentityOnly,page.ReadMode);Assert.Equal(256,page.Records.Count);
        Assert.All(page.Records,r=>{Assert.Empty(r.Name);Assert.Equal(LocalJournalReadMode.IdentityOnly,r.ReadMode);Assert.Equal(version,r.MajorVersion);Assert.Equal(version==2?16:32,r.FileIdHex.Length);});
        Assert.Equal(100+256*(version==2?64:80),page.NextReadUsn);Assert.NotEqual(page.Records[^1].Usn+1,page.NextReadUsn);
        var offset=8+256*(version==2?64:80);var rest=new byte[8+raw.Length-offset];raw.AsSpan(0,8).CopyTo(rest);raw.AsSpan(offset).CopyTo(rest.AsSpan(8));
        Assert.Equal(144,UsnRecordParser.ParseIdentityOnly(Journal,page.NextReadUsn,rest).Records.Count);
    }
    [Theory]
    [InlineData("Named")][InlineData("Offset")][InlineData("Version")][InlineData("Reason")][InlineData("Tracking")]
    [InlineData("Order")][InlineData("Next")][InlineData("Trailing")][InlineData("Length")]
    public void UnknownIdentityOnlyFrameNeverAdvances(string mutation)
    {
        var b=Frame(100,mutation=="Trailing"?300:3);var offset=mutation=="Trailing"?8+299*80:8;
        switch(mutation)
        {
            case "Named":BinaryPrimitives.WriteUInt16LittleEndian(b.AsSpan(80),2);break;
            case "Offset":BinaryPrimitives.WriteUInt16LittleEndian(b.AsSpan(82),0);break;
            case "Version":case "Trailing":BinaryPrimitives.WriteUInt16LittleEndian(b.AsSpan(offset+4),4);break;
            case "Reason":BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(64),0x40000000);break;
            case "Tracking":BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(68),0x80000000);break;
            case "Order":BinaryPrimitives.WriteInt64LittleEndian(b.AsSpan(8+80+40),100);break;
            case "Next":BinaryPrimitives.WriteInt64LittleEndian(b,100);break;
            case "Length":b=b[..^1];break;
        }
        Assert.Throws<LocalSourceGapException>(()=>UsnRecordParser.ParseIdentityOnly(Journal,100,b));
    }
    [Theory][InlineData(2)][InlineData(3)]
    public async Task OpaqueNativeAlignmentBytesRemainExactButNeverBecomeAName(int major)
    {
        var raw=Frame(100,1,(ushort)major);raw[^4]=0x6f;raw[^3]=0;raw[^2]=0x77;raw[^1]=0;
        var page=UsnRecordParser.ParseIdentityOnly(Journal,100,raw);Assert.Empty(page.Records.Single().Name);Assert.Throws<LocalSourceGapException>(()=>UsnRecordParser.Parse(Journal,100,raw));
        await using var f=await Fixture.Create();await using var s=await f.Open(LocalJournalReadMode.IdentityOnly);await s.CapturePageAsync((await s.ReadStateAsync(Ct)).Cursor,page,Ct);
        var stored=await f.Bytes("SELECT payload FROM local_source_inbox");Assert.Equal(raw.AsSpan(8).ToArray(),stored.AsSpan(8).ToArray());Assert.Empty((await s.ReadInboxAsync(cancellationToken:Ct)).Single().Name);
    }
    [Fact]
    public async Task MixedModesRestartDuplicateAndNoSilentDowngradePreserveRawNamedHistory()
    {
        await using var f=await Fixture.Create();var named=UsnRecordParser.Parse(Journal,100,LocalSourceContinuityTests.Frame(100,2));
        string namedHex;await using(var s=await f.Open()){await s.CapturePageAsync((await s.ReadStateAsync(Ct)).Cursor,named,Ct);namedHex=(string)(await f.Scalar("SELECT hex(payload) FROM local_source_inbox ORDER BY usn LIMIT 1"))!;}
        var page=UsnRecordParser.ParseIdentityOnly(Journal,named.NextReadUsn,Frame(named.NextReadUsn));
        await using(var s=await f.Open(LocalJournalReadMode.IdentityOnly))
        {
            var state=await s.ReadStateAsync(Ct);Assert.Equal(named.NextReadUsn,state.Cursor.NextReadUsn);Assert.Equal(LocalJournalReadMode.IdentityOnly,state.ReadMode);
            var cursor=state.Cursor;var first=await s.CapturePageAsync(cursor,page,Ct);Assert.False(first.Duplicate);Assert.Equal(5,first.ChangedRows);
            var replay=await s.CapturePageAsync(cursor,page,Ct);Assert.True(replay.Duplicate);Assert.Equal(0,replay.ChangedRows);
            var rows=await s.ReadInboxAsync(cancellationToken:Ct);Assert.Equal(5,rows.Count);Assert.All(rows.Take(2),r=>Assert.Equal(LocalJournalReadMode.Named,r.ReadMode));Assert.All(rows.Skip(2),r=>{Assert.Empty(r.Name);Assert.Equal(LocalJournalReadMode.IdentityOnly,r.ReadMode);});
            Assert.Equal(namedHex,await f.Scalar("SELECT hex(payload) FROM local_source_inbox ORDER BY usn LIMIT 1"));Assert.Equal(3L,await f.Scalar("SELECT COUNT(*) FROM local_source_inbox WHERE substr(payload,1,4)=x'4e55534e'"));
        }
        await using(var s=await f.Open(LocalJournalReadMode.IdentityOnly)){Assert.Equal(page.NextReadUsn,(await s.ReadStateAsync(Ct)).Cursor.NextReadUsn);Assert.Equal(5,(await s.ReadInboxAsync(cancellationToken:Ct)).Count);}
        await Assert.ThrowsAsync<LocalSourceGapException>(()=>f.Open());Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_source_roots"));Assert.Equal(5L,await f.Scalar("SELECT inbox_records FROM local_source_format"));
    }
    [Fact]
    public async Task MismatchedReaderModeIsDurableGapWithoutSkippedCursor()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open(LocalJournalReadMode.IdentityOnly);var prior=await s.ReadStateAsync(Ct);
        await using var reader=new Reader(f.Binding);var e=await Assert.ThrowsAsync<LocalSourceGapException>(()=>s.CaptureNextAsync(reader,cancellationToken:Ct));Assert.Equal("ReadModeMismatch",e.Code);
        var after=await s.ReadStateAsync(Ct);Assert.False(after.Cursor.Valid);Assert.Equal(prior.Cursor.NextReadUsn,after.Cursor.NextReadUsn);Assert.Equal(0,after.InboxRecords);
    }
    [Theory][InlineData(4)][InlineData(5)][InlineData(6)][InlineData(7)]
    public async Task UnknownDurableEnvelopeRefusesInsteadOfGuessingNamed(int offset)
    {
        await using var f=await Fixture.Create();await using var s=await f.Open(LocalJournalReadMode.IdentityOnly);var state=await s.ReadStateAsync(Ct);await s.CapturePageAsync(state.Cursor,UsnRecordParser.ParseIdentityOnly(Journal,100,Frame(100,1)),Ct);
        var raw=await f.Bytes("SELECT payload FROM local_source_inbox");raw[offset]=17;await f.SetPayload(raw);
        await Assert.ThrowsAsync<LocalSourceGapException>(()=>s.ReadInboxAsync(cancellationToken:Ct));Assert.Equal(180,(await s.ReadStateAsync(Ct)).Cursor.NextReadUsn);
    }
    [Fact]
    public async Task CancelledPageAndOwnerExclusionRetainOriginalIdentityOnlyCursor()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open(LocalJournalReadMode.IdentityOnly);var prior=await s.ReadStateAsync(Ct);
        await Assert.ThrowsAnyAsync<IOException>(()=>f.Open(LocalJournalReadMode.IdentityOnly));using var canceled=new CancellationTokenSource();canceled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>s.CapturePageAsync(prior.Cursor,UsnRecordParser.ParseIdentityOnly(Journal,100,Frame(100,256)),canceled.Token));
        Assert.Equal(prior.Cursor,(await s.ReadStateAsync(Ct)).Cursor);Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_source_inbox"));
    }
    [Fact]
    public async Task FormatOneCommittedWalBackupMigrationPreservesPauseCheckpointAndCursor()
    {
        await using var f=await Fixture.Create();
        await f.Store.AddMappingAsync(new("paused","client",Path.Combine(f.Source,"paused"),false,DateTimeOffset.UtcNow),Ct);
        await f.Store.SaveSyncUploadCheckpointAsync(new("checkpoint","https://audit.invalid/owned","stable","fingerprint","original"),Ct);
        await using(var s=await f.Open()){await s.CapturePageAsync((await s.ReadStateAsync(Ct)).Cursor,UsnRecordParser.Parse(Journal,100,LocalSourceContinuityTests.Frame(100,2)),Ct);}
        await f.Sql("ALTER TABLE local_source_roots DROP COLUMN read_mode; UPDATE local_source_format SET version=1");
        await using var held=await f.Connection();await f.Sql("CREATE TABLE own_wal_proof(value TEXT);INSERT INTO own_wal_proof VALUES('committed')");Assert.True(File.Exists(f.Database+"-wal"));
        await using(var s=await f.Open(LocalJournalReadMode.IdentityOnly)){Assert.Equal(260,(await s.ReadStateAsync(Ct)).Cursor.NextReadUsn);Assert.Equal(2,(await s.ReadInboxAsync(cancellationToken:Ct)).Count);}
        var old=await f.Store.CreateLocalSourceV1RollbackCopyAsync(Path.Combine(f.Directory,"format1-rollback.db"),Ct);
        await using var copy=new SqliteConnection("Data Source="+old+";Mode=ReadOnly;Pooling=False");await copy.OpenAsync(Ct);await using var q=copy.CreateCommand();
        q.CommandText="SELECT version FROM local_source_format";Assert.Equal(1L,await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT value FROM own_wal_proof";Assert.Equal("committed",await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT enabled FROM sync_mappings WHERE mapping_id='paused'";Assert.Equal(0L,await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT upload_id FROM sync_upload_checkpoints WHERE scope='checkpoint'";Assert.Equal("original",await q.ExecuteScalarAsync(Ct));
    }
    [Theory][InlineData("future")][InlineData("column")]
    public async Task FutureOrBrokenModeSchemaRefusesReadonlyBeforeMainWrites(string corruption)
    {
        await using var f=await Fixture.Create();await using(var s=await f.Open()){ }await using var held=await f.Connection();
        await f.Sql(corruption=="future"?"UPDATE local_source_format SET version=3":"ALTER TABLE local_source_roots DROP COLUMN read_mode");var prior=f.Hashes();
        await Assert.ThrowsAsync<InvalidDataException>(()=>new NeptuneStateStore(f.Database).InitializeAsync(Ct));Assert.Equal(prior,f.Hashes());
    }
    [Fact]
    public async Task IdentityOnlyProjectionRetainsUnqualifiedNamelessEventsWithoutMetadataAdoption()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open(LocalJournalReadMode.IdentityOnly);var (candidate,_)=await SyntheticCandidate(f,s,false);
        var projection=await s.BeginLocalProjectionAsync(candidate,new(MinimumFreeDiskBytes:0),Ct);var file="02000000000000000000000000000000";
        // A candidate file ID alone is not the qualified exact alias required
        // by this older projection module; retain both raw events unresolved.
        await Ingest(s,100,file,f.Binding.RootFileId,2);var apply=await s.ApplyNextLocalInboxAsync(projection.ProjectionId,100,Ct);Assert.Equal(0,apply.Known);Assert.Equal(1,apply.Unresolved);
        await Ingest(s,180,file,f.Binding.RootFileId,0x80000200);await s.ApplyNextLocalInboxAsync(projection.ProjectionId,180,Ct);
        var unknown=await s.ReadLocalUnresolvedPageAsync(projection.ProjectionId,cancellationToken:Ct);Assert.Equal(2,unknown.Count);Assert.All(unknown,r=>Assert.Empty(r.Name));Assert.Null(await s.ReadLocalMetadataClaimAsync(projection.ProjectionId,180,Ct));
        Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_projection_edges"));Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_projection_unresolved WHERE substr(payload,1,4)=x'4e55534e'"));Assert.Equal("Unknown",(await s.ReadLocalProjectionAsync(projection.ProjectionId,Ct)).LocalCompleteness);
    }
    [Fact]
    public async Task ZeroJournalBoundaryRequiresObservedCutAndPreservesOwnerAndUncertaintyGuards()
    {
        await using var f=await Fixture.Create();
        var bounds=new LocalJournalBounds(Journal,0,0,0,2,4,false);
        string coherence;
        await using(var session=await f.Store.OpenLocalJournalSessionAsync(f.Binding,bounds,0,admission:null,readMode:LocalJournalReadMode.IdentityOnly,cancellationToken:Ct))
        {
            var candidate=await SyntheticCandidate(f,session,true);coherence=candidate.Item2!;
            await Assert.ThrowsAsync<InvalidOperationException>(()=>session.ApplyNextCoherenceAsync(coherence,0,Ct));
            await Internal(session,"SetCoherenceCutAsync",coherence,bounds,Ct);
            var anchored=await session.ReadCoherenceAsync(coherence,Ct);
            Assert.Equal(LocalCoherenceStatus.Anchored,anchored.Status);
            Assert.Equal(0,anchored.StartUsn);Assert.Equal(0,anchored.CutUsn);Assert.Equal(0,anchored.AppliedUsn);
            Assert.Equal("Unknown",anchored.SourceCycle);Assert.Equal("Unknown",anchored.Baseline);
            await Assert.ThrowsAsync<InvalidDataException>(()=>Internal(session,"SetCoherenceCutAsync",coherence,bounds,Ct));
            await f.Store.InspectLocalJournalWalAsync(force:true,cancellationToken:Ct);
            var empty=await session.ApplyNextCoherenceAsync(coherence,0,Ct);
            Assert.Equal(0,empty.Records);Assert.Equal(0,empty.AppliedUsn);
            await Ingest(session,0,"02000000000000000000000000000000",f.Binding.RootFileId,2);
            var dirty=await session.ApplyNextCoherenceAsync(coherence,0,Ct);
            Assert.Equal(1,dirty.Dirty);Assert.Equal(0,dirty.Unresolved);
            await Ingest(session,80,new string('f',32),new string('e',32),0x10000);
            var uncertain=await session.ApplyNextCoherenceAsync(coherence,80,Ct);
            Assert.Equal(1,uncertain.Unresolved);
            await Ingest(session,160,new string('f',32),new string('e',32),2);
            var later=await session.ApplyNextCoherenceAsync(coherence,160,Ct);
            Assert.Equal(0,later.Outside);Assert.Equal(1,later.Unresolved);
            var final=await session.ReadCoherenceAsync(coherence,Ct);
            Assert.Equal(LocalCoherenceStatus.Uncertain,final.Status);Assert.Equal(0,final.CutUsn);Assert.Equal(240,final.AppliedUsn);
        }
        var currentBounds=bounds with{NextUsn=240};
        await using var reopened=await f.Store.OpenLocalJournalSessionAsync(f.Binding,currentBounds,240,admission:null,readMode:LocalJournalReadMode.IdentityOnly,cancellationToken:Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>reopened.ApplyNextCoherenceAsync(coherence,240,Ct));
    }
    [Fact]
    public async Task IdentityOnlyCoherenceKeepsKnownDirtyOutsideAndHardlinkUncertaintyDistinct()
    {
        await using var f=await Fixture.Create();await using var s=await f.Open(LocalJournalReadMode.IdentityOnly);var (_,coherence)=await SyntheticCandidate(f,s,true);var file="02000000000000000000000000000000";
        await Internal(s,"SetCoherenceCutAsync",coherence!,new LocalJournalBounds(Journal,100,0,100,2,4,false),Ct);await s.ApplyNextCoherenceAsync(coherence!,100,Ct);
        await Ingest(s,100,file,f.Binding.RootFileId,2);var dirty=await s.ApplyNextCoherenceAsync(coherence!,100,Ct);Assert.Equal(1,dirty.Dirty);Assert.Equal(0,dirty.Unresolved);
        await Ingest(s,180,new string('f',32),new string('e',32),2);var outside=await s.ApplyNextCoherenceAsync(coherence!,180,Ct);Assert.Equal(1,outside.Outside);Assert.Equal(0,outside.Unresolved);
        await Ingest(s,260,new string('f',32),new string('e',32),0x10000);var uncertain=await s.ApplyNextCoherenceAsync(coherence!,260,Ct);Assert.Equal(1,uncertain.Unresolved);var retained=Assert.Single(await s.ReadCoherenceUnresolvedPageAsync(coherence!,cancellationToken:Ct));Assert.Equal(LocalJournalReadMode.IdentityOnly,retained.Record.ReadMode);Assert.Empty(retained.Record.Name);
        Assert.Equal(LocalCoherenceStatus.Uncertain,(await s.ReadCoherenceAsync(coherence!,Ct)).Status);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_coherence_unresolved WHERE substr(payload,1,4)=x'4e55534e'"));Assert.Equal("Unknown",(await s.ReadCoherenceAsync(coherence!,Ct)).SourceCycle);
    }
    [Fact]
    public async Task ModeMigrationBackupAdmissionIncludesAllRetainedCopiesBeforeDdl()
    {
        await using var f=await Fixture.Create();await using(var s=await f.Open()){ }
        await f.Sql("ALTER TABLE local_source_roots DROP COLUMN read_mode;UPDATE local_source_format SET version=1");await File.WriteAllBytesAsync(f.Database+".pre-owned-quota.db",new byte[2*1024*1024],Ct);
        var before=f.Hashes();await Assert.ThrowsAsync<LocalSourceGapException>(()=>f.Open(LocalJournalReadMode.IdentityOnly,new(MaximumDatabaseBytes:2*1024*1024)));Assert.Equal(before,f.Hashes());Assert.Equal(1L,await f.Scalar("SELECT version FROM local_source_format"));
    }
    [Theory][InlineData(false)][InlineData(true)]
    public async Task RefusedNamedToIdentityOnlyTransitionDurablyRetainsDiscoveredContinuityGap(bool legacyWrite)
    {
        await using var f=await Fixture.Create();await using(var s=await f.Open()){await s.CapturePageAsync((await s.ReadStateAsync(Ct)).Cursor,UsnRecordParser.Parse(Journal,100,LocalSourceContinuityTests.Frame(100,1)),Ct);}
        if(legacyWrite)await f.Store.AddMappingAsync(new("paused","client",Path.Combine(f.Source,"paused"),false,DateTimeOffset.UtcNow),Ct);
        var bounds=new LocalJournalBounds(Journal,legacyWrite?100:200,0,100000000,2,4,false);
        await Assert.ThrowsAsync<LocalSourceGapException>(()=>f.Store.OpenLocalJournalSessionAsync(f.Binding,bounds,bounds.NextUsn,null,LocalJournalReadMode.IdentityOnly,Ct));
        Assert.Equal(0L,await f.Scalar("SELECT valid FROM local_source_roots"));Assert.Equal(legacyWrite?"LegacyWrite":"JournalGap",await f.Scalar("SELECT gap_code FROM local_source_roots"));Assert.Equal(180L,await f.Scalar("SELECT cursor FROM local_source_roots"));Assert.Equal(0L,await f.Scalar("SELECT read_mode FROM local_source_roots"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_source_inbox"));
    }
    private static async Task Ingest(LocalSourceJournalSession s,long start,string file,string parent,uint reason)
    {var raw=Frame(start,1,3,reason);Convert.FromHexString(file).CopyTo(raw,16);Convert.FromHexString(parent).CopyTo(raw,32);BinaryPrimitives.WriteUInt32LittleEndian(raw.AsSpan(76),0x20);await s.CapturePageAsync((await s.ReadStateAsync(Ct)).Cursor,UsnRecordParser.ParseIdentityOnly(Journal,start,raw),Ct);}
    private static Task Internal(LocalSourceJournalSession session,string method,params object[] args)=>
        (Task)typeof(LocalSourceJournalSession).GetMethod(method,BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(session,args)!;
    private static async Task<(string Candidate,string? Coherence)> SyntheticCandidate(Fixture f,LocalSourceJournalSession s,bool withCoherence)
    {
        var h=new LocalNamespaceHandleProof(f.Binding.RootFileId,"1111111111111111","aabbccdd50532e73",0x50532e73,1,0x10,false);var root=new LocalNamespaceRootProof(f.Source,f.Binding.VolumeGuid,"NTFS",h);
        var candidate=await s.BeginNamespaceCandidateAsync(root,new(MinimumFreeDiskBytes:0),Ct);string? coherence=null;
        if(withCoherence){var begin=(Task<LocalCoherenceState>)typeof(LocalSourceJournalSession).GetMethod("BeginCoherenceAsync",BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(s,[candidate.CandidateId,new LocalCoherenceAdmission(MinimumFreeDiskBytes:0),Ct])!;coherence=(await begin).CoherenceId;}
        var parent=(await s.ClaimNamespaceDirectoryAsync(candidate.CandidateId,Ct))!;await s.RecordNamespaceHandleAsync(parent,h,Ct);if(withCoherence)await Internal(s,"RecordCoherenceParentAsync",coherence!,parent,h,Ct);
        await s.StageNamespaceEntriesAsync(parent,[new("file","02000000000000000000000000000000",1,2,3,1,0x20,0)],Ct);
        if(withCoherence){await Internal(s,"StageCoherenceAliasesAsync",coherence!,parent,(IReadOnlyList<LocalDirectoryAlias>)new[]{new LocalDirectoryAlias("file","0200000000000000",1,0x20)},Ct);await Internal(s,"CompleteCoherenceParentAsync",coherence!,parent,Ct);}
        await s.CompleteNamespaceDirectoryAsync(parent,Ct);while(await s.RefillNamespaceFrontierAsync(candidate.CandidateId,Ct)is not null){}await s.FinishNamespaceAsync(candidate.CandidateId,LocalNamespaceStatus.EnumerationComplete,cancellationToken:Ct);return(candidate.CandidateId,coherence);
    }
    private sealed class Reader(LocalSourceBinding binding):ILocalJournalReader
    {
        public LocalJournalVolumeIdentity Volume=>new(binding.VolumeGuid,binding.VolumeSerial,"NTFS");
        public ValueTask<LocalJournalBounds> QueryAsync(CancellationToken cancellationToken=default)=>throw new InvalidOperationException("Mode guard must precede native read.");
        public ValueTask<LocalJournalPage> ReadAsync(LocalJournalCursor cursor,int maximumBufferBytes=256*1024,CancellationToken cancellationToken=default)=>throw new InvalidOperationException();
        public ValueTask DisposeAsync()=>ValueTask.CompletedTask;
    }
    internal sealed class Fixture:IAsyncDisposable
    {
        private static readonly string Root=Path.GetFullPath(Environment.GetEnvironmentVariable("NEPTUNE_UNPRIVILEGED_TEST_ROOT")??Path.Combine(Path.GetTempPath(),"neptune-unpriv-owned"));
        internal string Directory=Path.Combine(Root,Guid.NewGuid().ToString("N"));internal string Source=>Path.Combine(Directory,"source");internal string Database=>Path.Combine(Directory,"state.db");internal NeptuneStateStore Store=null!;
        internal LocalSourceBinding Binding=>new("audit","client","mapping",Source,"d8481792-21dd-422b-b116-35d025404382","50532e73","NTFS",new string('1',32),1,1,"rules-v1","nfc-casefold-v1");
        internal static async Task<Fixture> Create(){var f=new Fixture();System.IO.Directory.CreateDirectory(f.Source);f.Store=new(f.Database);await f.Store.InitializeAsync(Ct);await f.Store.AddMappingAsync(new("mapping","client",f.Source,true,DateTimeOffset.UtcNow),Ct);return f;}
        internal Task<LocalSourceJournalSession> Open(LocalJournalReadMode mode=LocalJournalReadMode.Named,LocalJournalAdmission? admission=null)=>Store.OpenLocalJournalSessionAsync(Binding,new(Journal,100,0,100000000,2,4,false),100,admission,mode,Ct);
        internal async Task<SqliteConnection> Connection(){var c=new SqliteConnection("Data Source="+Database+";Cache=Private;Pooling=False");await c.OpenAsync(Ct);return c;}
        internal async Task Sql(string sql){await using var c=await Connection();await using var q=c.CreateCommand();q.CommandText=sql;await q.ExecuteNonQueryAsync(Ct);}
        internal async Task<object?> Scalar(string sql){await using var c=await Connection();await using var q=c.CreateCommand();q.CommandText=sql;return await q.ExecuteScalarAsync(Ct);}
        internal async Task<byte[]> Bytes(string sql)=>(byte[])(await Scalar(sql))!;
        internal async Task SetPayload(byte[] bytes){await using var c=await Connection();await using var q=c.CreateCommand();q.CommandText="UPDATE local_source_inbox SET payload=$raw";q.Parameters.AddWithValue("$raw",bytes);await q.ExecuteNonQueryAsync(Ct);}
        internal string Hashes()=>string.Join(';',new[]{Database,Database+"-wal"}.Select(p=>{if(!File.Exists(p))return "missing";using var s=new FileStream(p,FileMode.Open,FileAccess.Read,FileShare.ReadWrite|FileShare.Delete);return Convert.ToHexStringLower(SHA256.HashData(s));}));
        internal async Task Evidence(string name,object value){var root=Environment.GetEnvironmentVariable("NEPTUNE_UNPRIVILEGED_EVIDENCE_ROOT");if(root is null)return;System.IO.Directory.CreateDirectory(root);await File.WriteAllTextAsync(Path.Combine(root,name+"-"+Path.GetFileName(Directory)+".json"),System.Text.Json.JsonSerializer.Serialize(value,new System.Text.Json.JsonSerializerOptions{WriteIndented=true}),Ct);}
        public ValueTask DisposeAsync(){SqliteConnection.ClearAllPools();var path=Path.GetFullPath(Directory);if(!path.StartsWith(Root+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))throw new InvalidOperationException("Owned path escaped.");System.IO.Directory.Delete(path,true);return ValueTask.CompletedTask;}
    }
}
