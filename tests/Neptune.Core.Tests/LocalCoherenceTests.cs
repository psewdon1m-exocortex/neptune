using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Microsoft.Win32.SafeHandles;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("LocalCoherenceQualification")]
public sealed partial class LocalCoherenceTests
{
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    [Fact]
    public async Task ActualNativeAnchorDualAliasesAndFreshUnchangedCycleRemainSeparateFromBytes()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();
        await using var coordinator=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var session=await f.Open(coordinator);
        var state=await coordinator.BuildAnchorAsync(session,cancellationToken:Ct);
        Assert.Equal(LocalCoherenceStatus.Anchored,state.Status);Assert.Equal("Unknown",state.SourceCycle);Assert.Equal("Unknown",state.LocalCompleteness);Assert.Equal("Unknown",state.Baseline);
        Assert.Equal(1,state.MatchedEntries);Assert.Equal(1,state.VerifiedParents);Assert.Equal(0,state.DirtyNodes);Assert.True(state.CutUsn>=state.StartUsn);
        var (id,wire)=NativeFileIds(f.File);
        Assert.Equal(wire,await f.Scalar("SELECT wire64 FROM local_coherence_nodes WHERE node_id='"+id+"'"));
        var fresh=await coordinator.ObserveChangesAsync(session,state.CoherenceId,Ct);Assert.Equal(LocalCoherenceStatus.Anchored,fresh.Status);Assert.Equal(0,fresh.DirtyNodes);
        Assert.True(fresh.AppliedUsn>=state.CutUsn);Assert.Empty(await session.ReadCoherenceDirtyPageAsync(state.CoherenceId,cancellationToken:Ct));
    }
    [Fact]
    public async Task ActualDataWriteBetweenEnumerationPassesDirtiesWithoutInvalidatingNamespace()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();var changed=false;
        await using var coordinator=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct,(o,ct)=>{if(!changed&&o.Stage==LocalCoherenceStage.Parent128Enumerated){File.WriteAllText(f.File,"longer content while no capture is claimed");changed=true;}return Task.CompletedTask;});await using var session=await f.Open(coordinator);
        var state=await coordinator.BuildAnchorAsync(session,cancellationToken:Ct);Assert.True(changed);Assert.Equal(LocalCoherenceStatus.Anchored,state.Status);Assert.Equal(1,state.DirtyNodes);
        var dirty=Assert.Single(await session.ReadCoherenceDirtyPageAsync(state.CoherenceId,cancellationToken:Ct));Assert.True(dirty.Generation>0);Assert.True((dirty.Reasons&7)!=0);Assert.Equal("Unknown",state.SourceCycle);
    }
    [Fact]
    public async Task ActualOutsideChurnUsesEstablishedMembershipWhileInsideDataGenerationAdvances()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await using var coordinator=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var session=await f.Open(coordinator);
        var state=await coordinator.BuildAnchorAsync(session,cancellationToken:Ct);
        for(var i=0;i<200;i++)File.WriteAllText(Path.Combine(f.Outside,"churn-"+i),"outside");
        var outside=await coordinator.ObserveChangesAsync(session,state.CoherenceId,Ct);Assert.Equal(LocalCoherenceStatus.Anchored,outside.Status);Assert.Equal(0,outside.DirtyNodes);Assert.True(outside.OutsideRecords>=200);
        File.WriteAllText(f.File,"g1");var first=await coordinator.ObserveChangesAsync(session,state.CoherenceId,Ct);var g1=Assert.Single(await session.ReadCoherenceDirtyPageAsync(state.CoherenceId,cancellationToken:Ct));
        File.WriteAllText(f.File,"g2");var second=await coordinator.ObserveChangesAsync(session,state.CoherenceId,Ct);var g2=Assert.Single(await session.ReadCoherenceDirtyPageAsync(state.CoherenceId,cancellationToken:Ct));
        Assert.Equal(LocalCoherenceStatus.Anchored,first.Status);Assert.Equal(LocalCoherenceStatus.Anchored,second.Status);Assert.True(g2.Generation>g1.Generation);Assert.True(g2.LastUsn>g1.LastUsn);Assert.Equal(g1.NodeId128,g2.NodeId128);
        Assert.InRange(f.Store.GetLocalCoherenceMetrics().MaximumChangedRows,1,260);
        await f.Evidence("outside-cycle",new{before=state,after=outside,g1,g2,metrics=f.Store.GetLocalCoherenceMetrics()});
    }
    [Theory]
    [InlineData("create")][InlineData("rename")][InlineData("move-in")][InlineData("delete")]
    public async Task ActualNamespaceChangeAfterAnchorPreservesUncertaintyAndNeverUsesCurrentAbsence(string change)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await using var coordinator=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var session=await f.Open(coordinator);
        var state=await coordinator.BuildAnchorAsync(session,cancellationToken:Ct);
        if(change=="create")File.WriteAllText(Path.Combine(f.Source,"new"),"x");
        else if(change=="rename")File.Move(f.File,Path.Combine(f.Source,"renamed"));
        else if(change=="delete")File.Delete(f.File);
        else{Directory.CreateDirectory(Path.Combine(f.Outside,"tree"));File.WriteAllText(Path.Combine(f.Outside,"tree","leaf"),"x");Directory.Move(Path.Combine(f.Outside,"tree"),Path.Combine(f.Source,"entered"));}
        var result=await coordinator.ObserveChangesAsync(session,state.CoherenceId,Ct);Assert.Equal(LocalCoherenceStatus.Uncertain,result.Status);Assert.True(result.Unresolved>0);
        var before=result.OutsideRecords;File.WriteAllText(Path.Combine(f.Outside,"after"),"unrelated after an unresolved transition");result=await coordinator.ObserveChangesAsync(session,state.CoherenceId,Ct);Assert.Equal(before,result.OutsideRecords);Assert.Equal(LocalCoherenceStatus.Uncertain,result.Status);
        Assert.NotEmpty(await session.ReadCoherenceUnresolvedPageAsync(state.CoherenceId,cancellationToken:Ct));
    }
    [Theory]
    [InlineData("create")][InlineData("rename")]
    public async Task MutationDuringScanRefusesInsteadOfStampCertification(string change)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();var changed=false;
        await using var coordinator=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct,(o,ct)=>{if(!changed&&o.Stage==LocalCoherenceStage.Parent128Enumerated){if(change=="create")File.WriteAllText(Path.Combine(f.Source,"new"),"x");else File.Move(f.File,Path.Combine(f.Source,"new"));changed=true;}return Task.CompletedTask;});await using var session=await f.Open(coordinator);
        await Assert.ThrowsAsync<InvalidDataException>(()=>coordinator.BuildAnchorAsync(session,cancellationToken:Ct));Assert.True(changed);Assert.Equal(2L,await f.Scalar("SELECT status FROM local_coherence_roots"));
        Assert.Equal(2L,await f.Scalar("SELECT status FROM local_namespace_candidates"));
    }
    [Fact]
    public async Task NativeEmptyDirectoriesWideAndDeepHaveExactMembershipCounts()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();Directory.CreateDirectory(Path.Combine(f.Source,"empty"));var cursor=f.Source;
        for(var i=0;i<16;i++){cursor=Path.Combine(cursor,"d");Directory.CreateDirectory(cursor);}File.WriteAllText(Path.Combine(cursor,"leaf"),"x");for(var i=0;i<300;i++)File.WriteAllText(Path.Combine(f.Source,"w"+i),"x");
        await using var coordinator=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var session=await f.Open(coordinator);var state=await coordinator.BuildAnchorAsync(session,cancellationToken:Ct);
        Assert.Equal(LocalCoherenceStatus.Anchored,state.Status);Assert.Equal(319,state.MatchedEntries);Assert.Equal(18,state.VerifiedParents);Assert.Equal(0,state.DirtyNodes);Assert.InRange(f.Store.GetLocalCoherenceMetrics().MaximumChangedRows,1,260);
    }
    [Fact]
    public async Task CancellationKeepsPartialMembershipAndCannotRetirePinnedCandidate()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();using var cancel=new CancellationTokenSource();
        await using var coordinator=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct,(o,ct)=>{cancel.Cancel();ct.ThrowIfCancellationRequested();return Task.CompletedTask;});await using var session=await f.Open(coordinator);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>coordinator.BuildAnchorAsync(session,cancellationToken:cancel.Token));Assert.Equal(2L,await f.Scalar("SELECT status FROM local_coherence_roots"));
        var id=(string)(await f.Scalar("SELECT coherence_id FROM local_coherence_roots"))!;var candidate=(string)(await f.Scalar("SELECT candidate_id FROM local_coherence_roots"))!;
        await session.BeginNamespaceCandidateAsync(coordinator.RootProof,cancellationToken:Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>session.RetireNamespaceBatchAsync(candidate,Ct));await session.SupersedeCoherenceAsync(id,"explicit cancelled recovery",Ct);
        while(!(await session.RetireCoherenceBatchAsync(id,Ct)).Removed){}Assert.Equal(0L,await f.Scalar("SELECT bytes FROM local_coherence_format"));
        while(!(await session.RetireNamespaceBatchAsync(candidate,Ct)).Removed){}
    }
    [Fact]
    public async Task PausedAndLegacyWriteInvalidateMembershipWithoutChangingPublishedCheckpoints()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await using var coordinator=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);await using var session=await f.Open(coordinator);var state=await coordinator.BuildAnchorAsync(session,cancellationToken:Ct);
        await f.Store.SaveSyncUploadCheckpointAsync(new("saved","https://test/target","old-key","old-hash","old-upload"),Ct);
        await f.Store.SetMappingEnabledAsync("mapping","client",false,Ct);Assert.Equal(LocalCoherenceStatus.Uncertain,(await session.ReadCoherenceAsync(state.CoherenceId,Ct)).Status);
        await Assert.ThrowsAnyAsync<IOException>(()=>coordinator.ObserveChangesAsync(session,state.CoherenceId,Ct));Assert.Equal("old-upload",await f.Scalar("SELECT upload_id FROM sync_upload_checkpoints WHERE scope='saved'"));
    }
    [Theory]
    [InlineData("version")][InlineData("drop")][InlineData("extra")]
    public async Task FutureAndBrokenOptionalSchemaRefuseBeforeWritableInitialize(string corruption)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await using(var coordinator=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct))await using(var session=await f.Open(coordinator)){await coordinator.BuildAnchorAsync(session,cancellationToken:Ct);}
        await using var held=await f.Connection();await f.Sql(corruption=="version"?"UPDATE local_coherence_format SET version=900":corruption=="drop"?"DROP TABLE local_coherence_dirty":"CREATE TABLE local_coherence_unrecognized(x)");var hashes=f.Hashes();
        await Assert.ThrowsAsync<InvalidDataException>(()=>f.Store.InitializeAsync(Ct));Assert.Equal(hashes,f.Hashes());
    }
    [Fact]
    public async Task MigrationBackupIncludesCommittedWalPausedMappingAndResumeCheckpoint()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await f.Store.AddMappingAsync(new("paused","client",f.Source+"-paused",false,DateTimeOffset.UtcNow),Ct);await f.Store.SaveSyncUploadCheckpointAsync(new("resume","https://test/f","key","fingerprint","upload"),Ct);
        await using var held=await f.Connection();await using(var coordinator=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct))await using(var session=await f.Open(coordinator)){await coordinator.BuildAnchorAsync(session,cancellationToken:Ct);}
        var destination=Path.Combine(f.Directory,"rollback.db");await f.Store.CreateLocalCoherenceRollbackCopyAsync(destination,Ct);
        await using var c=new SqliteConnection("Data Source="+destination+";Mode=ReadOnly;Pooling=False");await c.OpenAsync(Ct);await using var q=c.CreateCommand();q.CommandText="SELECT enabled FROM sync_mappings WHERE mapping_id='paused'";Assert.Equal(0L,await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT upload_id FROM sync_upload_checkpoints WHERE scope='resume'";Assert.Equal("upload",await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT COUNT(*) FROM sqlite_master WHERE name='local_coherence_format'";Assert.Equal(0L,await q.ExecuteScalarAsync(Ct));
    }
    [Theory]
    [InlineData("length")][InlineData("next")][InlineData("zero-id")][InlineData("utf16")][InlineData("short")][InlineData("tail")]
    public void AliasParserRejectsMalformedFrameWithoutNativeIdCoercion(string failure)
    {
        var b=Frame("raw",0x1122334455667788);if(failure=="length")BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(60),511);
        else if(failure=="next")BinaryPrimitives.WriteUInt32LittleEndian(b,107);else if(failure=="zero-id")Array.Clear(b,96,8);
        else if(failure=="utf16"){b[104]=0;b[105]=0xd8;}else if(failure=="short")b[68]=25;else{b=Frame("raw",1,128);b[^1]=1;}
        Assert.Throws<InvalidDataException>(()=>WindowsDirectoryAliasParser.Parse(b));
    }
    [Fact]
    public void AliasParserKeepsRawLongNameAndRealByteOrderEvenWithShortName()
    {var b=Frame("cafe\u0301.txt",0x1122334455667788);b[68]=6;Encoding.Unicode.GetBytes("CAF").CopyTo(b,70);var a=Assert.Single(WindowsDirectoryAliasParser.Parse(b));Assert.Equal("cafe\u0301.txt",a.Name);Assert.Equal("8877665544332211",a.FileId64);}
    private static byte[] Frame(string name,ulong id,int? length=null)
    {var n=Encoding.Unicode.GetBytes(name);var b=new byte[length??(104+n.Length)];BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(60),(uint)n.Length);BinaryPrimitives.WriteUInt64LittleEndian(b.AsSpan(96),id);BinaryPrimitives.WriteInt64LittleEndian(b.AsSpan(8),1);n.CopyTo(b,104);return b;}
    private static(string Id,string Wire) NativeFileIds(string path)
    {
        using var handle=File.OpenHandle(path);var b=new byte[24];if(!Native.GetFileInformationByHandleEx(handle,18,b,24)||!Native.GetFileInformationByHandle(handle,out var old))throw new IOException("Independent owned file ID read failed.");var wire=new byte[8];BinaryPrimitives.WriteUInt64LittleEndian(wire,((ulong)old.IndexHigh<<32)|old.IndexLow);return(Convert.ToHexStringLower(b.AsSpan(8)),Convert.ToHexStringLower(wire));
    }
    [StructLayout(LayoutKind.Sequential)]private readonly record struct FileTime(uint Low,uint High);
    [StructLayout(LayoutKind.Sequential)]private readonly record struct LegacyInfo(uint Attributes,FileTime Created,FileTime Accessed,FileTime Write,uint Volume,uint SizeHigh,uint SizeLow,uint Links,uint IndexHigh,uint IndexLow);
    private static class Native
    {
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]internal static extern SafeFileHandle CreateFile(string name,uint access,uint share,IntPtr security,uint creation,uint flags,IntPtr template);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool SetFileInformationByHandle(SafeFileHandle handle,int type,byte[] input,uint size);
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool CreateHardLink(string name,string existing,IntPtr reserved);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetFileInformationByHandleEx(SafeFileHandle handle,int type,[Out]byte[] output,uint size);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetFileInformationByHandle(SafeFileHandle handle,out LegacyInfo info);
    }
    private sealed class Fixture:IAsyncDisposable
    {
        private static readonly string Root=Path.GetFullPath(Environment.GetEnvironmentVariable("NEPTUNE_LOCAL_COHERENCE_TEST_ROOT")??Path.Combine(Path.GetTempPath(),"neptune-lc3b-owned"));
        public string Directory=Path.Combine(Root,Guid.NewGuid().ToString("N"));public string Source=>Path.Combine(Directory,"source");public string Outside=>Path.Combine(Directory,"outside");public string File=>Path.Combine(Source,"file.dat");public string Database=>Path.Combine(Directory,"state.db");public NeptuneStateStore Store=null!;
        public static async Task<Fixture> Create(){var f=new Fixture();System.IO.Directory.CreateDirectory(f.Source);System.IO.Directory.CreateDirectory(f.Outside);await System.IO.File.WriteAllTextAsync(f.File,"original",Ct);f.Store=new(f.Database);await f.Store.InitializeAsync(Ct);await f.Store.AddMappingAsync(new("mapping","client",f.Source,true,DateTimeOffset.UtcNow),Ct);return f;}
        public LocalSourceBinding Binding(WindowsLocalCoherenceCoordinator c){var p=c.RootProof;return new("audit","client","mapping",Source,p.VolumeGuid,p.Handle.LegacyVolumeSerial.ToString("x8"),"NTFS",p.Handle.FileId128,p.Handle.CreationTime,1,"rules-v1","nfc-casefold-v1");}
        public async Task<LocalSourceJournalSession> Open(WindowsLocalCoherenceCoordinator c,LocalJournalAdmission? admission=null){var b=await c.QueryJournalAsync(Ct);return await Store.OpenLocalJournalSessionAsync(Binding(c),b,b.NextUsn,admission??new(MinimumFreeDiskBytes:0),Ct);}
        public async Task<SqliteConnection> Connection(){var c=new SqliteConnection("Data Source="+Database+";Pooling=False;Cache=Private");await c.OpenAsync(Ct);return c;}
        public async Task<object?> Scalar(string sql){await using var c=await Connection();await using var q=c.CreateCommand();q.CommandText=sql;return await q.ExecuteScalarAsync(Ct);}
        public async Task Sql(string sql){await using var c=await Connection();await using var q=c.CreateCommand();q.CommandText=sql;await q.ExecuteNonQueryAsync(Ct);}
        public string Hashes(){static string Hash(string p){if(!System.IO.File.Exists(p))return "missing";using var f=new FileStream(p,FileMode.Open,FileAccess.Read,FileShare.ReadWrite|FileShare.Delete);return Convert.ToHexStringLower(SHA256.HashData(f));}return Hash(Database)+Hash(Database+"-wal");}
        public async Task Evidence(string name,object value){var evidence=Environment.GetEnvironmentVariable("NEPTUNE_LOCAL_COHERENCE_EVIDENCE_ROOT");if(evidence is null)return;System.IO.Directory.CreateDirectory(evidence);await System.IO.File.WriteAllTextAsync(Path.Combine(evidence,name+"-"+Path.GetFileName(Directory)+".json"),JsonSerializer.Serialize(value,new JsonSerializerOptions{WriteIndented=true}),Ct);}
        public ValueTask DisposeAsync(){SqliteConnection.ClearAllPools();var full=Path.GetFullPath(Directory);if(!full.StartsWith(Root+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))throw new InvalidOperationException("Owned fixture escaped root.");System.IO.Directory.Delete(full,true);return ValueTask.CompletedTask;}
    }
}
