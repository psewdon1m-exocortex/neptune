using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsFreshCycleTests
{
    [Fact]
    public async Task ActualLostPrepareReplyReusesServerOperationInsteadOfCreatingAnother()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }var clock=new Stage3Clock();await using var f=await Fixture.Create(clock);File.WriteAllText(Path.Combine(f.Source,"file"),"bytes");f.Remote.LosePrepare=true;await using var e=await f.Open();var first=await e.InitialSyncAsync(Ct);Assert.Equal("PendingOrUncertain",first.Decision);var original=f.Remote.LastInput;var early=await e.FreshSyncAsync(Ct);Assert.Equal("PendingOrUncertain",early.Decision);Assert.Equal(1,f.Remote.Prepares);await Stage3ChatBTests.AdvancePastDue(f,clock);var after=await e.FreshSyncAsync(Ct);Assert.Null(after.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",after.Decision);Assert.Equal(1,f.Remote.Prepares);Assert.Equal(1,f.Remote.Completes);Assert.Equal(JsonSerializer.Serialize(original),JsonSerializer.Serialize(f.Remote.LastInput));Assert.Equal(0,after.CapturedFiles);
    }
    [Fact]
    public async Task ActualNewWriteAfterFirstPublishCannotBeClearedByOlderGenerationAck()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }var clock=new Stage3Clock();await using var f=await Fixture.Create(clock);var file=Path.Combine(f.Source,"file");File.WriteAllText(file,"v1");var changed=false;
        await using var e=await f.Open((o,ct)=>{if(o.Stage==WindowsCycleStage.FilePublished&&!changed){changed=true;File.WriteAllText(file,"newer version after first publication");}return Task.CompletedTask;});var first=await e.InitialSyncAsync(Ct);await f.Evidence("late-generation",new{first,changed});Assert.Null(first.Error);Assert.True(changed);Assert.Equal("PendingOrUncertain",first.Decision);Assert.Equal(1,first.CapturedFiles);Assert.Equal(1,first.PublishedFiles);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files f JOIN local_coherence_dirty d ON d.node_id=f.node_id WHERE f.ack_journal<d.generation"));await Stage3ChatBTests.AwaitHint(f);clock.Advance(TimeSpan.FromSeconds(1));var due=await e.FreshSyncAsync(Ct);Assert.Null(due.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",due.Decision);Assert.Equal(1,due.CapturedFiles);Assert.Equal(1,due.PublishedFiles);Assert.Equal(2,f.Remote.Prepares);Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE phase=5"));
        Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files f JOIN local_coherence_dirty d ON d.node_id=f.node_id WHERE f.ack_journal<d.generation"));var repeat=await e.FreshSyncAsync(Ct);Assert.Equal(0,repeat.CapturedFiles);Assert.Equal("NoWorkAtAppliedEventBoundary",repeat.Decision);
    }
    [Fact]
    public async Task ActualTransientSpoolQuotaWaitsThenBothAdmissibleLargeFilesPublish()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());File.WriteAllBytes(Path.Combine(f.Source,"a"),new byte[2*1024*1024]);File.WriteAllBytes(Path.Combine(f.Source,"b"),new byte[2*1024*1024]);WindowsFreshSyncEngine? engine=null;var first=true;var observed=false;
        await using var e=await WindowsFreshSyncEngine.OpenAsync(f.Store,f.Source,Path.Combine(f.Directory,"spool"),new("mapping","client","audit","rules-v1",new Uri("https://test.invalid/"),f.Remote.Device,"audit-token","local-test",f.Remote.Binding),f.Remote,new(DataJobs:2,LargeJobs:2,LargeFileThreshold:1024*1024,SpoolBytes:3*1024*1024,MinimumFreeDiskBytes:0),observer:async(o,ct)=>
        {
            if(o.Stage==WindowsCycleStage.FileCaptured&&first)
            {first=false;var until=DateTime.UtcNow.AddSeconds(10);while(engine!.ResourceMetrics.MaximumLargeJobs<2&&DateTime.UtcNow<until)await Task.Delay(10,ct);observed=engine.ResourceMetrics.MaximumLargeJobs==2;}
        },cancellationToken:Ct);engine=e;var result=await e.InitialSyncAsync(Ct);await f.Evidence("quota-backpressure",new{result,observed,e.ResourceMetrics});Assert.Null(result.Error);Assert.True(observed);Assert.Equal(2,result.PublishedFiles);Assert.Equal(0,result.Deferred);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(2,e.ResourceMetrics.MaximumLargeJobs);Assert.InRange(e.ResourceMetrics.MaximumSpoolBytes,2*1024*1024,3*1024*1024);
    }
    [Theory]
    [InlineData("case")][InlineData("nfc")]
    public async Task ActualCanonicalParentCollisionPublishesNoChild(string kind)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();if(kind=="case")
        {
            using var h=NativeCycle.CreateFileW(f.Source,0x40000000,3,nint.Zero,3,0x02000000|0x00200000,nint.Zero); Assert.False(h.IsInvalid);
            // FILE_CASE_SENSITIVE_INFO on this empty owned fixture only.
            Assert.True(NativeCycle.SetFileInformationByHandle(h,23,new byte[]{1,0,0,0},4));File.WriteAllText(Path.Combine(f.Source,"Name"),"a");File.WriteAllText(Path.Combine(f.Source,"NAME"),"b");
        }
        else{File.WriteAllText(Path.Combine(f.Source,"café"),"a");File.WriteAllText(Path.Combine(f.Source,"cafe\u0301"),"b");}
        await using var e=await f.Open();var result=await e.InitialSyncAsync(Ct);Assert.NotNull(result.Error);Assert.Equal("PendingOrUncertain",result.Decision);Assert.Equal(0,result.PublishedFiles);Assert.Equal(0,f.Remote.Prepares);
    }
    [Fact]
    public async Task ActualHardlinkCaptureAndRemappedRootCannotBorrowNativeProof()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();var path=Path.Combine(f.Source,"file");File.WriteAllText(path,"local");Assert.True(NativeCycle.CreateHardLinkW(Path.Combine(f.Directory,"outside-link"),path,nint.Zero));await using var e=await f.Open();var result=await e.InitialSyncAsync(Ct);Assert.Equal("PendingOrUncertain",result.Decision);Assert.Equal(0,result.PublishedFiles);Assert.Equal(0,f.Remote.Prepares);Assert.Throws<IOException>(()=>Directory.Move(f.Source,Path.Combine(f.Directory,"replacement")));
    }
    [Fact]
    public async Task ActualPausedMappingPreservesPublishedLedgerAndRefusesFurtherEffects()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"original");await using var e=await f.Open();Assert.Equal("NoWorkAtAppliedEventBoundary",(await e.InitialSyncAsync(Ct)).Decision);var before=await f.Scalar("SELECT result_json FROM local_cycle_work WHERE phase=5");await f.Store.SetMappingEnabledAsync("mapping","client",false,Ct);File.WriteAllText(Path.Combine(f.Source,"file"),"new");var after=await e.FreshSyncAsync(Ct);Assert.NotNull(after.Error);Assert.Equal("PendingOrUncertain",after.Decision);Assert.Equal(before,await f.Scalar("SELECT result_json FROM local_cycle_work WHERE phase=5"));Assert.Equal(1,f.Remote.Prepares);
    }
    [Theory]
    [InlineData("version")][InlineData("drop")][InlineData("extra")]
    public async Task CycleFutureOrBrokenModuleRefusesInitializeBeforeWritingBytes(string kind)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await using(var e=await f.Open()){await e.InitialSyncAsync(Ct);}
        await using var c=new SqliteConnection("Data Source="+f.Database+";Pooling=False");await c.OpenAsync(Ct);await using var q=c.CreateCommand();q.CommandText=kind=="version"?"PRAGMA ignore_check_constraints=ON;UPDATE local_cycle_format SET version=900":kind=="drop"?"DROP TABLE local_cycle_remote":"CREATE TABLE local_cycle_unknown(x)";await q.ExecuteNonQueryAsync(Ct);
        static string Hash(string path){if(!File.Exists(path))return "absent";using var file=new FileStream(path,FileMode.Open,FileAccess.Read,FileShare.ReadWrite|FileShare.Delete);return Convert.ToHexStringLower(SHA256.HashData(file));}
        var before=Hash(f.Database)+Hash(f.Database+"-wal");await Assert.ThrowsAsync<InvalidDataException>(()=>f.Store.InitializeAsync(Ct));Assert.Equal(before,Hash(f.Database)+Hash(f.Database+"-wal"));
    }
    [Fact]
    public async Task CycleWalAwareBackupContainsPausedMappingAndResumeCheckpointBeforeAdditiveSchema()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await f.Store.AddMappingAsync(new("paused","client",f.Source+"-paused",false,DateTimeOffset.UtcNow),Ct);await f.Store.SaveSyncUploadCheckpointAsync(new("saved","https://test.invalid/file","original-key","original-hash","original-upload"),Ct);
        await using var held=new SqliteConnection("Data Source="+f.Database+";Pooling=False");await held.OpenAsync(Ct);await using(var e=await f.Open())await e.InitialSyncAsync(Ct);
        var rollback=Path.Combine(f.Directory,"rollback.db");await f.Store.CreateLocalCycleRollbackCopyAsync(rollback,Ct);await using var copy=new SqliteConnection("Data Source="+rollback+";Mode=ReadOnly;Pooling=False");await copy.OpenAsync(Ct);await using var q=copy.CreateCommand();q.CommandText="SELECT enabled FROM sync_mappings WHERE mapping_id='paused'";Assert.Equal(0L,await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT upload_id FROM sync_upload_checkpoints WHERE scope='saved'";Assert.Equal("original-upload",await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT COUNT(*) FROM sqlite_master WHERE name='local_cycle_format'";Assert.Equal(0L,await q.ExecuteScalarAsync(Ct));
    }
    private static class NativeCycle
    {
        [System.Runtime.InteropServices.DllImport("kernel32.dll",SetLastError=true,CharSet=System.Runtime.InteropServices.CharSet.Unicode)]internal static extern Microsoft.Win32.SafeHandles.SafeFileHandle CreateFileW(string path,uint access,uint share,nint security,uint mode,uint flags,nint template);
        [System.Runtime.InteropServices.DllImport("kernel32.dll",SetLastError=true)][return:System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]internal static extern bool SetFileInformationByHandle(Microsoft.Win32.SafeHandles.SafeFileHandle file,int info,byte[] data,uint size);
        [System.Runtime.InteropServices.DllImport("kernel32.dll",SetLastError=true,CharSet=System.Runtime.InteropServices.CharSet.Unicode)][return:System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]internal static extern bool CreateHardLinkW(string link,string file,nint security);
    }
}
