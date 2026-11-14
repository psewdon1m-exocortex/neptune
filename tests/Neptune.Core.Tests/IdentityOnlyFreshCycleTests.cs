using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.Principal;
using System.Text.Json;
using Microsoft.Win32.SafeHandles;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("Native fresh cycle")]
public sealed class IdentityOnlyFreshCycleTests
{
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    private static WindowsCycleConfiguration Configuration(WindowsFreshCycleTests.Fixture f)=>new("mapping","client","audit","rules-v1",new Uri("https://test.invalid/"),f.Remote.Device,"audit-token","local-test",f.Remote.Binding);
    private static Task<WindowsFreshSyncEngine> Open(WindowsFreshCycleTests.Fixture f,bool resume=false,
        Func<WindowsCycleObservation,CancellationToken,Task>? observer=null)
        =>resume?WindowsFreshSyncEngine.ResumeOrOpenAsync(LocalJournalReadMode.IdentityOnly,f.Store,f.Source,Path.Combine(f.Directory,"spool"),Configuration(f),f.Remote,
            new(MinimumFreeDiskBytes:0),observer:observer,cancellationToken:Ct)
        :WindowsFreshSyncEngine.OpenAsync(LocalJournalReadMode.IdentityOnly,f.Store,f.Source,Path.Combine(f.Directory,"spool"),Configuration(f),f.Remote,
            new(MinimumFreeDiskBytes:0),observer:observer,cancellationToken:Ct);

    [Fact]
    public async Task ActualMediumInitialOverlapsMoreThanTwoDirectoriesThenFreshAndK1AndRestart()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());
        File.WriteAllText(Path.Combine(f.Source,"early.dat"),"early sealed bytes");
        Directory.CreateDirectory(Path.Combine(f.Source,"late-a","nested-empty"));
        Directory.CreateDirectory(Path.Combine(f.Source,"late-b"));
        File.WriteAllText(Path.Combine(f.Source,"late-a","late.dat"),"late sealed bytes");
        var publication=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);var branches=0;var beforeCut=false;var reachedBarrier=false;
        var evidence=await ReducedMedium(async()=>
        {
            if(!OperatingSystem.IsWindows())throw new PlatformNotSupportedException();var rights=CurrentRights();Assert.Equal(8192,rights.Integrity);Assert.Equal(0,rights.Elevation);Assert.False(rights.AdminEnabled);Assert.True(rights.AdminDenyOnly);
            async Task Observe(WindowsCycleObservation o,CancellationToken ct)
            {
                if(!OperatingSystem.IsWindows())throw new PlatformNotSupportedException();
                Assert.Equal(rights,CurrentRights());
                if(o.Stage==WindowsCycleStage.FilePublished&&o.RelativePath=="early.dat"&&!publication.Task.IsCompleted){beforeCut=!reachedBarrier;publication.TrySetResult();}
                if(o.Stage==WindowsCycleStage.BranchEnumerated&&++branches==3){await publication.Task.WaitAsync(TimeSpan.FromSeconds(15),ct);reachedBarrier=true;}
            }
            WindowsCycleResult initial,fresh,changed,restored;WindowsCycleResourceMetrics metrics;string scope;string? coherence;int firstPrepares;
            await using(var e=await Open(f,observer:Observe))
            {
                scope=e.ScopeId;initial=await f.InitialWhenDueAsync(e,Ct);await f.Evidence("identity-medium-initial-primary",new{rights,initial,branches,beforeCut,reachedBarrier,e.ResourceMetrics});
                Assert.Null(initial.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",initial.Decision);Assert.True(reachedBarrier);Assert.True(beforeCut);Assert.Equal(2,initial.PublishedFiles);Assert.Equal(3,initial.PublishedDirectories);Assert.Equal(2,initial.CapturedFiles);Assert.True(branches>=8);coherence=initial.CoherenceId;
                var branchCount=branches;firstPrepares=f.Remote.Prepares;fresh=await f.FreshWhenDueAsync(e,Ct);
                Assert.Null(fresh.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",fresh.Decision);Assert.Equal(0,fresh.CapturedFiles);Assert.Equal(0,fresh.CapturedBytes);Assert.Equal(0,fresh.PublishedFiles);Assert.Equal(branchCount,branches);Assert.Equal(firstPrepares,f.Remote.Prepares);
                File.WriteAllText(Path.Combine(f.Source,"early.dat"),"one closed changed source generation");changed=await f.FreshWhenDueAsync(e,Ct);await f.Evidence("identity-medium-k1-primary",new{changed,e.ResourceMetrics});
                Assert.Null(changed.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",changed.Decision);Assert.Equal(1,changed.CapturedFiles);Assert.Equal(1,changed.PublishedFiles);Assert.Equal("match",f.Remote.LastInput!.Intent.Condition.Kind);Assert.Equal(branchCount,branches);Assert.Equal(firstPrepares+1,f.Remote.Prepares);
                metrics=e.ResourceMetrics;Assert.InRange(metrics.MaximumDataJobs,1,4);Assert.InRange(metrics.MaximumLargeJobs,0,2);Assert.InRange(metrics.MaximumSourceReaders,1,1);Assert.InRange(metrics.MaximumReadyQueue,0,32);Assert.InRange(metrics.MaximumSpoolBytes,0,1L<<30);
            }
            var preparesBefore=f.Remote.Prepares;var branchBefore=branches;
            await using(var e=await Open(f,true,Observe))
            {
                Assert.True(e.HasRetainedAnchor);Assert.Equal(scope,e.ScopeId);restored=await f.FreshWhenDueAsync(e,Ct);await f.Evidence("identity-medium-restored-primary",restored);
                Assert.Null(restored.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",restored.Decision);Assert.Equal(coherence,restored.CoherenceId);Assert.Equal(0,restored.CapturedFiles);Assert.Equal(0,restored.CapturedBytes);Assert.Equal(0,restored.PublishedFiles);Assert.Equal(branchBefore,branches);Assert.Equal(preparesBefore,f.Remote.Prepares);
            }
            Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_source_roots WHERE read_mode=1"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_scopes"));
            return new{rights,initial,fresh,changed,restored,scope,coherence,metrics,branches,publicationBeforeFinalEnumeration=beforeCut,ordinaryLinkedUser="UNVERIFIED",remote="Injected qualified-shape remote; actual Saturn interop remains a separate gate."};
        });await f.Evidence("identity-medium-cycle",evidence);
    }

    [Fact]
    public async Task ActualMediumLostCompleteRestartKeepsOriginalKeyUploadAndBytes()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());File.WriteAllText(Path.Combine(f.Source,"file.dat"),"sealed immutable bytes");f.Remote.LoseComplete=true;
        var result=await ReducedMedium(async()=>
        {
            string scope;WindowsCycleResult first,recovered;string original;
            await using(var e=await Open(f)){scope=e.ScopeId;first=await e.InitialSyncAsync(Ct);Assert.Equal("PendingOrUncertain",first.Decision);Assert.Equal(1,f.Remote.Prepares);Assert.Equal(1,f.Remote.Completes);original=JsonSerializer.Serialize(f.Remote.LastInput);}
            await using(var e=await Open(f,true)){Assert.Equal(scope,e.ScopeId);Assert.True(e.HasRetainedAnchor);recovered=await f.FreshWhenDueAsync(e,Ct);await f.Evidence("identity-medium-lost-reply-primary",new{first,recovered});Assert.Null(recovered.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",recovered.Decision);Assert.Equal(0,recovered.CapturedFiles);Assert.Equal(original,JsonSerializer.Serialize(f.Remote.LastInput));Assert.Equal(1,f.Remote.Prepares);Assert.Equal(1,f.Remote.Completes);}
            return new{first,recovered,scope,originalPrepareCount=f.Remote.Prepares,originalCompleteCount=f.Remote.Completes};
        });await f.Evidence("identity-medium-lost-reply",result);
    }

    [Fact]
    public async Task ActualMediumUnknownNamelessCreateRemainsUncertainWithoutDeletionOrAdoption()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"known"),"known");
        await ReducedMedium(async()=>
        {
            await using var e=await Open(f);var initial=await e.InitialSyncAsync(Ct);Assert.Null(initial.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",initial.Decision);var count=f.Remote.Count;var prepares=f.Remote.Prepares;
            Directory.CreateDirectory(Path.Combine(f.Source,"new-branch"));File.WriteAllText(Path.Combine(f.Source,"new-branch","unknown"),"new");var result=await e.FreshSyncAsync(Ct);await f.Evidence("identity-medium-unknown-namespace",result);
            Assert.Equal("PendingOrUncertain",result.Decision);Assert.NotNull(result.Error);Assert.Equal(count,f.Remote.Count);Assert.Equal(prepares,f.Remote.Prepares);Assert.Equal(0,result.CapturedFiles);return true;
        });
    }

    [Fact]
    public async Task ActualMediumPreExistingWriterStillDefersThenClosedRetryPreservesDirty()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());File.WriteAllText(Path.Combine(f.Source,"held"),"old");
        await ReducedMedium(async()=>
        {
            var writer=new FileStream(Path.Combine(f.Source,"held"),FileMode.Open,FileAccess.Write,FileShare.ReadWrite|FileShare.Delete);writer.WriteByte(42);writer.Flush(true);
            await using var e=await Open(f);var first=await e.InitialSyncAsync(Ct);await f.Evidence("identity-medium-writer-primary",first);Assert.Equal("PendingOrUncertain",first.Decision);Assert.True(first.Deferred>=1);Assert.Equal(0,first.PublishedFiles);
            await writer.DisposeAsync();await e.RetryDeferredAsync(Ct);var after=await f.FreshWhenDueAsync(e,Ct);await f.Evidence("identity-medium-writer-closed",after);Assert.Null(after.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",after.Decision);Assert.Equal(1,after.PublishedFiles);return true;
        });
    }

    [Fact]
    public async Task DefaultNamedDoesNotSilentlySelectIdentityOnlyWithoutDasdRights()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();
        var failure=await ReducedMedium(async()=>{try{await using var e=await f.Open();throw new Xunit.Sdk.XunitException("Default Named unexpectedly opened without DASD rights.");}catch(LocalSourceGapException error){var native=Assert.IsType<Win32Exception>(error.InnerException);Assert.Equal(5,native.NativeErrorCode);return new{error.Code,native.NativeErrorCode,error.Message,selectedMode="Named"};}});
        Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM sqlite_master WHERE name='local_source_roots'"));await f.Evidence("identity-medium-default-named-refusal",failure);
    }

    [Fact]
    public async Task InvalidExplicitModeRefusesBeforeModuleOrSourceOpen()
    {
        await using var f=await WindowsFreshCycleTests.Fixture.Create();var error=await Assert.ThrowsAsync<LocalSourceGapException>(()=>WindowsFreshSyncEngine.OpenAsync((LocalJournalReadMode)99,f.Store,f.Source,Path.Combine(f.Directory,"spool"),Configuration(f),f.Remote,cancellationToken:Ct));Assert.Equal("UnsupportedReadMode",error.Code);
        Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM sqlite_master WHERE name='local_source_roots'"));
    }

    [Fact]
    public async Task ActualNamedToExplicitMediumResumePreservesSameScopeAndConfirmedBaseline()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file.dat"),"original named closure");string scope;string? coherence;WindowsCycleResult named;
        await using(var e=await f.Open()){scope=e.ScopeId;named=await e.InitialSyncAsync(Ct);Assert.Null(named.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",named.Decision);coherence=named.CoherenceId;}
        var baselineBefore=(string)(await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"))!;var prepares=f.Remote.Prepares;
        var medium=await ReducedMedium(async()=>
        {
            await using var e=await Open(f,true);Assert.Equal(scope,e.ScopeId);Assert.True(e.HasRetainedAnchor);var result=await e.FreshSyncAsync(Ct);await f.Evidence("identity-medium-original-named-resume-primary",result);Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(coherence,result.CoherenceId);Assert.Equal(0,result.CapturedFiles);Assert.Equal(0,result.PublishedFiles);Assert.Equal(prepares,f.Remote.Prepares);return result;
        });
        Assert.Equal(baselineBefore,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_source_roots WHERE read_mode=1 AND valid=1"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_scopes"));await f.Evidence("identity-medium-original-named-resume",new{named,medium,scope,coherence,baselinePreserved=true,noSilentModeSelection=true});
    }

    [SupportedOSPlatform("windows")]
    private static async Task<T> ReducedMedium<T>(Func<Task<T>> action)
    {
        var before=CurrentRights();using var current=WindowsIdentity.GetCurrent();Check(Native.CreateRestrictedToken(current.AccessToken,5,0,nint.Zero,0,nint.Zero,0,nint.Zero,out var token));using(token)
        {
            Check(Native.ConvertStringSidToSidW("S-1-16-8192",out var sid));try{var label=new Label{Sid=sid,Attributes=0x20};Check(Native.SetTokenInformation(token,25,ref label,checked(Marshal.SizeOf<Label>()+(int)Native.GetLengthSid(sid))));}finally{Native.LocalFree(sid);}
            var value=await WindowsIdentity.RunImpersonatedAsync(token,action);Assert.Equal(before,CurrentRights());return value;
        }
    }
    [SupportedOSPlatform("windows")]
    private static TokenRights CurrentRights()
    {
        using var current=WindowsIdentity.GetCurrent();var token=current.AccessToken;
        int Value(int kind){var p=Info(token,kind);try{return Marshal.ReadInt32(p);}finally{Marshal.FreeHGlobal(p);}}
        var integrity=Info(token,25);int rid;try{var sid=Marshal.ReadIntPtr(integrity);var count=Marshal.ReadByte(Native.GetSidSubAuthorityCount(sid));rid=Marshal.ReadInt32(Native.GetSidSubAuthority(sid,(uint)(count-1)));}finally{Marshal.FreeHGlobal(integrity);}
        var groups=Info(token,2);bool enabled=false,denied=false;try{var count=Marshal.ReadInt32(groups);if(count<0||count>512)throw new InvalidDataException();for(var i=0;i<count;i++){var p=nint.Add(groups,8+i*16);if(new SecurityIdentifier(Marshal.ReadIntPtr(p)).Value=="S-1-5-32-544"){var bits=(uint)Marshal.ReadInt32(p,8);enabled=(bits&4)!=0;denied=(bits&16)!=0;}}}finally{Marshal.FreeHGlobal(groups);}return new(Value(20),rid,enabled,denied);
    }
    private static nint Info(SafeAccessTokenHandle token,int kind){Native.GetTokenInformation(token,kind,nint.Zero,0,out var size);if(size<1||size>65536)throw new Win32Exception(Marshal.GetLastWin32Error());var p=Marshal.AllocHGlobal(size);if(!Native.GetTokenInformation(token,kind,p,size,out var actual)||actual>size){var error=Marshal.GetLastWin32Error();Marshal.FreeHGlobal(p);throw new Win32Exception(error);}return p;}
    private static void Check(bool value){if(!value)throw new Win32Exception(Marshal.GetLastWin32Error());}
    private sealed record TokenRights(int Elevation,int Integrity,bool AdminEnabled,bool AdminDenyOnly);
    [StructLayout(LayoutKind.Sequential)]private struct Label{public nint Sid;public uint Attributes;}
    private static class Native
    {
        [DllImport("advapi32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool CreateRestrictedToken(SafeAccessTokenHandle original,uint flags,uint sidCount,nint sids,uint privilegeCount,nint privileges,uint restrictedCount,nint restricted,out SafeAccessTokenHandle result);
        [DllImport("advapi32.dll",CharSet=CharSet.Unicode,SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool ConvertStringSidToSidW(string text,out nint sid);
        [DllImport("advapi32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool SetTokenInformation(SafeAccessTokenHandle token,int kind,ref Label label,int size);
        [DllImport("advapi32.dll")]internal static extern uint GetLengthSid(nint sid);
        [DllImport("kernel32.dll")]internal static extern nint LocalFree(nint memory);
        [DllImport("advapi32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetTokenInformation(SafeAccessTokenHandle token,int kind,nint buffer,int size,out int returned);
        [DllImport("advapi32.dll")]internal static extern nint GetSidSubAuthorityCount(nint sid);
        [DllImport("advapi32.dll")]internal static extern nint GetSidSubAuthority(nint sid,uint index);
    }
}
