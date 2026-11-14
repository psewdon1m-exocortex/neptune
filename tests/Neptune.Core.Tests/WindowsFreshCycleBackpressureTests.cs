using System.Diagnostics;
using System.Net;
using System.Text.Json;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsFreshCycleTests
{
    private static Task<WindowsFreshSyncEngine> OpenBackpressure(Fixture f,AdmissionRemote remote,
        Func<WindowsCycleObservation,CancellationToken,Task>? observer=null)=>WindowsFreshSyncEngine.OpenAsync(
            f.Store,f.Source,Path.Combine(f.Directory,"spool"),AnchoredConfiguration(f),remote,
            new(MinimumFreeDiskBytes:0),observer:observer,cancellationToken:Ct);

    [Theory]
    [InlineData("prepare")][InlineData("append")]
    public async Task NativeOriginalRecoveryPrecedesFeedClosedByPrivateUpload(string lost)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await Fixture.Create(new Stage3Clock());File.WriteAllText(Path.Combine(f.Source,"file"),"sealed original");
        var r=new AdmissionRemote(f.Remote){LoseAppend=lost=="append"};f.Remote.LosePrepare=lost=="prepare";
        await using var e=await OpenBackpressure(f,r);var first=await e.InitialSyncAsync(Ct);
        Assert.Equal("PendingOrUncertain",first.Decision);Assert.Equal(1,f.Remote.Prepares);
        var original=JsonSerializer.Serialize(f.Remote.LastInput);var retry=await f.FreshWhenDueAsync(e,Ct);
        await f.Evidence("d1-private-feed-recovery",new{lost,first,retry,r.Attempts,r.FeedRefusals,r.Appends,e.ResourceMetrics});
        Assert.Null(retry.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",retry.Decision);
        Assert.Equal(0,retry.CapturedFiles);Assert.Equal(original,JsonSerializer.Serialize(f.Remote.LastInput));
        Assert.Equal(1,f.Remote.Prepares);Assert.Equal(1,f.Remote.Completes);Assert.Equal(1,r.Appends);
        Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory,"spool"),"*.spool"));
    }
    [Fact]
    public async Task NativeExplicitAdmissionRetriesKeepSameOriginalKeyAndSealedBytes()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());
        File.WriteAllText(Path.Combine(f.Source,"file"),"bytes");var r=new AdmissionRemote(f.Remote){RejectsRemaining=2};
        await using var e=await OpenBackpressure(f,r);var first=await e.InitialSyncAsync(Ct);Assert.NotNull(first.Error);Assert.Equal(1,r.Attempts);var second=await f.FreshWhenDueAsync(e,Ct);Assert.NotNull(second.Error);Assert.Equal(2,r.Attempts);var result=await f.FreshWhenDueAsync(e,Ct);
        await f.Evidence("d1-transient-admission",new{result,r.Attempts,r.Inputs,e.ResourceMetrics});
        Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);
        Assert.Equal(3,r.Attempts);Assert.Single(r.Inputs.Distinct());Assert.Equal(1,first.CapturedFiles);Assert.Equal(0,second.CapturedFiles);Assert.Equal(0,result.CapturedFiles);
        Assert.Equal(1,f.Remote.Prepares);Assert.Equal(1,f.Remote.Completes);
    }
    [Fact]
    public async Task NativePermanentAdmissionFailureStopsBeforeSixtyFourRetainedCapturesAndRetryRecovers()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());
        for(var i=0;i<68;i++)File.WriteAllText(Path.Combine(f.Source,"file-"+i),"bytes-"+i);
        var r=new AdmissionRemote(f.Remote){RejectsRemaining=int.MaxValue};await using var e=await OpenBackpressure(f,r);
        var timer=Stopwatch.StartNew();var first=await e.InitialSyncAsync(Ct);timer.Stop();
        await f.Evidence("d1-permanent-admission",new{first,elapsedMs=timer.ElapsedMilliseconds,r.Attempts,e.ResourceMetrics});
        Assert.Equal("PendingOrUncertain",first.Decision);Assert.InRange(first.CapturedFiles,1,4);
        Assert.InRange(Directory.GetFiles(Path.Combine(f.Directory,"spool"),"*.spool").Length,1,4);
        Assert.Equal(first.CapturedFiles,r.Attempts);Assert.True(timer.Elapsed<TimeSpan.FromSeconds(25));
        Assert.Equal(0,f.Remote.Prepares);Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE baseline IS NOT NULL"));
        var sealedKeys=r.Inputs.Distinct().ToArray();r.RejectsRemaining=0;
        var retry=await f.FreshWhenDueAsync(e,Ct);await f.Evidence("d1-admission-recovered",new{retry,r.Attempts,e.ResourceMetrics});
        Assert.Null(retry.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",retry.Decision);
        Assert.Equal(68,f.Remote.Prepares);Assert.Equal(68,f.Remote.Completes);
        Assert.All(sealedKeys,key=>Assert.Contains(key,r.Inputs));Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory,"spool"),"*.spool"));
    }
    [Fact]
    public async Task NativeCancellationJoinsBoundedDispatcherAndKeepsAttemptedOriginal()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();
        for(var i=0;i<68;i++)File.WriteAllText(Path.Combine(f.Source,"file-"+i),"bytes-"+i);
        var r=new AdmissionRemote(f.Remote){HoldPrepare=true};await using var e=await OpenBackpressure(f,r);
        using var cancellation=CancellationTokenSource.CreateLinkedTokenSource(Ct);var running=e.InitialSyncAsync(cancellation.Token);
        await r.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15),Ct);cancellation.Cancel();
        var result=await running.WaitAsync(TimeSpan.FromSeconds(8),Ct);await f.Evidence("d1-cancel-join",new{result,r.Attempts,e.ResourceMetrics});
        Assert.Equal("PendingOrUncertain",result.Decision);Assert.Contains("Cancelled",result.Error!);
        Assert.InRange(result.CapturedFiles,1,4);Assert.True((long)(await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE input_json IS NOT NULL AND capture_proof IS NOT NULL AND phase=4"))!>=1);
        Assert.InRange(Directory.GetFiles(Path.Combine(f.Directory,"spool"),"*.spool").Length,1,4);
        Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE baseline IS NOT NULL"));
    }
    [Fact]
    public async Task NativeShrinkingNewFileLimitNeverBlocksOriginalHistoricalRecovery()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"original sealed bytes");
        f.Remote.LosePrepare=true;string scope,operation,original;
        await using(var old=await f.Open()){scope=old.ScopeId;Assert.Equal("PendingOrUncertain",(await old.InitialSyncAsync(Ct)).Decision);operation=(string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE input_json IS NOT NULL"))!;original=JsonSerializer.Serialize(f.Remote.LastInput);}
        await using var e=await WindowsFreshSyncEngine.OpenAsync(f.Store,f.Source,Path.Combine(f.Directory,"spool"),AnchoredConfiguration(f),f.Remote,new(MinimumFreeDiskBytes:0,MaximumNewFileBytes:1),cancellationToken:Ct);
        var receipt=await e.RecoverOriginalWorkAsync(scope,operation,Ct);Assert.Null(receipt.Error);Assert.Equal("published",receipt.Status);
        Assert.Equal("NotAdopted",receipt.Baseline);Assert.Equal("Preserved",receipt.Dirty);Assert.Equal(original,JsonSerializer.Serialize(f.Remote.LastInput));Assert.Equal(1,f.Remote.Prepares);
        var current=await e.InitialSyncAsync(Ct);await f.Evidence("d1-limit-shrink",new{receipt,current,e.ResourceMetrics});
        Assert.Equal(0,current.CapturedFiles);Assert.Equal(1,current.Deferred);Assert.Equal(1,f.Remote.Prepares);
    }
    [Fact]
    public async Task NativeNewFileLimitDefersOnlyOversizeAndPreservesEmptyFilesAndDirectories()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();
        File.WriteAllText(Path.Combine(f.Source,"oversize"),"bytes");File.WriteAllBytes(Path.Combine(f.Source,"empty"),[]);Directory.CreateDirectory(Path.Combine(f.Source,"directory"));
        await using var e=await WindowsFreshSyncEngine.OpenAsync(f.Store,f.Source,Path.Combine(f.Directory,"spool"),AnchoredConfiguration(f),f.Remote,new(MinimumFreeDiskBytes:0,MaximumNewFileBytes:0),cancellationToken:Ct);
        var result=await e.InitialSyncAsync(Ct);Assert.Null(result.Error);Assert.Equal(1,result.Deferred);Assert.Equal(1,result.PublishedFiles);Assert.Equal(1,result.PublishedDirectories);Assert.Equal(1,result.CapturedFiles);Assert.Equal(0,result.CapturedBytes);
        Assert.Equal(1,f.Remote.Prepares);Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE phase=8 AND capture_proof IS NOT NULL"));
    }
    [Fact]
    public async Task NativeUnresolvedParentStopsFiniteWithoutCapturingBlockedChild()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());var directory=Path.Combine(f.Source,"parent");Directory.CreateDirectory(directory);File.WriteAllText(Path.Combine(directory,"child"),"bytes");
        var r=new AdmissionRemote(f.Remote){RejectFolder=true};await using var e=await OpenBackpressure(f,r);var first=await f.InitialWhenDueAsync(e,Ct).WaitAsync(TimeSpan.FromSeconds(20),Ct);
        await f.Evidence("blocked-parent-before-recovery",new{first,r.FolderAttempts,r.FolderInputs});
        Assert.Equal("PendingOrUncertain",first.Decision);Assert.Equal(0,first.CapturedFiles);Assert.Equal(0,f.Remote.Prepares);Assert.Equal(1,r.FolderAttempts);Assert.Single(r.FolderInputs.Distinct());
        Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE phase=4 AND input_json IS NOT NULL"));
        r.RejectFolder=false;var recovered=await f.FreshWhenDueAsync(e,Ct);Assert.Null(recovered.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",recovered.Decision);Assert.Equal(1,recovered.PublishedDirectories);Assert.Equal(1,recovered.PublishedFiles);
    }
    [Fact]
    public async Task NativeCancellationDuringExplicitAdmissionBackoffPreservesSameOriginal()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"bytes");
        var r=new AdmissionRemote(f.Remote){RejectsRemaining=int.MaxValue};await using var e=await OpenBackpressure(f,r);using var cancellation=CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var initial=e.InitialSyncAsync(cancellation.Token);await r.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15),Ct);cancellation.Cancel();
        var result=await initial.WaitAsync(TimeSpan.FromSeconds(8),Ct);Assert.Contains("Cancelled",result.Error!);Assert.InRange(r.Attempts,1,7);Assert.Equal(1,result.CapturedFiles);
        Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE phase=4 AND input_json IS NOT NULL AND capture_proof IS NOT NULL"));Assert.Equal(0,f.Remote.Prepares);Assert.Single(Directory.GetFiles(Path.Combine(f.Directory,"spool"),"*.spool"));
    }
    [Fact]
    public async Task NativeNewDirtyHintCannotHideActiveDeferredWriterOrClearLaterGeneration()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());var file=Path.Combine(f.Source,"file");File.WriteAllText(file,"initial");
        await using var writer=new FileStream(file,FileMode.Open,FileAccess.Write,FileShare.ReadWrite|FileShare.Delete);writer.WriteByte(41);writer.Flush(true);
        await using var e=await f.Open();var first=await e.InitialSyncAsync(Ct);Assert.Equal("PendingOrUncertain",first.Decision);Assert.Equal(0,f.Remote.Prepares);
        writer.Position=0;writer.WriteByte(42);writer.Flush(true);await Task.Delay(50,Ct);
        var later=await f.FreshWhenDueAsync(e,Ct);await f.Evidence("d1-deferred-late-hint",new{first,later,filePhase=await f.Scalar("SELECT phase FROM local_cycle_files"),workPhase=await f.Scalar("SELECT phase FROM local_cycle_work WHERE active=1")});
        Assert.Null(later.Error);Assert.Equal(1,later.PendingEntries);Assert.Equal(1,later.Deferred);Assert.Equal(0,later.CapturedFiles);Assert.Equal(0,f.Remote.Prepares);Assert.Equal(8L,await f.Scalar("SELECT phase FROM local_cycle_work WHERE active=1"));
        await writer.DisposeAsync();await e.RetryDeferredAsync(Ct);var closed=await f.FreshWhenDueAsync(e,Ct);
        Assert.Null(closed.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",closed.Decision);Assert.Equal(0,closed.PendingEntries);Assert.Equal(0,closed.Deferred);Assert.Equal(1,closed.PublishedFiles);Assert.Equal(1,f.Remote.Prepares);
        Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files f JOIN local_coherence_dirty d ON f.node_id=d.node_id WHERE f.ack_journal<d.generation"));
    }
    private sealed class AdmissionRemote(FakeRemote inner):IWindowsCycleRemote
    {
        private readonly object _gate=new();private readonly HashSet<string> _privateKeys=[];
        internal int RejectsRemaining,Attempts,FeedRefusals,Appends,FolderAttempts;internal bool LoseAppend,HoldPrepare,RejectFolder;
        internal int RejectAfterPublications;internal HttpStatusCode RejectionStatus=HttpStatusCode.TooManyRequests;internal TimeSpan? RetryAfter;
        internal readonly TaskCompletionSource Entered=new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly List<string> Inputs=[];
        internal readonly List<string> FolderInputs=[];
        public SyncRootBinding Binding=>inner.Binding;
        public Task<byte[]> StartSnapshotAsync(string id,CancellationToken ct)=>inner.StartSnapshotAsync(id,ct);
        public Task<byte[]> StepSnapshotAsync(string id,CancellationToken ct)=>inner.StepSnapshotAsync(id,ct);
        public Task<SyncSnapshotReady> ReadySnapshotAsync(string id,CancellationToken ct)=>inner.ReadySnapshotAsync(id,ct);
        public Task<SyncSnapshotEntriesPage> ReadEntriesAsync(SyncSnapshotReady r,string cursor,string? after,CancellationToken ct)=>inner.ReadEntriesAsync(r,cursor,after,ct);
        public Task<SyncFeedPage> ReadChangesAsync(string cursor,long after,CancellationToken ct)
        {lock(_gate){if(_privateKeys.Count>0){FeedRefusals++;throw new GuardedCycleRemoteException(HttpStatusCode.Conflict,"reconciliation_required");}}return inner.ReadChangesAsync(cursor,after,ct);}
        public Task<CycleFolderOutcome> EnsureDirectoryAsync(CycleFolderInput input,CancellationToken ct)
        {FolderAttempts++;FolderInputs.Add(JsonSerializer.Serialize(input));if(RejectFolder)throw new GuardedCycleRemoteException(HttpStatusCode.TooManyRequests,"admission_full");return inner.EnsureDirectoryAsync(input,ct);}
        public async Task<CycleUploadStatus> UploadAsync(string command,CycleUploadInput input,string? upload,string hash,CancellationToken ct)
        {
            if(command=="prepare")lock(_gate){Attempts++;Inputs.Add(JsonSerializer.Serialize(input));Entered.TrySetResult();if(RejectsRemaining>0&&inner.Completes>=RejectAfterPublications){RejectsRemaining--;throw new GuardedCycleRemoteException(RejectionStatus,"admission_full",true){RetryAfter=RetryAfter};}}
            try
            {
                var status=await inner.UploadAsync(command,input,upload,hash,ct);
                lock(_gate){if(status.Phase is "published" or "not_published")_privateKeys.Remove(input.Intent.IdempotencyKey);else _privateKeys.Add(input.Intent.IdempotencyKey);}
                if(command=="prepare"&&HoldPrepare){Entered.TrySetResult();await Task.Delay(Timeout.Infinite,ct);}
                return status;
            }
            catch(IOException){lock(_gate)_privateKeys.Add(input.Intent.IdempotencyKey);throw;}
        }
        public async Task<CycleUploadStatus> AppendAsync(CycleUploadInput input,string upload,string hash,long offset,ReadOnlyMemory<byte> bytes,CancellationToken ct)
        {
            Appends++;var status=await inner.AppendAsync(input,upload,hash,offset,bytes,ct);
            if(LoseAppend){LoseAppend=false;throw new IOException("lost append response after original bytes accepted");}return status;
        }
    }
}
