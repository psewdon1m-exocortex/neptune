using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;

[CollectionDefinition("Native fresh cycle",DisableParallelization=true)]public sealed class NativeFreshCycleCollection;
[Collection("Native fresh cycle")]
public sealed partial class WindowsFreshCycleTests
{
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    [Fact]
    public async Task ActualClosedNtfsInitialThenFreshZeroReadsAndOneChangedFileUsesOriginalCas()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());File.WriteAllText(Path.Combine(f.Source,"first.dat"),"first");Directory.CreateDirectory(Path.Combine(f.Source,"empty"));Directory.CreateDirectory(Path.Combine(f.Source,"nested"));File.WriteAllText(Path.Combine(f.Source,"nested","leaf"),"leaf");
        var branches=0;await using var e=await f.Open((o,ct)=>{if(o.Stage==WindowsCycleStage.BranchEnumerated)branches++;return Task.CompletedTask;});
        var first=await f.InitialWhenDueAsync(e,Ct);await f.Evidence("initial",new{first,e.ResourceMetrics});Assert.Null(first.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",first.Decision);Assert.Equal(2,first.PublishedFiles);Assert.Equal(2,first.PublishedDirectories);Assert.Equal(2,first.CapturedFiles);
        var branchBefore=branches;var prepares=f.Remote.Prepares;var repeat=await f.FreshWhenDueAsync(e,Ct);await f.Evidence("no-change",new{repeat,e.ResourceMetrics});Assert.Null(repeat.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",repeat.Decision);Assert.Equal(0,repeat.CapturedFiles);Assert.Equal(0,repeat.CapturedBytes);Assert.Equal(0,repeat.PublishedFiles);Assert.Equal(branchBefore,branches);Assert.Equal(prepares,f.Remote.Prepares);
        File.WriteAllText(Path.Combine(f.Source,"first.dat"),"changed same existing file");var changed=await f.FreshWhenDueAsync(e,Ct);await f.Evidence("one-dirty",new{changed,e.ResourceMetrics});Assert.Null(changed.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",changed.Decision);Assert.Equal(1,changed.CapturedFiles);Assert.Equal(1,changed.PublishedFiles);Assert.Equal(prepares+1,f.Remote.Prepares);Assert.Equal("match",f.Remote.LastInput!.Intent.Condition.Kind);Assert.Equal(branchBefore,branches);Assert.InRange(e.ResourceMetrics.MaximumDataJobs,1,4);Assert.InRange(e.ResourceMetrics.MaximumSourceReaders,1,1);Assert.InRange(e.ResourceMetrics.MaximumReadyQueue,0,32);
    }
    [Fact]
    public async Task ActualPublicationStartsBeforeLateBranchEnumerationFinishes()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"early.dat"),"early");Directory.CreateDirectory(Path.Combine(f.Source,"late"));File.WriteAllText(Path.Combine(f.Source,"late","later"),"later");
        var published=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);var branches=0;var waited=false;
        await using var e=await f.Open(async(o,ct)=>{if(o.Stage==WindowsCycleStage.FilePublished&&o.RelativePath=="early.dat")published.TrySetResult();if(o.Stage==WindowsCycleStage.BranchEnumerated&&++branches==3){await published.Task.WaitAsync(TimeSpan.FromSeconds(15),ct);waited=true;}});
        var result=await f.InitialWhenDueAsync(e,Ct);await f.Evidence("overlap",new{result,waited,branches,e.ResourceMetrics});Assert.Null(result.Error);Assert.True(waited);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);
    }
    [Fact]
    public async Task ActualWriterHeldBeforeScanIsDeferredThenCloseRetriedWithoutLostDirty()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());var file=Path.Combine(f.Source,"held");File.WriteAllText(file,"old");var held=new FileStream(file,FileMode.Open,FileAccess.Write,FileShare.ReadWrite|FileShare.Delete);held.WriteByte(42);held.Flush(true);
        await using var e=await f.Open();var first=await e.InitialSyncAsync(Ct);await f.Evidence("open-writer",first);Assert.Equal("PendingOrUncertain",first.Decision);Assert.True(first.Deferred>=1);Assert.Equal(0,first.PublishedFiles);await held.DisposeAsync();await e.RetryDeferredAsync(Ct);var after=await f.FreshWhenDueAsync(e,Ct);await f.Evidence("closed-writer",after);Assert.Null(after.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",after.Decision);Assert.Equal(1,after.PublishedFiles);
    }
    [Fact]
    public async Task ActualLostCompleteResponseKeepsSameUploadAndKeyForNextFreshRecovery()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());File.WriteAllText(Path.Combine(f.Source,"file"),"sealed");f.Remote.LoseComplete=true;await using var e=await f.Open();var first=await e.InitialSyncAsync(Ct);Assert.Equal("PendingOrUncertain",first.Decision);Assert.Equal(1,f.Remote.Prepares);Assert.Equal(1,f.Remote.Completes);var original=f.Remote.LastInput;
        var retry=await f.FreshWhenDueAsync(e,Ct);await f.Evidence("lost-complete",new{first,retry,f.Remote.Prepares,f.Remote.Completes});Assert.Null(retry.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",retry.Decision);Assert.Equal(JsonSerializer.Serialize(original),JsonSerializer.Serialize(f.Remote.LastInput));Assert.Equal(1,f.Remote.Prepares);Assert.Equal(1,f.Remote.Completes);Assert.Equal(0,retry.CapturedFiles);
    }
    [Fact]
    public async Task ActualPostPublicationCallbackFailureNeverDowngradesReceiptOrWorkerJoin()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"sealed");await using var e=await f.Open((o,ct)=>o.Stage==WindowsCycleStage.FilePublished?Task.FromException(new IOException("owned callback failure")):Task.CompletedTask);var result=await e.InitialSyncAsync(Ct);Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(1,result.PublishedFiles);Assert.True(e.ResourceMetrics.PostCommitDiagnostics>=1);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE phase=5 AND result_json IS NOT NULL"));
    }
    [Fact]
    public async Task ActualUnknownRemoteContentAndNewSourceBranchAreRefusedWithoutDeletion()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"local");f.Remote.AddExisting("file","remote");await using var e=await f.Open();var result=await e.InitialSyncAsync(Ct);Assert.Null(result.Error);Assert.Equal(1,result.Conflicts);Assert.Equal(0,result.PublishedFiles);Assert.Equal(0,f.Remote.Prepares);var before=f.Remote.Count;Directory.CreateDirectory(Path.Combine(f.Source,"new-branch"));var later=await e.FreshSyncAsync(Ct);Assert.Equal("PendingOrUncertain",later.Decision);Assert.NotNull(later.Error);Assert.Equal(before,f.Remote.Count);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE phase=2"));
    }
    internal sealed class Fixture:IAsyncDisposable
    {
        private static readonly string Root=Path.GetFullPath(Environment.GetEnvironmentVariable("NEPTUNE_FRESH_CYCLE_TEST_ROOT")??Path.Combine(Path.GetTempPath(),"neptune-lc3c-owned"));
        internal readonly string Directory=Path.Combine(Root,Guid.NewGuid().ToString("N"));internal string Source=>Path.Combine(Directory,"source");internal string Database=>Path.Combine(Directory,"state.db");internal NeptuneStateStore Store=null!;internal FakeRemote Remote=new();
        internal static async Task<Fixture> Create(TimeProvider? clock=null){var f=new Fixture();System.IO.Directory.CreateDirectory(f.Source);f.Store=new(f.Database,clock){ResourceScheduler=new(clock)};await f.Store.InitializeAsync(Ct);await f.Store.AddMappingAsync(new("mapping","client",f.Source,true,DateTimeOffset.UtcNow),Ct);return f;}
        internal Task<WindowsFreshSyncEngine> Open(Func<WindowsCycleObservation,CancellationToken,Task>? observer=null)=>WindowsFreshSyncEngine.OpenAsync(Store,Source,Path.Combine(Directory,"spool"),new("mapping","client","audit","rules-v1",new Uri("https://test.invalid/"),Remote.Device,"audit-token","local-test",Remote.Binding),Remote,new(MinimumFreeDiskBytes:0),observer:observer,cancellationToken:Ct);
        // Advance only the fixture clock, after the native watcher has had a
        // chance to persist its hints. Never clear production retry/quiet rows.
        internal async Task WaitDueAsync()
        {
            await Task.Delay(150, Ct);
            var due = Convert.ToInt64(await Scalar("SELECT MAX(due) FROM (SELECT COALESCE(MAX(not_before),0) AS due FROM client_operation_schedule UNION ALL SELECT COALESCE(MAX(due_at),0) FROM client_event_inbox)"));
            var milliseconds = Math.Max(0, due - Store.Clock.GetUtcNow().ToUnixTimeMilliseconds() + 1);
            Assert.InRange(milliseconds, 0, 300001);
            if (Store.Clock is Stage3Clock clock) clock.Advance(TimeSpan.FromMilliseconds(milliseconds));
            else if (milliseconds > 0) await Task.Delay(TimeSpan.FromMilliseconds(milliseconds), Ct);
        }
        internal async Task<WindowsCycleResult> FreshWhenDueAsync(WindowsFreshSyncEngine engine, CancellationToken ct)
        {
            await WaitDueAsync();
            return await FinishQuietBoundaryAsync(engine, await engine.FreshSyncAsync(ct), ct);
        }
        internal async Task<WindowsCycleResult> InitialWhenDueAsync(WindowsFreshSyncEngine engine, CancellationToken ct)
            => await FinishQuietBoundaryAsync(engine, await engine.InitialSyncAsync(ct), ct);
        private async Task<WindowsCycleResult> FinishQuietBoundaryAsync(WindowsFreshSyncEngine engine, WindowsCycleResult result, CancellationToken ct)
        {
            // A native hint delivered during capture is deliberately deferred
            // until its quiet deadline. Join at most three such boundaries;
            // real errors/conflicts/admission failures are never retried here.
            for (var n=0; n<3 && result.Decision=="PendingOrUncertain" && result.Error is null && result.Conflicts==0 && result.Deferred==0; n++)
            {
                if (Convert.ToInt64(await Scalar("SELECT COUNT(*) FROM client_event_inbox"))==0) break;
                await WaitDueAsync();
                var next=await engine.FreshSyncAsync(ct);
                result=next with { CapturedFiles=result.CapturedFiles+next.CapturedFiles, CapturedBytes=result.CapturedBytes+next.CapturedBytes, ReceivedBytes=result.ReceivedBytes+next.ReceivedBytes, PublishedFiles=result.PublishedFiles+next.PublishedFiles, PublishedDirectories=result.PublishedDirectories+next.PublishedDirectories };
            }
            return result;
        }
        internal async Task<object?> Scalar(string sql){await using var c=new SqliteConnection("Data Source="+Database+";Cache=Private;Pooling=False");await c.OpenAsync(Ct);await using var q=c.CreateCommand();q.CommandText=sql;return await q.ExecuteScalarAsync(Ct);}
        internal async Task Evidence(string name,object value){var root=Environment.GetEnvironmentVariable("NEPTUNE_FRESH_CYCLE_EVIDENCE_ROOT");if(root is null)return;System.IO.Directory.CreateDirectory(root);await File.WriteAllTextAsync(Path.Combine(root,name+"-"+Path.GetFileName(Directory)+".json"),JsonSerializer.Serialize(value,new JsonSerializerOptions{WriteIndented=true}),Ct);}
        private static void ForgetOwnedSpoolReservations(string full)
        {
            // Deferred captures can reserve bytes before a physical spool is
            // created. Test teardown must release every reservation belonging
            // to this exact disposable fixture, including those absent files.
            var gate = typeof(WindowsCycleSharedBudget).GetField("Gate",System.Reflection.BindingFlags.Static|System.Reflection.BindingFlags.NonPublic)!.GetValue(null)!;
            lock(gate)
            {
                var files = (Dictionary<string,long>)typeof(WindowsCycleSharedBudget).GetField("Files",System.Reflection.BindingFlags.Static|System.Reflection.BindingFlags.NonPublic)!.GetValue(null)!;
                foreach(var spool in files.Keys.Where(value=>Path.GetFullPath(value).StartsWith(full+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase)).ToArray())
                    WindowsCycleSharedBudget.Forget(spool);
            }
        }
        public ValueTask DisposeAsync(){SqliteConnection.ClearAllPools();var full=Path.GetFullPath(Directory);if(!full.StartsWith(Root+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))throw new InvalidOperationException("Owned fixture escaped.");ForgetOwnedSpoolReservations(full);System.IO.Directory.Delete(full,true);return ValueTask.CompletedTask;}
    }
    internal sealed partial class FakeRemote:IWindowsCycleRemote
    {
        private readonly object _gate=new();private readonly Dictionary<string,SyncDescriptor> _nodes=new();private readonly Dictionary<string,(CycleUploadInput Input,CycleUploadStatus Status)> _uploads=new();private readonly Dictionary<string,string> _keys=new();private readonly List<SyncFeedItem> _events=new();private readonly Dictionary<string,CycleFolderOutcome> _folders=new();private IReadOnlyList<SyncDescriptor> _snapshot=[];private long _cut;
        internal readonly string Device=Guid.NewGuid().ToString("D");public SyncRootBinding Binding{get;private set;}=new("neptune-sync/1","saturn.current-namespace.v1","test-server",Guid.NewGuid().ToString("D"),Guid.NewGuid().ToString("D"),"1",Guid.NewGuid().ToString("D"));
        internal int Prepares,Completes;internal bool LoseComplete,LosePrepare,CancelComplete;internal CycleUploadInput? LastInput;internal int Count{get{lock(_gate)return _nodes.Count;}}
        internal string AddFolder(string name,string? parent=null){var id=Guid.NewGuid().ToString("D");_nodes.Add(id,new(id,parent??Binding.RootId,name,"folder","0",null,null,Guid.NewGuid().ToString("D"),"AVAILABLE"));return id;}
        internal string AddAnchorFile(){var id=Guid.NewGuid().ToString("D");_nodes.Add(id,new(id,Binding.RootId,"file-anchor","file","0",new string('a',64),Guid.NewGuid().ToString("D"),Guid.NewGuid().ToString("D"),"AVAILABLE"));return id;}
        internal void RenameFolder(string id,string name){lock(_gate){var d=_nodes[id] with{Name=name,Version=Guid.NewGuid().ToString("D")};_nodes[id]=d;Event(d,Guid.NewGuid().ToString("D"));}}
        internal FakeRemote(){_nodes.Add(Binding.RootId,new(Binding.RootId,null,"root","folder","0",null,null,Guid.NewGuid().ToString("D"),"AVAILABLE"));}
        internal void AddExisting(string name,string text){var d=new SyncDescriptor(Guid.NewGuid().ToString("D"),Binding.RootId,name,"file",text.Length.ToString(CultureInfo.InvariantCulture),Convert.ToHexStringLower(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(text))),Guid.NewGuid().ToString("D"),Guid.NewGuid().ToString("D"),"AVAILABLE");_nodes.Add(d.NodeId,d);}
        public Task<byte[]> StartSnapshotAsync(string id,CancellationToken ct){lock(_gate){_snapshot=_nodes.Values.OrderBy(d=>d.NodeId,StringComparer.Ordinal).ToArray();_cut=_events.Count;return Task.FromResult(JsonSerializer.SerializeToUtf8Bytes(new{snapshotId=id,state="READY",startSeq=_cut.ToString(),appliedSeq=_cut.ToString(),cutSeq=_cut.ToString(),entries=_snapshot.Count,documentBytes=0}));}}
        public Task<byte[]> StepSnapshotAsync(string id,CancellationToken ct)=>StartSnapshotAsync(id,ct);
        public Task<SyncSnapshotReady> ReadySnapshotAsync(string id,CancellationToken ct)=>Task.FromResult(new SyncSnapshotReady(Binding,id,_cut.ToString(),"entries","changes",60,3600));
        public Task<SyncSnapshotEntriesPage> ReadEntriesAsync(SyncSnapshotReady r,string cursor,string? after,CancellationToken ct)=>Task.FromResult(new SyncSnapshotEntriesPage(Binding,r.SnapshotId,r.CutSeq,after,_snapshot.LastOrDefault()?.NodeId,_snapshot,"entries-done",false));
        public Task<SyncFeedPage> ReadChangesAsync(string cursor,long after,CancellationToken ct){lock(_gate){var items=_events.Skip((int)after).Take(500).ToArray();var to=after+items.Length;return Task.FromResult(new SyncFeedPage(Binding,after.ToString(),to.ToString(),items,"changes-"+to,to<_events.Count));}}
        public Task<CycleFolderOutcome> EnsureDirectoryAsync(CycleFolderInput i,CancellationToken ct)
        {
            lock(_gate){if(_folders.TryGetValue(i.OperationId,out var old))return Task.FromResult(old);var d=_nodes.Values.SingleOrDefault(d=>d.ParentId==i.ParentId&&d.Name==i.Name);var created=d is null;d??=new(Guid.NewGuid().ToString("D"),i.ParentId,i.Name,"folder","0",null,null,Guid.NewGuid().ToString("D"),"AVAILABLE");if(created){_nodes.Add(d.NodeId,d);Event(d,i.OperationId);}var hash=new string('b',64);var raw=JsonSerializer.Serialize(new{replayed=false,result=new{schema="saturn.catalog-folder-result.v2",operationId=i.OperationId,intentHash=hash,rootId=Binding.RootId,status=created?"CREATED":"EXISTING",description=Wire(d)}});var outcome=new CycleFolderOutcome(d,raw,hash);_folders.Add(i.OperationId,outcome);return Task.FromResult(outcome);}
        }
        public Task<CycleUploadStatus> UploadAsync(string command,CycleUploadInput i,string? upload,string hash,CancellationToken ct)
        {
            lock(_gate)
            {
                LastInput=i;if(command=="prepare")
                {if(_keys.TryGetValue(i.Intent.IdempotencyKey,out var existing))return Task.FromResult(_uploads[existing].Status with{Replayed=true});Prepares++;var id=Guid.NewGuid().ToString("D");var status=new CycleUploadStatus(id,id,Binding.RootId,hash,long.Parse(i.Intent.SizeBytes),0,"transport",true,false,null,null,null,null);_keys.Add(i.Intent.IdempotencyKey,id);_uploads.Add(id,(i,status));if(LosePrepare){LosePrepare=false;throw new IOException("lost prepare reply after durable operation");}return Task.FromResult(status);}
                var current=_uploads[upload!];if(command=="inspect"||current.Status.Phase=="published")return Task.FromResult(current.Status with{Replayed=true});Completes++;
                if(CancelComplete){CancelComplete=false;var stopped=current.Status with{Phase="not_published",TransferPending=false,ResultJson=JsonSerializer.Serialize(new{schema="saturn.catalog-upload-result.v2",status="NOT_PUBLISHED",uploadId=upload,operationId=upload,rootId=Binding.RootId,intentHash=hash,reason="cancelled_before_publication"})};_uploads[upload!]=(i,stopped);return Task.FromResult(stopped);}
                var d=new SyncDescriptor(i.Intent.TargetId??Guid.NewGuid().ToString("D"),i.Intent.ParentId,i.Intent.Name,"file",i.Intent.SizeBytes,i.Intent.Sha256,Guid.NewGuid().ToString("D"),Guid.NewGuid().ToString("D"),"AVAILABLE");if(i.Intent.Condition.Kind=="match"&&_nodes[d.NodeId].Version!=i.Intent.Condition.MetadataVersion)throw new IOException("remote CAS conflict");_nodes[d.NodeId]=d;Event(d,current.Status.UploadId);var seq=_events.Count.ToString();
                var raw=JsonSerializer.Serialize(new{schema="saturn.catalog-upload-result.v2",status="PUBLISHED",uploadId=upload,operationId=upload,rootId=Binding.RootId,intentHash=hash,publicationReceipt=new{schema="saturn.upload-publication.v1",uploadId=upload,operationId=upload,intentHash=hash,rootId=Binding.RootId,resourceId=d.NodeId,versionId=d.ContentVersionId,parentId=d.ParentId,name=d.Name,sizeBytes=d.SizeBytes,sha256=d.ContentSha256,etag="\"sha256-"+d.ContentSha256+"\"",publishedAt="2026-10-13T00:00:00.000Z"},publishedDescription=Wire(d),firstSeq=seq,lastSeq=seq});
                var done=current.Status with{Phase="published",TransferPending=false,ResultJson=raw,PublishedDescription=d,FirstSequence=_events.Count,LastSequence=_events.Count};_uploads[upload!]=(i,done);if(LoseComplete){LoseComplete=false;throw new IOException("lost complete reply after durable publication");}return Task.FromResult(done);
            }
        }
        public Task<CycleUploadStatus> AppendAsync(CycleUploadInput i,string id,string hash,long offset,ReadOnlyMemory<byte> bytes,CancellationToken ct){lock(_gate){Stage3IndividualOffsets.Add(offset);var current=_uploads[id];Assert.Equal(offset,current.Status.ReceivedBytes);var updated=current.Status with{ReceivedBytes=offset+bytes.Length};_uploads[id]=(i,updated);return Task.FromResult(updated);}}
        private void Event(SyncDescriptor d,string operation)=>_events.Add(new(d,(_events.Count+1).ToString(),"upsert",operation,null));
        internal static object Wire(SyncDescriptor d)=>new{node_id=d.NodeId,parent_id=d.ParentId,name=d.Name,type=d.Type,size_bytes=d.SizeBytes,content_sha256=d.ContentSha256,content_version_id=d.ContentVersionId,version=d.Version,availability=d.Availability};
    }
}
