using System.Reflection;
using System.Text.Json;
using System.Net;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;
[Collection("Native fresh cycle")]
public sealed partial class JournalGapReplacementTests
{
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    private static Task<T> Medium<T>(Func<Task<T>> action)=>(Task<T>)typeof(IdentityOnlyFreshCycleTests).GetMethod("ReducedMedium",BindingFlags.NonPublic|BindingFlags.Static)!.MakeGenericMethod(typeof(T)).Invoke(null,[action])!;
    private static readonly WindowsCycleNativeOptions Options=new(LocalJournalReadMode.IdentityOnly,true);
    private static WindowsCycleConfiguration Config(WindowsFreshCycleTests.Fixture f)=>new("mapping","client","audit","rules-v1",new Uri("https://test.invalid/"),f.Remote.Device,"audit-token","local-test",f.Remote.Binding);
    private static Task<WindowsFreshSyncEngine> Open(WindowsFreshCycleTests.Fixture f)=>WindowsFreshSyncEngine.OpenAsync(Options,f.Store,f.Source,Path.Combine(f.Directory,"spool"),Config(f),f.Remote,new(MinimumFreeDiskBytes:10L<<30),cancellationToken:Ct);
    private static Task<WindowsFreshSyncEngine> Replace(WindowsFreshCycleTests.Fixture f,JournalGapReplacementRequest r,WindowsCycleConfiguration? config=null,Func<WindowsCycleObservation,CancellationToken,Task>? observer=null)=>WindowsFreshSyncEngine.OpenJournalReplacementAsync(Options,r,f.Store,f.Source,Path.Combine(f.Directory,"spool"),config??Config(f),f.Remote,new(MinimumFreeDiskBytes:10L<<30),observer:observer,cancellationToken:Ct);
    private static LocalSourceJournalSession Session(WindowsFreshSyncEngine e)=>(LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(e)!;
    private static Task<T> SessionCall<T>(LocalSourceJournalSession session,string method,params object?[] args)
        =>(Task<T>)typeof(LocalSourceJournalSession).GetMethod(method,BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(session,args)!;
    private static Task SessionCall(LocalSourceJournalSession session,string method,params object?[] args)
        =>(Task)typeof(LocalSourceJournalSession).GetMethod(method,BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(session,args)!;
    private static async Task<string> Snapshot(WindowsFreshCycleTests.Fixture f,string scope)
    {
        await using var c=new SqliteConnection("Data Source="+f.Database+";Cache=Private;Pooling=False");await c.OpenAsync(Ct);var result=new List<object>();
        foreach(var sql in new[]{"SELECT binding_json,journal_id,cursor,gap_epoch,gap_code,owner_epoch FROM local_source_roots WHERE root_key=(SELECT root_key FROM local_cycle_scopes WHERE scope_id=$scope)","SELECT node_id,baseline,baseline_work,ack_journal,ack_watcher FROM local_cycle_files WHERE scope_id=$scope ORDER BY node_id","SELECT operation_id,entry_json,input_json,capture_proof,result_json,upload_id FROM local_cycle_work WHERE scope_id=$scope ORDER BY work_no"})
        {await using var q=c.CreateCommand();q.CommandText=sql;q.Parameters.AddWithValue("$scope",scope);await using var r=await q.ExecuteReaderAsync(Ct);while(await r.ReadAsync(Ct)){var row=new object[r.FieldCount];r.GetValues(row);result.Add(row);}}
        return JsonSerializer.Serialize(result);
    }
    [Fact]
    public async Task NativeExplicitGapReplacementKeepsOldEvidenceAndSameRequestScopeOnReopen()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"historical baseline");Directory.CreateDirectory(Path.Combine(f.Source,"empty"));await Medium(async()=>
        {
            string scope;await using(var original=await Open(f)){scope=original.ScopeId;Assert.Null((await original.InitialSyncAsync(Ct)).Error);await Session(original).MarkGapAsync("JournalGap",Ct);} // deliberately persisted fault, not physical-wrap claim
            await Assert.ThrowsAsync<LocalSourceGapException>(()=>WindowsFreshSyncEngine.ResumeOrOpenAsync(Options,f.Store,f.Source,Path.Combine(f.Directory,"spool"),Config(f),f.Remote,new(MinimumFreeDiskBytes:10L<<30),cancellationToken:Ct));var before=await Snapshot(f,scope);var requests=f.Remote.Prepares;var r=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),scope);string current;
            await using(var replacement=await Replace(f,r))
            {current=replacement.ScopeId;Assert.NotEqual(scope,current);Assert.Equal(r.ReplacementId,replacement.JournalReplacement!.ReplacementId);Assert.Equal(2,replacement.JournalReplacement.SubscriptionGeneration);Assert.Equal(before,await Snapshot(f,scope));var blocked=await replacement.InitialSyncAsync(Ct);Assert.Contains("explicit replacement",blocked.Error,StringComparison.OrdinalIgnoreCase);Assert.Equal(0,blocked.CapturedFiles);Assert.Equal(0,blocked.PublishedFiles);Assert.Equal(requests,f.Remote.Prepares);}
            await using var restored=await Replace(f,r);Assert.Equal(current,restored.ScopeId);Assert.Equal(r.ReplacementId,restored.JournalReplacement!.ReplacementId);Assert.Equal(before,await Snapshot(f,scope));Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_source_roots"));Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_scopes"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_replacement_requests"));Assert.Equal(5L,await f.Scalar("SELECT version FROM local_cycle_format"));Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE scope_id='"+current+"'"));await f.Evidence("explicit-gap-open-reopen",new{request=r,restored.JournalReplacement,originalEvidencePreserved=true,injectedDurableGap=true,actualPhysicalWrapClaim=false,requests=f.Remote.Prepares});return true;
        });
    }
    [Theory][InlineData("upgrade")][InlineData("configuration")][InlineData("disk-floor")]
    public async Task ExplicitReplacementFromNamedSourceV1PreservesOriginalsAndQualifiesMigration(string gate)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"retained named-v1 bytes");
        string old;
        await using(var e=await f.Open()){old=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}
        await using(var c=new SqliteConnection("Data Source="+f.Database+";Pooling=False"))
        {await c.OpenAsync(Ct);await using var q=c.CreateCommand();q.CommandText="ALTER TABLE local_source_roots DROP COLUMN read_mode;UPDATE local_source_format SET version=1";await q.ExecuteNonQueryAsync(Ct);}
        var before=await Snapshot(f,old);var request=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),old);
        var backupsBefore=Directory.GetFiles(f.Directory,"*.pre-local-source-v2-*.db").Order().ToArray();
        await Medium(async()=>
        {
            var config=gate=="configuration"?Config(f) with{RulesVersion="different-rules"}:Config(f);
            var budget=new WindowsCycleBudget(MinimumFreeDiskBytes:gate=="disk-floor"?long.MaxValue:10L<<30);
            Task<WindowsFreshSyncEngine> Replacement()=>WindowsFreshSyncEngine.OpenJournalReplacementAsync(Options,request,f.Store,f.Source,Path.Combine(f.Directory,"spool"),config,f.Remote,budget,cancellationToken:Ct);
            if(gate!="upgrade")
            {
                await Assert.ThrowsAnyAsync<Exception>(Replacement);
                Assert.Equal(1L,await f.Scalar("SELECT version FROM local_source_format"));
                Assert.Equal(backupsBefore,Directory.GetFiles(f.Directory,"*.pre-local-source-v2-*.db").Order().ToArray());
                Assert.Equal(before,await Snapshot(f,old));return true;
            }
            await using(var e=await Replacement())
            {
                Assert.Equal(2L,await f.Scalar("SELECT version FROM local_source_format"));Assert.Equal(5L,await f.Scalar("SELECT version FROM local_cycle_format"));
                Assert.Equal(0L,await f.Scalar("SELECT read_mode FROM local_source_roots WHERE root_key=(SELECT root_key FROM local_cycle_scopes WHERE scope_id='"+old+"')"));
                Assert.Equal(1L,await f.Scalar("SELECT read_mode FROM local_source_roots WHERE root_key=(SELECT root_key FROM local_cycle_scopes WHERE scope_id='"+e.ScopeId+"')"));
                Assert.Equal(before,await Snapshot(f,old));await e.QualifyJournalReplacementAsync(Ct);Assert.Null((await e.FreshSyncAsync(Ct)).Error);Assert.Equal(before,await Snapshot(f,old));
            }
            var migrationBackup=Assert.Single(Directory.GetFiles(f.Directory,"*.pre-local-source-v2-*.db").Except(backupsBefore));
            await using(var backup=new SqliteConnection("Data Source="+migrationBackup+";Mode=ReadOnly;Pooling=False"))
            {await backup.OpenAsync(Ct);await using var q=backup.CreateCommand();q.CommandText="SELECT version FROM local_source_format";Assert.Equal(1L,await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT count(*) FROM local_source_roots WHERE valid=0 AND gap_code='JournalGap'";Assert.Equal(1L,await q.ExecuteScalarAsync(Ct));}
            await using var again=await Replacement();Assert.Equal(LocalJournalReplacementStatus.Installed,again.JournalReplacement!.Status);
            var repeat=await again.FreshSyncAsync(Ct);Assert.Null(repeat.Error);Assert.Equal(0,repeat.CapturedFiles);Assert.Equal(before,await Snapshot(f,old));return true;
        });
    }
    [Theory][InlineData(false,false)][InlineData(true,false)][InlineData(false,true)][InlineData(true,true)]
    public async Task ReplacementRetainsOriginalAnchorThroughUnavailableSnapshotAndExpiredCursor(bool retainedUnbound,bool renamed)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"immutable original bytes");
        await Medium(async()=>
        {
            string old;await using(var e=await Open(f)){old=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}
            var before=await Snapshot(f,old);var prepares=f.Remote.Prepares;var anchor=await f.Scalar("SELECT anchor_json FROM local_cycle_scopes WHERE scope_id='"+old+"'");Assert.IsType<string>(anchor);
            var request=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),old);var remote=new AnchorRecoveryRemote(f.Remote){Unavailable=true};
            Task<WindowsFreshSyncEngine> Replacement()=>WindowsFreshSyncEngine.OpenJournalReplacementAsync(Options,request,f.Store,f.Source,Path.Combine(f.Directory,"spool"),Config(f),remote,new(MinimumFreeDiskBytes:10L<<30),cancellationToken:Ct);
            string scope;await using(var e=await Replacement())
            {scope=e.ScopeId;var failure=await Assert.ThrowsAsync<InvalidDataException>(()=>e.QualifyJournalReplacementAsync(Ct));Assert.Contains("not an available folder",failure.Message);Assert.Equal(LocalJournalReplacementStatus.LocalQualified,e.JournalReplacement!.Status);}
            // Model the retained scope produced by older builds, without
            // changing its explicit request, original evidence or native cut.
            if(retainedUnbound)await f.Scalar("UPDATE local_cycle_scopes SET anchor_json=NULL WHERE scope_id='"+scope+"'");
            remote.Unavailable=false;remote.Expire=true;if(renamed)f.Remote.RenameFolder(f.Remote.Binding.RootId,"changed original raw root name");
            await using(var e=await Replacement())
            {
                Assert.Equal(scope,e.ScopeId);
                if(renamed){var error=await Assert.ThrowsAsync<InvalidDataException>(()=>e.QualifyJournalReplacementAsync(Ct));Assert.Contains("anchor/ancestry",error.Message);Assert.Equal(LocalJournalReplacementStatus.LocalQualified,e.JournalReplacement!.Status);}
                else{await e.QualifyJournalReplacementAsync(Ct);Assert.Equal(LocalJournalReplacementStatus.Installed,e.JournalReplacement!.Status);Assert.Null((await e.FreshSyncAsync(Ct)).Error);var repeat=await e.FreshSyncAsync(Ct);Assert.Null(repeat.Error);Assert.Equal(0,repeat.CapturedFiles);Assert.Equal(0,repeat.PublishedFiles);Assert.Equal(anchor,await f.Scalar("SELECT anchor_json FROM local_cycle_scopes WHERE scope_id='"+scope+"'"));}
            }
            Assert.Equal(before,await Snapshot(f,old));Assert.Equal(prepares,f.Remote.Prepares);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_replacement_requests"));Assert.Equal(2,remote.Starts);return true;
        });
    }
    private sealed class AnchorRecoveryRemote(WindowsFreshCycleTests.FakeRemote inner):IWindowsCycleRemote
    {
        public SyncRootBinding Binding=>inner.Binding;internal bool Unavailable,Expire;internal int Starts;
        public Task<byte[]> StartSnapshotAsync(string id,CancellationToken ct){Starts++;return inner.StartSnapshotAsync(id,ct);}
        public Task<byte[]> StepSnapshotAsync(string id,CancellationToken ct)=>inner.StepSnapshotAsync(id,ct);
        public Task<SyncSnapshotReady> ReadySnapshotAsync(string id,CancellationToken ct)=>inner.ReadySnapshotAsync(id,ct);
        public async Task<SyncSnapshotEntriesPage> ReadEntriesAsync(SyncSnapshotReady ready,string cursor,string? after,CancellationToken ct)
        {var page=await inner.ReadEntriesAsync(ready,cursor,after,ct);return page with{Items=page.Items.Select(d=>Unavailable&&d.NodeId==Binding.RootId?d with{Availability="UNKNOWN"}:d).ToArray()};}
        public Task<SyncFeedPage> ReadChangesAsync(string cursor,long seq,CancellationToken ct){if(Expire){Expire=false;throw new GuardedCycleRemoteException(HttpStatusCode.Gone,"cursor_expired",true);}return inner.ReadChangesAsync(cursor,seq,ct);}
        public Task<CycleFolderOutcome> EnsureDirectoryAsync(CycleFolderInput input,CancellationToken ct)=>inner.EnsureDirectoryAsync(input,ct);
        public Task<CycleUploadStatus> UploadAsync(string command,CycleUploadInput input,string? upload,string hash,CancellationToken ct)=>inner.UploadAsync(command,input,upload,hash,ct);
        public Task<CycleUploadStatus> AppendAsync(CycleUploadInput input,string upload,string hash,long offset,ReadOnlyMemory<byte> bytes,CancellationToken ct)=>inner.AppendAsync(input,upload,hash,offset,bytes,ct);
    }
    [Fact]
    public async Task ExplicitReplacementResumesInterruptedFullMembershipWithSameParentPlanAndOldOriginals()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"original bytes");File.WriteAllText(Path.Combine(f.Directory,"outside"),"outside bytes");
        await Medium(async()=>
        {
            string old;await using(var e=await Open(f)){old=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}
            var before=await Snapshot(f,old);var request=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),old);var remote=new AnchorRecoveryRemote(f.Remote){Unavailable=true};var stop=false;
            Task<WindowsFreshSyncEngine> Replacement()=>WindowsFreshSyncEngine.OpenJournalReplacementAsync(Options,request,f.Store,f.Source,Path.Combine(f.Directory,"spool"),Config(f),remote,new(MinimumFreeDiskBytes:10L<<30),observer:(o,t)=>{if(stop&&o.Stage==WindowsCycleStage.BranchEnumerated){stop=false;throw new IOException("Interrupted replacement membership");}return Task.CompletedTask;},cancellationToken:Ct);
            string scope,plan;await using(var e=await Replacement())
            {
                scope=e.ScopeId;await Assert.ThrowsAsync<InvalidDataException>(()=>e.QualifyJournalReplacementAsync(Ct));Assert.Equal(LocalJournalReplacementStatus.LocalQualified,e.JournalReplacement!.Status);
                remote.Unavailable=false;remote.Expire=true;stop=true;
                // Exercise an interrupted full-membership proof explicitly.
                // An unrelated volume hardlink may already have been proved
                // outside the source; it does not guarantee this work exists.
                Assert.True(await Session(e).RequestFullParentProofAsync(scope,Ct));
                var error=await Assert.ThrowsAsync<IOException>(()=>e.QualifyJournalReplacementAsync(Ct));Assert.Contains("Interrupted replacement membership",error.Message);plan=Assert.IsType<string>(await f.Scalar("SELECT plan_id FROM local_parent_reconcile_plans WHERE status=0"));
            }
            remote.Expire=true;await using var resumed=await Replacement();Assert.Equal(scope,resumed.ScopeId);await resumed.QualifyJournalReplacementAsync(Ct);Assert.Equal(LocalJournalReplacementStatus.Installed,resumed.JournalReplacement!.Status);Assert.Equal(plan,await f.Scalar("SELECT plan_id FROM local_parent_reconcile_plans WHERE status=3"));Assert.Equal(before,await Snapshot(f,old));Assert.Equal(1,f.Remote.Prepares);return true;
        });
    }
    [Theory][InlineData("no-gap")][InlineData("non-journal")][InlineData("configuration")][InlineData("root-replaced")][InlineData("legacy")]
    public async Task ExplicitReplacementRefusesOtherCausesWithoutSourceReset(string kind)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"baseline");await Medium(async()=>
        {
            string scope;await using(var original=await Open(f)){scope=original.ScopeId;Assert.Null((await original.InitialSyncAsync(Ct)).Error);if(kind!="no-gap")await Session(original).MarkGapAsync(kind=="non-journal"?"WatcherGap":"JournalGap",Ct);}
            if(kind=="root-replaced"){Directory.Move(f.Source,Path.Combine(f.Directory,"old-source-retained"));Directory.CreateDirectory(f.Source);}
            if(kind=="legacy")await f.Store.AddMappingAsync(new("legacy-extra","client",Path.Combine(f.Directory,"unrelated"),false,DateTimeOffset.UtcNow),Ct);
            var before=await Snapshot(f,scope);var config=kind=="configuration"?Config(f)with{CredentialScope="changed"}:Config(f);await Assert.ThrowsAnyAsync<IOException>(()=>Replace(f,new(Guid.NewGuid().ToString("N"),scope),config));Assert.Equal(before,await Snapshot(f,scope));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_source_roots"));Assert.Equal(4L,await f.Scalar("SELECT version FROM local_cycle_format"));return true;
        });
    }
    [Fact]
    public async Task HistoricalFilesAndDirectoriesKeepOriginalKeysWithoutCurrentBaselineAdoption()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"old sealed bytes");Directory.CreateDirectory(Path.Combine(f.Source,"folder"));await Medium(async()=>
        {
            string old;await using(var engine=await Open(f)){old=engine.ScopeId;Assert.Null((await engine.InitialSyncAsync(Ct)).Error);await Session(engine).MarkGapAsync("JournalGap",Ct);}
            var before=await Snapshot(f,old);var file=(string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE size IS NOT NULL LIMIT 1"))!;var directory=(string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE size IS NULL LIMIT 1"))!;var prepares=f.Remote.Prepares;
            // Current bytes changed after the old sealed publication. A
            // historical receipt must not establish current B or byte equality.
            File.WriteAllText(Path.Combine(f.Source,"original"),"CURRENT different longer bytes");await using var replacement=await Replace(f,new(Guid.NewGuid().ToString("N"),old));var fileResult=await replacement.RecoverOriginalWorkAsync(old,file,Ct);var directoryResult=await replacement.RecoverOriginalDirectoryAsync(old,directory,Ct);
            Assert.Null(fileResult.Error);Assert.Null(directoryResult.Error);Assert.Equal("published",fileResult.Status);Assert.Equal("published",directoryResult.Status);Assert.Equal("NotAdopted",fileResult.Baseline);Assert.Equal("Preserved",fileResult.Dirty);Assert.Equal(prepares,f.Remote.Prepares);Assert.Equal(before,await Snapshot(f,old));Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE scope_id='"+replacement.ScopeId+"'"));Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_replacement_originals"));Assert.Equal(0,replacement.ResourceMetrics.MaximumSourceReaders);await f.Evidence("explicit-gap-historical-only",new{fileResult,directoryResult,originalSnapshotExact=true,currentBytesChanged=true,newCurrentBaselineCount=0,originalKeysRetained=true,resources=replacement.ResourceMetrics});return true;
        });
    }
    [Fact]
    public async Task PendingReplacementCannotBorrowUnlinkedOldScopeOrReplaceRequestId()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"old");await Medium(async()=>
        {
            string old;await using(var engine=await Open(f)){old=engine.ScopeId;Assert.Null((await engine.InitialSyncAsync(Ct)).Error);await Session(engine).MarkGapAsync("JournalGap",Ct);}
            var r=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),old);await using(var replacement=await Replace(f,r)){var original=(string)(await f.Scalar("SELECT operation_id FROM local_cycle_work LIMIT 1"))!;var result=await replacement.RecoverOriginalWorkAsync(replacement.ScopeId,original,Ct);Assert.NotNull(result.Error);Assert.Equal("Uncertain",result.Status);Assert.Equal(1,f.Remote.Prepares);}
            await Assert.ThrowsAsync<LocalSourceGapException>(()=>Replace(f,new(Guid.NewGuid().ToString("N"),old)));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_replacement_requests"));Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_source_roots"));return true;
        });
    }
    [Fact]
    public async Task NativeCompleteReplacementUsesCurrentBytesAndFeedThenFreshZeroReads()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"unchanged owned bytes");Directory.CreateDirectory(Path.Combine(f.Source,"empty"));await Medium(async()=>
        {
            string old;await using(var e=await Open(f)){old=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}var before=await Snapshot(f,old);var prepares=f.Remote.Prepares;await using var replacement=await Replace(f,new(Guid.NewGuid().ToString("N"),old));await replacement.QualifyJournalReplacementAsync(Ct);Assert.Equal(LocalJournalReplacementStatus.Installed,replacement.JournalReplacement!.Status);Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE scope_id='"+replacement.ScopeId+"' AND baseline IS NOT NULL"));
            var current=await replacement.FreshSyncAsync(Ct);await f.Evidence("explicit-gap-current-agreement",current);Assert.Null(current.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",current.Decision);Assert.Equal(1,current.CapturedFiles);Assert.Equal(0,current.PublishedFiles);Assert.Equal(prepares,f.Remote.Prepares);Assert.Equal(before,await Snapshot(f,old));Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE scope_id='"+replacement.ScopeId+"' AND baseline IS NOT NULL"));var repeat=await replacement.FreshSyncAsync(Ct);Assert.Null(repeat.Error);Assert.Equal(0,repeat.CapturedFiles);Assert.Equal("NoWorkAtAppliedEventBoundary",repeat.Decision);await f.Evidence("explicit-gap-fresh-zero",repeat);return true;
        });
    }
    [Fact]
    public async Task CancelledCandidateKeepsExactAttemptAndSecondCandidateCompletesSameScope()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();Directory.CreateDirectory(Path.Combine(f.Source,"nested"));File.WriteAllText(Path.Combine(f.Source,"nested","file"),"same original bytes");await Medium(async()=>
        {
            string old;await using(var e=await Open(f)){old=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}var before=await Snapshot(f,old);var request=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),old);string scope;using var cancellation=CancellationTokenSource.CreateLinkedTokenSource(Ct);
            await using(var e=await Replace(f,request,observer:(o,t)=>{if(o.Stage==WindowsCycleStage.BranchEnumerated)cancellation.Cancel();return Task.CompletedTask;})){scope=e.ScopeId;await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>e.QualifyJournalReplacementAsync(cancellation.Token));Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE scope_id='"+scope+"' AND baseline IS NOT NULL"));}
            var first=(string)(await f.Scalar("SELECT candidate_id FROM local_cycle_replacement_attempts WHERE attempt=1"))!;Assert.Equal(2L,await f.Scalar("SELECT state FROM local_cycle_replacement_attempts WHERE attempt=1"));Assert.Contains("Canceled",(string)(await f.Scalar("SELECT reason FROM local_cycle_replacement_attempts WHERE attempt=1"))!);Assert.Equal(2L,await f.Scalar("SELECT status FROM local_namespace_candidates WHERE candidate_id='"+first+"'"));
            await using(var e=await Replace(f,request)){Assert.Equal(scope,e.ScopeId);await e.QualifyJournalReplacementAsync(Ct);Assert.Equal(LocalJournalReplacementStatus.Installed,e.JournalReplacement!.Status);Assert.Equal(2,e.JournalReplacement.CandidateAttempts);var current=await e.FreshSyncAsync(Ct);Assert.Null(current.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",current.Decision);}
            Assert.Equal(first,await f.Scalar("SELECT candidate_id FROM local_cycle_replacement_attempts WHERE attempt=1"));Assert.NotEqual(first,await f.Scalar("SELECT candidate_id FROM local_cycle_replacement_attempts WHERE attempt=2"));Assert.Equal(1L,await f.Scalar("SELECT state FROM local_cycle_replacement_attempts WHERE attempt=2"));Assert.Equal(before,await Snapshot(f,old));await f.Evidence("replacement-cancel-second-candidate",new{request,scope,first,attempts=2,originalSnapshotExact=true});return true;
        });
    }
    [Fact]
    public async Task TwoCancelledCandidatesRefuseThirdWithoutResettingRequestOrHistory()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"original");await Medium(async()=>
        {
            string old;await using(var e=await Open(f)){old=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}var request=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),old);var before=await Snapshot(f,old);
            for(var i=0;i<2;i++){using var cancel=CancellationTokenSource.CreateLinkedTokenSource(Ct);await using var e=await Replace(f,request,observer:(o,t)=>{if(o.Stage==WindowsCycleStage.BranchEnumerated)cancel.Cancel();return Task.CompletedTask;});await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>e.QualifyJournalReplacementAsync(cancel.Token));}
            await using(var e=await Replace(f,request)){var failure=await Assert.ThrowsAsync<LocalSourceGapException>(()=>e.QualifyJournalReplacementAsync(Ct));Assert.Equal("ReplacementAttemptLimit",failure.Code);}Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_replacement_attempts"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_replacement_requests"));Assert.Equal(before,await Snapshot(f,old));return true;
        });
    }
    [Fact]
    public async Task AbruptUnboundBuildingCandidateRefusesReopenWithoutOwnerOrCursorMutation()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();await Medium(async()=>
        {
            string old;await using(var e=await Open(f)){old=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}var request=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),old);string candidate,scope;
            await using(var e=await Replace(f,request)){scope=e.ScopeId;await using var native=await WindowsNativeCycleCapture.OpenAsync(f.Source,Ct);candidate=(await Session(e).BeginNamespaceCandidateAsync(native.RootProof,new(MinimumFreeDiskBytes:10L<<30),Ct)).CandidateId;}
            var cursor=await f.Scalar("SELECT cursor FROM local_source_roots WHERE root_key=(SELECT root_key FROM local_cycle_scopes WHERE scope_id='"+scope+"')");var owner=await f.Scalar("SELECT owner_epoch FROM local_cycle_scopes WHERE scope_id='"+scope+"'");var failure=await Assert.ThrowsAsync<LocalSourceGapException>(()=>Replace(f,request));Assert.Equal("ReplacementBuildingRestart",failure.Code);Assert.Equal(cursor,await f.Scalar("SELECT cursor FROM local_source_roots WHERE root_key=(SELECT root_key FROM local_cycle_scopes WHERE scope_id='"+scope+"')"));Assert.Equal(owner,await f.Scalar("SELECT owner_epoch FROM local_cycle_scopes WHERE scope_id='"+scope+"'"));Assert.Equal(0L,await f.Scalar("SELECT status FROM local_namespace_candidates WHERE candidate_id='"+candidate+"'"));return true;
        });
    }
    [Fact]
    public async Task InstalledSameRequestReopenRetainsCurrentBaselineAndFreshZeroReads()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"closed original bytes");await Medium(async()=>
        {
            string old;await using(var e=await Open(f)){old=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}var request=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),old);string scope;
            await using(var e=await Replace(f,request)){scope=e.ScopeId;await e.QualifyJournalReplacementAsync(Ct);Assert.Null((await e.FreshSyncAsync(Ct)).Error);}var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE scope_id='"+scope+"'");var prepares=f.Remote.Prepares;
            await using var restored=await Replace(f,request);Assert.Equal(scope,restored.ScopeId);Assert.Equal(LocalJournalReplacementStatus.Installed,restored.JournalReplacement!.Status);await restored.QualifyJournalReplacementAsync(Ct);var fresh=await restored.FreshSyncAsync(Ct);Assert.Null(fresh.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",fresh.Decision);Assert.Equal(0,fresh.CapturedFiles);Assert.Equal(prepares,f.Remote.Prepares);Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE scope_id='"+scope+"'"));return true;
        });
    }
    [Theory][InlineData("before-seed")][InlineData("partial-seed")][InlineData("complete-seed")]
    public async Task CompletedNativeCutResumesOriginalCandidateAcrossSeedActivationBoundary(string boundary)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await WindowsFreshCycleTests.Fixture.Create();
        await Medium(async()=>
        {
            string old;
            await using(var original=await Open(f))
            {
                old=original.ScopeId;
                Assert.Null((await original.InitialSyncAsync(Ct)).Error);
                await Session(original).MarkGapAsync("JournalGap",Ct);
            }
            var originalSnapshot=await Snapshot(f,old);
            for(var i=0;i<251;i++)File.WriteAllText(Path.Combine(f.Source,"new-"+i.ToString("D3")),"current");
            var request=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),old);
            string scope,coherence,candidate;
            long after;
            await using(var replacement=await Replace(f,request))
            {
                scope=replacement.ScopeId;
                var session=Session(replacement);
                var coordinator=(WindowsLocalCoherenceCoordinator)typeof(WindowsFreshSyncEngine).GetField("_coordinator",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(replacement)!;
                var attempt=await SessionCall<int>(session,"BeginReplacementAttemptAsync",scope,request.ReplacementId,Ct);
                Func<string,string,CancellationToken,Task> record=(n,c,t)=>SessionCall(session,"RecordReplacementCandidateAsync",scope,request.ReplacementId,attempt,n,c,t);
                typeof(WindowsLocalCoherenceCoordinator).GetMethod("SetReplacementAnchorStarted",BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(coordinator,[record]);
                var state=await coordinator.BuildAnchorAsync(session,new(MinimumFreeDiskBytes:10L<<30),new(MinimumFreeDiskBytes:10L<<30),Ct);
                Assert.Equal(LocalCoherenceStatus.Anchored,state.Status);
                coherence=state.CoherenceId;candidate=state.CandidateId;
                if(boundary!="before-seed")Assert.Equal(250,await SessionCall<int>(session,"SeedCycleEntriesAsync",scope,Ct));
                if(boundary=="complete-seed")while(await SessionCall<int>(session,"SeedCycleEntriesAsync",scope,Ct)>0){ }
                after=(long)(await f.Scalar("SELECT seed_after FROM local_cycle_scopes WHERE scope_id='"+scope+"'"))!;
                Assert.Equal(0L,await f.Scalar("SELECT status FROM local_cycle_replacement_requests"));
                // Stop between real committed native/seed transactions. No
                // fabricated complete flag or candidate data is written.
            }
            var prepares=f.Remote.Prepares;
            await using(var restored=await Replace(f,request))
            {
                Assert.Equal(scope,restored.ScopeId);
                Assert.Equal(after,await f.Scalar("SELECT seed_after FROM local_cycle_scopes WHERE scope_id='"+scope+"'"));
                await restored.QualifyJournalReplacementAsync(Ct);
                Assert.Equal(LocalJournalReplacementStatus.Installed,restored.JournalReplacement!.Status);
                Assert.Equal(1,restored.JournalReplacement.CandidateAttempts);
            }
            Assert.Equal(coherence,await f.Scalar("SELECT coherence_id FROM local_cycle_replacement_attempts"));
            Assert.Equal(candidate,await f.Scalar("SELECT candidate_id FROM local_cycle_replacement_attempts"));
            Assert.Equal(251L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE scope_id='"+scope+"'"));
            Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE scope_id='"+scope+"' AND baseline IS NOT NULL"));
            Assert.Equal(prepares,f.Remote.Prepares);
            Assert.Equal(originalSnapshot,await Snapshot(f,old));
            await f.Evidence("replacement-seed-boundary-"+boundary,new{request,scope,coherence,candidate,after,originalSnapshotExact=true,currentPublications=0});
            return true;
        });
    }
    [Fact]
    public async Task CompetingReplacementOwnerCannotChangePendingRequestOrOriginalEvidence()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();
        await Medium(async()=>
        {
            string old;await using(var e=await Open(f)){old=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}
            var before=await Snapshot(f,old);var request=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),old);
            await using var owner=await Replace(f,request);
            var epoch=await f.Scalar("SELECT new_owner FROM local_cycle_replacement_requests");
            await Assert.ThrowsAnyAsync<IOException>(()=>Replace(f,request));
            await Assert.ThrowsAnyAsync<IOException>(()=>WindowsFreshSyncEngine.OpenJournalReplacementAsync(Options,request,f.Store,f.Source,Path.Combine(f.Directory,"competing-spool"),Config(f),f.Remote,new(MinimumFreeDiskBytes:10L<<30),cancellationToken:Ct));
            await Assert.ThrowsAnyAsync<IOException>(()=>Replace(f,new(Guid.NewGuid().ToString("N"),old)));
            Assert.Equal(epoch,await f.Scalar("SELECT new_owner FROM local_cycle_replacement_requests"));
            Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_replacement_requests"));
            Assert.Equal(before,await Snapshot(f,old));return true;
        });
    }
    [Fact]
    public async Task ReplacementSpoolFloorRefusalLeavesFormatFourAndOriginalGapIntact()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();
        await Medium(async()=>
        {
            string old;await using(var e=await Open(f)){old=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}
            var before=await Snapshot(f,old);
            await Assert.ThrowsAsync<BootstrapCaptureDeferredException>(()=>WindowsFreshSyncEngine.OpenJournalReplacementAsync(Options,new(Guid.NewGuid().ToString("N"),old),f.Store,f.Source,Path.Combine(f.Directory,"spool"),Config(f),f.Remote,new(MinimumFreeDiskBytes:long.MaxValue),cancellationToken:Ct));
            Assert.Equal(4L,await f.Scalar("SELECT version FROM local_cycle_format"));
            Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM sqlite_master WHERE name='local_cycle_replacement_requests'"));
            Assert.False(File.Exists(f.Database+".local-cycle-v5-backup.json"));Assert.Equal(before,await Snapshot(f,old));return true;
        });
    }
}
