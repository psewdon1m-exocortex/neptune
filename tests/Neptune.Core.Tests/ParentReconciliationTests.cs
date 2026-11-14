using System.Reflection;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
[Collection("Native fresh cycle")]
public sealed partial class ParentReconciliationTests
{
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    private static WindowsCycleConfiguration Config(WindowsFreshCycleTests.Fixture f)=>new("mapping","client","audit","rules-v1",new Uri("https://test.invalid/"),f.Remote.Device,"audit-token","local-test",f.Remote.Binding);
    private static Task<WindowsFreshSyncEngine> Open(WindowsFreshCycleTests.Fixture f,bool resume=false,Func<WindowsCycleObservation,CancellationToken,Task>? observer=null)
        =>resume?WindowsFreshSyncEngine.ResumeOrOpenAsync(new WindowsCycleNativeOptions(LocalJournalReadMode.IdentityOnly,true),f.Store,f.Source,Path.Combine(f.Directory,"spool"),Config(f),f.Remote,new(MinimumFreeDiskBytes:10L<<30),observer:observer,cancellationToken:Ct)
        :WindowsFreshSyncEngine.OpenAsync(new WindowsCycleNativeOptions(LocalJournalReadMode.IdentityOnly,true),f.Store,f.Source,Path.Combine(f.Directory,"spool"),Config(f),f.Remote,new(MinimumFreeDiskBytes:10L<<30),observer:observer,cancellationToken:Ct);
    private static Task<T> Medium<T>(Func<Task<T>> action)=>(Task<T>)typeof(IdentityOnlyFreshCycleTests).GetMethod("ReducedMedium",BindingFlags.NonPublic|BindingFlags.Static)!.MakeGenericMethod(typeof(T)).Invoke(null,[action])!;

    [Fact]
    public async Task ActualMediumKnownParentNewFileAndNestedBranchPreserveBaselineAndFreshZeroRead()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"baseline");for(var i=0;i<4;i++)Directory.CreateDirectory(Path.Combine(f.Source,"unaffected-"+i,"deep"));
        await Medium(async()=>
        {
            var branches=0;await using var e=await Open(f,observer:(o,t)=>{if(o.Stage==WindowsCycleStage.BranchEnumerated)branches++;return Task.CompletedTask;});Assert.Null((await Initial(e)).Error);var beforeBranches=branches;var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'");var candidateRows=await f.Scalar("SELECT COUNT(*) FROM local_namespace_entries");var eventBoundary=Convert.ToInt64(await f.Scalar("SELECT max(applied_usn) FROM local_coherence_roots"));
            File.WriteAllText(Path.Combine(f.Source,"new-file"),"new root bytes");Directory.CreateDirectory(Path.Combine(f.Source,"new-tree","inner","empty"));File.WriteAllText(Path.Combine(f.Source,"new-tree","inner","leaf"),"new nested bytes");
            var result=await Converge(e,f);await f.Evidence("parent-current-new-subtree-primary",new{result,branches,beforeBranches,sql=f.Store.GetParentReconciliationMetrics(),plans=await f.Scalar("SELECT json_group_array(json_object('plan',plan_id,'status',status,'full',full_membership,'start',start_usn,'cut',cut_usn,'after',record_after,'parents',parents,'error',error)) FROM local_parent_reconcile_plans"),unresolved=await f.Scalar("SELECT json_group_array(json_object('code',code,'n',n,'last',last)) FROM (SELECT code,count(*) n,max(usn) last FROM local_coherence_unresolved GROUP BY code)")});Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(2,result.CapturedFiles);Assert.Equal(2,result.PublishedFiles);Assert.Equal(3,result.PublishedDirectories);var full=Convert.ToInt64(await f.Scalar("SELECT full_membership FROM local_parent_reconcile_plans ORDER BY rowid DESC LIMIT 1"));var triggers=await FullProofTriggersAfter(e,eventBoundary);await f.Evidence("parent-new-subtree-full-obligations",new{eventBoundary,full,triggers});if(triggers.Count==0)Assert.Equal(0,full);Assert.Equal(beforeBranches+(full==1?12:4),branches);Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));Assert.Equal(candidateRows,await f.Scalar("SELECT COUNT(*) FROM local_namespace_entries"));Assert.True((long)(await f.Scalar("SELECT COUNT(*) FROM local_coherence_unresolved"))!>0);
            var b=branches;var prepares=f.Remote.Prepares;var repeat=await e.FreshSyncAsync(Ct);Assert.Null(repeat.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",repeat.Decision);Assert.Equal(0,repeat.CapturedFiles);Assert.Equal(b,branches);Assert.Equal(prepares,f.Remote.Prepares);Assert.InRange(f.Store.GetParentReconciliationMetrics().MaximumChangedRows,1,256);await f.Evidence("parent-current-fresh",new{repeat,branches,sql=f.Store.GetParentReconciliationMetrics()});return true;
        });
    }
    [Fact]
    public async Task ActualMediumRenameConflictRetainsOriginalBaselineAndNoRemoteEffect()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"baseline");
        await Medium(async()=>
        {
            await using var e=await Open(f);Assert.Null((await Initial(e)).Error);var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'");var work=await f.Scalar("SELECT input_json FROM local_cycle_work WHERE input_json IS NOT NULL");var effects=f.Remote.Prepares;File.Move(Path.Combine(f.Source,"original"),Path.Combine(f.Source,"renamed"));var result=await e.FreshSyncAsync(Ct);await f.Evidence("parent-current-rename-primary",new{result,ownedNative=await OwnedUnresolved(f,e),sql=f.Store.GetParentReconciliationMetrics()});Assert.Null(result.Error);Assert.Equal("PendingOrUncertain",result.Decision);Assert.True(result.Conflicts>=1);Assert.Equal(effects,f.Remote.Prepares);Assert.Equal(0,result.CapturedFiles);Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));Assert.Equal(work,await f.Scalar("SELECT input_json FROM local_cycle_work WHERE input_json IS NOT NULL"));return true;
        });
    }
    [Fact]
    public async Task ActualMediumInterruptedParentRetainsPlanUuidAndSameScopeOnReopen()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"baseline");
        await Medium(async()=>
        {
            var fail=false;string scope;string plan;await using(var e=await Open(f,observer:(o,t)=>fail&&o.Stage==WindowsCycleStage.BranchEnumerated?Task.FromException(new IOException("owned simulated parent interruption")):Task.CompletedTask))
            {scope=e.ScopeId;Assert.Null((await Initial(e)).Error);File.WriteAllText(Path.Combine(f.Source,"new"),"new bytes");fail=true;var stopped=await e.FreshSyncAsync(Ct);await f.Evidence("parent-current-interrupted-primary",stopped);Assert.NotNull(stopped.Error);plan=(string)(await f.Scalar("SELECT plan_id FROM local_parent_reconcile_plans WHERE status<3"))!;}
            await using var restored=await Open(f,true);Assert.Equal(scope,restored.ScopeId);var result=await Converge(restored);await f.Evidence("parent-current-resumed-primary",result);Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(1,result.PublishedFiles);Assert.Equal(plan,await f.Scalar("SELECT plan_id FROM local_parent_reconcile_plans WHERE status=3"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_plans"));return true;
        });
    }
    [Fact]
    public async Task ActualMediumNewWriterDefersWithoutAdoptionThenClosedRetry()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"baseline");
        await Medium(async()=>
        {
            await using var e=await Open(f);Assert.Null((await Initial(e)).Error);var path=Path.Combine(f.Source,"held-new");File.WriteAllText(path,"new");await using(var writer=new FileStream(path,FileMode.Open,FileAccess.Write,FileShare.ReadWrite|FileShare.Delete)){writer.WriteByte(42);writer.Flush(true);var first=await WaitForDeferred(e);await f.Evidence("parent-current-writer-primary",first);Assert.Equal("PendingOrUncertain",first.Decision);Assert.True(first.Deferred>=1);Assert.Equal(0,first.PublishedFiles);}
            await e.RetryDeferredAsync(Ct);var closed=await Converge(e);await f.Evidence("parent-current-writer-closed",closed);Assert.Null(closed.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",closed.Decision);Assert.Equal(1,closed.PublishedFiles);return true;
        });
    }
    private static async Task<object> OwnedUnresolved(WindowsFreshCycleTests.Fixture f,WindowsFreshSyncEngine engine)
    {
        var session=(LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(engine)!;var coherence=(string)typeof(WindowsFreshSyncEngine).GetField("_coherence",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(engine)!;
        var own=((string)(await f.Scalar("SELECT group_concat(node_id,',') FROM local_cycle_files"))!).Split(',').ToHashSet(StringComparer.Ordinal);var records=await session.ReadCoherenceUnresolvedPageAsync(coherence,cancellationToken:Ct);
        return records.Where(r=>own.Contains(r.Record.FileIdHex)).Select(r=>new{r.Usn,r.Code,r.Record.MajorVersion,r.Record.FileIdHex,r.Record.ParentIdHex,reason=r.Record.Reason.ToString("x8"),r.Record.ReadMode,namesEmitted=0}).ToArray();
    }
    [Fact]
    public async Task ActualMediumHardlinkRemainsUncertainAndPreservesBaseline()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"base");await Medium(async()=>
        {
            await using var e=await Open(f);Assert.Null((await Initial(e)).Error);var b=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'");var count=f.Remote.Prepares;Assert.True(CreateHardLinkW(Path.Combine(f.Source,"alias"),Path.Combine(f.Source,"original"),nint.Zero));var r=await e.FreshSyncAsync(Ct);await f.Evidence("parent-current-hardlink-refusal",r);Assert.Equal("PendingOrUncertain",r.Decision);Assert.Contains("Hardlink",r.Error,StringComparison.OrdinalIgnoreCase);Assert.Equal(0,r.PublishedFiles);Assert.Equal(count,f.Remote.Prepares);Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));return true;
        });
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task HardlinkDuringActiveDispatchJoinsWorkersBeforeMembershipRecovery(bool inside)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await WindowsFreshCycleTests.Fixture.Create();
        for(var i=0;i<32;i++)File.WriteAllText(Path.Combine(f.Source,"file-"+i),"original bytes "+i);
        var outside=Path.Combine(f.Directory,"outside");File.WriteAllText(outside,"unrelated bytes");
        var injected=0;var observed=false;
        await Medium(async()=>
        {
            await using var e=await Open(f,observer:async(o,t)=>
            {
                if(o.Stage!=WindowsCycleStage.FilePublished||Interlocked.CompareExchange(ref injected,1,0)!=0)return;
                Assert.True(CreateHardLinkW(Path.Combine(inside?f.Source:f.Directory,"injected-alias"),outside,nint.Zero));
                // Keep one real worker alive until the independent native journal
                // pump has fenced the scope. Recovery must join this worker first.
                for(var n=0;n<100;n++)
                {
                    if(await f.Scalar("SELECT error FROM local_coherence_roots WHERE status=2") is string error&&error=="HardlinkMembershipUnresolved") { observed=true;break; }
                    await Task.Delay(50,t);
                }
            });
            var initial=await e.InitialSyncAsync(Ct);
            Assert.Equal(1,injected);Assert.True(observed,"Native journal pump must observe the injected hardlink during dispatch");
            await f.Evidence("hardlink-active-dispatch",new{inside,initial,observed});
            if(inside)
            {
                Assert.NotNull(initial.Error);Assert.Equal("PendingOrUncertain",initial.Decision);
                if((long)(await f.Scalar("SELECT COUNT(*) FROM sqlite_master WHERE name='local_parent_reconcile_plans'"))!>0)
                    Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_plans WHERE status=3"));
                Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE json_extract(entry_json,'$.name')='injected-alias'"));
            }
            else
            {
                Assert.Null(initial.Error);var final=await Converge(e);Assert.Null(final.Error);
                Assert.Equal("NoWorkAtAppliedEventBoundary",final.Decision);Assert.Equal(32,f.Remote.Prepares);Assert.Equal(32,f.Remote.Completes);
                Assert.Equal(32,Directory.GetFiles(f.Source).Length);
                Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_plans"));
                Assert.True((long)(await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_external_cuts"))!>0);
                Assert.True((long)(await f.Scalar("SELECT COUNT(*) FROM local_coherence_unresolved WHERE code='HardlinkMembershipUnresolved'"))!>0);
                var repeat=await e.FreshSyncAsync(Ct);Assert.Null(repeat.Error);Assert.Equal(0,repeat.CapturedFiles);Assert.Equal(0,repeat.PublishedFiles);
            }
            return true;
        });
    }
    [Theory][InlineData(false,1)][InlineData(true,1)][InlineData(true,2)][InlineData(false,270)]
    public async Task UnknownOutsideHardlinkRequiresWholeCurrentMembershipAndRetainsHistory(bool interrupted,int links)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();
        Directory.CreateDirectory(Path.Combine(f.Source,"known","deep"));File.WriteAllText(Path.Combine(f.Source,"known","deep","original"),"retained baseline");
        var outside=Path.Combine(f.Directory,"outside");File.WriteAllText(outside,"unrelated volume bytes");
        await Medium(async()=>
        {
            string scope;object? baseline;var branches=0;var stop=false;var untilStop=links==2?2:1;string? plan=null;
            await using(var e=await Open(f,observer:(o,t)=>{if(o.Stage==WindowsCycleStage.BranchEnumerated){branches++;if(stop&&--untilStop==0){stop=false;throw new IOException("Interrupted full membership closure");}}return Task.CompletedTask;}))
            {
                scope=e.ScopeId;Assert.Null((await Initial(e)).Error);baseline=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'");var before=branches;
                for(var i=0;i<links;i++)Assert.True(CreateHardLinkW(Path.Combine(f.Directory,"outside-link-"+i),outside,nint.Zero));stop=interrupted;
                if(interrupted)await BeginRetainedHardlinkPlan(e);
                var result=await e.FreshSyncAsync(Ct);
                await f.Evidence("hardlink-noop-diagnostic",new{result,dirty=await f.Scalar("SELECT group_concat(node_id||char(58)||generation||char(58)||reasons) FROM local_coherence_dirty"),work=await f.Scalar("SELECT group_concat(entry_json||char(58)||phase) FROM local_cycle_work"),conflicts=await f.Scalar("SELECT group_concat(reason) FROM local_parent_reconcile_conflicts")});
                if(interrupted){Assert.NotNull(result.Error);Assert.Contains("Interrupted full membership",result.Error);plan=Assert.IsType<string>(await f.Scalar("SELECT plan_id FROM local_parent_reconcile_plans WHERE status<3"));}
                else{Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(0,result.CapturedFiles);Assert.Equal(before+(links>256?3:0),branches);}
                Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));
            }
            if(interrupted&&links==2)Assert.Equal(1L,await f.Scalar("SELECT count(*) FROM local_parent_reconcile_parents WHERE state=2"));
            var resumedBranches=0;
            await using var reopened=await Open(f,true,(o,t)=>{if(o.Stage==WindowsCycleStage.BranchEnumerated)resumedBranches++;return Task.CompletedTask;});Assert.Equal(scope,reopened.ScopeId);var resumed=await reopened.FreshSyncAsync(Ct);Assert.Null(resumed.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",resumed.Decision);Assert.Equal(0,resumed.CapturedFiles);Assert.Equal(1,f.Remote.Prepares);
            if(interrupted&&links==2)Assert.Equal(2,resumedBranches); // qualified root checkpoint is reused
            if(plan is not null)Assert.Equal(plan,await f.Scalar("SELECT plan_id FROM local_parent_reconcile_plans WHERE status=3"));
            Assert.True((long)(await f.Scalar("SELECT COUNT(*) FROM local_coherence_unresolved WHERE code='HardlinkMembershipUnresolved'"))!>0);Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));
            var again=await reopened.FreshSyncAsync(Ct);Assert.Null(again.Error);Assert.Equal(0,again.CapturedFiles);Assert.Equal(0,again.PublishedFiles);return true;
        });
    }
    [Fact]
    public async Task RetainedEmptyMembershipPlanPinsFirstNativeScanWithoutRewritingRegisteredBoundary()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"baseline");File.WriteAllText(Path.Combine(f.Directory,"outside"),"outside");
        await Medium(async()=>
        {
            LocalParentReconciliation pending;
            await using(var e=await Open(f))
            {Assert.Null((await Initial(e)).Error);var session=(LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(e)!;pending=await (Task<LocalParentReconciliation>)typeof(LocalSourceJournalSession).GetMethod("BeginParentReconciliationAsync",BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(session,[e.ScopeId,Ct])!;}
            Assert.True(CreateHardLinkW(Path.Combine(f.Directory,"outside-link"),Path.Combine(f.Directory,"outside"),nint.Zero));
            await using var reopened=await Open(f,true);var result=await reopened.FreshSyncAsync(Ct);Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(0,result.CapturedFiles);Assert.Equal(pending.PlanId,await f.Scalar("SELECT plan_id FROM local_parent_reconcile_plans WHERE status=3"));Assert.Equal(pending.StartUsn,await f.Scalar("SELECT start_usn FROM local_parent_reconcile_plans"));Assert.True((long)(await f.Scalar("SELECT scan_start_usn FROM local_parent_reconcile_plans"))!>pending.StartUsn);return true;
        });
    }
    [Theory][InlineData("inside")][InlineData("during-cut")][InlineData("root-change-during-cut")]
    public async Task FullMembershipRefusesInsideOrChangingEvidenceWithoutPublishing(string kind)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();Directory.CreateDirectory(Path.Combine(f.Source,"known","deep"));File.WriteAllText(Path.Combine(f.Source,"known","deep","original"),"baseline");
        var outside=Path.Combine(f.Directory,"outside");File.WriteAllText(outside,"unrelated bytes");var mutate=false;
        await Medium(async()=>
        {
            await using var e=await Open(f,observer:(o,t)=>
            {if(mutate&&o.Stage==WindowsCycleStage.BranchEnumerated){mutate=false;if(kind=="during-cut")Assert.True(CreateHardLinkW(Path.Combine(f.Directory,"during-link"),outside,nint.Zero));else File.WriteAllText(Path.Combine(f.Source,"created-during-cut"),"must not publish");}return Task.CompletedTask;});
            Assert.Null((await Initial(e)).Error);var before=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'");var effects=f.Remote.Prepares;
            Assert.True(CreateHardLinkW(kind=="inside"?Path.Combine(f.Source,"known","deep","new-link"):Path.Combine(f.Directory,"outside-link"),outside,nint.Zero));mutate=kind!="inside";
            // Exercise an already retained full-tree obligation, including its
            // interruption/closure gates, independently of the new fast path.
            await BeginRetainedHardlinkPlan(e);
            var result=await e.FreshSyncAsync(Ct);Assert.NotNull(result.Error);Assert.Equal("PendingOrUncertain",result.Decision);Assert.Equal(0,result.PublishedFiles);Assert.Equal(effects,f.Remote.Prepares);Assert.Equal(before,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_plans WHERE status=3"));return true;
        });
    }
    [Fact]
    public async Task ParentMembershipMigrationKeepsV1WalOriginalsAndOffersSeparateRollbackCopy()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"baseline");
        await Medium(async()=>{await using var e=await Open(f);Assert.Null((await Initial(e)).Error);File.WriteAllText(Path.Combine(f.Source,"second"),"second");Assert.Null((await Converge(e)).Error);return true;});
        var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'");var operation=await f.Scalar("SELECT input_json FROM local_cycle_work WHERE work_no=1");
        await using var held=new Microsoft.Data.Sqlite.SqliteConnection("Data Source="+f.Database+";Pooling=False");await held.OpenAsync(Ct);
        {await using var q=held.CreateCommand();q.CommandText="DROP TABLE local_parent_reconcile_orphan_closes;DROP TABLE local_parent_reconcile_incarnations;ALTER TABLE local_parent_reconcile_plans DROP COLUMN full_membership;ALTER TABLE local_parent_reconcile_plans DROP COLUMN hardlink_after;ALTER TABLE local_parent_reconcile_plans DROP COLUMN hardlink_verified;ALTER TABLE local_parent_reconcile_plans DROP COLUMN scan_start_usn;UPDATE local_parent_reconcile_format SET version=1;PRAGMA journal_mode=WAL;PRAGMA wal_autocheckpoint=0;CREATE TABLE membership_migration_wal(value TEXT);INSERT INTO membership_migration_wal VALUES('retained WAL evidence')";await q.ExecuteNonQueryAsync(Ct);}
        Assert.True(new FileInfo(f.Database+"-wal").Length>0);
        await Medium(async()=>{await using var e=await Open(f,true);File.WriteAllText(Path.Combine(f.Source,"third"),"third");Assert.Null((await e.FreshSyncAsync(Ct)).Error);return true;});
        Assert.Equal(4L,await f.Scalar("SELECT version FROM local_parent_reconcile_format"));Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));Assert.Equal(operation,await f.Scalar("SELECT input_json FROM local_cycle_work WHERE work_no=1"));
        var rollback=Path.Combine(f.Directory,"membership-v1-rollback.db");await f.Store.CreateParentMembershipRollbackCopyAsync(rollback,Ct);
        await using var copy=new Microsoft.Data.Sqlite.SqliteConnection("Data Source="+rollback+";Mode=ReadOnly;Pooling=False");await copy.OpenAsync(Ct);await using var query=copy.CreateCommand();query.CommandText="SELECT version FROM local_parent_reconcile_format";Assert.Equal(1L,await query.ExecuteScalarAsync(Ct));query.CommandText="SELECT value FROM membership_migration_wal";Assert.Equal("retained WAL evidence",await query.ExecuteScalarAsync(Ct));query.CommandText="SELECT input_json FROM local_cycle_work WHERE work_no=1";Assert.Equal(operation,await query.ExecuteScalarAsync(Ct));
        var evidence=Environment.GetEnvironmentVariable("NEPTUNE_FRESH_CYCLE_EVIDENCE_ROOT");if(evidence is not null)
        {Directory.CreateDirectory(evidence);var file=Path.Combine(evidence,"parent-membership-v2-"+Path.GetFileName(f.Directory)+".db");await using var destination=new Microsoft.Data.Sqlite.SqliteConnection("Data Source="+file+";Pooling=False");await destination.OpenAsync(Ct);held.BackupDatabase(destination);await f.Evidence("parent-membership-migration",new{version=4,oldVersion=1,rollbackWalVerified=true,originalInputPreserved=true,currentCopy=file});}
    }
    [Fact]
    public async Task RetainedVerifiedParentCutCannotClearLaterNativeNamespaceEvents()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"baseline");
        await Medium(async()=>
        {
            await using var e=await Open(f);Assert.Null((await Initial(e)).Error);File.WriteAllText(Path.Combine(f.Source,"second"),"second");Assert.Null((await Converge(e)).Error);
            var plan=Assert.IsType<string>(await f.Scalar("SELECT plan_id FROM local_parent_reconcile_plans WHERE status=3"));var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'");
            var session=(LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(e)!;var coordinator=(WindowsLocalCoherenceCoordinator)typeof(WindowsFreshSyncEngine).GetField("_coordinator",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(e)!;var coherence=(string)typeof(WindowsFreshSyncEngine).GetField("_coherence",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(e)!;
            File.WriteAllText(Path.Combine(f.Directory,"outside-after-installed-cut"),"outside");await coordinator.ObserveChangesAsync(session,coherence,Ct);while((await session.RetireCoherenceInboxBatchAsync(coherence,Ct)).Rows>0){}
            File.WriteAllText(Path.Combine(f.Source,"after-cut"),"later namespace");var observed=await coordinator.ObserveChangesAsync(session,coherence,Ct);Assert.Equal(LocalCoherenceStatus.Uncertain,observed.Status);
            // An already installed scan can now have had its raw prefix
            // retired. Artificially rewinding its status cannot resurrect
            // that proof or clear a later native namespace event.
            await f.Scalar("UPDATE local_parent_reconcile_plans SET status=2 WHERE plan_id='"+plan+"';UPDATE local_parent_reconcile_versions SET pending_plan='"+plan+"' WHERE coherence_id='"+coherence+"'");
            var refusal=await Assert.ThrowsAsync<LocalSourceGapException>(()=>(Task)typeof(LocalSourceJournalSession).GetMethod("FinishParentReconciliationAsync",BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(session,[e.ScopeId,plan,Ct])!);Assert.Equal("ParentPlanBinding",refusal.Code);
            Assert.Equal(LocalCoherenceStatus.Uncertain,(await session.ReadCoherenceAsync(coherence,Ct)).Status);Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));
            await f.Scalar("UPDATE local_parent_reconcile_plans SET status=3 WHERE plan_id='"+plan+"'");
            var next=await Converge(e);Assert.Null(next.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",next.Decision);Assert.Equal(1,next.PublishedFiles);Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_plans WHERE status=3"));return true;
        });
    }
    [Fact]
    public async Task PendingNativeParentScanPinsAppliedInboxEvenWhenMembershipIsAnchored()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"baseline");
        await Medium(async()=>
        {
            await using var e=await Open(f);Assert.Null((await Initial(e)).Error);
            var session=(LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(e)!;var coordinator=(WindowsLocalCoherenceCoordinator)typeof(WindowsFreshSyncEngine).GetField("_coordinator",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(e)!;var coherence=(string)typeof(WindowsFreshSyncEngine).GetField("_coherence",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(e)!;
            await (Task)typeof(LocalSourceJournalSession).GetMethod("BeginParentReconciliationAsync",BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(session,[e.ScopeId,Ct])!;
            var start=(await session.ReadStateAsync(Ct)).Cursor.NextReadUsn;File.WriteAllText(Path.Combine(f.Directory,"outside-during-pending-scan"),"outside");
            Assert.Equal(LocalCoherenceStatus.Anchored,(await coordinator.ObserveChangesAsync(session,coherence,Ct)).Status);
            var raw=await f.Scalar("SELECT group_concat(hex(payload),';') FROM (SELECT payload FROM local_source_inbox WHERE usn>="+start+" ORDER BY usn)");Assert.NotNull(raw);
            await Assert.ThrowsAsync<InvalidOperationException>(async()=>{while((await session.RetireCoherenceInboxBatchAsync(coherence,Ct)).Rows>0){}});
            Assert.Equal(raw,await f.Scalar("SELECT group_concat(hex(payload),';') FROM (SELECT payload FROM local_source_inbox WHERE usn>="+start+" ORDER BY usn)"));return true;
        });
    }
    [Theory][InlineData("journal")][InlineData("arbitrary")][InlineData("candidate")]
    public async Task ActualOptionsResumeCannotClearUnrelatedGapOrChangedCandidate(string kind)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"base");string? b;await Medium(async()=>{await using var e=await Open(f);Assert.Null((await Initial(e)).Error);return true;});b=(string)(await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"))!;
        await using(var c=new Microsoft.Data.Sqlite.SqliteConnection("Data Source="+f.Database+";Cache=Private;Pooling=False")){await c.OpenAsync(Ct);await using var q=c.CreateCommand();q.CommandText=kind=="journal"?"UPDATE local_source_roots SET valid=0,gap_epoch=gap_epoch+1,gap_code='Owned journal gap'":kind=="arbitrary"?"UPDATE local_cycle_scopes SET status=2,error='Arbitrary uncertainty cannot clear'":"UPDATE local_namespace_candidates SET root_created=root_created+1";await q.ExecuteNonQueryAsync(Ct);}
        await Medium(async()=>{IOException? error=null;try{await using var unexpected=await Open(f,true);}catch(IOException e){error=e;}Assert.NotNull(error);return true;});Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_scopes"));Assert.Equal(1,f.Remote.Prepares);
    }
    [Theory][InlineData("future")][InlineData("missing-index")]
    public async Task ParentModuleRefusesBeforeWritableInitializeWithoutDbWalMutation(string kind)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"base");await Medium(async()=>{await using var e=await Open(f);Assert.Null((await Initial(e)).Error);File.WriteAllText(Path.Combine(f.Source,"new"),"new");Assert.Null((await e.FreshSyncAsync(Ct)).Error);return true;});
        await using var held=new Microsoft.Data.Sqlite.SqliteConnection("Data Source="+f.Database+";Cache=Private;Pooling=False");await held.OpenAsync(Ct);await using(var q=held.CreateCommand()){q.CommandText=kind=="future"?"UPDATE local_parent_reconcile_format SET version=99":"DROP INDEX local_parent_reconcile_children";await q.ExecuteNonQueryAsync(Ct);}
        static string Hash(string file){if(!File.Exists(file))return "absent";using var r=new FileStream(file,FileMode.Open,FileAccess.Read,FileShare.ReadWrite|FileShare.Delete);return Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(r));}var before=Hash(f.Database)+Hash(f.Database+"-wal");await Assert.ThrowsAsync<InvalidDataException>(()=>f.Store.InitializeAsync(Ct));Assert.Equal(before,Hash(f.Database)+Hash(f.Database+"-wal"));
    }
    [Fact]
    public async Task ActualMediumNewGenerationDuringPublicationRemainsDirtyThenConverges()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"base");await Medium(async()=>
        {
            var changed=false;await using var e=await Open(f,observer:(o,t)=>{if(o.Stage==WindowsCycleStage.FilePublished&&o.RelativePath=="new"&&!changed){changed=true;File.WriteAllText(Path.Combine(f.Source,"new"),"g plus one longer bytes");}return Task.CompletedTask;});Assert.Null((await Initial(e)).Error);File.WriteAllText(Path.Combine(f.Source,"new"),"g0");var r=await Converge(e,f);await f.Evidence("parent-current-g-plus-one",r);Assert.Null(r.Error);Assert.True(changed);Assert.Equal("NoWorkAtAppliedEventBoundary",r.Decision);Assert.Equal(2,r.CapturedFiles);Assert.Equal(2,r.PublishedFiles);Assert.Equal(3,f.Remote.Prepares);Assert.Equal("match",f.Remote.LastInput!.Intent.Condition.Kind);return true;
        });
    }
    [Fact]
    public async Task ActualCaseSensitiveParentCanonicalCollisionRejectsBeforeNewEffects()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"base");var sensitive=Directory.CreateDirectory(Path.Combine(f.Source,"sensitive")).FullName;
        using(var h=CreateFileW(sensitive,0x100,7,nint.Zero,3,0x02000000,nint.Zero)){Assert.False(h.IsInvalid);uint enabled=1;Assert.True(SetFileInformationByHandle(h,23,ref enabled,4));}
        await Medium(async()=>
        {
            await using var e=await Open(f);Assert.Null((await Initial(e)).Error);var prepares=f.Remote.Prepares;foreach(var name in new[]{"A.dat","a.dat"}){using var h=CreateFileW(Path.Combine(sensitive,name),0x40000000,3,nint.Zero,2,0x01000080,nint.Zero);Assert.False(h.IsInvalid);using var stream=new FileStream(h,FileAccess.Write);stream.WriteByte(42);}
            var r=await e.FreshSyncAsync(Ct);await f.Evidence("parent-current-case-collision",r);Assert.Equal("PendingOrUncertain",r.Decision);Assert.NotNull(r.Error);Assert.Equal(0,r.CapturedFiles);Assert.Equal(0,r.PublishedFiles);Assert.Equal(prepares,f.Remote.Prepares);Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_plans WHERE status=3"));return true;
        });
    }
    [Fact]
    public async Task SupersededUnreferencedParentVersionsRetireButBaselineWorkAndHistoryRemain()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"base");await Medium(async()=>
        {
            await using var e=await Open(f);Assert.Null((await Initial(e)).Error);var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'");var work=await f.Scalar("SELECT input_json FROM local_cycle_work WHERE input_json IS NOT NULL");
            for(var i=0;i<4;i++){File.WriteAllText(Path.Combine(f.Source,"new-"+i),"bytes-"+i);var r=await Converge(e);Assert.Null(r.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",r.Decision);}
            Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_plans"));Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_parents"));Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));Assert.Equal(work,await f.Scalar("SELECT input_json FROM local_cycle_work WHERE input_json IS NOT NULL"));Assert.True((long)(await f.Scalar("SELECT COUNT(*) FROM local_coherence_unresolved"))!>0);Assert.InRange(f.Store.GetParentReconciliationMetrics().MaximumChangedRows,1,256);await f.Evidence("parent-terminal-retirement",new{metrics=f.Store.GetParentReconciliationMetrics(),plans=await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_plans"),historicalUnresolved=await f.Scalar("SELECT COUNT(*) FROM local_coherence_unresolved")});return true;
        });
    }
    [Fact]
    public async Task ActualSamePathReplacementIsConflictAndRetainsOriginalAuthority()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();var original=Path.Combine(f.Source,"original");File.WriteAllText(original,"base");await Medium(async()=>
        {
            await using var e=await Open(f);Assert.Null((await Initial(e)).Error);var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'");var work=await f.Scalar("SELECT input_json FROM local_cycle_work WHERE input_json IS NOT NULL");var prepares=f.Remote.Prepares;
            // Keep the old native ID alive under another name, preventing ID
            // reuse from weakening this replacement-identity fixture.
            File.Move(original,Path.Combine(f.Source,"old-retained"));File.WriteAllText(original,"replacement bytes");var r=await e.FreshSyncAsync(Ct);await f.Evidence("parent-same-path-replacement",r);Assert.Null(r.Error);Assert.Equal("PendingOrUncertain",r.Decision);Assert.True(r.Conflicts>0);Assert.Equal(prepares,f.Remote.Prepares);Assert.Equal(0,r.CapturedFiles);Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));Assert.Equal(work,await f.Scalar("SELECT input_json FROM local_cycle_work WHERE input_json IS NOT NULL"));var conflicts=await e.ReadParentConflictsAsync(cancellationToken:Ct);Assert.Contains(conflicts,c=>c.Reason.Contains("identity",StringComparison.OrdinalIgnoreCase));return true;
        });
    }
    [Fact]
    public async Task NamespaceMutationDuringActualParentPassCannotActivatePartialVersion()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"base");await Medium(async()=>
        {
            var mutate=false;await using var e=await Open(f,observer:(o,t)=>{if(mutate&&o.Stage==WindowsCycleStage.BranchEnumerated){mutate=false;File.WriteAllText(Path.Combine(f.Source,"late"),"changed during parent");}return Task.CompletedTask;});Assert.Null((await Initial(e)).Error);var b=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'");var prepares=f.Remote.Prepares;File.WriteAllText(Path.Combine(f.Source,"new"),"new");mutate=true;var r=await e.FreshSyncAsync(Ct);await f.Evidence("parent-mid-pass-mutation-refusal",r);Assert.Equal("PendingOrUncertain",r.Decision);Assert.Contains("changed",r.Error,StringComparison.OrdinalIgnoreCase);Assert.Equal(prepares,f.Remote.Prepares);Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_plans WHERE status=3"));return true;
        });
    }
    [Fact]
    public async Task AtomicReplacementKeepsRemoteIdentityAndOriginalLedgerAcrossRestart()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await WindowsFreshCycleTests.Fixture.Create();
        var path=Path.Combine(f.Source,"Агентность.md");File.WriteAllText(path,"original bytes");
        await Medium(async()=>
        {
            string scope;object? remote;object? original;
            await using(var engine=await Open(f))
            {
                Assert.Null((await Initial(engine)).Error);scope=engine.ScopeId;
                remote=await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE kind=0");
                original=await f.Scalar("SELECT entry_json||input_json||capture_proof FROM local_cycle_work WHERE work_no=1");
                var temp=Path.Combine(f.Directory,"save.tmp");File.WriteAllText(temp,"replacement bytes");File.Replace(temp,path,null);
                var result=await Converge(engine);await f.Evidence("atomic-replace-before-restart",result);
                Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(1,result.PublishedFiles);
                Assert.Equal(remote,await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE kind=0"));
                Assert.Equal(original,await f.Scalar("SELECT entry_json||input_json||capture_proof FROM local_cycle_work WHERE work_no=1"));
            }
            await using var resumed=await Open(f,true);Assert.Equal(scope,resumed.ScopeId);
            File.WriteAllText(path,"in-place after restart");var next=await Converge(resumed);
            Assert.Null(next.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",next.Decision);Assert.Equal(1,next.PublishedFiles);
            Assert.Equal(remote,await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE kind=0"));Assert.Equal("match",f.Remote.LastInput!.Intent.Condition.Kind);
            var noop=await resumed.FreshSyncAsync(Ct);Assert.Null(noop.Error);Assert.Equal(0,noop.CapturedFiles);Assert.Equal(0,noop.PublishedFiles);
            Assert.Single(Directory.GetFiles(f.Source));Assert.Equal("in-place after restart",File.ReadAllText(path));return true;
        });
    }
    [Fact]
    public async Task ChangedParentClosureRetriesWithFreshProofAfterRestart()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"base");
        await Medium(async()=>
        {
            var mutate=false;string scope;
            await using(var engine=await Open(f,observer:(o,t)=>{if(mutate&&o.Stage==WindowsCycleStage.BranchEnumerated){mutate=false;File.WriteAllText(Path.Combine(f.Source,"late"),"during scan");}return Task.CompletedTask;}))
            {
                Assert.Null((await Initial(engine)).Error);scope=engine.ScopeId;File.WriteAllText(Path.Combine(f.Source,"new"),"new");mutate=true;
                var changed=await engine.FreshSyncAsync(Ct);Assert.Contains("changed",changed.Error,StringComparison.OrdinalIgnoreCase);Assert.Equal(0,changed.PublishedFiles);
            }
            await using var resumed=await Open(f,true);Assert.Equal(scope,resumed.ScopeId);await resumed.RetryDeferredAsync(Ct);
            var recovered=await Converge(resumed);await f.Evidence("changed-parent-recovery",recovered);
            Assert.Null(recovered.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",recovered.Decision);Assert.Equal(2,recovered.PublishedFiles);return true;
        });
    }
    private static async Task BeginRetainedHardlinkPlan(WindowsFreshSyncEngine engine)
    {
        var session=(LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(engine)!;
        var coordinator=(WindowsLocalCoherenceCoordinator)typeof(WindowsFreshSyncEngine).GetField("_coordinator",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(engine)!;
        var coherence=(string)typeof(WindowsFreshSyncEngine).GetField("_coherence",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(engine)!;
        await coordinator.ObserveChangesAsync(session,coherence,Ct);
        await session.BeginParentReconciliationAsync(engine.ScopeId,Ct);
    }
    private static async Task<WindowsCycleResult> Converge(WindowsFreshSyncEngine engine,WindowsFreshCycleTests.Fixture? quietFixture=null)
    {
        WindowsCycleResult result=null!;long published=0,captured=0,directories=0;
        for(var attempt=0;attempt<30;attempt++)
        {
            // Exact capture-count assertions must enter through the same quiet
            // deadline as the production watcher scheduler. Polling earlier can
            // safely re-dirty an already captured generation with a late hint.
            if(quietFixture is not null)await quietFixture.WaitDueAsync();
            result=await engine.FreshSyncAsync(Ct);published+=result.PublishedFiles;captured+=result.CapturedFiles;directories+=result.PublishedDirectories;
            if(result.Error is null&&result.Decision=="NoWorkAtAppliedEventBoundary")break;
            await Task.Delay(500,Ct);
        }
        return result with{PublishedFiles=published,CapturedFiles=captured,PublishedDirectories=directories};
    }
    [Fact]
    public async Task RepeatedAtomicSavesAndTemporaryAbsenceKeepOneRemoteFile()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();var path=Path.Combine(f.Source,"note.md");File.WriteAllText(path,"base");
        await Medium(async()=>
        {
            await using var engine=await Open(f);Assert.Null((await Initial(engine)).Error);var remote=await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE kind=0");
            for(var i=0;i<6;i++)
            {
                var temp=Path.Combine(f.Directory,"save.tmp");File.WriteAllText(temp,"save "+i);if(i%2==0)File.Replace(temp,path,null);else File.Move(temp,path,true);
                var result=await Converge(engine);Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(remote,await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE kind=0"));
            }
            var held=Path.Combine(f.Directory,"outside-gap.md");File.Move(path,held);var gap=await engine.FreshSyncAsync(Ct);Assert.Null(gap.Error);Assert.Equal(2,f.Remote.Count); // root folder plus one file
            File.Move(held,path);var restored=await Converge(engine);Assert.Null(restored.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",restored.Decision);
            Assert.Equal(remote,await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE kind=0"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE kind=0"));Assert.Single(Directory.GetFiles(f.Source));Assert.Equal("save 5",File.ReadAllText(path));return true;
        });
    }
    [Fact]
    public async Task AtomicSaveAfterLostCompletionRecoversOriginalBeforeLatestBytes()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();var path=Path.Combine(f.Source,"note.md");File.WriteAllText(path,"base");
        await Medium(async()=>
        {
            object? remote,original;string operation;
            await using(var engine=await Open(f))
            {
                Assert.Null((await Initial(engine)).Error);remote=await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE kind=0");f.Remote.LoseComplete=true;File.WriteAllText(path,"attempted old bytes");
                for(var i=0;i<20;i++){await engine.FreshSyncAsync(Ct);if(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE active=1 AND phase=4")is string)break;await Task.Delay(250,Ct);}
                operation=Assert.IsType<string>(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE active=1 AND phase=4"));original=await f.Scalar("SELECT entry_json||input_json||capture_proof FROM local_cycle_work WHERE operation_id='"+operation+"'");
                var temp=Path.Combine(f.Directory,"save.tmp");File.WriteAllText(temp,"latest bytes");File.Replace(temp,path,null);
            }
            await using var resumed=await Open(f,true);var result=await Converge(resumed);await f.Evidence("atomic-save-original-recovery",result);
            Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(3,f.Remote.Prepares);Assert.Equal(3,f.Remote.Completes);
            Assert.Equal(original,await f.Scalar("SELECT entry_json||input_json||capture_proof FROM local_cycle_work WHERE operation_id='"+operation+"'"));Assert.Equal(remote,await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE kind=0"));
            Assert.Equal(Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes("latest bytes"))),f.Remote.LastInput!.Intent.Sha256);Assert.Single(Directory.GetFiles(f.Source));return true;
        });
    }
    [Fact]
    public async Task ParentIncarnationMigrationBacksUpV2WalAndRetainsOriginals()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"note"),"base");
        await Medium(async()=>{await using var e=await Open(f);Assert.Null((await Initial(e)).Error);File.WriteAllText(Path.Combine(f.Source,"other"),"new");Assert.Null((await Converge(e)).Error);return true;});
        var original=await f.Scalar("SELECT group_concat(entry_json||input_json||capture_proof) FROM local_cycle_work");
        await using var held=new Microsoft.Data.Sqlite.SqliteConnection("Data Source="+f.Database+";Pooling=False");await held.OpenAsync(Ct);
        await using(var q=held.CreateCommand()){q.CommandText="DROP TABLE local_parent_reconcile_orphan_closes;DROP TABLE local_parent_reconcile_incarnations;UPDATE local_parent_reconcile_format SET version=2;PRAGMA journal_mode=WAL;PRAGMA wal_autocheckpoint=0;CREATE TABLE incarnation_wal(value TEXT);INSERT INTO incarnation_wal VALUES('preserved')";await q.ExecuteNonQueryAsync(Ct);}
        await Medium(async()=>{await using var e=await Open(f,true);File.WriteAllText(Path.Combine(f.Source,"third"),"third");Assert.Null((await Converge(e)).Error);return true;});
        Assert.Equal(4L,await f.Scalar("SELECT version FROM local_parent_reconcile_format"));var rollback=await f.Store.CreateParentIncarnationRollbackCopyAsync(Path.Combine(f.Directory,"rollback-v2.db"),Ct);
        await using var old=new Microsoft.Data.Sqlite.SqliteConnection("Data Source="+rollback+";Mode=ReadOnly;Pooling=False");await old.OpenAsync(Ct);await using var read=old.CreateCommand();read.CommandText="SELECT version FROM local_parent_reconcile_format";Assert.Equal(2L,await read.ExecuteScalarAsync(Ct));read.CommandText="SELECT value FROM incarnation_wal";Assert.Equal("preserved",await read.ExecuteScalarAsync(Ct));read.CommandText="SELECT group_concat(entry_json||input_json||capture_proof) FROM local_cycle_work";Assert.Equal(original,await read.ExecuteScalarAsync(Ct));
    }
    private static async Task<WindowsCycleResult> Initial(WindowsFreshSyncEngine engine)
    {
        var result=await engine.InitialSyncAsync(Ct);
        if(result.Error is not null||result.Decision=="NoWorkAtAppliedEventBoundary")return result;
        var settled=await Converge(engine);
        return settled with{PublishedFiles=result.PublishedFiles+settled.PublishedFiles,CapturedFiles=result.CapturedFiles+settled.CapturedFiles,PublishedDirectories=result.PublishedDirectories+settled.PublishedDirectories};
    }
    private static async Task<WindowsCycleResult> WaitForDeferred(WindowsFreshSyncEngine engine)
    {
        WindowsCycleResult result=null!;
        for(var i=0;i<12;i++){result=await engine.FreshSyncAsync(Ct);if(result.Deferred>0||result.Error is not null)break;await Task.Delay(250,Ct);}
        return result;
    }
    [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]private static extern bool CreateHardLinkW(string link,string target,nint security);
    [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]private static extern SafeFileHandle CreateFileW(string path,uint access,uint share,nint security,uint creation,uint flags,nint template);
    [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]private static extern bool SetFileInformationByHandle(SafeFileHandle handle,int kind,ref uint value,uint size);
}
