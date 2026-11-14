using System.Net;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsFreshCycleTests
{
    private static Task<WindowsFreshSyncEngine> CursorEngine(Fixture f,CursorRemote remote)=>WindowsFreshSyncEngine.ResumeOrOpenAsync(f.Store,f.Source,Path.Combine(f.Directory,"spool"),AnchoredConfiguration(f),remote,new(MinimumFreeDiskBytes:0),cancellationToken:Ct);
    [Theory]
    [InlineData(410,"cursor_expired")][InlineData(409,"journal_gap")][InlineData(400,"invalid_cursor")]
    public async Task ActualQualifiedCursorGapInstallsSeparateRWithoutCaptureOrBaselineReset(int status,string code)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"closed original");var r=new CursorRemote(f.Remote);await using var e=await CursorEngine(f,r);Assert.Null((await e.InitialSyncAsync(Ct)).Error);
        var b=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0");var legacy=await f.Scalar("SELECT ready_json FROM local_cycle_scopes");r.Failure=new((HttpStatusCode)status,code,true);r.Failures=1;
        var result=await e.FreshSyncAsync(Ct);await f.Evidence("cursor-recovery",new{result,r.Starts,r.EntryReads,metrics=f.Store.GetLocalCycleMetrics()});Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(0,result.CapturedFiles);Assert.Equal(1,f.Remote.Prepares);Assert.Equal(2,r.Starts);Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));Assert.Equal(legacy,await f.Scalar("SELECT ready_json FROM local_cycle_scopes"));Assert.NotNull(await f.Scalar("SELECT active_remote FROM local_cycle_scopes"));Assert.Null(await f.Scalar("SELECT pending_remote FROM local_cycle_scopes")as string);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_refreshes WHERE status=1"));
        var repeat=await e.FreshSyncAsync(Ct);Assert.Null(repeat.Error);Assert.Equal(0,repeat.CapturedBytes);Assert.Equal(2,r.Starts);
    }
    [Theory]
    [InlineData(410,"invalid_cursor",true)][InlineData(410,"root_unavailable",true)][InlineData(409,"binding_changed",true)][InlineData(403,"cursor_expired",true)][InlineData(410,"cursor_expired",false)]
    [InlineData(400,"invalid_cursor",false)][InlineData(403,"invalid_cursor",true)][InlineData(500,"invalid_cursor",true)]
    public async Task ActualUnqualifiedWireFailureNeverCreatesStageOrChangesOriginalB(int status,string code,bool recognized)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"original");var r=new CursorRemote(f.Remote);await using var e=await CursorEngine(f,r);Assert.Null((await e.InitialSyncAsync(Ct)).Error);var b=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0");r.Failure=new((HttpStatusCode)status,code,recognized);r.Failures=1;var result=await e.FreshSyncAsync(Ct);Assert.NotNull(result.Error);Assert.Equal("PendingOrUncertain",result.Decision);Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_refreshes"));Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));Assert.Equal(1,f.Remote.Prepares);
    }
    [Fact]
    public async Task ActualInvalidCursorFromStageEntriesDoesNotAuthorizeAnotherRefresh()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"original");var r=new CursorRemote(f.Remote);await using var e=await CursorEngine(f,r);Assert.Null((await e.InitialSyncAsync(Ct)).Error);
        var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0");r.Failures=1;r.InvalidStageEntry=true;
        var result=await e.FreshSyncAsync(Ct);Assert.NotNull(result.Error);Assert.Contains("invalid_cursor",result.Error);Assert.Equal(2,r.Starts);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_refreshes"));Assert.Null(await f.Scalar("SELECT active_remote FROM local_cycle_scopes")as string);Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));Assert.Equal(1,f.Remote.Prepares);
    }
    [Fact]
    public async Task ActualStagePageFailureRestartUsesOriginalSnapshotAndNeverExposesPartialR()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"source");var r=new CursorRemote(f.Remote);string scope,b,originalStage;
        await using(var e=await CursorEngine(f,r))
        {
            scope=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);b=(string)(await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"))!;for(var i=0;i<600;i++)f.Remote.AddExisting("unrelated-"+i,"remote");r.Failures=1;r.FailPageAfterFirst=true;
            var stopped=await e.FreshSyncAsync(Ct);Assert.NotNull(stopped.Error);originalStage=(string)(await f.Scalar("SELECT pending_remote FROM local_cycle_scopes"))!;Assert.NotNull(originalStage);Assert.Null(await f.Scalar("SELECT active_remote FROM local_cycle_scopes")as string);Assert.Equal(500L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_refresh_remote"));Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));
        }
        r.FailPageAfterFirst=false;await using var restored=await CursorEngine(f,r);Assert.Equal(scope,restored.ScopeId);var finished=await restored.FreshSyncAsync(Ct);Assert.Null(finished.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",finished.Decision);Assert.Equal(originalStage,await f.Scalar("SELECT active_remote FROM local_cycle_scopes"));Assert.Equal(2,r.Starts);Assert.Equal(0,finished.CapturedFiles);Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));Assert.Equal(602L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_refresh_remote"));
    }
    [Fact]
    public async Task ActualGapRecoversHistoricalOriginalThenKeepsLaterDirtyGeneration()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());var path=Path.Combine(f.Source,"file");File.WriteAllText(path,"original");f.Remote.LoseComplete=true;var r=new CursorRemote(f.Remote);await using var e=await CursorEngine(f,r);Assert.NotNull((await e.InitialSyncAsync(Ct)).Error);var input=await f.Scalar("SELECT input_json FROM local_cycle_work WHERE work_no=1");var upload=await f.Scalar("SELECT upload_id FROM local_cycle_work WHERE work_no=1");r.Failures=1;r.OnStageEntries=()=>File.WriteAllText(path,"later closed generation");var final=await f.FreshWhenDueAsync(e,Ct);Assert.Null(final.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",final.Decision);Assert.Equal(input,await f.Scalar("SELECT input_json FROM local_cycle_work WHERE work_no=1"));Assert.Equal(upload,await f.Scalar("SELECT upload_id FROM local_cycle_work WHERE work_no=1"));Assert.Equal(2,f.Remote.Prepares);Assert.Equal(1,final.CapturedFiles);Assert.Equal(2L,await f.Scalar("SELECT baseline_work FROM local_cycle_files WHERE kind=0"));
    }
    [Fact]
    public async Task ActualReplacementRemoteChangeBecomesConflictWithoutOverwriteOrFalseNoop()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"original");var r=new CursorRemote(f.Remote);await using var e=await CursorEngine(f,r);Assert.Null((await e.InitialSyncAsync(Ct)).Error);var b=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0");r.Failures=1;r.ChangeStageDescriptors=true;var final=await e.FreshSyncAsync(Ct);Assert.Null(final.Error);Assert.Equal("PendingOrUncertain",final.Decision);Assert.Equal(1,final.Conflicts);Assert.Equal(0,final.CapturedFiles);Assert.Equal(1,f.Remote.Prepares);Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));var again=await e.FreshSyncAsync(Ct);Assert.Equal(1,again.Conflicts);Assert.Equal("PendingOrUncertain",again.Decision);
    }
    [Fact]
    public async Task ActualGapBudgetIsTwoNewStagesAndCurrentRRemainsUnchanged()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"original");var r=new CursorRemote(f.Remote);await using var e=await CursorEngine(f,r);Assert.Null((await e.InitialSyncAsync(Ct)).Error);r.Failures=100;var final=await e.FreshSyncAsync(Ct);Assert.NotNull(final.Error);Assert.Equal(3,r.Starts);Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_refreshes"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_refreshes WHERE status=2"));Assert.Null(await f.Scalar("SELECT active_remote FROM local_cycle_scopes")as string);Assert.NotNull(await f.Scalar("SELECT pending_remote FROM local_cycle_scopes")as string);Assert.Equal(1,f.Remote.Prepares);
        var retired=await e.RetireRemoteHistoryBatchAsync(Ct);Assert.InRange(retired.Records,0,120);Assert.InRange(retired.ChangedRows,0,123);Assert.NotNull(await f.Scalar("SELECT pending_remote FROM local_cycle_scopes")as string);
    }
    [Theory]
    [InlineData("anchor")][InlineData("native-gap")][InlineData("binding")]
    public async Task ActualStageIdentityOrNativeGapCannotInstallCandidateRemote(string kind)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"source");var r=new CursorRemote(f.Remote);await using var e=await CursorEngine(f,r);Assert.Null((await e.InitialSyncAsync(Ct)).Error);var b=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0");r.Failures=1;r.ChangeStageAnchor=kind=="anchor";r.ChangeStageBinding=kind=="binding";
        if(kind=="native-gap")r.OnStageEntries=()=>Directory.CreateDirectory(Path.Combine(f.Source,"unresolved-new-branch"));
        var result=await e.FreshSyncAsync(Ct);Assert.NotNull(result.Error);Assert.Null(await f.Scalar("SELECT active_remote FROM local_cycle_scopes")as string);Assert.NotNull(await f.Scalar("SELECT pending_remote FROM local_cycle_scopes")as string);Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));Assert.Equal(1,f.Remote.Prepares);Assert.Equal(0,result.CapturedFiles);await f.Evidence("cursor-stage-refused-"+kind,result);
    }
    [Fact]
    public async Task ActualLostStageStartReplyResumesOriginalUuidAndRetiresOnlySupersededGeneration()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"source");var r=new CursorRemote(f.Remote);string pending,scope;
        await using(var e=await CursorEngine(f,r)){scope=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);r.Failures=1;r.LoseStageStart=true;Assert.NotNull((await e.FreshSyncAsync(Ct)).Error);pending=(string)(await f.Scalar("SELECT pending_remote FROM local_cycle_scopes"))!;Assert.Null(await f.Scalar("SELECT ready_json FROM local_cycle_refreshes")as string);}
        await using var restored=await CursorEngine(f,r);Assert.Equal(scope,restored.ScopeId);Assert.Null((await restored.FreshSyncAsync(Ct)).Error);Assert.Equal(pending,await f.Scalar("SELECT active_remote FROM local_cycle_scopes"));Assert.Equal(r.StartIds[1],r.StartIds[2]);Assert.Equal(0,(await restored.RetireRemoteHistoryBatchAsync(Ct)).Records);
        r.Failures=1;Assert.Null((await restored.FreshSyncAsync(Ct)).Error);var active=(string)(await f.Scalar("SELECT active_remote FROM local_cycle_scopes"))!;Assert.NotEqual(pending,active);var b=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0");
        for(var i=0;i<4;i++){var page=await restored.RetireRemoteHistoryBatchAsync(Ct);Assert.InRange(page.Records,0,120);Assert.InRange(page.ChangedRows,0,123);}
        Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_refreshes"));Assert.Equal(active,await f.Scalar("SELECT active_remote FROM local_cycle_scopes"));Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));Assert.Equal("NoWorkAtAppliedEventBoundary",(await restored.FreshSyncAsync(Ct)).Decision);
    }
    [Fact]
    public async Task ActualFormatTwoWalMigrationPreservesOriginalStateAndSeparateRollback()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"historical original");string scope,input,b;
        await using(var e=await CursorEngine(f,new(f.Remote))){scope=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);}
        // Read original work without relying on a new schema-specific field.
        input=(string)(await f.Scalar("SELECT input_json FROM local_cycle_work WHERE work_no=1"))!;b=(string)(await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"))!;
        await f.Store.AddMappingAsync(new("paused-refresh","client",Path.Combine(f.Directory,"paused-mapping"),false,DateTimeOffset.UtcNow),Ct);
        // The extra legacy write deliberately invalidates this scope. Migration
        // must preserve it rather than manufacture a valid new source anchor.
        await ExecuteSql(f,"DROP TABLE local_cycle_refresh_conflicts;DROP TABLE local_cycle_refresh_remote;DROP TABLE local_cycle_refreshes;ALTER TABLE local_cycle_scopes DROP COLUMN active_remote;ALTER TABLE local_cycle_scopes DROP COLUMN pending_remote;ALTER TABLE local_cycle_format RENAME TO old_refresh_format;CREATE TABLE local_cycle_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL CHECK(version=2),bytes INTEGER NOT NULL DEFAULT 0);INSERT INTO local_cycle_format SELECT singleton,2,bytes FROM old_refresh_format;DROP TABLE old_refresh_format;");
        await using(var c=new SqliteConnection("Data Source="+f.Database+";Cache=Private;Pooling=False"))
        {
            await c.OpenAsync(Ct);await using var q=c.CreateCommand();q.CommandText="PRAGMA journal_mode=WAL;PRAGMA wal_autocheckpoint=0;CREATE TABLE refresh_wal_probe(value TEXT);INSERT INTO refresh_wal_probe VALUES('committed WAL before migration')";await q.ExecuteNonQueryAsync(Ct);
            await Assert.ThrowsAsync<LocalSourceGapException>(()=>CursorEngine(f,new(f.Remote)));
        }
        Assert.Equal(4L,await f.Scalar("SELECT version FROM local_cycle_format"));Assert.Equal(scope,await f.Scalar("SELECT scope_id FROM local_cycle_scopes"));Assert.Equal(input,await f.Scalar("SELECT input_json FROM local_cycle_work WHERE work_no=1"));Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));Assert.Equal(0L,await f.Scalar("SELECT enabled FROM sync_mappings WHERE mapping_id='paused-refresh'"));
        var rollback=Path.Combine(f.Directory,"v2-rollback.db");await f.Store.CreateLocalCycleV2RollbackCopyAsync(rollback,Ct);await using var copy=new SqliteConnection("Data Source="+rollback+";Mode=ReadOnly;Cache=Private;Pooling=False");await copy.OpenAsync(Ct);await using var check=copy.CreateCommand();check.CommandText="SELECT version FROM local_cycle_format";Assert.Equal(2L,await check.ExecuteScalarAsync(Ct));check.CommandText="SELECT value FROM refresh_wal_probe";Assert.Equal("committed WAL before migration",await check.ExecuteScalarAsync(Ct));check.CommandText="SELECT input_json FROM local_cycle_work WHERE work_no=1";Assert.Equal(input,await check.ExecuteScalarAsync(Ct));
        var evidence=Environment.GetEnvironmentVariable("NEPTUNE_FRESH_CYCLE_EVIDENCE_ROOT");if(evidence is not null){Directory.CreateDirectory(evidence);await using var source=new SqliteConnection("Data Source="+f.Database+";Cache=Private;Pooling=False");await source.OpenAsync(Ct);await using var destination=new SqliteConnection("Data Source="+Path.Combine(evidence,"format3-old-client-probe.db")+";Cache=Private;Pooling=False");await destination.OpenAsync(Ct);source.BackupDatabase(destination);}
    }
    [Theory]
    [InlineData("start")][InlineData("step")][InlineData("ready")][InlineData("entries")]
    public async Task ActualSnapshotAdmissionRetriesSameStageWithoutDeferringWholeCycle(string phase)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"original");var r=new CursorRemote(f.Remote);await using var e=await CursorEngine(f,r);Assert.Null((await e.InitialSyncAsync(Ct)).Error);
        var b=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0");r.Failures=1;r.StageBusy=1;r.BusyPhase=phase;
        var result=await e.FreshSyncAsync(Ct);Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(0,r.StageBusy);Assert.Equal(2,r.Starts);Assert.Equal(0,result.CapturedFiles);Assert.Equal(1,f.Remote.Prepares);Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));
        Assert.Single(r.AdmissionIds.Distinct());Assert.Equal(r.StartIds[1],r.AdmissionIds[0]);
    }
    [Theory]
    [InlineData("cursor_expired")][InlineData("stage_unavailable")]
    public async Task ActualExpiredPendingSnapshotReplacesOnlyRemoteCandidate(string code)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"original");var r=new CursorRemote(f.Remote);await using var e=await CursorEngine(f,r);Assert.Null((await e.InitialSyncAsync(Ct)).Error);
        var b=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0");r.Failures=1;r.ExpiredStage=code;
        var result=await e.FreshSyncAsync(Ct);Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(3,r.Starts);Assert.NotEqual(r.StartIds[1],r.StartIds[2]);Assert.Equal(0,result.CapturedFiles);Assert.Equal(1,f.Remote.Prepares);Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));Assert.Null(await f.Scalar("SELECT pending_remote FROM local_cycle_scopes")as string);
    }
    [Theory]
    [InlineData(410,"root_unavailable",true)][InlineData(403,"stage_unavailable",true)][InlineData(410,"stage_unavailable",false)][InlineData(500,"stage_unavailable",true)]
    public async Task ActualUnqualifiedPendingStageFailureDoesNotAuthorizeReplacement(int status,string code,bool recognized)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"original");var r=new CursorRemote(f.Remote);await using var e=await CursorEngine(f,r);Assert.Null((await e.InitialSyncAsync(Ct)).Error);
        var b=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0");r.Failures=1;r.StageFailure=new((HttpStatusCode)status,code,recognized);
        var result=await e.FreshSyncAsync(Ct);Assert.NotNull(result.Error);Assert.Equal(2,r.Starts);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_refreshes"));Assert.Null(await f.Scalar("SELECT active_remote FROM local_cycle_scopes")as string);Assert.Equal(0,result.CapturedFiles);Assert.Equal(1,f.Remote.Prepares);Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));
    }
    private sealed class CursorRemote(FakeRemote inner):IWindowsCycleRemote
    {
        public SyncRootBinding Binding=>inner.Binding;internal int Starts,EntryReads,Failures;internal bool FailPageAfterFirst,ChangeStageDescriptors,ChangeStageAnchor,ChangeStageBinding,LoseStageStart,InvalidStageEntry;internal readonly List<string> StartIds=[];internal Action? OnStageEntries;internal GuardedCycleRemoteException Failure=new(HttpStatusCode.Gone,"cursor_expired",true);
        internal int StageBusy;internal string BusyPhase="start";internal string? ExpiredStage;internal GuardedCycleRemoteException? StageFailure;internal readonly List<string> AdmissionIds=[];
        private void Busy(string phase,string id){if(Starts>1&&phase==BusyPhase&&StageBusy>0){AdmissionIds.Add(id);StageBusy--;throw new GuardedCycleRemoteException(HttpStatusCode.TooManyRequests,"admission_full",true);}}
        public async Task<byte[]> StartSnapshotAsync(string id,CancellationToken ct){Starts++;StartIds.Add(id);try{Busy("start",id);}catch{Starts--;StartIds.RemoveAt(StartIds.Count-1);throw;}if(Starts>1&&StageFailure is {} failure)throw failure;if(Starts>1&&ExpiredStage is {} code){ExpiredStage=null;throw new GuardedCycleRemoteException(HttpStatusCode.Gone,code,true);}var result=await inner.StartSnapshotAsync(id,ct);if(Starts>1&&LoseStageStart){LoseStageStart=false;throw new IOException("Lost stage start acknowledgement");}if(Starts>1&&StageBusy>0&&BusyPhase=="step"){var node=System.Text.Json.Nodes.JsonNode.Parse(result)!;node["state"]="BASE";return System.Text.Encoding.UTF8.GetBytes(node.ToJsonString());}return result;}
        public Task<byte[]> StepSnapshotAsync(string id,CancellationToken ct){Busy("step",id);return inner.StepSnapshotAsync(id,ct);}
        public Task<SyncSnapshotReady> ReadySnapshotAsync(string id,CancellationToken ct){Busy("ready",id);return inner.ReadySnapshotAsync(id,ct);}
        public async Task<SyncSnapshotEntriesPage> ReadEntriesAsync(SyncSnapshotReady ready,string cursor,string? after,CancellationToken ct)
        {
            Busy("entries",ready.SnapshotId);
            if(Starts>1&&InvalidStageEntry)throw new GuardedCycleRemoteException(HttpStatusCode.BadRequest,"invalid_cursor",true);
            EntryReads++;if(Starts>1&&after is not null&&FailPageAfterFirst)throw new IOException("Owned stage page reply interrupted.");if(Starts>1&&OnStageEntries is {} action){OnStageEntries=null;action();}var page=await inner.ReadEntriesAsync(ready,cursor,after,ct);var items=page.Items.Where(d=>after is null||StringComparer.Ordinal.Compare(d.NodeId,after)>0).Take(500).ToArray();var more=page.Items.Count(d=>after is null||StringComparer.Ordinal.Compare(d.NodeId,after)>0)>500;
            if(Starts>1&&ChangeStageDescriptors)items=items.Select(d=>d.Type=="file"?d with{Version="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}:d).ToArray();if(Starts>1&&ChangeStageAnchor)items=items.Select(d=>d.NodeId==Binding.RootId?d with{Name="changed-root-name"}:d).ToArray();return new(Starts>1&&ChangeStageBinding?Binding with{ServerEpoch="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}:Binding,ready.SnapshotId,ready.CutSeq,after,items.LastOrDefault()?.NodeId,items,"entries-"+(items.LastOrDefault()?.NodeId??"done"),more);
        }
        public Task<SyncFeedPage> ReadChangesAsync(string cursor,long seq,CancellationToken ct){if(Failures>0){Failures--;throw Failure;}return inner.ReadChangesAsync(cursor,seq,ct);}
        public Task<CycleFolderOutcome> EnsureDirectoryAsync(CycleFolderInput input,CancellationToken ct)=>inner.EnsureDirectoryAsync(input,ct);
        public Task<CycleUploadStatus> UploadAsync(string command,CycleUploadInput input,string? upload,string hash,CancellationToken ct)=>inner.UploadAsync(command,input,upload,hash,ct);
        public Task<CycleUploadStatus> AppendAsync(CycleUploadInput input,string upload,string hash,long offset,ReadOnlyMemory<byte> bytes,CancellationToken ct)=>inner.AppendAsync(input,upload,hash,offset,bytes,ct);
    }
}
