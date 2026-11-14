using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsFreshCycleTests
{
    [Theory]
    [InlineData(false)][InlineData(true)]
    public async Task ActualRestartBeforeInitialScanReusesEmptyScopeWithoutClaimingAnchor(bool edited)
    {
        if(!OperatingSystem.IsWindows()){Assert.Skip("Requires Windows native filesystem fixture");return;}
        await using var f=await Fixture.Create();var file=Path.Combine(f.Source,"file");File.WriteAllText(file,"original");string scope;
        await using(var first=await Resume(f)){scope=first.ScopeId;Assert.False(first.HasRetainedAnchor);}
        Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work"));
        if(edited)File.WriteAllText(file,"edited while the first scan had not started");
        await using(var next=await Resume(f)){
            Assert.Equal(scope,next.ScopeId);Assert.False(next.HasRetainedAnchor);
            var result=await next.InitialSyncAsync(Ct);Assert.Null(result.Error);Assert.Equal(1,result.PublishedFiles);
        }
        await using var complete=await Resume(f);Assert.True(complete.HasRetainedAnchor);Assert.Equal(scope,complete.ScopeId);
        Assert.Null((await complete.FreshSyncAsync(Ct)).Error);Assert.Equal(1,f.Remote.Prepares);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_scopes"));
    }
    private static Task<WindowsFreshSyncEngine> Resume(Fixture f,WindowsCycleConfiguration? configuration=null,string? spool=null,
        Func<WindowsCycleObservation,CancellationToken,Task>? observer=null)=>WindowsFreshSyncEngine.ResumeOrOpenAsync(f.Store,f.Source,spool??Path.Combine(f.Directory,"spool"),configuration??AnchoredConfiguration(f),f.Remote,new(MinimumFreeDiskBytes:0),observer:observer,cancellationToken:Ct);
    [Fact]
    public async Task ActualTwentyEngineLifetimesRetainOneAnchorBaselineAndZeroContentReads()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"closed bytes");Directory.CreateDirectory(Path.Combine(f.Source,"empty"));var branches=0;string scope,coherence;
        await using(var e=await Resume(f,observer:(o,ct)=>{if(o.Stage==WindowsCycleStage.BranchEnumerated)branches++;return Task.CompletedTask;}))
        {Assert.False(e.HasRetainedAnchor);var initial=await e.InitialSyncAsync(Ct);Assert.Null(initial.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",initial.Decision);scope=e.ScopeId;coherence=initial.CoherenceId!;}
        var branchCount=branches;var baselines=(string)(await f.Scalar("SELECT group_concat(baseline,'|') FROM local_cycle_files WHERE baseline IS NOT NULL"))!;
        for(var i=0;i<21;i++)
        {
            await using var e=await Resume(f,observer:(o,ct)=>{if(o.Stage==WindowsCycleStage.BranchEnumerated)branches++;return Task.CompletedTask;});Assert.True(e.HasRetainedAnchor);Assert.Equal(scope,e.ScopeId);
            var cycle=await e.FreshSyncAsync(Ct);Assert.Null(cycle.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",cycle.Decision);Assert.Equal(coherence,cycle.CoherenceId);Assert.Equal(0,cycle.CapturedFiles);Assert.Equal(0,cycle.CapturedBytes);Assert.Equal(0,cycle.PublishedFiles);Assert.Equal(branchCount,branches);
        }
        Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_scopes"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_namespace_candidates"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_coherence_roots"));Assert.Equal(baselines,await f.Scalar("SELECT group_concat(baseline,'|') FROM local_cycle_files WHERE baseline IS NOT NULL"));Assert.Equal(1,f.Remote.Prepares);
        await f.Evidence("resume21",new{scope,coherence,branches,f.Remote.Prepares,owner=await f.Scalar("SELECT owner_epoch FROM local_cycle_scopes")});
    }
    [Fact]
    public async Task ActualOfflineClosedWriteBecomesOneDirtyWithOriginalBaselineCas()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();var file=Path.Combine(f.Source,"file");File.WriteAllText(file,"old");string scope;
        await using(var e=await Resume(f)){scope=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);}File.WriteAllText(file,"changed while process was absent");
        await using var restored=await Resume(f);var result=await restored.FreshSyncAsync(Ct);Assert.Equal(scope,restored.ScopeId);Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(1,result.CapturedFiles);Assert.Equal(1,result.PublishedFiles);Assert.Equal("match",f.Remote.LastInput!.Intent.Condition.Kind);Assert.Equal(2,f.Remote.Prepares);
    }
    [Theory]
    [InlineData("prepare")][InlineData("complete")]
    public async Task ActualRestartRecoversIdenticalOriginalWithoutCaptureOrNewOperation(string lost)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());File.WriteAllText(Path.Combine(f.Source,"file"),"immutable original");f.Remote.LosePrepare=lost=="prepare";f.Remote.LoseComplete=lost=="complete";string scope,input,operation;
        await using(var e=await Resume(f)){scope=e.ScopeId;Assert.NotNull((await e.InitialSyncAsync(Ct)).Error);operation=(string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE input_json IS NOT NULL"))!;input=(string)(await f.Scalar("SELECT input_json FROM local_cycle_work WHERE operation_id='"+operation+"'"))!;}
        await using var restored=await Resume(f);var result=await f.FreshWhenDueAsync(restored,Ct);Assert.True(restored.HasRetainedAnchor);Assert.Equal(scope,restored.ScopeId);Assert.Null(result.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",result.Decision);Assert.Equal(0,result.CapturedFiles);Assert.Equal(1,f.Remote.Prepares);Assert.Equal(1,f.Remote.Completes);Assert.Equal(input,await f.Scalar("SELECT input_json FROM local_cycle_work WHERE operation_id='"+operation+"'"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work"));Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory,"spool"),"*.spool"));
    }
    [Theory]
    [InlineData("anchor")][InlineData("rules")][InlineData("policy")]
    public async Task ActualFailedBindingResumeKeepsEvidenceAndLaterOriginalBindingCanResume(string kind)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"closed");string scope;
        await using(var e=await Resume(f)){scope=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);}var config=AnchoredConfiguration(f);config=kind=="anchor"?config with{RemoteAnchorId=Guid.NewGuid().ToString("D")}:kind=="rules"?config with{RulesVersion="rules-v2"}:config with{AllowVerifiedEqualAdoption=true};
        if(kind=="rules")await Assert.ThrowsAsync<InvalidOperationException>(()=>Resume(f,config));else await Assert.ThrowsAnyAsync<IOException>(()=>Resume(f,config));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_scopes"));
        // Rules are part of local root identity, so rebinding may fence the
        // original subscription. Other failed config checks consume only an
        // owner epoch and must not poison a valid original anchor.
        if(kind!="rules"){await using var e=await Resume(f);Assert.Equal(scope,e.ScopeId);Assert.Null((await e.FreshSyncAsync(Ct)).Error);}
    }
    [Fact]
    public async Task ActualPausedMappingRefusesWithoutDeletingBaseline()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"closed");await using(var e=await Resume(f))Assert.Null((await e.InitialSyncAsync(Ct)).Error);var before=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0");
        await ExecuteSql(f,"UPDATE sync_mappings SET enabled=0");await Assert.ThrowsAnyAsync<IOException>(()=>Resume(f));Assert.Equal(before,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));
    }
    [Theory]
    [InlineData("journal-gap")][InlineData("legacy-write")]
    public async Task ActualDurableGapOrLegacyWriteRefusesResumeWithoutLosingOriginal(string cause)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"original bytes");f.Remote.LosePrepare=true;
        await using(var e=await Resume(f))
        {
            Assert.NotNull((await e.InitialSyncAsync(Ct)).Error);
            if(cause=="journal-gap")
            {
                var field=typeof(WindowsFreshSyncEngine).GetField("_session",System.Reflection.BindingFlags.Instance|System.Reflection.BindingFlags.NonPublic)!;
                await ((LocalSourceJournalSession)field.GetValue(e)!).MarkGapAsync("OwnedResumeRegressionGap",Ct);
            }
        }
        var original=await f.Scalar("SELECT input_json FROM local_cycle_work WHERE active=1");var operation=await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE active=1");
        if(cause=="legacy-write")await f.Store.UpsertSyncFileAsync(new("mapping","owned-legacy-write",1,DateTimeOffset.UtcNow,"hash",null,"synced",DateTimeOffset.UtcNow,null),Ct);
        await Assert.ThrowsAnyAsync<IOException>(()=>Resume(f));Assert.Equal(original,await f.Scalar("SELECT input_json FROM local_cycle_work WHERE active=1"));Assert.Equal(operation,await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE active=1"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_scopes"));Assert.Equal(1,f.Remote.Prepares);
    }
    [Fact]
    public async Task ActualSpoolRebindRefusesOriginalSealedWorkAndOriginalPathCanResume()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());File.WriteAllText(Path.Combine(f.Source,"file"),"retained bytes");f.Remote.LosePrepare=true;await using(var e=await Resume(f))Assert.NotNull((await e.InitialSyncAsync(Ct)).Error);
        var original=await f.Scalar("SELECT input_json FROM local_cycle_work WHERE active=1");var bytes=File.ReadAllBytes(Directory.GetFiles(Path.Combine(f.Directory,"spool"),"*.spool").Single());await Assert.ThrowsAnyAsync<IOException>(()=>Resume(f,spool:Path.Combine(f.Directory,"other-spool")));Assert.Equal(original,await f.Scalar("SELECT input_json FROM local_cycle_work WHERE active=1"));Assert.Equal(bytes,File.ReadAllBytes(Directory.GetFiles(Path.Combine(f.Directory,"spool"),"*.spool").Single()));
        await using var restored=await Resume(f);var result=await f.FreshWhenDueAsync(restored,Ct);Assert.Null(result.Error);Assert.Equal(0,result.CapturedFiles);Assert.Equal(1,f.Remote.Prepares);
    }
    [Theory]
    [InlineData("future")][InlineData("missing-index")]
    public async Task ActualFutureOrIncompleteCycleModuleRefusesBeforeWritableInitialize(string kind)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await using(var e=await Resume(f))Assert.Null((await e.InitialSyncAsync(Ct)).Error);
        await using var held=new SqliteConnection("Data Source="+f.Database+";Cache=Private;Pooling=False");await held.OpenAsync(Ct);await using(var q=held.CreateCommand())
        {
            q.CommandText=kind=="future"?"ALTER TABLE local_cycle_format RENAME TO owned_previous;CREATE TABLE local_cycle_format(singleton INTEGER PRIMARY KEY,version INTEGER NOT NULL,bytes INTEGER NOT NULL);INSERT INTO local_cycle_format SELECT singleton,4,bytes FROM owned_previous;DROP TABLE owned_previous":"DROP INDEX local_cycle_scopes_root";await q.ExecuteNonQueryAsync(Ct);
        }
        static string Hash(string path){if(!File.Exists(path))return "absent";using var input=new FileStream(path,FileMode.Open,FileAccess.Read,FileShare.ReadWrite|FileShare.Delete);return Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(input));}
        var before=Hash(f.Database)+Hash(f.Database+"-wal");await Assert.ThrowsAsync<InvalidDataException>(()=>f.Store.InitializeAsync(Ct));Assert.Equal(before,Hash(f.Database)+Hash(f.Database+"-wal"));
    }
    [Fact]
    public async Task ActualCancelledInitialScanIsNotRestoredAsCompleteAnchor()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();Directory.CreateDirectory(Path.Combine(f.Source,"late"));File.WriteAllText(Path.Combine(f.Source,"late","file"),"bytes");
        await using(var e=await Resume(f,observer:(o,ct)=>o.Stage==WindowsCycleStage.BranchEnumerated?Task.FromException(new OperationCanceledException("owned scan barrier")):Task.CompletedTask))Assert.NotNull((await e.InitialSyncAsync(Ct)).Error);
        await Assert.ThrowsAnyAsync<IOException>(()=>Resume(f));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_scopes"));
    }
    [Fact]
    public async Task ActualLatestBaselineAndUnknownOriginalPinHistoryWhileOlderTerminalWorkRetires()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());var file=Path.Combine(f.Source,"file");File.WriteAllText(file,"v1");await using var e=await Resume(f);Assert.Null((await e.InitialSyncAsync(Ct)).Error);File.WriteAllText(file,"v2");Assert.Null((await f.FreshWhenDueAsync(e,Ct)).Error);var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0");Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work"));
        var retired=await e.RetireHistoryBatchAsync(Ct);Assert.Equal(1,retired.Work.Records);Assert.InRange(retired.Work.ChangedRows,1,123);Assert.InRange(retired.Journal.ChangedRows,0,244);Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work"));
        File.WriteAllText(file,"v3");f.Remote.LosePrepare=true;var uncertain=await f.FreshWhenDueAsync(e,Ct);Assert.NotNull(uncertain.Error);var original=await f.Scalar("SELECT input_json FROM local_cycle_work WHERE active=1");await e.RetireHistoryBatchAsync(Ct);Assert.Equal(original,await f.Scalar("SELECT input_json FROM local_cycle_work WHERE active=1"));Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));var final=await f.FreshWhenDueAsync(e,Ct);Assert.Null(final.Error);Assert.Equal(0,final.CapturedFiles);Assert.Equal("NoWorkAtAppliedEventBoundary",final.Decision);
    }
    [Fact]
    public async Task ActualRetirementCursorWrapFindsFormerlyPinnedBaselineAndCancellationKeepsCursor()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());var file=Path.Combine(f.Source,"file");File.WriteAllText(file,"v1");await using var e=await Resume(f);Assert.Null((await e.InitialSyncAsync(Ct)).Error);Assert.Equal(0,(await e.RetireHistoryBatchAsync(Ct)).Work.Records);Assert.Equal(1L,await f.Scalar("SELECT work_retire_after FROM local_cycle_scopes"));
        File.WriteAllText(file,"v2");Assert.Null((await f.FreshWhenDueAsync(e,Ct)).Error);Assert.Equal(0,(await e.RetireHistoryBatchAsync(Ct)).Work.Records);Assert.Equal(2L,await f.Scalar("SELECT work_retire_after FROM local_cycle_scopes"));using var stopped=new CancellationTokenSource();stopped.Cancel();await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>e.RetireHistoryBatchAsync(stopped.Token));Assert.Equal(2L,await f.Scalar("SELECT work_retire_after FROM local_cycle_scopes"));Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work"));
        Assert.Equal(0,(await e.RetireHistoryBatchAsync(Ct)).Work.Records);Assert.Equal(0L,await f.Scalar("SELECT work_retire_after FROM local_cycle_scopes"));var retired=await e.RetireHistoryBatchAsync(Ct);Assert.Equal(1,retired.Work.Records);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work"));Assert.Equal(2L,await f.Scalar("SELECT baseline_work FROM local_cycle_files WHERE kind=0"));await f.Evidence("retire-cursor-wrap",retired);
    }
    private static async Task ExecuteSql(Fixture f,string sql){await using var c=new SqliteConnection("Data Source="+f.Database+";Cache=Private;Pooling=False");await c.OpenAsync(Ct);await using var q=c.CreateCommand();q.CommandText=sql;await q.ExecuteNonQueryAsync(Ct);}
    [Fact]
    public async Task ActualVersionOneWalMigrationPreservesPauseCheckpointBaselineAndOriginalIntent()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();await f.Store.AddMappingAsync(new("paused-v1","client",f.Source+"-paused",false,DateTimeOffset.UtcNow),Ct);await f.Store.SaveSyncUploadCheckpointAsync(new("saved-v1","https://test.invalid/file","key-v1","hash-v1","upload-v1"),Ct);File.WriteAllText(Path.Combine(f.Source,"file"),"immutable");f.Remote.LoseComplete=true;string scope;
        await using(var e=await Resume(f)){scope=e.ScopeId;Assert.NotNull((await e.InitialSyncAsync(Ct)).Error);}
        var input=await f.Scalar("SELECT input_json FROM local_cycle_work");var result=await f.Scalar("SELECT result_json FROM local_cycle_work");
        await ExecuteSql(f,"DROP TABLE local_cycle_refresh_conflicts;DROP TABLE local_cycle_refresh_remote;DROP TABLE local_cycle_refreshes;ALTER TABLE local_cycle_scopes DROP COLUMN active_remote;ALTER TABLE local_cycle_scopes DROP COLUMN pending_remote;DROP TABLE local_cycle_active;DROP INDEX local_cycle_scopes_root;ALTER TABLE local_cycle_scopes DROP COLUMN work_retire_after;ALTER TABLE local_cycle_scopes DROP COLUMN confirmed_files;ALTER TABLE local_cycle_scopes DROP COLUMN confirmed_bytes;ALTER TABLE local_cycle_format RENAME TO original_format;CREATE TABLE local_cycle_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL CHECK(version=1),bytes INTEGER NOT NULL DEFAULT 0);INSERT INTO local_cycle_format SELECT singleton,1,bytes FROM original_format;DROP TABLE original_format;");
        // A separate live WAL connection contributes data the Backup API must
        // include. Ordinary copying of the .db alone would lose these writes.
        await using var hold=new SqliteConnection("Data Source="+f.Database+";Cache=Private;Pooling=False");await hold.OpenAsync(Ct);await using(var q=hold.CreateCommand()){q.CommandText="PRAGMA wal_autocheckpoint=0;CREATE TABLE owned_wal_probe(value TEXT);INSERT INTO owned_wal_probe VALUES('durable-WAL-row')";await q.ExecuteNonQueryAsync(Ct);}
        await using(var e=await Resume(f)){Assert.Equal(scope,e.ScopeId);Assert.True(e.HasRetainedAnchor);Assert.Equal(4L,await f.Scalar("SELECT version FROM local_cycle_format"));Assert.Equal(input,await f.Scalar("SELECT input_json FROM local_cycle_work"));Assert.Equal(result,await f.Scalar("SELECT result_json FROM local_cycle_work"));}
        var rollback=Path.Combine(f.Directory,"rollback-v1.db");await f.Store.CreateLocalCycleV1RollbackCopyAsync(rollback,Ct);await using var copy=new SqliteConnection("Data Source="+rollback+";Mode=ReadOnly;Cache=Private;Pooling=False");await copy.OpenAsync(Ct);await using var check=copy.CreateCommand();check.CommandText="SELECT version FROM local_cycle_format";Assert.Equal(1L,await check.ExecuteScalarAsync(Ct));check.CommandText="SELECT enabled FROM sync_mappings WHERE mapping_id='paused-v1'";Assert.Equal(0L,await check.ExecuteScalarAsync(Ct));check.CommandText="SELECT upload_id FROM sync_upload_checkpoints WHERE scope='saved-v1'";Assert.Equal("upload-v1",await check.ExecuteScalarAsync(Ct));check.CommandText="SELECT value FROM owned_wal_probe";Assert.Equal("durable-WAL-row",await check.ExecuteScalarAsync(Ct));check.CommandText="SELECT input_json FROM local_cycle_work";Assert.Equal(input,await check.ExecuteScalarAsync(Ct));
    }
    [Fact]
    public async Task ActualSupersededUnattemptedHistoryRetiresInBoundedStages()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();string old;
        await using(var e=await f.Open())old=e.ScopeId;await using var current=await f.Open();Assert.Null((await current.InitialSyncAsync(Ct)).Error);
        await using(var c=new SqliteConnection("Data Source="+f.Database+";Cache=Private;Pooling=False"))
        {
            await c.OpenAsync(Ct);using var tx=c.BeginTransaction();await using var insert=c.CreateCommand();insert.Transaction=tx;insert.CommandText="INSERT INTO local_cycle_files(scope_id,node_id,parent_id,path,name,created,kind,size,bytes) VALUES($scope,$node,'root',$node,$node,1,0,0,256)";insert.Parameters.AddWithValue("$scope",old);var node=insert.Parameters.AddWithValue("$node","");for(var i=0;i<600;i++){node.Value=i.ToString("x32");await insert.ExecuteNonQueryAsync(Ct);}insert.CommandText="UPDATE local_cycle_scopes SET bytes=bytes+153600 WHERE scope_id=$scope;UPDATE local_cycle_format SET bytes=bytes+153600 WHERE singleton=1";await insert.ExecuteNonQueryAsync(Ct);await tx.CommitAsync(Ct);
        }
        var rows=0;var batches=0;var max=0L;
        while(true){var r=await current.RetireSupersededScopeBatchAsync(old,Ct);Assert.Null(r.Blocked);rows+=r.Records;batches++;max=Math.Max(max,r.ChangedRows);Assert.InRange(r.ChangedRows,0,123);if(r.ScopeRemoved)break;Assert.InRange(batches,1,20);}
        Assert.Equal(600,rows);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_scopes"));await f.Evidence("scope-retirement",new{rows,batches,max});
    }
    [Fact]
    public async Task ActualSupersededHistoricalBaselineAndOriginalAttemptPinFixedRetirementPage()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"published historical bytes");string previous;
        await using(var e=await f.Open()){previous=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);}
        var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0");var original=await f.Scalar("SELECT input_json FROM local_cycle_work WHERE phase=5");await using var current=await f.Open();var retired=await current.RetireSupersededScopeBatchAsync(previous,Ct);Assert.NotNull(retired.Blocked);Assert.Equal(0,retired.ChangedRows);Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE baseline IS NOT NULL AND kind=0"));Assert.Equal(original,await f.Scalar("SELECT input_json FROM local_cycle_work WHERE phase=5"));Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_scopes"));
    }
}
