using System.Reflection;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class WindowsFreshCycleTests
{
    [Fact]
    public async Task ActualConfirmedTotalsTrackK1DifferentBytesZeroFilesNoopAndRestart()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());var file=Path.Combine(f.Source,"file");File.WriteAllText(file,"v1");File.WriteAllBytes(Path.Combine(f.Source,"zero"),[]);Directory.CreateDirectory(Path.Combine(f.Source,"empty"));string scope;
        await using(var e=await Resume(f))
        {
            scope=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);Assert.Equal(new WindowsCycleConfirmedTotals(2,2),await e.ReadConfirmedTotalsAsync(Ct));var noop=await f.FreshWhenDueAsync(e,Ct);Assert.Equal(0,noop.CapturedFiles);Assert.Equal(new WindowsCycleConfirmedTotals(2,2),await e.ReadConfirmedTotalsAsync(Ct));File.WriteAllText(file,"larger closed version");Assert.Null((await f.FreshWhenDueAsync(e,Ct)).Error);Assert.Equal(new WindowsCycleConfirmedTotals(2,21),await e.ReadConfirmedTotalsAsync(Ct));
        }
        await using var restored=await Resume(f);Assert.Equal(scope,restored.ScopeId);Assert.Equal(new WindowsCycleConfirmedTotals(2,21),await restored.ReadConfirmedTotalsAsync(Ct));Assert.Equal(0,(await f.FreshWhenDueAsync(restored,Ct)).CapturedFiles);await f.Evidence("confirmed-totals-restart",await restored.ReadConfirmedTotalsAsync(Ct));
    }
    [Fact]
    public async Task ActualExplicitVerifiedEqualAdoptionCountsOnlyAfterAcknowledgedBaseline()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"equal");f.Remote.AddExisting("file","equal");await using var e=await Resume(f,AnchoredConfiguration(f)with{AllowVerifiedEqualAdoption=true});Assert.Equal(new WindowsCycleConfirmedTotals(0,0),await e.ReadConfirmedTotalsAsync(Ct));var result=await e.InitialSyncAsync(Ct);Assert.Null(result.Error);Assert.Equal(new WindowsCycleConfirmedTotals(1,5),await e.ReadConfirmedTotalsAsync(Ct));Assert.Equal(0,f.Remote.Prepares);
    }
    [Fact]
    public async Task ActualLateGenerationAndDuplicateAckPreserveTotalsAndDirty()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"confirmed");await using var e=await Resume(f);Assert.Null((await e.InitialSyncAsync(Ct)).Error);var session=(LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(e)!;var capture=typeof(WindowsFreshSyncEngine).GetField("_capture",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(e)!;var proof=capture.GetType().GetProperty("RootProof")!.GetValue(capture)!;var operation=(string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE input_json IS NOT NULL"))!;var ack=typeof(LocalSourceJournalSession).GetMethod("AcknowledgeCycleAsync",BindingFlags.Instance|BindingFlags.NonPublic)!;
        Assert.True(await(Task<bool>)ack.Invoke(session,[e.ScopeId,operation,proof,Ct])!);Assert.Equal(new WindowsCycleConfirmedTotals(1,9),await e.ReadConfirmedTotalsAsync(Ct));var hint=typeof(LocalSourceJournalSession).GetMethod("MarkCycleHintsAsync",BindingFlags.Instance|BindingFlags.NonPublic)!;await(Task)hint.Invoke(session,[e.ScopeId,new[]{"file"},Ct])!;Assert.False(await(Task<bool>)ack.Invoke(session,[e.ScopeId,operation,proof,Ct])!);Assert.Equal(new WindowsCycleConfirmedTotals(1,9),await e.ReadConfirmedTotalsAsync(Ct));Assert.Equal(0L,await f.Scalar("SELECT phase FROM local_cycle_files WHERE kind=0"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE ack_watcher<watcher_generation"));
    }
    [Fact]
    public async Task ActualNewerBaselineRejectsOlderTotalsPromotionWithoutMutation()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create(new Stage3Clock());var path=Path.Combine(f.Source,"file");File.WriteAllText(path,"v1");await using var e=await Resume(f);Assert.Null((await e.InitialSyncAsync(Ct)).Error);var oldEntry=JsonSerializer.Deserialize<LocalCycleEntry>((string)(await f.Scalar("SELECT entry_json FROM local_cycle_work WHERE work_no=1"))!,new JsonSerializerOptions(JsonSerializerDefaults.Web))!;var oldBaseline=JsonSerializer.Deserialize<LocalCycleBaseline>((string)(await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"))!,new JsonSerializerOptions(JsonSerializerDefaults.Web))!;File.WriteAllText(path,"newer larger");Assert.Null((await f.FreshWhenDueAsync(e,Ct)).Error);var before=await e.ReadConfirmedTotalsAsync(Ct);
        await using var c=new SqliteConnection("Data Source="+f.Database+";Cache=Private;Pooling=False");await c.OpenAsync(Ct);using var t=c.BeginTransaction();var method=typeof(NeptuneStateStore).GetMethod("PromoteCycleConfirmedTotals",BindingFlags.Static|BindingFlags.NonPublic)!;await(Task)method.Invoke(null,[c,t,e.ScopeId,oldEntry,oldBaseline.Description,1L,Ct])!;await t.CommitAsync(Ct);Assert.Equal(before,await e.ReadConfirmedTotalsAsync(Ct));Assert.Equal(new WindowsCycleConfirmedTotals(1,12),before);Assert.Equal(2L,await f.Scalar("SELECT baseline_work FROM local_cycle_files WHERE kind=0"));
    }
    [Fact]
    public async Task ActualVersionOneMigrationBackfillsHistoricalBaselineOnceWithoutRecapture()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"stored baseline");File.WriteAllBytes(Path.Combine(f.Source,"zero"),[]);string scope;
        await using(var e=await Resume(f)){scope=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);Assert.Equal(new WindowsCycleConfirmedTotals(2,15),await e.ReadConfirmedTotalsAsync(Ct));}
        var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='file'");
        await ExecuteSql(f,"DROP TABLE local_cycle_refresh_conflicts;DROP TABLE local_cycle_refresh_remote;DROP TABLE local_cycle_refreshes;ALTER TABLE local_cycle_scopes DROP COLUMN active_remote;ALTER TABLE local_cycle_scopes DROP COLUMN pending_remote;DROP TABLE local_cycle_active;DROP INDEX local_cycle_scopes_root;ALTER TABLE local_cycle_scopes DROP COLUMN work_retire_after;ALTER TABLE local_cycle_scopes DROP COLUMN confirmed_files;ALTER TABLE local_cycle_scopes DROP COLUMN confirmed_bytes;ALTER TABLE local_cycle_format RENAME TO original_format;CREATE TABLE local_cycle_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL CHECK(version=1),bytes INTEGER NOT NULL DEFAULT 0);INSERT INTO local_cycle_format SELECT singleton,1,bytes FROM original_format;DROP TABLE original_format;");
        var prepares=f.Remote.Prepares;await using var restored=await Resume(f);Assert.Equal(scope,restored.ScopeId);Assert.Equal(new WindowsCycleConfirmedTotals(2,15),await restored.ReadConfirmedTotalsAsync(Ct));var result=await restored.FreshSyncAsync(Ct);Assert.Null(result.Error);Assert.Equal(0,result.CapturedFiles);Assert.Equal(prepares,f.Remote.Prepares);Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='file'"));await f.Evidence("v1-native-positive-backfill",await restored.ReadConfirmedTotalsAsync(Ct));
    }
}
