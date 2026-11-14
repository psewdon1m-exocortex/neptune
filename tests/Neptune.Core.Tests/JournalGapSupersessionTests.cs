using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    [Theory]
    [InlineData("expired-journal")][InlineData("expired-capacity")]
    [InlineData("retained-capacity")][InlineData("retained-healthy")]
    public async Task ExplicitPendingGapSuccessorRequiresNativeLossAndPreservesOriginalLineage(string gate)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"old immutable bytes");
        await Medium(async()=>
        {
            string original;await using(var e=await Open(f)){original=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}
            var first=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),original);string failed;
            await using(var e=await Replace(f,first)){failed=e.ScopeId;if(gate!="retained-healthy")await Session(e).MarkGapAsync(gate.EndsWith("capacity",StringComparison.Ordinal)?"InboxCapacity":"JournalGap",Ct);}
            if(gate.StartsWith("expired",StringComparison.Ordinal))await f.Scalar("UPDATE local_source_roots SET cursor=0 WHERE root_key=(SELECT root_key FROM local_cycle_scopes WHERE scope_id='"+failed+"')");
            var oldProof=await Snapshot(f,original);var failedProof=await Snapshot(f,failed);var prepares=f.Remote.Prepares;
            var next=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),original,first.ReplacementId);
            if(!gate.StartsWith("expired",StringComparison.Ordinal))
            {
                var error=await Assert.ThrowsAsync<LocalSourceGapException>(()=>Replace(f,next));Assert.Equal("ReplacementContinuityPresent",error.Code);
                Assert.Equal(5L,await f.Scalar("SELECT version FROM local_cycle_format"));Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_source_roots"));Assert.Equal(oldProof,await Snapshot(f,original));Assert.Equal(failedProof,await Snapshot(f,failed));return true;
            }
            await using var held=new SqliteConnection("Data Source="+f.Database+";Pooling=False;Cache=Private");await held.OpenAsync(Ct);
            await using(var marker=held.CreateCommand()){marker.CommandText="CREATE TABLE supersession_wal_marker(value TEXT NOT NULL);INSERT INTO supersession_wal_marker VALUES('committed WAL retained')";await marker.ExecuteNonQueryAsync(Ct);}
            Assert.True(File.Exists(f.Database+"-wal"));
            string current;
            await using(var e=await Replace(f,next))
            {
                current=e.ScopeId;Assert.Equal(3,e.JournalReplacement!.SubscriptionGeneration);Assert.Equal(original,e.JournalReplacement.OriginalScopeId);Assert.Equal(6L,await f.Scalar("SELECT version FROM local_cycle_format"));
                Assert.Equal(next.ReplacementId,await f.Scalar("SELECT superseded_by FROM local_cycle_replacement_requests WHERE replacement_id='"+first.ReplacementId+"'"));
                Assert.Equal(first.ReplacementId,await f.Scalar("SELECT supersedes_id FROM local_cycle_replacement_requests WHERE replacement_id='"+next.ReplacementId+"'"));
                Assert.Equal(oldProof,await Snapshot(f,original));Assert.Equal(failedProof,await Snapshot(f,failed));
                await e.QualifyJournalReplacementAsync(Ct);Assert.Equal(LocalJournalReplacementStatus.Installed,e.JournalReplacement.Status);var fresh=await e.FreshSyncAsync(Ct);Assert.Null(fresh.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",fresh.Decision);Assert.Equal(0,fresh.PublishedFiles);
            }
            await using(var reopened=await Replace(f,next)){Assert.Equal(current,reopened.ScopeId);var repeat=await reopened.FreshSyncAsync(Ct);Assert.Null(repeat.Error);Assert.Equal(0,repeat.CapturedFiles);}
            await Assert.ThrowsAsync<LocalSourceGapException>(()=>Replace(f,first));
            await Assert.ThrowsAsync<InvalidDataException>(()=>Replace(f,next with{SupersedesReplacementId=null}));
            Assert.Equal(prepares,f.Remote.Prepares);Assert.Equal(oldProof,await Snapshot(f,original));Assert.Equal(failedProof,await Snapshot(f,failed));Assert.Equal(3L,await f.Scalar("SELECT COUNT(*) FROM local_source_roots"));
            var rollback=Path.Combine(f.Directory,"v5-rollback.db");
            await (Task<string>)typeof(NeptuneStateStore).GetMethod("CreateLocalCycleV5RollbackCopyAsync")!.Invoke(f.Store,[rollback,Ct])!;
            await using var copy=new SqliteConnection("Data Source="+rollback+";Mode=ReadOnly;Pooling=False");await copy.OpenAsync(Ct);await using var q=copy.CreateCommand();q.CommandText="SELECT version FROM local_cycle_format";Assert.Equal(5L,await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT COUNT(*) FROM local_cycle_replacement_requests";Assert.Equal(1L,await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT value FROM supersession_wal_marker";Assert.Equal("committed WAL retained",await q.ExecuteScalarAsync(Ct));
            var evidence=Environment.GetEnvironmentVariable("NEPTUNE_FRESH_CYCLE_EVIDENCE_ROOT");if(evidence is not null&&gate=="expired-capacity")
            {Directory.CreateDirectory(evidence);await using var destination=new SqliteConnection("Data Source="+Path.Combine(evidence,"cycle-v6-"+Path.GetFileName(f.Directory)+".db")+";Pooling=False");await destination.OpenAsync(Ct);held.BackupDatabase(destination);}
            await f.Evidence("pending-gap-successor-"+gate,new{first,next,current,originalEvidencePreserved=true,failedCandidatePreserved=true,nativeBoundsCompared=true,cursorInjectedIntoOwnedFixture=true,physicalWrapClaim=false});return true;
        });
    }

    [Theory]
    [InlineData("unknown-parent")][InlineData("original-scope")][InlineData("configuration")][InlineData("active-binding")][InlineData("candidate-work")]
    public async Task ExplicitSupersessionRefusesUnrelatedOrOwnedCandidateWorkBeforeMigration(string gate)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"retained");
        await Medium(async()=>
        {
            string original;await using(var e=await Open(f)){original=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}
            var first=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),original);string failed;
            await using(var e=await Replace(f,first)){failed=e.ScopeId;await Session(e).MarkGapAsync("JournalGap",Ct);}
            await f.Scalar("UPDATE local_source_roots SET cursor=0 WHERE root_key=(SELECT root_key FROM local_cycle_scopes WHERE scope_id='"+failed+"')");
            if(gate=="active-binding")await f.Scalar("UPDATE local_source_active SET root_key=(SELECT root_key FROM local_cycle_scopes WHERE scope_id='"+original+"')");
            if(gate=="candidate-work")await f.Scalar("INSERT INTO local_cycle_files(scope_id,node_id,parent_id,path,name,created,kind,size,bytes) SELECT '"+failed+"',node_id,parent_id,path,name,created,kind,size,256 FROM local_cycle_files WHERE scope_id='"+original+"' AND kind=0;INSERT INTO local_cycle_work(operation_id,scope_id,node_id,journal_generation,watcher_generation,entry_json,phase) SELECT '"+Guid.NewGuid().ToString("N")+"','"+failed+"',node_id,journal_generation,watcher_generation,entry_json,0 FROM local_cycle_work WHERE scope_id='"+original+"' LIMIT 1"); // owned fixture: an unexpected queued operation keeps a valid foreign key and must be retained
            var before=await Snapshot(f,original)+await Snapshot(f,failed);
            var next=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),gate=="original-scope"?failed:original,gate=="unknown-parent"?Guid.NewGuid().ToString("N"):first.ReplacementId);
            await Assert.ThrowsAsync<LocalSourceGapException>(()=>Replace(f,next,gate=="configuration"?Config(f) with{RulesVersion="other"}:null));
            Assert.Equal(before,await Snapshot(f,original)+await Snapshot(f,failed));Assert.Equal(5L,await f.Scalar("SELECT version FROM local_cycle_format"));Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_source_roots"));Assert.Empty(Directory.GetFiles(f.Directory,"*.pre-local-cycle-v6-*.db"));return true;
        });
    }
}
