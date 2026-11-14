using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class ParentReconciliationTests
{
    [Theory][InlineData(false)][InlineData(true)]
    public async Task RepeatedOutsideHardlinkClosureRecoversWithoutAnotherTreeScan(bool removed)
    {
        if(!OperatingSystem.IsWindows()){Assert.Skip("Native Windows required");return;}
        await using var f=await WindowsFreshCycleTests.Fixture.Create();
        Directory.CreateDirectory(Path.Combine(f.Source,"known","deep"));File.WriteAllText(Path.Combine(f.Source,"known","deep","original"),"retained baseline");
        var outside=Path.Combine(f.Directory,"outside");File.WriteAllText(outside,"unrelated bytes");
        await Medium(async()=>
        {
            var inject=false;var branches=0;
            await using var e=await Open(f,observer:(o,t)=>
            {
                if(o.Stage==WindowsCycleStage.BranchEnumerated){branches++;if(inject){inject=false;Assert.True(CreateHardLinkW(Path.Combine(f.Directory,"during-link"),outside,nint.Zero));}}
                return Task.CompletedTask;
            });
            Assert.Null((await Initial(e)).Error);var baseline=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'");
            var originals=await f.Scalar("SELECT group_concat(input_json||intent_hash) FROM local_cycle_work WHERE input_json IS NOT NULL");
            // Retained legacy full-scan recovery remains supported. Explicitly
            // create that obligation: ordinary first outside events now use
            // the native witness before any full-tree plan is needed.
            var session=(LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session",System.Reflection.BindingFlags.Instance|System.Reflection.BindingFlags.NonPublic)!.GetValue(e)!;
            Assert.True(CreateHardLinkW(Path.Combine(f.Directory,"outside-link"),outside,nint.Zero));
            var coordinator=(WindowsLocalCoherenceCoordinator)typeof(WindowsFreshSyncEngine).GetField("_coordinator",System.Reflection.BindingFlags.Instance|System.Reflection.BindingFlags.NonPublic)!.GetValue(e)!;
            var coherence=(string)typeof(WindowsFreshSyncEngine).GetField("_coherence",System.Reflection.BindingFlags.Instance|System.Reflection.BindingFlags.NonPublic)!.GetValue(e)!;
            await coordinator.ObserveChangesAsync(session,coherence,Ct);
            await session.BeginParentReconciliationAsync(e.ScopeId,Ct);inject=true;
            var rejected=await e.FreshSyncAsync(Ct);Assert.NotNull(rejected.Error);
            Assert.Equal(1L,await f.Scalar("SELECT count(*) FROM local_parent_reconcile_plans WHERE status=4 AND full_membership=1"));
            var scanned=branches;
            if(removed){File.Delete(Path.Combine(f.Directory,"during-link"));File.Delete(Path.Combine(f.Directory,"outside-link"));File.Delete(outside);}
            var due=Convert.ToInt64(await f.Scalar("SELECT not_before FROM client_operation_schedule WHERE operation_id='cycle'"));
            var remaining=due-DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();if(remaining>0)await Task.Delay(TimeSpan.FromMilliseconds(remaining+50),Ct);
            var recovered=await e.FreshSyncAsync(Ct);await f.Evidence("external-hardlink-recovery",new{removed,rejected,recovered,branches,scanned});
            Assert.Null(recovered.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",recovered.Decision);Assert.Equal(scanned,branches);
            Assert.Equal(0,recovered.CapturedFiles);Assert.Equal(0,recovered.PublishedFiles);Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));
            Assert.Equal(originals,await f.Scalar("SELECT group_concat(input_json||intent_hash) FROM local_cycle_work WHERE input_json IS NOT NULL"));
            Assert.Equal(1L,await f.Scalar("SELECT count(*) FROM local_parent_reconcile_external_cuts"));
            Assert.True((long)(await f.Scalar("SELECT count(*) FROM local_coherence_unresolved WHERE code='HardlinkMembershipUnresolved'"))!>0);
            File.WriteAllText(outside,"new outside incarnation");Assert.True(CreateHardLinkW(Path.Combine(f.Directory,"later-link"),outside,nint.Zero));
            var later=await e.FreshSyncAsync(Ct);Assert.Null(later.Error);Assert.Equal(scanned,branches);Assert.Equal(0,later.CapturedFiles);
            Assert.Single(Directory.GetFiles(f.Source,"*",SearchOption.AllDirectories));
            Assert.Equal(5L,await f.Scalar("SELECT version FROM local_parent_reconcile_format"));
            var oldReader=Environment.GetEnvironmentVariable("NEPTUNE_EXTERNAL_CUT_OLD_READER");
            if(!string.IsNullOrWhiteSpace(oldReader))
            {
                var context=new System.Runtime.Loader.AssemblyLoadContext("v4-reader-"+Guid.NewGuid().ToString("N"),isCollectible:true);
                try
                {
                    var type=context.LoadFromAssemblyPath(Path.GetFullPath(oldReader)).GetType("Neptune.Core.NeptuneStateStore")!;
                    var oldStore=Activator.CreateInstance(type,[f.Database,null])!;
                    var preflight=type.GetMethod("PreflightParentReconcileAsync",System.Reflection.BindingFlags.Instance|System.Reflection.BindingFlags.NonPublic)!;
                    var error=await Assert.ThrowsAsync<InvalidDataException>(async()=>await (Task)preflight.Invoke(oldStore,[Ct])!);
                    Assert.Contains("Unsupported optional namespace format",error.Message);
                    Assert.Equal(originals,await f.Scalar("SELECT group_concat(input_json||intent_hash) FROM local_cycle_work WHERE input_json IS NOT NULL"));
                }
                finally{context.Unload();}
            }
            var rollback=await f.Store.CreateParentExternalCutRollbackCopyAsync(Path.Combine(f.Directory,"rollback-v4.db"),Ct);
            await using(var copy=new Microsoft.Data.Sqlite.SqliteConnection("Data Source="+rollback+";Mode=ReadOnly;Pooling=False"))
            {
                await copy.OpenAsync(Ct);await using var q=copy.CreateCommand();q.CommandText="SELECT version FROM local_parent_reconcile_format";Assert.Equal(4L,await q.ExecuteScalarAsync(Ct));
                q.CommandText="SELECT group_concat(input_json||intent_hash) FROM local_cycle_work WHERE input_json IS NOT NULL";Assert.Equal(originals,await q.ExecuteScalarAsync(Ct));
            }
            var scope=e.ScopeId;await e.DisposeAsync();
            await using var resumed=await Open(f,true,observer:async(o,t)=>{if(o.Stage==WindowsCycleStage.BranchEnumerated){branches++;await f.Evidence("external-cut-branch",new{branches,plans=await f.Scalar("SELECT json_group_array(json_object('plan',plan_id,'status',status,'start',start_usn,'after',record_after,'full',full_membership,'scan',scan_start_usn,'parents',parents)) FROM local_parent_reconcile_plans")});}});
            Assert.Equal(scope,resumed.ScopeId);var fresh=await resumed.FreshSyncAsync(Ct);Assert.Null(fresh.Error);Assert.Equal(scanned,branches);Assert.Equal(0,fresh.CapturedFiles);
            // A later ordinary inside change starts after the qualified cut;
            // historical outside hardlinks must not seed a new full-tree scan.
            var externalCut=Convert.ToInt64(await f.Scalar("SELECT max(cut_usn) FROM local_parent_reconcile_external_cuts"));
            File.WriteAllText(Path.Combine(f.Source,"known","deep","new-file"),"new inside bytes");
            var added=await Converge(resumed,f);Assert.Null(added.Error);Assert.Equal(1,added.PublishedFiles);
            var ambient=Convert.ToInt64(await f.Scalar("SELECT count(*) FROM local_coherence_unresolved WHERE code='HardlinkMembershipUnresolved' AND usn>="+externalCut));
            var full=Convert.ToInt64(await f.Scalar("SELECT full_membership FROM local_parent_reconcile_plans WHERE status=3 ORDER BY rowid DESC LIMIT 1"));
            await f.Evidence("external-cut-reasons",new{externalCut,records=await f.Scalar("SELECT json_group_array(json_object('usn',usn,'reason',hex(substr(payload,65,4)),'node',hex(substr(payload,17,16)),'parent',hex(substr(payload,33,16)))) FROM local_coherence_unresolved WHERE usn>="+externalCut)});
            await f.Evidence("external-cut-next-inside",new{externalCut,ambient,full,branches,scanned,added,plans=await f.Scalar("SELECT json_group_array(json_object('plan',plan_id,'status',status,'start',start_usn,'after',record_after,'full',full_membership,'scan',scan_start_usn,'parents',parents)) FROM local_parent_reconcile_plans"),unresolved=await f.Scalar("SELECT json_group_array(json_object('usn',usn,'code',code)) FROM local_coherence_unresolved WHERE usn>="+externalCut)});
            // New deletes/orphan rename closes on this shared volume also
            // require the existing full-proof gate, independently of hardlink
            // recovery. Qualify them from retained records after this cut;
            // historical outside hardlinks alone may never cause a rescan.
            var triggers=await FullProofTriggersAfter(resumed,externalCut);
            await f.Evidence("external-cut-new-full-obligations",new{externalCut,triggers,full});
            if(triggers.Count==0)Assert.Equal(0,full);
            Assert.Equal(scanned+(full==1?3:1),branches);
            originals=await f.Scalar("SELECT group_concat(input_json||intent_hash) FROM local_cycle_work WHERE input_json IS NOT NULL");
            // A subsequent inside alias must still fence publication even after
            // the current outside-cut witness has been persisted and reopened.
            Assert.True(CreateHardLinkW(Path.Combine(f.Source,"inside-alias"),outside,nint.Zero));
            var effects=f.Remote.Prepares;var inside=await resumed.FreshSyncAsync(Ct);Assert.NotNull(inside.Error);Assert.Equal(effects,f.Remote.Prepares);
            Assert.Equal(originals,await f.Scalar("SELECT group_concat(input_json||intent_hash) FROM local_cycle_work WHERE input_json IS NOT NULL"));
            Assert.Equal(baseline,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));return true;
        });
    }
    [Theory][InlineData("outside")][InlineData("inside")][InlineData("changed")][InlineData("deleted-unqualified")][InlineData("replaced-alias")][InlineData("inside-deletion-claim")]
    public async Task NativeOutsideHardlinkWitnessChecksAllAliasesAndRemainsStable(string kind)
    {
        if(!OperatingSystem.IsWindows()){Assert.Skip("Native Windows required");return;}
        await using var f=await WindowsFreshCycleTests.Fixture.Create();var outside=Path.Combine(f.Directory,"outside");File.WriteAllText(outside,"unrelated");
        await using var source=await WindowsLocalNamespaceSource.OpenAsync(f.Source,Ct);
        // Resolve the real file identity through a native directory enumeration.
        await using var directory=await WindowsLocalNamespaceSource.OpenAsync(f.Directory,Ct);
        await using var reader=await directory.OpenDirectoryAsync(new("candidate",directory.RootProof.Handle.FileId128,"",directory.RootProof.Handle.CreationTime,"claim"),Ct);
        var entries=new List<LocalNamespaceEntry>();while(true){var page=await reader.ReadPageAsync(Ct);entries.AddRange(page.Entries);if(page.End)break;}
        var file=Assert.Single(entries,x=>x.Name=="outside");
        var link=Path.Combine(kind.StartsWith("inside",StringComparison.Ordinal)?f.Source:f.Directory,"alias");Assert.True(CreateHardLinkW(link,outside,nint.Zero));
        if(kind=="deleted-unqualified")
        {File.Delete(link);File.Delete(outside);Assert.Throws<System.ComponentModel.Win32Exception>(()=>WindowsOutsideHardlinkProof.Open(source.RootProof,[new(3,file.FileId128,0)],Ct));}
        else if(kind.StartsWith("inside",StringComparison.Ordinal))Assert.Throws<IOException>(()=>WindowsOutsideHardlinkProof.Open(source.RootProof,[new(3,file.FileId128,0,kind=="inside-deletion-claim")],Ct));
        else
        {
            using var proof=WindowsOutsideHardlinkProof.Open(source.RootProof,[new(3,file.FileId128,0)],Ct);proof.Validate(Ct);
            if(kind=="changed"){Assert.True(CreateHardLinkW(Path.Combine(f.Source,"changed-inside"),outside,nint.Zero));Assert.Throws<IOException>(()=>proof.Validate(Ct));}
            if(kind=="replaced-alias"){File.Delete(link);File.WriteAllText(link,"different native identity");Assert.Throws<IOException>(()=>proof.Validate(Ct));}
        }
    }
    [Fact]
    public async Task RetainedRepeatedHardlinkFailureQualifiesCurrentNativeWitness()
    {
        var path=Environment.GetEnvironmentVariable("NEPTUNE_EXTERNAL_CUT_REPRO");
        var source=Environment.GetEnvironmentVariable("NEPTUNE_EXTERNAL_CUT_SOURCE");
        if(string.IsNullOrWhiteSpace(path)||string.IsNullOrWhiteSpace(source)){Assert.Skip("Requires the offline repeated-hardlink failure snapshot and native test root");return;}
        // Use a disposable copy: a rejected plan has retained but unapplied
        // native suffix records. Replay through the retained cut exactly as a
        // fresh service run does; never modify the failure evidence or live DB.
        var directory=Path.Combine(Path.GetTempPath(),"neptune-external-retained-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        var copied=Path.Combine(directory,"cycle.db");File.Copy(path,copied);
        try
        {
        await using var c=new Microsoft.Data.Sqlite.SqliteConnection("Data Source="+copied+";Pooling=False");await c.OpenAsync(Ct);
        string root,owner,scope;
        await using(var q=c.CreateCommand())
        {q.CommandText="SELECT p.root_key,r.owner_token,p.scope_id FROM local_parent_reconcile_plans p JOIN local_source_roots r ON r.root_key=p.root_key WHERE p.status=4 AND p.full_membership=1 ORDER BY p.rowid DESC LIMIT 1";await using var r=await q.ExecuteReaderAsync(Ct);Assert.True(await r.ReadAsync(Ct));root=r.GetString(0);owner=r.GetString(1);scope=r.GetString(2);}
        var store=new NeptuneStateStore(copied);var admission=new LocalJournalAdmission(MaximumDatabaseBytes:16L<<30);
        string coherent;long cut;
        await using(var q=c.CreateCommand()){q.CommandText="SELECT coherence_id FROM local_cycle_scopes WHERE scope_id=$scope";q.Parameters.AddWithValue("$scope",scope);coherent=(string)(await q.ExecuteScalarAsync(Ct))!;}
        await using(var q=c.CreateCommand()){q.CommandText="SELECT cursor FROM local_source_roots WHERE root_key=$root";q.Parameters.AddWithValue("$root",root);cut=(long)(await q.ExecuteScalarAsync(Ct))!;}
        for(var n=0;n<10000;n++)
        {
            await using var q=c.CreateCommand();q.CommandText="SELECT applied_usn FROM local_coherence_roots WHERE coherence_id=$id";q.Parameters.AddWithValue("$id",coherent);var applied=(long)(await q.ExecuteScalarAsync(Ct))!;if(applied>=cut)break;
            var next=await store.ApplyOwnedCoherenceAsync(root,owner,coherent,applied,admission,Ct);Assert.True(next.AppliedUsn>applied);
        }
        await Medium(async()=>
        {
            var claims=await store.ReadParentExternalClaims(root,owner,scope,admission,Ct);Assert.NotNull(claims);
            await using var native=await WindowsLocalNamespaceSource.OpenAsync(source,Ct);
            using var proof=WindowsOutsideHardlinkProof.Open(native.RootProof,claims.Files,Ct);proof.Validate(Ct);
            var evidence=Environment.GetEnvironmentVariable("NEPTUNE_FRESH_CYCLE_EVIDENCE_ROOT");
            if(evidence is not null)await File.WriteAllTextAsync(Path.Combine(evidence,"retained-external-proof.json"),System.Text.Json.JsonSerializer.Serialize(new{claims,proof.Evidence},new System.Text.Json.JsonSerializerOptions{WriteIndented=true}),Ct);
            return true;
        });
        }
        finally{Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();Directory.Delete(directory,true);}
    }
}
