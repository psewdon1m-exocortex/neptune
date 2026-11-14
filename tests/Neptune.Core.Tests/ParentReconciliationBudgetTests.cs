using System.Diagnostics;
using System.Reflection;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("Native fresh cycle")]
public sealed class ParentReconciliationBudgetTests
{
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    private static Task<T> Medium<T>(Func<Task<T>> action)=>(Task<T>)typeof(IdentityOnlyFreshCycleTests).GetMethod("ReducedMedium",BindingFlags.NonPublic|BindingFlags.Static)!.MakeGenericMethod(typeof(T)).Invoke(null,[action])!;
    private static async Task<int> Step(LocalSourceJournalSession s,string method,string scope)=>await(Task<int>)typeof(LocalSourceJournalSession).GetMethod(method,BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(s,[scope,Ct])!;
    private static async Task Exec(SqliteConnection c,string sql){await using var q=c.CreateCommand();q.CommandText=sql;await q.ExecuteNonQueryAsync(Ct);}
    [Theory][InlineData(100000)][InlineData(500000)]
    public async Task StructuralObsoleteParentScaleHasBoundedRetirementPinsAndPhysicalAdmission(int count)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"base");await Medium(async()=>
        {
            var config=new WindowsCycleConfiguration("mapping","client","audit","rules-v1",new Uri("https://test.invalid/"),f.Remote.Device,"audit-token","local-test",f.Remote.Binding);
            await using var engine=await WindowsFreshSyncEngine.OpenAsync(new WindowsCycleNativeOptions(LocalJournalReadMode.IdentityOnly,true),f.Store,f.Source,Path.Combine(f.Directory,"spool"),config,f.Remote,new(MinimumFreeDiskBytes:10L<<30),cancellationToken:Ct);Assert.Null((await engine.InitialSyncAsync(Ct)).Error);
            for(var i=0;i<2;i++){File.WriteAllText(Path.Combine(f.Source,"new-"+i),"bytes"+i);Assert.Null((await engine.FreshSyncAsync(Ct)).Error);}
            var session=(LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(engine)!;var b=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'");var work=await f.Scalar("SELECT input_json FROM local_cycle_work WHERE input_json IS NOT NULL");
            var obsolete=(string)(await f.Scalar("SELECT current_plan FROM local_parent_reconcile_versions LIMIT 1"))!;var parent=(string)(await f.Scalar("SELECT node_id FROM local_parent_reconcile_parents WHERE plan_id='"+obsolete+"' LIMIT 1"))!;
            // Structural SQLite workload only: these discardable rows are in
            // a superseded membership version, never current native proof.
            await using var writer=new SqliteConnection("Data Source="+f.Database+";Cache=Private;Pooling=False");await writer.OpenAsync(Ct);await Exec(writer,"PRAGMA synchronous=FULL");var clock=Stopwatch.StartNew();long heapMax=0,rssMax=0;var process=Process.GetCurrentProcess();
            await using(var q=writer.CreateCommand())
            {
                q.CommandText="INSERT INTO local_parent_reconcile_entries(plan_id,parent_id,node_id,name,canonical,created,kind,size,path) VALUES($id,$parent,$node,$name,$name,1,0,0,$name)";q.Parameters.AddWithValue("$id",obsolete);q.Parameters.AddWithValue("$parent",parent);var node=q.Parameters.Add("$node",SqliteType.Text);var name=q.Parameters.Add("$name",SqliteType.Text);
                for(var i=0;i<count;i+=256){using var tx=writer.BeginTransaction();q.Transaction=tx;for(var j=i;j<Math.Min(i+256,count);j++){node.Value="ff"+j.ToString("x30");name.Value="structural-discard-"+j.ToString("D7");await q.ExecuteNonQueryAsync(Ct);}await tx.CommitAsync(Ct);heapMax=Math.Max(heapMax,GC.GetTotalMemory(false));process.Refresh();rssMax=Math.Max(rssMax,process.WorkingSet64);}
            }
            var setupMs=clock.Elapsed.TotalMilliseconds;Assert.Equal(0,await Step(session,"RetireParentReconciliationAsync",engine.ScopeId)); // current version pin
            Assert.True(await Step(session,"PromoteParentReconciliationAsync",engine.ScopeId)>0);
            await Exec(writer,"UPDATE local_parent_reconcile_versions SET pending_plan='"+obsolete+"'");Assert.Equal(0,await Step(session,"RetireParentReconciliationAsync",engine.ScopeId));await Exec(writer,"UPDATE local_parent_reconcile_versions SET pending_plan=NULL");
            await Exec(writer,"INSERT INTO local_parent_reconcile_conflicts SELECT scope_id,node_id,'"+obsolete+"','owned retirement pin fixture' FROM local_cycle_files WHERE name='original'");Assert.Equal(0,await Step(session,"RetireParentReconciliationAsync",engine.ScopeId));await Exec(writer,"DELETE FROM local_parent_reconcile_conflicts WHERE plan_id='"+obsolete+"'");
            var cursor=(await session.ReadStateAsync(Ct)).Cursor;var before=(long)(await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_entries WHERE plan_id='"+obsolete+"'"))!;
            await Exec(writer,"UPDATE local_cycle_scopes SET max_profile=1048576");var disk=await Assert.ThrowsAsync<LocalSourceGapException>(()=>Step(session,"RetireParentReconciliationAsync",engine.ScopeId));Assert.Equal("DiskAdmission",disk.Code);Assert.Equal(before,await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_entries WHERE plan_id='"+obsolete+"'"));Assert.Equal(cursor,(await session.ReadStateAsync(Ct)).Cursor);await Exec(writer,"UPDATE local_cycle_scopes SET max_profile=17179869184");
            long pressureWal=0;
            if(count==100000)
            {
                // Pin an old read snapshot, then charge the ACTUAL physical WAL
                // before a new retirement batch. PASSIVE cannot clear it.
                await using var held=new SqliteConnection("Data Source="+f.Database+";Cache=Private;Pooling=False");await held.OpenAsync(Ct);using var read=held.BeginTransaction(deferred:true);await using(var q=held.CreateCommand()){q.Transaction=read;q.CommandText="SELECT version FROM local_cycle_format";await q.ExecuteScalarAsync(Ct);}
                await Exec(writer,"PRAGMA wal_autocheckpoint=0;CREATE TABLE own_parent_pressure(value BLOB)");await using(var q=writer.CreateCommand()){q.CommandText="INSERT INTO own_parent_pressure VALUES(zeroblob(8192))";while((File.Exists(f.Database+"-wal")?new FileInfo(f.Database+"-wal").Length:0)<62L*1024*1024){using var tx=writer.BeginTransaction();q.Transaction=tx;for(var i=0;i<32;i++)await q.ExecuteNonQueryAsync(Ct);await tx.CommitAsync(Ct);}}
                pressureWal=new FileInfo(f.Database+"-wal").Length;var wal=await Assert.ThrowsAsync<LocalSourceGapException>(()=>Step(session,"RetireParentReconciliationAsync",engine.ScopeId));Assert.Equal("WalAdmission",wal.Code);Assert.Equal(before,await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_entries WHERE plan_id='"+obsolete+"'"));Assert.Equal(cursor,(await session.ReadStateAsync(Ct)).Cursor);await read.RollbackAsync(Ct);
            }
            await f.Store.InspectLocalJournalWalAsync(force:true,cancellationToken:Ct);clock.Restart();for(var i=0;i<8;i++)Assert.Equal(1,await Step(session,"RetireParentReconciliationAsync",engine.ScopeId));var eightMs=clock.Elapsed.TotalMilliseconds;
            Assert.Equal(before-1024,await f.Scalar("SELECT COUNT(*) FROM local_parent_reconcile_entries WHERE plan_id='"+obsolete+"'"));Assert.Equal(b,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE name='original'"));Assert.Equal(work,await f.Scalar("SELECT input_json FROM local_cycle_work WHERE input_json IS NOT NULL"));Assert.InRange(f.Store.GetParentReconciliationMetrics().MaximumChangedRows,1,256);
            await f.Evidence("parent-structural-scale-"+count,new{count,setupMs,eightMs,retirementRows=1024,partialRetirement=true,nativeThroughputClaim=false,heapMax,rssMax,dbBytes=new FileInfo(f.Database).Length,walBytes=File.Exists(f.Database+"-wal")?new FileInfo(f.Database+"-wal").Length:0,backupBytes=Directory.EnumerateFiles(f.Directory,"state.db.pre-*.db").Sum(p=>new FileInfo(p).Length),pressureWal,metrics=f.Store.GetParentReconciliationMetrics(),journal=f.Store.GetLocalJournalMetrics()});return true;
        });
    }
}
