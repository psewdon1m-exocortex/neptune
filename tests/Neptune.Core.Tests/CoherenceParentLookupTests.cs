using System.Reflection;
using System.Runtime.InteropServices;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class CoherenceParentLookupTests
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate int Progress(IntPtr state);
    [DllImport("e_sqlite3",CallingConvention=CallingConvention.Cdecl)]
    private static extern void sqlite3_progress_handler(IntPtr database,int instructions,Progress? callback,IntPtr state);

    [Fact]
    public async Task ExactAliasLookupDoesNotVisitEveryParentForEveryOutsideRecord()
    {
        var ct=TestContext.Current.CancellationToken;
        await using var c=new SqliteConnection("Data Source=:memory:");await c.OpenAsync(ct);
        async Task Execute(string sql){await using var q=c.CreateCommand();q.CommandText=sql;await q.ExecuteNonQueryAsync(ct);}
        var schema=(Dictionary<string,string>)typeof(NeptuneStateStore).GetField("ParentReconcileSchema",BindingFlags.NonPublic|BindingFlags.Static)!.GetValue(null)!;
        foreach(var sql in schema.Values)await Execute(sql);
        await Execute("CREATE TABLE local_cycle_scopes(scope_id TEXT PRIMARY KEY,coherence_id TEXT NOT NULL);CREATE TABLE local_coherence_nodes(coherence_id TEXT,node_id TEXT,created INTEGER,kind INTEGER,wire64 TEXT,PRIMARY KEY(coherence_id,node_id));CREATE UNIQUE INDEX test_coherence_wire ON local_coherence_nodes(coherence_id,wire64);INSERT INTO local_cycle_scopes VALUES('scope','cohere');INSERT INTO local_parent_reconcile_plans(plan_id,root_key,scope_id,coherence_id,owner_epoch,revision,gap,journal,start_usn,cut_usn,status) VALUES('full','root','scope','cohere',1,0,0,'journal',100,200,3)");
        using(var t=c.BeginTransaction())
        {
            await using var q=c.CreateCommand();q.Transaction=t;
            q.CommandText="INSERT INTO local_parent_reconcile_versions(coherence_id,parent_id,pending_plan) VALUES('cohere',$id,'full')";var id=q.Parameters.Add("$id",SqliteType.Text);
            for(var n=0;n<5000;n++){id.Value=n.ToString("x32");await q.ExecuteNonQueryAsync(ct);}await t.CommitAsync(ct);
        }
        var method=typeof(NeptuneStateStore).GetMethod("CoherenceNodeAsync",BindingFlags.NonPublic|BindingFlags.Static)!;
        using var transaction=c.BeginTransaction();long steps=0;Progress progress=_=>{steps++;return 0;};
        sqlite3_progress_handler(c.Handle!.DangerousGetHandle(),1,progress,IntPtr.Zero);
        try
        {
            for(var n=0;n<100;n++)
            {
                var args=new object?[method.GetParameters().Length];object?[] known=[c,transaction,"cohere",(ushort)2,"ffffffffffffffff",TestContext.Current.CancellationToken,true];known.CopyTo(args,0);
                var task=(Task)method.Invoke(null,args)!;await task;Assert.Null(task.GetType().GetProperty("Result")!.GetValue(task));
            }
        }
        finally{sqlite3_progress_handler(c.Handle!.DangerousGetHandle(),0,null,IntPtr.Zero);GC.KeepAlive(progress);}
        // VM operations are deterministic evidence of indexed lookup; wall
        // time on a busy user's PC is not the assertion.
        Assert.InRange(steps,1,500000);
    }
    [Theory]
    [InlineData("pending")][InlineData("installed")][InlineData("ambiguous")][InlineData("fragmented")][InlineData("v3")]
    public async Task ExactAliasLookupKeepsCurrentVersionPendingFenceAndAmbiguity(string mode)
    {
        var ct=TestContext.Current.CancellationToken;await using var c=new SqliteConnection("Data Source=:memory:");await c.OpenAsync(ct);
        async Task Execute(string sql){await using var q=c.CreateCommand();q.CommandText=sql;await q.ExecuteNonQueryAsync(ct);}
        var schema=(Dictionary<string,string>)typeof(NeptuneStateStore).GetField("ParentReconcileSchema",BindingFlags.NonPublic|BindingFlags.Static)!.GetValue(null)!;
        foreach(var sql in schema.Values)await Execute(sql);
        await Execute("CREATE TABLE local_cycle_scopes(scope_id TEXT PRIMARY KEY,coherence_id TEXT NOT NULL);CREATE TABLE local_coherence_nodes(coherence_id TEXT,node_id TEXT,created INTEGER,kind INTEGER,wire64 TEXT,PRIMARY KEY(coherence_id,node_id));CREATE UNIQUE INDEX test_coherence_wire ON local_coherence_nodes(coherence_id,wire64);INSERT INTO local_cycle_scopes VALUES('scope','cohere')");
        foreach(var plan in new[]{"current","pending","other"})
        {
            var installed=plan!="pending"||mode is "installed" or "v3";
            await Execute($"INSERT INTO local_parent_reconcile_plans(plan_id,root_key,scope_id,coherence_id,owner_epoch,revision,gap,journal,start_usn,cut_usn,status) VALUES('{plan}','root','scope','cohere',1,0,0,'journal',100,200,{(installed?3:0)});INSERT INTO local_parent_reconcile_parents(plan_id,node_id,path,created,claim) VALUES('{plan}','parent-{(plan=="other"?"other":"same")}','parent',1,'claim');INSERT INTO local_parent_reconcile_entries(plan_id,parent_id,node_id,name,canonical,created,kind,size,wire64,path) VALUES('{plan}','parent-{(plan=="other"?"other":"same")}','11111111111111111111111111111111','file','file',{(plan=="pending"?2:1)},0,1,'1111111111111111','parent/file')");
        }
        await Execute("INSERT INTO local_parent_reconcile_versions VALUES('cohere','parent-same','current','pending')");
        if(mode=="ambiguous")await Execute("INSERT INTO local_parent_reconcile_versions VALUES('cohere','parent-other','other',NULL)");
        if(mode=="fragmented")for(var n=0;n<65;n++)await Execute($"INSERT INTO local_parent_reconcile_plans(plan_id,root_key,scope_id,coherence_id,owner_epoch,revision,gap,journal,start_usn,status) VALUES('extra-{n}','root','scope','cohere',1,0,0,'journal',100,3)");
        using var transaction=c.BeginTransaction();var method=typeof(NeptuneStateStore).GetMethod("CoherenceNodeAsync",BindingFlags.NonPublic|BindingFlags.Static)!;
        async Task<object?> Lookup()
        {
            var args=new object?[method.GetParameters().Length];object?[] known=[c,transaction,"cohere",(ushort)(mode=="v3"?3:2),mode=="v3"?"11111111111111111111111111111111":"1111111111111111",ct,true];known.CopyTo(args,0);
            var task=(Task)method.Invoke(null,args)!;await task;return task.GetType().GetProperty("Result")!.GetValue(task);
        }
        if(mode=="ambiguous"){var failure=await Assert.ThrowsAsync<LocalSourceGapException>(async()=>{_ =await Lookup();});Assert.Equal("ParentAliasAmbiguity",failure.Code);}
        else{var found=Assert.IsType<object>(await Lookup(),exactMatch:false);Assert.Equal(mode is "installed" or "v3"?2L:1L,found.GetType().GetProperty("Created")!.GetValue(found));}
    }
}
