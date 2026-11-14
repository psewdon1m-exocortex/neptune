using System.Reflection;
using System.Runtime.InteropServices;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class RetainedNativeLookupTests
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] private delegate int Progress(IntPtr state);
    [DllImport("e_sqlite3",CallingConvention=CallingConvention.Cdecl)]
    private static extern void sqlite3_progress_handler(IntPtr db,int instructions,Progress? callback,IntPtr state);

    [Theory]
    [InlineData(false, 2)] [InlineData(true, 2)]
    [InlineData(false, 3)] [InlineData(true, 3)]
    public async Task RetainedRenameLookupPreservesIdentityWithoutScanningEveryManagedFile(bool incarnations,int major)
    {
        var ct=TestContext.Current.CancellationToken;
        await using var c=new SqliteConnection("Data Source=:memory:");await c.OpenAsync(ct);
        async Task Execute(string sql){await using var q=c.CreateCommand();q.CommandText=sql;await q.ExecuteNonQueryAsync(ct);}
        var schema=(Dictionary<string,string>)typeof(NeptuneStateStore).GetField("ParentReconcileSchema",BindingFlags.NonPublic|BindingFlags.Static)!.GetValue(null)!;
        foreach(var sql in schema.Values)await Execute(sql);
        await Execute("CREATE TABLE local_cycle_scopes(scope_id TEXT PRIMARY KEY,coherence_id TEXT,status INTEGER);CREATE INDEX coherence_scope ON local_cycle_scopes(coherence_id,scope_id);CREATE TABLE local_cycle_files(scope_id TEXT,node_id TEXT,PRIMARY KEY(scope_id,node_id)) WITHOUT ROWID;CREATE TABLE local_coherence_nodes(coherence_id TEXT,node_id TEXT,wire64 TEXT,PRIMARY KEY(coherence_id,node_id));CREATE INDEX coherence_wire ON local_coherence_nodes(coherence_id,wire64);INSERT INTO local_cycle_scopes VALUES('scope','cohere',0),('retired','cohere',3),('other','other',0);WITH RECURSIVE numbers(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM numbers WHERE n<5000) INSERT INTO local_cycle_files SELECT 'scope',printf('%032x',n) FROM numbers;INSERT INTO local_cycle_files VALUES('retired','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'),('other','cccccccccccccccccccccccccccccccc');INSERT INTO local_coherence_nodes VALUES('cohere','00000000000000000000000000000001','0000000000000001'),('cohere','00000000000000000000000000000002','0000000000000002'),('cohere','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','bbbbbbbbbbbbbbbb'),('cohere','cccccccccccccccccccccccccccccccc','cccccccccccccccc');INSERT INTO local_parent_reconcile_plans(plan_id,root_key,scope_id,coherence_id,owner_epoch,revision,gap,journal,start_usn,status) VALUES('plan','root','scope','cohere',1,0,0,'journal',100,3);INSERT INTO local_parent_reconcile_parents(plan_id,node_id,path,created,claim) VALUES('plan','parent','',1,'claim');INSERT INTO local_parent_reconcile_entries(plan_id,parent_id,node_id,name,canonical,created,kind,size,wire64,path) VALUES('plan','parent','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','file','file',2,0,33,'aaaaaaaaaaaaaaaa','file')");
        if(incarnations)
        {
            await Execute((string)typeof(NeptuneStateStore).GetField("IncarnationDdl",BindingFlags.NonPublic|BindingFlags.Static)!.GetRawConstantValue()!);
            await Execute("INSERT INTO local_parent_reconcile_incarnations VALUES('scope','00000000000000000000000000000002','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',2,'00000000000000000000000000000002',1,'plan',100,0)");
        }
        using var transaction=c.BeginTransaction();
        var method=typeof(NeptuneStateStore).GetMethod("IsRetainedCycleNative",BindingFlags.NonPublic|BindingFlags.Static)!;
        Task<bool> Lookup(string wire)=> (Task<bool>)method.Invoke(null,[c,transaction,"cohere",new LocalUsnRecord(1,wire,"parent",0x1000,0,0,"file",(ushort)major),ct])!;
        string Wire(string full)=>major==3?full:full[16..];
        Assert.True(await Lookup(Wire("00000000000000000000000000000001")));
        Assert.Equal(!incarnations,await Lookup(Wire("00000000000000000000000000000002")));
        Assert.Equal(incarnations,await Lookup(Wire("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")));
        Assert.False(await Lookup(Wire("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")));
        Assert.False(await Lookup(Wire("cccccccccccccccccccccccccccccccc")));
        long steps=0;Progress progress=_=>{steps++;return 0;};
        sqlite3_progress_handler(c.Handle!.DangerousGetHandle(),1,progress,IntPtr.Zero);
        try{for(var i=0;i<20;i++)Assert.False(await Lookup(Wire("ffffffffffffffffffffffffffffffff")));}
        finally{sqlite3_progress_handler(c.Handle!.DangerousGetHandle(),0,null,IntPtr.Zero);GC.KeepAlive(progress);}
        // Count database VM work instead of asserting timing on a busy PC.
        Assert.InRange(steps,1,100000);
    }
}
