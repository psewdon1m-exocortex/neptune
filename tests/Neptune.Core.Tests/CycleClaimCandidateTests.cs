using System.Reflection;
using System.Runtime.InteropServices;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class CycleClaimCandidateTests
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] private delegate int Progress(IntPtr state);
    [DllImport("e_sqlite3",CallingConvention=CallingConvention.Cdecl)]
    private static extern void sqlite3_progress_handler(IntPtr db,int instructions,Progress? callback,IntPtr state);

    [Theory]
    [InlineData("ready")][InlineData("delayed")][InlineData("excluded")]
    [InlineData("parent-conflict")][InlineData("active-original")][InlineData("remote-conflict")]
    [InlineData("remote-parent-conflict")][InlineData("other-scope")][InlineData("directory-only")]
    public async Task ClaimKeepsAdmissionGatesWithoutScanningConfirmedPrefix(string mode)
    {
        var ct=TestContext.Current.CancellationToken;
        await using var c=new SqliteConnection("Data Source=:memory:");await c.OpenAsync(ct);
        c.CreateFunction<string,string,bool>("neptune_path_within",MappingSyncPolicy.IsWithin,isDeterministic:true);
        async Task Execute(string sql){await using var q=c.CreateCommand();q.CommandText=sql;await q.ExecuteNonQueryAsync(ct);}
        await Execute("""
            CREATE TABLE local_cycle_files(scope_id TEXT,node_id TEXT,parent_id TEXT,path TEXT,phase INTEGER,kind INTEGER,remote_node TEXT,PRIMARY KEY(scope_id,node_id)) WITHOUT ROWID;
            CREATE INDEX local_cycle_files_page ON local_cycle_files(scope_id,phase,node_id);
            CREATE TABLE local_cycle_work(scope_id TEXT,node_id TEXT,active INTEGER,UNIQUE(scope_id,node_id,active));
            CREATE TABLE client_event_inbox(root_key TEXT,path TEXT,due_at INTEGER,PRIMARY KEY(root_key,path)) WITHOUT ROWID;
            CREATE TABLE local_cycle_scopes(scope_id TEXT PRIMARY KEY,active_remote TEXT);
            CREATE TABLE local_cycle_refreshes(scope_id TEXT PRIMARY KEY,reconcile_pass INTEGER);
            CREATE TABLE local_cycle_refresh_conflicts(scope_id TEXT,node_id TEXT,pass INTEGER,PRIMARY KEY(scope_id,node_id)) WITHOUT ROWID;
            INSERT INTO local_cycle_scopes VALUES('scope','remote');INSERT INTO local_cycle_refreshes VALUES('remote',1);
            INSERT INTO local_cycle_files VALUES('scope','z-parent','root','folder',4,1,'remote-parent'),('scope','z-target','z-parent','folder/file',0,0,NULL);
            WITH RECURSIVE n(x) AS(SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<10000)
            INSERT INTO local_cycle_files SELECT 'scope',printf('a-%05d',x),'root',printf('done-%05d',x),4,0,'published' FROM n;
            """);
        switch(mode)
        {
            case "delayed":await Execute("INSERT INTO client_event_inbox VALUES('event-root','folder/file',2000)");break;
            case "parent-conflict":await Execute("UPDATE local_cycle_files SET phase=2 WHERE node_id='z-parent'");break;
            case "active-original":await Execute("INSERT INTO local_cycle_work VALUES('scope','z-target',1)");break;
            case "remote-conflict":await Execute("INSERT INTO local_cycle_refresh_conflicts VALUES('remote','z-target',1)");break;
            case "remote-parent-conflict":await Execute("INSERT INTO local_cycle_refresh_conflicts VALUES('remote','z-parent',1)");break;
            case "other-scope":await Execute("UPDATE local_cycle_files SET scope_id='other' WHERE node_id='z-target'");break;
        }
        var sql=(string)typeof(NeptuneStateStore).GetMethod("CycleClaimCandidateSql",BindingFlags.NonPublic|BindingFlags.Static)!.Invoke(null,[mode=="directory-only",true])!;
        long steps=0;Progress progress=_=>{steps++;return 0;};
        sqlite3_progress_handler(c.Handle!.DangerousGetHandle(),1,progress,IntPtr.Zero);
        try
        {
            for(var i=0;i<5;i++)
            {
                await using var q=c.CreateCommand();q.CommandText=sql;
                q.Parameters.AddWithValue("$scope","scope");q.Parameters.AddWithValue("$root","root");
                q.Parameters.AddWithValue("$eventRoot","event-root");q.Parameters.AddWithValue("$now",1000);
                q.Parameters.AddWithValue("$excludes",mode=="excluded"?"[\"folder\"]":"[]");
                Assert.Equal(mode=="ready"?"z-target":null,await q.ExecuteScalarAsync(ct));
            }
        }
        finally{sqlite3_progress_handler(c.Handle!.DangerousGetHandle(),0,null,IntPtr.Zero);GC.KeepAlive(progress);}
        Assert.InRange(steps,1,2000);
    }
}
