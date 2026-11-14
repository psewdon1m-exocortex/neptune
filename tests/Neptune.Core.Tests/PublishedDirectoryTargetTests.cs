using System.Reflection;
using System.Runtime.InteropServices;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class PublishedDirectoryTargetTests
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] private delegate int Progress(IntPtr state);
    [DllImport("e_sqlite3",CallingConvention=CallingConvention.Cdecl)]
    private static extern void sqlite3_progress_handler(IntPtr db,int instructions,Progress? callback,IntPtr state);

    [Theory]
    [InlineData("published")][InlineData("inactive")][InlineData("wrong-phase")]
    [InlineData("file")][InlineData("other-scope")][InlineData("missing")]
    public async Task OriginalDirectoryLookupDoesNotScanUnrelatedPublicationHistory(string mode)
    {
        var ct=TestContext.Current.CancellationToken;
        await using var c=new SqliteConnection("Data Source=:memory:");await c.OpenAsync(ct);
        async Task Execute(string sql){await using var q=c.CreateCommand();q.CommandText=sql;await q.ExecuteNonQueryAsync(ct);}
        await Execute("CREATE TABLE local_cycle_files(scope_id TEXT,node_id TEXT,remote_node TEXT,kind INTEGER,PRIMARY KEY(scope_id,node_id),UNIQUE(scope_id,remote_node)) WITHOUT ROWID;CREATE TABLE local_cycle_work(work_no INTEGER PRIMARY KEY,scope_id TEXT,node_id TEXT,phase INTEGER,active INTEGER,result_json TEXT,UNIQUE(scope_id,node_id,active));CREATE INDEX local_cycle_work_page ON local_cycle_work(scope_id,active,phase,work_no);CREATE INDEX local_cycle_work_node ON local_cycle_work(scope_id,node_id,phase,work_no);INSERT INTO local_cycle_files VALUES('scope','folder','remote-folder',1);INSERT INTO local_cycle_work VALUES(1,'scope','folder',5,1,'original folder receipt');WITH RECURSIVE numbers(n) AS(SELECT 2 UNION ALL SELECT n+1 FROM numbers WHERE n<=10000) INSERT INTO local_cycle_work SELECT n,'scope',printf('file-%05d',n),5,1,'unrelated file receipt' FROM numbers");
        switch(mode)
        {
            case "inactive":await Execute("UPDATE local_cycle_work SET active=NULL WHERE work_no=1");break;
            case "wrong-phase":await Execute("UPDATE local_cycle_work SET phase=6 WHERE work_no=1");break;
            case "file":await Execute("UPDATE local_cycle_files SET kind=0");break;
            case "other-scope":await Execute("UPDATE local_cycle_files SET scope_id='other';UPDATE local_cycle_work SET scope_id='other' WHERE work_no=1");break;
            case "missing":await Execute("DELETE FROM local_cycle_files");break;
        }
        var sql=(string)typeof(NeptuneStateStore).GetField("PublishedDirectoryTargetSql",BindingFlags.NonPublic|BindingFlags.Static)!.GetRawConstantValue()!;
        long steps=0;Progress progress=_=>{steps++;return 0;};
        sqlite3_progress_handler(c.Handle!.DangerousGetHandle(),1,progress,IntPtr.Zero);
        try
        {
            for(var i=0;i<20;i++){await using var q=c.CreateCommand();q.CommandText=sql;q.Parameters.AddWithValue("$scope","scope");q.Parameters.AddWithValue("$node","remote-folder");Assert.Equal(mode=="published"?"original folder receipt":null,await q.ExecuteScalarAsync(ct));}
        }
        finally{sqlite3_progress_handler(c.Handle!.DangerousGetHandle(),0,null,IntPtr.Zero);GC.KeepAlive(progress);}
        Assert.InRange(steps,1,5000);
    }
}
