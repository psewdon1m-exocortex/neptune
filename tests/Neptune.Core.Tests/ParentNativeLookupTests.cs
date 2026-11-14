using System.Reflection;
using System.Runtime.InteropServices;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class ParentNativeLookupTests
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] private delegate int Progress(IntPtr state);
    [DllImport("e_sqlite3",CallingConvention=CallingConvention.Cdecl)]
    private static extern void sqlite3_progress_handler(IntPtr db,int instructions,Progress? callback,IntPtr state);

    [Theory]
    [InlineData("f.created,f.kind")]
    [InlineData("f.path,f.created,f.kind,f.node_id")]
    public async Task ParentScanAndImportProbeNativeIdentityWithoutRescanningCorpus(string columns)
    {
        var ct=TestContext.Current.CancellationToken;
        await using var c=new SqliteConnection("Data Source=:memory:");await c.OpenAsync(ct);
        async Task Execute(string sql){await using var q=c.CreateCommand();q.CommandText=sql;await q.ExecuteNonQueryAsync(ct);}
        await Execute("CREATE TABLE local_cycle_files(scope_id TEXT,node_id TEXT,path TEXT,created INTEGER,kind INTEGER,PRIMARY KEY(scope_id,node_id)) WITHOUT ROWID");
        await Execute((string)typeof(NeptuneStateStore).GetField("IncarnationDdl",BindingFlags.NonPublic|BindingFlags.Static)!.GetRawConstantValue()!);
        await Execute("WITH RECURSIVE n(v) AS(SELECT 1 UNION ALL SELECT v+1 FROM n WHERE v<10000) INSERT INTO local_cycle_files SELECT 'scope',printf('%032x',v),'file-'||v,v,v%2 FROM n;INSERT INTO local_cycle_files VALUES('other','ffffffffffffffffffffffffffffffff','foreign',77,1);INSERT INTO local_parent_reconcile_incarnations VALUES('scope','00000000000000000000000000000002','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',2,'00000000000000000000000000000002',1,'plan',100,0),('scope','00000000000000000000000000000003','00000000000000000000000000000003',3,'old',1,'plan',100,0),('other','ffffffffffffffffffffffffffffffff','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',77,'old',1,'plan',100,0)");
        var method=typeof(NeptuneStateStore).GetMethod("ParentNativeIdentitySql",BindingFlags.NonPublic|BindingFlags.Static)!;
        var actual=(string)method.Invoke(null,[columns])!;
        var reference="SELECT "+columns+" FROM local_cycle_files f LEFT JOIN local_parent_reconcile_incarnations i ON i.scope_id=f.scope_id AND i.node_id=f.node_id WHERE f.scope_id=$scope AND COALESCE(i.native_id,f.node_id)=$node";
        async Task<string[]> Lookup(string sql,string node)
        {
            await using var q=c.CreateCommand();q.CommandText=sql;q.Parameters.AddWithValue("$scope","scope");q.Parameters.AddWithValue("$node",node);
            await using var r=await q.ExecuteReaderAsync(ct);var rows=new List<string>();
            while(await r.ReadAsync(ct))rows.Add(string.Join("|",Enumerable.Range(0,r.FieldCount).Select(n=>r.GetValue(n).ToString())));
            return rows.OrderBy(x=>x,StringComparer.Ordinal).ToArray();
        }
        foreach(var node in new[]{"00000000000000000000000000000001","00000000000000000000000000000002","00000000000000000000000000000003","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","ffffffffffffffffffffffffffffffff"})
            Assert.Equal(await Lookup(reference,node),await Lookup(actual,node));
        long steps=0;Progress progress=_=>{steps++;return 0;};
        sqlite3_progress_handler(c.Handle!.DangerousGetHandle(),1,progress,IntPtr.Zero);
        try { for(var n=0;n<10;n++){Assert.Empty(await Lookup(actual,"ffffffffffffffffffffffffffffffff"));Assert.Single(await Lookup(actual,"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));} }
        finally {sqlite3_progress_handler(c.Handle!.DangerousGetHandle(),0,null,IntPtr.Zero);GC.KeepAlive(progress);}
        Assert.InRange(steps,1,5000);
    }
}
