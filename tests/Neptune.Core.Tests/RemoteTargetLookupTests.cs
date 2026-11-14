using System.Reflection;
using System.Runtime.InteropServices;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class RemoteTargetLookupTests
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] private delegate int Progress(IntPtr state);
    [DllImport("e_sqlite3", CallingConvention = CallingConvention.Cdecl)]
    private static extern void sqlite3_progress_handler(IntPtr db, int instructions, Progress? callback, IntPtr state);

    [Theory]
    [InlineData(false, "present")][InlineData(true, "present")]
    [InlineData(false, "absent")][InlineData(true, "absent")]
    [InlineData(false, "collision")][InlineData(true, "collision")]
    [InlineData(false, "deleted")][InlineData(true, "deleted")]
    public async Task LookupIsBoundedByNameAndRetainsCollisionEvidence(bool refresh, string mode)
    {
        var ct = TestContext.Current.CancellationToken;
        var table = refresh ? "local_cycle_refresh_remote" : "local_cycle_remote";
        var index = refresh ? "local_cycle_refresh_name" : "local_cycle_remote_name";
        await using var connection = new SqliteConnection("Data Source=:memory:"); await connection.OpenAsync(ct);
        async Task Execute(string sql) { await using var q = connection.CreateCommand(); q.CommandText = sql; await q.ExecuteNonQueryAsync(ct); }
        // Same WITHOUT ROWID layout and name index as both durable generations.
        // No ANALYZE: new/restored client profiles have no cardinality statistics.
        await Execute($"CREATE TABLE {table}(scope_id TEXT NOT NULL,node_id TEXT NOT NULL,parent_id TEXT,name TEXT NOT NULL,name_key TEXT NOT NULL,kind INTEGER NOT NULL,description TEXT NOT NULL,seq INTEGER NOT NULL,deleted INTEGER NOT NULL,bytes INTEGER NOT NULL,PRIMARY KEY(scope_id,node_id)) WITHOUT ROWID;CREATE INDEX {index} ON {table}(scope_id,parent_id,name_key,deleted,node_id);WITH RECURSIVE n(i) AS(SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<10000) INSERT INTO {table} SELECT 'scope',printf('noise-%05d',i),'parent',printf('unrelated-%05d',i),printf('unrelated-%05d',i),0,'unrelated',1,0,256 FROM n;INSERT INTO {table} VALUES('other-scope','wrong-scope','parent','target','target',0,'wrong scope',1,0,256),('scope','wrong-parent','other-parent','target','target',0,'wrong parent',1,0,256)");
        if (mode != "absent") await Execute($"INSERT INTO {table} VALUES('scope','target','parent','target','target',0,'original',1,{(mode == "deleted" ? 1 : 0)},256)");
        if (mode == "collision") await Execute($"INSERT INTO {table} VALUES('scope','collision','parent','TARGET','target',0,'canonical collision',1,0,256)");
        var method = typeof(NeptuneStateStore).GetMethod("CycleRemoteTargetSql", BindingFlags.NonPublic | BindingFlags.Static)!;
        var sql = (string)method.Invoke(null, [table])!;
        long steps = 0; Progress progress = _ => { steps += 100; return 0; };
        sqlite3_progress_handler(connection.Handle!.DangerousGetHandle(), 100, progress, IntPtr.Zero);
        try
        {
            for (var i = 0; i < 20; i++)
            {
                await using var q = connection.CreateCommand(); q.CommandText = sql;
                q.Parameters.AddWithValue("$scope", "scope"); q.Parameters.AddWithValue("$parent", "parent"); q.Parameters.AddWithValue("$key", "target");
                var actual = new List<string>(); await using var reader = await q.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct)) actual.Add(reader.GetString(0));
                var expected = mode switch { "present" => new[] { "original" }, "collision" => ["canonical collision", "original"], _ => [] };
                Assert.Equal(expected.Order(), actual.Order());
            }
        }
        finally { sqlite3_progress_handler(connection.Handle!.DangerousGetHandle(), 0, null, IntPtr.Zero); GC.KeepAlive(progress); }
        // VM operations, not flaky elapsed-time assertions. A root scan performs
        // >1M steps here; exact name seeks must remain under this bounded budget.
        Assert.InRange(steps, 0, 5000);
    }
}
