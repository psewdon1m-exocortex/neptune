using System.Diagnostics;
using System.Reflection;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsFreshCycleTests
{
    private static Task<SqliteConnection> OwnedConnection(NeptuneStateStore store)
        =>(Task<SqliteConnection>)typeof(NeptuneStateStore).GetMethod("OpenLocalSourceConnectionAsync",BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(store,[Ct,true])!;

    [Fact]
    public async Task ActualOwnedConnectionsReuseHandlesAndReleaseOnlyTheirPoolBeforeUnlock()
    {
        if(!OperatingSystem.IsWindows()){Assert.Skip("Requires Windows native filesystem fixture");return;}
        await using var f=await Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"original");
        var engine=await CursorEngine(f,new(f.Remote));
        try
        {
            Assert.Null((await engine.InitialSyncAsync(Ct)).Error);
            var handles=new HashSet<object>(ReferenceEqualityComparer.Instance);var timer=Stopwatch.StartNew();
            for(var n=0;n<200;n++){await using var connection=await OwnedConnection(f.Store);handles.Add(connection.Handle!);await using var q=connection.CreateCommand();q.CommandText="SELECT version FROM local_cycle_format";Assert.Equal(4L,await q.ExecuteScalarAsync(Ct));}
            await f.Evidence("owned-connection-reuse",new{opens=200,handles=handles.Count,elapsedMs=timer.ElapsedMilliseconds});
            Assert.InRange(handles.Count,1,2);
            var before=await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0");Assert.Null((await engine.FreshSyncAsync(Ct)).Error);Assert.Equal(before,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));Assert.Equal(1,f.Remote.Prepares);
            using var unrelated=new SqliteConnection(new SqliteConnectionStringBuilder{DataSource=Path.Combine(f.Directory,"unrelated.db"),Pooling=true}.ToString());
            await unrelated.OpenAsync(Ct);var unrelatedHandle=unrelated.Handle;await unrelated.CloseAsync();
            // Fixture initialization uses the separate pre-existing legacy
            // pool. Close that pool only, leaving the owned and control pools.
            using(var legacy=new SqliteConnection((string)typeof(NeptuneStateStore).GetProperty("ConnectionString",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(f.Store)!))SqliteConnection.ClearPool(legacy);
            await engine.DisposeAsync();
            try{await unrelated.OpenAsync(Ct);Assert.Same(unrelatedHandle,unrelated.Handle);}finally{SqliteConnection.ClearPool(unrelated);await unrelated.CloseAsync();}
            // No ClearAllPools helper is allowed before this assertion.
            using(var exclusive=new FileStream(f.Database,FileMode.Open,FileAccess.ReadWrite,FileShare.None))Assert.True(exclusive.Length>0);
            await using var reopened=await CursorEngine(f,new(f.Remote));Assert.Equal(engine.ScopeId,reopened.ScopeId);Assert.Null((await reopened.FreshSyncAsync(Ct)).Error);Assert.Equal(before,await f.Scalar("SELECT baseline FROM local_cycle_files WHERE kind=0"));
        }
        finally{await engine.DisposeAsync();}
    }
    [Fact]
    public async Task ActualReusedConnectionRevalidatesSchemaAndRestoresForeignKeys()
    {
        if(!OperatingSystem.IsWindows()){Assert.Skip("Requires Windows native filesystem fixture");return;}
        await using var f=await Fixture.Create();await using var engine=await f.Open();Assert.Null((await engine.InitialSyncAsync(Ct)).Error);
        await using(var c=await OwnedConnection(f.Store)){await using var q=c.CreateCommand();q.CommandText="PRAGMA foreign_keys=OFF";await q.ExecuteNonQueryAsync(Ct);}
        await using(var c=await OwnedConnection(f.Store)){await using var q=c.CreateCommand();q.CommandText="PRAGMA foreign_keys";Assert.Equal(1L,await q.ExecuteScalarAsync(Ct));q.CommandText="PRAGMA schema_version=123456; ALTER TABLE local_cycle_format RENAME TO saved_local_cycle_format; CREATE TABLE local_cycle_format(singleton INTEGER PRIMARY KEY,version INTEGER);INSERT INTO local_cycle_format VALUES(1,999)";await q.ExecuteNonQueryAsync(Ct);}
        await Assert.ThrowsAsync<InvalidDataException>(()=>OwnedConnection(f.Store));
    }
}
