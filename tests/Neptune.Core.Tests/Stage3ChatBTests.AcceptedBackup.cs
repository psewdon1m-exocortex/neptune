using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class Stage3ChatBTests
{
    public static bool NoHistoricalWriter => string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("NEPTUNE_HISTORICAL_CORE"));
    [Fact(Skip="Pinned predecessor DLL was removed by artifact cleanup; provide NEPTUNE_HISTORICAL_CORE. This is NOT_RUN, not rollback qualification.", SkipWhen=nameof(NoHistoricalWriter))]
    public async Task B12_PinnedHistoricalWriterRefusesAcceptedOriginalsWithoutChangingDatabaseOrWal()
    {
        RequireWindows();await using var f=await WindowsFreshCycleTests.Fixture.Create();
        File.WriteAllText(Path.Combine(f.Source,"original"),"sealed accepted bytes");f.Remote.LosePrepare=true;
        await using(var engine=await f.Open())
        {
            Assert.NotNull((await engine.InitialSyncAsync(Ct)).Error);
            await Session(engine).EnableSmallBatchesAsync(engine.ScopeId,Ct);
        }
        await VerifyAcceptedProfileBackup(f,"historical-writer",true);
    }
    private static async Task VerifyAcceptedProfileBackup(WindowsFreshCycleTests.Fixture f,string kind,bool historicalWriter=false)
    {
        // Transfer this closed engine fixture into the ordinary profile layout.
        // The transfer and tested backup both use SQLite's WAL-aware backup API.
        var root=Path.Combine(f.Directory,"accepted-profile");Directory.CreateDirectory(root);var database=Path.Combine(root,"neptune.db");
        await using(var source=new SqliteConnection("Data Source="+f.Database+";Mode=ReadOnly;Pooling=False"))
        await using(var copy=new SqliteConnection("Data Source="+database+";Pooling=False"))
        {await source.OpenAsync(Ct);await copy.OpenAsync(Ct);source.BackupDatabase(copy);}
        var spool=Path.Combine(f.Directory,"spool");var spoolCount=0;
        foreach(var path in Directory.EnumerateFiles(spool,"*",SearchOption.AllDirectories))
        {if(path.EndsWith(".lock",StringComparison.OrdinalIgnoreCase)||path.EndsWith(".partial",StringComparison.OrdinalIgnoreCase))continue;var target=Path.Combine(root,"spool",Path.GetRelativePath(spool,path));Directory.CreateDirectory(Path.GetDirectoryName(target)!);File.Copy(path,target);spoolCount++;}
        Assert.True(spoolCount>0);File.WriteAllText(Path.Combine(root,"enrollment.fixture"),"encrypted-fixture-canary");var profile=new WindowsProfileContext("accepted-"+kind,root,false);var output=Path.Combine(f.Directory,"accepted-output.json");
        await WindowsProfileCommands.ExecuteAsync(profile,["--profile-command","pause","--output",output]);
        var before=Assert.Single(await WindowsProfileCommands.InspectAsync(profile,Ct));Assert.True(before.ActiveOriginals>0);
        await using var held=new SqliteConnection("Data Source="+database+";Pooling=False");await held.OpenAsync(Ct);
        await using(var q=held.CreateCommand()){q.CommandText="PRAGMA journal_mode=WAL;PRAGMA wal_autocheckpoint=0;CREATE TABLE accepted_backup_marker(value TEXT);INSERT INTO accepted_backup_marker VALUES('committed WAL accepted originals')";await q.ExecuteNonQueryAsync(Ct);}
        Assert.True(File.Exists(database+"-wal"));var backup=Path.Combine(f.Directory,"accepted-restored");await WindowsProfileCommands.BackupAsync(profile,backup,Ct);
        var restored=new WindowsProfileContext(profile.ProfileId,backup,false);Assert.Equal(before.EvidenceSha256,Assert.Single(await WindowsProfileCommands.InspectAsync(restored,Ct)).EvidenceSha256);Assert.False(new WindowsPreferencesStore(backup).Read().SynchronizationEnabled);
        await using var restoredDb=new SqliteConnection("Data Source="+Path.Combine(backup,"neptune.db")+";Mode=ReadOnly;Pooling=False");await restoredDb.OpenAsync(Ct);
        // Compare every row of every operation/batch/stream table, including
        // serialized originals, per-item receipts and durable ACK maps.
        var tables=new List<string>();await using(var q=held.CreateCommand()){q.CommandText="SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE '%batch%' OR name LIKE '%stream%' OR name='local_cycle_work' OR name='accepted_backup_marker') ORDER BY name";await using var reader=await q.ExecuteReaderAsync(Ct);while(await reader.ReadAsync(Ct))tables.Add(reader.GetString(0));}
        static async Task<string> Rows(SqliteConnection connection,string table,CancellationToken ct)
        {var rows=new List<string>();await using var q=connection.CreateCommand();q.CommandText="SELECT * FROM \""+table.Replace("\"","\"\"")+"\"";await using var reader=await q.ExecuteReaderAsync(ct);while(await reader.ReadAsync(ct)){var values=new object?[reader.FieldCount];for(var n=0;n<values.Length;n++)values[n]=reader.IsDBNull(n)?null:reader.GetValue(n);rows.Add(System.Text.Json.JsonSerializer.Serialize(values));}rows.Sort(StringComparer.Ordinal);return string.Join('\n',rows);}
        foreach(var table in tables)Assert.Equal(await Rows(held,table,Ct),await Rows(restoredDb,table,Ct));
        object oldReaderEvidence=new{status="NOT_RUN",reason="Separate B12 pinned historical writer gate"};
        if(historicalWriter)
        {
        var oldCore=Path.GetFullPath(Environment.GetEnvironmentVariable("NEPTUNE_HISTORICAL_CORE")??throw new InvalidOperationException("Pinned predecessor path required"));const string predecessor="b3c26bf78f3c7a6329f1f97144de804f345fb192f749da9355988f729e273fc5";
        var oldManifest=Path.Combine(f.Directory,"old-writer.json");await File.WriteAllTextAsync(oldManifest,System.Text.Json.JsonSerializer.Serialize(new{core=oldCore,sha256=predecessor,database,profile=root}),Ct);
        static byte[] HashShared(string path){using var input=new FileStream(path,FileMode.Open,FileAccess.Read,FileShare.ReadWrite|FileShare.Delete);return SHA256.HashData(input);}
        var mainBefore=HashShared(database);var walBefore=HashShared(database+"-wal");var oldReader=await CacheChild("old-writer",oldManifest,0);Assert.True(oldReader.GetProperty("refused").GetBoolean());Assert.Equal(mainBefore,HashShared(database));Assert.Equal(walBefore,HashShared(database+"-wal"));
        oldReaderEvidence=new{status="PASS",oldReader,databaseAndWalUnchanged=true};
        }
        foreach(var file in Directory.EnumerateFiles(root,"*",SearchOption.AllDirectories).Where(path=>!path.StartsWith(database,StringComparison.OrdinalIgnoreCase)))
        {var copy=Path.Combine(backup,Path.GetRelativePath(root,file));Assert.Equal(SHA256.HashData(await File.ReadAllBytesAsync(file,Ct)),SHA256.HashData(await File.ReadAllBytesAsync(copy,Ct)));}
        await f.Evidence("B12-accepted-backup-"+kind,new{before.ActiveOriginals,before.UnknownOriginals,before.EvidenceSha256,spoolCount,tables,restored=true,wal=true,oldReader=oldReaderEvidence});
    }
}
