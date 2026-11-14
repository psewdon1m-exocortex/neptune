using Microsoft.Data.Sqlite;
using Neptune.Core;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("Native fresh cycle")]
public sealed class WindowsProfileBackupBoundaryTests
{
    [Fact]
    public async Task MixedSeparatorBackupPreservesSqliteWalAndOriginalSpoolOnce()
    {
        if(!OperatingSystem.IsWindows()){Assert.Skip("Windows path semantics required");return;}
        var root=Path.TrimEndingDirectorySeparator(Path.GetFullPath(Environment.GetEnvironmentVariable("NEPTUNE_FRESH_CYCLE_TEST_ROOT")??Path.GetTempPath()));
        var fixture=Path.Combine(root,"profile-backup-"+Guid.NewGuid().ToString("N"));
        Assert.Equal(root,Path.GetDirectoryName(Path.GetFullPath(fixture)));
        Assert.False(Directory.Exists(fixture));
        Directory.CreateDirectory(fixture);
        try
        {
        var state=Path.Combine(fixture,"profile");Directory.CreateDirectory(state);
        var profile=new WindowsProfileContext("backup-regression",state.Replace('\\','/'),false);
        var ct=TestContext.Current.CancellationToken;
        var file=Path.Combine(state,"neptune.db");await new NeptuneStateStore(file).InitializeAsync(ct);
        await using var writer=new SqliteConnection(new SqliteConnectionStringBuilder{DataSource=file,Pooling=false}.ToString());
        await writer.OpenAsync(ct);
        await using(var command=writer.CreateCommand())
        {command.CommandText="PRAGMA journal_mode=WAL; CREATE TABLE backup_test_original(id TEXT PRIMARY KEY,body BLOB); INSERT INTO backup_test_original VALUES('same-original',X'01020304');";await command.ExecuteNonQueryAsync(ct);}
        var spool=Path.Combine(state,"spool");Directory.CreateDirectory(spool);var original=new byte[]{1,2,3,4};await File.WriteAllBytesAsync(Path.Combine(spool,"original.bin"),original,ct);
        var destination=Path.Combine(fixture,"backup");await WindowsProfileCommands.BackupAsync(profile,destination,ct);
        Assert.False(File.Exists(Path.Combine(destination,"neptune.db-wal")));
        await using var restored=new SqliteConnection(new SqliteConnectionStringBuilder{DataSource=Path.Combine(destination,"neptune.db"),Mode=SqliteOpenMode.ReadOnly,Pooling=false}.ToString());await restored.OpenAsync(ct);
        await using(var command=restored.CreateCommand())
        {command.CommandText="PRAGMA quick_check";Assert.Equal("ok",await command.ExecuteScalarAsync(ct));command.CommandText="SELECT hex(body) FROM backup_test_original WHERE id='same-original'";Assert.Equal("01020304",await command.ExecuteScalarAsync(ct));}
        Assert.Equal(original,await File.ReadAllBytesAsync(Path.Combine(destination,"spool","original.bin"),ct));
        var raw=await File.ReadAllTextAsync(Path.Combine(destination,"backup.json"),ct);
        Assert.Contains("neptune.db",raw);
        await Assert.ThrowsAsync<IOException>(()=>WindowsProfileCommands.BackupAsync(profile,destination,ct));
        await Assert.ThrowsAsync<InvalidDataException>(()=>WindowsProfileCommands.BackupAsync(profile,Path.Combine(state,"inside"),ct));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            Assert.Equal(root,Path.GetDirectoryName(Path.GetFullPath(fixture)));
            Assert.False((File.GetAttributes(fixture)&FileAttributes.ReparsePoint)!=0);
            Directory.Delete(fixture,true);
        }
    }
}
