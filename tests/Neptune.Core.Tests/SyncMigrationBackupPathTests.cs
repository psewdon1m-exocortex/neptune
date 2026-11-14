using System.Reflection;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
[Collection("Native fresh cycle")]
public sealed class SyncMigrationBackupPathTests
{
    [Theory]
    [InlineData(254)][InlineData(280)]
    public async Task ActualNativeWalBackupSupportsDatabaseAndJournalCrossingMaxPath(int targetLength)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }var root=Path.Combine(Path.GetTempPath(),"neptune-owned-backup-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(root);var source=Path.Combine(root,"source.db");var target=Path.Combine(root,new string('t',targetLength-root.Length-4)+".db");Assert.Equal(targetLength,target.Length);
        try
        {
            await using var held=new SqliteConnection("Data Source="+source+";Cache=Private;Pooling=False");await held.OpenAsync(TestContext.Current.CancellationToken);await using(var q=held.CreateCommand()){q.CommandText="PRAGMA journal_mode=WAL;PRAGMA wal_autocheckpoint=0;CREATE TABLE owned_wal(value TEXT);INSERT INTO owned_wal VALUES('committed-WAL-only')";await q.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);}Assert.True(new FileInfo(source+"-wal").Length>0);
            var method=typeof(NeptuneStateStore).GetMethod("CreateConsistentSyncDatabaseCopyAsync",BindingFlags.NonPublic|BindingFlags.Static)!;await (Task)method.Invoke(null,[source,target,TestContext.Current.CancellationToken])!;
            // The public result/manifest path remains ordinary; qualification
            // of native I/O uses its explicit extended counterpart here.
            Assert.False(target.StartsWith("\\\\?\\",StringComparison.Ordinal));Assert.True(File.Exists(target));await using var copy=new SqliteConnection("Data Source=\\\\?\\"+target+";Mode=ReadOnly;Cache=Private;Pooling=False");await copy.OpenAsync(TestContext.Current.CancellationToken);await using var check=copy.CreateCommand();check.CommandText="SELECT value FROM owned_wal";Assert.Equal("committed-WAL-only",await check.ExecuteScalarAsync(TestContext.Current.CancellationToken));check.CommandText="PRAGMA quick_check";Assert.Equal("ok",await check.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        }
        finally{SqliteConnection.ClearAllPools();if(!Path.GetFullPath(root).StartsWith(Path.TrimEndingDirectorySeparator(Path.GetFullPath(Path.GetTempPath()))+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))throw new InvalidOperationException("Owned temp path escaped.");Directory.Delete(root,true);}
    }
}
