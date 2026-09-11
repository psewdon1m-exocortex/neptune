using Microsoft.Data.Sqlite;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class RetentionTests
{
    [Fact]
    public async Task CompletedHistoryIsBoundedAndActiveSpoolSurvivesCleanup()
    {
        var root = Path.Combine(Path.GetTempPath(), "neptune-retention-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var filename = Path.Combine(root, "state.db");
        try
        {
            var state = new NeptuneStateStore(filename);
            var cancellation = TestContext.Current.CancellationToken;
            await state.InitializeAsync(cancellation);
            var timestamp = DateTimeOffset.UtcNow.AddDays(-40);
            for (var index = 0; index < 1005; index++)
                await state.AddRunAsync(new BackupRun(index.ToString(), "kernel", "client", "complete", null, 1, "sha", null, 1, 1, timestamp.AddMinutes(index), timestamp.AddMinutes(index), null), cancellation);
            await using var connection = new SqliteConnection($"Data Source={filename}");
            await connection.OpenAsync(cancellation);
            await using var query = connection.CreateCommand(); query.CommandText = "SELECT COUNT(*) FROM backup_runs";
            Assert.InRange(Convert.ToInt32(await query.ExecuteScalarAsync(cancellation)), 1000, 1001);
            var spool = Path.Combine(root, "spool"); Directory.CreateDirectory(spool);
            var active = Path.Combine(spool, "active.zip"); var complete = Path.Combine(spool, "complete.zip"); var orphan = Path.Combine(spool, "orphan.zip");
            foreach (var file in new[] { active, complete, orphan }) { await File.WriteAllTextAsync(file, "synthetic", cancellation); File.SetLastWriteTimeUtc(file, DateTime.UtcNow.AddDays(-2)); }
            await state.AddRunAsync(new BackupRun("active", "volt", "client", "retry-wait", active, 9, "sha", null, 0, 1, timestamp, timestamp, null), cancellation);
            await state.AddRunAsync(new BackupRun("completed", "volt", "client", "complete", complete, 9, "sha", null, 9, 1, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, null), cancellation);
            await state.PruneSpoolAsync(spool, cancellation);
            Assert.True(File.Exists(active)); Assert.False(File.Exists(complete)); Assert.False(File.Exists(orphan));
        }
        finally { SqliteConnection.ClearAllPools(); Directory.Delete(root, true); }
    }

    [Fact]
    public async Task RemovingMappingCascadesMetadataAndTerminalCommandsDiscardPayload()
    {
        var root = Path.Combine(Path.GetTempPath(), "neptune-metadata-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(root);
        var filename = Path.Combine(root, "state.db");
        try
        {
            var state = new NeptuneStateStore(filename); var cancellation = TestContext.Current.CancellationToken;
            await state.InitializeAsync(cancellation);
            await state.AddMappingAsync(new SyncMapping("mapping", "client", root, true, DateTimeOffset.UtcNow), cancellation);
            await state.UpsertSyncFileAsync(new SyncFileRecord("mapping", "file.txt", 1, DateTimeOffset.UtcNow, "sha", "etag", "complete", DateTimeOffset.UtcNow, null), cancellation);
            await state.RemoveMappingAsync("mapping", "client", cancellation);
            Assert.Null(await state.GetSyncFileAsync("mapping", "file.txt", cancellation));
            await state.SaveRemoteCommandAsync("command", "volt", "archive.run", "synthetic-sensitive-payload", "succeeded", null, cancellation);
            await using var connection = new SqliteConnection($"Data Source={filename}"); await connection.OpenAsync(cancellation);
            await using var query = connection.CreateCommand(); query.CommandText = "SELECT payload FROM remote_commands WHERE command_id='command'";
            Assert.Equal("{}", await query.ExecuteScalarAsync(cancellation));
        }
        finally { SqliteConnection.ClearAllPools(); Directory.Delete(root, true); }
    }
}
