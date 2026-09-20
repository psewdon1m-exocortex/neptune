using Microsoft.Data.Sqlite;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class MastermindMigrationTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConcurrentWorkersMigrateOnceAndPreserveExistingRuns(bool legacy)
    {
        var cancellation = TestContext.Current.CancellationToken;
        var directory = Path.Combine(Path.GetTempPath(), "neptune-migration-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            for (var iteration = 0; iteration < 4; iteration++)
            {
                var path = Path.Combine(directory, iteration + ".db");
                if (legacy)
                {
                    await new NeptuneStateStore(path).InitializeAsync(cancellation);
                    await using var seed = new SqliteConnection("Data Source=" + path);
                    await seed.OpenAsync(cancellation);
                    await using var command = seed.CreateCommand();
                    command.CommandText = """
                        ALTER TABLE backup_runs DROP COLUMN export_generation;
                        INSERT INTO backup_runs(run_id,project_id,client_instance_id,state,created_at,updated_at)
                        VALUES('preserved','mastermind','existing-client','complete','2026-01-01','2026-01-01');
                        """;
                    await command.ExecuteNonQueryAsync(cancellation);
                }
                var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var workers = Enumerable.Range(0, 16).Select(_ => Task.Run(async () =>
                {
                    await start.Task.WaitAsync(cancellation);
                    await new NeptuneStateStore(path).InitializeAsync(cancellation);
                }, cancellation)).ToArray();
                start.SetResult();
                await Task.WhenAll(workers).WaitAsync(TimeSpan.FromSeconds(45), cancellation);
                await using var check = new SqliteConnection("Data Source=" + path);
                await check.OpenAsync(cancellation);
                await using var inspect = check.CreateCommand();
                inspect.CommandText = "SELECT COUNT(*) FROM pragma_table_info('backup_runs') WHERE name='export_generation'";
                Assert.Equal(1L, await inspect.ExecuteScalarAsync(cancellation));
                inspect.CommandText = "SELECT COUNT(*) FROM backup_runs WHERE run_id='preserved' AND client_instance_id='existing-client' AND export_generation IS NULL";
                Assert.Equal(legacy ? 1L : 0L, await inspect.ExecuteScalarAsync(cancellation));
            }
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            Directory.Delete(directory, recursive: true);
        }
    }
}
