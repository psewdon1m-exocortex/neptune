using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class LocalSqlDiagnosticsTests
{
    [Fact]
    public async Task CountsExecutedStatementsInsteadOfCreatedCommands()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync(ct);
        SyncDiagnostics.AttachLocalSqliteDiagnostics(connection);
        var diagnostics = new SyncDiagnostics();
        using (diagnostics.Measure(SyncPhase.Transfer))
        {
            using var helper = SyncDiagnostics.MeasureLocalHelper("FixedTestHelper");
            using var unused = connection.CreateCommand(); unused.CommandText = "SELECT 99";
            using var reused = connection.CreateCommand(); reused.CommandText = "SELECT 1; SELECT 2";
            await reused.ExecuteNonQueryAsync(ct);
            reused.CommandText = "SELECT 3"; await reused.ExecuteScalarAsync(ct);
            await reused.ExecuteScalarAsync(ct);
        }
        Assert.Equal(4, diagnostics.Snapshot().Phases.Single(p => p.Phase == SyncPhase.Transfer).LocalSqlStatements);
        Assert.Equal(0, diagnostics.Snapshot().Phases.Single(p => p.Phase == SyncPhase.Idle).LocalSqlStatements);
        using var outside = connection.CreateCommand(); outside.CommandText = "SELECT 4"; await outside.ExecuteScalarAsync(ct);
        Assert.Equal(4, diagnostics.Snapshot().Phases.Sum(p => p.LocalSqlStatements));
        Assert.Equal(4, Assert.Single(diagnostics.Snapshot().LocalHelpers).Statements);
        Assert.Equal(1, Assert.Single(diagnostics.Snapshot().LocalHelpers).Calls);
    }
}
