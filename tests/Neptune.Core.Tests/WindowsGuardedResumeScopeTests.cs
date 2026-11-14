using Microsoft.Data.Sqlite;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class WindowsGuardedResumeScopeTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task RetainedCurrentAttemptsAndTerminalHistoryAreAllowedWithoutChangingTheirEvidence()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.AddAsync("current", 3, true);
        await fixture.AddAsync("terminal-history", 4, false);
        var before = await File.ReadAllBytesAsync(fixture.Database, Ct);
        await WindowsGuardedMappingSession.RequireNoOtherActiveScopeAsync(fixture.Database, "current", Ct);
        Assert.Equal(before, await File.ReadAllBytesAsync(fixture.Database, Ct));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(10)]
    public async Task AnotherActiveScopeRefusesAllRecognizedPhasesAndPreservesOriginalWork(int phase)
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.AddAsync("current", 3, true);
        await fixture.AddAsync("previous-original", phase, true);
        var before = await File.ReadAllBytesAsync(fixture.Database, Ct);
        await Assert.ThrowsAsync<InvalidDataException>(() => WindowsGuardedMappingSession.RequireNoOtherActiveScopeAsync(fixture.Database, "current", Ct));
        Assert.Equal(before, await File.ReadAllBytesAsync(fixture.Database, Ct));
    }

    [Fact]
    public async Task ExcessHistoricalScopesRefuseBoundedlyRatherThanWalkingUnknownHistory()
    {
        await using var fixture = await Fixture.CreateAsync();
        for (var i = 0; i < 17; i++) await fixture.AddAsync("old-" + i, 4, false);
        await Assert.ThrowsAsync<InvalidDataException>(() => WindowsGuardedMappingSession.RequireNoOtherActiveScopeAsync(fixture.Database, "current", Ct));
    }

    [Fact]
    public async Task CrossScopeProbeUsesScopeActiveIndex()
    {
        await using var fixture = await Fixture.CreateAsync();
        await using var db = new SqliteConnection("Data Source=" + fixture.Database + ";Pooling=False");
        await db.OpenAsync(Ct);
        await using var q = db.CreateCommand();
        q.CommandText = "EXPLAIN QUERY PLAN SELECT 1 FROM local_cycle_work INDEXED BY local_cycle_work_page WHERE scope_id=$scope AND active=1 LIMIT 1";
        q.Parameters.AddWithValue("$scope", "previous");
        await using var reader = await q.ExecuteReaderAsync(Ct);
        var details = new List<string>();
        while (await reader.ReadAsync(Ct)) details.Add(reader.GetString(3));
        Assert.Contains(details, d => d.Contains("SEARCH", StringComparison.Ordinal) && d.Contains("local_cycle_work_page", StringComparison.Ordinal) && d.Contains("scope_id=? AND active=?", StringComparison.Ordinal));
    }

    private sealed class Fixture : IAsyncDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "neptune-resume-bridge-" + Guid.NewGuid().ToString("N"));
        internal string Database => Path.Combine(_directory, "state.db");
        internal static async Task<Fixture> CreateAsync()
        {
            var fixture = new Fixture(); Directory.CreateDirectory(fixture._directory);
            await using var db = new SqliteConnection("Data Source=" + fixture.Database + ";Pooling=False");
            await db.OpenAsync(Ct); await using var q = db.CreateCommand();
            q.CommandText = "CREATE TABLE local_cycle_scopes(scope_id TEXT PRIMARY KEY);CREATE TABLE local_cycle_work(work_no INTEGER PRIMARY KEY,scope_id TEXT NOT NULL,active INTEGER,phase INTEGER NOT NULL);CREATE INDEX local_cycle_work_page ON local_cycle_work(scope_id,active,phase,work_no)";
            await q.ExecuteNonQueryAsync(Ct); return fixture;
        }
        internal async Task AddAsync(string scope, int phase, bool active)
        {
            await using var db = new SqliteConnection("Data Source=" + Database + ";Pooling=False");
            await db.OpenAsync(Ct); await using var q = db.CreateCommand();
            q.CommandText = "INSERT INTO local_cycle_scopes(scope_id) VALUES($scope);INSERT INTO local_cycle_work(scope_id,active,phase) VALUES($scope,$active,$phase)";
            q.Parameters.AddWithValue("$scope", scope); q.Parameters.AddWithValue("$active", active ? 1 : DBNull.Value); q.Parameters.AddWithValue("$phase", phase);
            await q.ExecuteNonQueryAsync(Ct);
        }
        public ValueTask DisposeAsync()
        {
            SqliteConnection.ClearAllPools();
            var parent = Path.TrimEndingDirectorySeparator(Path.GetFullPath(Path.GetTempPath()));
            if (!string.Equals(Path.GetDirectoryName(Path.GetFullPath(_directory)), parent, StringComparison.OrdinalIgnoreCase)) throw new InvalidDataException("Owned test directory escaped its temporary parent.");
            Directory.Delete(_directory, recursive: true); return ValueTask.CompletedTask;
        }
    }
}
