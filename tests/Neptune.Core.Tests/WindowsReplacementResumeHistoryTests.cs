using Microsoft.Data.Sqlite;
using Neptune.Core;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WindowsResumeAllowsOnlyQualifiedReplacementHistory(bool twice)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create();
        File.WriteAllText(Path.Combine(f.Source, "original"), "retained published bytes");
        Directory.CreateDirectory(Path.Combine(f.Source, "empty"));
        await Medium(async () =>
        {
            string original;
            await using (var first = await Open(f))
            {
                original = first.ScopeId;
                Assert.Null((await first.InitialSyncAsync(Ct)).Error);
                await SetHistoryActive(f.Database, original);
                await Session(first).MarkGapAsync("JournalGap", Ct);
            }
            var before = await Snapshot(f, original);
            var previous = original;
            if (twice)
            {
                await using var middle = await Replace(f, new(Guid.NewGuid().ToString("N"), previous));
                await middle.QualifyJournalReplacementAsync(Ct);
                Assert.Null((await middle.FreshSyncAsync(Ct)).Error);
                previous = middle.ScopeId;
                await Session(middle).MarkGapAsync("JournalGap", Ct);
            }
            await using var current = await Replace(f, new(Guid.NewGuid().ToString("N"), previous));
            await current.QualifyJournalReplacementAsync(Ct);
            await WindowsGuardedMappingSession.RequireNoOtherActiveScopeAsync(f.Database, current.ScopeId, Ct, current);
            Assert.Equal(before, await Snapshot(f, original));
            return true;
        });
    }

    [Theory]
    [InlineData("input")]
    [InlineData("outcome")]
    [InlineData("missing-proof")]
    [InlineData("wrong-node")]
    [InlineData("pending")]
    [InlineData("unknown-phase")]
    [InlineData("gap")]
    [InlineData("uninstalled")]
    public async Task WindowsResumeRefusesUnqualifiedRetainedEvidenceWithoutWriting(string changed)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create();
        File.WriteAllText(Path.Combine(f.Source, "original"), "retain original outcome");
        Directory.CreateDirectory(Path.Combine(f.Source, "empty"));
        await Medium(async () =>
        {
            string original;
            await using (var first = await Open(f))
            {
                original = first.ScopeId;
                Assert.Null((await first.InitialSyncAsync(Ct)).Error);
                await SetHistoryActive(f.Database, original);
                await Session(first).MarkGapAsync("JournalGap", Ct);
            }
            var request = new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"), original);
            await using var current = await Replace(f, request);
            await current.QualifyJournalReplacementAsync(Ct);
            await using (var c = new SqliteConnection("Data Source=" + f.Database + ";Pooling=False"))
            {
                await c.OpenAsync(Ct); await using var q = c.CreateCommand();
                q.Parameters.AddWithValue("$scope", original); q.Parameters.AddWithValue("$request", request.ReplacementId);
                q.CommandText = changed switch
                {
                    "input" => "UPDATE local_cycle_work SET input_json=input_json||' ' WHERE scope_id=$scope AND phase=5",
                    "outcome" => "UPDATE local_cycle_work SET result_json=result_json||' ' WHERE scope_id=$scope AND phase=5",
                    "missing-proof" => "DELETE FROM local_cycle_replacement_originals WHERE replacement_id=$request",
                    "wrong-node" => "UPDATE local_cycle_replacement_originals SET node_id='wrong-node' WHERE replacement_id=$request",
                    "pending" => "UPDATE local_cycle_work SET phase=4 WHERE scope_id=$scope AND phase=5",
                    "unknown-phase" => "UPDATE local_cycle_work SET phase=10 WHERE scope_id=$scope AND phase=5",
                    "gap" => "UPDATE local_source_roots SET gap_code='InboxCapacity' WHERE root_key=(SELECT root_key FROM local_cycle_scopes WHERE scope_id=$scope)",
                    "uninstalled" => "UPDATE local_cycle_replacement_requests SET status=2 WHERE replacement_id=$request",
                    _ => throw new InvalidOperationException()
                };
                Assert.True(await q.ExecuteNonQueryAsync(Ct) > 0);
            }
            var before = await Snapshot(f, original); var prepares = f.Remote.Prepares;
            var database = await ReadShared(f.Database);
            var wal = await ReadShared(f.Database + "-wal");
            await Assert.ThrowsAsync<InvalidDataException>(() => WindowsGuardedMappingSession.RequireNoOtherActiveScopeAsync(f.Database, current.ScopeId, Ct, current));
            Assert.Equal(database, await ReadShared(f.Database));
            Assert.Equal(wal, await ReadShared(f.Database + "-wal"));
            Assert.Equal(before, await Snapshot(f, original)); Assert.Equal(prepares, f.Remote.Prepares);
            return true;
        });
    }

    private static async Task<byte[]> ReadShared(string path)
    {
        await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        using var bytes = new MemoryStream(); await stream.CopyToAsync(bytes, Ct); return bytes.ToArray();
    }

    private static async Task SetHistoryActive(string database, string scope)
    {
        await using var c = new SqliteConnection("Data Source=" + database + ";Pooling=False");
        await c.OpenAsync(Ct);
        await using var q = c.CreateCommand();
        q.CommandText = "UPDATE local_cycle_work SET active=1 WHERE scope_id=$scope AND phase=5";
        q.Parameters.AddWithValue("$scope", scope);
        Assert.Equal(2, await q.ExecuteNonQueryAsync(Ct));
    }
}
