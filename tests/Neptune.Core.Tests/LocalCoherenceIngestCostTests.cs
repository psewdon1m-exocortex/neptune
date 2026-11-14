using System.Diagnostics;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class LocalCoherenceTests
{
    [Fact]
    public async Task OwnedSessionKeepsWalOpenWithoutPoolOrPinnedReadTransaction()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.Create();
        await using var c = await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source, Ct);
        var s = await f.Open(c);
        try
        {
            var state = await s.ReadStateAsync(Ct);
            SqliteConnection.ClearAllPools();
            Assert.True(File.Exists(f.Database + "-shm"), "An owned session must retain its WAL connection after global pool eviction.");
            await s.CapturePageAsync(state.Cursor, UsnRecordParser.Parse(state.Cursor.JournalIdHex, state.Cursor.NextReadUsn,
                SyntheticV3(state.Cursor.NextReadUsn, 1, "ffffffffffffffffffffffffffffffff", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", 1)), Ct);
            await using (var db = await f.Connection())
            {
                await using var q = db.CreateCommand(); q.CommandText = "PRAGMA wal_checkpoint(PASSIVE)";
                await using var r = await q.ExecuteReaderAsync(Ct); Assert.True(await r.ReadAsync(Ct));
                Assert.Equal(0, r.GetInt32(0)); Assert.Equal(r.GetInt32(1), r.GetInt32(2));
            }
            Assert.True(File.Exists(f.Database + "-shm"));
        }
        finally { await s.DisposeAsync(); }
        SqliteConnection.ClearAllPools();
        // A final read-only SQLite connection may leave reusable WAL/SHM
        // files. The ownership contract is released handles, not deletion.
        foreach (var file in new[] { f.Database, f.Database + "-wal", f.Database + "-shm" })
            if (File.Exists(file)) { using var exclusive = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.None); }
        await using var resumed = await f.Open(c);
        Assert.True((await resumed.ReadStateAsync(Ct)).Cursor.OwnerEpoch > 1);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task BoundedOutsideIngestMeasuresCaptureApplyAndRetirementCost(bool initializationPoolAlive)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.Create();
        await using var c = await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source, Ct);
        await using var s = await f.Open(c);
        var anchor = await c.BuildAnchorAsync(s, cancellationToken: Ct);
        anchor = await c.ObserveChangesAsync(s, anchor.CoherenceId, Ct);
        while ((await s.RetireCoherenceInboxBatchAsync(anchor.CoherenceId, Ct)).Rows > 0) { }
        var retainedBefore = (await s.ReadStateAsync(Ct)).InboxRecords;
        if (!initializationPoolAlive) SqliteConnection.ClearAllPools();
        var nativeBefore = await c.QueryJournalAsync(Ct);
        var samples = new List<object>();
        var wall = Stopwatch.StartNew();
        for (var page = 0; page < 16; page++)
        {
            var timer = Stopwatch.StartNew();
            var cursor = (await s.ReadStateAsync(Ct)).Cursor;
            var read = timer.Elapsed.TotalMilliseconds; timer.Restart();
            await s.CapturePageAsync(cursor, UsnRecordParser.Parse(cursor.JournalIdHex, cursor.NextReadUsn,
                SyntheticV3(cursor.NextReadUsn, 256, "ffffffffffffffffffffffffffffffff", "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", 1)), Ct);
            var capture = timer.Elapsed.TotalMilliseconds; timer.Restart();
            var applied = await s.ApplyNextCoherenceAsync(anchor.CoherenceId, cursor.NextReadUsn, Ct);
            Assert.Equal(256, applied.Records);
            Assert.Equal(256, applied.Outside);
            var apply = timer.Elapsed.TotalMilliseconds; timer.Restart();
            var retired = 0;
            while (true)
            {
                var batch = await s.RetireCoherenceInboxBatchAsync(anchor.CoherenceId, Ct);
                Assert.InRange(batch.ChangedRows, 0, 260);
                retired += batch.Rows;
                if (batch.Rows == 0) break;
            }
            Assert.Equal(256, retired);
            samples.Add(new { read, capture, apply, retire = timer.Elapsed.TotalMilliseconds });
        }
        wall.Stop();
        var nativeAfter = await c.QueryJournalAsync(Ct);
        Assert.Equal(retainedBefore, (await s.ReadStateAsync(Ct)).InboxRecords);
        await f.Evidence("bounded-ingest-cost", new { initializationPoolAlive, samples, records = 4096, wallMilliseconds = wall.Elapsed.TotalMilliseconds,
            nativeBefore, nativeAfter, metrics = f.Store.GetLocalCoherenceMetrics() });
    }
}
