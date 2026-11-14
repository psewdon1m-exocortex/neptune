using System.Globalization;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("Native fresh cycle")]
[Trait("Lane", "Live")]
public sealed class ReleaseAcceptanceSnapshotBoundaryTests
{
    [Theory]
    [InlineData("truncated-body")]
    [InlineData("missing-final-brace")]
    [InlineData("duplicate-node")]
    [InlineData("wrong-start-key")]
    [InlineData("wrong-terminal-key")]
    [InlineData("empty-cursor")]
    public void F14_ProductionSnapshotParserRejectsTruncationDuplicatesAndCursorGaps(string change)
    {
        using var corpus = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(AppContext.BaseDirectory, "TestData", "SyncWire", "snapshot-pages.v1.json")));
        var ready = SyncProtocolReader.ReadSnapshot(Encoding.UTF8.GetBytes(corpus.RootElement.GetProperty("snapshot").GetRawText()));
        var vector = corpus.RootElement.GetProperty("cases").EnumerateArray().Single(row => row.GetProperty("name").GetString() == "nonempty terminal page");
        var after = vector.GetProperty("afterNodeId").GetString();
        var raw = Convert.FromHexString(vector.GetProperty("inputHex").GetString()!);
        Assert.NotEmpty(SyncSnapshotEntriesReader.Read(raw, ready, after).Items);
        if (change == "truncated-body") raw = raw[..(raw.Length / 2)];
        else if (change == "missing-final-brace") raw = raw[..^1];
        else
        {
            var page = JsonNode.Parse(raw)!;
            if (change == "duplicate-node") page["items"]!.AsArray()[1] = page["items"]!.AsArray()[0]!.DeepClone();
            else if (change == "wrong-start-key") page["after_node_id"] = "skipped-start";
            else if (change == "wrong-terminal-key") page["last_node_id"] = "skipped-terminal";
            else page["next_cursor"] = "";
            raw = JsonSerializer.SerializeToUtf8Bytes(page);
        }
        Assert.Throws<SyncWireException>(() => SyncSnapshotEntriesReader.Read(raw, ready, after));
    }

    [Fact]
    public async Task F14_ProductionSnapshotParserAndDurablePagesCrossOneMillionAndResumeExactPrefix()
    {
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException("Owned Windows native profile required.");
        var target = int.Parse(Environment.GetEnvironmentVariable("NEPTUNE_ACCEPTANCE_SNAPSHOT_ROWS") ?? throw new InvalidOperationException("Explicit owned snapshot row count required."), CultureInfo.InvariantCulture);
        Assert.True(target == 1000 || target == 1_004_282, "Only small fixture probe or full F14 boundary admitted.");
        var ct = TestContext.Current.CancellationToken;
        var fixture = await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());
        var budget = new WindowsCycleBudget(DataJobs: 1, LargeJobs: 1, MinimumFreeDiskBytes: 0, ProfileBytes: 2L * 1024 * 1024 * 1024);
        var configuration = new WindowsCycleConfiguration("mapping", "client", "audit", "rules-v1", new Uri("https://test.invalid/"), fixture.Remote.Device, "audit-token", "local-test", fixture.Remote.Binding);
        WindowsFreshSyncEngine? engine = null;
        var cuts = target == 1000 ? new[] { 500, 1000 } : new[] { 999_999, 1_000_000, 1_000_001, 1_004_282 };
        var checkpoints = new List<object>();
        var timer = System.Diagnostics.Stopwatch.StartNew();
        string? stageId = null;
        var reopened = 0;
        try
        {
            engine = await Open();
            Assert.Null((await engine.InitialSyncAsync(ct)).Error);
            var scope = engine.ScopeId;
            var stage = await Session().BeginCycleRefreshAsync(scope, false, ct);
            stageId = stage.Id;
            var ready = new SyncSnapshotReady(configuration.Binding, stage.SnapshotId, "0", "entries", "changes", 60, 3600);
            await Session().SaveCycleStageReadyAsync(scope, ready, ct);
            var count = 0;
            string? after = null;
            while (count < target)
            {
                var nextCut = cuts.First(cut => cut > count);
                var length = Math.Min(500, nextCut - count);
                var descriptors = Enumerable.Range(count, length).Select(Descriptor).ToArray();
                var raw = JsonSerializer.SerializeToUtf8Bytes(new
                {
                    protocol = ready.Binding.Protocol, descriptor_schema = ready.Binding.DescriptorSchema,
                    server_identity = ready.Binding.ServerIdentity, root_id = ready.Binding.RootId,
                    server_epoch = ready.Binding.ServerEpoch, policy_version = ready.Binding.PolicyVersion,
                    binding_version = ready.Binding.BindingVersion, snapshot_id = ready.SnapshotId,
                    cut_seq = ready.CutSeq, after_node_id = after, last_node_id = descriptors[^1].NodeId,
                    items = descriptors.Select(WindowsFreshCycleTests.FakeRemote.Wire),
                    next_cursor = "entries-" + (count + length).ToString(CultureInfo.InvariantCulture), has_more = count + length < target
                });
                var page = SyncSnapshotEntriesReader.Read(raw, ready, after);
                Assert.InRange(raw.Length, 1, SyncProtocolReader.MaxPageBytes);
                if (count == 0)
                {
                    // Persist part of a page without advancing its durable
                    // cursor, reopen the actual native profile, then replay.
                    await Session().SaveCycleStageEntriesAsync(scope, page, 0, ct);
                    var partial = await Session().ReadCycleStageProgressAsync(scope, ct);
                    Assert.Null(partial.AfterNodeId);
                    Assert.Equal(120L, await fixture.Scalar("SELECT COUNT(*) FROM local_cycle_refresh_remote"));
                    await Reopen();
                    Assert.Equal(partial, await Session().ReadCycleStageProgressAsync(scope, ct));
                }
                for (var offset = 0; offset < page.Items.Count; offset += 120)
                    await Session().SaveCycleStageEntriesAsync(scope, page, offset, ct);
                count += length;
                after = page.LastNodeId;
                if (cuts.Contains(count))
                {
                    var progress = await Session().ReadCycleStageProgressAsync(scope, ct);
                    Assert.Equal(after, progress.AfterNodeId);
                    Assert.Equal((long)count, await fixture.Scalar("SELECT COUNT(*) FROM local_cycle_refresh_remote"));
                    checkpoints.Add(new { count, progress.SnapshotId, progress.AfterNodeId, progress.EntriesCursor, progress.SnapshotComplete, elapsedSeconds = timer.Elapsed.TotalSeconds });
                    await fixture.Evidence("F14-snapshot-progress", new { target, count, reopened, checkpoints, database = fixture.Database, stageId, elapsedSeconds = timer.Elapsed.TotalSeconds });
                    if (count == (target == 1000 ? 500 : 1_000_000))
                    {
                        await Reopen();
                        Assert.Equal(progress, await Session().ReadCycleStageProgressAsync(scope, ct));
                    }
                }
                else if (count % 5000 == 0)
                    await fixture.Evidence("F14-snapshot-progress", new { target, count, reopened, database = fixture.Database, stageId, elapsedSeconds = timer.Elapsed.TotalSeconds });
            }
            var completed = await Session().ReadCycleStageProgressAsync(scope, ct);
            Assert.True(completed.SnapshotComplete);
            Assert.Equal(2, reopened);
            Assert.Equal(scope, engine.ScopeId);
            Assert.Equal(stageId, (await Session().ReadCycleRefreshAsync(scope, ct))!.Id);
            using var expectedHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            using var actualHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            await using (var database = new SqliteConnection("Data Source=" + fixture.Database + ";Mode=ReadOnly;Pooling=False"))
            {
                await database.OpenAsync(ct);
                await using var query = database.CreateCommand();
                query.CommandText = "SELECT description FROM local_cycle_refresh_remote WHERE scope_id=$scope ORDER BY node_id";
                query.Parameters.AddWithValue("$scope", stageId);
                await using var reader = await query.ExecuteReaderAsync(ct);
                var index = 0;
                while (await reader.ReadAsync(ct))
                {
                    var actual = JsonSerializer.Deserialize<SyncDescriptor>(reader.GetString(0), LocalCycleWire.Json)!;
                    var expected = Descriptor(index);
                    Assert.Equal(expected, actual);
                    expectedHash.AppendData(Encoding.UTF8.GetBytes(Canonical(expected)));
                    actualHash.AppendData(Encoding.UTF8.GetBytes(Canonical(actual)));
                    index++;
                }
                Assert.Equal(target, index);
            }
            var expectedSha = Convert.ToHexStringLower(expectedHash.GetHashAndReset());
            var actualSha = Convert.ToHexStringLower(actualHash.GetHashAndReset());
            Assert.Equal(expectedSha, actualSha);
            await fixture.Evidence("F14-snapshot-boundary-result", new { target, fullBoundary = target == 1_004_282, checkpoints, reopened, expectedSha, actualSha, rowsVerified = target, database = fixture.Database, stageId, completed, elapsedSeconds = timer.Elapsed.TotalSeconds, metrics = fixture.Store.GetLocalCycleMetrics(), physicalSourceFiles = 0, qualification = "production parser and durable snapshot paging only; no million-file filesystem sync or API/TLS claim" });
        }
        finally
        {
            if (engine is not null) await engine.DisposeAsync();
            SqliteConnection.ClearAllPools();
            // Retain this explicitly owned large DB on PASS or FAIL for audit;
            // no test-finally cleanup may erase a failed boundary checkpoint.
            await fixture.Evidence("F14-retained-fixture", new { target, fixture.Database, fixture.Directory, stageId, reopened, elapsedSeconds = timer.Elapsed.TotalSeconds });
        }

        async Task<WindowsFreshSyncEngine> Open() => await WindowsFreshSyncEngine.ResumeOrOpenAsync(fixture.Store, fixture.Source, Path.Combine(fixture.Directory, "spool"), configuration, fixture.Remote, budget, cancellationToken: ct);
        LocalSourceJournalSession Session() => (LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(engine)!;
        async Task Reopen()
        {
            await engine!.DisposeAsync();
            engine = null;
            fixture.Store = new(fixture.Database, new Stage3Clock());
            engine = await Open();
            reopened++;
        }
        SyncDescriptor Descriptor(int index)
        {
            if (index == 0) return new(configuration.Binding.RootId, null, "root", "folder", "0", null, null, "00000000-0000-0000-0000-000000000001", "AVAILABLE");
            var n = index.ToString("D10", CultureInfo.InvariantCulture);
            return new("zz-node-" + n, configuration.Binding.RootId, "file-" + n, "file", (index % 49).ToString(CultureInfo.InvariantCulture), Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes("expected-" + n))), "content-" + n, "00000000-0000-0000-0000-" + index.ToString("x12", CultureInfo.InvariantCulture), "AVAILABLE");
        }
        static string Canonical(SyncDescriptor d) => string.Join('|', d.NodeId, d.ParentId, d.Name, d.Type, d.SizeBytes, d.ContentSha256, d.ContentVersionId, d.Version, d.Availability) + "\n";
    }
}
