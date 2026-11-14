using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    [Fact]
    public async Task BindingPreviewSelectsCurrentInstalledReplacementAndKeepsHistory()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create();
        File.WriteAllText(Path.Combine(f.Source, "retained.txt"), "retained original");
        await Medium(async () =>
        {
            string original;
            await using (var engine = await Open(f))
            {
                original = engine.ScopeId;
                Assert.Null((await engine.InitialSyncAsync(Ct)).Error);
                await Session(engine).MarkGapAsync("JournalGap", Ct);
            }
            var evidence = await Snapshot(f, original);
            string current;
            await using (var replacement = await Replace(f, new(Guid.NewGuid().ToString("N"), original)))
            {
                await replacement.QualifyJournalReplacementAsync(Ct);
                Assert.Equal(LocalJournalReplacementStatus.Installed, replacement.JournalReplacement!.Status);
                Assert.Null((await replacement.FreshSyncAsync(Ct)).Error);
                current = replacement.ScopeId;
            }
            var binding = f.Remote.Binding with { BindingVersion = Guid.NewGuid().ToString("D") };
            var artifact = JsonSerializer.Serialize(new
            {
                schema = "saturn.sync-adoption.v1", planId = Guid.NewGuid().ToString("D"),
                planSha256 = new string('a', 64), treeSha256 = new string('b', 64),
                binding = LocalCycleWire.BindingObject(binding),
                @namespace = new { codec = "legacy", policyHash = new string('c', 64) },
                device_id = f.Remote.Device, backend_identity = "local-test",
                qualification_receipt_sha256 = new string('d', 64)
            });
            var plan = await f.Store.PreviewServerBindingAsync(artifact, Ct);
            Assert.Equal(current, plan.OriginalScope);
            Assert.Empty(plan.Blockers);
            Assert.Equal(2L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_active"));
            Assert.Equal(evidence, await Snapshot(f, original));

            await using var db = new SqliteConnection("Data Source=" + f.Database + ";Pooling=False");
            await db.OpenAsync(Ct);
            await using var q = db.CreateCommand();
            q.CommandText = "UPDATE local_cycle_replacement_requests SET status=2";
            await q.ExecuteNonQueryAsync(Ct);
            await Assert.ThrowsAsync<InvalidOperationException>(() => f.Store.PreviewServerBindingAsync(artifact, Ct));
            q.CommandText = "UPDATE local_cycle_replacement_requests SET status=3";
            await q.ExecuteNonQueryAsync(Ct);
            Assert.Equal(current, (await f.Store.PreviewServerBindingAsync(artifact, Ct)).OriginalScope);
            Assert.Equal(evidence, await Snapshot(f, original));
            return true;
        });
    }
}
