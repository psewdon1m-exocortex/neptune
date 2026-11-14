using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    [Fact]
    public async Task SmallBatchHistoricalArtifactRequiresOriginalTerminalOutcomeAndExplicitReplacementLineage()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; } await using var f = await WindowsFreshCycleTests.Fixture.Create();
        File.WriteAllText(Path.Combine(f.Source, "old-original"), "retained immutable batch bytes");
        await Medium(async () =>
        {
            string previous, operation, batch, originalInput; var batchPath = "";
            await using (var e = await Open(f))
            {
                previous = e.ScopeId; var s = Session(e); f.Remote.LosePrepare = true; _ = await e.InitialSyncAsync(Ct);
                operation = (string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE input_json IS NOT NULL"))!;
                originalInput = (string)(await f.Scalar("SELECT input_json FROM local_cycle_work WHERE operation_id='" + operation + "'"))!;
                await EnableBatch(s, previous); batch = Guid.NewGuid().ToString("D"); var row = await CreateBatch(s, previous, batch, operation);
                Func<string, CancellationToken, Task<LocalCycleWork>> read = (op, token) => SessionCall<LocalCycleWork>(s, "ReadCycleWorkAsync", previous, op, token);
                Func<string, CancellationToken, Task> bind = (identity, token) => SessionCall(s, "BindSmallBatchSpoolAsync", previous, batch, identity, token);
                var hash = await BatchSpoolCall<string?>("TryBuildAsync", BatchResources(e), row, read, bind, Ct);
                await SessionCall(s, "ChangeSmallBatchAsync", previous, batch, LocalSmallBatchPhase.Ready, hash, Ct);
                await BatchPhase(s, previous, batch, LocalSmallBatchPhase.Attempted); batchPath = Path.Combine(f.Directory, "spool", batch + ".spool");
                await s.MarkGapAsync("JournalGap", Ct);
            }
            await using var successor = await Replace(f, new(Guid.NewGuid().ToString("N"), previous));
            var pending = await successor.ReconcileSmallBatchArtifactsAsync(previous, Ct); Assert.Equal(0, pending.Removed); Assert.Equal(1, pending.Retained); Assert.True(File.Exists(batchPath));
            await Assert.ThrowsAsync<InvalidOperationException>(() => successor.ReconcileSmallBatchArtifactsAsync(Guid.NewGuid().ToString("N"), Ct));
            Assert.True(File.Exists(batchPath));
            var recovered = await successor.RecoverOriginalWorkAsync(previous, operation, Ct); Assert.Null(recovered.Error); Assert.Equal("published", recovered.Status);
            var cleaned = await successor.ReconcileSmallBatchArtifactsAsync(previous, Ct); Assert.Equal(1, cleaned.Removed); Assert.Equal(0, cleaned.Retained); Assert.False(File.Exists(batchPath));
            Assert.Equal(originalInput, await f.Scalar("SELECT input_json FROM local_cycle_work WHERE operation_id='" + operation + "'"));
            Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches")); Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE baseline_work<>0"));
            Assert.Equal(1, f.Remote.Prepares); await f.Evidence("batch-historical-original-cleanup", new { previous, successor = successor.ScopeId, recovered, cleaned, baseline = "NotAdopted" }); return true;
        });
    }
}
