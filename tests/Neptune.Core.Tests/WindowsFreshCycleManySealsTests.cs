using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsFreshCycleTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ActualResumeValidatesEveryRetainedSealBeyondFirstPage(bool corruptLastPath)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.Create(new Stage3Clock());
        for (var i = 0; i < 160; i++) File.WriteAllText(Path.Combine(f.Source, $"file-{i:D4}.md"), $"original {i}");
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var published = 0;
        string scope;
        await using (var engine = await Resume(f, observer: (o, _) =>
        {
            if (o.Stage == WindowsCycleStage.FilePublished && Interlocked.Increment(ref published) == 130) stop.Cancel();
            return Task.CompletedTask;
        }))
        {
            scope = engine.ScopeId;
            Assert.NotNull((await engine.InitialSyncAsync(stop.Token)).Error);
        }
        var count = Convert.ToInt64(await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE active=1 AND spool_path IS NOT NULL"));
        Assert.True(count > 128, $"Expected three pages of real retained seals, got {count}");
        var originals = await f.Scalar("SELECT group_concat(input_json,'|') FROM (SELECT input_json FROM local_cycle_work ORDER BY work_no)");
        var last = Convert.ToInt64(await f.Scalar("SELECT MAX(work_no) FROM local_cycle_work WHERE active=1 AND spool_path IS NOT NULL"));
        var path = (string)(await f.Scalar($"SELECT spool_path FROM local_cycle_work WHERE work_no={last}"))!;
        if (corruptLastPath)
        {
            await ExecuteSql(f, $"UPDATE local_cycle_work SET spool_path=spool_path||'.foreign' WHERE work_no={last}");
            var error = await Assert.ThrowsAsync<LocalSourceGapException>(() => Resume(f));
            Assert.Contains("spool", error.Message);
            Assert.Equal(originals, await f.Scalar("SELECT group_concat(input_json,'|') FROM (SELECT input_json FROM local_cycle_work ORDER BY work_no)"));
            await ExecuteSql(f, $"UPDATE local_cycle_work SET spool_path='{path.Replace("'", "''")}' WHERE work_no={last}");
        }
        await using var resumed = await Resume(f);
        Assert.True(resumed.HasRetainedAnchor);
        Assert.Equal(scope, resumed.ScopeId);
        Assert.Equal(originals, await f.Scalar("SELECT group_concat(input_json,'|') FROM (SELECT input_json FROM local_cycle_work ORDER BY work_no)"));
        // Cancellation may leave the in-flight capture deferred. The next
        // scheduled pass explicitly retries it without altering its intent.
        await resumed.RetryDeferredAsync(Ct);
        var result = await f.FreshWhenDueAsync(resumed, Ct);
        await f.Evidence("many-seals-resume", new { count, corruptLastPath, scope, result });
        Assert.Null(result.Error);
        Assert.Equal("NoWorkAtAppliedEventBoundary", result.Decision);
        Assert.Equal(160, f.Remote.Prepares);
        Assert.Equal(160, f.Remote.Completes);
        Assert.Equal(160, Directory.GetFiles(f.Source).Length);
        Assert.Equal(1L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_scopes"));
        Assert.Empty(Directory.GetFiles(Path.Combine(f.Directory, "spool"), "*.spool"));
    }
}
