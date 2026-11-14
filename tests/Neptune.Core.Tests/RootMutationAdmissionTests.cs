using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class RootMutationAdmissionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    [Fact]
    public async Task SameParentJobsOverlapButThirdAndOtherParentsWait()
    {
        var gate = new RootMutationAdmission(2, () => Assert.Fail("No unresolved original"));
        using var first = await gate.AcquireAsync("a", Ct);
        using var second = await gate.AcquireAsync("a", Ct);
        var third = gate.AcquireAsync("a", Ct); var other = gate.AcquireAsync("b", Ct);
        Assert.False(third.IsCompleted); Assert.False(other.IsCompleted);
        first.Complete(); using var admitted = await third.WaitAsync(TimeSpan.FromSeconds(2), Ct);
        second.Complete(); Assert.False(other.IsCompleted);
        admitted.Complete(); using var last = await other.WaitAsync(TimeSpan.FromSeconds(2), Ct); last.Complete();
    }
    [Fact]
    public async Task ExclusiveRequestCannotBeStarvedByLaterSameParentJob()
    {
        var gate = new RootMutationAdmission(2, () => Assert.Fail("No unresolved original"));
        using var first = await gate.AcquireAsync("a", Ct);
        var exclusive = gate.AcquireAsync(null, Ct); var later = gate.AcquireAsync("a", Ct);
        Assert.False(exclusive.IsCompleted); Assert.False(later.IsCompleted);
        first.Complete(); using var cas = await exclusive.WaitAsync(TimeSpan.FromSeconds(2), Ct);
        Assert.False(later.IsCompleted); cas.Complete();
        using var final = await later.WaitAsync(TimeSpan.FromSeconds(2), Ct); final.Complete();
    }
    [Fact]
    public async Task CancelledHeadDoesNotBlockCompatibleWorkOrConsumeAPermit()
    {
        var gate = new RootMutationAdmission(2, () => Assert.Fail("No unresolved original"));
        using var first = await gate.AcquireAsync("a", Ct);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var cancelled = gate.AcquireAsync(null, stop.Token); var later = gate.AcquireAsync("a", Ct);
        await stop.CancelAsync(); await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelled);
        using var second = await later.WaitAsync(TimeSpan.FromSeconds(2), Ct);
        first.Complete(); second.Complete();
    }
    [Fact]
    public async Task LostOutcomeStopsQueuedAndFutureAdmissionBeforeWakingWaiters()
    {
        var stopped = false;
        var gate = new RootMutationAdmission(2, () => stopped = true);
        using var first = await gate.AcquireAsync("a", Ct);
        using var second = await gate.AcquireAsync("a", Ct);
        var waiting = gate.AcquireAsync(null, Ct);
        first.Attempt(); first.Dispose();
        await Assert.ThrowsAsync<RootMutationDeferredException>(() => waiting);
        Assert.True(stopped);
        second.Complete();
        await Assert.ThrowsAsync<RootMutationDeferredException>(() => gate.AcquireAsync("a", Ct));
    }
    [Fact]
    public async Task PreRequestFailureReleasesButTerminalReceiptDoesNotWaitForCleanup()
    {
        var gate = new RootMutationAdmission(1, () => Assert.Fail("Terminal or never attempted"));
        var first = await gate.AcquireAsync(null, Ct); var next = gate.AcquireAsync("a", Ct);
        first.Dispose(); using var second = await next.WaitAsync(TimeSpan.FromSeconds(2), Ct);
        second.Attempt(); var last = gate.AcquireAsync("a", Ct); Assert.False(last.IsCompleted);
        second.Complete(); using var third = await last.WaitAsync(TimeSpan.FromSeconds(2), Ct);
        third.Complete();
    }
    [Fact]
    public async Task QueueBoundAndCancellationPreserveExistingWaiters()
    {
        var gate = new RootMutationAdmission(1, () => Assert.Fail("No attempted request"));
        using var first = await gate.AcquireAsync(null, Ct);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var queued = Enumerable.Range(0, 64).Select(_ => gate.AcquireAsync("a", stop.Token)).ToArray();
        await Assert.ThrowsAsync<CycleSpoolAdmissionException>(() => gate.AcquireAsync("a", Ct));
        await stop.CancelAsync();
        foreach (var pending in queued) await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        first.Dispose(); using var next = await gate.AcquireAsync("a", Ct); next.Complete();
    }

    [Fact]
    public async Task ThirdDispatcherOverlapsTerminalCleanupWithoutAThirdNetworkJob()
    {
        var gate = new RootMutationAdmission(2, () => Assert.Fail("Terminal receipts are saved"));
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var third = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstTwo = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int dispatches = 0, active = 0, peak = 0;
        async Task<IReadOnlyDictionary<string, bool>> Send(IReadOnlyList<LocalCycleWork> originals, CancellationToken ct)
        {
            var number = Interlocked.Increment(ref dispatches);
            using var lease = await gate.AcquireAsync("same-parent", ct);
            var count = Interlocked.Increment(ref active); Interlocked.Exchange(ref peak, Math.Max(peak, count));
            lease.Attempt();
            if (number <= 2)
            {
                if (count == 2) firstTwo.TrySetResult();
                await firstTwo.Task.WaitAsync(TimeSpan.FromSeconds(3), ct);
            }
            Interlocked.Decrement(ref active); lease.Complete();
            if (number == 3) third.TrySetResult();
            await release.Task; // terminal local cleanup is deliberately held
            return originals.ToDictionary(w => w.OperationId, _ => true);
        }
        static LocalCycleWork Work() => new(Guid.NewGuid().ToString("D"), "scope", new("node", "parent", "file", "file", 1, false, 1,
            new(2, 3), null, null), LocalCycleWorkPhase.Sealed, "owned.spool", 1, new string('a', 64), "native proof", "full original", new string('b', 64), null, 0, null, null);
        await using var scheduler = new SmallFileBatchScheduler(true, 16, 2, Send, Ct, true);
        var tasks = Enumerable.Range(0, 48).Select(_ => scheduler.SubmitAsync(Work())).ToArray();
        try
        {
            await third.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
            Assert.Equal(3, dispatches); Assert.Equal(2, peak);
            Assert.All(tasks, task => Assert.False(task.IsCompleted));
        }
        finally { release.TrySetResult(); }
        Assert.All(await Task.WhenAll(tasks), result => Assert.True(result));
    }
}
