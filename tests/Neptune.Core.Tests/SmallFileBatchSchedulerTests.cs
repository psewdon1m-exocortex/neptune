using System.Diagnostics;
using System.Reflection;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class SmallFileBatchSchedulerTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static LocalCycleWork Work() => new(Guid.NewGuid().ToString("D"), "scope", new("node", "parent", "file", "file", 1, false, 1,
        new(2, 3), null, null), LocalCycleWorkPhase.Sealed, "owned.spool", 1, new string('a', 64), "native proof", "full original", new string('b', 64), null, 0, null, null);
    private static readonly Type SchedulerType = typeof(NeptuneStateStore).Assembly.GetType("Neptune.Core.SmallFileBatchScheduler")!;
    private static IAsyncDisposable Create(bool bulk, Func<IReadOnlyList<LocalCycleWork>, CancellationToken, Task<IReadOnlyDictionary<string, bool>>> send, CancellationToken stop)
        => (IAsyncDisposable)Activator.CreateInstance(SchedulerType, BindingFlags.Instance | BindingFlags.NonPublic, null, [bulk, send, stop], null)!;
    private static Task<bool> Submit(IAsyncDisposable scheduler, LocalCycleWork work) => (Task<bool>)SchedulerType.GetMethod("SubmitAsync", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(scheduler, [work])!;

    [Fact]
    public async Task SmallBatchPipelineCancellationJoinsBothTransportsAndCancelsQueuedOriginals()
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var both = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int active = 0, peak = 0, started = 0;
        Func<IReadOnlyList<LocalCycleWork>, CancellationToken, Task<IReadOnlyDictionary<string, bool>>> send = async (work, ct) =>
        {
            Assert.Equal(16, work.Count);
            var count = Interlocked.Increment(ref active); Interlocked.Exchange(ref peak, Math.Max(peak, count));
            if (Interlocked.Increment(ref started) == 2) both.TrySetResult();
            try { await release.Task; ct.ThrowIfCancellationRequested(); return work.ToDictionary(w => w.OperationId, _ => true); }
            finally { Interlocked.Decrement(ref active); }
        };
        await using var scheduler = (IAsyncDisposable)Activator.CreateInstance(SchedulerType, BindingFlags.Instance | BindingFlags.NonPublic, null, [true, 16, 2, send, stop.Token], null)!;
        var tasks = Enumerable.Range(0, 48).Select(_ => Submit(scheduler, Work())).ToArray();
        try
        {
            await both.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
            Assert.Equal(2, peak); Assert.Equal(2, started);
            await stop.CancelAsync();
            Assert.All(tasks.Take(32), task => Assert.False(task.IsCompleted));
        }
        finally { release.TrySetResult(); }
        foreach (var task in tasks) await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await task);
        Assert.Equal(0, active); Assert.Equal(2, started);
    }

    [Fact]
    public async Task SmallBatchPipelineRejectsCorruptMembershipWithoutAcceptingOtherOriginals()
    {
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var number = 0;
        Func<IReadOnlyList<LocalCycleWork>, CancellationToken, Task<IReadOnlyDictionary<string, bool>>> send = async (work, ct) =>
        {
            if (Interlocked.Increment(ref number) == 1)
            {
                await second.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
                return new Dictionary<string, bool> { ["unrelated"] = true };
            }
            second.TrySetResult(); return work.ToDictionary(w => w.OperationId, _ => true);
        };
        await using var scheduler = (IAsyncDisposable)Activator.CreateInstance(SchedulerType, BindingFlags.Instance | BindingFlags.NonPublic, null, [true, 16, 2, send, Ct], null)!;
        var tasks = Enumerable.Range(0, 32).Select(_ => Submit(scheduler, Work())).ToArray();
        foreach (var task in tasks.Take(16)) await Assert.ThrowsAsync<InvalidDataException>(async () => await task);
        Assert.All(await Task.WhenAll(tasks.Skip(16)), value => Assert.True(value));
    }

    [Theory]
    [InlineData(false)][InlineData(true)]
    public async Task SmallBatchSchedulerFlushesFourOriginalsWithoutWaitingForAge(bool bulk)
    {
        var groups = new List<string[]>();
        await using var scheduler = Create(bulk, (work, _) => { groups.Add(work.Select(w => w.OperationId).ToArray()); return Task.FromResult<IReadOnlyDictionary<string, bool>>(work.ToDictionary(w => w.OperationId, _ => true)); }, Ct);
        var originals = Enumerable.Range(0, 4).Select(_ => Work()).ToArray(); var tasks = originals.Select(w => Submit(scheduler, w)).ToArray();
        var done = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(3), Ct);
        Assert.All(done, value => Assert.True(value)); Assert.Single(groups); Assert.Equal(originals.Select(w => w.OperationId), groups[0]);
    }

    [Theory]
    [InlineData(false, 250)][InlineData(true, 1000)]
    public async Task SmallBatchSchedulerFlushesAgedGroupAndReturnsSingletonToIndividual(bool bulk, int age)
    {
        var groups = 0; await using var scheduler = Create(bulk, (work, _) => { groups++; return Task.FromResult<IReadOnlyDictionary<string, bool>>(work.ToDictionary(w => w.OperationId, _ => true)); }, Ct);
        var watch = Stopwatch.StartNew(); var first = Submit(scheduler, Work()); var second = Submit(scheduler, Work());
        Assert.All(await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(5), Ct), value => Assert.True(value));
        Assert.InRange(watch.ElapsedMilliseconds, age - 20, age + 3000); Assert.Equal(1, groups);
        Assert.False(await Submit(scheduler, Work()).WaitAsync(TimeSpan.FromSeconds(5), Ct)); Assert.Equal(1, groups);
    }

    [Fact]
    public async Task SmallBatchSchedulerCancellationWaitsForStartedTransportToJoin()
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct); var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var scheduler = Create(false, async (_, token) => { started.TrySetResult(); await release.Task; token.ThrowIfCancellationRequested(); throw new InvalidOperationException(); }, stop.Token);
        var waiting = Enumerable.Range(0, 4).Select(_ => Submit(scheduler, Work())).ToArray(); await started.Task.WaitAsync(TimeSpan.FromSeconds(3), Ct);
        await stop.CancelAsync(); Assert.All(waiting, t => Assert.False(t.IsCompleted)); release.SetResult();
        foreach (var task in waiting) await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await task);
    }

    [Fact]
    public async Task SmallBatchSchedulerRejectsUnrelatedOutcomeForEveryWaitingOriginal()
    {
        await using var scheduler = Create(false, (_, _) => Task.FromResult<IReadOnlyDictionary<string, bool>>(new Dictionary<string, bool> { ["other"] = true }), Ct);
        var waiting = Enumerable.Range(0, 4).Select(_ => Submit(scheduler, Work())).ToArray();
        foreach (var task in waiting) await Assert.ThrowsAsync<InvalidDataException>(async () => await task);
    }
}
