using System.Diagnostics;
using System.Threading.Channels;

namespace Neptune.Core;

// Only sealed immutable originals enter this bounded pipeline. Capture leases
// are released before waiting here; transport leases remain separately bounded.
// Cancellation joins every started group before its original spools can retire.
internal sealed class SmallFileBatchScheduler : IAsyncDisposable
{
    private sealed record Pending(LocalCycleWork Work, TaskCompletionSource<bool> Completion);
    internal const int BulkItems = 16, BulkWorkers = 32;
    // Two admitted network jobs consume 32 callers. A third dispatcher can
    // prepare or clean up while the root gate limits network jobs to two.
    // 48 originals plus at most three aggregate spools remain below 64.
    internal const int PipelinedWorkers = BulkWorkers + BulkItems;
    private readonly Channel<Pending> _queue;
    private readonly Func<IReadOnlyList<LocalCycleWork>, CancellationToken, Task<IReadOnlyDictionary<string, bool>>> _send;
    private readonly TimeSpan _age; private readonly CancellationToken _stop; private readonly Task _runner;
    private readonly int _maximumItems, _parallel;

    internal SmallFileBatchScheduler(bool bulk,
        Func<IReadOnlyList<LocalCycleWork>, CancellationToken, Task<IReadOnlyDictionary<string, bool>>> send, CancellationToken stop)
        : this(bulk, 4, 1, send, stop) { }

    internal SmallFileBatchScheduler(bool bulk, int maximumItems, int parallel,
        Func<IReadOnlyList<LocalCycleWork>, CancellationToken, Task<IReadOnlyDictionary<string, bool>>> send, CancellationToken stop)
        : this(bulk, maximumItems, parallel, send, stop, false) { }

    internal SmallFileBatchScheduler(bool bulk, int maximumItems, int parallel,
        Func<IReadOnlyList<LocalCycleWork>, CancellationToken, Task<IReadOnlyDictionary<string, bool>>> send, CancellationToken stop, bool overlapCleanup)
    {
        if (maximumItems is < 2 or > BulkItems || parallel is < 1 or > 2) throw new ArgumentOutOfRangeException(nameof(maximumItems));
        (_age, _send, _stop, _maximumItems, _parallel) = (TimeSpan.FromMilliseconds(bulk ? 1000 : 250), send, stop, maximumItems, parallel + (overlapCleanup ? 1 : 0));
        _queue = Channel.CreateBounded<Pending>(new BoundedChannelOptions(maximumItems * parallel)
        { SingleReader = true, SingleWriter = false, FullMode = BoundedChannelFullMode.Wait, AllowSynchronousContinuations = false });
        _runner = Run();
    }

    internal async Task<bool> SubmitAsync(LocalCycleWork original)
    {
        if (original.Entry.Directory || original.Size is null or < 0 or > SmallFileBatchPlan.MaximumFileBytes || original.InputJson is null || original.UploadId is not null)
            throw new ArgumentException("Only small sealed original file inputs enter the age queue.");
        var pending = new Pending(original, new(TaskCreationOptions.RunContinuationsAsynchronously));
        await _queue.Writer.WriteAsync(pending, _stop);
        // Do not stop awaiting a started transport just because cancellation
        // arrived: its request/stream must finish before the engine disposes.
        return await pending.Completion.Task;
    }

    private async Task Run()
    {
        Exception? stopped = null;
        var running = new List<Task>(_parallel);
        try
        {
            while (await _queue.Reader.WaitToReadAsync(_stop))
            {
                if (running.Count >= _parallel) await Task.WhenAny(running);
                running.RemoveAll(task => task.IsCompleted);
                _stop.ThrowIfCancellationRequested();
                if (!_queue.Reader.TryRead(out var first)) continue;
                var group = new List<Pending>(_maximumItems) { first }; var timer = Stopwatch.StartNew();
                try
                {
                    while (group.Count < _maximumItems)
                    {
                        _stop.ThrowIfCancellationRequested();
                        if (_queue.Reader.TryRead(out var next)) { group.Add(next); continue; }
                        var remaining = _age - timer.Elapsed; if (remaining <= TimeSpan.Zero) break;
                        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(_stop); deadline.CancelAfter(remaining);
                        try { if (!await _queue.Reader.WaitToReadAsync(deadline.Token)) break; }
                        catch (OperationCanceledException) when (!_stop.IsCancellationRequested) { break; }
                    }
                    _stop.ThrowIfCancellationRequested();
                    if (group.Select(p => p.Work.OperationId).Distinct(StringComparer.Ordinal).Count() != group.Count) throw new InvalidDataException("Duplicate originals cannot enter one batch.");
                    if (group.Count == 1) { first.Completion.TrySetResult(false); continue; }
                    running.Add(Send(group));
                }
                catch (Exception error)
                {
                    foreach (var pending in group) Fail(pending, error);
                    if (_stop.IsCancellationRequested) throw;
                }
            }
        }
        catch (Exception error) { stopped = error; }
        finally
        {
            _queue.Writer.TryComplete(stopped);
            while (_queue.Reader.TryRead(out var pending)) Fail(pending, stopped ?? new InvalidOperationException("Batch scheduler is closed."));
            await Task.WhenAll(running);
        }
    }
    private async Task Send(List<Pending> group)
    {
        try
        {
            var outcome = await _send(group.Select(p => p.Work).ToArray(), _stop);
            if (outcome.Count != group.Count || group.Any(p => !outcome.ContainsKey(p.Work.OperationId))) throw new InvalidDataException("Batch dispatcher returned unrelated original outcomes.");
            foreach (var pending in group) pending.Completion.TrySetResult(outcome[pending.Work.OperationId]);
        }
        catch (Exception error) { foreach (var pending in group) Fail(pending, error); }
    }
    private void Fail(Pending pending, Exception error)
    {
        if (error is OperationCanceledException && _stop.IsCancellationRequested) pending.Completion.TrySetCanceled(_stop);
        else pending.Completion.TrySetException(error);
    }
    public async ValueTask DisposeAsync() { _queue.Writer.TryComplete(); await _runner; }
}
