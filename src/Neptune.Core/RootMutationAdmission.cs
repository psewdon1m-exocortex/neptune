namespace Neptune.Core;

// The server permits two absent-only execution jobs only for the same parent.
// An exclusive request must not be bypassed by newer compatible jobs. This
// gate owns network admission, not source capture or terminal spool cleanup.
internal sealed class RootMutationAdmission(int maximumJobs, Action stopped)
{
    private sealed class Pending(string? cohort, CancellationToken token)
    {
        internal readonly string? Cohort = cohort;
        internal readonly CancellationToken Token = token;
        internal readonly TaskCompletionSource<Lease> Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal LinkedListNode<Pending>? Node;
    }
    private readonly object _sync = new();
    private readonly LinkedList<Pending> _queue = new();
    private string? _cohort;
    private int _active;
    private bool _stopped;

    internal async Task<Lease> AcquireAsync(string? cohort, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (maximumJobs is < 1 or > 2) throw new ArgumentOutOfRangeException(nameof(maximumJobs));
        var request = new Pending(cohort, ct);
        lock (_sync)
        {
            if (_stopped) throw new RootMutationDeferredException();
            if (_queue.Count >= 64) throw new CycleSpoolAdmissionException("Root admission queue is full; originals retained.");
            request.Node = _queue.AddLast(request);
            Drain();
        }
        using var registration = ct.Register(() => Cancel(request));
        var lease = await request.Completion.Task;
        if (ct.IsCancellationRequested) { lease.Dispose(); ct.ThrowIfCancellationRequested(); }
        return lease;
    }
    private void Cancel(Pending request)
    {
        lock (_sync)
        {
            if (request.Node is null) return;
            _queue.Remove(request.Node); request.Node = null;
            request.Completion.TrySetCanceled(request.Token);
            Drain();
        }
    }
    private void Drain()
    {
        while (!_stopped && _queue.First is { } node)
        {
            var request = node.Value;
            if (_active != 0 && (_active >= maximumJobs || _cohort is null || request.Cohort is null || _cohort != request.Cohort)) return;
            _queue.RemoveFirst(); request.Node = null;
            _cohort = request.Cohort; _active++;
            request.Completion.TrySetResult(new(this));
        }
    }
    private void Release(bool unresolved)
    {
        lock (_sync)
        {
            if (unresolved && !_stopped)
            {
                _stopped = true;
                // Pause the engine before waking any deferred work. No new
                // request may race the durable recovery of this original.
                stopped();
                foreach (var pending in _queue)
                {
                    pending.Node = null;
                    pending.Completion.TrySetException(new RootMutationDeferredException());
                }
                _queue.Clear();
            }
            if (--_active == 0) _cohort = null;
            Drain();
        }
    }
    internal sealed class Lease(RootMutationAdmission owner) : IDisposable
    {
        private int _released;
        private bool _attempted;
        internal void Attempt() => _attempted = true;
        // Call only AFTER validated terminal receipts are durable locally.
        // Local post-commit reconciliation/cleanup can then overlap the next job.
        internal void Complete() { if (Interlocked.Exchange(ref _released, 1) == 0) owner.Release(false); }
        public void Dispose() { if (Interlocked.Exchange(ref _released, 1) == 0) owner.Release(_attempted); }
    }
}

internal sealed class RootMutationDeferredException() : IOException("A root mutation has an unresolved original; recover it before new network work.");
