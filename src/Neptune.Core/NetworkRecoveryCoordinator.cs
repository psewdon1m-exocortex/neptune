using System.Threading.Channels;

namespace Neptune.Core;

public sealed class NetworkUnavailableException(string message = "Waiting for network; queued operations are retained.") : IOException(message);

// One probe and one route generation per sync service. OS events are hints;
// only a successful authenticated probe opens admission again.
public sealed class NetworkRecoveryCoordinator : IAsyncDisposable
{
    private readonly object _gate = new();
    private readonly Func<CancellationToken, Task> _probe;
    private readonly Channel<bool> _signals = Channel.CreateBounded<bool>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropWrite });
    private readonly CancellationTokenSource _shutdown = new();
    private CancellationTokenSource _route = new();
    private CancellationTokenSource? _probeAttempt;
    private readonly Task _worker;
    private readonly TimeSpan _debounce, _retry;
    private long _generation;
    private bool _waiting, _disposed;
    private DateTimeOffset _notBefore;
    public event Action? WorkAvailable;
    public string State { get; private set; } = "Online";
    public NetworkRecoveryCoordinator(Func<CancellationToken, Task> probe, TimeSpan? debounce = null, TimeSpan? retry = null)
    { _probe = probe; _debounce = debounce ?? TimeSpan.FromMilliseconds(750); _retry = retry ?? TimeSpan.FromSeconds(30); _worker = RunAsync(); }
    public CancellationToken Admit()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_waiting) throw new NetworkUnavailableException(State);
            return _route.Token;
        }
    }
    public void Invalidate(string state = "Waiting for network", TimeSpan? retryAfter = null)
    {
        CancellationTokenSource route; CancellationTokenSource? probe;
        lock (_gate)
        {
            if (_disposed) return;
            _waiting = true; State = state; _generation++; route = _route; probe = _probeAttempt;
            ExtendNotBefore(retryAfter);
        }
        try { route.Cancel(); } catch (ObjectDisposedException) { /* Shutdown/recovery already drained this generation. */ }
        try { probe?.Cancel(); } catch (ObjectDisposedException) { }
        _signals.Writer.TryWrite(true);
    }
    public void Recovered() { lock (_gate) if (!_waiting) State = "Online"; }
    private void ExtendNotBefore(TimeSpan? delay)
    {
        if (delay is not { } value || value <= TimeSpan.Zero) return;
        var now = DateTimeOffset.UtcNow;
        var until = value >= DateTimeOffset.MaxValue - now ? DateTimeOffset.MaxValue : now + value;
        if (until > _notBefore) _notBefore = until;
    }
    private async Task RunAsync()
    {
        var ct = _shutdown.Token;
        try
        {
            while (await _signals.Reader.WaitToReadAsync(ct))
            {
                await Task.Delay(_debounce, ct);
                while (_signals.Reader.TryRead(out _)) { }
                while (true)
                {
                    ct.ThrowIfCancellationRequested();
                    long generation; DateTimeOffset notBefore;
                    lock (_gate) { if (!_waiting) break; generation = _generation; notBefore = _notBefore; }
                    var remaining = notBefore - DateTimeOffset.UtcNow;
                    if (remaining > TimeSpan.Zero)
                    { await Task.Delay(remaining > TimeSpan.FromDays(1) ? TimeSpan.FromDays(1) : remaining, ct); continue; }
                    using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    lock (_gate) { if (generation != _generation || _disposed) continue; _probeAttempt = deadline; }
                    try
                    {
                        deadline.CancelAfter(SyncHttpPolicy.ControlTimeout);
                        await _probe(deadline.Token);
                        lock (_gate)
                        {
                            if (generation != _generation || _disposed) continue;
                            _route.Dispose(); _route = new(); _waiting = false; State = "Recovering operations";
                            WorkAvailable?.Invoke();
                        }
                        break;
                    }
                    catch (Exception error) when (!ct.IsCancellationRequested)
                    {
                        lock (_gate)
                        {
                            if (generation != _generation || _disposed) continue;
                            State = NetworkFailure.State(error) ?? "Waiting for network";
                            ExtendNotBefore(_retry);
                            ExtendNotBefore((error as GuardedCycleRemoteException)?.RetryAfter);
                        }
                    }
                    finally { lock (_gate) if (ReferenceEquals(_probeAttempt, deadline)) _probeAttempt = null; }
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }
    public async ValueTask DisposeAsync()
    {
        lock (_gate) { if (_disposed) return; _disposed = true; }
        _shutdown.Cancel(); _route.Cancel(); await _worker;
        _route.Dispose(); _shutdown.Dispose();
    }
}
