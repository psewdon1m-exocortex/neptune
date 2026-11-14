using System.Diagnostics;

namespace Neptune.Core;

public enum SyncPhase { Idle, JournalIngest, Replay, Frontier, NativeScan, HardlinkVerification, Import, Retirement, RemoteFeed, Capture, Transfer, Recovery }
public sealed record SyncPhaseMeasurement(SyncPhase Phase, long Calls, long ElapsedMilliseconds, long MaximumMilliseconds, long SqlCommands, long Items)
{public long Active { get; init; }public long RunningMilliseconds { get; init; }
 public long LocalSqlStatements { get; init; }public long LocalSqlMilliseconds { get; init; }}
public sealed record SyncDiagnosticSnapshot(DateTimeOffset At, IReadOnlyList<SyncPhaseMeasurement> Phases, long InboxRecords, long JournalLag, long Retries, string? WaitReason)
{
    public IReadOnlyList<LocalSqlHelperMeasurement> LocalHelpers { get; init; } = [];
    public long StreamSourceBytes{get;init;}public long StreamRecoveryReadBytes{get;init;}
    public long CapturedBytes{get;init;}public long ReceivedBytes{get;init;}public long PublishedFiles{get;init;}
    // Neither a network ACK nor a catalog receipt exposes the server's
    // intermediate write/verification counters. Unknown stays explicit.
    public long? BackendWrittenBytes{get;init;}public long? BackendVerifiedBytes{get;init;}
    public SyncOperationSchedule? Schedule{get;init;}public SyncSchedulerSnapshot? Scheduler{get;init;}
}
public sealed record LocalSqlHelperMeasurement(string Operation, long Calls, long ElapsedMilliseconds, long Statements, long SqlMilliseconds);

// Fixed enum labels only. No paths, credentials, SQL text or file contents.
public sealed class SyncDiagnostics
{
    private sealed class Counter { internal long Calls, Ticks, MaximumTicks, Sql, Items,Active,Started,LocalStatements,LocalSqlNanoseconds; }
    private sealed record Context(SyncDiagnostics Owner, SyncPhase Phase);
    private static readonly AsyncLocal<Context?> Current = new();
    private static readonly AsyncLocal<Counter?> CurrentHelper = new();
    private readonly Dictionary<string, Counter> _localHelpers = new(StringComparer.Ordinal);
    private readonly Counter[] _counters = Enum.GetValues<SyncPhase>().Select(_ => new Counter()).ToArray();
    private long _inbox, _lag, _retries;
    private long _streamBytes,_streamRecoveryBytes;
    private string? _wait;

    internal IDisposable Measure(SyncPhase phase) => new Measurement(this, phase);
    internal static void SqlCommandCreated() { if (Current.Value is { } c) Interlocked.Increment(ref c.Owner._counters[(int)c.Phase].Sql); }
    // The binding exposes SQLite PROFILE (millisecond timer resolution). Count
    // completed native statements, including commands reused or containing more
    // than one statement. Never retain SQL text, parameters, or connection state.
    internal static void AttachLocalSqliteDiagnostics(Microsoft.Data.Sqlite.SqliteConnection connection)
        => SQLitePCL.raw.sqlite3_profile(connection.Handle, (SQLitePCL.delegate_profile)((_, _, nanoseconds) =>
        {
            if (Current.Value is not { } context) return;
            var counter = context.Owner._counters[(int)context.Phase];
            Interlocked.Increment(ref counter.LocalStatements);
            Interlocked.Add(ref counter.LocalSqlNanoseconds, Math.Max(0, nanoseconds));
            if (CurrentHelper.Value is { } helper)
            {
                Interlocked.Increment(ref helper.LocalStatements);
                Interlocked.Add(ref helper.LocalSqlNanoseconds, Math.Max(0, nanoseconds));
            }
        }), null);
    internal static IDisposable? MeasureLocalHelper(string operation)
    {
        if (Current.Value is not { } context) return null;
        Counter counter;
        lock (context.Owner._localHelpers)
        {
            if (!context.Owner._localHelpers.TryGetValue(operation, out counter!))
            {
                if (context.Owner._localHelpers.Count >= 127) operation = "Other";
                if (!context.Owner._localHelpers.TryGetValue(operation, out counter!)) context.Owner._localHelpers.Add(operation, counter = new());
            }
        }
        return new HelperMeasurement(counter);
    }
    private IReadOnlyList<LocalSqlHelperMeasurement> LocalHelpers()
    {
        lock (_localHelpers) return _localHelpers.Select(pair => new LocalSqlHelperMeasurement(pair.Key,
            Interlocked.Read(ref pair.Value.Calls), Milliseconds(Interlocked.Read(ref pair.Value.Ticks)),
            Interlocked.Read(ref pair.Value.LocalStatements), Interlocked.Read(ref pair.Value.LocalSqlNanoseconds) / 1_000_000)).ToArray();
    }
    internal void Items(SyncPhase phase, long count) => Interlocked.Add(ref _counters[(int)phase].Items, count);
    internal void StreamRead(int bytes,bool recovery){Interlocked.Add(ref _streamBytes,bytes);if(recovery)Interlocked.Add(ref _streamRecoveryBytes,bytes);}
    internal void Backlog(long inbox, long lag) { Interlocked.Exchange(ref _inbox, inbox); Interlocked.Exchange(ref _lag, Math.Max(0, lag)); }
    internal void Retry(string reason) { Interlocked.Increment(ref _retries); Volatile.Write(ref _wait, reason); }
    internal void ClearWait() => Volatile.Write(ref _wait, null);
    public SyncDiagnosticSnapshot Snapshot() => new(DateTimeOffset.UtcNow,
        Enum.GetValues<SyncPhase>().Select(phase => { var c = _counters[(int)phase]; return new SyncPhaseMeasurement(phase,
            Interlocked.Read(ref c.Calls), Milliseconds(Interlocked.Read(ref c.Ticks)), Milliseconds(Interlocked.Read(ref c.MaximumTicks)),
            Interlocked.Read(ref c.Sql), Interlocked.Read(ref c.Items)){Active=Interlocked.Read(ref c.Active),RunningMilliseconds=Interlocked.Read(ref c.Started) is var started&&started>0?Milliseconds(Stopwatch.GetTimestamp()-started):0,
                LocalSqlStatements=Interlocked.Read(ref c.LocalStatements),LocalSqlMilliseconds=Interlocked.Read(ref c.LocalSqlNanoseconds)/1_000_000}; }).ToArray(),
        Interlocked.Read(ref _inbox), Interlocked.Read(ref _lag), Interlocked.Read(ref _retries), Volatile.Read(ref _wait)){StreamSourceBytes=Interlocked.Read(ref _streamBytes),StreamRecoveryReadBytes=Interlocked.Read(ref _streamRecoveryBytes),LocalHelpers=LocalHelpers()};
    private static long Milliseconds(long ticks) => (long)(ticks * 1000d / Stopwatch.Frequency);
    private sealed class HelperMeasurement : IDisposable
    {
        private readonly Counter? _previous = CurrentHelper.Value;
        private readonly Counter _counter;
        private readonly long _started = Stopwatch.GetTimestamp();
        internal HelperMeasurement(Counter counter) { _counter = counter; CurrentHelper.Value = counter; }
        public void Dispose()
        {
            Interlocked.Increment(ref _counter.Calls); Interlocked.Add(ref _counter.Ticks, Stopwatch.GetTimestamp() - _started);
            CurrentHelper.Value = _previous;
        }
    }
    private sealed class Measurement : IDisposable
    {
        private readonly Context? _previous;
        private readonly Counter _counter;
        private readonly long _started = Stopwatch.GetTimestamp();
        private bool _disposed;
        internal Measurement(SyncDiagnostics owner, SyncPhase phase)
        { _previous = Current.Value; Current.Value = new(owner, phase); _counter = owner._counters[(int)phase];if(Interlocked.Increment(ref _counter.Active)==1)Interlocked.Exchange(ref _counter.Started,_started); }
        public void Dispose()
        {
            if (_disposed) return; _disposed = true;
            var elapsed = Stopwatch.GetTimestamp() - _started;
            if(Interlocked.Decrement(ref _counter.Active)==0)Interlocked.Exchange(ref _counter.Started,0);
            Interlocked.Increment(ref _counter.Calls); Interlocked.Add(ref _counter.Ticks, elapsed);
            long previous;
            do { previous = Interlocked.Read(ref _counter.MaximumTicks); if (previous >= elapsed) break; }
            while (Interlocked.CompareExchange(ref _counter.MaximumTicks, elapsed, previous) != previous);
            Current.Value = _previous;
        }
    }
}
