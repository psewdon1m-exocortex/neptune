namespace Neptune.Core;

// One native reader per volume/read mode within this process. The reader owns
// no subscription cursor: every profile commits its own page and owner epoch.
// Shared pages are immutable, bounded, and revalidated against fresh bounds.
internal sealed class WindowsVolumeJournalSubscription : ILocalJournalReader
{
    private sealed class Shared(WindowsUsnJournalReader reader,WindowsVolumeJournalCache cache)
    {
        internal readonly WindowsUsnJournalReader Reader = reader;
        internal readonly SemaphoreSlim Serial = new(1, 1);
        internal readonly WindowsVolumeJournalCache Cache=cache;
        internal int References;
    }
    private static readonly object Gate = new();
    private static readonly Dictionary<(string Volume, LocalJournalReadMode Mode), Shared> Readers = new();
    private readonly (string Volume, LocalJournalReadMode Mode) _key;
    private readonly Shared _shared;
    private int _disposed;
    private WindowsVolumeJournalSubscription((string, LocalJournalReadMode) key, Shared shared) => (_key, _shared) = (key, shared);
    internal static WindowsVolumeJournalSubscription Open(string volume, LocalJournalReadMode mode)
    {
        lock (Gate)
        {
            var key = (volume, mode);
            if (!Readers.TryGetValue(key, out var shared))
            {
                var reader=new WindowsUsnJournalReader(volume,mode);
                try{Readers.Add(key,shared=new(reader,new(volume,mode)));}
                catch{reader.DisposeAsync().AsTask().GetAwaiter().GetResult();throw;}
            }
            shared.References++;
            return new(key, shared);
        }
    }
    public LocalJournalVolumeIdentity Volume => _shared.Reader.Volume;
    public LocalJournalReadMode ReadMode => _key.Mode;
    public async ValueTask<LocalJournalBounds> QueryAsync(CancellationToken cancellationToken = default)
    {
        await _shared.Serial.WaitAsync(cancellationToken);
        try { ObjectDisposedException.ThrowIf(_disposed != 0, this); return await _shared.Reader.QueryAsync(cancellationToken); }
        finally { _shared.Serial.Release(); }
    }
    public async ValueTask<LocalJournalPage> ReadAsync(LocalJournalCursor cursor, int maximumBufferBytes = 256 * 1024, CancellationToken cancellationToken = default)
    {
        await _shared.Serial.WaitAsync(cancellationToken);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            return await Task.Run(()=>_shared.Cache.Read(_shared.Reader,cursor,maximumBufferBytes,cancellationToken),CancellationToken.None);
        }
        finally { _shared.Serial.Release(); }
    }
    public async ValueTask DisposeAsync()
    {
        await _shared.Serial.WaitAsync();
        try
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
            lock (Gate)
            {
                if (--_shared.References != 0) return;
                Readers.Remove(_key);
            }
            _shared.Cache.Dispose();await _shared.Reader.DisposeAsync();
        }
        finally { _shared.Serial.Release(); }
    }
}
