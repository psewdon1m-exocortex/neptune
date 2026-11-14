using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private int _ownedLocalPool;
    private string LocalSourceConnectionString(bool pooling)=>new SqliteConnectionStringBuilder
    {DataSource=NativeDatabasePath(databasePath),Mode=SqliteOpenMode.ReadWrite,Cache=SqliteCacheMode.Private,Pooling=pooling,ForeignKeys=true,DefaultTimeout=10}.ToString();
    internal void CloseOwnedLocalPool()
    {
        if(Interlocked.Exchange(ref _ownedLocalPool,0)==0)return;
        // Only this profile's connections are retired, before its OS owner
        // lock is released. An idle pool never extends an ownership epoch.
        using var key=new SqliteConnection(LocalSourceConnectionString(true));SqliteConnection.ClearPool(key);
    }
    private async Task<LocalSourceJournalSession> CreateOwnedJournalSessionAsync(
        FileStream ownerLock, string root, string owner, LocalJournalAdmission admission, CancellationToken ct)
    {
        // Closing SQLite's final connection checkpoints/removes WAL and SHM.
        // Repeating that for every bounded operation creates its own NTFS
        // journal traffic. A connection owned by this session prevents that
        // feedback loop independently of global pool eviction. It never holds
        // a transaction, so passive checkpoints and schema preflights stay live.
        var lifetime = new SqliteConnection(ReadOnlySyncConnectionString(databasePath));
        try
        {
            await lifetime.OpenAsync(ct);
            await using var query = lifetime.CreateCommand();
            query.CommandText = "SELECT version FROM sync_state_format WHERE singleton=1";
            if (await query.ExecuteScalarAsync(ct) is not long version || version != SyncStateFormatVersion)
                throw new InvalidDataException("Main state format changed while opening the owned session.");
            // Keep each transaction short, but retain SQLite's page/schema
            // caches between bounded operations for this exclusive session.
            if(Interlocked.CompareExchange(ref _ownedLocalPool,1,0)!=0)throw new InvalidOperationException("A local connection pool already has an owner.");
            return new(this, ownerLock, lifetime, root, owner, admission);
        }
        catch { await lifetime.DisposeAsync(); throw; }
    }
}
