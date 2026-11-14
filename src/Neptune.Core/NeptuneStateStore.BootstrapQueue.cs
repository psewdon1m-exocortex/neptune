using System.Diagnostics;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private BootstrapCheckpointResult? _bootstrapLastCheckpoint;
    private long _bootstrapCheckpointAttempts, _bootstrapAdmissionStops;
    public const long BootstrapWalAdmissionBytes = 64L * 1024 * 1024;
    public BootstrapCheckpointMetrics GetBootstrapCheckpointMetrics() => new(Interlocked.Read(ref _bootstrapCheckpointAttempts),
        Interlocked.Read(ref _bootstrapAdmissionStops), Volatile.Read(ref _bootstrapLastCheckpoint));

    public async Task<BootstrapFrontierBatch?> RefillBootstrapFrontierAsync(string scanId, string ownerToken, CancellationToken cancellationToken = default)
    {
        await CheckBootstrapAdmissionAsync(cancellationToken);
        await using var connection = await OpenBootstrapConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var effects = await BootstrapTotalChangesAsync(connection, transaction, cancellationToken);
        await RequireBootstrapOwnerAsync(connection, transaction, scanId, ownerToken, cancellationToken);
        long parentId, cursor;
        await using (var parent = SyncCommand(connection, transaction, "SELECT directory_id,child_cursor FROM bootstrap_scan_directories WHERE scan_id=$scan AND state=2 AND refill_complete=0 ORDER BY directory_id LIMIT 1"))
        {
            parent.Parameters.AddWithValue("$scan", scanId);
            await using var reader = await parent.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken)) return null;
            parentId = reader.GetInt64(0); cursor = reader.GetInt64(1);
        }
        // The kind-specific keyset index skips files without scanning a wide
        // parent's siblings, and one transaction admits at most 256 children.
        var children = new List<(long Id, string Path)>(BootstrapMetadataBatchLimit);
        await using (var entries = SyncCommand(connection, transaction, "SELECT entry_id,relative_path FROM bootstrap_scan_entries WHERE scan_id=$scan AND parent_directory_id=$parent AND kind=1 AND entry_id>$cursor ORDER BY entry_id LIMIT $limit"))
        {
            entries.Parameters.AddWithValue("$scan", scanId); entries.Parameters.AddWithValue("$parent", parentId);
            entries.Parameters.AddWithValue("$cursor", cursor); entries.Parameters.AddWithValue("$limit", BootstrapMetadataBatchLimit);
            await using var reader = await entries.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken)) children.Add((reader.GetInt64(0), reader.GetString(1)));
        }
        foreach (var child in children)
        {
            await using var insert = SyncCommand(connection, transaction, "INSERT INTO bootstrap_scan_directories(scan_id,relative_path,state) VALUES($scan,$path,0)");
            insert.Parameters.AddWithValue("$scan", scanId); insert.Parameters.AddWithValue("$path", child.Path);
            await insert.ExecuteNonQueryAsync(cancellationToken);
            cursor = child.Id;
        }
        await using var remaining = SyncCommand(connection, transaction, "SELECT 1 FROM bootstrap_scan_entries WHERE scan_id=$scan AND parent_directory_id=$parent AND kind=1 AND entry_id>$cursor ORDER BY entry_id LIMIT 1");
        remaining.Parameters.AddWithValue("$scan", scanId); remaining.Parameters.AddWithValue("$parent", parentId); remaining.Parameters.AddWithValue("$cursor", cursor);
        var complete = await remaining.ExecuteScalarAsync(cancellationToken) is null;
        await using var update = SyncCommand(connection, transaction, "UPDATE bootstrap_scan_directories SET child_cursor=$cursor,refill_complete=$complete WHERE scan_id=$scan AND directory_id=$parent AND state=2");
        update.Parameters.AddWithValue("$scan", scanId); update.Parameters.AddWithValue("$parent", parentId);
        update.Parameters.AddWithValue("$cursor", cursor); update.Parameters.AddWithValue("$complete", complete ? 1 : 0);
        await update.ExecuteNonQueryAsync(cancellationToken);
        await CommitBootstrapWriteAsync(connection, transaction, effects, cancellationToken);
        return new(parentId, children.Count, complete, cursor);
    }

    public async Task<BootstrapRetirementBatch> RetireBootstrapBatchAsync(string mappingId, CancellationToken cancellationToken = default)
    {
        ValidateId(mappingId, nameof(mappingId));
        await CheckBootstrapAdmissionAsync(cancellationToken);
        await using var connection = await OpenBootstrapConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var effects = await BootstrapTotalChangesAsync(connection, transaction, cancellationToken);
        await using var oldest = SyncCommand(connection, transaction, "SELECT scan_id FROM bootstrap_scan_runs WHERE mapping_id=$mapping AND retired=1 ORDER BY generation LIMIT 1");
        oldest.Parameters.AddWithValue("$mapping", mappingId);
        if (await oldest.ExecuteScalarAsync(cancellationToken) is not string scanId) return new(null, 0, 0, false);
        await using var entries = SyncCommand(connection, transaction, "DELETE FROM bootstrap_scan_entries WHERE entry_id IN (SELECT entry_id FROM bootstrap_scan_entries WHERE scan_id=$scan ORDER BY entry_id LIMIT $limit)");
        entries.Parameters.AddWithValue("$scan", scanId); entries.Parameters.AddWithValue("$limit", BootstrapMetadataBatchLimit);
        var removedEntries = await entries.ExecuteNonQueryAsync(cancellationToken);
        var removedDirectories = 0;
        var removedGeneration = false;
        if (removedEntries == 0)
        {
            // All entries are gone before deleting any directory, so its FK
            // cascade cannot create unbounded hidden work.
            await using var directories = SyncCommand(connection, transaction, "DELETE FROM bootstrap_scan_directories WHERE directory_id IN (SELECT directory_id FROM bootstrap_scan_directories WHERE scan_id=$scan ORDER BY directory_id LIMIT $limit)");
            directories.Parameters.AddWithValue("$scan", scanId); directories.Parameters.AddWithValue("$limit", BootstrapMetadataBatchLimit);
            removedDirectories = await directories.ExecuteNonQueryAsync(cancellationToken);
            if (removedDirectories == 0)
            {
                await using var run = SyncCommand(connection, transaction, "DELETE FROM bootstrap_scan_runs WHERE scan_id=$scan AND retired=1");
                run.Parameters.AddWithValue("$scan", scanId);
                removedGeneration = await run.ExecuteNonQueryAsync(cancellationToken) == 1;
            }
        }
        await CommitBootstrapWriteAsync(connection, transaction, effects, cancellationToken);
        return new(scanId, removedEntries, removedDirectories, removedGeneration);
    }

    // Physical high-water WAL length triggers a rare inspection, not a claim
    // that it is all uncheckpointed. PASSIVE never truncates or waits for a long
    // reader; new metadata admission stops if outstanding frames exceed budget.
    public async Task<BootstrapCheckpointResult> CheckBootstrapWalAsync(bool force = false, CancellationToken cancellationToken = default)
    {
        var physical = File.Exists(databasePath + "-wal") ? new FileInfo(databasePath + "-wal").Length : 0;
        var last = Volatile.Read(ref _bootstrapLastCheckpoint);
        var now = Environment.TickCount64;
        if (!force && (physical <= BootstrapWalAdmissionBytes || now < Interlocked.Read(ref _bootstrapNextCheckpointAt)))
            return last is { Backpressure: true } ? last : new(false, physical, 0, 0, 0, false, false);
        if (await PreflightBootstrapTablesAsync(cancellationToken) != 2) throw new InvalidDataException("Bootstrap checkpoint requires known v2 state.");
        await using var connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = NativeDatabasePath(databasePath), Mode = SqliteOpenMode.ReadWrite, Cache = SqliteCacheMode.Private, Pooling = false, DefaultTimeout = 1
        }.ToString());
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA busy_timeout=0";
        await command.ExecuteNonQueryAsync(cancellationToken);
        command.CommandText = "PRAGMA page_size";
        var pageSize = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken));
        command.CommandText = "PRAGMA wal_checkpoint(PASSIVE)";
        Interlocked.Increment(ref _bootstrapCheckpointAttempts);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        var busy = reader.GetInt64(0) != 0;
        var log = reader.GetInt64(1); var checkpointed = reader.GetInt64(2);
        var outstanding = log >= 0 && checkpointed >= 0 ? Math.Max(0, log - checkpointed) * pageSize : physical;
        var result = new BootstrapCheckpointResult(true, physical, log, checkpointed, outstanding, busy, outstanding > BootstrapWalAdmissionBytes);
        Volatile.Write(ref _bootstrapLastCheckpoint, result);
        Interlocked.Exchange(ref _bootstrapNextCheckpointAt, now + 1000);
        return result;
    }

    private async Task CheckBootstrapAdmissionAsync(CancellationToken cancellationToken)
    {
        var checkpoint = await CheckBootstrapWalAsync(cancellationToken: cancellationToken);
        if (checkpoint.Backpressure)
        {
            Interlocked.Increment(ref _bootstrapAdmissionStops);
            throw new BootstrapStorageBackpressureException(checkpoint);
        }
    }

    private static async Task<long> BootstrapTotalChangesAsync(SqliteConnection connection, SqliteTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = SyncCommand(connection, transaction, "SELECT total_changes()");
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken));
    }

    private async Task CommitBootstrapWriteAsync(SqliteConnection connection, SqliteTransaction transaction, long before, CancellationToken cancellationToken)
    {
        var changed = await BootstrapTotalChangesAsync(connection, transaction, cancellationToken) - before;
        var timer = Stopwatch.StartNew();
        await transaction.CommitAsync(cancellationToken);
        timer.Stop();
        Interlocked.Increment(ref _bootstrapTransactions);
        Interlocked.Exchange(ref _bootstrapLastChangedRows, changed);
        Interlocked.Exchange(ref _bootstrapLastCommitTicks, timer.Elapsed.Ticks);
        UpdateBootstrapMaximum(ref _bootstrapMaxChangedRows, changed);
        UpdateBootstrapMaximum(ref _bootstrapMaxCommitTicks, timer.Elapsed.Ticks);
    }

    private static void UpdateBootstrapMaximum(ref long location, long value)
    {
        var previous = Interlocked.Read(ref location);
        while (value > previous)
        {
            var observed = Interlocked.CompareExchange(ref location, value, previous);
            if (observed == previous) break;
            previous = observed;
        }
    }
}
