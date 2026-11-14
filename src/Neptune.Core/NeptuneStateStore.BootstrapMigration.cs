using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private sealed record BootstrapMigrationBackup(string SourcePath, string BackupPath, string Sha256, int SourceFormat, int TargetFormat, DateTimeOffset CreatedAt);
    private string BootstrapMigrationManifestPath => databasePath + ".bootstrap-v2-backup.json";

    private async Task ValidateOptionalBootstrapBeforeInitializeAsync(CancellationToken cancellationToken)
        => _ = await PreflightBootstrapTablesAsync(cancellationToken, initializedProfileRequired: false);

    private async Task EnsureBootstrapSchemaAsync(CancellationToken cancellationToken)
    {
        var format = await PreflightBootstrapTablesAsync(cancellationToken);
        if (format == 2) return;
        await using var migrationLock = await AcquireSyncMigrationLockAsync(cancellationToken);
        format = await PreflightBootstrapTablesAsync(cancellationToken);
        if (format == 2) return;
        if (format == 1) await BackupBootstrapV1Async(cancellationToken);
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        if (format == 0) await InitializeBootstrapTablesAsync(connection, transaction, cancellationToken);
        else
        {
            await using var version = SyncCommand(connection, transaction, "SELECT version FROM bootstrap_scan_format WHERE singleton=1");
            if (await version.ExecuteScalarAsync(cancellationToken) is not long actual || actual != 1)
                throw new InvalidDataException("Bootstrap format changed during migration.");
            await ExecuteSyncAsync(connection, transaction, """
                ALTER TABLE bootstrap_scan_runs ADD COLUMN eligible_through INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE bootstrap_scan_runs ADD COLUMN retired INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE bootstrap_scan_directories ADD COLUMN file_count INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE bootstrap_scan_directories ADD COLUMN directory_count INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE bootstrap_scan_directories ADD COLUMN byte_count INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE bootstrap_scan_directories ADD COLUMN max_entry_id INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE bootstrap_scan_directories ADD COLUMN child_cursor INTEGER NOT NULL DEFAULT 0;
                ALTER TABLE bootstrap_scan_directories ADD COLUMN refill_complete INTEGER NOT NULL DEFAULT 0;
                DROP INDEX bootstrap_scan_eligible;
                CREATE INDEX bootstrap_scan_eligible ON bootstrap_scan_entries(scan_id,entry_id);
                CREATE INDEX bootstrap_scan_child_refill ON bootstrap_scan_entries(scan_id,parent_directory_id,kind,entry_id);
                CREATE INDEX bootstrap_scan_frontier_refill ON bootstrap_scan_directories(scan_id,state,refill_complete,directory_id);
                CREATE INDEX bootstrap_scan_retired ON bootstrap_scan_runs(mapping_id,retired,generation);
                UPDATE bootstrap_scan_runs SET status=3,error='Module migration interrupted the old metadata generation; a fresh source enumeration is required.' WHERE status IN (0,1);
                UPDATE bootstrap_scan_format SET version=2 WHERE singleton=1;
                """, cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
    }

    private async Task BackupBootstrapV1Async(CancellationToken cancellationToken)
    {
        // Includes committed WAL pages and full integrity inspection. A legacy
        // metadata generation is never promoted to a new continuous source cut.
        var candidate = databasePath + ".bootstrap-v2-" + Guid.NewGuid().ToString("N") + ".partial";
        try
        {
            await CreateConsistentSyncDatabaseCopyAsync(databasePath, candidate, cancellationToken);
            var hash = await HashSyncBackupAsync(candidate, cancellationToken);
            var backup = databasePath + ".pre-bootstrap-v2-" + hash + ".db";
            if (File.Exists(backup))
            {
                if (await HashSyncBackupAsync(backup, cancellationToken) != hash) throw new InvalidDataException("The bootstrap backup identity changed.");
                File.Delete(candidate);
            }
            else File.Move(candidate, backup);
            var manifest = new BootstrapMigrationBackup(Path.GetFullPath(databasePath), Path.GetFullPath(backup), hash, 1, 2, DateTimeOffset.UtcNow);
            var temporary = BootstrapMigrationManifestPath + "." + Guid.NewGuid().ToString("N") + ".partial";
            try
            {
                await File.WriteAllTextAsync(temporary, JsonSerializer.Serialize(manifest, SyncStateJson), cancellationToken);
                using (var stream = new FileStream(temporary, FileMode.Open, FileAccess.Write, FileShare.None)) stream.Flush(true);
                File.Move(temporary, BootstrapMigrationManifestPath, overwrite: true);
            }
            finally { if (File.Exists(temporary)) File.Delete(temporary); }
        }
        finally { if (File.Exists(candidate)) File.Delete(candidate); }
    }

    // Produces a separate verified old-format profile. Installing it is an
    // offline operator decision; it never overwrites the live DB/WAL or newer
    // user/profile writes made after the migration's snapshot point.
    public async Task<string> CreateBootstrapV1RollbackCopyAsync(string destinationPath, CancellationToken cancellationToken = default)
    {
        var target = Path.GetFullPath(destinationPath);
        if (string.Equals(target, Path.GetFullPath(databasePath), OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal)
            || File.Exists(target) || File.Exists(target + "-wal") || File.Exists(target + "-shm"))
            throw new InvalidOperationException("Rollback requires a separate absent destination.");
        var manifest = JsonSerializer.Deserialize<BootstrapMigrationBackup>(await File.ReadAllTextAsync(BootstrapMigrationManifestPath, cancellationToken), SyncStateJson)
            ?? throw new InvalidDataException("Bootstrap migration manifest is invalid.");
        if (manifest.SourcePath != Path.GetFullPath(databasePath) || manifest.SourceFormat != 1 || manifest.TargetFormat != 2
            || !Path.GetFullPath(manifest.BackupPath).StartsWith(Path.GetFullPath(databasePath) + ".pre-bootstrap-v2-", StringComparison.Ordinal)
            || await HashSyncBackupAsync(manifest.BackupPath, cancellationToken) != manifest.Sha256)
            throw new InvalidDataException("Bootstrap rollback backup identity cannot be verified.");
        Directory.CreateDirectory(Path.GetDirectoryName(target)!);
        await CreateConsistentSyncDatabaseCopyAsync(manifest.BackupPath, target, cancellationToken);
        return target;
    }
}
