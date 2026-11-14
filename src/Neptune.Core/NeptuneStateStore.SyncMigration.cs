using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private sealed record SyncMigrationBackup(string SourcePath, string BackupPath, string Sha256, int TargetFormat, DateTimeOffset CreatedAt);
    private string SyncMigrationManifestPath => databasePath + ".sync-state-v1-backup.json";

    // Read-only inspection precedes any writer PRAGMA/DDL, including for a DB
    // from a newer build which has none of our legacy tables.
    private async Task<bool> PreflightSyncStateAsync(bool verifyLegacyIntegrity, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!File.Exists(databasePath))
        {
            if (File.Exists(databasePath + "-wal") || File.Exists(databasePath + "-shm"))
                throw new InvalidDataException("Orphan profile journal files require explicit recovery before initialization.");
            return false;
        }
        try
        {
            await using var connection = new SqliteConnection(ReadOnlySyncConnectionString(databasePath));
            await connection.OpenAsync(cancellationToken);
            using var transaction = connection.BeginTransaction(deferred: true);
            await using var tables = SyncCommand(connection, transaction, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'");
            var names = new HashSet<string>(StringComparer.Ordinal);
            await using (var reader = await tables.ExecuteReaderAsync(cancellationToken))
                while (await reader.ReadAsync(cancellationToken)) names.Add(reader.GetString(0));
            if (names.Contains("sync_state_format"))
            {
                await using var version = SyncCommand(connection, transaction, "SELECT version FROM sync_state_format WHERE singleton=1");
                if (await version.ExecuteScalarAsync(cancellationToken) is not long number || number != SyncStateFormatVersion)
                    throw new InvalidDataException("This Neptune build cannot write the profile's sync state format.");
                await VerifySyncSchemaAsync(connection, transaction, names, cancellationToken);
                return false;
            }
            if (names.Count == 0) return false;
            if (!names.Contains("sync_mappings") && !names.Contains("backup_runs") && !names.Contains("sync_upload_checkpoints"))
                throw new InvalidDataException("The database is not a recognized Neptune legacy profile.");
            if (verifyLegacyIntegrity) await CheckSyncDatabaseIntegrityAsync(connection, transaction, cancellationToken);
            return true;
        }
        catch (SqliteException error)
        {
            throw new InvalidDataException("The profile cannot be safely inspected; no migration was attempted.", error);
        }
    }

    // Explicit full inspection for startup/recovery tooling. Normal SyncAsync
    // performs a bounded schema/version check, not an O(profile data) scan.
    public async Task VerifySyncIntegrityAsync(CancellationToken cancellationToken = default)
    {
        await PreflightSyncStateAsync(verifyLegacyIntegrity: false, cancellationToken);
        await using var connection = new SqliteConnection(ReadOnlySyncConnectionString(databasePath));
        await connection.OpenAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: true);
        await CheckSyncDatabaseIntegrityAsync(connection, transaction, cancellationToken);
    }

    private static async Task CheckSyncDatabaseIntegrityAsync(SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken)
    {
        await using var check = SyncCommand(connection, transaction, "PRAGMA quick_check(1)");
        if (await check.ExecuteScalarAsync(cancellationToken) is not string result || result != "ok")
            throw new InvalidDataException("The profile failed SQLite integrity inspection.");
        await using var foreign = SyncCommand(connection, transaction, "PRAGMA foreign_key_check");
        await using var reader = await foreign.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken)) throw new InvalidDataException("The profile has inconsistent foreign key references.");
    }

    private static async Task VerifySyncSchemaAsync(SqliteConnection connection, SqliteTransaction transaction, HashSet<string> tables, CancellationToken cancellationToken)
    {
        // Metadata only: bounded by the format's fixed number of tables/columns.
        // This is not a full integrity certification of user data pages.
        var required = new Dictionary<string, string[]>(StringComparer.Ordinal)
        {
            ["backup_runs"] = ["run_id", "client_instance_id", "export_generation"],
            ["sync_mappings"] = ["mapping_id", "client_instance_id", "local_path", "enabled", "created_at"],
            ["sync_files"] = ["mapping_id", "relative_path", "sha256", "state", "updated_at"],
            ["sync_mapping_stats"] = ["mapping_id", "mirrored_bytes", "mirrored_files", "last_success_at"],
            ["remote_commands"] = ["command_id", "project_id", "kind", "state", "updated_at"],
            ["sync_upload_checkpoints"] = ["scope", "target_uri", "idempotency_key", "fingerprint", "upload_id"],
            ["sync_state_format"] = ["singleton", "version", "migrated_at"],
            ["sync_state_roots"] = ["mapping_id", "client_instance_id", "identity_json", "server_origin", "server_root_id", "legacy_revision", "invalidation_generation", "needs_reconciliation"],
            ["sync_state_entries"] = ["mapping_id", "entry_id", "parent_entry_id", "name_key", "local_generation", "presence", "filesystem_id", "local_json"],
            ["sync_state_baselines"] = ["mapping_id", "entry_id", "remote_node_id", "baseline_json"],
            ["sync_state_remote"] = ["mapping_id", "node_id", "last_seq", "remote_json"],
            ["sync_state_dirty"] = ["mapping_id", "entry_id", "observed_generation", "reasons", "first_dirty", "due_at"],
            ["sync_state_cursors"] = ["mapping_id", "journal_id", "position", "valid", "captured_at"],
            ["sync_state_operations"] = ["operation_id", "mapping_id", "entry_id", "local_generation", "intent_json", "fingerprint", "stage", "invalidation_generation", "legacy_revision", "attempt", "worker_id", "lease_until", "receipt_json", "cancellation_fence_json"],
            ["sync_state_dependencies"] = ["operation_id", "dependency_id"]
        };
        foreach (var (table, columns) in required)
        {
            if (!tables.Contains(table)) throw new InvalidDataException("The profile sync state schema is incomplete.");
            await using var command = SyncCommand(connection, transaction, $"PRAGMA table_info('{table}')");
            var actual = new HashSet<string>(StringComparer.Ordinal);
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
                while (await reader.ReadAsync(cancellationToken)) actual.Add(reader.GetString(1));
            if (columns.Any(column => !actual.Contains(column))) throw new InvalidDataException("The profile sync state schema is incompatible.");
        }
        await using var objects = SyncCommand(connection, transaction, "SELECT name FROM sqlite_master WHERE type IN ('trigger','index')");
        var guards = new HashSet<string>(StringComparer.Ordinal);
        await using (var reader = await objects.ExecuteReaderAsync(cancellationToken))
            while (await reader.ReadAsync(cancellationToken)) guards.Add(reader.GetString(0));
        if (new[] { "sync_state_baselines_remote", "sync_state_entries_live_name", "sync_state_legacy_mapping_insert", "sync_state_legacy_mapping_update", "sync_state_legacy_mapping_delete",
            "sync_state_legacy_file_insert", "sync_state_legacy_file_update", "sync_state_legacy_file_delete", "sync_state_legacy_checkpoint_insert", "sync_state_legacy_checkpoint_update", "sync_state_legacy_checkpoint_delete" }
            .Any(name => !guards.Contains(name))) throw new InvalidDataException("The profile sync state safety guards are incomplete.");
    }

    private static string ReadOnlySyncConnectionString(string path) => new SqliteConnectionStringBuilder
    {
        DataSource = NativeDatabasePath(path), Mode = SqliteOpenMode.ReadOnly, Cache = SqliteCacheMode.Private,
        Pooling = false, DefaultTimeout = 30
    }.ToString();

    private async Task<FileStream> AcquireSyncMigrationLockAsync(CancellationToken cancellationToken)
    {
        var until = DateTimeOffset.UtcNow.AddSeconds(30);
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try { return new FileStream(databasePath + ".sync-state-migration.lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None); }
            catch (IOException) when (DateTimeOffset.UtcNow < until) { await Task.Delay(25, cancellationToken); }
        }
    }

    private async Task BackupLegacySyncStateAsync(CancellationToken cancellationToken)
    {
        var candidate = databasePath + ".sync-state-v1-" + Guid.NewGuid().ToString("N") + ".partial";
        try
        {
            await CreateConsistentSyncDatabaseCopyAsync(databasePath, candidate, cancellationToken);
            var hash = await HashSyncBackupAsync(candidate, cancellationToken);
            var backup = databasePath + ".pre-sync-state-v1-" + hash + ".db";
            if (File.Exists(backup))
            {
                if (await HashSyncBackupAsync(backup, cancellationToken) != hash) throw new InvalidDataException("A migration backup identity was modified.");
                File.Delete(candidate);
            }
            else File.Move(candidate, backup);
            var manifest = new SyncMigrationBackup(Path.GetFullPath(databasePath), Path.GetFullPath(backup), hash, SyncStateFormatVersion, DateTimeOffset.UtcNow);
            var temporaryManifest = SyncMigrationManifestPath + "." + Guid.NewGuid().ToString("N") + ".partial";
            try
            {
                await File.WriteAllTextAsync(temporaryManifest, JsonSerializer.Serialize(manifest, SyncStateJson), cancellationToken);
                using (var stream = new FileStream(temporaryManifest, FileMode.Open, FileAccess.Write, FileShare.None)) stream.Flush(flushToDisk: true);
                File.Move(temporaryManifest, SyncMigrationManifestPath, overwrite: true);
            }
            finally { if (File.Exists(temporaryManifest)) File.Delete(temporaryManifest); }
        }
        finally { if (File.Exists(candidate)) File.Delete(candidate); }
    }

    // Explicit rollback produces a separate old-format DB. It never overwrites
    // an active profile or touches its WAL/SHM: replacing a profile is an offline
    // operator step after all clients have stopped and journals are preserved.
    public async Task<string> CreateLegacySyncRollbackCopyAsync(string destinationPath, CancellationToken cancellationToken = default)
    {
        var target = Path.GetFullPath(destinationPath);
        if (string.Equals(target, Path.GetFullPath(databasePath), OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal)
            || File.Exists(target) || File.Exists(target + "-wal") || File.Exists(target + "-shm"))
            throw new InvalidOperationException("Rollback requires a separate, absent destination database.");
        var manifest = JsonSerializer.Deserialize<SyncMigrationBackup>(await File.ReadAllTextAsync(SyncMigrationManifestPath, cancellationToken), SyncStateJson)
            ?? throw new InvalidDataException("The migration backup manifest is invalid.");
        var expectedPrefix = Path.GetFullPath(databasePath) + ".pre-sync-state-v1-";
        if (manifest.SourcePath != Path.GetFullPath(databasePath) || manifest.TargetFormat != SyncStateFormatVersion
            || !Path.GetFullPath(manifest.BackupPath).StartsWith(expectedPrefix, StringComparison.Ordinal)
            || await HashSyncBackupAsync(manifest.BackupPath, cancellationToken) != manifest.Sha256)
            throw new InvalidDataException("The migration backup identity/integrity cannot be verified.");
        Directory.CreateDirectory(Path.GetDirectoryName(target)!);
        await CreateConsistentSyncDatabaseCopyAsync(manifest.BackupPath, target, cancellationToken);
        return target;
    }

    private static async Task CreateConsistentSyncDatabaseCopyAsync(string source, string target, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // SQLite may create a rollback journal whose suffix crosses MAX_PATH
        // even when the database filename itself is shorter than 260 chars.
        // Keep manifests/public paths ordinary, but always supply the Windows
        // VFS with an absolute extended path (including UNC) for native I/O.
        // Reserve a new file so recovery cannot overwrite somebody else's DB.
        using (new FileStream(target, FileMode.CreateNew, FileAccess.Write, FileShare.None)) { }
        try
        {
            await using var input = new SqliteConnection(ReadOnlySyncConnectionString(source));
            await input.OpenAsync(cancellationToken);
            await using var output = new SqliteConnection(new SqliteConnectionStringBuilder
            {
                DataSource = NativeDatabasePath(target), Mode = SqliteOpenMode.ReadWrite, Cache = SqliteCacheMode.Private, Pooling = false
            }.ToString());
            await output.OpenAsync(cancellationToken);
            // SQLite Backup API includes committed WAL pages; copying .db alone
            // loses those pages. SQLite holds a consistent snapshot during copy.
            input.BackupDatabase(output);
            cancellationToken.ThrowIfCancellationRequested();
            await CheckSyncDatabaseIntegrityAsync(output, null, cancellationToken);
        }
        catch { if (File.Exists(target)) File.Delete(target); throw; }
        using var flush = new FileStream(target, FileMode.Open, FileAccess.Write, FileShare.None);
        flush.Flush(flushToDisk: true);
    }

    private static async Task<string> HashSyncBackupAsync(string path, CancellationToken cancellationToken)
    {
        await using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536, useAsync: true);
        return Convert.ToHexStringLower(await SHA256.HashDataAsync(file, cancellationToken));
    }
}
