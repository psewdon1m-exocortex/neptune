using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private static readonly Dictionary<string, string> LocalSourceSchemaV1 = CreateLocalSourceSchema();
    private static readonly Dictionary<string, string> LocalSourceSchema = CreateLocalSourceSchemaV2();
    private sealed record LocalSourceBackup(string SourcePath, string BackupPath, string Sha256, int TargetFormat, int SourceFormat = 0);
    private string LocalSourceBackupManifest => databasePath + ".local-source-v2-backup.json";

    private static Dictionary<string,string> CreateLocalSourceSchemaV2()
    {
        var result = CreateLocalSourceSchema();
        result["local_source_roots"] = result["local_source_roots"][..^1]+",read_mode INTEGER NOT NULL DEFAULT 0 CHECK(read_mode IN(0,1)))";
        return result;
    }

    private static Dictionary<string, string> CreateLocalSourceSchema()
    {
        var objects = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["local_source_format"] = "CREATE TABLE local_source_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL,inbox_records INTEGER NOT NULL DEFAULT 0 CHECK(inbox_records>=0),inbox_bytes INTEGER NOT NULL DEFAULT 0 CHECK(inbox_bytes>=0))",
            ["local_source_revision"] = "CREATE TABLE local_source_revision(singleton INTEGER PRIMARY KEY CHECK(singleton=1),revision INTEGER NOT NULL CHECK(revision>=0))",
            ["local_source_roots"] = "CREATE TABLE local_source_roots(root_key TEXT PRIMARY KEY,mapping_id TEXT NOT NULL,binding_json TEXT NOT NULL,volume_key TEXT NOT NULL,journal_id TEXT NOT NULL,cursor INTEGER NOT NULL CHECK(cursor>=0),owner_token TEXT NOT NULL,owner_epoch INTEGER NOT NULL CHECK(owner_epoch>0),valid INTEGER NOT NULL CHECK(valid IN(0,1)),gap_epoch INTEGER NOT NULL DEFAULT 0 CHECK(gap_epoch>=0),gap_code TEXT,legacy_revision INTEGER NOT NULL CHECK(legacy_revision>=0),inbox_records INTEGER NOT NULL DEFAULT 0 CHECK(inbox_records>=0),inbox_bytes INTEGER NOT NULL DEFAULT 0 CHECK(inbox_bytes>=0),retired_through INTEGER NOT NULL DEFAULT 0 CHECK(retired_through>=0),last_start INTEGER,last_end INTEGER,last_hash TEXT)",
            ["local_source_active"] = "CREATE TABLE local_source_active(mapping_id TEXT PRIMARY KEY,root_key TEXT NOT NULL UNIQUE)",
            ["local_source_inbox"] = "CREATE TABLE local_source_inbox(root_key TEXT NOT NULL,journal_id TEXT NOT NULL,usn INTEGER NOT NULL CHECK(usn>=0),payload BLOB NOT NULL CHECK(length(payload)<=592),byte_count INTEGER NOT NULL CHECK(byte_count=length(payload)+64),PRIMARY KEY(root_key,journal_id,usn)) WITHOUT ROWID",
            ["local_source_root_mapping"] = "CREATE INDEX local_source_root_mapping ON local_source_roots(mapping_id,root_key)"
        };
        // O(1) and independent of a qualified server root. Old clients may
        // ignore optional tables; their legacy writes still break continuity.
        foreach (var table in new[] { "sync_mappings", "sync_files", "sync_upload_checkpoints" })
            foreach (var action in new[] { "INSERT", "UPDATE", "DELETE" })
            {
                var name = "local_source_legacy_" + table + "_" + action.ToLowerInvariant();
                objects[name] = $"CREATE TRIGGER {name} AFTER {action} ON {table} BEGIN UPDATE local_source_revision SET revision=revision+1 WHERE singleton=1; END";
            }
        return objects;
    }

    private async Task ValidateOptionalLocalSourceBeforeInitializeAsync(CancellationToken cancellationToken)
        => _ = await PreflightLocalSourceAsync(cancellationToken);

    private async Task<int> PreflightLocalSourceAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!File.Exists(databasePath)) return 0;
        try
        {
            await using var connection = new SqliteConnection(ReadOnlySyncConnectionString(databasePath));
            await connection.OpenAsync(cancellationToken);
            using var transaction = connection.BeginTransaction(deferred: true);
            await using var objects = SyncCommand(connection, transaction, "SELECT name,sql FROM sqlite_master WHERE (name GLOB 'local_source_*' OR tbl_name GLOB 'local_source_*' AND type IN ('trigger','index')) AND name NOT LIKE 'sqlite_autoindex_%'");
            var actual = new Dictionary<string, string>(StringComparer.Ordinal);
            await using (var reader = await objects.ExecuteReaderAsync(cancellationToken))
                while (await reader.ReadAsync(cancellationToken)) actual.Add(reader.GetString(0), reader.IsDBNull(1) ? "" : reader.GetString(1));
            if (actual.Count == 0) return 0;
            if (!actual.ContainsKey("local_source_format")) throw new InvalidDataException("The local source module is incomplete.");
            await using var version = SyncCommand(connection, transaction, "SELECT version FROM local_source_format WHERE singleton=1");
            if (await version.ExecuteScalarAsync(cancellationToken) is not long format || format is not (1 or 2))
                throw new InvalidDataException("This Neptune build cannot write the local source module format.");
            // Fixed metadata only; this is not full profile data certification.
            static string Normalize(string sql) => string.Concat(sql.Where(c => !char.IsWhiteSpace(c))).TrimEnd(';');
            var expected = format==1?LocalSourceSchemaV1:LocalSourceSchema;
            if (actual.Count != expected.Count || expected.Any(pair => !actual.TryGetValue(pair.Key, out var sql)
                || Normalize(sql) != Normalize(pair.Value))) throw new InvalidDataException("Local source schema or legacy invalidation guards are incompatible.");
            return (int)format;
        }
        catch (SqliteException error) { throw new InvalidDataException("Local source module cannot be safely inspected; no writes attempted.", error); }
    }

    private async Task EnsureLocalSourceAsync(CancellationToken cancellationToken)
    {
        if (await PreflightLocalSourceAsync(cancellationToken) == 2) return;
        await using var migrationLock = await AcquireSyncMigrationLockAsync(cancellationToken);
        var previous = await PreflightLocalSourceAsync(cancellationToken);
        if (previous == 2) return;
        // SQLite Backup API includes committed WAL; copying .db would not.
        var candidate = databasePath + ".local-source-" + Guid.NewGuid().ToString("N") + ".partial";
        try
        {
            await CreateConsistentSyncDatabaseCopyAsync(databasePath, candidate, cancellationToken);
            var hash = await HashSyncBackupAsync(candidate, cancellationToken);
            var backup = databasePath + ".pre-local-source-v2-" + hash + ".db";
            if (File.Exists(backup))
            {
                if (await HashSyncBackupAsync(backup, cancellationToken) != hash) throw new InvalidDataException("Local source migration backup identity changed.");
                File.Delete(candidate);
            }
            else File.Move(candidate, backup);
            var manifest = JsonSerializer.Serialize(new LocalSourceBackup(Path.GetFullPath(databasePath), backup, hash, 2, previous), SyncStateJson);
            var temporary = LocalSourceBackupManifest + "." + Guid.NewGuid().ToString("N") + ".partial";
            try
            {
                await File.WriteAllTextAsync(temporary, manifest, cancellationToken);
                using (var stream = new FileStream(temporary, FileMode.Open, FileAccess.Write, FileShare.None)) stream.Flush(true);
                File.Move(temporary, LocalSourceBackupManifest, overwrite: true);
            }
            finally { if (File.Exists(temporary)) File.Delete(temporary); }
            await using var connection = await OpenLocalSourceConnectionAsync(cancellationToken, schemaRequired: false);
            using var transaction = connection.BeginTransaction(deferred: false);
            if (previous==0)
            {
                foreach (var sql in LocalSourceSchema.Values) await ExecuteSyncAsync(connection, transaction, sql, cancellationToken);
                await ExecuteSyncAsync(connection, transaction, "INSERT INTO local_source_format(singleton,version) VALUES(1,2); INSERT INTO local_source_revision(singleton,revision) VALUES(1,0)", cancellationToken);
            }
            else
            {
                // ADD COLUMN with a fixed default is metadata-only: old named
                // root/record bytes and historical binding keys stay unchanged.
                await ExecuteSyncAsync(connection, transaction, "ALTER TABLE local_source_roots ADD COLUMN read_mode INTEGER NOT NULL DEFAULT 0 CHECK(read_mode IN(0,1)); UPDATE local_source_format SET version=2 WHERE singleton=1", cancellationToken);
            }
            await transaction.CommitAsync(cancellationToken);
        }
        finally { if (File.Exists(candidate)) File.Delete(candidate); }
    }

    // Separate pre-module profile for offline rollback, never in-place overwrite.
    public async Task<string> CreateLocalSourceRollbackCopyAsync(string destinationPath, CancellationToken cancellationToken = default)
        => await CreateLocalSourceRollbackCopy(destinationPath,
            File.Exists(databasePath+".local-source-v1-backup.json")?1:2,0,cancellationToken);

    public Task<string> CreateLocalSourceV1RollbackCopyAsync(string destinationPath, CancellationToken cancellationToken = default)
        => CreateLocalSourceRollbackCopy(destinationPath,2,1,cancellationToken);

    private async Task<string> CreateLocalSourceRollbackCopy(string destinationPath,int targetFormat,int sourceFormat,CancellationToken cancellationToken)
    {
        var destination = Path.GetFullPath(destinationPath);
        if (destination == Path.GetFullPath(databasePath) || File.Exists(destination) || File.Exists(destination + "-wal") || File.Exists(destination + "-shm"))
            throw new InvalidOperationException("Rollback requires a separate absent destination.");
        var manifestPath=databasePath+".local-source-v"+targetFormat+"-backup.json";
        if (new FileInfo(manifestPath).Length>16*1024) throw new InvalidDataException("Local source rollback manifest exceeds its bound.");
        using var document=Sync.Protocol.SyncProtocolReader.Parse(await File.ReadAllBytesAsync(manifestPath,cancellationToken));
        string[] fields=targetFormat==1?["sourcePath","backupPath","sha256","targetFormat"]:["sourcePath","backupPath","sha256","targetFormat","sourceFormat"];
        if(document.RootElement.ValueKind!=JsonValueKind.Object||document.RootElement.EnumerateObject().Count()!=fields.Length||document.RootElement.EnumerateObject().Any(p=>!fields.Contains(p.Name,StringComparer.Ordinal)))throw new InvalidDataException("Local source rollback manifest has unknown/missing fields.");
        var manifest = JsonSerializer.Deserialize<LocalSourceBackup>(document.RootElement.GetRawText(), SyncStateJson)
            ?? throw new InvalidDataException("Local source backup manifest is invalid.");
        if (manifest.SourcePath != Path.GetFullPath(databasePath) || manifest.TargetFormat != targetFormat || manifest.SourceFormat!=sourceFormat
            || manifest.Sha256.Length!=64 || manifest.Sha256.Any(c=>!char.IsAsciiHexDigit(c)) || manifest.Sha256!=manifest.Sha256.ToLowerInvariant()
            || manifest.BackupPath != Path.GetFullPath(databasePath) + ".pre-local-source-v"+targetFormat+"-"+manifest.Sha256+".db"
            || await HashSyncBackupAsync(manifest.BackupPath, cancellationToken) != manifest.Sha256)
            throw new InvalidDataException("Local source rollback identity cannot be verified.");
        Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
        await CreateConsistentSyncDatabaseCopyAsync(manifest.BackupPath, destination, cancellationToken);
        return destination;
    }
}
