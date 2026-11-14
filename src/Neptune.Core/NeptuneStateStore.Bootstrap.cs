using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Diagnostics;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    public const int BootstrapMetadataBatchLimit = 256;
    private long _bootstrapTransactions, _bootstrapMaxChangedRows, _bootstrapMaxCommitTicks;
    private long _bootstrapLastChangedRows, _bootstrapLastCommitTicks;
    private long _bootstrapNextCheckpointAt;
    public BootstrapWriteMetrics GetBootstrapWriteMetrics() => new(Interlocked.Read(ref _bootstrapTransactions),
        Interlocked.Read(ref _bootstrapMaxChangedRows), TimeSpan.FromTicks(Interlocked.Read(ref _bootstrapMaxCommitTicks)).TotalMilliseconds,
        Interlocked.Read(ref _bootstrapLastChangedRows), TimeSpan.FromTicks(Interlocked.Read(ref _bootstrapLastCommitTicks)).TotalMilliseconds);

    internal async Task<FileStream> AcquireBootstrapOwnerAsync(string mappingId, CancellationToken cancellationToken)
    {
        ValidateId(mappingId, nameof(mappingId));
        var key = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(mappingId)));
        cancellationToken.ThrowIfCancellationRequested();
        // Whole-scanner ownership, released by OS on process death. A new owner
        // may mark the old generation interrupted; no PID guessing is needed.
        return await Task.FromResult(new FileStream(databasePath + ".bootstrap-" + key + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None));
    }

    public async Task<BootstrapScanState> BeginBootstrapScanAsync(string scanId, string mappingId, string clientInstanceId,
        string sourceRoot, string ownerToken, CancellationToken cancellationToken = default)
    {
        foreach (var id in new[] { scanId, mappingId, clientInstanceId, ownerToken }) ValidateId(id, nameof(scanId));
        sourceRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(sourceRoot));
        await EnsureBootstrapSchemaAsync(cancellationToken);
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var effects = await BootstrapTotalChangesAsync(connection, transaction, cancellationToken);
        var existing = await ReadBootstrapStateAsync(connection, transaction, scanId, cancellationToken);
        if (existing is not null)
        {
            if (existing.MappingId != mappingId || existing.SourceRoot != sourceRoot || existing.OwnerToken != ownerToken)
                throw new InvalidOperationException("A scan generation cannot be rebound to another source/owner.");
            await using var client = SyncCommand(connection, transaction, "SELECT client_instance_id FROM bootstrap_scan_runs WHERE scan_id=$scan");
            client.Parameters.AddWithValue("$scan", scanId);
            if (await client.ExecuteScalarAsync(cancellationToken) is not string existingClient || existingClient != clientInstanceId)
                throw new InvalidOperationException("A scan generation cannot be rebound to another client.");
            return existing;
        }
        await using var mapping = SyncCommand(connection, transaction, "SELECT local_path FROM sync_mappings WHERE mapping_id=$mapping AND client_instance_id=$client AND enabled=1");
        mapping.Parameters.AddWithValue("$mapping", mappingId);
        mapping.Parameters.AddWithValue("$client", clientInstanceId);
        if (await mapping.ExecuteScalarAsync(cancellationToken) is not string path
            || !string.Equals(Path.TrimEndingDirectorySeparator(Path.GetFullPath(path)), sourceRoot, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal))
            throw new InvalidOperationException("The scan source is not this client's enabled mapping.");
        // Visibility retains one terminal generation. Physical removal is a
        // separate bounded maintenance batch, never a tree-wide FK cascade.
        await using var prune = SyncCommand(connection, transaction, "UPDATE bootstrap_scan_runs SET retired=1 WHERE mapping_id=$mapping AND retired=0 AND status<>0 AND scan_id NOT IN (SELECT scan_id FROM bootstrap_scan_runs WHERE mapping_id=$mapping AND retired=0 ORDER BY generation DESC LIMIT 1)");
        prune.Parameters.AddWithValue("$mapping", mappingId);
        await prune.ExecuteNonQueryAsync(cancellationToken);
        await using var insert = SyncCommand(connection, transaction, "INSERT INTO bootstrap_scan_runs(scan_id,mapping_id,client_instance_id,source_root,owner_token,status,created_at,generation) VALUES($scan,$mapping,$client,$source,$owner,0,$at,(SELECT ifnull(MAX(generation),0)+1 FROM bootstrap_scan_runs WHERE mapping_id=$mapping)); INSERT INTO bootstrap_scan_directories(scan_id,relative_path,state) VALUES($scan,'',0)");
        insert.Parameters.AddWithValue("$scan", scanId);
        insert.Parameters.AddWithValue("$mapping", mappingId);
        insert.Parameters.AddWithValue("$client", clientInstanceId);
        insert.Parameters.AddWithValue("$source", sourceRoot);
        insert.Parameters.AddWithValue("$owner", ownerToken);
        insert.Parameters.AddWithValue("$at", DateTimeOffset.UtcNow.ToString("O"));
        await insert.ExecuteNonQueryAsync(cancellationToken);
        var created = (await ReadBootstrapStateAsync(connection, transaction, scanId, cancellationToken))!;
        await CommitBootstrapWriteAsync(connection, transaction, effects, cancellationToken);
        return created;
    }

    internal async Task RecoverBootstrapOwnerAsync(string mappingId, CancellationToken cancellationToken)
    {
        await EnsureBootstrapSchemaAsync(cancellationToken);
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var effects = await BootstrapTotalChangesAsync(connection, transaction, cancellationToken);
        await using var command = SyncCommand(connection, transaction, "UPDATE bootstrap_scan_runs SET status=$interrupted,error='Previous scanner ownership ended; a fresh generation must enumerate again.' WHERE mapping_id=$mapping AND status=0");
        command.Parameters.AddWithValue("$mapping", mappingId);
        command.Parameters.AddWithValue("$interrupted", (int)BootstrapScanStatus.Interrupted);
        await command.ExecuteNonQueryAsync(cancellationToken);
        await CommitBootstrapWriteAsync(connection, transaction, effects, cancellationToken);
    }

    public async Task<BootstrapScanState?> GetBootstrapScanAsync(string scanId, CancellationToken cancellationToken = default)
    {
        var format = await PreflightBootstrapTablesAsync(cancellationToken);
        if (format == 0) return null;
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        return await ReadBootstrapStateAsync(connection, null, scanId, cancellationToken, legacy: format == 1);
    }

    public async Task<IReadOnlyList<BootstrapScanState>> ListBootstrapScansAsync(string mappingId, CancellationToken cancellationToken = default)
    {
        var format = await PreflightBootstrapTablesAsync(cancellationToken);
        if (format == 0) return [];
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        await using var command = SyncCommand(connection, null, "SELECT scan_id FROM bootstrap_scan_runs WHERE mapping_id=$mapping " + (format == 2 ? "AND retired=0 " : "") + "ORDER BY generation DESC LIMIT 2");
        command.Parameters.AddWithValue("$mapping", mappingId);
        var ids = new List<string>(2);
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            while (await reader.ReadAsync(cancellationToken)) ids.Add(reader.GetString(0));
        var result = new List<BootstrapScanState>(2);
        foreach (var id in ids) result.Add((await ReadBootstrapStateAsync(connection, null, id, cancellationToken, legacy: format == 1))!);
        return result;
    }

    public async Task<BootstrapDirectory?> ClaimBootstrapDirectoryAsync(string scanId, string ownerToken, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenBootstrapConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var effects = await BootstrapTotalChangesAsync(connection, transaction, cancellationToken);
        await RequireBootstrapOwnerAsync(connection, transaction, scanId, ownerToken, cancellationToken);
        await using (var outstanding = SyncCommand(connection, transaction, "SELECT 1 FROM bootstrap_scan_directories WHERE scan_id=$scan AND state=1 LIMIT 1"))
        {
            outstanding.Parameters.AddWithValue("$scan", scanId);
            if (await outstanding.ExecuteScalarAsync(cancellationToken) is not null)
                throw new InvalidOperationException("Exactly one parent may enumerate; later eligibility must not overtake an earlier parent.");
        }
        BootstrapDirectory? directory = null;
        await using (var query = SyncCommand(connection, transaction, "SELECT directory_id,relative_path FROM bootstrap_scan_directories WHERE scan_id=$scan AND state=0 ORDER BY directory_id LIMIT 1"))
        {
            query.Parameters.AddWithValue("$scan", scanId);
            await using var reader = await query.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken)) directory = new(scanId, reader.GetInt64(0), reader.GetString(1), ownerToken);
        }
        if (directory is null)
        {
            await using var refill = SyncCommand(connection, transaction, "SELECT 1 FROM bootstrap_scan_directories WHERE scan_id=$scan AND state=2 AND refill_complete=0 LIMIT 1");
            refill.Parameters.AddWithValue("$scan", scanId);
            if (await refill.ExecuteScalarAsync(cancellationToken) is not null)
                throw new InvalidOperationException("The child frontier requires another bounded refill before enumeration can finish.");
            return null;
        }
        await using var claim = SyncCommand(connection, transaction, "UPDATE bootstrap_scan_directories SET state=1 WHERE scan_id=$scan AND directory_id=$directory AND state=0");
        AddBootstrapDirectory(claim, directory);
        await claim.ExecuteNonQueryAsync(cancellationToken);
        await CommitBootstrapWriteAsync(connection, transaction, effects, cancellationToken);
        return directory;
    }

    public async Task StageBootstrapEntriesAsync(BootstrapDirectory directory, IReadOnlyList<BootstrapSourceEntry> entries, CancellationToken cancellationToken = default)
    {
        if (entries.Count > BootstrapMetadataBatchLimit) throw new ArgumentOutOfRangeException(nameof(entries));
        ValidateSyncBatch(entries);
        foreach (var entry in entries)
        {
            ValidateName(entry.RawName);
            if (entry.Size < 0 || entry.IsDirectory && entry.Size != 0) throw new ArgumentException("Scan metadata is invalid.");
        }
        await CheckBootstrapAdmissionAsync(cancellationToken);
        await using var connection = await OpenBootstrapConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var effects = await BootstrapTotalChangesAsync(connection, transaction, cancellationToken);
        await RequireBootstrapDirectoryAsync(connection, transaction, directory, cancellationToken);
        long files = 0, directories = 0, bytes = 0, maxId = 0;
        foreach (var entry in entries)
        {
            var relative = directory.RelativePath.Length == 0 ? entry.RawName : directory.RelativePath + "/" + entry.RawName;
            if (relative.Length > 32768) throw new InvalidDataException("Scan path exceeds its bound.");
            var key = WebDavSyncClient.CanonicalPathKey(entry.RawName);
            await using var exists = SyncCommand(connection, transaction, "SELECT raw_name,kind,size,modified_at FROM bootstrap_scan_entries WHERE scan_id=$scan AND parent_directory_id=$directory AND name_key=$key");
            AddBootstrapDirectory(exists, directory);
            exists.Parameters.AddWithValue("$key", key);
            await using (var reader = await exists.ExecuteReaderAsync(cancellationToken))
                if (await reader.ReadAsync(cancellationToken))
                {
                    if (reader.GetString(0) != entry.RawName || reader.GetBoolean(1) != entry.IsDirectory || reader.GetInt64(2) != entry.Size
                        || DateTimeOffset.Parse(reader.GetString(3), CultureInfo.InvariantCulture) != entry.ModifiedAt)
                        throw new InvalidDataException("Local siblings collide at one canonical Saturn name or changed during enumeration.");
                    continue;
                }
            await using var insert = SyncCommand(connection, transaction, "INSERT INTO bootstrap_scan_entries(scan_id,parent_directory_id,raw_name,name_key,relative_path,kind,size,modified_at,eligible) VALUES($scan,$directory,$raw,$key,$relative,$kind,$size,$modified,0) RETURNING entry_id");
            AddBootstrapDirectory(insert, directory);
            insert.Parameters.AddWithValue("$raw", entry.RawName);
            insert.Parameters.AddWithValue("$key", key);
            insert.Parameters.AddWithValue("$relative", relative);
            insert.Parameters.AddWithValue("$kind", entry.IsDirectory ? 1 : 0);
            insert.Parameters.AddWithValue("$size", entry.Size);
            insert.Parameters.AddWithValue("$modified", entry.ModifiedAt.ToUniversalTime().ToString("O"));
            maxId = Convert.ToInt64(await insert.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
            if (entry.IsDirectory) directories++; else { files++; bytes = checked(bytes + entry.Size); }
        }
        if (files + directories != 0)
        {
            await using var counts = SyncCommand(connection, transaction, "UPDATE bootstrap_scan_directories SET file_count=file_count+$files,directory_count=directory_count+$dirs,byte_count=byte_count+$bytes,max_entry_id=MAX(max_entry_id,$max) WHERE scan_id=$scan AND directory_id=$directory");
            AddBootstrapDirectory(counts, directory);
            counts.Parameters.AddWithValue("$files", files); counts.Parameters.AddWithValue("$dirs", directories);
            counts.Parameters.AddWithValue("$bytes", bytes); counts.Parameters.AddWithValue("$max", maxId);
            await counts.ExecuteNonQueryAsync(cancellationToken);
        }
        await CommitBootstrapWriteAsync(connection, transaction, effects, cancellationToken);
        await CheckBootstrapAdmissionAsync(cancellationToken);
    }

    public async Task CompleteBootstrapDirectoryAsync(BootstrapDirectory directory, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenBootstrapConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var effects = await BootstrapTotalChangesAsync(connection, transaction, cancellationToken);
        await RequireBootstrapDirectoryAsync(connection, transaction, directory, cancellationToken);
        await using var complete = SyncCommand(connection, transaction, """
            UPDATE bootstrap_scan_directories SET state=2 WHERE scan_id=$scan AND directory_id=$directory;
            UPDATE bootstrap_scan_runs SET
                files=files+(SELECT file_count FROM bootstrap_scan_directories WHERE scan_id=$scan AND directory_id=$directory),
                directories=directories+(SELECT directory_count FROM bootstrap_scan_directories WHERE scan_id=$scan AND directory_id=$directory),
                bytes=bytes+(SELECT byte_count FROM bootstrap_scan_directories WHERE scan_id=$scan AND directory_id=$directory),
                eligible_through=MAX(eligible_through,(SELECT max_entry_id FROM bootstrap_scan_directories WHERE scan_id=$scan AND directory_id=$directory))
                WHERE scan_id=$scan;
            """);
        AddBootstrapDirectory(complete, directory);
        await complete.ExecuteNonQueryAsync(cancellationToken);
        await CommitBootstrapWriteAsync(connection, transaction, effects, cancellationToken);
    }

    public async Task<IReadOnlyList<BootstrapEligibleEntry>> ListBootstrapEligibleEntriesAsync(string scanId, long afterEntryId = 0,
        int limit = BootstrapMetadataBatchLimit, CancellationToken cancellationToken = default)
    {
        if (limit is < 1 or > BootstrapMetadataBatchLimit || afterEntryId < 0) throw new ArgumentOutOfRangeException(nameof(limit));
        await using var connection = await OpenBootstrapConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: true);
        var scan = await ReadBootstrapStateAsync(connection, transaction, scanId, cancellationToken);
        if (scan is null || scan.Status is not (BootstrapScanStatus.Scanning or BootstrapScanStatus.EnumerationComplete)
            || !await BootstrapMappingMatchesAsync(connection, transaction, scan, cancellationToken)) return [];
        await using var command = SyncCommand(connection, transaction, """
            SELECT e.entry_id,e.parent_directory_id,e.relative_path,e.kind,e.size,e.modified_at
            FROM bootstrap_scan_entries e JOIN bootstrap_scan_runs r ON r.scan_id=e.scan_id
            JOIN bootstrap_scan_directories parent ON parent.scan_id=e.scan_id AND parent.directory_id=e.parent_directory_id
            WHERE e.scan_id=$scan AND e.entry_id>$after AND e.entry_id<=r.eligible_through AND parent.state=2 AND r.status IN (0,1) AND r.retired=0
              AND NOT EXISTS(SELECT 1 FROM bootstrap_scan_runs newer WHERE newer.mapping_id=r.mapping_id AND newer.generation>r.generation)
            ORDER BY e.entry_id LIMIT $limit;
            """);
        command.Parameters.AddWithValue("$scan", scanId);
        command.Parameters.AddWithValue("$after", afterEntryId);
        command.Parameters.AddWithValue("$limit", limit);
        var result = new List<BootstrapEligibleEntry>(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken)) result.Add(new(reader.GetInt64(0), reader.GetInt64(1), reader.GetString(2), reader.GetBoolean(3), reader.GetInt64(4), DateTimeOffset.Parse(reader.GetString(5), CultureInfo.InvariantCulture)));
        return result;
    }

    public async Task FinishBootstrapScanAsync(string scanId, string ownerToken, BootstrapScanStatus status, string? error = null, CancellationToken cancellationToken = default)
    {
        if (status == BootstrapScanStatus.Scanning || !Enum.IsDefined(status) || error is { Length: > 2048 }) throw new ArgumentException("Invalid scan termination.");
        await using var connection = await OpenBootstrapConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var effects = await BootstrapTotalChangesAsync(connection, transaction, cancellationToken);
        await RequireBootstrapOwnerAsync(connection, transaction, scanId, ownerToken, cancellationToken, validateMapping: status == BootstrapScanStatus.EnumerationComplete);
        if (status == BootstrapScanStatus.EnumerationComplete)
        {
            await using var pending = SyncCommand(connection, transaction, "SELECT 1 FROM bootstrap_scan_directories WHERE scan_id=$scan AND state IN (0,1) UNION ALL SELECT 1 FROM bootstrap_scan_directories WHERE scan_id=$scan AND state=2 AND refill_complete=0 LIMIT 1");
            pending.Parameters.AddWithValue("$scan", scanId);
            if (await pending.ExecuteScalarAsync(cancellationToken) is not null)
                throw new InvalidOperationException("An incomplete directory queue cannot complete enumeration.");
        }
        await using var finish = SyncCommand(connection, transaction, "UPDATE bootstrap_scan_runs SET status=$status,error=$error WHERE scan_id=$scan");
        finish.Parameters.AddWithValue("$scan", scanId);
        finish.Parameters.AddWithValue("$status", (int)status);
        finish.Parameters.AddWithValue("$error", (object?)error ?? DBNull.Value);
        await finish.ExecuteNonQueryAsync(cancellationToken);
        await CommitBootstrapWriteAsync(connection, transaction, effects, cancellationToken);
    }

    private static async Task InitializeBootstrapTablesAsync(SqliteConnection connection, SqliteTransaction transaction, CancellationToken cancellationToken)
    {
        await using (var table = SyncCommand(connection, transaction, "SELECT COUNT(*) FROM sqlite_master WHERE name='bootstrap_scan_format' AND type='table'"))
            if (Convert.ToInt64(await table.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture) != 0)
            {
                await using var version = SyncCommand(connection, transaction, "SELECT version FROM bootstrap_scan_format WHERE singleton=1");
                if (await version.ExecuteScalarAsync(cancellationToken) is not long number || number != 2) throw new InvalidDataException("Bootstrap scan format is unsupported.");
            }
        await ExecuteSyncAsync(connection, transaction, """
            CREATE TABLE IF NOT EXISTS bootstrap_scan_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS bootstrap_scan_runs(
                scan_id TEXT PRIMARY KEY,mapping_id TEXT NOT NULL,client_instance_id TEXT NOT NULL,source_root TEXT NOT NULL,owner_token TEXT NOT NULL,
                status INTEGER NOT NULL,created_at TEXT NOT NULL,generation INTEGER NOT NULL,files INTEGER NOT NULL DEFAULT 0,directories INTEGER NOT NULL DEFAULT 0,bytes INTEGER NOT NULL DEFAULT 0,error TEXT,
                eligible_through INTEGER NOT NULL DEFAULT 0,retired INTEGER NOT NULL DEFAULT 0,
                UNIQUE(mapping_id,generation));
            CREATE UNIQUE INDEX IF NOT EXISTS bootstrap_scan_one_active ON bootstrap_scan_runs(mapping_id) WHERE status=0;
            CREATE INDEX IF NOT EXISTS bootstrap_scan_retired ON bootstrap_scan_runs(mapping_id,retired,generation);
            CREATE TABLE IF NOT EXISTS bootstrap_scan_directories(
                directory_id INTEGER PRIMARY KEY AUTOINCREMENT,scan_id TEXT NOT NULL,relative_path TEXT NOT NULL,state INTEGER NOT NULL,
                file_count INTEGER NOT NULL DEFAULT 0,directory_count INTEGER NOT NULL DEFAULT 0,byte_count INTEGER NOT NULL DEFAULT 0,
                max_entry_id INTEGER NOT NULL DEFAULT 0,child_cursor INTEGER NOT NULL DEFAULT 0,refill_complete INTEGER NOT NULL DEFAULT 0,
                UNIQUE(scan_id,relative_path),FOREIGN KEY(scan_id) REFERENCES bootstrap_scan_runs(scan_id) ON DELETE CASCADE);
            CREATE INDEX IF NOT EXISTS bootstrap_scan_directory_queue ON bootstrap_scan_directories(scan_id,state,directory_id);
            CREATE INDEX IF NOT EXISTS bootstrap_scan_frontier_refill ON bootstrap_scan_directories(scan_id,state,refill_complete,directory_id);
            CREATE UNIQUE INDEX IF NOT EXISTS bootstrap_scan_directory_identity ON bootstrap_scan_directories(scan_id,directory_id);
            CREATE TABLE IF NOT EXISTS bootstrap_scan_entries(
                entry_id INTEGER PRIMARY KEY AUTOINCREMENT,scan_id TEXT NOT NULL,parent_directory_id INTEGER NOT NULL,
                raw_name TEXT NOT NULL,name_key TEXT NOT NULL,relative_path TEXT NOT NULL,kind INTEGER NOT NULL,size INTEGER NOT NULL,modified_at TEXT NOT NULL,eligible INTEGER NOT NULL,
                UNIQUE(scan_id,parent_directory_id,name_key),FOREIGN KEY(scan_id) REFERENCES bootstrap_scan_runs(scan_id) ON DELETE CASCADE,
                FOREIGN KEY(scan_id,parent_directory_id) REFERENCES bootstrap_scan_directories(scan_id,directory_id) ON DELETE CASCADE);
            CREATE INDEX IF NOT EXISTS bootstrap_scan_eligible ON bootstrap_scan_entries(scan_id,entry_id);
            CREATE INDEX IF NOT EXISTS bootstrap_scan_child_refill ON bootstrap_scan_entries(scan_id,parent_directory_id,kind,entry_id);
            INSERT INTO bootstrap_scan_format(singleton,version) VALUES(1,2) ON CONFLICT(singleton) DO NOTHING;
            """, cancellationToken);
    }

    private async Task<SqliteConnection> OpenBootstrapConnectionAsync(CancellationToken cancellationToken)
    {
        if (await PreflightBootstrapTablesAsync(cancellationToken) != 2) throw new InvalidDataException("Bootstrap scan v2 state must be initialized/migrated before work.");
        return await OpenSyncConnectionAsync(cancellationToken);
    }

    private async Task<int> PreflightBootstrapTablesAsync(CancellationToken cancellationToken, bool initializedProfileRequired = true)
    {
        if (!File.Exists(databasePath))
        {
            if (!initializedProfileRequired) return 0;
            throw new InvalidDataException("The profile must be initialized before bootstrap scanning.");
        }
        var legacyMain = await PreflightSyncStateAsync(verifyLegacyIntegrity: false, cancellationToken);
        if (legacyMain && initializedProfileRequired) throw new InvalidDataException("The profile must be initialized before bootstrap scanning.");
        try
        {
            await using var connection = new SqliteConnection(ReadOnlySyncConnectionString(databasePath));
            await connection.OpenAsync(cancellationToken);
            using var transaction = connection.BeginTransaction(deferred: true);
            await using var objects = SyncCommand(connection, transaction, "SELECT name,type FROM sqlite_master WHERE name GLOB 'bootstrap_scan_*'");
            var tables = new HashSet<string>(StringComparer.Ordinal);
            var indexes = new HashSet<string>(StringComparer.Ordinal);
            await using (var reader = await objects.ExecuteReaderAsync(cancellationToken))
                while (await reader.ReadAsync(cancellationToken))
                    (reader.GetString(1) == "table" ? tables : indexes).Add(reader.GetString(0));
            if (tables.Count == 0 && indexes.Count == 0) return 0;
            if (!tables.Contains("bootstrap_scan_format")) throw new InvalidDataException("An orphan or unknown bootstrap scan schema requires explicit recovery.");
            await using var version = SyncCommand(connection, transaction, "SELECT version FROM bootstrap_scan_format WHERE singleton=1");
            if (await version.ExecuteScalarAsync(cancellationToken) is not long number || number is not (1 or 2)) throw new InvalidDataException("Bootstrap scan format is unsupported.");
            var columnsByTable = new Dictionary<string, string[]>(StringComparer.Ordinal)
            {
                ["bootstrap_scan_format"] = ["singleton", "version"],
                ["bootstrap_scan_runs"] = ["scan_id", "mapping_id", "client_instance_id", "source_root", "owner_token", "status", "created_at", "generation", "files", "directories", "bytes", "error"],
                ["bootstrap_scan_directories"] = ["directory_id", "scan_id", "relative_path", "state"],
                ["bootstrap_scan_entries"] = ["entry_id", "scan_id", "parent_directory_id", "raw_name", "name_key", "relative_path", "kind", "size", "modified_at", "eligible"]
            };
            if (number == 2)
            {
                columnsByTable["bootstrap_scan_runs"] = [.. columnsByTable["bootstrap_scan_runs"], "eligible_through", "retired"];
                columnsByTable["bootstrap_scan_directories"] = [.. columnsByTable["bootstrap_scan_directories"], "file_count", "directory_count", "byte_count", "max_entry_id", "child_cursor", "refill_complete"];
            }
            foreach (var (table, columns) in columnsByTable)
            {
                if (!tables.Contains(table)) throw new InvalidDataException("Bootstrap scan schema is incomplete.");
                await using var info = SyncCommand(connection, transaction, $"PRAGMA table_info('{table}')");
                var actual = new HashSet<string>(StringComparer.Ordinal);
                await using (var reader = await info.ExecuteReaderAsync(cancellationToken))
                    while (await reader.ReadAsync(cancellationToken)) actual.Add(reader.GetString(1));
                if (columns.Any(column => !actual.Contains(column))) throw new InvalidDataException("Bootstrap scan schema is incompatible.");
            }
            if (new[] { "bootstrap_scan_one_active", "bootstrap_scan_directory_queue", "bootstrap_scan_directory_identity", "bootstrap_scan_eligible" }.Any(name => !indexes.Contains(name)))
                throw new InvalidDataException("Bootstrap scan safety indexes are incomplete.");
            if (tables.Count != columnsByTable.Count || indexes.Count != (number == 1 ? 4 : 7))
                throw new InvalidDataException("Bootstrap scan namespace has an unknown schema object.");
            await RequireBootstrapIndexAsync(connection, transaction, "bootstrap_scan_runs", "bootstrap_scan_one_active", ["mapping_id"], unique: true, partial: true, cancellationToken);
            await using (var predicate = SyncCommand(connection, transaction, "SELECT sql FROM sqlite_master WHERE name='bootstrap_scan_one_active' AND type='index'"))
            {
                var sql = await predicate.ExecuteScalarAsync(cancellationToken) as string;
                if (sql is null || !string.Concat(sql.Where(character => !char.IsWhiteSpace(character))).ToUpperInvariant().EndsWith("WHERESTATUS=0", StringComparison.Ordinal))
                    throw new InvalidDataException("Bootstrap ownership predicate is incompatible.");
            }
            await RequireBootstrapIndexAsync(connection, transaction, "bootstrap_scan_runs", null, ["mapping_id", "generation"], true, false, cancellationToken);
            await RequireBootstrapIndexAsync(connection, transaction, "bootstrap_scan_directories", null, ["scan_id", "relative_path"], true, false, cancellationToken);
            await RequireBootstrapIndexAsync(connection, transaction, "bootstrap_scan_directories", "bootstrap_scan_directory_identity", ["scan_id", "directory_id"], true, false, cancellationToken);
            await RequireBootstrapIndexAsync(connection, transaction, "bootstrap_scan_directories", "bootstrap_scan_directory_queue", ["scan_id", "state", "directory_id"], false, false, cancellationToken);
            await RequireBootstrapIndexAsync(connection, transaction, "bootstrap_scan_entries", null, ["scan_id", "parent_directory_id", "name_key"], true, false, cancellationToken);
            await RequireBootstrapIndexAsync(connection, transaction, "bootstrap_scan_entries", "bootstrap_scan_eligible", number == 1 ? ["scan_id", "eligible", "entry_id"] : ["scan_id", "entry_id"], false, false, cancellationToken);
            if (number == 2)
            {
                await RequireBootstrapIndexAsync(connection, transaction, "bootstrap_scan_entries", "bootstrap_scan_child_refill", ["scan_id", "parent_directory_id", "kind", "entry_id"], false, false, cancellationToken);
                await RequireBootstrapIndexAsync(connection, transaction, "bootstrap_scan_directories", "bootstrap_scan_frontier_refill", ["scan_id", "state", "refill_complete", "directory_id"], false, false, cancellationToken);
                await RequireBootstrapIndexAsync(connection, transaction, "bootstrap_scan_runs", "bootstrap_scan_retired", ["mapping_id", "retired", "generation"], false, false, cancellationToken);
            }
            await RequireBootstrapForeignKeyAsync(connection, transaction, "bootstrap_scan_directories", "bootstrap_scan_runs", ["scan_id"], ["scan_id"], cancellationToken);
            await RequireBootstrapForeignKeyAsync(connection, transaction, "bootstrap_scan_entries", "bootstrap_scan_runs", ["scan_id"], ["scan_id"], cancellationToken);
            await RequireBootstrapForeignKeyAsync(connection, transaction, "bootstrap_scan_entries", "bootstrap_scan_directories", ["scan_id", "parent_directory_id"], ["scan_id", "directory_id"], cancellationToken);
            return (int)number;
        }
        catch (SqliteException error) { throw new InvalidDataException("Bootstrap scan metadata cannot be safely inspected.", error); }
    }

    private static async Task RequireBootstrapIndexAsync(SqliteConnection connection, SqliteTransaction transaction, string table,
        string? requiredName, string[] columns, bool unique, bool partial, CancellationToken cancellationToken)
    {
        await using var indexes = SyncCommand(connection, transaction, "SELECT name FROM pragma_index_list($table) WHERE [unique]=$unique AND partial=$partial");
        indexes.Parameters.AddWithValue("$table", table);
        indexes.Parameters.AddWithValue("$unique", unique ? 1 : 0);
        indexes.Parameters.AddWithValue("$partial", partial ? 1 : 0);
        var candidates = new List<string>();
        await using (var reader = await indexes.ExecuteReaderAsync(cancellationToken))
            while (await reader.ReadAsync(cancellationToken))
                if (requiredName is null || reader.GetString(0) == requiredName) candidates.Add(reader.GetString(0));
        foreach (var candidate in candidates)
        {
            await using var info = SyncCommand(connection, transaction, "SELECT name FROM pragma_index_info($index) ORDER BY seqno");
            info.Parameters.AddWithValue("$index", candidate);
            var actual = new List<string>();
            await using (var reader = await info.ExecuteReaderAsync(cancellationToken))
                while (await reader.ReadAsync(cancellationToken)) actual.Add(reader.IsDBNull(0) ? "" : reader.GetString(0));
            if (actual.SequenceEqual(columns, StringComparer.Ordinal)) return;
        }
        throw new InvalidDataException("Bootstrap scan index semantics are incompatible.");
    }

    private static async Task RequireBootstrapForeignKeyAsync(SqliteConnection connection, SqliteTransaction transaction, string table,
        string parent, string[] from, string[] to, CancellationToken cancellationToken)
    {
        await using var command = SyncCommand(connection, transaction, "SELECT id,seq,[from],[to] FROM pragma_foreign_key_list($table) WHERE [table]=$parent AND on_delete='CASCADE' ORDER BY id,seq");
        command.Parameters.AddWithValue("$table", table);
        command.Parameters.AddWithValue("$parent", parent);
        var groups = new Dictionary<long, List<(string From, string To)>>();
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            while (await reader.ReadAsync(cancellationToken))
            {
                var id = reader.GetInt64(0);
                if (!groups.TryGetValue(id, out var pairs)) groups.Add(id, pairs = []);
                pairs.Add((reader.GetString(2), reader.GetString(3)));
            }
        if (!groups.Values.Any(pairs => pairs.Select(pair => pair.From).SequenceEqual(from, StringComparer.Ordinal)
            && pairs.Select(pair => pair.To).SequenceEqual(to, StringComparer.Ordinal)))
            throw new InvalidDataException("Bootstrap scan foreign key scope is incompatible.");
    }

    private static async Task<BootstrapScanState?> ReadBootstrapStateAsync(SqliteConnection connection, SqliteTransaction? transaction, string scanId, CancellationToken cancellationToken, bool legacy = false)
    {
        await using var command = SyncCommand(connection, transaction, "SELECT mapping_id,owner_token,source_root,status,files,directories,bytes,error,generation FROM bootstrap_scan_runs WHERE scan_id=$scan" + (legacy ? "" : " AND retired=0"));
        command.Parameters.AddWithValue("$scan", scanId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        var status = (BootstrapScanStatus)reader.GetInt32(3);
        if (!Enum.IsDefined(status) || reader.GetInt64(4) < 0 || reader.GetInt64(5) < 0 || reader.GetInt64(6) < 0 || reader.GetInt64(8) <= 0)
            throw new InvalidDataException("Bootstrap scan state is invalid.");
        return new(scanId, reader.GetString(0), reader.GetString(1), reader.GetString(2), status, reader.GetInt64(4), reader.GetInt64(5), reader.GetInt64(6), reader.IsDBNull(7) ? null : reader.GetString(7), reader.GetInt64(8));
    }

    private static async Task RequireBootstrapOwnerAsync(SqliteConnection connection, SqliteTransaction transaction, string scanId, string ownerToken, CancellationToken cancellationToken, bool validateMapping = true)
    {
        var scan = await ReadBootstrapStateAsync(connection, transaction, scanId, cancellationToken);
        if (scan is null || scan.OwnerToken != ownerToken || scan.Status != BootstrapScanStatus.Scanning)
            throw new InvalidOperationException("This scanner no longer owns the active generation.");
        if (!validateMapping) return;
        if (!await BootstrapMappingMatchesAsync(connection, transaction, scan, cancellationToken))
            throw new InvalidOperationException("The scan mapping is paused, removed or rebound.");
    }

    private static async Task<bool> BootstrapMappingMatchesAsync(SqliteConnection connection, SqliteTransaction transaction, BootstrapScanState scan, CancellationToken cancellationToken)
    {
        await using var enabled = SyncCommand(connection, transaction, "SELECT m.local_path FROM sync_mappings m JOIN bootstrap_scan_runs r ON r.mapping_id=m.mapping_id AND r.client_instance_id=m.client_instance_id WHERE r.scan_id=$scan AND m.enabled=1");
        enabled.Parameters.AddWithValue("$scan", scan.ScanId);
        return await enabled.ExecuteScalarAsync(cancellationToken) is string path
            && string.Equals(Path.TrimEndingDirectorySeparator(Path.GetFullPath(path)), scan.SourceRoot, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
    }

    private static async Task RequireBootstrapDirectoryAsync(SqliteConnection connection, SqliteTransaction transaction, BootstrapDirectory directory, CancellationToken cancellationToken)
    {
        await RequireBootstrapOwnerAsync(connection, transaction, directory.ScanId, directory.OwnerToken, cancellationToken);
        await using var command = SyncCommand(connection, transaction, "SELECT COUNT(*) FROM bootstrap_scan_directories WHERE scan_id=$scan AND directory_id=$directory AND state=1 AND relative_path=$relative");
        AddBootstrapDirectory(command, directory);
        command.Parameters.AddWithValue("$relative", directory.RelativePath);
        if (Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture) != 1) throw new InvalidOperationException("The directory is not owned for enumeration.");
    }

    private static void AddBootstrapDirectory(SqliteCommand command, BootstrapDirectory directory)
    {
        command.Parameters.AddWithValue("$scan", directory.ScanId);
        command.Parameters.AddWithValue("$directory", directory.DirectoryId);
    }
}
