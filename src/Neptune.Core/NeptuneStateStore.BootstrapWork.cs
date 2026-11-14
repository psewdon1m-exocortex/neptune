using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    internal string BootstrapExecutionDatabasePath => Path.GetFullPath(databasePath);
    internal Task<FileStream> AcquireBootstrapExecutionOwnerAsync(string mappingId, CancellationToken cancellationToken)
    {
        ValidateId(mappingId, nameof(mappingId)); cancellationToken.ThrowIfCancellationRequested();
        var key = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(mappingId)));
        return Task.FromResult(new FileStream(databasePath + ".bootstrap-execution-" + key + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None));
    }
    private const string BootstrapWorkColumns = "w.work_id,w.session_id,s.scan_id,s.generation,w.entry_id,w.operation_id,s.mapping_id,s.source_root,s.target_root,s.credential_scope,w.relative_path,w.kind,w.enumerated_size,w.modified_at,w.phase,w.spool_path,w.sealed_size,w.sha256,w.source_identity,w.attempt,w.transport_mode,w.upload_id,w.received_bytes,w.receipt,w.error,s.client_instance_id,s.expected_root_id,s.receipt_policy,w.intent_hash,w.request_path";
    private static string BootstrapWorkSelectColumns(int format) => BootstrapWorkColumns + (format == 3
        ? ",CASE WHEN w.native_capture IS NULL THEN NULL WHEN typeof(w.native_capture)='text' AND length(CAST(w.native_capture AS BLOB))<=32768 THEN w.native_capture ELSE '<invalid-native-capture>' END"
        : ",NULL");

    private async Task ValidateOptionalBootstrapWorkBeforeInitializeAsync(CancellationToken cancellationToken)
        => _ = await PreflightBootstrapWorkAsync(cancellationToken);

    private async Task<int> PreflightBootstrapWorkAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(databasePath)) return 0;
        try
        {
            await using var connection = new SqliteConnection(ReadOnlySyncConnectionString(databasePath));
            await connection.OpenAsync(cancellationToken);
            using var transaction = connection.BeginTransaction(deferred: true);
            await using var objects = SyncCommand(connection, transaction, "SELECT name,type FROM sqlite_master WHERE name GLOB 'bootstrap_work_*'");
            var names = new HashSet<string>();
            await using (var reader = await objects.ExecuteReaderAsync(cancellationToken))
                while (await reader.ReadAsync(cancellationToken)) names.Add(reader.GetString(0));
            if (names.Count == 0) return 0;
            if (!names.SetEquals(["bootstrap_work_format", "bootstrap_work_sessions", "bootstrap_work_items", "bootstrap_work_queue"]))
                throw new InvalidDataException("Unknown bootstrap execution schema requires explicit recovery.");
            await using var version = SyncCommand(connection, transaction, "SELECT version FROM bootstrap_work_format WHERE singleton=1");
            if (await version.ExecuteScalarAsync(cancellationToken) is not long value || value is not (1 or 2 or 3)) throw new InvalidDataException("Bootstrap execution format is unsupported.");
            foreach (var (table, columns) in new Dictionary<string, string[]> {
                ["bootstrap_work_format"] = ["singleton", "version"],
                ["bootstrap_work_sessions"] = ["session_id", "scan_id", "generation", "mapping_id", "client_instance_id", "source_root", "target_root", "credential_scope", "cursor"],
                ["bootstrap_work_items"] = ["work_id", "session_id", "entry_id", "operation_id", "relative_path", "kind", "enumerated_size", "modified_at", "phase", "spool_path", "sealed_size", "sha256", "source_identity", "attempt", "transport_mode", "upload_id", "received_bytes", "receipt", "error"] })
            {
                await using var info = SyncCommand(connection, transaction, $"PRAGMA table_info('{table}')");
                var actual = new HashSet<string>();
                await using (var reader = await info.ExecuteReaderAsync(cancellationToken)) while (await reader.ReadAsync(cancellationToken)) actual.Add(reader.GetString(1));
                var expectedColumns = value >= 2 ? columns.Concat(table == "bootstrap_work_sessions" ? ["expected_root_id", "receipt_policy"]
                    : table == "bootstrap_work_items" ? ["intent_hash", "request_path"] : Array.Empty<string>()) : columns;
                if (value == 3 && table == "bootstrap_work_items") expectedColumns = expectedColumns.Concat(["native_capture"]);
                if (!actual.SetEquals(expectedColumns)) throw new InvalidDataException("Bootstrap execution schema is incompatible.");
            }
            await RequireBootstrapIndexAsync(connection, transaction, "bootstrap_work_items", "bootstrap_work_queue", ["session_id", "phase", "work_id"], false, false, cancellationToken);
            await RequireBootstrapIndexAsync(connection, transaction, "bootstrap_work_items", null, ["session_id", "entry_id"], true, false, cancellationToken);
            await RequireBootstrapIndexAsync(connection, transaction, "bootstrap_work_items", null, ["operation_id"], true, false, cancellationToken);
            await RequireBootstrapForeignKeyAsync(connection, transaction, "bootstrap_work_items", "bootstrap_work_sessions", ["session_id"], ["session_id"], cancellationToken);
            return (int)value;
        }
        catch (SqliteException error) { throw new InvalidDataException("Bootstrap execution schema cannot be safely inspected.", error); }
    }

    internal async Task EnsureBootstrapWorkSchemaAsync(CancellationToken cancellationToken)
    {
        if (await PreflightBootstrapWorkAsync(cancellationToken) is 2 or 3) return;
        await using var migration = await AcquireSyncMigrationLockAsync(cancellationToken);
        var previousFormat = await PreflightBootstrapWorkAsync(cancellationToken);
        if (previousFormat is 2 or 3) return;
        await BackupBeforeBootstrapWorkAsync(cancellationToken,2);
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        if (previousFormat == 1)
        {
            // Existing outcomes never acquire authority retroactively. Default
            // policy/root/hash remain LegacyOnly/null under this additive step.
            await ExecuteSyncAsync(connection,transaction,"""
                ALTER TABLE bootstrap_work_sessions ADD COLUMN expected_root_id TEXT;
                ALTER TABLE bootstrap_work_sessions ADD COLUMN receipt_policy INTEGER NOT NULL DEFAULT 0 CHECK(receipt_policy BETWEEN 0 AND 2);
                ALTER TABLE bootstrap_work_items ADD COLUMN intent_hash TEXT;
                ALTER TABLE bootstrap_work_items ADD COLUMN request_path TEXT;
                UPDATE bootstrap_work_format SET version=2 WHERE singleton=1;
                """,cancellationToken);
        }
        else await ExecuteSyncAsync(connection, transaction, """
            CREATE TABLE bootstrap_work_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL);
            CREATE TABLE bootstrap_work_sessions(session_id TEXT PRIMARY KEY,scan_id TEXT NOT NULL,generation INTEGER NOT NULL,mapping_id TEXT NOT NULL,
                client_instance_id TEXT NOT NULL,source_root TEXT NOT NULL,target_root TEXT NOT NULL,credential_scope TEXT NOT NULL,cursor INTEGER NOT NULL DEFAULT 0,
                expected_root_id TEXT,receipt_policy INTEGER NOT NULL DEFAULT 0 CHECK(receipt_policy BETWEEN 0 AND 2));
            CREATE TABLE bootstrap_work_items(work_id INTEGER PRIMARY KEY AUTOINCREMENT,session_id TEXT NOT NULL,entry_id INTEGER NOT NULL,
                operation_id TEXT NOT NULL UNIQUE,relative_path TEXT NOT NULL,kind INTEGER NOT NULL,enumerated_size INTEGER NOT NULL,modified_at TEXT NOT NULL,
                phase INTEGER NOT NULL,spool_path TEXT,sealed_size INTEGER,sha256 TEXT,source_identity TEXT,attempt INTEGER NOT NULL DEFAULT 0,
                transport_mode TEXT,upload_id TEXT,received_bytes INTEGER NOT NULL DEFAULT 0,receipt TEXT,error TEXT,intent_hash TEXT,request_path TEXT,
                UNIQUE(session_id,entry_id),FOREIGN KEY(session_id) REFERENCES bootstrap_work_sessions(session_id) ON DELETE CASCADE);
            CREATE INDEX bootstrap_work_queue ON bootstrap_work_items(session_id,phase,work_id);
            INSERT INTO bootstrap_work_format VALUES(1,2);
            """, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    internal async Task<string> BeginBootstrapWorkSessionAsync(BootstrapScanState scan, string client, Uri targetRoot, string scope, BootstrapReceiptExpectation? expectation, CancellationToken cancellationToken)
    {
        await EnsureBootstrapWorkSchemaAsync(cancellationToken);
        ValidateId(scope, nameof(scope));
        expectation = expectation?.Validated();
        var id = Guid.NewGuid().ToString("N");
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        await RequireBootstrapOwnerAsync(connection, transaction, scan.ScanId, scan.OwnerToken, cancellationToken);
        await using var insert = SyncCommand(connection, transaction, "INSERT INTO bootstrap_work_sessions(session_id,scan_id,generation,mapping_id,client_instance_id,source_root,target_root,credential_scope,expected_root_id,receipt_policy) VALUES($id,$scan,$generation,$mapping,$client,$source,$target,$scope,$root,$policy)");
        insert.Parameters.AddWithValue("$id", id); insert.Parameters.AddWithValue("$scan", scan.ScanId); insert.Parameters.AddWithValue("$generation", scan.Generation);
        insert.Parameters.AddWithValue("$mapping", scan.MappingId); insert.Parameters.AddWithValue("$client", client); insert.Parameters.AddWithValue("$source", scan.SourceRoot);
        insert.Parameters.AddWithValue("$target", targetRoot.AbsoluteUri); insert.Parameters.AddWithValue("$scope", scope);
        insert.Parameters.AddWithValue("$root",(object?)expectation?.AssignedRootId ?? DBNull.Value);
        insert.Parameters.AddWithValue("$policy",(int)(expectation?.Policy ?? BootstrapReceiptPolicy.LegacyOnly));
        await insert.ExecuteNonQueryAsync(cancellationToken); await transaction.CommitAsync(cancellationToken);
        return id;
    }

    internal async Task EnqueueBootstrapWorkAsync(string sessionId, IReadOnlyList<BootstrapEligibleEntry> entries, CancellationToken cancellationToken)
    {
        if (entries.Count == 0) return;
        if (entries.Count > BootstrapMetadataBatchLimit) throw new ArgumentOutOfRangeException(nameof(entries));
        await CheckBootstrapAdmissionAsync(cancellationToken);
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        await RequireBootstrapWorkSessionAsync(connection, transaction, sessionId, cancellationToken);
        await using var targetQuery = SyncCommand(connection,transaction,"SELECT target_root FROM bootstrap_work_sessions WHERE session_id=$id");
        targetQuery.Parameters.AddWithValue("$id",sessionId);
        var targetRoot = new Uri((string)(await targetQuery.ExecuteScalarAsync(cancellationToken))!);
        foreach (var entry in entries)
        {
            await using var insert = SyncCommand(connection, transaction, "INSERT INTO bootstrap_work_items(session_id,entry_id,operation_id,relative_path,kind,enumerated_size,modified_at,phase,request_path) VALUES($session,$entry,$operation,$path,$kind,$size,$modified,0,$request) ON CONFLICT(session_id,entry_id) DO NOTHING");
            insert.Parameters.AddWithValue("$session", sessionId); insert.Parameters.AddWithValue("$entry", entry.EntryId);
            insert.Parameters.AddWithValue("$operation", Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(sessionId + "\n" + entry.EntryId.ToString(CultureInfo.InvariantCulture)))));
            insert.Parameters.AddWithValue("$path", entry.RelativePath); insert.Parameters.AddWithValue("$kind", entry.IsDirectory ? 1 : 0);
            insert.Parameters.AddWithValue("$size", entry.Size); insert.Parameters.AddWithValue("$modified", entry.ModifiedAt.ToString("O"));
            insert.Parameters.AddWithValue("$request",entry.IsDirectory ? DBNull.Value : BootstrapUploadReceiptParser.RequestPath(targetRoot,entry.RelativePath));
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }
        await using var cursor = SyncCommand(connection, transaction, "UPDATE bootstrap_work_sessions SET cursor=MAX(cursor,$cursor) WHERE session_id=$id");
        cursor.Parameters.AddWithValue("$id", sessionId); cursor.Parameters.AddWithValue("$cursor", entries[^1].EntryId);
        await cursor.ExecuteNonQueryAsync(cancellationToken); await transaction.CommitAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<BootstrapWorkRecord>> ListBootstrapWorkAsync(string sessionId, long afterWorkId = 0, int limit = 256, CancellationToken cancellationToken = default)
    {
        if (limit is < 1 or > 256 || afterWorkId < 0) throw new ArgumentOutOfRangeException(nameof(limit));
        var format = await PreflightBootstrapWorkAsync(cancellationToken);
        if (format == 0) return [];
        if (format == 1) { await EnsureBootstrapWorkSchemaAsync(cancellationToken); format = 2; }
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        await using var command = SyncCommand(connection, null, $"SELECT {BootstrapWorkSelectColumns(format)} FROM bootstrap_work_items w JOIN bootstrap_work_sessions s ON s.session_id=w.session_id WHERE w.session_id=$id AND w.work_id>$after ORDER BY w.work_id LIMIT $limit");
        command.Parameters.AddWithValue("$id", sessionId); command.Parameters.AddWithValue("$after", afterWorkId); command.Parameters.AddWithValue("$limit", limit);
        var result = new List<BootstrapWorkRecord>(limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken)) result.Add(ReadBootstrapWork(reader));
        return result;
    }

    internal async Task SealBootstrapWorkAsync(BootstrapWorkRecord work, BootstrapSealedSource sealedSource, CancellationToken cancellationToken)
    {
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        await RequireBootstrapWorkSessionAsync(connection, transaction, work.SessionId, cancellationToken);
        await using var update = SyncCommand(connection, transaction, "UPDATE bootstrap_work_items SET phase=1,spool_path=$spool,sealed_size=$size,sha256=$hash,source_identity=$identity WHERE work_id=$id AND operation_id=$operation AND phase=0 AND attempt=0");
        update.Parameters.AddWithValue("$spool", sealedSource.SpoolPath); update.Parameters.AddWithValue("$size", sealedSource.Size); update.Parameters.AddWithValue("$hash", sealedSource.Sha256);
        update.Parameters.AddWithValue("$identity", sealedSource.SourceIdentity); AddWorkIdentity(update, work);
        if (await update.ExecuteNonQueryAsync(cancellationToken) != 1) throw new InvalidOperationException("Work cannot change its sealed identity.");
        await transaction.CommitAsync(cancellationToken);
    }

    internal async Task BeginBootstrapWorkAttemptAsync(BootstrapWorkRecord work, CancellationToken cancellationToken)
    {
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        await RequireBootstrapWorkSessionAsync(connection, transaction, work.SessionId, cancellationToken);
        await using var update = SyncCommand(connection, transaction, "UPDATE bootstrap_work_items SET phase=2,attempt=attempt+1,error=NULL WHERE work_id=$id AND operation_id=$operation AND phase IN (1,2,3,7)");
        AddWorkIdentity(update, work);
        if (await update.ExecuteNonQueryAsync(cancellationToken) != 1) throw new InvalidOperationException("Only sealed/unresolved work may start transport.");
        await transaction.CommitAsync(cancellationToken);
    }

    // A received/published outcome survives pause/remap/new generation/cancel.
    // It describes the old immutable intent, not permission to start new I/O.
    internal async Task SaveBootstrapWorkCheckpointAsync(BootstrapWorkRecord work, BootstrapTransferCheckpoint checkpoint)
    {
        if (checkpoint.TransportMode is not ("session" or "put") || checkpoint.ReceivedBytes < 0 || checkpoint.ReceivedBytes > work.SealedSize
            || checkpoint.UploadId is not null && !BootstrapUploadReceiptParser.IsUuid(checkpoint.UploadId)
            || checkpoint.TransportMode == "put" && (checkpoint.UploadId is not null || checkpoint.IntentHash is not null)
            || checkpoint.IntentHash is not null && (!BootstrapUploadReceiptParser.IsDigest(checkpoint.IntentHash) || checkpoint.UploadId is null))
            throw new InvalidDataException("Checkpoint binding/offset is invalid.");
        await using var connection = await OpenSyncConnectionAsync(CancellationToken.None);
        await using var command = SyncCommand(connection, null, "UPDATE bootstrap_work_items SET transport_mode=$mode,upload_id=COALESCE(upload_id,$upload),intent_hash=COALESCE(intent_hash,$hash),received_bytes=$received,phase=CASE WHEN $received>0 THEN 3 ELSE phase END WHERE work_id=$id AND operation_id=$operation AND attempt>0 AND phase IN (2,3) AND $received>=received_bytes AND $received<=sealed_size AND (upload_id IS NULL OR upload_id=$upload) AND (intent_hash IS NULL OR intent_hash=$hash) AND (transport_mode IS NULL OR transport_mode=$mode OR transport_mode='session' AND $mode='put' AND upload_id IS NULL AND intent_hash IS NULL AND attempt=1 AND received_bytes=0)");
        command.Parameters.AddWithValue("$mode", checkpoint.TransportMode); command.Parameters.AddWithValue("$upload", (object?)checkpoint.UploadId ?? DBNull.Value);
        command.Parameters.AddWithValue("$received", checkpoint.ReceivedBytes); AddWorkIdentity(command, work);
        command.Parameters.AddWithValue("$hash",(object?)checkpoint.IntentHash ?? DBNull.Value);
        if (await command.ExecuteNonQueryAsync() != 1) throw new InvalidOperationException("Checkpoint no longer matches its sent intent.");
    }
    internal async Task FinishBootstrapWorkAsync(BootstrapWorkRecord work, BootstrapWorkPhase phase, string? receipt = null, string? error = null)
    {
        if (phase is not (BootstrapWorkPhase.Published or BootstrapWorkPhase.ObservedEqual or BootstrapWorkPhase.Conflict or BootstrapWorkPhase.Failed or BootstrapWorkPhase.Deferred)
            || phase is BootstrapWorkPhase.Published or BootstrapWorkPhase.ObservedEqual && (work.Attempt < 1 || receipt is null))
            throw new InvalidOperationException("Only a sent immutable intent may receive a publication outcome.");
        var format = await PreflightBootstrapWorkAsync(CancellationToken.None);
        await using var connection = await OpenSyncConnectionAsync(CancellationToken.None);
        using var transaction = connection.BeginTransaction(deferred:false);
        await using var read = SyncCommand(connection,transaction,$"SELECT {BootstrapWorkSelectColumns(format)} FROM bootstrap_work_items w JOIN bootstrap_work_sessions s ON s.session_id=w.session_id WHERE w.work_id=$id AND w.operation_id=$operation");
        AddWorkIdentity(read,work);
        BootstrapWorkRecord persisted;
        await using (var reader = await read.ExecuteReaderAsync())
        { if (!await reader.ReadAsync()) throw new InvalidDataException("Execution intent was lost."); persisted = ReadBootstrapWork(reader); }
        if (receipt is not null)
        {
            var parsed = JsonSerializer.Deserialize<BootstrapPublicationReceipt>(receipt) ?? throw new InvalidDataException("Publication receipt is empty.");
            BootstrapUploadReceiptParser.ValidateCommittedReceipt(parsed,persisted);
            if (persisted.Phase is BootstrapWorkPhase.Published or BootstrapWorkPhase.ObservedEqual && persisted.Receipt is not null)
            {
                var old = JsonSerializer.Deserialize<BootstrapPublicationReceipt>(persisted.Receipt)!;
                if (old with { ImmutableReceiptJson = null } != parsed with { ImmutableReceiptJson = null }) throw new InvalidDataException("Historical publication cannot change.");
            }
        }
        await using var command = SyncCommand(connection, transaction, "UPDATE bootstrap_work_items SET phase=$phase,receipt=COALESCE(receipt,$receipt),error=$error WHERE work_id=$id AND operation_id=$operation AND phase NOT IN (4,5) AND ($phase NOT IN (4,5) OR attempt>0 AND phase IN (2,3))");
        command.Parameters.AddWithValue("$phase", (int)phase); command.Parameters.AddWithValue("$receipt", (object?)receipt ?? DBNull.Value);
        command.Parameters.AddWithValue("$error", (object?)(error is { Length: > 2048 } ? error[..2048] : error) ?? DBNull.Value); AddWorkIdentity(command, work);
        await command.ExecuteNonQueryAsync();
        await transaction.CommitAsync();
    }
    internal async Task SaveBootstrapPostCommitDiagnosticAsync(BootstrapWorkRecord work, string error)
    {
        await using var connection = await OpenSyncConnectionAsync(CancellationToken.None);
        await using var command = SyncCommand(connection, null, "UPDATE bootstrap_work_items SET error=$error WHERE work_id=$id AND operation_id=$operation AND phase IN (4,5) AND receipt IS NOT NULL");
        command.Parameters.AddWithValue("$error", "post-commit: " + (error.Length > 2000 ? error[..2000] : error)); AddWorkIdentity(command, work);
        await command.ExecuteNonQueryAsync();
    }
    private static void AddWorkIdentity(SqliteCommand command, BootstrapWorkRecord work)
    { command.Parameters.AddWithValue("$id", work.WorkId); command.Parameters.AddWithValue("$operation", work.OperationId); }
    private static BootstrapWorkRecord ReadBootstrapWork(SqliteDataReader r)
    {
        string? S(int i) => r.IsDBNull(i) ? null : r.GetString(i);
        var phase = (BootstrapWorkPhase)r.GetInt32(14);
        if (!Enum.IsDefined(phase) || r.GetInt64(3) <= 0 || r.GetInt64(12) < 0 || r.GetInt64(22) < 0) throw new InvalidDataException("Execution work is corrupt.");
        var policy = (BootstrapReceiptPolicy)r.GetInt32(27);
        if (!Enum.IsDefined(policy) || S(26) is { } root && !BootstrapUploadReceiptParser.IsUuid(root)
            || S(28) is { } fingerprint && !BootstrapUploadReceiptParser.IsDigest(fingerprint)
            || policy != BootstrapReceiptPolicy.LegacyOnly && S(26) is null) throw new InvalidDataException("Execution receipt binding is corrupt.");
        return new(r.GetInt64(0), r.GetString(1), r.GetString(2), r.GetInt64(3), r.GetInt64(4), r.GetString(5), r.GetString(6), r.GetString(7), r.GetString(8), r.GetString(9), r.GetString(10), r.GetBoolean(11), r.GetInt64(12), DateTimeOffset.Parse(r.GetString(13), CultureInfo.InvariantCulture), phase,
            S(15), r.IsDBNull(16) ? null : r.GetInt64(16), S(17), S(18), r.GetInt32(19), S(20), S(21), r.GetInt64(22), S(23), S(24), S(25),S(26),policy,S(28),S(29),S(30));
    }
    private static async Task RequireBootstrapWorkSessionAsync(SqliteConnection connection, SqliteTransaction transaction, string session, CancellationToken ct)
    {
        await using var query = SyncCommand(connection, transaction, "SELECT s.source_root,m.local_path FROM bootstrap_work_sessions s JOIN sync_mappings m ON m.mapping_id=s.mapping_id AND m.client_instance_id=s.client_instance_id AND m.enabled=1 JOIN bootstrap_scan_runs r ON r.scan_id=s.scan_id AND r.generation=s.generation WHERE s.session_id=$id AND r.status IN (0,1,3) AND r.retired=0 AND NOT EXISTS(SELECT 1 FROM bootstrap_scan_runs newer WHERE newer.mapping_id=s.mapping_id AND newer.generation>s.generation)");
        query.Parameters.AddWithValue("$id", session);
        await using var reader = await query.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct) || !string.Equals(Path.TrimEndingDirectorySeparator(Path.GetFullPath(reader.GetString(0))), Path.TrimEndingDirectorySeparator(Path.GetFullPath(reader.GetString(1))), OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal))
            throw new InvalidOperationException("Execution mapping/generation is paused, removed, rebound or superseded.");
    }
}
