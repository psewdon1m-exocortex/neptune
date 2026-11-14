using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private const int SyncStateFormatVersion = 1;
    private const int SyncBatchLimit = 1000;
    private static readonly JsonSerializerOptions SyncStateJson = new(JsonSerializerDefaults.Web);

    private static async Task InitializeSyncStateAsync(SqliteConnection connection, SqliteTransaction transaction, CancellationToken cancellationToken)
    {
        await ExecuteSyncAsync(connection, transaction, "CREATE TABLE IF NOT EXISTS sync_state_format(singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL,migrated_at TEXT NOT NULL)", cancellationToken);
        await using (var version = SyncCommand(connection, transaction, "SELECT version FROM sync_state_format WHERE singleton=1"))
        {
            var existing = await version.ExecuteScalarAsync(cancellationToken);
            if (existing is not null && Convert.ToInt32(existing, CultureInfo.InvariantCulture) != SyncStateFormatVersion)
                throw new InvalidDataException("This Neptune build cannot write the profile's sync state format.");
        }
        await ExecuteSyncAsync(connection, transaction, """
            CREATE TABLE IF NOT EXISTS sync_state_roots (
                mapping_id TEXT PRIMARY KEY, client_instance_id TEXT NOT NULL, identity_json TEXT,
                server_origin TEXT, server_root_id TEXT,
                legacy_revision INTEGER NOT NULL DEFAULT 0 CHECK(legacy_revision>=0),
                invalidation_generation INTEGER NOT NULL DEFAULT 0 CHECK(invalidation_generation>=0),
                needs_reconciliation INTEGER NOT NULL DEFAULT 1 CHECK(needs_reconciliation IN (0,1)),
                UNIQUE(server_origin,server_root_id)
            );
            CREATE TABLE IF NOT EXISTS sync_state_entries (
                mapping_id TEXT NOT NULL, entry_id TEXT NOT NULL, parent_entry_id TEXT,
                name_key TEXT NOT NULL, local_generation INTEGER NOT NULL CHECK(local_generation>0),
                presence INTEGER NOT NULL, filesystem_id TEXT, local_json TEXT NOT NULL,
                PRIMARY KEY(mapping_id,entry_id),
                FOREIGN KEY(mapping_id) REFERENCES sync_state_roots(mapping_id),
                FOREIGN KEY(mapping_id,parent_entry_id) REFERENCES sync_state_entries(mapping_id,entry_id) DEFERRABLE INITIALLY DEFERRED
            );
            CREATE INDEX IF NOT EXISTS sync_state_entries_parent ON sync_state_entries(mapping_id,parent_entry_id,name_key);
            CREATE INDEX IF NOT EXISTS sync_state_entries_filesystem ON sync_state_entries(mapping_id,filesystem_id);
            CREATE UNIQUE INDEX IF NOT EXISTS sync_state_entries_live_name ON sync_state_entries(mapping_id,ifnull(parent_entry_id,''),name_key) WHERE presence=1;
            CREATE TABLE IF NOT EXISTS sync_state_baselines (
                mapping_id TEXT NOT NULL, entry_id TEXT NOT NULL, remote_node_id TEXT NOT NULL, baseline_json TEXT NOT NULL,
                PRIMARY KEY(mapping_id,entry_id),
                FOREIGN KEY(mapping_id,entry_id) REFERENCES sync_state_entries(mapping_id,entry_id)
            );
            CREATE INDEX IF NOT EXISTS sync_state_baselines_remote ON sync_state_baselines(mapping_id,remote_node_id);
            CREATE TABLE IF NOT EXISTS sync_state_remote (
                mapping_id TEXT NOT NULL, node_id TEXT NOT NULL, last_seq INTEGER NOT NULL CHECK(last_seq>=0),
                remote_json TEXT NOT NULL, PRIMARY KEY(mapping_id,node_id),
                FOREIGN KEY(mapping_id) REFERENCES sync_state_roots(mapping_id)
            );
            CREATE TABLE IF NOT EXISTS sync_state_dirty (
                mapping_id TEXT NOT NULL, entry_id TEXT NOT NULL, observed_generation INTEGER NOT NULL CHECK(observed_generation>0),
                reasons INTEGER NOT NULL, first_dirty INTEGER NOT NULL, due_at INTEGER NOT NULL,
                PRIMARY KEY(mapping_id,entry_id), FOREIGN KEY(mapping_id) REFERENCES sync_state_roots(mapping_id)
            );
            CREATE INDEX IF NOT EXISTS sync_state_dirty_due ON sync_state_dirty(mapping_id,due_at,entry_id);
            CREATE TABLE IF NOT EXISTS sync_state_cursors (
                mapping_id TEXT PRIMARY KEY, journal_id TEXT NOT NULL, position INTEGER NOT NULL CHECK(position>=0),
                valid INTEGER NOT NULL CHECK(valid IN (0,1)), captured_at TEXT NOT NULL,
                FOREIGN KEY(mapping_id) REFERENCES sync_state_roots(mapping_id)
            );
            CREATE TABLE IF NOT EXISTS sync_state_operations (
                operation_id TEXT PRIMARY KEY, mapping_id TEXT NOT NULL, entry_id TEXT NOT NULL,
                local_generation INTEGER NOT NULL, intent_json TEXT NOT NULL, fingerprint TEXT NOT NULL,
                stage INTEGER NOT NULL, invalidation_generation INTEGER NOT NULL, legacy_revision INTEGER NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 0, worker_id TEXT, lease_until TEXT, receipt_json TEXT, cancellation_fence_json TEXT,
                FOREIGN KEY(mapping_id,entry_id) REFERENCES sync_state_entries(mapping_id,entry_id)
            );
            CREATE INDEX IF NOT EXISTS sync_state_operations_pending ON sync_state_operations(mapping_id,stage,operation_id);
            CREATE TABLE IF NOT EXISTS sync_state_dependencies (
                operation_id TEXT NOT NULL, dependency_id TEXT NOT NULL CHECK(operation_id<>dependency_id),
                PRIMARY KEY(operation_id,dependency_id),
                FOREIGN KEY(operation_id) REFERENCES sync_state_operations(operation_id),
                FOREIGN KEY(dependency_id) REFERENCES sync_state_operations(operation_id)
            );
            INSERT INTO sync_state_roots(mapping_id,client_instance_id)
                SELECT mapping_id,client_instance_id FROM sync_mappings WHERE 1
                ON CONFLICT(mapping_id) DO NOTHING;
            CREATE TRIGGER IF NOT EXISTS sync_state_legacy_mapping_insert AFTER INSERT ON sync_mappings
            BEGIN
                INSERT INTO sync_state_roots(mapping_id,client_instance_id) VALUES(NEW.mapping_id,NEW.client_instance_id)
                    ON CONFLICT(mapping_id) DO UPDATE SET legacy_revision=legacy_revision+1,invalidation_generation=invalidation_generation+1,needs_reconciliation=1;
            END;
            CREATE TRIGGER IF NOT EXISTS sync_state_legacy_mapping_update AFTER UPDATE ON sync_mappings
            BEGIN
                UPDATE sync_state_roots SET legacy_revision=legacy_revision+1,invalidation_generation=invalidation_generation+1,needs_reconciliation=1 WHERE mapping_id=OLD.mapping_id;
                UPDATE sync_state_cursors SET valid=0 WHERE mapping_id=OLD.mapping_id;
            END;
            CREATE TRIGGER IF NOT EXISTS sync_state_legacy_mapping_delete AFTER DELETE ON sync_mappings
            BEGIN
                UPDATE sync_state_roots SET legacy_revision=legacy_revision+1,invalidation_generation=invalidation_generation+1,needs_reconciliation=1 WHERE mapping_id=OLD.mapping_id;
                UPDATE sync_state_cursors SET valid=0 WHERE mapping_id=OLD.mapping_id;
            END;
            CREATE TRIGGER IF NOT EXISTS sync_state_legacy_file_insert AFTER INSERT ON sync_files
            BEGIN
                UPDATE sync_state_roots SET legacy_revision=legacy_revision+1,invalidation_generation=invalidation_generation+1,needs_reconciliation=1 WHERE mapping_id=NEW.mapping_id;
                UPDATE sync_state_cursors SET valid=0 WHERE mapping_id=NEW.mapping_id;
            END;
            CREATE TRIGGER IF NOT EXISTS sync_state_legacy_file_update AFTER UPDATE ON sync_files
            BEGIN
                UPDATE sync_state_roots SET legacy_revision=legacy_revision+1,invalidation_generation=invalidation_generation+1,needs_reconciliation=1 WHERE mapping_id IN (OLD.mapping_id,NEW.mapping_id);
                UPDATE sync_state_cursors SET valid=0 WHERE mapping_id IN (OLD.mapping_id,NEW.mapping_id);
            END;
            CREATE TRIGGER IF NOT EXISTS sync_state_legacy_file_delete AFTER DELETE ON sync_files
            BEGIN
                UPDATE sync_state_roots SET legacy_revision=legacy_revision+1,invalidation_generation=invalidation_generation+1,needs_reconciliation=1 WHERE mapping_id=OLD.mapping_id;
                UPDATE sync_state_cursors SET valid=0 WHERE mapping_id=OLD.mapping_id;
            END;
            CREATE TRIGGER IF NOT EXISTS sync_state_legacy_checkpoint_insert AFTER INSERT ON sync_upload_checkpoints
            BEGIN
                UPDATE sync_state_roots SET legacy_revision=legacy_revision+1,invalidation_generation=invalidation_generation+1,needs_reconciliation=1;
                UPDATE sync_state_cursors SET valid=0;
            END;
            CREATE TRIGGER IF NOT EXISTS sync_state_legacy_checkpoint_update AFTER UPDATE ON sync_upload_checkpoints
            BEGIN
                UPDATE sync_state_roots SET legacy_revision=legacy_revision+1,invalidation_generation=invalidation_generation+1,needs_reconciliation=1;
                UPDATE sync_state_cursors SET valid=0;
            END;
            CREATE TRIGGER IF NOT EXISTS sync_state_legacy_checkpoint_delete AFTER DELETE ON sync_upload_checkpoints
            BEGIN
                UPDATE sync_state_roots SET legacy_revision=legacy_revision+1,invalidation_generation=invalidation_generation+1,needs_reconciliation=1;
                UPDATE sync_state_cursors SET valid=0;
            END;
            """, cancellationToken);
        await using var record = SyncCommand(connection, transaction, "INSERT INTO sync_state_format(singleton,version,migrated_at) VALUES(1,$version,$at) ON CONFLICT(singleton) DO NOTHING");
        record.Parameters.AddWithValue("$version", SyncStateFormatVersion);
        record.Parameters.AddWithValue("$at", DateTimeOffset.UtcNow.ToString("O"));
        await record.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<SyncRootState> RegisterSyncRootAsync(SyncRootIdentity identity, CancellationToken cancellationToken = default)
    {
        ValidateRootIdentity(identity);
        identity = identity with { ServerOrigin = new Uri(identity.ServerOrigin.GetLeftPart(UriPartial.Authority) + "/") };
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var root = await ReadSyncRootAsync(connection, transaction, identity.MappingId, cancellationToken);
        if (root.Identity is not null && root.Identity != identity) throw new InvalidOperationException("A sync root identity cannot be rebound without explicit reconciliation/migration.");
        await using var bind = SyncCommand(connection, transaction, """
            UPDATE sync_state_roots SET identity_json=$identity,server_origin=$origin,server_root_id=$root
            WHERE mapping_id=$mapping AND client_instance_id=$client
              AND EXISTS(SELECT 1 FROM sync_mappings WHERE mapping_id=$mapping AND client_instance_id=$client);
            """);
        bind.Parameters.AddWithValue("$identity", EncodeSync(identity));
        bind.Parameters.AddWithValue("$origin", identity.ServerOrigin.AbsoluteUri);
        bind.Parameters.AddWithValue("$root", identity.ServerRootId);
        bind.Parameters.AddWithValue("$mapping", identity.MappingId);
        bind.Parameters.AddWithValue("$client", identity.ClientInstanceId);
        try
        {
            if (await bind.ExecuteNonQueryAsync(cancellationToken) != 1) throw new InvalidOperationException("The mapping owner changed or the mapping was removed.");
        }
        catch (SqliteException error) when (error.SqliteErrorCode == 19)
        {
            throw new InvalidOperationException("Another mapping in this profile already owns this Saturn root.", error);
        }
        await transaction.CommitAsync(cancellationToken);
        return root with { Identity = identity };
    }

    public async Task<SyncRootState> GetSyncRootStateAsync(string mappingId, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        return await ReadSyncRootAsync(connection, null, mappingId, cancellationToken);
    }

    public async Task ObserveSyncLocalEntriesAsync(string mappingId, IReadOnlyList<SyncLocalEntry> entries, CancellationToken cancellationToken = default)
    {
        ValidateSyncBatch(entries);
        foreach (var entry in entries) ValidateLocalEntry(entry);
        if (entries.Select(entry => entry.EntryId).Distinct(StringComparer.Ordinal).Count() != entries.Count) throw new ArgumentException("A metadata batch cannot redefine one entry twice.");
        var batch = entries.ToDictionary(entry => entry.EntryId, StringComparer.Ordinal);
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        await ReadBoundSyncRootAsync(connection, transaction, mappingId, cancellationToken);
        foreach (var entry in entries)
        {
            if (entry.ParentEntryId is not null)
            {
                var parent = batch.GetValueOrDefault(entry.ParentEntryId)
                    ?? await ReadSyncJsonAsync<SyncLocalEntry>(connection, transaction, "sync_state_entries", "local_json", "entry_id", mappingId, entry.ParentEntryId, cancellationToken);
                if (parent is null || parent.Kind != SyncEntryKind.Directory || parent.Presence != SyncEntryPresence.Present)
                    throw new InvalidOperationException("A local parent must be an observed present directory.");
                var ancestor = parent;
                var visited = new HashSet<string>(StringComparer.Ordinal) { entry.EntryId };
                while (ancestor is not null && batch.TryGetValue(ancestor.EntryId, out var batchAncestor))
                {
                    if (!visited.Add(ancestor.EntryId)) throw new InvalidOperationException("Local parent references cannot form a cycle.");
                    ancestor = batchAncestor.ParentEntryId is null ? null : batch.GetValueOrDefault(batchAncestor.ParentEntryId);
                }
                await using var cycle = SyncCommand(connection, transaction, """
                    WITH RECURSIVE ancestors(id,parent) AS (
                        SELECT entry_id,parent_entry_id FROM sync_state_entries WHERE mapping_id=$mapping AND entry_id=$parent
                        UNION SELECT e.entry_id,e.parent_entry_id FROM sync_state_entries e JOIN ancestors a ON e.entry_id=a.parent WHERE e.mapping_id=$mapping
                    ) SELECT COUNT(*) FROM ancestors WHERE id=$entry OR parent=$entry;
                    """);
                AddSyncKey(cycle, mappingId, entry.EntryId);
                cycle.Parameters.AddWithValue("$parent", entry.ParentEntryId);
                if (Convert.ToInt64(await cycle.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture) != 0) throw new InvalidOperationException("Local parent references cannot form a cycle.");
            }
            var current = await ReadSyncJsonAsync<SyncLocalEntry>(connection, transaction, "sync_state_entries", "local_json", "entry_id", mappingId, entry.EntryId, cancellationToken);
            if (current is not null && (entry.LocalGeneration < current.LocalGeneration
                || entry.LocalGeneration == current.LocalGeneration && current != entry))
                throw new InvalidOperationException("Local generations are monotonic and immutable once observed.");
            if (current == entry) continue; // Re-observation is not a new mutation.
            await using var command = SyncCommand(connection, transaction, """
                INSERT INTO sync_state_entries(mapping_id,entry_id,parent_entry_id,name_key,local_generation,presence,filesystem_id,local_json)
                VALUES($mapping,$entry,$parent,$name,$generation,$presence,$filesystem,$json)
                ON CONFLICT(mapping_id,entry_id) DO UPDATE SET parent_entry_id=excluded.parent_entry_id,name_key=excluded.name_key,
                    local_generation=excluded.local_generation,presence=excluded.presence,filesystem_id=excluded.filesystem_id,local_json=excluded.local_json;
                """);
            AddSyncKey(command, mappingId, entry.EntryId);
            command.Parameters.AddWithValue("$parent", (object?)entry.ParentEntryId ?? DBNull.Value);
            command.Parameters.AddWithValue("$name", entry.NameKey);
            command.Parameters.AddWithValue("$generation", entry.LocalGeneration);
            command.Parameters.AddWithValue("$presence", (int)entry.Presence);
            command.Parameters.AddWithValue("$filesystem", (object?)entry.FileSystemId ?? DBNull.Value);
            command.Parameters.AddWithValue("$json", EncodeSync(entry));
            await command.ExecuteNonQueryAsync(cancellationToken);
            await UpsertSyncDirtyAsync(connection, transaction, mappingId, new(entry.EntryId, entry.LocalGeneration, SyncDirtyReason.LocalChange, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow), cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<SyncEntryState> GetSyncEntryStateAsync(string mappingId, string entryId, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: true);
        var local = await ReadSyncJsonAsync<SyncLocalEntry>(connection, transaction, "sync_state_entries", "local_json", "entry_id", mappingId, entryId, cancellationToken);
        var baseline = await ReadSyncJsonAsync<SyncBaseline>(connection, transaction, "sync_state_baselines", "baseline_json", "entry_id", mappingId, entryId, cancellationToken);
        var remote = baseline is null ? null : await ReadSyncJsonAsync<SyncRemoteEntry>(connection, transaction, "sync_state_remote", "remote_json", "node_id", mappingId, baseline.RemoteNodeId, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return new(local, baseline, remote);
    }

    public async Task ApplySyncRemoteEntriesAsync(string mappingId, string rootEpoch, IReadOnlyList<SyncRemoteEntry> entries, CancellationToken cancellationToken = default)
    {
        ValidateSyncBatch(entries);
        foreach (var entry in entries) ValidateRemoteEntry(entry);
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var root = await ReadBoundSyncRootAsync(connection, transaction, mappingId, cancellationToken);
        if (root.Identity!.RootEpoch != rootEpoch) throw new InvalidOperationException("Remote versions from another root epoch cannot be applied.");
        foreach (var entry in entries)
            if (await UpsertSyncRemoteAsync(connection, transaction, mappingId, entry, cancellationToken))
                await MarkSyncRemoteDirtyAsync(connection, transaction, mappingId, entry.NodeId, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task CaptureSyncJournalAsync(string mappingId, SyncJournalCursor? expected, SyncJournalCursor next,
        IReadOnlyList<SyncDirtyHint> hints, CancellationToken cancellationToken = default)
    {
        ValidateSyncBatch(hints);
        ValidateCursor(next);
        foreach (var hint in hints) ValidateHint(hint);
        if (expected is not null && (next.JournalId == expected.JournalId && next.Position < expected.Position
            || next.JournalId != expected.JournalId && next.Valid)) throw new InvalidOperationException("Journal continuity must be re-established after a reset.");
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        await ReadBoundSyncRootAsync(connection, transaction, mappingId, cancellationToken);
        var current = await ReadSyncCursorAsync(connection, transaction, mappingId, cancellationToken);
        if (current != expected) throw new InvalidOperationException("The durable journal cursor changed; capture must be read again.");
        foreach (var hint in hints) await UpsertSyncDirtyAsync(connection, transaction, mappingId, hint, cancellationToken);
        await using var cursor = SyncCommand(connection, transaction, """
            INSERT INTO sync_state_cursors(mapping_id,journal_id,position,valid,captured_at) VALUES($mapping,$journal,$position,$valid,$at)
            ON CONFLICT(mapping_id) DO UPDATE SET journal_id=excluded.journal_id,position=excluded.position,valid=excluded.valid,captured_at=excluded.captured_at;
            """);
        cursor.Parameters.AddWithValue("$mapping", mappingId);
        cursor.Parameters.AddWithValue("$journal", next.JournalId);
        cursor.Parameters.AddWithValue("$position", next.Position);
        cursor.Parameters.AddWithValue("$valid", next.Valid ? 1 : 0);
        cursor.Parameters.AddWithValue("$at", DateTimeOffset.UtcNow.ToString("O"));
        await cursor.ExecuteNonQueryAsync(cancellationToken);
        if (!next.Valid) await InvalidateSyncRootAsync(connection, transaction, mappingId, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<SyncJournalCursor?> GetSyncJournalCursorAsync(string mappingId, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        return await ReadSyncCursorAsync(connection, null, mappingId, cancellationToken);
    }

    public async Task<IReadOnlyList<SyncDirtyHint>> ListSyncDirtyEntriesAsync(string mappingId, int limit = SyncBatchLimit, CancellationToken cancellationToken = default)
    {
        ValidateSyncBatch(limit);
        var result = new List<SyncDirtyHint>();
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        await using var command = SyncCommand(connection, null, "SELECT entry_id,observed_generation,reasons,first_dirty,due_at FROM sync_state_dirty WHERE mapping_id=$mapping ORDER BY due_at,entry_id LIMIT $limit");
        command.Parameters.AddWithValue("$mapping", mappingId);
        command.Parameters.AddWithValue("$limit", limit);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken)) result.Add(new(reader.GetString(0), reader.GetInt64(1), (SyncDirtyReason)reader.GetInt32(2), DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(3)), DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(4))));
        return result;
    }

    public async Task<SyncDurableOperation> PlanSyncOperationAsync(SyncOperationIntent intent, CancellationToken cancellationToken = default)
    {
        ValidateIntent(intent);
        intent = intent with { Dependencies = intent.Dependencies.Order(StringComparer.Ordinal).ToArray() };
        var intentJson = EncodeSync(intent);
        var fingerprint = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes("neptune.local-sync-intent.v1\n" + intentJson)));
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var root = await ReadBoundSyncRootAsync(connection, transaction, intent.MappingId, cancellationToken);
        var existing = await ReadSyncOperationAsync(connection, transaction, intent.OperationId, cancellationToken);
        if (existing is not null)
        {
            if (existing.Fingerprint != fingerprint) throw new InvalidOperationException("The operation ID is already bound to another immutable intent.");
            return existing;
        }
        if (root.Identity!.RootEpoch != intent.RootEpoch || root.Identity.RulesVersion != intent.RulesVersion)
            throw new InvalidOperationException("Operation epoch or rules do not match the bound root.");
        var entry = await ReadSyncJsonAsync<SyncLocalEntry>(connection, transaction, "sync_state_entries", "local_json", "entry_id", intent.MappingId, intent.EntryId, cancellationToken)
            ?? throw new InvalidOperationException("The operation requires an observed local entry.");
        if (entry.LocalGeneration != intent.LocalGeneration || entry.Presence != SyncEntryPresence.Present
            || entry.RulesVersion != intent.RulesVersion || entry.RawName != intent.RawName || entry.NameKey != intent.NameKey || entry.Size != intent.Size
            || intent.Kind == SyncIntentKind.EnsureDirectory && entry.Kind != SyncEntryKind.Directory
            || intent.Kind != SyncIntentKind.EnsureDirectory && (entry.Kind != SyncEntryKind.File || entry.HashGeneration != entry.LocalGeneration || entry.Sha256 != intent.Sha256))
            throw new InvalidOperationException("The intent is not the sealed observed local generation.");
        if (intent.Expected.Kind == SyncExpectedKind.Version)
        {
            var remote = await ReadSyncJsonAsync<SyncRemoteEntry>(connection, transaction, "sync_state_remote", "remote_json", "node_id", intent.MappingId, intent.Expected.NodeId!, cancellationToken);
            if (remote is null || !remote.Authoritative || remote.Deleted || remote.Version != intent.Expected.Version)
                throw new InvalidOperationException("The destination is not authoritatively observed at the expected version.");
            if (intent.Kind == SyncIntentKind.AdoptEqual)
            {
                if (remote.Kind != entry.Kind || remote.Size != entry.Size || remote.Sha256 != entry.Sha256)
                    throw new InvalidOperationException("Adoption requires equal verified content.");
            }
            else
            {
                var baseline = await ReadSyncJsonAsync<SyncBaseline>(connection, transaction, "sync_state_baselines", "baseline_json", "entry_id", intent.MappingId, intent.EntryId, cancellationToken);
                if (baseline is null || baseline.RootEpoch != intent.RootEpoch || baseline.RemoteNodeId != intent.Expected.NodeId
                    || baseline.RemoteVersion != intent.Expected.Version || baseline.InvalidationGeneration != root.InvalidationGeneration)
                    throw new InvalidOperationException("An overwrite must use the agreed baseline, never a fresh remote version.");
            }
        }
        foreach (var dependency in intent.Dependencies)
        {
            var parent = await ReadSyncOperationAsync(connection, transaction, dependency, cancellationToken);
            if (parent is null || parent.Intent.MappingId != intent.MappingId) throw new InvalidOperationException("Dependencies must be existing operations of the same root.");
        }
        await using var insert = SyncCommand(connection, transaction, """
            INSERT INTO sync_state_operations(operation_id,mapping_id,entry_id,local_generation,intent_json,fingerprint,stage,invalidation_generation,legacy_revision)
            VALUES($op,$mapping,$entry,$generation,$json,$fingerprint,$stage,$invalidation,$legacy);
            """);
        insert.Parameters.AddWithValue("$op", intent.OperationId);
        AddSyncKey(insert, intent.MappingId, intent.EntryId);
        insert.Parameters.AddWithValue("$generation", intent.LocalGeneration);
        insert.Parameters.AddWithValue("$json", intentJson);
        insert.Parameters.AddWithValue("$fingerprint", fingerprint);
        insert.Parameters.AddWithValue("$stage", (int)SyncOperationStage.LocalPlanned);
        insert.Parameters.AddWithValue("$invalidation", root.InvalidationGeneration);
        insert.Parameters.AddWithValue("$legacy", root.LegacyRevision);
        await insert.ExecuteNonQueryAsync(cancellationToken);
        foreach (var dependency in intent.Dependencies)
        {
            await using var add = SyncCommand(connection, transaction, "INSERT INTO sync_state_dependencies(operation_id,dependency_id) VALUES($op,$dependency)");
            add.Parameters.AddWithValue("$op", intent.OperationId);
            add.Parameters.AddWithValue("$dependency", dependency);
            await add.ExecuteNonQueryAsync(cancellationToken);
        }
        await transaction.CommitAsync(cancellationToken);
        return new(intent, fingerprint, SyncOperationStage.LocalPlanned, root.InvalidationGeneration, root.LegacyRevision, 0, null, null, null);
    }

    public async Task<SyncDurableOperation?> GetSyncOperationAsync(string operationId, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        return await ReadSyncOperationAsync(connection, null, operationId, cancellationToken);
    }

    public async Task<SyncDurableOperation> BeginSyncOperationAsync(string operationId, string workerId, DateTimeOffset leaseUntil, CancellationToken cancellationToken = default)
    {
        ValidateId(workerId, nameof(workerId));
        if (leaseUntil <= DateTimeOffset.UtcNow || leaseUntil > DateTimeOffset.UtcNow.AddMinutes(5)) throw new ArgumentOutOfRangeException(nameof(leaseUntil));
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var operation = await ReadSyncOperationAsync(connection, transaction, operationId, cancellationToken) ?? throw new InvalidOperationException("No durable operation exists.");
        var root = await ReadBoundSyncRootAsync(connection, transaction, operation.Intent.MappingId, cancellationToken);
        if (!root.MappingEnabled || root.InvalidationGeneration != operation.InvalidationGeneration || root.LegacyRevision != operation.LegacyRevision)
            throw new InvalidOperationException("Paused, changed or uncertain root work cannot begin before reconciliation.");
        if (operation.Stage is not (SyncOperationStage.LocalPlanned or SyncOperationStage.Ready))
            throw new InvalidOperationException("The operation cannot be sent; an uncertain outcome requires server status first.");
        await using var dependencies = SyncCommand(connection, transaction, """
            SELECT COUNT(*) FROM sync_state_dependencies d JOIN sync_state_operations o ON o.operation_id=d.dependency_id
            WHERE d.operation_id=$op AND o.stage<>$acked;
            """);
        dependencies.Parameters.AddWithValue("$op", operationId);
        dependencies.Parameters.AddWithValue("$acked", (int)SyncOperationStage.ClientAcked);
        if (Convert.ToInt64(await dependencies.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture) != 0)
            throw new InvalidOperationException("Operation dependencies are not acknowledged.");
        await SetSyncStageAsync(connection, transaction, operationId, SyncOperationStage.Sending, cancellationToken, workerId, leaseUntil, true);
        await transaction.CommitAsync(cancellationToken);
        return operation with { Stage = SyncOperationStage.Sending, Attempt = operation.Attempt + 1, WorkerId = workerId, LeaseUntil = leaseUntil };
    }

    public async Task MarkSyncOperationOutcomeUnknownAsync(string operationId, CancellationToken cancellationToken = default)
    {
        await ChangeSyncOperationAsync(operationId, operation => operation.Stage == SyncOperationStage.Sending
            ? SyncOperationStage.OutcomeUnknown : throw new InvalidOperationException("Only a sent operation can have an unknown outcome."), cancellationToken);
    }

    public async Task CancelSyncOperationAsync(string operationId, CancellationToken cancellationToken = default)
    {
        await ChangeSyncOperationAsync(operationId, operation => operation.Stage switch
        {
            SyncOperationStage.ClientAcked => throw new InvalidOperationException("A published operation cannot be cancelled locally."),
            SyncOperationStage.Cancelled => SyncOperationStage.Cancelled,
            SyncOperationStage.CancelRequested => SyncOperationStage.CancelRequested,
            _ when operation.Attempt == 0 => SyncOperationStage.Cancelled,
            _ => SyncOperationStage.CancelRequested,
        }, cancellationToken);
    }

    public async Task ConfirmSyncOperationAbsentAsync(string operationId, bool cancelled, CancellationToken cancellationToken = default)
    {
        // The future adapter must have a durable immutable-ID registry/status
        // contract before allowing same-ID retry. Absence is a point-in-time
        // observation; a delayed request may still arrive and must deduplicate.
        // It never proves cancellation: only a durable fence can establish that.
        if (cancelled) throw new InvalidOperationException("Sent cancellation requires an authoritative durable server fence proof.");
        await ChangeSyncOperationAsync(operationId, operation =>
        {
            if (operation.Stage is not (SyncOperationStage.OutcomeUnknown or SyncOperationStage.CancelRequested))
                throw new InvalidOperationException("An absence status applies only to an uncertain or cancelling operation.");
            return operation.Stage == SyncOperationStage.CancelRequested ? SyncOperationStage.CancelRequested : SyncOperationStage.Ready;
        }, cancellationToken);
    }

    public async Task ConfirmSyncCancellationFenceAsync(SyncCancellationFence proof, CancellationToken cancellationToken = default)
    {
        ValidateId(proof.FenceId, nameof(proof.FenceId));
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var operation = await ReadSyncOperationAsync(connection, transaction, proof.OperationId, cancellationToken) ?? throw new InvalidOperationException("No durable operation exists.");
        var root = await ReadBoundSyncRootAsync(connection, transaction, operation.Intent.MappingId, cancellationToken);
        if (proof.ServerRootId != root.Identity!.ServerRootId || proof.RootEpoch != root.Identity.RootEpoch
            || proof.RootEpoch != operation.Intent.RootEpoch || proof.IntentFingerprint != operation.Fingerprint || operation.Attempt == 0)
            throw new InvalidDataException("The cancellation fence does not match the sent immutable root/operation.");
        if (operation.Stage == SyncOperationStage.Cancelled)
        {
            if (operation.CancellationFence != proof) throw new InvalidDataException("Another cancellation fence was returned for this operation.");
            return;
        }
        if (operation.Stage != SyncOperationStage.CancelRequested) throw new InvalidOperationException("Only requested sent cancellation can accept a server fence.");
        await using var cancel = SyncCommand(connection, transaction, "UPDATE sync_state_operations SET stage=$cancelled,cancellation_fence_json=$proof,worker_id=NULL,lease_until=NULL WHERE operation_id=$op");
        cancel.Parameters.AddWithValue("$cancelled", (int)SyncOperationStage.Cancelled);
        cancel.Parameters.AddWithValue("$proof", EncodeSync(proof));
        cancel.Parameters.AddWithValue("$op", proof.OperationId);
        await cancel.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<int> RecoverExpiredSyncOperationsAsync(string mappingId, DateTimeOffset now, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        await using var command = SyncCommand(connection, transaction, """
            UPDATE sync_state_operations SET stage=$unknown,worker_id=NULL,lease_until=NULL
            WHERE mapping_id=$mapping AND stage=$sending AND lease_until<=$now;
            """);
        command.Parameters.AddWithValue("$unknown", (int)SyncOperationStage.OutcomeUnknown);
        command.Parameters.AddWithValue("$sending", (int)SyncOperationStage.Sending);
        command.Parameters.AddWithValue("$mapping", mappingId);
        command.Parameters.AddWithValue("$now", now.ToUniversalTime().ToString("O"));
        var count = await command.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return count;
    }

    public async Task ApplySyncPublishedReceiptAsync(SyncPublicationReceipt receipt, CancellationToken cancellationToken = default)
    {
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var operation = await ReadSyncOperationAsync(connection, transaction, receipt.OperationId, cancellationToken) ?? throw new InvalidOperationException("The receipt has no durable local intent.");
        var root = await ReadBoundSyncRootAsync(connection, transaction, operation.Intent.MappingId, cancellationToken);
        var intent = operation.Intent;
        if (!receipt.Published || receipt.Durability is not ("verified_readback" or "verified_storage_agent")
            || receipt.ServerRootId != root.Identity!.ServerRootId || receipt.RootEpoch != root.Identity.RootEpoch
            || receipt.RootEpoch != intent.RootEpoch || receipt.IntentFingerprint != operation.Fingerprint
            || receipt.LocalGeneration != intent.LocalGeneration || receipt.Sha256 != intent.Sha256 || receipt.Size != intent.Size
            || intent.Expected.Kind == SyncExpectedKind.Version && receipt.NodeId != intent.Expected.NodeId)
            throw new InvalidDataException("The receipt does not prove publication of the sealed root/operation/generation/content.");
        ValidateId(receipt.NodeId, nameof(receipt.NodeId));
        ValidateId(receipt.Version, nameof(receipt.Version));
        if (receipt.PublishedSequence <= 0) throw new InvalidDataException("The publication sequence is invalid.");
        if (operation.Stage == SyncOperationStage.ClientAcked)
        {
            if (operation.Receipt != receipt) throw new InvalidDataException("A different result was returned for an acknowledged operation.");
            return;
        }
        if (operation.Attempt == 0 || operation.Stage is not (SyncOperationStage.Sending or SyncOperationStage.OutcomeUnknown or SyncOperationStage.CancelRequested or SyncOperationStage.Ready or SyncOperationStage.RetryWait))
            throw new InvalidDataException("Only previously sent or uncertain operations can acknowledge a publication receipt.");
        var baseline = new SyncBaseline(intent.LocalGeneration, receipt.NodeId, receipt.Version, receipt.RootEpoch,
            receipt.PublishedSequence, receipt.Sha256, receipt.Size, intent.Kind == SyncIntentKind.EnsureDirectory ? SyncEntryKind.Directory : SyncEntryKind.File,
            intent.ParentRemoteNodeId, intent.RawName, operation.InvalidationGeneration);
        var previous = await ReadSyncJsonAsync<SyncBaseline>(connection, transaction, "sync_state_baselines", "baseline_json", "entry_id", intent.MappingId, intent.EntryId, cancellationToken);
        if (previous is null || previous.PublishedSequence < baseline.PublishedSequence
            || previous.PublishedSequence == baseline.PublishedSequence && previous.AgreedLocalGeneration <= baseline.AgreedLocalGeneration)
        {
            await using var agree = SyncCommand(connection, transaction, "INSERT INTO sync_state_baselines(mapping_id,entry_id,remote_node_id,baseline_json) VALUES($mapping,$entry,$node,$json) ON CONFLICT(mapping_id,entry_id) DO UPDATE SET remote_node_id=excluded.remote_node_id,baseline_json=excluded.baseline_json");
            AddSyncKey(agree, intent.MappingId, intent.EntryId);
            agree.Parameters.AddWithValue("$node", receipt.NodeId);
            agree.Parameters.AddWithValue("$json", EncodeSync(baseline));
            await agree.ExecuteNonQueryAsync(cancellationToken);
        }
        var remote = new SyncRemoteEntry(receipt.NodeId, intent.ParentRemoteNodeId, intent.RawName, intent.NameKey, baseline.Kind,
            receipt.Size, receipt.Sha256, receipt.Version, receipt.PublishedSequence, false);
        await UpsertSyncRemoteAsync(connection, transaction, intent.MappingId, remote, cancellationToken);
        var currentRemote = await ReadSyncJsonAsync<SyncRemoteEntry>(connection, transaction, "sync_state_remote", "remote_json", "node_id", intent.MappingId, receipt.NodeId, cancellationToken);
        var newerRemote = currentRemote is not null && currentRemote.LastSequence > receipt.PublishedSequence;
        var agreed = await ReadSyncJsonAsync<SyncBaseline>(connection, transaction, "sync_state_baselines", "baseline_json", "entry_id", intent.MappingId, intent.EntryId, cancellationToken);
        var remoteUnagreed = newerRemote && (agreed is null || currentRemote!.LastSequence > agreed.PublishedSequence || currentRemote.Version != agreed.RemoteVersion);
        await using var acknowledge = SyncCommand(connection, transaction, "UPDATE sync_state_operations SET stage=$acked,receipt_json=$receipt,worker_id=NULL,lease_until=NULL WHERE operation_id=$op");
        acknowledge.Parameters.AddWithValue("$acked", (int)SyncOperationStage.ClientAcked);
        acknowledge.Parameters.AddWithValue("$receipt", EncodeSync(receipt));
        acknowledge.Parameters.AddWithValue("$op", receipt.OperationId);
        await acknowledge.ExecuteNonQueryAsync(cancellationToken);
        if (root.InvalidationGeneration == operation.InvalidationGeneration && !newerRemote)
        {
            await using var clear = SyncCommand(connection, transaction, "DELETE FROM sync_state_dirty WHERE mapping_id=$mapping AND entry_id=$entry AND observed_generation<=$generation");
            AddSyncKey(clear, intent.MappingId, intent.EntryId);
            clear.Parameters.AddWithValue("$generation", intent.LocalGeneration);
            await clear.ExecuteNonQueryAsync(cancellationToken);
        }
        var local = await ReadSyncJsonAsync<SyncLocalEntry>(connection, transaction, "sync_state_entries", "local_json", "entry_id", intent.MappingId, intent.EntryId, cancellationToken);
        if (local is not null && (agreed is null || local.LocalGeneration > agreed.AgreedLocalGeneration || root.InvalidationGeneration != operation.InvalidationGeneration || remoteUnagreed))
            await UpsertSyncDirtyAsync(connection, transaction, intent.MappingId, new(intent.EntryId, local.LocalGeneration,
                remoteUnagreed ? SyncDirtyReason.RemoteChange : SyncDirtyReason.LocalChange, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow), cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    private async Task<SqliteConnection> OpenSyncConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(ConnectionString);
        try
        {
            await connection.OpenAsync(cancellationToken);
            await using var options = connection.CreateCommand();
            options.CommandText = "PRAGMA synchronous=FULL; SELECT version FROM sync_state_format WHERE singleton=1";
            var version = await options.ExecuteScalarAsync(cancellationToken);
            if (version is null || Convert.ToInt32(version, CultureInfo.InvariantCulture) != SyncStateFormatVersion)
                throw new InvalidDataException("The sync state format is unsupported or was not initialized.");
            return connection;
        }
        catch { await connection.DisposeAsync(); throw; }
    }

    private static SqliteCommand SyncCommand(SqliteConnection connection, SqliteTransaction? transaction, string sql)
    {
        SyncDiagnostics.SqlCommandCreated();
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        return command;
    }

    private static async Task ExecuteSyncAsync(SqliteConnection connection, SqliteTransaction transaction, string sql, CancellationToken cancellationToken)
    {
        await using var command = SyncCommand(connection, transaction, sql);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<SyncRootState> ReadSyncRootAsync(SqliteConnection connection, SqliteTransaction? transaction, string mappingId, CancellationToken cancellationToken)
    {
        await using var command = SyncCommand(connection, transaction, """
            SELECT r.identity_json,r.legacy_revision,r.invalidation_generation,r.needs_reconciliation,
                ifnull((SELECT enabled FROM sync_mappings m WHERE m.mapping_id=r.mapping_id AND m.client_instance_id=r.client_instance_id),0)
            FROM sync_state_roots r WHERE r.mapping_id=$mapping;
            """);
        command.Parameters.AddWithValue("$mapping", mappingId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) throw new InvalidOperationException("No sync state root exists for this mapping.");
        return new(mappingId, reader.IsDBNull(0) ? null : DecodeSync<SyncRootIdentity>(reader.GetString(0)), reader.GetInt64(1), reader.GetInt64(2), reader.GetBoolean(3), reader.GetBoolean(4));
    }

    private static async Task<SyncRootState> ReadBoundSyncRootAsync(SqliteConnection connection, SqliteTransaction transaction, string mappingId, CancellationToken cancellationToken)
    {
        var root = await ReadSyncRootAsync(connection, transaction, mappingId, cancellationToken);
        if (root.Identity is null) throw new InvalidOperationException("Legacy root identity is unknown; bind verified identities before planning.");
        return root;
    }

    private static async Task<T?> ReadSyncJsonAsync<T>(SqliteConnection connection, SqliteTransaction? transaction,
        string table, string column, string idColumn, string mappingId, string id, CancellationToken cancellationToken) where T : class
    {
        // Table/column identifiers are private constants, never caller input.
        await using var command = SyncCommand(connection, transaction, $"SELECT {column} FROM {table} WHERE mapping_id=$mapping AND {idColumn}=$entry");
        AddSyncKey(command, mappingId, id);
        return await command.ExecuteScalarAsync(cancellationToken) is string value ? DecodeSync<T>(value) : null;
    }

    private static async Task<SyncJournalCursor?> ReadSyncCursorAsync(SqliteConnection connection, SqliteTransaction? transaction, string mappingId, CancellationToken cancellationToken)
    {
        await using var command = SyncCommand(connection, transaction, "SELECT journal_id,position,valid FROM sync_state_cursors WHERE mapping_id=$mapping");
        command.Parameters.AddWithValue("$mapping", mappingId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? new(reader.GetString(0), reader.GetInt64(1), reader.GetBoolean(2)) : null;
    }

    private static async Task UpsertSyncDirtyAsync(SqliteConnection connection, SqliteTransaction transaction, string mappingId, SyncDirtyHint hint, CancellationToken cancellationToken)
    {
        await using var command = SyncCommand(connection, transaction, """
            INSERT INTO sync_state_dirty(mapping_id,entry_id,observed_generation,reasons,first_dirty,due_at)
            VALUES($mapping,$entry,$generation,$reasons,$first,$due)
            ON CONFLICT(mapping_id,entry_id) DO UPDATE SET observed_generation=MAX(observed_generation,excluded.observed_generation),
                reasons=reasons|excluded.reasons,first_dirty=MIN(first_dirty,excluded.first_dirty),due_at=MIN(due_at,excluded.due_at);
            """);
        AddSyncKey(command, mappingId, hint.EntryId);
        command.Parameters.AddWithValue("$generation", hint.ObservedGeneration);
        command.Parameters.AddWithValue("$reasons", (int)hint.Reasons);
        command.Parameters.AddWithValue("$first", hint.FirstDirtyAt.ToUnixTimeMilliseconds());
        command.Parameters.AddWithValue("$due", hint.DueAt.ToUnixTimeMilliseconds());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InvalidateSyncRootAsync(SqliteConnection connection, SqliteTransaction transaction, string mappingId, CancellationToken cancellationToken)
    {
        await using var command = SyncCommand(connection, transaction, "UPDATE sync_state_roots SET invalidation_generation=invalidation_generation+1,needs_reconciliation=1 WHERE mapping_id=$mapping");
        command.Parameters.AddWithValue("$mapping", mappingId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<bool> UpsertSyncRemoteAsync(SqliteConnection connection, SqliteTransaction transaction, string mappingId, SyncRemoteEntry entry, CancellationToken cancellationToken)
    {
        var current = await ReadSyncJsonAsync<SyncRemoteEntry>(connection, transaction, "sync_state_remote", "remote_json", "node_id", mappingId, entry.NodeId, cancellationToken);
        if (current is not null && current.LastSequence > entry.LastSequence) return false;
        if (current is not null && current.LastSequence == entry.LastSequence)
        {
            if (current with { Authoritative = entry.Authoritative } != entry) throw new InvalidDataException("One root sequence cannot describe two different remote states.");
            if (current.Authoritative || !entry.Authoritative) return false;
        }
        await using var command = SyncCommand(connection, transaction, "INSERT INTO sync_state_remote(mapping_id,node_id,last_seq,remote_json) VALUES($mapping,$entry,$seq,$json) ON CONFLICT(mapping_id,node_id) DO UPDATE SET last_seq=excluded.last_seq,remote_json=excluded.remote_json");
        AddSyncKey(command, mappingId, entry.NodeId);
        command.Parameters.AddWithValue("$seq", entry.LastSequence);
        command.Parameters.AddWithValue("$json", EncodeSync(entry));
        await command.ExecuteNonQueryAsync(cancellationToken);
        return true;
    }

    private static async Task MarkSyncRemoteDirtyAsync(SqliteConnection connection, SqliteTransaction transaction, string mappingId, string nodeId, CancellationToken cancellationToken)
    {
        // Indexed node matching avoids decoding/scanning every baseline. If a
        // damaged mapping has more matches than one batch, require full recovery.
        await using var command = SyncCommand(connection, transaction, """
            INSERT INTO sync_state_dirty(mapping_id,entry_id,observed_generation,reasons,first_dirty,due_at)
            SELECT e.mapping_id,e.entry_id,e.local_generation,$reason,$now,$now
            FROM sync_state_baselines b JOIN sync_state_entries e ON e.mapping_id=b.mapping_id AND e.entry_id=b.entry_id
            WHERE b.mapping_id=$mapping AND b.remote_node_id=$node LIMIT 1000
            ON CONFLICT(mapping_id,entry_id) DO UPDATE SET observed_generation=MAX(observed_generation,excluded.observed_generation),
                reasons=reasons|excluded.reasons,first_dirty=MIN(first_dirty,excluded.first_dirty),due_at=MIN(due_at,excluded.due_at);
            """);
        command.Parameters.AddWithValue("$mapping", mappingId);
        command.Parameters.AddWithValue("$node", nodeId);
        command.Parameters.AddWithValue("$reason", (int)SyncDirtyReason.RemoteChange);
        command.Parameters.AddWithValue("$now", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        await command.ExecuteNonQueryAsync(cancellationToken);
        await using var overflow = SyncCommand(connection, transaction, "SELECT COUNT(*) FROM (SELECT 1 FROM sync_state_baselines WHERE mapping_id=$mapping AND remote_node_id=$node LIMIT 1001)");
        overflow.Parameters.AddWithValue("$mapping", mappingId);
        overflow.Parameters.AddWithValue("$node", nodeId);
        if (Convert.ToInt64(await overflow.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture) > SyncBatchLimit)
            await InvalidateSyncRootAsync(connection, transaction, mappingId, cancellationToken);
    }

    private static async Task<SyncDurableOperation?> ReadSyncOperationAsync(SqliteConnection connection, SqliteTransaction? transaction, string operationId, CancellationToken cancellationToken)
    {
        await using var command = SyncCommand(connection, transaction, "SELECT intent_json,fingerprint,stage,invalidation_generation,legacy_revision,attempt,worker_id,lease_until,receipt_json,cancellation_fence_json FROM sync_state_operations WHERE operation_id=$op");
        command.Parameters.AddWithValue("$op", operationId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        return new(DecodeSync<SyncOperationIntent>(reader.GetString(0)), reader.GetString(1), (SyncOperationStage)reader.GetInt32(2), reader.GetInt64(3), reader.GetInt64(4), reader.GetInt32(5),
            reader.IsDBNull(6) ? null : reader.GetString(6), reader.IsDBNull(7) ? null : DateTimeOffset.Parse(reader.GetString(7), CultureInfo.InvariantCulture), reader.IsDBNull(8) ? null : DecodeSync<SyncPublicationReceipt>(reader.GetString(8)),
            reader.IsDBNull(9) ? null : DecodeSync<SyncCancellationFence>(reader.GetString(9)));
    }

    private async Task ChangeSyncOperationAsync(string operationId, Func<SyncDurableOperation, SyncOperationStage> transition, CancellationToken cancellationToken)
    {
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        var operation = await ReadSyncOperationAsync(connection, transaction, operationId, cancellationToken) ?? throw new InvalidOperationException("No durable operation exists.");
        await SetSyncStageAsync(connection, transaction, operationId, transition(operation), cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    private static async Task SetSyncStageAsync(SqliteConnection connection, SqliteTransaction transaction, string operationId,
        SyncOperationStage stage, CancellationToken cancellationToken, string? worker = null, DateTimeOffset? leaseUntil = null, bool incrementAttempt = false)
    {
        await using var command = SyncCommand(connection, transaction, "UPDATE sync_state_operations SET stage=$stage,worker_id=$worker,lease_until=$lease,attempt=attempt+$increment WHERE operation_id=$op");
        command.Parameters.AddWithValue("$stage", (int)stage);
        command.Parameters.AddWithValue("$worker", (object?)worker ?? DBNull.Value);
        command.Parameters.AddWithValue("$lease", leaseUntil is null ? DBNull.Value : leaseUntil.Value.ToUniversalTime().ToString("O"));
        command.Parameters.AddWithValue("$increment", incrementAttempt ? 1 : 0);
        command.Parameters.AddWithValue("$op", operationId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddSyncKey(SqliteCommand command, string mappingId, string entryId)
    {
        command.Parameters.AddWithValue("$mapping", mappingId);
        command.Parameters.AddWithValue("$entry", entryId);
    }

    private static string EncodeSync<T>(T value)
    {
        var json = JsonSerializer.Serialize(value, SyncStateJson);
        if (Encoding.UTF8.GetByteCount(json) > 32 * 1024) throw new InvalidDataException("A local sync state record exceeds its bound.");
        return json;
    }

    private static T DecodeSync<T>(string json)
    {
        if (Encoding.UTF8.GetByteCount(json) > 32 * 1024) throw new InvalidDataException("A local sync state record exceeds its bound.");
        return JsonSerializer.Deserialize<T>(json, SyncStateJson) ?? throw new InvalidDataException("A local sync state record is empty.");
    }

    private static void ValidateSyncBatch(int count)
    {
        if (count is < 0 or > SyncBatchLimit) throw new ArgumentOutOfRangeException(nameof(count), "Sync state batches are bounded to 1000 records.");
    }

    private static void ValidateSyncBatch<T>(IReadOnlyList<T> entries)
    {
        ValidateSyncBatch(entries.Count);
        if (entries.Sum(entry => (long)Encoding.UTF8.GetByteCount(EncodeSync(entry))) > 4 * 1024 * 1024)
            throw new ArgumentException("A sync state metadata batch exceeds 4 MiB.");
    }

    private static void ValidateId(string value, string name)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > 128 || value.Any(c => !char.IsAscii(c) || char.IsControl(c))) throw new ArgumentException("Opaque identities must contain 1–128 ASCII characters.", name);
    }

    private static void ValidateHash(string? value)
    {
        if (value is not null && (value.Length != 64 || value.Any(c => !(char.IsAsciiDigit(c) || c is >= 'a' and <= 'f')))) throw new ArgumentException("A content hash must be lowercase SHA-256.");
    }

    private static void ValidateName(string value)
    {
        if (value is "" or "." or ".." || value.Length > 255 || value.IndexOfAny(['/', '\\']) >= 0 || value.Any(char.IsControl)) throw new ArgumentException("An entry requires its raw safe single-segment name.");
    }

    private static void ValidateRootIdentity(SyncRootIdentity identity)
    {
        foreach (var value in new[] { identity.MappingId, identity.ClientInstanceId, identity.ProfileId, identity.DeviceId, identity.ServerRootId, identity.RootEpoch, identity.SourceVolumeIdentity, identity.SourceRootIdentity, identity.RulesVersion }) ValidateId(value, nameof(identity));
        if (!identity.ServerOrigin.IsAbsoluteUri || identity.ServerOrigin.Scheme is not ("https" or "http") || identity.ServerOrigin.UserInfo.Length != 0
            || identity.ServerOrigin.Query.Length != 0 || identity.ServerOrigin.Fragment.Length != 0 || identity.ServerOrigin.AbsolutePath != "/") throw new ArgumentException("Server identity requires an origin without credentials/path/query.");
        if (identity.NameCodec != "legacy" || identity.Mode != "upload_mirror_guarded") throw new InvalidOperationException("Codec/policy migration is a separate versioned stage.");
    }

    private static void ValidateLocalEntry(SyncLocalEntry entry)
    {
        ValidateId(entry.EntryId, nameof(entry.EntryId));
        if (entry.ParentEntryId is not null) ValidateId(entry.ParentEntryId, nameof(entry.ParentEntryId));
        if (entry.ParentEntryId == entry.EntryId) throw new ArgumentException("A local entry cannot parent itself.");
        ValidateName(entry.RawName);
        if (string.IsNullOrEmpty(entry.NameKey) || entry.NameKey.Length > 1024) throw new ArgumentException("A bounded name key is required.");
        if (!Enum.IsDefined(entry.Kind) || !Enum.IsDefined(entry.Presence) || entry.LocalGeneration <= 0 || entry.IdentityGeneration < 0 || entry.Size < 0
            || entry.HashGeneration is < 0 || entry.HashGeneration > entry.LocalGeneration || (entry.Sha256 is null) != (entry.HashGeneration is null)
            || entry.Kind == SyncEntryKind.Directory && (entry.Size != 0 || entry.Sha256 is not null)) throw new ArgumentException("Local metadata/generation is invalid.");
        ValidateHash(entry.Sha256);
        ValidateId(entry.RulesVersion, nameof(entry.RulesVersion));
        if (entry.FileSystemId is { Length: > 256 }) throw new ArgumentException("Filesystem identity exceeds its bound.");
    }

    private static void ValidateRemoteEntry(SyncRemoteEntry entry)
    {
        ValidateId(entry.NodeId, nameof(entry.NodeId));
        if (entry.ParentNodeId is not null) ValidateId(entry.ParentNodeId, nameof(entry.ParentNodeId));
        ValidateId(entry.Version, nameof(entry.Version));
        ValidateName(entry.RawName);
        ValidateHash(entry.Sha256);
        if (!Enum.IsDefined(entry.Kind) || entry.Size < 0 || entry.LastSequence < 0 || string.IsNullOrEmpty(entry.NameKey) || entry.NameKey.Length > 1024
            || entry.Kind == SyncEntryKind.Directory && (entry.Size != 0 || entry.Sha256 is not null)) throw new ArgumentException("Remote metadata is invalid.");
    }

    private static void ValidateHint(SyncDirtyHint hint)
    {
        ValidateId(hint.EntryId, nameof(hint.EntryId));
        if (hint.ObservedGeneration <= 0 || hint.Reasons == SyncDirtyReason.None || ((int)hint.Reasons & ~63) != 0 || hint.DueAt < hint.FirstDirtyAt) throw new ArgumentException("Dirty work must identify a valid generation/reason/time range.");
    }

    private static void ValidateCursor(SyncJournalCursor cursor)
    {
        ValidateId(cursor.JournalId, nameof(cursor.JournalId));
        if (cursor.Position < 0) throw new ArgumentOutOfRangeException(nameof(cursor.Position));
    }

    private static void ValidateIntent(SyncOperationIntent intent)
    {
        foreach (var id in new[] { intent.OperationId, intent.MappingId, intent.EntryId, intent.RootEpoch, intent.RulesVersion, intent.ParentRemoteNodeId }) ValidateId(id, nameof(intent));
        ValidateName(intent.RawName);
        ValidateHash(intent.Sha256);
        if (string.IsNullOrEmpty(intent.NameKey) || intent.NameKey.Length > 1024) throw new ArgumentException("An intent requires the immutable name key.");
        if (!Enum.IsDefined(intent.Kind) || !Enum.IsDefined(intent.Expected.Kind) || intent.LocalGeneration <= 0 || intent.Size < 0 || intent.Dependencies.Count > 64
            || intent.Dependencies.Distinct(StringComparer.Ordinal).Count() != intent.Dependencies.Count || intent.Dependencies.Contains(intent.OperationId, StringComparer.Ordinal)) throw new ArgumentException("An operation intent is invalid.");
        foreach (var dependency in intent.Dependencies) ValidateId(dependency, nameof(intent.Dependencies));
        if (intent.Expected.Kind == SyncExpectedKind.Absent && (intent.Expected.NodeId is not null || intent.Expected.Version is not null)
            || intent.Expected.Kind == SyncExpectedKind.Version && (intent.Expected.NodeId is null || intent.Expected.Version is null)) throw new ArgumentException("Expected destination is either absent or an exact node/version.");
        if (intent.Expected.Kind == SyncExpectedKind.Version) { ValidateId(intent.Expected.NodeId!, nameof(intent.Expected.NodeId)); ValidateId(intent.Expected.Version!, nameof(intent.Expected.Version)); }
        if (intent.Kind == SyncIntentKind.EnsureDirectory && (intent.Size != 0 || intent.Sha256 is not null)
            || intent.Kind != SyncIntentKind.EnsureDirectory && (intent.Sha256 is null || intent.SourceSnapshotIdentity is null)
            || intent.Kind == SyncIntentKind.AdoptEqual && intent.Expected.Kind != SyncExpectedKind.Version) throw new ArgumentException("A file intent requires a sealed source and content hash; adoption requires a version.");
        if (intent.SourceSnapshotIdentity is not null) ValidateId(intent.SourceSnapshotIdentity, nameof(intent.SourceSnapshotIdentity));
    }
}
