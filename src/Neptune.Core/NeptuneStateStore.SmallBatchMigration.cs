using System.Text.Json;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    // Optional transport state. Existing work rows retain original source seals,
    // CAS inputs, server UUIDs and receipts; references pin them until cleanup.
    private static readonly Dictionary<string, string> SmallBatchCycleSchema = new(StringComparer.Ordinal)
    {
        ["local_cycle_batches"] = "CREATE TABLE local_cycle_batches(batch_id TEXT PRIMARY KEY,scope_id TEXT NOT NULL REFERENCES local_cycle_scopes(scope_id),plan_json TEXT NOT NULL,plan_hash TEXT NOT NULL CHECK(length(plan_hash)=64),size INTEGER NOT NULL CHECK(size BETWEEN 0 AND 33554432),spool_hash TEXT CHECK(spool_hash IS NULL OR length(spool_hash)=64),spool_identity TEXT CHECK(spool_identity IS NULL OR length(spool_identity)=64),phase INTEGER NOT NULL DEFAULT 0 CHECK(phase BETWEEN 0 AND 4),bytes INTEGER NOT NULL CHECK(bytes BETWEEN 1024 AND 4325376),CHECK(length(CAST(plan_json AS BLOB))<=4194304),CHECK(phase NOT IN(1,2) OR spool_hash IS NOT NULL AND spool_identity IS NOT NULL))",
        ["local_cycle_batch_items"] = "CREATE TABLE local_cycle_batch_items(batch_id TEXT NOT NULL REFERENCES local_cycle_batches(batch_id),ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 499),operation_id TEXT NOT NULL REFERENCES local_cycle_work(operation_id),work_no INTEGER NOT NULL CHECK(work_no>0),offset INTEGER NOT NULL CHECK(offset BETWEEN 0 AND 33554432),size INTEGER NOT NULL CHECK(size BETWEEN 0 AND 262144),source_identity TEXT NOT NULL CHECK(length(source_identity)=64),PRIMARY KEY(batch_id,ordinal),UNIQUE(batch_id,operation_id),CHECK(offset+size<=33554432)) WITHOUT ROWID",
        ["local_cycle_batches_scope"] = "CREATE INDEX local_cycle_batches_scope ON local_cycle_batches(scope_id,phase,batch_id)",
        ["local_cycle_batch_items_work"] = "CREATE INDEX local_cycle_batch_items_work ON local_cycle_batch_items(operation_id,batch_id)",
        ["local_cycle_batches_immutable"] = "CREATE TRIGGER local_cycle_batches_immutable BEFORE UPDATE ON local_cycle_batches WHEN NEW.batch_id IS NOT OLD.batch_id OR NEW.scope_id IS NOT OLD.scope_id OR NEW.plan_json IS NOT OLD.plan_json OR NEW.plan_hash IS NOT OLD.plan_hash OR NEW.size IS NOT OLD.size OR NEW.bytes IS NOT OLD.bytes OR (NEW.spool_identity IS NOT OLD.spool_identity AND NOT(OLD.phase=0 AND NEW.phase=0 AND OLD.spool_identity IS NULL AND NEW.spool_identity IS NOT NULL)) OR (OLD.spool_hash IS NOT NULL AND NEW.spool_hash IS NOT OLD.spool_hash) OR (NEW.spool_hash IS NOT OLD.spool_hash AND NOT(OLD.phase=0 AND NEW.phase=1)) OR NOT(NEW.phase=OLD.phase OR OLD.phase=0 AND NEW.phase IN(1,3) OR OLD.phase=1 AND NEW.phase IN(2,3) OR OLD.phase=2 AND NEW.phase=3 OR OLD.phase=3 AND NEW.phase=4) BEGIN SELECT RAISE(ABORT,'small batch original or phase changed');END",
        ["local_cycle_batches_delete"] = "CREATE TRIGGER local_cycle_batches_delete BEFORE DELETE ON local_cycle_batches WHEN OLD.phase<>4 BEGIN SELECT RAISE(ABORT,'small batch spool cleanup is not confirmed');END",
        ["local_cycle_batches_terminal"] = "CREATE TRIGGER local_cycle_batches_terminal BEFORE UPDATE OF phase ON local_cycle_batches WHEN OLD.phase=2 AND NEW.phase=3 AND EXISTS(SELECT 1 FROM local_cycle_batch_items i JOIN local_cycle_work w ON w.operation_id=i.operation_id WHERE i.batch_id=OLD.batch_id AND (w.phase NOT IN(5,7) OR w.upload_id IS NULL OR w.result_json IS NULL)) BEGIN SELECT RAISE(ABORT,'unknown original publication pins aggregate');END",
        ["local_cycle_batch_items_insert"] = "CREATE TRIGGER local_cycle_batch_items_insert BEFORE INSERT ON local_cycle_batch_items WHEN NOT EXISTS(SELECT 1 FROM local_cycle_batches b JOIN local_cycle_work w ON w.scope_id=b.scope_id WHERE b.batch_id=NEW.batch_id AND b.phase=0 AND w.operation_id=NEW.operation_id AND w.work_no=NEW.work_no AND w.size=NEW.size AND NEW.offset+NEW.size<=b.size) BEGIN SELECT RAISE(ABORT,'small batch original reference changed');END",
        ["local_cycle_batch_items_immutable"] = "CREATE TRIGGER local_cycle_batch_items_immutable BEFORE UPDATE ON local_cycle_batch_items BEGIN SELECT RAISE(ABORT,'small batch original reference is immutable');END",
        ["local_cycle_batch_items_delete"] = "CREATE TRIGGER local_cycle_batch_items_delete BEFORE DELETE ON local_cycle_batch_items WHEN NOT EXISTS(SELECT 1 FROM local_cycle_batches b WHERE b.batch_id=OLD.batch_id AND b.phase=4) BEGIN SELECT RAISE(ABORT,'small batch original remains pinned');END",
    };
    private static readonly Lazy<Dictionary<string, string>> BatchCycleSchema = new(() =>
    {
        var schema = new Dictionary<string, string>(CycleSchemaV6, StringComparer.Ordinal);
        schema["local_cycle_format"] = schema["local_cycle_format"].Replace("CHECK(version=6)", "CHECK(version=7)", StringComparison.Ordinal);
        foreach (var pair in SmallBatchCycleSchema) schema.Add(pair.Key, pair.Value);
        return schema;
    });
    private static Dictionary<string, string> CycleSchemaV7 => BatchCycleSchema.Value;

    // Only a separately qualified batch caller may request this extension.
    // A normal Initialize/Open/EnsureCycle leaves recognized4/5/6 unchanged.
    // Upgrade directly from the current shape, with one WAL-aware backup; no
    // replacement requests, subscriptions, operations or baseline rows change.
    private async Task EnsureCycleSmallBatchesAsync(LocalJournalAdmission admission, CancellationToken ct)
    {
        if (await PreflightCycleAsync(ct) == 7) return;
        await using var migration = await AcquireSyncMigrationLockAsync(ct);
        var previous = await PreflightCycleAsync(ct); if (previous == 7) return;
        if (previous is not (4 or 5 or 6)) throw new InvalidDataException("Batch extension requires a recognized current cycle4/5/6.");
        var copy = checked(new FileInfo(databasePath).Length + (File.Exists(databasePath + "-wal") ? new FileInfo(databasePath + "-wal").Length : 0));
        CheckLocalDiskAdmission(admission, checked(copy + NamespaceReserve(64)));
        var temp = databasePath + ".cycle-batches-" + Guid.NewGuid().ToString("N") + ".partial";
        try
        {
            await CreateCoherenceDatabaseCopy(databasePath, temp, ct);
            var hash = await HashSyncBackupAsync(temp, ct); var backup = Path.GetFullPath(databasePath) + ".pre-local-cycle-v7-" + hash + ".db";
            if (File.Exists(backup))
            {
                if (await HashSyncBackupAsync(backup, ct) != hash) throw new InvalidDataException("Batch extension backup changed.");
                File.Delete(temp);
            }
            else File.Move(temp, backup);
            var manifest = JsonSerializer.Serialize(new CycleBackup(Path.GetFullPath(databasePath), backup, hash, 7), LocalCycleWire.Json);
            var partial = databasePath + ".local-cycle-v7-backup.json." + Guid.NewGuid().ToString("N") + ".partial";
            try
            {
                await File.WriteAllTextAsync(partial, manifest, ct);
                using (var file = new FileStream(partial, FileMode.Open, FileAccess.Write, FileShare.None)) file.Flush(true);
                File.Move(partial, databasePath + ".local-cycle-v7-backup.json", true);
            }
            finally { if (File.Exists(partial)) File.Delete(partial); }
            await CheckLocalAdmissionAsync(admission, 0, NamespaceReserve(64), ct);
            await using var connection = await OpenLocalSourceConnectionAsync(ct); using var transaction = connection.BeginTransaction(deferred: false);
            await ExecuteSyncAsync(connection, transaction, "ALTER TABLE local_cycle_format RENAME TO previous_cycle_format", ct);
            await ExecuteSyncAsync(connection, transaction, CycleSchemaV7["local_cycle_format"], ct);
            await ExecuteSyncAsync(connection, transaction, "INSERT INTO local_cycle_format SELECT singleton,7,bytes FROM previous_cycle_format;DROP TABLE previous_cycle_format", ct);
            if (previous == 4)
            {
                foreach (var pair in CycleSchemaV6.Where(pair => !CycleSchema.ContainsKey(pair.Key)))
                    await ExecuteSyncAsync(connection, transaction, pair.Value, ct);
            }
            else if (previous == 5)
            {
                foreach (var column in CycleSupersessionColumns)
                    await ExecuteSyncAsync(connection, transaction, "ALTER TABLE local_cycle_replacement_requests ADD COLUMN " + column, ct);
                await ExecuteSyncAsync(connection, transaction, "DROP INDEX local_cycle_replacement_pending", ct);
                await ExecuteSyncAsync(connection, transaction, CycleSchemaV6["local_cycle_replacement_pending"], ct);
                await ExecuteSyncAsync(connection, transaction, CycleSchemaV6["local_cycle_replacement_successor"], ct);
            }
            foreach (var sql in SmallBatchCycleSchema.Values) await ExecuteSyncAsync(connection, transaction, sql, ct);
            await transaction.CommitAsync(ct);
        }
        finally { if (File.Exists(temp)) File.Delete(temp); }
    }

    // Returns a separate pre-upgrade copy. It never rolls a live profile or
    // server publications backwards, nor removes retained batch artifacts.
    public Task<string> CreateLocalCyclePreBatchRollbackCopyAsync(string destination, CancellationToken cancellationToken = default)
        => CreateCycleRollbackCopy(destination, 7, cancellationToken);
}
