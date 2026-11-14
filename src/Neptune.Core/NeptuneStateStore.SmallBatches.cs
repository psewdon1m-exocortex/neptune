using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public enum LocalSmallBatchPhase { Building, Ready, Attempted, CleanupPending, Cleaned }
public sealed record LocalSmallBatchSpan(string OperationId, long WorkNumber, long Offset, long Size, string SourceIdentity);
public sealed record LocalSmallBatch(string ScopeId, SmallFileBatchPlan Plan, LocalSmallBatchPhase Phase,
    string? SpoolHash, IReadOnlyList<LocalSmallBatchSpan> Spans, string? SpoolIdentity = null);

public sealed partial class NeptuneStateStore
{
    // A fixed sealed-original bound, independent of active capture workers.
    // Wire/storage constraints and every original validation remain unchanged.
    internal const int LocalSmallBatchItems = SmallFileBatchScheduler.BulkItems, LocalSmallBatchCapacity = 64;

    internal async Task EnableOwnedSmallBatches(string root, string owner, string id, LocalJournalAdmission local, CancellationToken ct)
    {
        await CycleAdmission(root, owner, id, local, 64, ct);
        await using var c = await OpenCycleConnection(ct); using var t = c.BeginTransaction(deferred: true);
        var scope = await RequireCycle(c, t, root, owner, id, false, ct); await t.CommitAsync(ct);
        await EnsureCycleSmallBatchesAsync(CycleBudget(local, scope), ct);
    }

    internal async Task<LocalSmallBatch> CreateOwnedSmallBatch(string root, string owner, string id,
        string batch, IReadOnlyList<string> operations, LocalJournalAdmission local, CancellationToken ct, bool executionJob = false)
    {
        if (!LocalCycleWire.Uuid(batch) || operations.Count is < 1 or > LocalSmallBatchItems
            || operations.Distinct(StringComparer.Ordinal).Count() != operations.Count) throw new ArgumentException("Invalid bounded batch originals.");
        if (await PreflightCycleAsync(ct) != 7) throw new InvalidOperationException("Batch state is not explicitly enabled.");
        await CycleAdmission(root, owner, id, local, LocalSmallBatchItems + 8, ct);
        await using var c = await OpenCycleConnection(ct); using var t = c.BeginTransaction(deferred: false);
        var before = await NamespaceChanges(c, t, ct); var scope = await RequireCycle(c, t, root, owner, id, false, ct);
        await using (var count = SyncCommand(c, t, "SELECT COUNT(*) FROM(SELECT 1 FROM local_cycle_batches LIMIT 64)"))
            if (Convert.ToInt64(await count.ExecuteScalarAsync(ct)) >= LocalSmallBatchCapacity) throw new LocalSourceGapException("BatchCapacity", "Retained aggregate capacity reached; preserve originals and use individual transport.");
        var entries = new List<SmallFileBatchEntry>(operations.Count); var spans = new List<LocalSmallBatchSpan>(operations.Count); long offset = 0;
        foreach (var operation in operations)
        {
            var w = await ReadCycleWork(c, t, operation, ct);
            if (w.ScopeId != id || w.Entry.Directory || w.CaptureProof is null || w.InputJson is null || w.IntentHash is null
                || w.Size is null or < 0 or > SmallFileBatchPlan.MaximumFileBytes || w.SpoolPath is null || w.UploadId is not null
                || w.Phase is not (LocalCycleWorkPhase.Sealed or LocalCycleWorkPhase.Sending or LocalCycleWorkPhase.OutcomeUnknown))
                throw new InvalidOperationException("Aggregate requires retained sealed originals without a known server session.");
            var input = JsonSerializer.Deserialize<CycleUploadInput>(w.InputJson, LocalCycleWire.Json) ?? throw new InvalidDataException("Original intent missing.");
            if (input.Intent.IdempotencyKey != "neptune-cycle-" + operation || LocalCycleWire.IntentHash(input) != w.IntentHash
                || input.Intent.Sha256 != w.Sha256 || LocalCycleWire.Decimal(input.Intent.SizeBytes) != w.Size
                || input.BackendIdentity != scope.Configuration.BackendIdentity || input.Intent.DeviceId != scope.Configuration.DeviceId) throw new InvalidDataException("Original seal or intent changed.");
            await using var q = SyncCommand(c, t, "SELECT work_no FROM local_cycle_work WHERE operation_id=$op AND active=1");
            q.Parameters.AddWithValue("$op", operation); if (await q.ExecuteScalarAsync(ct) is not long number || number <= 0) throw new InvalidOperationException("Original work is not active.");
            var identity = CycleHash(w.CaptureProof); entries.Add(new(input, number, identity));
            spans.Add(new(operation, number, offset, w.Size.Value, identity)); offset = checked(offset + w.Size.Value);
        }
        var plan = SmallFileBatchPlan.Create(scope.Configuration.Binding, batch, scope.Configuration.RulesVersion, entries);
        if (executionJob && plan.CanExecuteAsJob) plan = plan.AsExecutionJob();
        var raw = Encoding.UTF8.GetString(plan.EncodePersisted()); var bytes = checked(1024 + Encoding.UTF8.GetByteCount(raw) + operations.Count * 256);
        // Admission includes the complete encoded metadata before any insertion.
        CheckLocalDiskAdmission(CycleBudget(local, scope), checked(bytes + NamespaceReserve(LocalSmallBatchItems + 8)));
        await using (var q = SyncCommand(c, t, "INSERT INTO local_cycle_batches(batch_id,scope_id,plan_json,plan_hash,size,bytes) VALUES($batch,$scope,$plan,$hash,$size,$bytes)"))
        {
            q.Parameters.AddWithValue("$batch", batch); CycleId(q, id); q.Parameters.AddWithValue("$plan", raw);
            q.Parameters.AddWithValue("$hash", CycleHash(raw)); q.Parameters.AddWithValue("$size", offset); q.Parameters.AddWithValue("$bytes", bytes); await q.ExecuteNonQueryAsync(ct);
        }
        for (var n = 0; n < spans.Count; n++)
        {
            var span = spans[n]; await using var q = SyncCommand(c, t, "INSERT INTO local_cycle_batch_items VALUES($batch,$ordinal,$op,$work,$offset,$size,$identity)");
            q.Parameters.AddWithValue("$batch", batch); q.Parameters.AddWithValue("$ordinal", n); q.Parameters.AddWithValue("$op", span.OperationId);
            q.Parameters.AddWithValue("$work", span.WorkNumber); q.Parameters.AddWithValue("$offset", span.Offset);
            q.Parameters.AddWithValue("$size", span.Size); q.Parameters.AddWithValue("$identity", span.SourceIdentity); await q.ExecuteNonQueryAsync(ct);
        }
        await AddCycleBytes(c, t, id, bytes, ct); await CommitCycle(c, t, before, ct);
        return new(id, plan, LocalSmallBatchPhase.Building, null, spans.AsReadOnly());
    }

    private static async Task<LocalSmallBatch> ReadSmallBatch(SqliteConnection c, SqliteTransaction t, LocalCycleScope scope, string batch, CancellationToken ct,
        IDictionary<string, LocalCycleWork>? originals = null)
    {
        string raw, hash; long size; string? spool, identity; LocalSmallBatchPhase phase;
        await using (var q = SyncCommand(c, t, "SELECT plan_json,plan_hash,size,spool_hash,phase,spool_identity FROM local_cycle_batches WHERE batch_id=$batch AND scope_id=$scope"))
        {
            q.Parameters.AddWithValue("$batch", batch); CycleId(q, scope.ScopeId); await using var r = await q.ExecuteReaderAsync(ct);
            if (!await r.ReadAsync(ct)) throw new InvalidOperationException("Original batch is absent from this scope.");
            raw = r.GetString(0); hash = r.GetString(1); size = r.GetInt64(2); spool = r.IsDBNull(3) ? null : r.GetString(3); phase = (LocalSmallBatchPhase)r.GetInt32(4);
            identity = r.IsDBNull(5) ? null : r.GetString(5);
        }
        if (CycleHash(raw) != hash) throw new InvalidDataException("Original batch fingerprint changed.");
        var plan = SmallFileBatchPlan.RestorePersisted(Encoding.UTF8.GetBytes(raw), scope.Configuration.Binding);
        if (plan.BatchId != batch || plan.RulesVersion != scope.Configuration.RulesVersion || plan.DataBytes != size || plan.Entries.Count > LocalSmallBatchItems) throw new InvalidDataException("Batch scope or limits changed.");
        var spans = new List<LocalSmallBatchSpan>(LocalSmallBatchItems);
        await using (var q = SyncCommand(c, t, "SELECT ordinal,operation_id,work_no,offset,size,source_identity FROM local_cycle_batch_items WHERE batch_id=$batch ORDER BY ordinal LIMIT $limit"))
        {
            q.Parameters.AddWithValue("$batch", batch); q.Parameters.AddWithValue("$limit", LocalSmallBatchItems + 1); await using var r = await q.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                if (r.GetInt64(0) != spans.Count) throw new InvalidDataException("Batch offset order changed.");
                spans.Add(new(r.GetString(1), r.GetInt64(2), r.GetInt64(3), r.GetInt64(4), r.GetString(5)));
            }
        }
        if (spans.Count != plan.Entries.Count) throw new InvalidDataException("Batch membership changed.");
        var uploads = new Dictionary<string, string>(StringComparer.Ordinal); long offset = 0;
        for (var n = 0; n < spans.Count; n++)
        {
            var span = spans[n]; var entry = plan.Entries[n]; var w = await ReadCycleWork(c, t, span.OperationId, ct);
            if (w.ScopeId != scope.ScopeId || span.Offset != offset || span.Size != w.Size || span.Size != LocalCycleWire.Decimal(entry.Original.Intent.SizeBytes)
                || w.CaptureProof is null || span.SourceIdentity != CycleHash(w.CaptureProof) || span.SourceIdentity != entry.SourceIdentity || span.WorkNumber != entry.SourceGeneration
                || entry.Original.Intent.IdempotencyKey != "neptune-cycle-" + span.OperationId || w.InputJson is null || w.IntentHash != LocalCycleWire.IntentHash(entry.Original)
                || LocalCycleWire.IntentHash(JsonSerializer.Deserialize<CycleUploadInput>(w.InputJson, LocalCycleWire.Json)!) != w.IntentHash || w.Sha256 != entry.Original.Intent.Sha256)
                throw new InvalidDataException("Batch no longer matches immutable native originals.");
            await using var q = SyncCommand(c, t, "SELECT work_no FROM local_cycle_work WHERE operation_id=$op"); q.Parameters.AddWithValue("$op", span.OperationId);
            if (!Equals(await q.ExecuteScalarAsync(ct), span.WorkNumber)) throw new InvalidDataException("Original source generation changed.");
            using var originalInput = JsonDocument.Parse(JsonSerializer.SerializeToUtf8Bytes(LocalCycleWire.InputObject(JsonSerializer.Deserialize<CycleUploadInput>(w.InputJson!, LocalCycleWire.Json)!)));
            using var declaredInput = JsonDocument.Parse(JsonSerializer.SerializeToUtf8Bytes(LocalCycleWire.InputObject(entry.Original)));
            if (!JsonElement.DeepEquals(originalInput.RootElement, declaredInput.RootElement)) throw new InvalidDataException("Complete original transport input changed.");
            originals?.Add(span.OperationId, w);
            if (w.UploadId is not null) uploads.Add(entry.Original.Intent.IdempotencyKey, w.UploadId); offset = checked(offset + span.Size);
        }
        if (offset != size) throw new InvalidDataException("Aggregate span length changed.");
        return new(scope.ScopeId, plan.WithOriginalUploadIds(uploads), phase, spool, spans.AsReadOnly(), identity);
    }

    internal async Task SaveOwnedSmallBatchOutcome(string root, string owner, string id, string batch,
        SmallFileBatchOutcome outcome, LocalJournalAdmission local, CancellationToken ct)
    {
        if (outcome.BatchId != batch || outcome.Items.Count is < 1 or > LocalSmallBatchItems)
            throw new InvalidDataException("Invalid bounded original receipt set.");
        await CycleAdmission(root, owner, id, local, 5 * outcome.Items.Count, ct);
        await using var c = await OpenCycleConnection(ct); using var t = c.BeginTransaction(deferred: false);
        var before = await NamespaceChanges(c, t, ct);
        var scope = await RequireCycle(c, t, root, owner, id, false, ct);
        var originals = new Dictionary<string, LocalCycleWork>(StringComparer.Ordinal);
        var row = await ReadSmallBatch(c, t, scope, batch, ct, originals);
        if (row.Phase != LocalSmallBatchPhase.Attempted || outcome.Items.Count != row.Spans.Count)
            throw new InvalidDataException("Receipt set changed attempted original membership.");
        for (var n = 0; n < row.Spans.Count; n++)
        {
            var entry = row.Plan.Entries[n]; var item = outcome.Items[n]; var work = originals[row.Spans[n].OperationId];
            if (item.OperationId != entry.Original.Intent.IdempotencyKey || item.SourceGeneration != entry.SourceGeneration || item.SourceIdentity != entry.SourceIdentity)
                throw new InvalidDataException("Batch response changed original source identity or order.");
            if (item.Status is not { } status)
            {
                if (item.State == "MISSING" && work.UploadId is not null || item.State is not ("MISSING" or "CONFLICT" or "UNRESOLVED"))
                    throw new InvalidDataException("Missing receipt cannot change a known original outcome.");
                continue;
            }
            var expected = status.Phase switch { "published" => "PUBLISHED", "not_published" => "NOT_PUBLISHED",
                "physical_pending" or "reconciliation_required" => "UNRESOLVED", _ => "PENDING" };
            if (item.State != expected) throw new InvalidDataException("Batch state contradicts its original receipt.");
            if (work.Phase == LocalCycleWorkPhase.Published || work.Phase == LocalCycleWorkPhase.Conflict && work.ResultJson is not null)
            {
                if (work.UploadId != status.UploadId || !LocalCycleWire.SameResult(work.ResultJson, status.ResultJson) || work.ReceivedBytes != status.ReceivedBytes)
                    throw new InvalidDataException("Batch replay changed an already durable original outcome.");
                continue;
            }
            // Same wire validation and monotonic offset predicate as individual
            // recovery, against current rows in this single writer transaction.
            await ApplyCycleStatus(c, t, id, work, status, ct);
            if(status.Phase is "published" or "not_published")
            {
                await using var clear=SyncCommand(c,t,"DELETE FROM client_operation_schedule WHERE scope_id=$scope AND operation_id=$operation");
                CycleId(clear,id);clear.Parameters.AddWithValue("$operation",work.OperationId);await clear.ExecuteNonQueryAsync(ct);
            }
        }
        // No spool release, baseline change, or dirty-generation ACK here.
        await CommitCycle(c, t, before, ct);
    }

    internal async Task BindOwnedSmallBatchSpool(string root, string owner, string id, string batch, string identity, LocalJournalAdmission local, CancellationToken ct)
    {
        if (!LocalCycleWire.Hash(identity)) throw new InvalidDataException("Invalid native spool incarnation.");
        await CycleAdmission(root, owner, id, local, 2, ct); await using var c = await OpenCycleConnection(ct); using var t = c.BeginTransaction(deferred: false);
        var before = await NamespaceChanges(c, t, ct); _ = await RequireCycle(c, t, root, owner, id, false, ct);
        await using var q = SyncCommand(c, t, "UPDATE local_cycle_batches SET spool_identity=$identity WHERE batch_id=$batch AND scope_id=$scope AND phase=0 AND spool_identity IS NULL");
        q.Parameters.AddWithValue("$identity", identity); q.Parameters.AddWithValue("$batch", batch); CycleId(q, id);
        if (await q.ExecuteNonQueryAsync(ct) != 1) throw new InvalidOperationException("Aggregate incarnation is already assigned or no longer building.");
        await CommitCycle(c, t, before, ct);
    }

    internal async Task<LocalSmallBatch> ReadOwnedSmallBatch(string root, string owner, string id, string batch, CancellationToken ct, string? recoveryCurrent = null)
    {
        await using var c = await OpenCycleConnection(ct); using var t = c.BeginTransaction(deferred: true);
        var scope = recoveryCurrent is null || recoveryCurrent == id ? await RequireCycle(c, t, root, owner, id, true, ct)
            : await RequireHistoricalCycle(c, t, root, owner, recoveryCurrent, id, ct);
        return await ReadSmallBatch(c, t, scope, batch, ct);
    }

    internal async Task ChangeOwnedSmallBatch(string root, string owner, string id, string batch, LocalSmallBatchPhase next,
        string? spoolHash, LocalJournalAdmission local, CancellationToken ct, string? recoveryCurrent = null)
    {
        if (recoveryCurrent is not null && recoveryCurrent != id && next is not (LocalSmallBatchPhase.CleanupPending or LocalSmallBatchPhase.Cleaned))
            throw new InvalidOperationException("Historical aggregates only permit cleanup after original recovery.");
        await CycleAdmission(root, owner, recoveryCurrent ?? id, local, LocalSmallBatchItems + 8, ct); await using var c = await OpenCycleConnection(ct); using var t = c.BeginTransaction(deferred: false);
        var before = await NamespaceChanges(c, t, ct); var scope = recoveryCurrent is null || recoveryCurrent == id ? await RequireCycle(c, t, root, owner, id, false, ct)
            : await RequireHistoricalCycle(c, t, root, owner, recoveryCurrent, id, ct);
        var row = await ReadSmallBatch(c, t, scope, batch, ct);
        if (next == LocalSmallBatchPhase.Ready)
        {
            if (row.Phase != LocalSmallBatchPhase.Building || row.SpoolIdentity is null || spoolHash?.Length != 64 || spoolHash.Any(ch => !(ch is >= '0' and <= '9' or >= 'a' and <= 'f')))
                throw new InvalidOperationException("Only a fully sealed aggregate can become ready.");
        }
        else if (spoolHash is not null) throw new ArgumentException("Spool fingerprint is assigned exactly once.");
        if (next == LocalSmallBatchPhase.Attempted)
        {
            if (row.Phase != LocalSmallBatchPhase.Ready) throw new InvalidOperationException("Batch attempt requires ready bytes.");
            foreach (var span in row.Spans)
            {
                await using var mark = SyncCommand(c, t, "UPDATE local_cycle_work SET phase=3,error=NULL WHERE operation_id=$op AND active=1 AND phase IN(1,3,4) AND upload_id IS NULL AND input_json IS NOT NULL");
                mark.Parameters.AddWithValue("$op", span.OperationId); if (await mark.ExecuteNonQueryAsync(ct) != 1) throw new InvalidOperationException("Original attempt changed before batch send.");
            }
        }
        else if (next == LocalSmallBatchPhase.CleanupPending)
        {
            if (row.Phase is not (LocalSmallBatchPhase.Building or LocalSmallBatchPhase.Ready or LocalSmallBatchPhase.Attempted)) throw new InvalidOperationException("Invalid cleanup transition.");
            if (row.Phase == LocalSmallBatchPhase.Attempted)
                foreach (var span in row.Spans) RequireSmallBatchTerminal(await ReadCycleWork(c, t, span.OperationId, ct));
        }
        else if (next == LocalSmallBatchPhase.Cleaned)
        {
            if (row.Phase != LocalSmallBatchPhase.CleanupPending) throw new InvalidOperationException("Spool deletion must follow durable cleanup authorization.");
        }
        else if (next != LocalSmallBatchPhase.Ready) throw new ArgumentException("Invalid batch phase.");
        await using var q = SyncCommand(c, t, "UPDATE local_cycle_batches SET phase=$next,spool_hash=COALESCE($hash,spool_hash) WHERE batch_id=$batch AND phase=$old");
        q.Parameters.AddWithValue("$next", (int)next); q.Parameters.AddWithValue("$hash", (object?)spoolHash ?? DBNull.Value);
        q.Parameters.AddWithValue("$batch", batch); q.Parameters.AddWithValue("$old", (int)row.Phase);
        if (await q.ExecuteNonQueryAsync(ct) != 1) throw new InvalidOperationException("Batch phase changed concurrently.");
        await CommitCycle(c, t, before, ct);
    }

    private static void RequireSmallBatchTerminal(LocalCycleWork w)
    {
        if (w.Phase is not (LocalCycleWorkPhase.Published or LocalCycleWorkPhase.Conflict) || w.UploadId is null || w.ResultJson is null || w.InputJson is null || w.IntentHash is null || w.Size is null)
            throw new InvalidOperationException("Unknown original publication pins aggregate bytes.");
        var input = JsonSerializer.Deserialize<CycleUploadInput>(w.InputJson, LocalCycleWire.Json)!; using var result = JsonDocument.Parse(w.ResultJson);
        var phase = w.Phase == LocalCycleWorkPhase.Published ? "published" : "not_published";
        var wire = JsonSerializer.SerializeToUtf8Bytes(new { session = new { uploadId = w.UploadId, operationId = w.UploadId, rootId = input.Intent.RootId,
            intentHash = w.IntentHash, expectedSizeBytes = w.Size.Value.ToString(CultureInfo.InvariantCulture), receivedSizeBytes = w.ReceivedBytes.ToString(CultureInfo.InvariantCulture),
            phase, transferPending = false }, result = result.RootElement, replayed = true });
        _ = LocalCycleWire.Status(wire, input, w.IntentHash, w.UploadId);
    }

    internal async Task ForgetOwnedSmallBatch(string root, string owner, string id, string batch, LocalJournalAdmission local, CancellationToken ct, string? recoveryCurrent = null)
    {
        await CycleAdmission(root, owner, recoveryCurrent ?? id, local, LocalSmallBatchItems + 8, ct); await using var c = await OpenCycleConnection(ct); using var t = c.BeginTransaction(deferred: false);
        var before = await NamespaceChanges(c, t, ct);
        if (recoveryCurrent is null || recoveryCurrent == id) _ = await RequireCycle(c, t, root, owner, id, false, ct);
        else _ = await RequireHistoricalCycle(c, t, root, owner, recoveryCurrent, id, ct);
        await using var q = SyncCommand(c, t, "SELECT bytes FROM local_cycle_batches WHERE batch_id=$batch AND scope_id=$scope AND phase=4");
        q.Parameters.AddWithValue("$batch", batch); CycleId(q, id); if (await q.ExecuteScalarAsync(ct) is not long bytes) throw new InvalidOperationException("Retained batch must be cleaned before retirement.");
        q.CommandText = "DELETE FROM local_cycle_batch_items WHERE batch_id=$batch;DELETE FROM local_cycle_batches WHERE batch_id=$batch"; await q.ExecuteNonQueryAsync(ct);
        await AddCycleBytes(c, t, id, -bytes, ct); await CommitCycle(c, t, before, ct);
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task SaveSmallBatchOutcomeAsync(string id, string batch, SmallFileBatchOutcome outcome, CancellationToken ct)
        => SerialAsync(async () => { await _store.SaveOwnedSmallBatchOutcome(_root, _owner, id, batch, outcome, _admission, ct); return true; }, ct);
    internal Task BindSmallBatchSpoolAsync(string id, string batch, string identity, CancellationToken ct) => SerialAsync(async () => { await _store.BindOwnedSmallBatchSpool(_root, _owner, id, batch, identity, _admission, ct); return true; }, ct);
    internal Task EnableSmallBatchesAsync(string id, CancellationToken ct) => SerialAsync(async () => { await _store.EnableOwnedSmallBatches(_root, _owner, id, _admission, ct); return true; }, ct);
    internal Task<LocalSmallBatch> CreateSmallBatchAsync(string id, string batch, IReadOnlyList<string> operations, CancellationToken ct) => SerialAsync(() => _store.CreateOwnedSmallBatch(_root, _owner, id, batch, operations, _admission, ct), ct);
    internal Task<LocalSmallBatch> CreateSmallExecutionJobAsync(string id, string batch, IReadOnlyList<string> operations, CancellationToken ct) => SerialAsync(() => _store.CreateOwnedSmallBatch(_root, _owner, id, batch, operations, _admission, ct, true), ct);
    internal Task<LocalSmallBatch> ReadSmallBatchAsync(string id, string batch, CancellationToken ct) => SerialAsync(() => _store.ReadOwnedSmallBatch(_root, _owner, id, batch, ct), ct);
    internal Task ChangeSmallBatchAsync(string id, string batch, LocalSmallBatchPhase phase, string? hash, CancellationToken ct) => SerialAsync(async () => { await _store.ChangeOwnedSmallBatch(_root, _owner, id, batch, phase, hash, _admission, ct); return true; }, ct);
    internal Task ForgetSmallBatchAsync(string id, string batch, CancellationToken ct) => SerialAsync(async () => { await _store.ForgetOwnedSmallBatch(_root, _owner, id, batch, _admission, ct); return true; }, ct);
}
