namespace Neptune.Core;

public sealed record LocalSmallBatchReference(string ScopeId, string BatchId, LocalSmallBatchPhase Phase);

public sealed partial class NeptuneStateStore
{
    internal async Task<IReadOnlyList<LocalSmallBatchReference>> ListOwnedSmallBatches(string root, string owner, string current, string previous, CancellationToken ct)
    {
        await using var c = await OpenCycleConnection(ct); using var t = c.BeginTransaction(deferred: true);
        if (current == previous) _ = await RequireCycle(c, t, root, owner, current, true, ct);
        else _ = await RequireHistoricalCycle(c, t, root, owner, current, previous, ct);
        if (!await HasCycleBatchState(c, t, ct)) return [];
        await using var q = SyncCommand(c, t, "SELECT batch_id,phase FROM local_cycle_batches WHERE scope_id=$scope ORDER BY phase,batch_id LIMIT 65");
        CycleId(q, previous); await using var reader = await q.ExecuteReaderAsync(ct); var rows = new List<LocalSmallBatchReference>();
        while (await reader.ReadAsync(ct))
        {
            if (rows.Count >= LocalSmallBatchCapacity) throw new InvalidDataException("Retained aggregate inventory exceeds its fixed bound.");
            rows.Add(new(previous, reader.GetString(0), (LocalSmallBatchPhase)reader.GetInt32(1)));
        }
        return rows.AsReadOnly();
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task<IReadOnlyList<LocalSmallBatchReference>> ListSmallBatchesAsync(string current, string previous, CancellationToken ct)
        => SerialAsync(() => _store.ListOwnedSmallBatches(_root, _owner, current, previous, ct), ct);
    internal Task<LocalSmallBatch> ReadHistoricalSmallBatchAsync(string current, string previous, string batch, CancellationToken ct)
        => SerialAsync(() => _store.ReadOwnedSmallBatch(_root, _owner, previous, batch, ct, current), ct);
    internal Task ChangeHistoricalSmallBatchAsync(string current, string previous, string batch, LocalSmallBatchPhase phase, CancellationToken ct)
        => SerialAsync(async () => { await _store.ChangeOwnedSmallBatch(_root, _owner, previous, batch, phase, null, _admission, ct, current); return true; }, ct);
    internal Task ForgetHistoricalSmallBatchAsync(string current, string previous, string batch, CancellationToken ct)
        => SerialAsync(async () => { await _store.ForgetOwnedSmallBatch(_root, _owner, previous, batch, _admission, ct, current); return true; }, ct);
}
