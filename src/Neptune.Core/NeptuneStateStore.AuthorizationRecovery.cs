namespace Neptune.Core;

internal sealed record AuthorizationWait(string Operation, string Kind, int Attempts);

public sealed partial class NeptuneStateStore
{
    internal async Task<IReadOnlyList<AuthorizationWait>> ReadAuthorizationWaits(string root, string owner, string scope, CancellationToken ct)
    {
        await using var c = await OpenCycleConnection(ct); using var t = c.BeginTransaction(deferred: true);
        _ = await RequireCycle(c, t, root, owner, scope, true, ct);
        await using var q = SyncCommand(c, t, "SELECT operation_id,kind,attempts FROM client_operation_schedule WHERE scope_id=$scope AND state='AUTH_REQUIRED' ORDER BY CASE WHEN kind='cycle' THEN 1 ELSE 0 END,operation_id LIMIT 120");
        CycleId(q, scope); var result = new List<AuthorizationWait>();
        await using var reader = await q.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) result.Add(new(reader.GetString(0), reader.GetString(1), reader.GetInt32(2)));
        return result;
    }

    internal async Task ResumeAuthorizedOperation(string root, string owner, string scope, AuthorizationWait original, CancellationToken ct)
    {
        await ClientAdmission(root, owner, scope, 2, ct);
        await using var c = await OpenCycleConnection(ct); using var t = c.BeginTransaction(deferred: false);
        _ = await RequireCycle(c, t, root, owner, scope, true, ct);
        await using var q = SyncCommand(c, t, "UPDATE client_operation_schedule SET state='RETRY_WAIT',not_before=0 WHERE scope_id=$scope AND operation_id=$operation AND kind=$kind AND state='AUTH_REQUIRED' AND attempts=$attempts");
        CycleId(q, scope); q.Parameters.AddWithValue("$operation", original.Operation); q.Parameters.AddWithValue("$kind", original.Kind); q.Parameters.AddWithValue("$attempts", original.Attempts);
        ct.ThrowIfCancellationRequested(); await q.ExecuteNonQueryAsync(ct); await t.CommitAsync(ct);
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task<IReadOnlyList<AuthorizationWait>> ReadAuthorizationWaitsAsync(string scope, CancellationToken ct)
        => SerialAsync(() => _store.ReadAuthorizationWaits(_root, _owner, scope, ct), ct);
    internal Task ResumeAuthorizedOperationAsync(string scope, AuthorizationWait original, CancellationToken ct)
        => SerialAsync(async () => { await _store.ResumeAuthorizedOperation(_root, _owner, scope, original, ct); return true; }, ct);
}
