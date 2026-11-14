using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    internal async Task RequireOwnedRetainedHistory(string root, string owner, string current, CancellationToken ct)
    {
        await using var c = await OpenCycleConnection(ct);
        using var t = c.BeginTransaction(deferred: true);
        var live = await RequireCycle(c, t, root, owner, current, false, ct);
        var previous = new List<string>(16);
        await using (var q = SyncCommand(c, t, "SELECT scope_id FROM local_cycle_scopes WHERE scope_id<>$scope ORDER BY scope_id LIMIT 17"))
        {
            q.Parameters.AddWithValue("$scope", current);
            await using var r = await q.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct)) previous.Add(r.GetString(0));
        }
        if (previous.Count > 16) throw new InvalidDataException("Guarded historical scope count exceeds its recognized bound.");
        var ancestors = await InstalledPublicationAncestors(c, t, live, ct);
        foreach (var scope in previous)
        {
            if(await HasClientOriginals(c,t,scope,true,ct))throw new InvalidDataException("Historical namespace/stream originals require recovery before another scope can publish.");
            await using var active = SyncCommand(c, t, "SELECT 1 FROM local_cycle_work INDEXED BY local_cycle_work_page WHERE scope_id=$scope AND active=1 LIMIT 1");
            active.Parameters.AddWithValue("$scope", scope);
            if (await active.ExecuteScalarAsync(ct) is null) continue;
            var index = -1;
            for (var i = 0; i < ancestors.Count; i++) if (ancestors[i].ScopeId == scope) { index = i; break; }
            if (index < 0) throw new InvalidDataException("Another active guarded scope has no installed replacement lineage.");
            var successor = index == 0 ? current : ancestors[index - 1].ScopeId;
            string replacement;
            await using (var q = SyncCommand(c, t, "SELECT r.replacement_id FROM local_cycle_replacement_requests r JOIN local_source_roots old ON old.root_key=r.original_root WHERE r.new_scope=$new AND r.original_scope=$old AND r.status=3 AND r.original_gap='JournalGap' AND old.valid=0 AND old.gap_code=r.original_gap AND old.cursor=r.original_cursor AND old.journal_id=r.original_journal AND old.gap_epoch=r.original_gap_epoch"))
            {
                q.Parameters.AddWithValue("$new", successor); q.Parameters.AddWithValue("$old", scope);
                replacement = await q.ExecuteScalarAsync(ct) as string ?? throw new InvalidDataException("Retained replacement lost its original typed gap.");
            }
            await using (var unknown = SyncCommand(c, t, "SELECT 1 FROM local_cycle_work INDEXED BY local_cycle_work_page WHERE scope_id=$scope AND active=1 AND phase NOT BETWEEN 0 AND 9 LIMIT 1"))
            {
                unknown.Parameters.AddWithValue("$scope", scope);
                if (await unknown.ExecuteScalarAsync(ct) is not null) throw new InvalidDataException("Unrecognized retained work phase.");
            }
            // Read bounded pages through the scope/active/phase/work index.
            // No old work is acknowledged, retired, or promoted into current B.
            for (var phase = 0; phase <= 9; phase++)
            {
                long after = 0;
                while (true)
                {
                    var page = new List<(long Number, string Node, LocalCycleWork Work)>(32);
                    await using (var q = SyncCommand(c, t, $"SELECT {CycleWorkColumns},work_no,node_id FROM local_cycle_work INDEXED BY local_cycle_work_page WHERE scope_id=$scope AND active=1 AND phase=$phase AND work_no>$after ORDER BY work_no LIMIT 32"))
                    {
                        q.Parameters.AddWithValue("$scope", scope); q.Parameters.AddWithValue("$phase", phase); q.Parameters.AddWithValue("$after", after);
                        await using var r = await q.ExecuteReaderAsync(ct);
                        while (await r.ReadAsync(ct)) page.Add((r.GetInt64(14), r.GetString(15), CycleWork(r)));
                    }
                    foreach (var item in page)
                    {
                        var w = item.Work;
                        if (w.Entry.NodeId != item.Node) throw new InvalidDataException("Retained work changed node identity.");
                        if (w.InputJson is null)
                        {
                            if (w.Phase is LocalCycleWorkPhase.Prepared or LocalCycleWorkPhase.Sending or LocalCycleWorkPhase.OutcomeUnknown or LocalCycleWorkPhase.Published)
                                throw new InvalidDataException("Attempted retained work lost its original input.");
                        }
                        else
                        {
                            var status = w.Phase == LocalCycleWorkPhase.Published ? "published" : w.Phase == LocalCycleWorkPhase.Conflict ? "not_published" : null;
                            if (status is null || status == "published" && w.ResultJson is null)
                                throw new InvalidDataException("Retained original outcome is unresolved.");
                            await using var q = SyncCommand(c, t, "SELECT 1 FROM local_cycle_replacement_originals WHERE replacement_id=$replacement AND operation_id=$operation AND work_no=$number AND original_scope=$scope AND node_id=$node AND kind=$kind AND input_hash=$input AND outcome_hash IS $outcome AND status=$status");
                            q.Parameters.AddWithValue("$replacement", replacement); q.Parameters.AddWithValue("$operation", w.OperationId);
                            q.Parameters.AddWithValue("$number", item.Number); q.Parameters.AddWithValue("$scope", scope); q.Parameters.AddWithValue("$node", item.Node);
                            q.Parameters.AddWithValue("$kind", w.Entry.Directory ? 1 : 0); q.Parameters.AddWithValue("$input", CycleHash(w.InputJson));
                            q.Parameters.AddWithValue("$outcome", w.ResultJson is null ? DBNull.Value : CycleHash(w.ResultJson)); q.Parameters.AddWithValue("$status", status);
                            if (await q.ExecuteScalarAsync(ct) is null) throw new InvalidDataException("Retained original differs from its qualified replacement outcome.");
                        }
                        after = item.Number;
                    }
                    if (page.Count < 32) break;
                }
            }
        }
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task RequireRetainedHistoryAsync(string current, CancellationToken ct) => SerialAsync(async () =>
    { await _store.RequireOwnedRetainedHistory(_root, _owner, current, ct); return true; }, ct);
}

public sealed partial class WindowsFreshSyncEngine
{
    public async Task RequireRetainedHistoryAsync(CancellationToken cancellationToken = default)
    {
        await _serial.WaitAsync(cancellationToken);
        try { ObjectDisposedException.ThrowIf(_disposed, this); await _session.RequireRetainedHistoryAsync(ScopeId, cancellationToken); }
        finally { _serial.Release(); }
    }
}
