using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private sealed record CoherenceParentLookup(IReadOnlyList<string>? Plans);

    // A full-tree reconciliation may leave thousands of parent version rows
    // pointing at one plan. Search its existing unique ID index first, then
    // qualify only the matched parent's current/pending pointer. The shortlist
    // belongs to this transaction; it cannot outlive a version activation.
    private static async Task<CoherenceParentLookup> ReadCoherenceParentLookup(SqliteConnection c,SqliteTransaction t,string id,CancellationToken ct)
    {
        var plans=new List<string>(65);
        // Cycle history has a separately enforced 16-scope bound. The plan
        // index uses each exact scope prefix; no parent/entry scan is needed.
        await using var q=SyncCommand(c,t,"SELECT p.plan_id FROM local_cycle_scopes s CROSS JOIN local_parent_reconcile_plans p INDEXED BY local_parent_reconcile_history WHERE s.coherence_id=$coherence AND p.scope_id=s.scope_id AND p.coherence_id=$coherence LIMIT 65");
        CoherenceId(q,id);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))plans.Add(r.GetString(0));
        // Preserve the previous exact lookup for fragmented histories. A
        // truncated shortlist must never certify an absent current identity.
        return new(plans.Count==65?null:plans);
    }
}
