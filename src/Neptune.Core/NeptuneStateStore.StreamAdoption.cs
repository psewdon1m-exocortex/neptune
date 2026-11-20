using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private static async Task<int> ReconsiderUnattemptedLegacyStreamConflicts(SqliteConnection c,SqliteTransaction t,
        LocalCycleScope scope,CancellationToken ct)
    {
        if(!scope.Configuration.AllowVerifiedEqualAdoption)return 0;
        const string legacyError="CycleConflictException: Streaming cannot overwrite an unknown remote version.";
        var exclusions="";
        foreach(var table in new[]{"client_stream_operations","client_metadata_seals","client_namespace_operations"})
        {
            await using var exists=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE type='table' AND name=$name");
            exists.Parameters.AddWithValue("$name",table);
            if(await exists.ExecuteScalarAsync(ct)is not null)
                exclusions+=" AND NOT EXISTS(SELECT 1 FROM "+table+" e WHERE e.operation_id=w.operation_id)";
        }
        var nodes=new List<string>();
        await using(var q=SyncCommand(c,t,"SELECT f.node_id FROM local_cycle_files f JOIN local_cycle_work w ON w.scope_id=f.scope_id AND w.node_id=f.node_id WHERE f.scope_id=$scope AND f.phase=2 AND w.phase=7 AND w.active IS NULL AND w.error=$error AND w.capture_proof IS NULL AND w.spool_path IS NULL AND w.size IS NULL AND w.sha256 IS NULL AND w.input_json IS NULL AND w.intent_hash IS NULL AND w.upload_id IS NULL AND w.result_json IS NULL AND w.received=0 AND w.work_no=(SELECT MAX(latest.work_no) FROM local_cycle_work latest WHERE latest.scope_id=f.scope_id AND latest.node_id=f.node_id) AND NOT EXISTS(SELECT 1 FROM local_cycle_work active WHERE active.scope_id=f.scope_id AND active.node_id=f.node_id AND active.active=1)"+exclusions+" ORDER BY f.node_id LIMIT 120"))
        {
            CycleId(q,scope.ScopeId);q.Parameters.AddWithValue("$error",legacyError);
            await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))nodes.Add(r.GetString(0));
        }
        foreach(var node in nodes)
        {
            // Preserve the terminal conflict row and its identity. Normal
            // claiming may make a new, unattempted verification job; no old
            // captured original, upload key, receipt, or UNKNOWN is reset.
            await using var q=SyncCommand(c,t,"UPDATE local_cycle_files SET phase=0 WHERE scope_id=$scope AND node_id=$node AND phase=2");
            CycleId(q,scope.ScopeId);q.Parameters.AddWithValue("$node",node);
            if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidOperationException("Legacy conflict changed before reconsideration.");
        }
        return nodes.Count;
    }
}
