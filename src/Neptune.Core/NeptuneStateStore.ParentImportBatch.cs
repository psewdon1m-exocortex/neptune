using System.Text.Json;
using Microsoft.Data.Sqlite;
namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    private sealed record ParentKnown(string Logical,string Path,long Created,int Kind);
    private static async Task<Dictionary<string,ParentKnown>> ParentKnownPage(SqliteConnection c,SqliteTransaction t,string scope,IReadOnlyList<ParentReconcileEntry> entries,CancellationToken ct)
    {
        var rows=new Dictionary<string,ParentKnown>(StringComparer.Ordinal);
        await using var q=SyncCommand(c,t,"SELECT j.value,f.node_id,f.path,f.created,f.kind FROM json_each($nodes) j CROSS JOIN local_cycle_files f WHERE f.scope_id=$scope AND f.node_id=j.value AND NOT EXISTS(SELECT 1 FROM local_parent_reconcile_incarnations i WHERE i.scope_id=f.scope_id AND i.node_id=f.node_id) UNION ALL SELECT j.value,f.node_id,f.path,f.created,f.kind FROM json_each($nodes) j CROSS JOIN local_parent_reconcile_incarnations i CROSS JOIN local_cycle_files f ON f.scope_id=i.scope_id AND f.node_id=i.node_id WHERE i.scope_id=$scope AND i.native_id=j.value");
        q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$nodes",JsonSerializer.Serialize(entries.Select(e=>e.Node).Distinct(StringComparer.Ordinal)));await using var r=await q.ExecuteReaderAsync(ct);
        while(await r.ReadAsync(ct))if(!rows.TryAdd(r.GetString(0),new(r.GetString(1),r.GetString(2),r.GetInt64(3),r.GetInt32(4))))throw new LocalSourceGapException("ParentAliasAmbiguity","Native identity resolves to multiple retained originals.");
        return rows;
    }
    private static async Task RestoreKnownParentPage(SqliteConnection c,SqliteTransaction t,string scope,IReadOnlyList<string> nodes,CancellationToken ct)
    {
        if(nodes.Count==0)return;
        var restored=new List<string>();
        await using(var q=SyncCommand(c,t,"DELETE FROM local_parent_reconcile_conflicts WHERE scope_id=$scope AND node_id IN(SELECT value FROM json_each($nodes)) AND (reason LIKE 'Previously known source item is absent%' OR reason LIKE 'Managed object absent from complete root membership%' OR reason LIKE 'Different current native identity occupies%') RETURNING node_id"))
        {q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$nodes",JsonSerializer.Serialize(nodes));await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))restored.Add(r.GetString(0));}
        if(restored.Count==0)return;
        await using var update=SyncCommand(c,t,"UPDATE local_cycle_files AS f SET phase=CASE WHEN EXISTS(SELECT 1 FROM local_cycle_work w WHERE w.scope_id=f.scope_id AND w.node_id=f.node_id AND w.active=1) THEN 1 ELSE 0 END WHERE f.scope_id=$scope AND f.node_id IN(SELECT value FROM json_each($nodes))");
        update.Parameters.AddWithValue("$scope",scope);update.Parameters.AddWithValue("$nodes",JsonSerializer.Serialize(restored));await update.ExecuteNonQueryAsync(ct);
    }
}
