using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    private static async Task<Dictionary<string,CoherenceNode>> ParentAliasNodes(SqliteConnection c,SqliteTransaction t,string coherence,IReadOnlyList<LocalDirectoryAlias> aliases,CancellationToken ct)
    {
        if(aliases.Select(a=>a.FileId64).Distinct(StringComparer.Ordinal).Count()!=aliases.Count)
            throw new LocalSourceGapException("ParentAliasAmbiguity","Native page contains multiple identity aliases.");
        var result=new Dictionary<string,CoherenceNode>(StringComparer.Ordinal);
        var wires=JsonSerializer.Serialize(aliases.Select(a=>a.FileId64));
        var lookup=await ReadCoherenceParentLookup(c,t,coherence,ct);
        if(lookup.Plans is null||lookup.Plans.Count>0)
        {
            var sql=lookup.Plans is null
                ? "SELECT j.value,e.node_id,e.created,e.kind FROM json_each($wires) j CROSS JOIN local_parent_reconcile_versions v LEFT JOIN local_parent_reconcile_plans p ON p.plan_id=v.pending_plan JOIN local_parent_reconcile_entries e ON e.plan_id=CASE WHEN p.status=3 THEN v.pending_plan ELSE v.current_plan END AND e.parent_id=v.parent_id WHERE v.coherence_id=$coherence AND e.wire64=j.value"
                : $"SELECT j.value,e.node_id,e.created,e.kind FROM json_each($wires) j CROSS JOIN local_parent_reconcile_entries e CROSS JOIN local_parent_reconcile_versions v LEFT JOIN local_parent_reconcile_plans p ON p.plan_id=v.pending_plan WHERE e.plan_id IN({string.Join(',',Enumerable.Range(0,lookup.Plans.Count).Select(n=>"$plan"+n))}) AND e.wire64=j.value AND v.coherence_id=$coherence AND v.parent_id=e.parent_id AND e.plan_id=CASE WHEN p.status=3 THEN v.pending_plan ELSE v.current_plan END";
            await using var q=SyncCommand(c,t,sql);CoherenceId(q,coherence);q.Parameters.AddWithValue("$wires",wires);
            if(lookup.Plans is not null)for(var n=0;n<lookup.Plans.Count;n++)q.Parameters.AddWithValue("$plan"+n,lookup.Plans[n]);
            await using var r=await q.ExecuteReaderAsync(ct);
            while(await r.ReadAsync(ct))if(!result.TryAdd(r.GetString(0),new(r.GetString(1),r.GetInt64(2),r.GetInt32(3))))
                throw new LocalSourceGapException("ParentAliasAmbiguity","Current version contains multiple hardlink/identity aliases.");
        }
        await using(var q=SyncCommand(c,t,"SELECT j.value,n.node_id,n.created,n.kind FROM json_each($wires) j CROSS JOIN local_coherence_nodes n WHERE n.coherence_id=$coherence AND n.wire64=j.value"))
        {CoherenceId(q,coherence);q.Parameters.AddWithValue("$wires",wires);await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))result.TryAdd(r.GetString(0),new(r.GetString(1),r.GetInt64(2),r.GetInt32(3)));}
        return result;
    }
}
