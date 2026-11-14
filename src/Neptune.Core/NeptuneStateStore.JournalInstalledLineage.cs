using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    // A replacement can adopt a receipt without creating another publication.
    // Follow only installed, immutable request edges back to the actual receipt;
    // never treat an adopted baseline or matching remote name as ownership.
    private static async Task<IReadOnlyList<LocalCycleScope>> InstalledPublicationAncestors(
        SqliteConnection c,SqliteTransaction t,LocalCycleScope live,CancellationToken ct)
    {
        var result=new List<LocalCycleScope>();
        await using(var present=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='local_cycle_replacement_requests'"))
            if(await present.ExecuteScalarAsync(ct)is null)return result;
        var unsuperseded=await CycleFormat(c,t,ct)>=6?" AND superseded_by IS NULL":"";
        var seen=new HashSet<string>(StringComparer.Ordinal){live.ScopeId};
        var next=live;
        var configuration=JsonSerializer.Serialize(live.Configuration,LocalCycleWire.Json);
        while(true)
        {
            string? previous;
            await using(var q=SyncCommand(c,t,"SELECT original_scope FROM local_cycle_replacement_requests WHERE new_scope=$scope AND status=3"+unsuperseded))
            {q.Parameters.AddWithValue("$scope",next.ScopeId);previous=await q.ExecuteScalarAsync(ct)as string;}
            if(previous is null)return result;
            if(seen.Count>=16||!seen.Add(previous))throw new InvalidDataException("Installed publication lineage is cyclic or exceeds retained scope capacity.");
            var prior=await CycleScope(c,t,previous,ct);
            if(prior.Revision!=live.Revision||JsonSerializer.Serialize(prior.Configuration,LocalCycleWire.Json)!=configuration
                ||!await HasReplacementLineage(c,t,next.ScopeId,previous,next.Root,prior.Root,live.Configuration,true,ct))
                throw new InvalidDataException("Installed publication lineage changed principal, configuration or revision.");
            await using(var q=SyncCommand(c,t,"SELECT old.binding_json,new.binding_json,r.original_binding,r.new_binding FROM local_cycle_replacement_requests r JOIN local_source_roots old ON old.root_key=r.original_root JOIN local_source_roots new ON new.root_key=r.new_root WHERE r.new_scope=$scope AND r.original_scope=$previous"))
            {
                q.Parameters.AddWithValue("$scope",next.ScopeId);q.Parameters.AddWithValue("$previous",previous);
                await using var r=await q.ExecuteReaderAsync(ct);
                if(!await r.ReadAsync(ct)||r.GetString(0)!=r.GetString(2)||r.GetString(1)!=r.GetString(3))
                    throw new InvalidDataException("Installed publication lineage lost its immutable native binding.");
                var oldBinding=JsonSerializer.Deserialize<LocalSourceBinding>(r.GetString(0),SyncStateJson)!;
                var newBinding=JsonSerializer.Deserialize<LocalSourceBinding>(r.GetString(1),SyncStateJson)!;
                if(LocalHash(r.GetString(0))!=prior.Root||LocalHash(r.GetString(1))!=next.Root
                    ||newBinding.SubscriptionGeneration<=oldBinding.SubscriptionGeneration
                    ||(oldBinding with{SubscriptionGeneration=newBinding.SubscriptionGeneration})!=newBinding)
                    throw new InvalidDataException("Installed publication lineage cannot borrow another native incarnation.");
            }
            result.Add(prior);next=prior;
        }
    }
}
