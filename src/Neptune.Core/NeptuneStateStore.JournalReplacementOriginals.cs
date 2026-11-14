using System.Text.Json;
using System.Text;
using Microsoft.Data.Sqlite;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;
public sealed partial class NeptuneStateStore
{
    internal async Task<bool> HasReplacementDirectoryPublication(string root,string owner,string current,LocalCycleEntry entry,SyncDescriptor remote,CancellationToken ct)
    {
        if(!entry.Directory||await PreflightCycleAsync(ct)<5)return false;await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);var scope=await RequireCycle(c,t,root,owner,current,false,ct);
        foreach(var prior in await InstalledPublicationAncestors(c,t,scope,ct))
        {
            await using var work=SyncCommand(c,t,$"SELECT {CycleWorkColumns} FROM local_cycle_work WHERE scope_id=$scope AND node_id=$node AND phase=5 ORDER BY work_no DESC LIMIT 1");work.Parameters.AddWithValue("$scope",prior.ScopeId);work.Parameters.AddWithValue("$node",entry.NodeId);await using var reader=await work.ExecuteReaderAsync(ct);if(!await reader.ReadAsync(ct))continue;var w=CycleWork(reader);
            if(!w.Entry.Directory||w.Entry.Created!=entry.Created||w.Entry.RelativePath!=entry.RelativePath||w.InputJson is null||w.ResultJson is null)continue;var input=JsonSerializer.Deserialize<CycleFolderInput>(w.InputJson,LocalCycleWire.Json)!;var outcome=LocalCycleWire.Folder(Encoding.UTF8.GetBytes(w.ResultJson),input);
            if(outcome.Description.NodeId==remote.NodeId&&remote.Type=="folder"&&remote.Name==entry.RawName&&remote.ParentId==input.ParentId&&input.RootId==scope.Configuration.Binding.RootId)return true;
        }
        return false;
    }
    private static async Task RetainReplacementOriginal(SqliteConnection c,SqliteTransaction t,string current,LocalCycleWork work,string status,CancellationToken ct)
    {
        await using var present=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='local_cycle_replacement_requests'");if(await present.ExecuteScalarAsync(ct)is null)return;
        await using var q=SyncCommand(c,t,"INSERT INTO local_cycle_replacement_originals(replacement_id,operation_id,work_no,original_scope,node_id,kind,input_hash,outcome_hash,status) SELECT r.replacement_id,w.operation_id,w.work_no,w.scope_id,w.node_id,$kind,$input,$outcome,$status FROM local_cycle_replacement_requests r JOIN local_cycle_work w ON w.operation_id=$operation AND w.scope_id=r.original_scope WHERE r.new_scope=$scope ON CONFLICT(replacement_id,operation_id) DO UPDATE SET outcome_hash=excluded.outcome_hash,status=excluded.status WHERE local_cycle_replacement_originals.input_hash=excluded.input_hash AND local_cycle_replacement_originals.original_scope=excluded.original_scope");
        q.Parameters.AddWithValue("$scope",current);q.Parameters.AddWithValue("$operation",work.OperationId);q.Parameters.AddWithValue("$kind",work.Entry.Directory?1:0);q.Parameters.AddWithValue("$input",CycleHash(work.InputJson!));q.Parameters.AddWithValue("$outcome",work.ResultJson is null?DBNull.Value:(object)CycleHash(work.ResultJson));q.Parameters.AddWithValue("$status",status);await q.ExecuteNonQueryAsync(ct);
    }
    internal async Task<LocalCycleWork> ReadOwnedDirectoryRecovery(string root,string owner,string current,string previous,string operation,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireHistoricalCycle(c,t,root,owner,current,previous,ct);var w=await ReadCycleWork(c,t,operation,ct);
        if(w.ScopeId!=previous||!w.Entry.Directory||w.InputJson is null)throw new InvalidDataException("Directory recovery requires the immutable original guarded intent.");var input=JsonSerializer.Deserialize<CycleFolderInput>(w.InputJson,LocalCycleWire.Json)!;LocalCycleWire.FolderInput(input);if(input.OperationId!=w.OperationId)throw new InvalidDataException("Original directory operation identity changed.");return w;
    }
    internal async Task SaveOwnedDirectoryRecovery(string root,string owner,string current,string previous,string operation,CycleFolderOutcome outcome,LocalJournalAdmission admission,CancellationToken ct)
    {
        await CycleAdmission(root,owner,current,admission,5,ct);await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,t,ct);_=await RequireHistoricalCycle(c,t,root,owner,current,previous,ct);var w=await ReadCycleWork(c,t,operation,ct);
        if(w.ScopeId!=previous||!w.Entry.Directory||w.InputJson is null)throw new InvalidDataException("Original directory intent is absent.");var input=JsonSerializer.Deserialize<CycleFolderInput>(w.InputJson,LocalCycleWire.Json)!;var checkedOutcome=LocalCycleWire.Folder(Encoding.UTF8.GetBytes(outcome.RawJson),input);if(checkedOutcome.Description!=outcome.Description||checkedOutcome.IntentHash!=outcome.IntentHash)throw new InvalidDataException("Directory registry result changed.");
        if(w.Phase==LocalCycleWorkPhase.Published&&w.ResultJson is not null)
        {var old=LocalCycleWire.Folder(Encoding.UTF8.GetBytes(w.ResultJson),input);if(old.Description!=checkedOutcome.Description||old.IntentHash!=checkedOutcome.IntentHash)throw new InvalidDataException("Historical directory outcome cannot regress.");}
        else
        {await using var q=SyncCommand(c,t,"UPDATE local_cycle_work SET phase=5,result_json=$result,error=NULL WHERE operation_id=$operation");q.Parameters.AddWithValue("$operation",operation);q.Parameters.AddWithValue("$result",outcome.RawJson);await q.ExecuteNonQueryAsync(ct);w=await ReadCycleWork(c,t,operation,ct);}
        await RetainReplacementOriginal(c,t,current,w,"published",ct);await CommitCycle(c,t,before,ct);
        // Stored historical registry outcome ONLY. B/R/dirty/cursor untouched.
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<bool> HasReplacementDirectoryPublicationAsync(string current,LocalCycleEntry entry,SyncDescriptor remote,CancellationToken ct)=>SerialAsync(()=>_store.HasReplacementDirectoryPublication(_root,_owner,current,entry,remote,ct),ct);
    internal Task<LocalCycleWork> ReadDirectoryRecoveryAsync(string current,string previous,string operation,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedDirectoryRecovery(_root,_owner,current,previous,operation,ct),ct);
    internal Task SaveDirectoryRecoveryAsync(string current,string previous,string operation,CycleFolderOutcome outcome,CancellationToken ct)=>SerialAsync(async()=>{await _store.SaveOwnedDirectoryRecovery(_root,_owner,current,previous,operation,outcome,_admission,ct);return true;},ct);
}
