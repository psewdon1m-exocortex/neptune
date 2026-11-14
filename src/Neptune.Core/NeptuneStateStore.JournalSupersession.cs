using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private sealed record ReplacementGapEvidence(string Schema,string ReplacementId,string ScopeId,string Root,string Binding,
        string Journal,long Cursor,long GapEpoch,string FirstGap,long OwnerEpoch,long Revision,LocalJournalBounds NativeBounds);
    private sealed record ReplacementSupersession(string PreviousId,string Root,LocalSourceBinding Binding,string GapJson,bool Reopen);
    private static async Task<int> CycleFormat(SqliteConnection c,SqliteTransaction t,CancellationToken ct)
    {await using var q=SyncCommand(c,t,"SELECT version FROM local_cycle_format WHERE singleton=1");return Convert.ToInt32(await q.ExecuteScalarAsync(ct));}

    private static async Task<ReplacementSupersession?> ReadReplacementSupersession(SqliteConnection c,SqliteTransaction t,
        JournalGapReplacementRequest request,ReplacementOriginal original,LocalJournalBounds bounds,CancellationToken ct)
    {
        var version=await CycleFormat(c,t,ct);string? savedPrevious=null,savedGap=null;var saved=false;
        if(version>=6)
        {
            await using var q=SyncCommand(c,t,"SELECT supersedes_id,superseded_by,replacement_gap_json FROM local_cycle_replacement_requests WHERE replacement_id=$id");q.Parameters.AddWithValue("$id",request.ReplacementId);await using var r=await q.ExecuteReaderAsync(ct);
            if(await r.ReadAsync(ct))
            {
                saved=true;savedPrevious=r.IsDBNull(0)?null:r.GetString(0);savedGap=r.IsDBNull(2)?null:r.GetString(2);
                if(!r.IsDBNull(1))throw new LocalSourceGapException("ReplacementSuperseded","This request was explicitly superseded; its original evidence remains retained.");
                if(savedPrevious!=request.SupersedesReplacementId)throw new InvalidDataException("Immutable supersession request changed.");
            }
        }
        if(request.SupersedesReplacementId is not{} previous)return null;
        ReplacementId(previous);if(previous==request.ReplacementId||version<5)throw new InvalidDataException("Supersession requires a different existing explicit request.");
        string failedScope,failedRoot,failedBinding;
        await using(var q=SyncCommand(c,t,"SELECT original_scope,new_scope,new_root,new_binding,configuration,status"+(version>=6?",superseded_by":"")+" FROM local_cycle_replacement_requests WHERE replacement_id=$id"))
        {
            q.Parameters.AddWithValue("$id",previous);await using var r=await q.ExecuteReaderAsync(ct);
            if(!await r.ReadAsync(ct)||r.GetString(0)!=request.OriginalScopeId||r.GetString(4)!=JsonSerializer.Serialize(original.Scope.Configuration,LocalCycleWire.Json)||r.GetInt32(5)==3)
                throw new LocalSourceGapException("ReplacementSupersessionIdentity","Only the specified unfinished request for these immutable originals can be superseded.");
            var child=version>=6&&!r.IsDBNull(6)?r.GetString(6):null;
            if(child!=(saved?request.ReplacementId:null))throw new LocalSourceGapException("ReplacementSuperseded","The previous request already has a different explicit successor.");
            failedScope=r.GetString(1);failedRoot=r.GetString(2);failedBinding=r.GetString(3);
        }
        var failed=await ReadReplacementOriginal(c,t,failedScope,ct);
        if(failed.Scope.Root!=failedRoot||failed.BindingJson!=failedBinding||failed.Revision!=original.Revision||failed.Scope.Revision!=original.Revision
            ||failed.Binding with{SubscriptionGeneration=original.Binding.SubscriptionGeneration}!=original.Binding
            ||JsonSerializer.Serialize(failed.Scope.Configuration,LocalCycleWire.Json)!=JsonSerializer.Serialize(original.Scope.Configuration,LocalCycleWire.Json))
            throw new LocalSourceGapException("ReplacementSupersessionIdentity","Failed candidate identity, configuration or revision changed.");
        await using(var q=SyncCommand(c,t,"SELECT root_key FROM local_source_active WHERE mapping_id=$mapping"))
        {
            q.Parameters.AddWithValue("$mapping",failed.Binding.MappingId);var active=await q.ExecuteScalarAsync(ct)as string;
            if(!saved&&active!=failedRoot)throw new LocalSourceGapException("ReplacementSuperseded","Failed candidate is no longer the active subscription.");
        }
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_cycle_work WHERE scope_id=$scope LIMIT 1"))
        {q.Parameters.AddWithValue("$scope",failedScope);if(await q.ExecuteScalarAsync(ct)is not null)throw new LocalSourceGapException("ReplacementCandidateWork","Candidate work must not be orphaned by supersession.");}
        if(!failed.Valid&&failed.Code is not("JournalGap" or "InboxCapacity"))throw new LocalSourceGapException("ReplacementCause","Unrelated candidate uncertainty cannot be superseded.");
        var observed=bounds;
        if(saved)
        {
            if(savedGap is null||System.Text.Encoding.UTF8.GetByteCount(savedGap)>16*1024)throw new InvalidDataException("Supersession native loss evidence is absent or unbounded.");
            var evidence=JsonSerializer.Deserialize<ReplacementGapEvidence>(savedGap,LocalCycleWire.Json)??throw new InvalidDataException("Supersession native evidence is invalid.");observed=evidence.NativeBounds;
        }
        var gap=new ReplacementGapEvidence("neptune.native-replacement-gap.v1",previous,failedScope,failedRoot,failed.BindingJson,failed.Journal,failed.Cursor,failed.Gap,failed.Code,failed.Owner,failed.Revision,observed);
        var raw=JsonSerializer.Serialize(gap,LocalCycleWire.Json);
        if(saved&&raw!=savedGap)throw new InvalidDataException("Retained failed candidate or immutable native-loss proof changed.");
        var lost=false;try{WindowsUsnJournalReader.ValidateCursor(observed,new(failed.Journal,failed.Cursor,failed.Owner,failed.Valid));}catch(LocalSourceGapException e)when(e.Code=="JournalGap"){lost=true;}
        if(!lost)throw new LocalSourceGapException("ReplacementContinuityPresent","Resource pressure or a retained cursor cannot authorize another subscription; native journal loss is required.");
        return new(previous,failedRoot,failed.Binding with{SubscriptionGeneration=checked(failed.Binding.SubscriptionGeneration+1)},raw,saved);
    }
}
