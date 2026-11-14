using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    internal async Task<LocalCycleScope?> ResumeOwnedCycle(string root,string owner,WindowsCycleConfiguration configuration,
        WindowsCycleBudget budget,string spoolRoot,LocalJournalAdmission local,CancellationToken ct,bool parentReconciliation=false)
    {
        LocalCycleWire.Configuration(configuration);
        ValidateCycleBudget(budget);
        await EnsureCycleAsync(local,ct);await CheckLocalAdmissionAsync(local,0,NamespaceReserve(8),ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);
        var before=await NamespaceChanges(c,t,ct);var state=await ReadLocalStateAsync(c,t,root,owner,ct);
        await RequireNoPendingBindingMigration(c,t,ct);
        await RequireLocalMappingAsync(c,t,state.Binding,ct);
        string? id;await using(var q=SyncCommand(c,t,"SELECT scope_id FROM local_cycle_active WHERE root_key=$root"))
        {q.Parameters.AddWithValue("$root",root);id=await q.ExecuteScalarAsync(ct)as string;}
        if(id is null){await t.CommitAsync(ct);return null;}
        var scope=await CycleScope(c,t,id,ct);
        if(JsonSerializer.Serialize(configuration,LocalCycleWire.Json)!=JsonSerializer.Serialize(scope.Configuration,LocalCycleWire.Json))
            throw new LocalSourceGapException("CycleResumeBinding","Retained scope cannot reuse another configuration, anchor or authority.");
        var structural=parentReconciliation&&scope.Status==2&&scope.Error==StructuralHint;
        if(!state.Cursor.Valid||state.LegacyRevision!=await LocalRevisionAsync(c,t,ct)||scope.Revision!=state.LegacyRevision||scope.Gap!=state.GapEpoch||scope.Status==3||scope.Status==2&&!structural)
            throw new LocalSourceGapException("CycleResumeGap","Retained source continuity, mapping or revision is uncertain; original evidence remains intact.");
        if(!scope.SeedComplete||scope.CoherenceId is null)
        {
            // Admission can fail before the first namespace observation. Hand
            // off that same empty scope, without inventing a retained anchor.
            // Once any candidate/file/original exists the ordinary refusal stays.
            if(!scope.SeedComplete&&scope.CoherenceId is null&&scope.SeedAfter==0&&scope.Files==0&&scope.Status==0
                &&scope.OwnerEpoch>=1&&scope.OwnerEpoch<=state.Cursor.OwnerEpoch)
            {
                await using var empty=SyncCommand(c,t,"""
                    SELECT EXISTS(SELECT 1 FROM local_cycle_files WHERE scope_id=$scope)
                        OR EXISTS(SELECT 1 FROM local_cycle_work WHERE scope_id=$scope)
                        OR EXISTS(SELECT 1 FROM local_namespace_candidates WHERE root_key=$root)
                        OR EXISTS(SELECT 1 FROM local_coherence_roots WHERE root_key=$root)
                    """);
                CycleId(empty,id);empty.Parameters.AddWithValue("$root",root);
                if(Convert.ToInt64(await empty.ExecuteScalarAsync(ct))==0)
                {
                    await using var handoff=SyncCommand(c,t,"UPDATE local_cycle_scopes SET owner_epoch=$epoch,max_profile=$profile,free_floor=$floor WHERE scope_id=$scope");
                    CycleId(handoff,id);handoff.Parameters.AddWithValue("$epoch",state.Cursor.OwnerEpoch);handoff.Parameters.AddWithValue("$profile",budget.ProfileBytes);handoff.Parameters.AddWithValue("$floor",budget.MinimumFreeDiskBytes);
                    if(await handoff.ExecuteNonQueryAsync(ct)!=1)throw new InvalidDataException("Empty cycle owner handoff lost its scope.");
                    scope=await RequireCycle(c,t,root,owner,id,false,ct);await CommitCycle(c,t,before,ct);return scope;
                }
            }
            throw new LocalSourceGapException("CycleResumeIncomplete","Interrupted namespace indexing cannot be called a retained coherent anchor.");
        }
        var coherence=await ReadCoherenceAsync(c,t,scope.CoherenceId,ct);var candidate=await ReadNamespaceAsync(c,t,coherence.State.CandidateId,ct);
        await using(var incarnation=SyncCommand(c,t,"SELECT 1 FROM local_namespace_candidates WHERE candidate_id=$candidate AND root_created=$created"))
        {incarnation.Parameters.AddWithValue("$candidate",coherence.State.CandidateId);incarnation.Parameters.AddWithValue("$created",state.Binding.RootIncarnation);if(await incarnation.ExecuteScalarAsync(ct)is null)throw new LocalSourceGapException("CycleResumeCandidate","Retained native candidate incarnation differs; original evidence retained.");}
        // Published receipts can retain their seals until the next remote refresh.
        // Bound each read page, not the total number of recoverable originals.
        // The ownership transaction keeps the keyset stable through validation.
        var originalSpoolRoot=Path.GetFullPath(spoolRoot);long afterWork=0;
        while(true)
        {
            ct.ThrowIfCancellationRequested();
            await using var q=SyncCommand(c,t,"SELECT work_no,operation_id,spool_path FROM local_cycle_work WHERE scope_id=$scope AND active=1 AND spool_path IS NOT NULL AND work_no>$after ORDER BY work_no LIMIT 64");
            CycleId(q,id);q.Parameters.AddWithValue("$after",afterWork);
            await using var r=await q.ExecuteReaderAsync(ct);var seals=0;
            while(await r.ReadAsync(ct))
            {
                afterWork=r.GetInt64(0);seals++;
                if(r.GetString(2)!=Path.Combine(originalSpoolRoot,r.GetString(1)+".spool"))
                    throw new LocalSourceGapException("CycleResumeSpool","Retained sealed work cannot rebind its original owned spool.");
            }
            if(seals<64)break;
        }
        var previous=scope.OwnerEpoch;var current=state.Cursor.OwnerEpoch;
        var scoped=parentReconciliation&&coherence.State.Status==LocalCoherenceStatus.Uncertain&&coherence.State.Error is("ScopedNamespaceTransitionUnresolved" or "HardlinkMembershipUnresolved");
        if(previous<1||previous>current||coherence.State.OwnerEpoch!=previous||candidate.State.OwnerEpoch!=previous
            ||coherence.Root!=root||candidate.RootKey!=root||coherence.State.Status!=LocalCoherenceStatus.Anchored&&!scoped||candidate.State.Status!=LocalNamespaceStatus.EnumerationComplete
            ||coherence.State.JournalId!=state.Cursor.JournalIdHex||coherence.State.LegacyRevision!=state.LegacyRevision||candidate.State.LegacyRevision!=state.LegacyRevision
            ||coherence.State.GapEpoch!=state.GapEpoch||coherence.State.AppliedUsn>state.Cursor.NextReadUsn||coherence.State.AppliedUsn<coherence.State.CutUsn
            ||candidate.State.RootFileId128!=state.Binding.RootFileId||candidate.Mapping!=state.Binding.MappingId||candidate.Client!=state.Binding.ClientId)
            throw new LocalSourceGapException("CycleResumeAnchor","Retained native membership/owner/cursor identity is not reusable.");
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_coherence_active a JOIN local_namespace_active n ON n.candidate_id=$candidate WHERE a.root_key=$root AND a.coherence_id=$coherence AND n.mapping_id=$mapping"))
        {q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$coherence",scope.CoherenceId);q.Parameters.AddWithValue("$candidate",coherence.State.CandidateId);q.Parameters.AddWithValue("$mapping",state.Binding.MappingId);if(await q.ExecuteScalarAsync(ct)is null)throw new LocalSourceGapException("CycleResumeSuperseded","Historical candidate/coherence is not the active retained anchor.");}
        // Whole-lifetime OS ownership was acquired before this transaction.
        // Only fixed owner/budget rows change. B, generations, journal prefixes,
        // all original attempts and current remote tokens retain their bytes.
        await using(var q=SyncCommand(c,t,"UPDATE local_cycle_scopes SET owner_epoch=$epoch,max_profile=$profile,free_floor=$floor WHERE scope_id=$scope;UPDATE local_coherence_roots SET owner_epoch=$epoch,profile_cap=$profile,free_floor=$floor WHERE coherence_id=$coherence;UPDATE local_namespace_candidates SET owner_epoch=$epoch,max_profile=$profile,free_floor=$floor WHERE candidate_id=$candidate"))
        {CycleId(q,id);q.Parameters.AddWithValue("$epoch",current);q.Parameters.AddWithValue("$profile",budget.ProfileBytes);q.Parameters.AddWithValue("$floor",budget.MinimumFreeDiskBytes);q.Parameters.AddWithValue("$coherence",scope.CoherenceId);q.Parameters.AddWithValue("$candidate",coherence.State.CandidateId);if(await q.ExecuteNonQueryAsync(ct)!=3)throw new InvalidDataException("Retained owner handoff lost a dependent row.");}
        if(parentReconciliation&&await HasParentModule(c,t,ct))
        {
            await using var q=SyncCommand(c,t,"UPDATE local_parent_reconcile_plans SET owner_epoch=$epoch WHERE scope_id=$scope AND root_key=$root AND coherence_id=$coherence AND owner_epoch=$previous AND revision=$revision AND gap=$gap AND journal=$journal AND status<3");CycleId(q,id);q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$coherence",scope.CoherenceId);q.Parameters.AddWithValue("$epoch",current);q.Parameters.AddWithValue("$previous",previous);q.Parameters.AddWithValue("$revision",state.LegacyRevision);q.Parameters.AddWithValue("$gap",state.GapEpoch);q.Parameters.AddWithValue("$journal",state.Cursor.JournalIdHex);await q.ExecuteNonQueryAsync(ct);
        }
        scope=parentReconciliation?await RequireParentCycle(c,t,root,owner,id,ct):await RequireCycle(c,t,root,owner,id,false,ct);await CommitCycle(c,t,before,ct);return scope;
    }
}

public sealed partial class LocalSourceJournalSession
{
    internal Task<LocalCycleScope?> ResumeCycleAsync(WindowsCycleConfiguration configuration,WindowsCycleBudget budget,string spoolRoot,CancellationToken ct)
        =>SerialAsync(()=>_store.ResumeOwnedCycle(_root,_owner,configuration,budget,spoolRoot,_admission,ct),ct);
    internal Task<LocalCycleScope?> ResumeParentCycleAsync(WindowsCycleConfiguration configuration,WindowsCycleBudget budget,string spoolRoot,CancellationToken ct)
        =>SerialAsync(()=>_store.ResumeOwnedCycle(_root,_owner,configuration,budget,spoolRoot,_admission,ct,true),ct);
}
