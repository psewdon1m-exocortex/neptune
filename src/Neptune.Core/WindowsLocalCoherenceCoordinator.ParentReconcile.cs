namespace Neptune.Core;

public sealed partial class WindowsLocalCoherenceCoordinator
{
    // Instance-scoped fault observation at the native witness boundary.
    internal Func<CancellationToken,Task>? ExternalHardlinkProofOpened { get; set; }
    private async Task<T> Measured<T>(SyncPhase phase,Func<Task<T>> action)
    {using var measurement=Diagnostics.Measure(phase);var result=await action();if(result is int items)Diagnostics.Items(phase,items);return result;}
    internal async Task<LocalCoherenceState> ReconcileParentsAsync(LocalSourceJournalSession session,string scope,string coherence,
        CancellationToken cancellationToken,Func<LocalCoherenceObservation,CancellationToken,Task>? observer=null)
    {
        var ct=cancellationToken;await _serial.WaitAsync(ct);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed,this);await RequireNativeRootBinding(session,ct);
            if(await TryExternalHardlinkCut(session,scope,coherence,ct))return await session.ReadCoherenceAsync(coherence,ct);
            while(await Measured(SyncPhase.Retirement,()=>session.PromoteParentReconciliationAsync(scope,ct))>0)ct.ThrowIfCancellationRequested();
            while(await Measured(SyncPhase.Retirement,()=>session.RetireParentReconciliationAsync(scope,ct))>0)ct.ThrowIfCancellationRequested();
            var plan=await session.BeginParentReconciliationAsync(scope,ct);if(plan.CoherenceId!=coherence)throw new InvalidDataException("Parent plan cannot rebind original coherent scope.");
            var bounds=await _journal.QueryAsync(ct);WindowsUsnJournalReader.ValidateCursor(bounds,new(bounds.JournalIdHex,plan.StartUsn,(await session.ReadStateAsync(ct)).Cursor.OwnerEpoch,true));
            if(plan.Status==LocalParentReconciliationStatus.Scanning)
            {
                try
                {
                while(await Measured(SyncPhase.Frontier,()=>session.ResetClaimedParentReconciliationAsync(scope,plan.PlanId,ct))>0)ct.ThrowIfCancellationRequested();
                while(await Measured(SyncPhase.Frontier,()=>session.SeedParentReconciliationAsync(scope,plan.PlanId,ct))>0)ct.ThrowIfCancellationRequested();
                plan=await session.BeginParentReconciliationAsync(scope,ct);
                if(plan.Parents==0)
                {
                    // Watcher delivery may lag an already installed native cut.
                    // An obsolete structural path then has no remaining USN
                    // parent. Prove current root membership instead of clearing
                    // uncertainty or stranding the durable empty plan. Journal,
                    // identity, hardlink and deletion approval gates still apply.
                    if(!await session.RequestFullParentProofAsync(scope,ct))
                        throw new LocalSourceGapException("ParentUnscopedAmbiguity","Pending parent proof cannot be expanded.");
                    plan=await session.BeginParentReconciliationAsync(scope,ct);
                }
                while(true)
                {
                    while(await Measured(SyncPhase.Frontier,()=>session.RefillParentReconciliationAsync(scope,plan.PlanId,ct))>0)ct.ThrowIfCancellationRequested();
                    var parent=await Measured(SyncPhase.Frontier,()=>session.ClaimParentReconciliationAsync(scope,plan.PlanId,ct));if(parent is null)break;
                    using var scan=Diagnostics.Measure(SyncPhase.NativeScan);
                    var native=new LocalNamespaceDirectory(plan.PlanId,parent.NodeId,parent.Path,parent.Created,parent.Claim);
                    await using var reader=await _source.OpenDirectoryAsync(native,ct);
                    while(true)
                    {
                        var page=await reader.ReadPageAsync(ct);for(var i=0;i<page.Entries.Count;i+=128)await session.StageParentReconciliationEntriesAsync(scope,parent,page.Entries.Skip(i).Take(128).ToArray(),ct);if(page.End)break;
                    }
                    await using var aliases=await _aliases.OpenAsync(native,reader.Proof,ct);
                    while(true)
                    {
                        var page=await aliases.ReadPageAsync(ct);for(var i=0;i<page.Entries.Count;i+=128)await session.StageParentReconciliationAliasesAsync(scope,parent,page.Entries.Skip(i).Take(128).ToArray(),ct);if(page.End)break;
                    }
                    if(observer is not null)await observer(new(LocalCoherenceStage.ParentAliasesEnumerated,plan.PlanId,parent.NodeId),ct);
                    await aliases.ValidateUnchangedAsync(ct);await reader.ValidateUnchangedAsync(ct);await session.CompleteParentReconciliationDirectoryAsync(scope,parent,reader.Proof,ct);
                }
                // Revalidate the actual root, not a string-prefix ancestry
                // claim; directory sharing alone need not fence POSIX rename.
                await using(var root=await _source.OpenDirectoryAsync(new(plan.PlanId,RootProof.Handle.FileId128,"",RootProof.Handle.CreationTime,plan.PlanId),ct))await root.ValidateUnchangedAsync(ct);
                await CaptureThrough(session,await _journal.QueryAsync(ct),ct);
                while(await Measured(SyncPhase.HardlinkVerification,()=>session.VerifyParentReconciliationAsync(scope,plan.PlanId,ct))>0)ct.ThrowIfCancellationRequested();
                }
                catch(LocalSourceGapException error)when(error.Code=="ParentRenameHalfMissing")
                {
                    if(!await session.RejectCrossCutParentRenameAsync(scope,plan.PlanId,ct))throw;
                    throw new BootstrapCaptureDeferredException("Retained ordered rename crosses the candidate scan cut; rejected candidate requires a fresh native proof.");
                }
                catch(LocalSourceGapException error)when(error.Code=="ParentChangedDuringClosure")
                {
                    // A rejected candidate can never prove this interval on retry.
                    // Retain its rejection, keep native history and installed versions,
                    // and let the next observation start a new bounded candidate.
                    await session.RejectChangedParentAsync(scope,plan.PlanId,ct);
                    throw new BootstrapCaptureDeferredException(error.Message);
                }
            }
            // Enumeration ingests a later native cut. Apply that already
            // retained prefix before activating its current membership; this
            // preserves data dirties and UNKNOWN dispositions at the cut.
            plan=await session.BeginParentReconciliationAsync(scope,ct);
            while(true)
            {
                var current=await session.ReadCoherenceAsync(coherence,ct);if(current.AppliedUsn>=plan.CutUsn)break;
                var applied=await session.ApplyNextCoherenceAsync(coherence,current.AppliedUsn,ct);if(applied.AppliedUsn<=current.AppliedUsn)throw new LocalSourceGapException("CoherenceNoProgress","Parent cut cannot skip retained native evidence.");
            }
            while(await Measured(SyncPhase.Import,()=>session.ImportParentReconciliationAsync(scope,plan.PlanId,ct))>0)ct.ThrowIfCancellationRequested();
            while(await Measured(SyncPhase.Import,()=>session.MarkParentReconciliationMissingAsync(scope,plan.PlanId,ct))>0)ct.ThrowIfCancellationRequested();
            while(await Measured(SyncPhase.Import,()=>session.InstallParentReconciliationVersionsAsync(scope,plan.PlanId,ct))>0)ct.ThrowIfCancellationRequested();
            await session.FinishParentReconciliationAsync(scope,plan.PlanId,ct);
            return await session.ReadCoherenceAsync(coherence,ct);
        }
        finally{_serial.Release();}
    }
    private async Task<bool> TryExternalHardlinkCut(LocalSourceJournalSession session,string scope,string coherence,CancellationToken ct)
    {
        // A racing outside writer invalidates the short witness, not the whole
        // managed tree. Retry the native proof with fresh retained claims, then
        // defer safely instead of expanding to an expensive full-tree scan.
        for(var attempt=0;attempt<3;attempt++)
        {
            var claims=await session.ReadParentExternalClaimsAsync(scope,ct);if(claims is null)return false;
            try
            {
            using var proof=WindowsOutsideHardlinkProof.Open(RootProof,claims.Files,ct);
            if(ExternalHardlinkProofOpened is not null)await ExternalHardlinkProofOpened(ct);
            await CaptureThrough(session,await _journal.QueryAsync(ct),ct);
            while(true)
            {
                var state=await session.ReadCoherenceAsync(coherence,ct);var source=await session.ReadStateAsync(ct);
                if(state.AppliedUsn>=source.Cursor.NextReadUsn)break;
                var applied=await session.ApplyNextCoherenceAsync(coherence,state.AppliedUsn,ct);
                if(applied.AppliedUsn<=state.AppliedUsn)throw new LocalSourceGapException("CoherenceNoProgress","Native outside witness cannot skip retained evidence.");
            }
            await RequireNativeRootBinding(session,ct);
            await using var root=await _source.OpenDirectoryAsync(new("external-current-proof",RootProof.Handle.FileId128,"",RootProof.Handle.CreationTime,"native"),ct);
            await root.ValidateUnchangedAsync(ct);
            if(await session.InstallParentExternalCutAsync(claims,proof,ct))return true;
            }
            catch(WindowsOutsideHardlinkProof.CapacityException){return false; /* bounded native path cannot cover this identity; retain full proof */}
            catch(IOException){/* keep UNKNOWN and retry native evidence */}
            catch(System.ComponentModel.Win32Exception){/* inaccessible/moving identity remains unqualified */}
            if(attempt<2)await Task.Delay(50,ct);
        }
        throw new BootstrapCaptureDeferredException("External hardlink membership proof is unstable or unavailable; retained evidence requires a later native retry.");
    }
}
