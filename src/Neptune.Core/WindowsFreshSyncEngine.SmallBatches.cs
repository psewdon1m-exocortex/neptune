namespace Neptune.Core;

public sealed record WindowsSmallBatchCleanup(int Removed, int Retained);

public sealed partial class WindowsFreshSyncEngine
{
    private ISmallFileBatchRemote? _smallBatchRemote;
    private SmallFileBatchCapability? _smallBatchCapability;
    private string? _smallBatchQualification;
    private readonly object _legacyBatchCapabilityGate = new();
    private Task<SmallFileBatchCapability?>? _legacyBatchCapability;
    private SmallFileBatchScheduler? _smallBatchScheduler;
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, CycleUploadStatus> _smallBatchRecoveryStatuses = new(StringComparer.Ordinal);

    private async Task ReadSmallBatchRecoveryStatuses(CancellationToken ct)
    {
        _smallBatchRecoveryStatuses.Clear();
        if (_remote is not ISmallFileBatchRemote remote) return;
        var references = (await _session.ListSmallBatchesAsync(ScopeId, ScopeId, ct)).Where(row => row.Phase == LocalSmallBatchPhase.Attempted).ToArray();
        if (references.Length == 0) return;
        // Accepted protocols are pinned in immutable local rows; disabling a
        // new-work capability must not disable their authenticated reader.
        var capability = _smallBatchCapability;
        var missing=new Dictionary<string,LocalCycleWork>(StringComparer.Ordinal);
        foreach (var reference in references)
        {
            var row = await _session.ReadSmallBatchAsync(ScopeId, reference.BatchId, ct);
            SmallFileBatchOutcome outcome;
            try { outcome = await Admission(() => remote.ReadSmallBatchAsync(row.Plan, ct), ct); }
            catch (GuardedCycleRemoteException error) when (error.StructuredWireCode && error.Status == System.Net.HttpStatusCode.NotFound && error.Code == "batch_missing")
            {
                // The declaration may never have reached the server. Only the
                // exact original per-file protocol can resolve that uncertainty.
                continue;
            }
            if (outcome.BatchId != row.Plan.BatchId || outcome.Items.Count != row.Spans.Count) throw new InvalidDataException("Batch status changed original membership.");
            for (var n = 0; n < row.Spans.Count; n++)
            {
                var entry = row.Plan.Entries[n]; var item = outcome.Items[n]; var op = row.Spans[n].OperationId;
                if (item.OperationId != entry.Original.Intent.IdempotencyKey || item.SourceGeneration != entry.SourceGeneration || item.SourceIdentity != entry.SourceIdentity)
                    throw new InvalidDataException("Batch status changed original source identity or order.");
                var work = await _session.ReadCycleWorkAsync(ScopeId, op, ct);
                if (item.Status is not { } status)
                {
                    if (item.State == "MISSING" && work.UploadId is not null || item.State is not ("MISSING" or "CONFLICT" or "UNRESOLVED"))
                        throw new InvalidDataException("Batch status lost an accepted original upload.");
                    if(item.State=="MISSING"&&!_smallBatchRecoveryStatuses.ContainsKey(op)&&work.UploadId is null&&work.Phase is LocalCycleWorkPhase.Sealed or LocalCycleWorkPhase.Sending or LocalCycleWorkPhase.OutcomeUnknown)
                        missing.TryAdd(op,work);
                    continue;
                }
                var expected = status.Phase switch { "published" => "PUBLISHED", "not_published" => "NOT_PUBLISHED", "physical_pending" or "reconciliation_required" => "UNRESOLVED", _ => "PENDING" };
                if (item.State != expected) throw new InvalidDataException("Batch status contradicts its original outcome.");
                if (work.Phase == LocalCycleWorkPhase.Published || work.Phase == LocalCycleWorkPhase.Conflict && work.ResultJson is not null)
                {
                    if (work.UploadId != status.UploadId || !LocalCycleWire.SameResult(work.ResultJson, status.ResultJson) || work.ReceivedBytes != status.ReceivedBytes)
                        throw new InvalidDataException("Batch status changed a durable terminal original.");
                    continue;
                }
                if(!_smallBatchRecoveryStatuses.TryAdd(op,status))
                {
                    var previous=_smallBatchRecoveryStatuses[op];
                    if(previous.UploadId!=status.UploadId||previous.OperationId!=status.OperationId||previous.RootId!=status.RootId||previous.IntentHash!=status.IntentHash||previous.ExpectedBytes!=status.ExpectedBytes)throw new InvalidDataException("Recovery batch original identity changed.");
                    var terminal=previous.Phase is "published" or "not_published";var nextTerminal=status.Phase is "published" or "not_published";
                    if(terminal&&nextTerminal&&(previous.Phase!=status.Phase||!LocalCycleWire.SameResult(previous.ResultJson,status.ResultJson)||previous.ReceivedBytes!=status.ReceivedBytes))throw new InvalidDataException("Recovery batch terminal original changed.");
                    if(!terminal&&(nextTerminal||status.ReceivedBytes>=previous.ReceivedBytes))_smallBatchRecoveryStatuses[op]=status;
                }
                missing.Remove(op);
            }
        }
        // New aggregate identity, same immutable individual originals. Never
        // reuse an accepted upload ID or mutate old batch membership.
        if(_smallBatchRemote is not null&&capability is not null&&capability.QualificationReceiptSha256==_smallBatchQualification)
        {
            _smallBatchCapability=capability;
            foreach(var group in missing.Values.Chunk(Math.Min(NeptuneStateStore.LocalSmallBatchItems,capability.MaximumItems)))
                await DispatchSmallBatch(group,ct,recoverySubset:true);
        }
        // These are hints for the normal original dispatcher, not receipts
        // committed here. It validates native proof/spool and the full original
        // status before persisting anything; B and dirty ACK still require L/R.
    }

    // Explicit bounded qualification only. Startup and ordinary UI profiles
    // do not call this automatically; measured economic acceptance remains a
    // separate deployment gate, while the server must authorize this scope.
    public async Task<bool> EnableSmallFileBatchQualificationAsync(CancellationToken cancellationToken = default)
    {
        var ct = cancellationToken; await _serial.WaitAsync(ct);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_remote is not ISmallFileBatchRemote remote || _resources.Budget.DataJobs < 2) return false;
            var capability = await remote.ReadSmallBatchCapabilitiesAsync(ct); if (capability is null || capability.MaximumItems < 2) return false;
            RequireBatchCapability(capability); await _session.EnableSmallBatchesAsync(ScopeId, ct);
            _smallBatchRemote = remote; _smallBatchCapability = capability; _smallBatchQualification = capability.QualificationReceiptSha256; return true;
        }
        finally { _serial.Release(); }
    }
    public async Task<bool> ConfigureSmallFileBatchesAsync(bool enabled,CancellationToken ct=default)
    {
        await _serial.WaitAsync(ct);
        try{_requestedBatches=enabled;if(!enabled){_smallBatchCapability=null;_smallBatchRemote=null;_smallBatchQualification=null;}return enabled;}
        finally{_serial.Release();}
    }
    private bool _requestedBatches;
    private async Task ApplyRequestedBatchSetting(CancellationToken ct)
    {
        lock (_legacyBatchCapabilityGate) _legacyBatchCapability = null;
        if(!_requestedBatches||_smallBatchRemote is not null||_remote is not ISmallFileBatchRemote remote||_resources.Budget.DataJobs<2)return;
        var capability=await remote.ReadSmallBatchCapabilitiesAsync(ct);if(capability is null||capability.MaximumItems<2)return;
        RequireBatchCapability(capability);await _session.EnableSmallBatchesAsync(ScopeId,ct);
        _smallBatchRemote=remote;_smallBatchCapability=capability;_smallBatchQualification=capability.QualificationReceiptSha256;
    }

    private void RequireBatchCapability(SmallFileBatchCapability cap)
    {
        if (cap.Binding != _configuration.Binding || cap.DeviceId != _configuration.DeviceId || cap.BackendIdentity != _configuration.BackendIdentity
            || !LocalCycleWire.Hash(cap.QualificationReceiptSha256) || cap.MaximumItems is < 1 or > SmallFileBatchPlan.MaximumItems
            || cap.MaximumFileBytes is < 1 or > SmallFileBatchPlan.MaximumFileBytes || cap.MaximumDataBytes < cap.MaximumFileBytes || cap.MaximumDataBytes > SmallFileBatchPlan.MaximumDataBytes
            || cap.MaximumMetadataBytes is < 1024 or > SmallFileBatchPlan.MaximumMetadataBytes
            || cap.ParallelJobs is < 1 or > 2 || !cap.ExecutionJob && cap.ParallelJobs != 1 || cap.SingleRequest && (!cap.ExecutionJob || cap.ParallelJobs != 2)) throw new InvalidDataException("Batch capability changed the admitted client scope or ceilings.");
    }
    private async Task RefreshSmallBatchQualification(CancellationToken ct)
    {
        if (_smallBatchRemote is null) return;
        var capability = await _smallBatchRemote.ReadSmallBatchCapabilitiesAsync(ct);
        if (capability is not null) RequireBatchCapability(capability);
        // Requalification is explicit after a deployment receipt changes.
        _smallBatchCapability = capability?.QualificationReceiptSha256 == _smallBatchQualification ? capability : null;
    }

    private Task<SmallFileBatchCapability?> ReadQualifiedLegacyBatchCapability(ISmallFileBatchRemote remote, CancellationToken ct)
    {
        // Coalesce only this cycle's optional protocol negotiation. Every new
        // cycle rechecks it, and the server still validates each operation.
        lock (_legacyBatchCapabilityGate)
            return _legacyBatchCapability ??= Read();
        async Task<SmallFileBatchCapability?> Read()
        {
            var capability = await remote.ReadLegacySmallBatchCapabilitiesAsync(ct);
            if (capability is null) return null;
            RequireBatchCapability(capability);
            if (capability.ExecutionJob) throw new InvalidDataException("Legacy batch probe returned another protocol.");
            return capability.QualificationReceiptSha256 == _smallBatchQualification ? capability : null;
        }
    }

    private Task<IReadOnlyDictionary<string,bool>> DispatchSmallBatch(IReadOnlyList<LocalCycleWork> originals,CancellationToken ct)=>DispatchSmallBatch(originals,ct,false);
    private async Task<IReadOnlyDictionary<string, bool>> DispatchSmallBatch(IReadOnlyList<LocalCycleWork> originals, CancellationToken ct,bool recoverySubset)
    {
        if (_smallBatchCapability is { ExecutionJob: true } && originals.Count > 1)
        {
            // An age group can mix parents, overwrites and empty files. Each
            // job still has one parent and the original absent-only contract;
            // ineligible originals must not disable jobs for the other files.
            var groups = originals.GroupBy(work =>
            {
                var input = System.Text.Json.JsonSerializer.Deserialize<CycleUploadInput>(work.InputJson!, LocalCycleWire.Json)!;
                return (input.Intent.ParentId, JobEligible: SmallFileBatchPlan.IsExecutionJobInput(input));
            }).ToArray();
            if (groups.Length > 1)
            {
                var results = new Dictionary<string, bool>(StringComparer.Ordinal);
                // The root gate preserves compatibility across dispatchers.
                // An ambiguous response stops admission of later partitions;
                // their sealed originals remain available to normal recovery.
                foreach (var group in groups)
                    foreach (var result in await DispatchSmallBatchGroup(group.ToArray(), ct, true))
                        results.Add(result.Key, result.Value);
                return results;
            }
        }
        return await DispatchSmallBatchGroup(originals, ct, recoverySubset);
    }
    private async Task<IReadOnlyDictionary<string, bool>> DispatchSmallBatchGroup(IReadOnlyList<LocalCycleWork> originals, CancellationToken ct, bool recoverySubset)
    {
        var fallback = originals.ToDictionary(w => w.OperationId, _ => false, StringComparer.Ordinal);
        var capability = _smallBatchCapability; var remote = _smallBatchRemote;
        if (capability is null || remote is null) return fallback;
        RootMutationAdmission.Lease? admission=null;
        try
        {
        if (capability.ExecutionJob && originals.Any(work => !SmallFileBatchPlan.IsExecutionJobInput(
            System.Text.Json.JsonSerializer.Deserialize<CycleUploadInput>(work.InputJson!, LocalCycleWire.Json)!)))
        {
            // The optional capability probe itself observes root admission.
            admission=await AcquireRootMutation(null,ct);
            capability = await ReadQualifiedLegacyBatchCapability(remote, ct);
        }
        if (capability is null || originals.Count < (recoverySubset?1:2) || originals.Count > capability.MaximumItems
            || originals.Any(w => w.Size > capability.MaximumFileBytes) || originals.Sum(w => w.Size ?? 0) > capability.MaximumDataBytes) return fallback;
        LocalSmallBatch row;
        try { row = capability.ExecutionJob
            ? await _session.CreateSmallExecutionJobAsync(ScopeId, Guid.NewGuid().ToString("D"), originals.Select(w => w.OperationId).ToArray(), ct)
            : await _session.CreateSmallBatchAsync(ScopeId, Guid.NewGuid().ToString("D"), originals.Select(w => w.OperationId).ToArray(), ct); }
        catch (LocalSourceGapException error) when (error.Code == "BatchCapacity") { return fallback; }
        var batch = row.Plan.BatchId;
        if (!capability.Admits(row.Plan)) { await CleanupSmallBatchArtifact(ScopeId, batch, ct); return fallback; }
        var hash = await LocalSmallBatchSpool.TryBuildAsync(_resources, row,
            (op, token) => _session.ReadCycleWorkAsync(ScopeId, op, token),
            (identity, token) => _session.BindSmallBatchSpoolAsync(ScopeId, batch, identity, token), ct);
        if (hash is null) { await CleanupSmallBatchArtifact(ScopeId, batch, ct); return fallback; }
        await _session.ChangeSmallBatchAsync(ScopeId, batch, LocalSmallBatchPhase.Ready, hash, ct);
        // Preparation may overlap terminal cleanup, but only compatible
        // requests can enter the server. No permit is held during aggregation.
        admission??=await AcquireRootMutation(capability.ExecutionJob?row.Plan.Entries[0].Original.Intent.ParentId:null,ct);
        await _session.ChangeSmallBatchAsync(ScopeId, batch, LocalSmallBatchPhase.Attempted, null, ct);
        admission?.Attempt();
        row = await _session.ReadSmallBatchAsync(ScopeId, batch, ct);
        if (capability.SingleRequest)
        {
            // The exact plan, originals and Attempted marker are durable before
            // any request. A lost response is resolved by the retained job's
            // status in a later cycle; this stream is never blindly replayed.
            await using (var bytes = await LocalSmallBatchSpool.OpenAsync(_resources, row, ct))
            {
                using var slot = await TransferSlot(bytes.Length, ct);
                var result = await remote.ExecuteSmallJobAsync(row.Plan, bytes, ct);
                await SaveBatchOutcomes(row, result, ct);
                if(BatchIsTerminal(result))admission?.Complete();
            }
            await CleanupSmallBatchArtifact(ScopeId, batch, ct);
            return originals.ToDictionary(w => w.OperationId, _ => true, StringComparer.Ordinal);
        }
        var prepared = await Admission(() => remote.PrepareSmallBatchAsync(row.Plan, ct), ct); await SaveBatchOutcomes(row, prepared, ct);
        if(BatchIsTerminal(prepared))admission?.Complete();
        row = await _session.ReadSmallBatchAsync(ScopeId, batch, ct);
        if (prepared.Items.Any(item => item.State is "MISSING" or "PENDING"))
        {
            // No implicit retry may rewind this stream after an ambiguous
            // failure. The next cycle recovers the same individual originals.
            await using var bytes = await LocalSmallBatchSpool.OpenAsync(_resources, row, ct);
            using var slot=await TransferSlot(bytes.Length,ct);
            var result = await remote.SendSmallBatchAsync(row.Plan, bytes, ct); await SaveBatchOutcomes(row, result, ct);
            if(BatchIsTerminal(result))admission?.Complete();
        }
        await CleanupSmallBatchArtifact(ScopeId, batch, ct);
        return originals.ToDictionary(w => w.OperationId, _ => true, StringComparer.Ordinal);
        }
        finally{admission?.Dispose();}
    }
    private static bool BatchIsTerminal(SmallFileBatchOutcome outcome)
        =>outcome.Items.All(item=>item.Status?.Phase is "published" or "not_published");

    private Task SaveBatchOutcomes(LocalSmallBatch row, SmallFileBatchOutcome outcome, CancellationToken ct)
        => _session.SaveSmallBatchOutcomeAsync(ScopeId, row.Plan.BatchId, outcome, ct);
    public async Task<WindowsSmallBatchCleanup> ReconcileSmallBatchArtifactsAsync(string? previousScopeId = null, CancellationToken cancellationToken = default)
    {
        var ct = cancellationToken; await _serial.WaitAsync(ct);
        try { ObjectDisposedException.ThrowIf(_disposed, this); return await ReconcileSmallBatchArtifacts(previousScopeId ?? ScopeId, ct); }
        finally { _serial.Release(); }
    }
    private async Task<WindowsSmallBatchCleanup> ReconcileSmallBatchArtifacts(string previous, CancellationToken ct)
    {
        var removed = 0; var retained = 0;
        foreach (var row in await _session.ListSmallBatchesAsync(ScopeId, previous, ct))
            if (await CleanupSmallBatchArtifact(previous, row.BatchId, ct)) removed++; else retained++;
        return new(removed, retained);
    }
    private async Task<bool> CleanupSmallBatchArtifact(string previous, string batch, CancellationToken ct)
    {
        var row = await _session.ReadHistoricalSmallBatchAsync(ScopeId, previous, batch, ct);
        if (row.Phase == LocalSmallBatchPhase.Attempted)
            foreach (var span in row.Spans)
            {
                var original = previous == ScopeId ? await _session.ReadCycleWorkAsync(ScopeId, span.OperationId, ct)
                    : await _session.ReadCycleRecoveryAsync(ScopeId, previous, span.OperationId, ct);
                if (original.Phase is not (LocalCycleWorkPhase.Published or LocalCycleWorkPhase.Conflict) || original.UploadId is null || original.ResultJson is null) return false;
            }
        if (row.Phase < LocalSmallBatchPhase.CleanupPending)
        {
            // A crash before the incarnation was recorded retains the unknown
            // artifact. It must not become a blind path-based deletion.
            if (row.SpoolIdentity is null && File.Exists(_resources.PathFor(batch))) return false;
            await _session.ChangeHistoricalSmallBatchAsync(ScopeId, previous, batch, LocalSmallBatchPhase.CleanupPending, ct);
            row = await _session.ReadHistoricalSmallBatchAsync(ScopeId, previous, batch, ct);
        }
        if (row.Phase == LocalSmallBatchPhase.CleanupPending)
        {
            await LocalSmallBatchSpool.RemoveAsync(_resources, row, ct);
            await _session.ChangeHistoricalSmallBatchAsync(ScopeId, previous, batch, LocalSmallBatchPhase.Cleaned, ct);
        }
        else if (File.Exists(_resources.PathFor(batch))) throw new InvalidDataException("Cleaned batch path reappeared; unknown bytes retained.");
        await _session.ForgetHistoricalSmallBatchAsync(ScopeId, previous, batch, ct); return true;
    }
}
