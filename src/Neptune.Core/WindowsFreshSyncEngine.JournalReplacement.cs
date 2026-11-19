namespace Neptune.Core;

public sealed partial class WindowsFreshSyncEngine
{
    private LocalJournalReplacement? _journalReplacement;
    private readonly SemaphoreSlim _replacementSerial=new(1,1);
    public LocalJournalReplacement? JournalReplacement=>_journalReplacement;
    public async Task QualifyJournalReplacementAsync(CancellationToken cancellationToken=default)
    {
        var ct=cancellationToken;await _replacementSerial.WaitAsync(ct);
        try
        {
            var plan=_journalReplacement??throw new InvalidOperationException("Explicit replacement engine required.");
            if(plan.Status==LocalJournalReplacementStatus.Installed)return;
            // A cut during ordinary parent reconciliation leaves status=2.
            // Finish that exact owned plan before reading original outcomes,
            // whose strict cycle guard correctly refuses an unfinished proof.
            // This invokes the normal native proof; it does not relax the guard
            // or install historical file baselines as current membership.
            await _serial.WaitAsync(ct);
            try
            {
                if(plan.Status is LocalJournalReplacementStatus.LocalQualified or LocalJournalReplacementStatus.RemoteQualified
                    &&_coherence is not null&&(await _session.ReadCycleScopeAsync(ScopeId,ct)).Status==2)
                {
                    await RequireReplacementNativeIdentity(ct);
                    var state=await _coordinator.ReconcileParentsAsync(_session,ScopeId,_coherence,ct,async(o,t)=>
                    {if(_observer is not null)await _observer(new(WindowsCycleStage.BranchEnumerated,ScopeId,RelativePath:o.ParentId128),t);});
                    if(state.Status!=LocalCoherenceStatus.Anchored)throw new LocalSourceGapException("ReplacementLocalUnknown",state.Error??"Retained parent proof remains incomplete.");
                }
            }
            finally{_serial.Release();}
            while(true)
            {
                var page=await _session.ReadReplacementOriginalPageAsync(ScopeId,plan.ReplacementId,ct);if(page.Items.Count==0)break;var after=page.After;
                foreach(var item in page.Items)
                {
                    var result=item.Directory?await RecoverOriginalDirectoryAsync(plan.OriginalScopeId,item.Operation,ct):await RecoverOriginalWorkAsync(plan.OriginalScopeId,item.Operation,ct);
                    if(result.Error is not null||result.Status is not("published" or "not_published"))throw new LocalSourceGapException("ReplacementOriginalUnresolved",result.Error??"Original outcome is unresolved; preserve key/bytes.");
                    await _session.AdvanceReplacementOriginalAsync(ScopeId,plan.ReplacementId,after,item.Number,ct);after=item.Number;
                }
            }
            await _serial.WaitAsync(ct);
            try
            {
                ObjectDisposedException.ThrowIf(_disposed,this);
                if(plan.Status==LocalJournalReplacementStatus.SourceOpened)
                {
                    var completed=await _session.ReadReplacementCompletedAnchorAsync(ScopeId,plan.ReplacementId,ct);
                    var attempt=completed?.Attempt??0;
                    var state=completed?.State;
                    if(state is null)
                    {
                    while(await _session.ClearIncompleteReplacementSeedAsync(ScopeId,plan.ReplacementId,ct)==128)ct.ThrowIfCancellationRequested();
                    attempt=await _session.BeginReplacementAttemptAsync(ScopeId,plan.ReplacementId,ct);
                    _coordinator.SetReplacementAnchorStarted(async(candidate,coherence,t)=>await _session.RecordReplacementCandidateAsync(ScopeId,plan.ReplacementId,attempt,candidate,coherence,t));
                    try
                    {
                        state=await _coordinator.BuildAnchorAsync(_session,new(MaximumProfileBytes:_resources.Budget.ProfileBytes,MinimumFreeDiskBytes:_resources.Budget.MinimumFreeDiskBytes),new(MaximumProfileBytes:_resources.Budget.ProfileBytes,MinimumFreeDiskBytes:_resources.Budget.MinimumFreeDiskBytes),ct);
                        if(state.Status!=LocalCoherenceStatus.Anchored)throw new LocalSourceGapException("ReplacementLocalUnknown",state.Error??"Current native candidate is not complete.");
                    }
                    catch(Exception e)
                    {
                        // Cancellation leaves an explicitly incomplete candidate
                        // and immutable attempt IDs. Recovery may allocate at
                        // most one further candidate; it never certifies this one.
                        try{await _session.FailReplacementAttemptAsync(ScopeId,plan.ReplacementId,attempt,e.GetType().Name+": "+e.Message,CancellationToken.None);}catch(Exception diagnostic){e.Data["ReplacementFailurePersistence"]=diagnostic.ToString();}throw;
                    }
                    finally{_coordinator.SetReplacementAnchorStarted(null);}
                    }
                    // Once the native cut is committed, cancellation during
                    // seeding must retain that cut and its continuation cursor.
                    _coherence=state.CoherenceId;await _session.BindCycleCoherenceAsync(ScopeId,_coherence,ct);while(await _session.SeedCycleEntriesAsync(ScopeId,ct)>0)ct.ThrowIfCancellationRequested();
                    await RequireReplacementNativeIdentity(ct);await _session.CompleteReplacementStageAsync(ScopeId,plan.ReplacementId,LocalJournalReplacementStatus.LocalQualified,ct);plan=plan with{Status=LocalJournalReplacementStatus.LocalQualified,CandidateAttempts=attempt,Error=null};_journalReplacement=plan;
                }
                if(plan.Status==LocalJournalReplacementStatus.LocalQualified)
                {
                    await SyncRemote(ct);await Observe(ct);await RequireReplacementNativeIdentity(ct);await _session.CompleteReplacementStageAsync(ScopeId,plan.ReplacementId,LocalJournalReplacementStatus.RemoteQualified,ct);plan=plan with{Status=LocalJournalReplacementStatus.RemoteQualified};_journalReplacement=plan;
                }
                if(plan.Status==LocalJournalReplacementStatus.RemoteQualified)
                {
                    await Observe(ct);await SyncRemote(ct);await RequireReplacementNativeIdentity(ct);await _session.CompleteReplacementStageAsync(ScopeId,plan.ReplacementId,LocalJournalReplacementStatus.Installed,ct);_journalReplacement=plan with{Status=LocalJournalReplacementStatus.Installed};
                }
            }
            finally{_serial.Release();}
        }
        finally{_replacementSerial.Release();}
    }
    private async Task RequireReplacementNativeIdentity(CancellationToken ct)
    {
        await using var current=await WindowsNativeCycleCapture.OpenAsync(_capture.RootProof.RootPath,ct);
        if(current.RootProof!=_capture.RootProof||_remote.Binding!=_configuration.Binding)throw new InvalidDataException("Replacement native incarnation or current full authority changed.");
    }
    private Task<T> RecoveryRequest<T>(Func<Task<T>> request,CancellationToken ct)=>Admission(async()=>
    {
        if(_journalReplacement is not null)await RequireReplacementNativeIdentity(ct);return await request();
    },ct);
    public Task<WindowsCycleRecovery> RecoverOriginalDirectoryAsync(string originalScopeId,string operationId,CancellationToken cancellationToken=default)
        =>Task.Run(async()=>
        {
            await _serial.WaitAsync(cancellationToken);try
            {
                ObjectDisposedException.ThrowIf(_disposed,this);var ct=cancellationToken;
                var work=await _session.ReadDirectoryRecoveryAsync(ScopeId,originalScopeId,operationId,ct);
                var input=System.Text.Json.JsonSerializer.Deserialize<CycleFolderInput>(work.InputJson!,LocalCycleWire.Json)!;
                // Qualified modern registry: SAME immutable nine-field input
                // and UUID, including original topology. No MKCOL/fresh key.
                var result=await RecoveryRequest(()=>_remote.EnsureDirectoryAsync(input,ct),ct);
                await _session.SaveDirectoryRecoveryAsync(ScopeId,originalScopeId,operationId,result,ct);
                return new WindowsCycleRecovery(originalScopeId,operationId,"published",result.RawJson,null);
            }
            catch(Exception e){return new WindowsCycleRecovery(originalScopeId,operationId,"Uncertain",null,e.GetType().Name+": "+e.Message);}
            finally{_serial.Release();}
        },CancellationToken.None);
    // Explicit request only. This first lifecycle boundary opens a new real
    // subscription and pins historical originals; it does NOT run a scan,
    // publish, adopt B, clear dirty or certify the lost interval.
    public static async Task<WindowsFreshSyncEngine> OpenJournalReplacementAsync(WindowsCycleNativeOptions options,
        JournalGapReplacementRequest request,NeptuneStateStore store,string sourceRoot,string spoolRoot,
        WindowsCycleConfiguration configuration,IWindowsCycleRemote remote,WindowsCycleBudget? budget=null,
        Func<WindowsCycleObservation,CancellationToken,Task>? observer=null,CancellationToken cancellationToken=default)
    {
        var ct=cancellationToken;UsnRecordParser.ValidateMode(options.ReadMode);LocalCycleWire.Configuration(configuration);
        if(remote.Binding!=configuration.Binding)throw new InvalidDataException("Replacement current authority does not match the original full configuration.");
        budget??=new();var source=Path.TrimEndingDirectorySeparator(Path.GetFullPath(sourceRoot));var spool=Path.GetFullPath(spoolRoot);
        static bool Inside(string a,string b)=>b.Equals(a,StringComparison.OrdinalIgnoreCase)||b.StartsWith(a+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase);
        if(Inside(source,spool)||Inside(source,store.BootstrapExecutionDatabasePath))throw new InvalidOperationException("Replacement state/spool cannot be inside source.");
        WindowsLocalCoherenceCoordinator? coordinator=null;WindowsNativeCycleCapture? capture=null;LocalSourceJournalSession? session=null;WindowsLocalChangeWatcher? watcher=null;LocalCycleResources? resources=null;WindowsFreshSyncEngine? replacementEngine=null;
        try
        {
            coordinator=await WindowsLocalCoherenceCoordinator.OpenWithModeAsync(source,options.ReadMode,ct,async(o,t)=>
            {
                if(replacementEngine is null||o.Stage!=LocalCoherenceStage.ParentAliasesEnumerated)return;
                await replacementEngine._session.BindCycleCoherenceAsync(replacementEngine.ScopeId,null,t);while(await replacementEngine._session.SeedCycleEntriesAsync(replacementEngine.ScopeId,t)>0)t.ThrowIfCancellationRequested();
                if(observer is not null)await observer(new(WindowsCycleStage.BranchEnumerated,replacementEngine.ScopeId,RelativePath:o.ParentId128),t);
            });capture=await WindowsNativeCycleCapture.OpenAsync(source,ct);
            if(capture.RootProof!=coordinator.RootProof)throw new InvalidDataException("Native replacement root changed between owned handles.");
            watcher=new(source);resources=new(spool,budget);
            var opened=await store.OpenJournalReplacement(request,configuration,capture.RootProof,await coordinator.QueryJournalAsync(ct),budget,options.ReadMode,ct);session=opened.Session;
            var engine=new WindowsFreshSyncEngine(configuration,remote,session,coordinator,capture,watcher,resources,opened.Replacement.CurrentScopeId,observer,options.ReconcileParents){_journalReplacement=opened.Replacement,_coherence=opened.CoherenceId};
            replacementEngine=engine;
            await engine.StartWatcher(ct);
            if(observer is not null)await observer(new(WindowsCycleStage.ScopeOpened,engine.ScopeId),ct);return engine;
        }
        catch{try{if(watcher is not null)await watcher.DisposeAsync();}finally{resources?.Dispose();if(session is not null)await session.DisposeAsync();if(capture is not null)await capture.DisposeAsync();if(coordinator is not null)await coordinator.DisposeAsync();}throw;}
    }
}
