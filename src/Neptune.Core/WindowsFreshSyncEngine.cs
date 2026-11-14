using System.Globalization;
using System.Text.Json;
using System.Threading.Channels;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

// Native engine for negotiated guarded roots. Ordinary Windows sessions use
// the same persisted authority; an activated root cannot fall back to DAV.
public sealed partial class WindowsFreshSyncEngine : IAsyncDisposable
{
    private readonly WindowsCycleConfiguration _configuration;private readonly IWindowsCycleRemote _remote;
    private readonly LocalSourceJournalSession _session;private readonly WindowsLocalCoherenceCoordinator _coordinator;
    private readonly WindowsNativeCycleCapture _capture;private readonly WindowsLocalChangeWatcher _watcher;
    private readonly LocalCycleResources _resources;private readonly Func<WindowsCycleObservation,CancellationToken,Task>? _observer;
    private readonly SemaphoreSlim _serial=new(1,1);private bool _disposed;private string? _coherence;
    private SemaphoreSlim? _wake;private string _dispatch="";private long _capturedFiles,_capturedBytes,_received,_publishedFiles,_publishedDirectories;
    private int _remoteRefreshes;private int _pauseNew;private string? _backpressure;private Exception? _scheduledPause;private readonly bool _reconcileParents;
    private WindowsFreshSyncEngine(WindowsCycleConfiguration configuration,IWindowsCycleRemote remote,LocalSourceJournalSession session,
        WindowsLocalCoherenceCoordinator coordinator,WindowsNativeCycleCapture capture,WindowsLocalChangeWatcher watcher,
        LocalCycleResources resources,string scope,Func<WindowsCycleObservation,CancellationToken,Task>? observer,bool reconcileParents)
        =>(_configuration,_remote,_session,_coordinator,_capture,_watcher,_resources,ScopeId,_observer,_reconcileParents)=(configuration,remote,session,coordinator,capture,watcher,resources,scope,observer,reconcileParents);
    public string ScopeId{get;}
    public SyncDiagnosticSnapshot Diagnostics=>_coordinator.Diagnostics.Snapshot() with{Schedule=Volatile.Read(ref _schedule),Scheduler=_session.ResourceScheduler.Snapshot(),CapturedBytes=Interlocked.Read(ref _capturedBytes),ReceivedBytes=Interlocked.Read(ref _received),PublishedFiles=Interlocked.Read(ref _publishedFiles)};
    public event Action? WorkAvailable;
    private async Task StartWatcher(CancellationToken ct)
    { await _session.InitializeClientEvolutionAsync(ct);if(_remote is IWindowsCycleExtensions{NamespacePolicy:{ } authority})await _session.BindClientNamespaceAsync(ScopeId,authority,ct);_watcher.WorkAvailable+=OnLocalWorkAvailable;await _watcher.StartAsync(_session,ct);_schedule=await _session.ReadOperationScheduleAsync(ScopeId,ct);_watcher.ScheduleRetry(_schedule.NextAttemptUnixMilliseconds); }
    public Task<MappingPolicyPreview> PreviewPolicyAsync(MappingSyncPolicy policy,CancellationToken ct=default)=>_session.PreviewPolicyAsync(ScopeId,policy,ct);
    public Task<MappingSyncPolicy> ReadMappingPolicyAsync(CancellationToken ct=default)=>_session.ReadMappingPolicyAsync(ScopeId,ct);
    public async Task ApplyPolicyAsync(MappingSyncPolicy policy,string digest,CancellationToken ct=default)
    {await _serial.WaitAsync(ct);try{await _session.ApplyPolicyAsync(ScopeId,policy,digest,ct);while(await _session.ContinuePolicyAsync(ScopeId,ct)==250)ct.ThrowIfCancellationRequested();}finally{_serial.Release();}}
    public async Task<bool> WaitForRemoteHintAsync(CancellationToken ct=default)
    {
        if(_remote is not IWindowsCycleExtensions remote||!remote.Supports("changes-wait-v1"))return false;
        using var waiting=CancellationTokenSource.CreateLinkedTokenSource(ct);
        void LocalWake(){try{waiting.Cancel();}catch(ObjectDisposedException){ /* An already dispatched callback may outlive unsubscription. */ }}
        WorkAvailable+=LocalWake;
        try
        {
            LocalCycleRemoteProgress progress;await _serial.WaitAsync(waiting.Token);
            try
            {
                if(_disposed||_coherence is null)return false;
                // Subscribe before reading durable hints: an event before subscription
                // (including a restored event) and one during the read both wake us.
                var local=await _session.ReadEventHintsAsync(waiting.Token);
                if(local.Gap||local.Hints.Count>0)return true;
                progress=await _session.ReadCycleRemoteProgressAsync(ScopeId,waiting.Token);
            }
            finally{_serial.Release();}
            if(progress.ChangesCursor is null||!progress.Complete)return false;
            return (await Admission(()=>remote.WaitForChangesAsync(progress.ChangesCursor,progress.RequestSequence,waiting.Token),waiting.Token)).Items.Count>0;
        }
        catch(OperationCanceledException)when(!ct.IsCancellationRequested&&waiting.IsCancellationRequested){return true;}
        catch(GuardedCycleRemoteException e)when(IsCursorGap(e,feed:true)){return true;}
        finally{WorkAvailable-=LocalWake;}
    }
    private void OnLocalWorkAvailable(){Wake();WorkAvailable?.Invoke();}
    // A restored coherent namespace still needs native Observe + a fresh
    // remote feed. This property is not an instant byte-clean certificate.
    public bool HasRetainedAnchor=>_coherence is not null;
    public async Task<IReadOnlyList<LocalParentReconciliationConflict>> ReadParentConflictsAsync(string afterNode="",CancellationToken cancellationToken=default)
    {await _serial.WaitAsync(cancellationToken);try{ObjectDisposedException.ThrowIf(_disposed,this);return await _session.ReadParentConflictsAsync(ScopeId,afterNode,cancellationToken);}finally{_serial.Release();}}
    public WindowsCycleResourceMetrics ResourceMetrics=>_resources.Metrics;
    public async Task<WindowsCycleConfirmedTotals> ReadConfirmedTotalsAsync(CancellationToken cancellationToken=default)
    {await _serial.WaitAsync(cancellationToken);try{ObjectDisposedException.ThrowIf(_disposed,this);return await _session.ReadCycleConfirmedTotalsAsync(ScopeId,cancellationToken);}finally{_serial.Release();}}
    public static async Task<WindowsFreshSyncEngine> OpenAsync(NeptuneStateStore store,string sourceRoot,string spoolRoot,
        WindowsCycleConfiguration configuration,IWindowsCycleRemote remote,WindowsCycleBudget? budget=null,long subscriptionGeneration=1,
        Func<WindowsCycleObservation,CancellationToken,Task>? observer=null,CancellationToken cancellationToken=default)
        =>await OpenCoreAsync(store,sourceRoot,spoolRoot,configuration,remote,LocalJournalReadMode.Named,false,budget,subscriptionGeneration,observer,false,cancellationToken);
    // This overload is opt-in. Lack of DASD rights never silently changes the
    // default Named subscription or interprets redacted names as real names.
    public static Task<WindowsFreshSyncEngine> OpenAsync(LocalJournalReadMode nativeMode,NeptuneStateStore store,string sourceRoot,string spoolRoot,
        WindowsCycleConfiguration configuration,IWindowsCycleRemote remote,WindowsCycleBudget? budget=null,long subscriptionGeneration=1,
        Func<WindowsCycleObservation,CancellationToken,Task>? observer=null,CancellationToken cancellationToken=default)
        =>OpenCoreAsync(store,sourceRoot,spoolRoot,configuration,remote,nativeMode,false,budget,subscriptionGeneration,observer,false,cancellationToken);
    public static Task<WindowsFreshSyncEngine> OpenAsync(WindowsCycleNativeOptions options,NeptuneStateStore store,string sourceRoot,string spoolRoot,
        WindowsCycleConfiguration configuration,IWindowsCycleRemote remote,WindowsCycleBudget? budget=null,long subscriptionGeneration=1,
        Func<WindowsCycleObservation,CancellationToken,Task>? observer=null,CancellationToken cancellationToken=default)
        =>OpenCoreAsync(store,sourceRoot,spoolRoot,configuration,remote,options.ReadMode,options.ReconcileParents,budget,subscriptionGeneration,observer,false,cancellationToken);
    public static Task<WindowsFreshSyncEngine> ResumeOrOpenAsync(NeptuneStateStore store,string sourceRoot,string spoolRoot,
        WindowsCycleConfiguration configuration,IWindowsCycleRemote remote,WindowsCycleBudget? budget=null,long subscriptionGeneration=1,
        Func<WindowsCycleObservation,CancellationToken,Task>? observer=null,CancellationToken cancellationToken=default)
        =>OpenCoreAsync(store,sourceRoot,spoolRoot,configuration,remote,LocalJournalReadMode.Named,false,budget,subscriptionGeneration,observer,true,cancellationToken);
    public static Task<WindowsFreshSyncEngine> ResumeOrOpenAsync(LocalJournalReadMode nativeMode,NeptuneStateStore store,string sourceRoot,string spoolRoot,
        WindowsCycleConfiguration configuration,IWindowsCycleRemote remote,WindowsCycleBudget? budget=null,long subscriptionGeneration=1,
        Func<WindowsCycleObservation,CancellationToken,Task>? observer=null,CancellationToken cancellationToken=default)
        =>OpenCoreAsync(store,sourceRoot,spoolRoot,configuration,remote,nativeMode,false,budget,subscriptionGeneration,observer,true,cancellationToken);
    public static Task<WindowsFreshSyncEngine> ResumeOrOpenAsync(WindowsCycleNativeOptions options,NeptuneStateStore store,string sourceRoot,string spoolRoot,
        WindowsCycleConfiguration configuration,IWindowsCycleRemote remote,WindowsCycleBudget? budget=null,long subscriptionGeneration=1,
        Func<WindowsCycleObservation,CancellationToken,Task>? observer=null,CancellationToken cancellationToken=default)
        =>OpenCoreAsync(store,sourceRoot,spoolRoot,configuration,remote,options.ReadMode,options.ReconcileParents,budget,subscriptionGeneration,observer,true,cancellationToken);
    private static async Task<WindowsFreshSyncEngine> OpenCoreAsync(NeptuneStateStore store,string sourceRoot,string spoolRoot,
        WindowsCycleConfiguration configuration,IWindowsCycleRemote remote,LocalJournalReadMode nativeMode,bool reconcileParents,WindowsCycleBudget? budget,long subscriptionGeneration,
        Func<WindowsCycleObservation,CancellationToken,Task>? observer,bool restore,CancellationToken cancellationToken)
    {
        UsnRecordParser.ValidateMode(nativeMode);LocalCycleWire.Configuration(configuration);if(remote.Binding!=configuration.Binding)throw new InvalidDataException("Remote enrollment differs from cycle configuration.");
        budget??=new();if(budget.MaximumNewFileBytes is <0)throw new ArgumentOutOfRangeException(nameof(budget));var source=Path.TrimEndingDirectorySeparator(Path.GetFullPath(sourceRoot));var spool=Path.GetFullPath(spoolRoot);
        static bool Inside(string parent,string child)=>child.Equals(parent,StringComparison.OrdinalIgnoreCase)||child.StartsWith(parent+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase);
        if(Inside(source,spool)||Inside(source,store.BootstrapExecutionDatabasePath))throw new InvalidOperationException("Cycle state and immutable spool must remain outside source tree.");
        await store.InitializeAsync(cancellationToken);WindowsFreshSyncEngine? engine=null;WindowsLocalCoherenceCoordinator? coordinator=null;WindowsNativeCycleCapture? capture=null;LocalSourceJournalSession? session=null;WindowsLocalChangeWatcher? watcher=null;LocalCycleResources? resources=null;
        try
        {
            coordinator=await WindowsLocalCoherenceCoordinator.OpenWithModeAsync(source,nativeMode,cancellationToken,async(observation,ct)=>
            {
                if(engine is null)return;
                await engine._session.BindCycleCoherenceAsync(engine.ScopeId,null,ct);
                while(await engine._session.SeedCycleEntriesAsync(engine.ScopeId,ct)>0)ct.ThrowIfCancellationRequested();
                engine.Wake();
                if(engine._observer is not null)await engine._observer(new(WindowsCycleStage.BranchEnumerated,engine.ScopeId,RelativePath:observation.ParentId128),ct);
            });
            capture=await WindowsNativeCycleCapture.OpenAsync(source,cancellationToken);var proof=coordinator.RootProof;
            if(proof!=capture.RootProof)throw new IOException("Source root changed between owned native handles.");
            watcher=new(source);resources=new(spool,budget);
            var bounds=await coordinator.QueryJournalAsync(cancellationToken);
            var binding=new LocalSourceBinding(configuration.ProfileId,configuration.ClientId,configuration.MappingId,source,proof.VolumeGuid,
                proof.Handle.LegacyVolumeSerial.ToString("x8"),"NTFS",proof.Handle.FileId128,proof.Handle.CreationTime,subscriptionGeneration,configuration.RulesVersion,"windows-raw-nfc-v1");
            session=await store.OpenLocalJournalSessionAsync(binding,bounds,bounds.NextUsn,new(MaximumDatabaseBytes:budget.ProfileBytes,MinimumFreeDiskBytes:budget.MinimumFreeDiskBytes),nativeMode,cancellationToken);
            var retained=restore?reconcileParents?await session.ResumeParentCycleAsync(configuration,budget,spool,cancellationToken):await session.ResumeCycleAsync(configuration,budget,spool,cancellationToken):null;
            var scope=retained?.ScopeId??await session.BeginCycleAsync(configuration,budget,cancellationToken);engine=new(configuration,remote,session,coordinator,capture,watcher,resources,scope,observer,reconcileParents);engine._coherence=retained?.CoherenceId;
            await engine.StartWatcher(cancellationToken);
            if(observer is not null)await observer(new(WindowsCycleStage.ScopeOpened,scope),cancellationToken);return engine;
        }
        catch{try{if(watcher is not null)await watcher.DisposeAsync();}finally{resources?.Dispose();if(session is not null)await session.DisposeAsync();if(capture is not null)await capture.DisposeAsync();if(coordinator is not null)await coordinator.DisposeAsync();}throw;}
    }
    private void Wake(){try{_wake?.Release();}catch(SemaphoreFullException){}}
    private void PauseNew(string reason,Exception? scheduledError=null)
    {
        Interlocked.CompareExchange(ref _backpressure,reason,null);
        if(scheduledError is not null)
        {
            Exception? before;
            do
            {
                before=Volatile.Read(ref _scheduledPause);
                if(before is not null&&before is not RootMutationDeferredException)break;
            }while(Interlocked.CompareExchange(ref _scheduledPause,scheduledError,before)!=before);
        }
        Interlocked.Exchange(ref _pauseNew,1);Wake();
    }
    private sealed class ScheduledWorkDeferredException(Exception cause):IOException("Original operation awaits its durable retry: "+cause.Message,cause);
    // Workers have joined before this is read. A persisted per-operation wait
    // already owns its deadline and pressure signal; never add a second wait.
    private Exception PausedWork()=>_scheduledPause is { } cause?new ScheduledWorkDeferredException(cause):new CycleSpoolAdmissionException(_backpressure!);
    // Only an explicit non-admission response permits finite same-request
    // retries. An ambiguous transport error is recovered by inspect/same key
            // in a subsequent dispatcher, never by allocating or capturing again.
    private readonly SyncRetryController _retry=new();
    private Task<T> Admission<T>(Func<Task<T>> request,CancellationToken ct)=>_scheduledOperation.Value is null?_retry.ExecuteAsync(request,_coordinator.Diagnostics,ct):request();
    public Task<WindowsCycleResult> InitialSyncAsync(CancellationToken cancellationToken=default)
        =>Task.Run(()=>RunAsync(initial:true,cancellationToken),CancellationToken.None);
    public Task<WindowsCycleResult> FreshSyncAsync(CancellationToken cancellationToken=default)
        =>Task.Run(()=>RunAsync(initial:false,cancellationToken),CancellationToken.None);
    // An explicit retry does not change an attempted operation or its bytes.
    public async Task RetryDeferredAsync(CancellationToken ct=default)
    {await _serial.WaitAsync(ct);try{_retryDeferredRequested=true;}finally{_serial.Release();}}
    private bool _retryDeferredRequested;
    // At most two bounded transactions. No current baseline/dirty evidence is
    // removed; unresolved receipts and retained owned spools remain pinned.
    public async Task<WindowsCycleHistoryRetirement> RetireHistoryBatchAsync(CancellationToken ct=default)
    {
        await _serial.WaitAsync(ct);try
        {
            ObjectDisposedException.ThrowIf(_disposed,this);if(_coherence is null)throw new InvalidOperationException("Retirement needs retained coherent scope.");
            var page=await _session.ReadCycleRetirableWorkAsync(ScopeId,ct);var safe=page.Records.Where(w=>w.SpoolPath is null||w.SpoolPath==_resources.PathFor(w.OperationId)&&!File.Exists(w.SpoolPath)).Select(w=>w.OperationId).ToArray();
            var work=await _session.RetireCycleWorkAsync(ScopeId,page,safe,ct);var journal=await _session.RetireCycleAppliedHistoryAsync(ScopeId,_coherence,ct);return new(work,journal);
        }
        finally{_serial.Release();}
    }
    public async Task<LocalCycleHistoryRetirement> RetireRemoteHistoryBatchAsync(CancellationToken ct=default)
    {await _serial.WaitAsync(ct);try{ObjectDisposedException.ThrowIf(_disposed,this);return await _session.RetireCycleRemoteHistoryAsync(ScopeId,ct);}finally{_serial.Release();}}
    public async Task<LocalCycleHistoryRetirement> RetireSupersededScopeBatchAsync(string previousScopeId,CancellationToken ct=default)
    {await _serial.WaitAsync(ct);try{ObjectDisposedException.ThrowIf(_disposed,this);return await _session.RetireCycleScopeAsync(ScopeId,previousScopeId,ct);}finally{_serial.Release();}}
    // Explicit new-owner recovery of one already attempted immutable operation.
    // It records historical evidence only; it cannot establish a new source B.
    public Task<WindowsCycleRecovery> RecoverOriginalWorkAsync(string originalScopeId,string operationId,CancellationToken cancellationToken=default)
        =>Task.Run(async()=>
        {
            await _serial.WaitAsync(cancellationToken);try
            {
                ObjectDisposedException.ThrowIf(_disposed,this);var ct=cancellationToken;var work=await _session.ReadCycleRecoveryAsync(ScopeId,originalScopeId,operationId,ct);
                WindowsNativeCycleCapture.ValidatePersisted(work,work.Size!.Value,work.Sha256!,work.CaptureProof!,_capture.RootProof);var input=JsonSerializer.Deserialize<CycleUploadInput>(work.InputJson!,LocalCycleWire.Json)!;
                var status=await RecoveryRequest(()=>_remote.UploadAsync(work.UploadId is null?"prepare":"inspect",input,work.UploadId,work.IntentHash!,ct),ct);await _session.SaveCycleRecoveryAsync(ScopeId,originalScopeId,operationId,status,ct);
                if(status.Phase is "published" or "not_published")
                {
                    if(status.Phase=="published"&&work.SpoolPath==_resources.PathFor(operationId))try{_resources.Release(operationId,true);}catch{_resources.Diagnostic();}
                    return new WindowsCycleRecovery(originalScopeId,operationId,status.Phase,status.ResultJson,null);
                }
                await _resources.ReserveAsync(operationId,work.Size.Value,ct);
                try
                {
                    await using var spool=await _resources.OpenAsync(work,true,ct);spool.Position=status.ReceivedBytes;var buffer=new byte[8*1024*1024];
                    while(spool.Position<spool.Length){var offset=spool.Position;var length=(int)Math.Min(buffer.Length,spool.Length-offset);await spool.ReadExactlyAsync(buffer.AsMemory(0,length),ct);status=await RecoveryRequest(()=>_remote.AppendAsync(input,status.UploadId,work.IntentHash!,offset,buffer.AsMemory(0,length),ct),ct);await _session.SaveCycleRecoveryAsync(ScopeId,originalScopeId,operationId,status,ct);if(status.ReceivedBytes!=offset+length)throw new InvalidDataException("Original recovery offset changed.");}
                    status=await RecoveryRequest(()=>_remote.UploadAsync("complete",input,status.UploadId,work.IntentHash!,ct),ct);await _session.SaveCycleRecoveryAsync(ScopeId,originalScopeId,operationId,status,ct);await spool.DisposeAsync();if(status.Phase=="published")try{_resources.Release(operationId,true);}catch{_resources.Diagnostic();}
                    return new WindowsCycleRecovery(originalScopeId,operationId,status.Phase,status.ResultJson,null);
                }
                finally{_resources.Release(operationId,false);}
            }
            catch(Exception e){return new WindowsCycleRecovery(originalScopeId,operationId,"Uncertain",null,e.GetType().Name+": "+e.Message);}
            finally{_serial.Release();}
        },CancellationToken.None);
    private Task<LocalCycleRemoteProgress> Progress(bool pending,CancellationToken ct)=>pending?_session.ReadCycleStageProgressAsync(ScopeId,ct):_session.ReadCycleRemoteProgressAsync(ScopeId,ct);
    private sealed class CursorGapException(GuardedCycleRemoteException cause):IOException("Qualified remote cursor gap: "+cause.Code,cause);
    // Saturn removes opaque feed tokens when their snapshot lease is collected.
    // A typed 400 on this pinned feed route can therefore mean an expired read,
    // not a changed source baseline. Requalify through the existing bounded R
    // stage; never reinterpret an auth error or a write/start response this way.
    private static bool IsCursorGap(GuardedCycleRemoteException e,bool feed)
        =>e.StructuredWireCode&&(e.Status==System.Net.HttpStatusCode.Gone&&e.Code=="cursor_expired"
            ||e.Status==System.Net.HttpStatusCode.Conflict&&e.Code=="journal_gap"
            ||feed&&e.Status==System.Net.HttpStatusCode.BadRequest&&e.Code=="invalid_cursor");
    private static async Task<T> CursorRead<T>(Func<Task<T>> read,bool feed=false)
    {
        try{return await read();}
        catch(GuardedCycleRemoteException e)when(IsCursorGap(e,feed)){throw new CursorGapException(e);}
    }
    private async Task SyncRemote(CancellationToken ct)
    {
        using var measurement=_coordinator.Diagnostics.Measure(SyncPhase.RemoteFeed);
        var pending=await _session.ReadCycleRefreshAsync(ScopeId,ct)is not null;
        while(true)
        {
            try
            {
                await FillRemote(pending,ct);
                if(pending)
                {
                    await Observe(ct);
                    while(await _session.ReconcileCycleStageAsync(ScopeId,ct)==120)ct.ThrowIfCancellationRequested();
                    await _session.InstallCycleStageAsync(ScopeId,ct);
                }
                while(await _session.QualifyBindingAsync(ScopeId,ct)==250)ct.ThrowIfCancellationRequested();
                return;
            }
            catch(CursorGapException)
            {
                if(_coherence is null||_remoteRefreshes>=2)throw;
                await Observe(ct);await _session.BeginCycleRefreshAsync(ScopeId,pending,ct);_remoteRefreshes++;pending=true;
            }
        }
    }
    private async Task FillRemote(bool pending,CancellationToken ct)
    {
        var p=await Progress(pending,ct);
        if(p.ReadyJson is null)
        {
            var snapshot=pending?p.SnapshotId!:await _session.ReserveCycleSnapshotAsync(ScopeId,ct);var raw=await SnapshotStageRead(()=>_remote.StartSnapshotAsync(snapshot,ct),pending,ct);
            for(;;)
            {
                if(raw.Length>LocalCycleWire.MetadataLimit)throw new InvalidDataException("Snapshot progress exceeds fixed limit.");using var json=SyncProtocolReader.Parse(raw);var e=json.RootElement;
                var fields=new[]{"snapshotId","state","startSeq","appliedSeq","cutSeq","entries","documentBytes"};
                if(e.ValueKind!=JsonValueKind.Object||e.EnumerateObject().Count()!=fields.Length||e.EnumerateObject().Any(f=>!fields.Contains(f.Name,StringComparer.Ordinal))||e.GetProperty("snapshotId").GetString()!=snapshot)throw new InvalidDataException("Original snapshot progress identity/shape changed.");
                _=LocalCycleWire.Decimal(e.GetProperty("startSeq").GetString()!);_=LocalCycleWire.Decimal(e.GetProperty("appliedSeq").GetString()!);
                var state=e.GetProperty("state").GetString();if(state=="READY")break;if(state is not("BASE" or "OVERLAY"))throw new InvalidDataException("Snapshot discarded without READY authority.");ct.ThrowIfCancellationRequested();raw=await SnapshotStageRead(()=>_remote.StepSnapshotAsync(snapshot,ct),pending,ct);
            }
            var ready=await SnapshotStageRead(()=>_remote.ReadySnapshotAsync(snapshot,ct),pending,ct);if(pending)await _session.SaveCycleStageReadyAsync(ScopeId,ready,ct);else await _session.SaveCycleReadyAsync(ScopeId,ready,ct);p=await Progress(pending,ct);
        }
        if(!p.SnapshotComplete)
        {
            var ready=JsonSerializer.Deserialize<SyncSnapshotReady>(p.ReadyJson!,LocalCycleWire.Json)!;
            while(!p.Complete)
            {
                var page=await CursorRead(()=>_remoteReadRetry.ExecuteAsync(()=>_remote.ReadEntriesAsync(ready,p.EntriesCursor!,p.AfterNodeId,ct),_coordinator.Diagnostics,ct));
                for(var i=0;i<Math.Max(1,page.Items.Count);i+=120)if(pending)await _session.SaveCycleStageEntriesAsync(ScopeId,page,i,ct);else await _session.SaveCycleSnapshotEntriesAsync(ScopeId,page,i,ct);
                p=await Progress(pending,ct);
            }
        }
        while(true)
        {
            p=await Progress(pending,ct);var page=await CursorRead(()=>_remoteReadRetry.ExecuteAsync(()=>_remote.ReadChangesAsync(p.ChangesCursor!,p.RequestSequence,ct),_coordinator.Diagnostics,ct),feed:true);
            for(var i=0;i<Math.Max(1,page.Items.Count);i+=64)if(pending)await _session.SaveCycleStageFeedAsync(ScopeId,page,i,ct);else await _session.SaveCycleFeedAsync(ScopeId,page,i,ct);
            if(!page.HasMore)break;
        }
        if(!pending)await _session.ValidateCycleAnchorAsync(ScopeId,ct);
    }
    // Read staging is separate from immutable publication work. Retry only an
    // explicit non-admission on the same request, even inside a scheduled cycle.
    // An expired/collected pending stage may be replaced through the existing
    // two-stage budget; original B and publication operations remain untouched.
    private readonly SyncRetryController _remoteReadRetry=new();
    private async Task<T> SnapshotStageRead<T>(Func<Task<T>> request,bool pending,CancellationToken ct)
    {
        try{return await _remoteReadRetry.ExecuteAsync(request,_coordinator.Diagnostics,ct);}
        catch(GuardedCycleRemoteException e)when(pending&&e.StructuredWireCode&&e.Status==System.Net.HttpStatusCode.Gone&&e.Code is "cursor_expired" or "stage_unavailable")
        {throw new CursorGapException(e);}
    }
    private async Task Observe(CancellationToken ct)
    {
        await _watcher.FlushAsync(ct);
        var firstPage=await _session.ReadEventHintsAsync(ct);
        if(firstPage.Gap)
        {
            if(!_reconcileParents)throw new LocalSourceGapException("WatcherGap","Durable watcher gap requires full source reconciliation.");
            await _watcher.BeginGapRecoveryAsync(ct);
            if(!await _session.RequestFullParentProofAsync(ScopeId,ct))
            {await _coordinator.ReconcileParentsAsync(_session,ScopeId,_coherence!,ct);if(!await _session.RequestFullParentProofAsync(ScopeId,ct))throw new LocalSourceGapException("WatcherGap","Pending source proof must finish before gap recovery.");}
            var recovered=await _coordinator.ReconcileParentsAsync(_session,ScopeId,_coherence!,ct);
            if(recovered.Status!=LocalCoherenceStatus.Anchored)throw new LocalSourceGapException("WatcherGap","Full membership proof is incomplete.");
            await _session.AckEventGapAsync(firstPage.GapGeneration,ct);
        }
        while(true)
        {
            var page=await _session.ReadEventHintsAsync(ct,dueOnly:true);
            if(page.Gap)throw new LocalSourceGapException("WatcherGap","A later watcher gap remains queued for full reconciliation.");
            if(page.Hints.Count==0){_watcher.ScheduleRemaining(page.NextDue);break;}var hints=page.Hints.Select(h=>h.Path).ToArray();
            if(_reconcileParents)await _session.MarkParentCycleHintsAsync(ScopeId,hints,ct);else await _session.MarkCycleHintsAsync(ScopeId,hints,ct);
            // A crash before this ACK can only redirty; a later generation is retained.
            await _session.AckEventHintsAsync(page.Hints,ct);
        }
        var state=await _coordinator.ObserveAndRetireChangesAsync(_session,_coherence!,ct);
        // Older clients may have installed a replacement conflict with no later
        // watcher event. Re-prove those candidates on upgrade without resetting
        // their baseline or requiring another edit from the user.
        if(_reconcileParents&&state.Status==LocalCoherenceStatus.Anchored&&await _session.HasRecoverableParentReplacementAsync(ScopeId,ct))
            await _session.RequestFullParentProofAsync(ScopeId,ct);
        if(_reconcileParents&&(state.Status!=LocalCoherenceStatus.Anchored||(await _session.ReadCycleScopeAsync(ScopeId,ct)).Status==2))
            state=await _coordinator.ReconcileParentsAsync(_session,ScopeId,_coherence!,ct,async(o,t)=>{if(_observer is not null)await _observer(new(WindowsCycleStage.BranchEnumerated,ScopeId,RelativePath:o.ParentId128),t);});
        if(state.Status!=LocalCoherenceStatus.Anchored)throw new LocalSourceGapException("CycleNamespaceUncertain",state.Error??"Scoped reconciliation required.");
        while(await _session.ImportCycleDirtyAsync(ScopeId,ct)>0)ct.ThrowIfCancellationRequested();
        if(_reconcileParents)while(await _session.RestoreParentConflictsAsync(ScopeId,ct)>0)ct.ThrowIfCancellationRequested();
    }
    private async Task<WindowsCycleResult> RunAsync(bool initial,CancellationToken ct)
    {
        await _serial.WaitAsync(ct);_syncProgress.Begin();_scheduledOperation.Value="cycle";var firstFiles=_capturedFiles;var firstBytes=_capturedBytes;var firstReceived=_received;var firstPublished=_publishedFiles;var firstDirectories=_publishedDirectories;string? error=null;var retryableFailure=false;
        using var measurement=_coordinator.Diagnostics.Measure(SyncPhase.Recovery);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed,this);_dispatch=Guid.NewGuid().ToString("N");_pauseNew=0;_backpressure=null;_scheduledPause=null;_remoteRefreshes=0;if(initial==(_coherence is not null))throw new InvalidOperationException("Initial once; fresh cycles reuse the established native anchor.");
            if(await _session.HasPendingReplacementAsync(ScopeId,ct))throw new LocalSourceGapException("ReplacementPending","Use explicit replacement qualification; ordinary cycles cannot publish or adopt a pending replacement.");
            if(!await _session.ScheduleReadyAsync(ScopeId,"cycle","cycle",ct))throw new OperationNotDueException();
            while(await _session.ResumeDueOperationsAsync(ScopeId,ct)==120)ct.ThrowIfCancellationRequested();
            // A retained structural plan must close LOCAL membership before
            // the ordinary dispatcher can use a status2 scope. This grants no
            // remote authority: original recovery/feed and current CAS remain
            // below, and no data worker runs during this closure.
            if(!initial&&_reconcileParents&&((await _session.ReadCycleScopeAsync(ScopeId,ct)).Status==2
                ||(await _session.ReadCoherenceAsync(_coherence!,ct)).Status!=LocalCoherenceStatus.Anchored))await Observe(ct);
            await ApplyRequestedBatchSetting(ct);
            while(await _session.ContinuePolicyAsync(ScopeId,ct)==250)ct.ThrowIfCancellationRequested();
            if(_retryDeferredRequested){while(await _session.RetryCycleDeferredAsync(ScopeId,ct)>0)ct.ThrowIfCancellationRequested();_retryDeferredRequested=false;}
            // Pending private server transports can close the current feed.
            // Inspect/replay immutable originals first, using historical intent
            // only. B/dirty acknowledgement still requires Observe + fresh feed.
            await ReconcileSmallBatchArtifacts(ScopeId,ct);
            if(!initial){await ReadSmallBatchRecoveryStatuses(ct);await ProcessWork(()=>true,ct,originalsOnly:true);await ReconcileSmallBatchArtifacts(ScopeId,ct);if(Volatile.Read(ref _pauseNew)!=0)throw PausedWork();}
            await RefreshSmallBatchQualification(ct);
            await SyncRemote(ct);
            if(initial)
            {
                _syncProgress.Stage(SyncProgressStage.Scanning);
                using var scanShutdown=CancellationTokenSource.CreateLinkedTokenSource(ct);var done=0;using var wake=new SemaphoreSlim(0,1);_wake=wake;
                var pumping=ProcessWork(()=>Volatile.Read(ref done)!=0,scanShutdown.Token,bulk:true);
                LocalCoherenceState state;var scanFailed=false;
                try
                {
                    state=await _coordinator.BuildAnchorAsync(_session,new(MaximumProfileBytes:_resources.Budget.ProfileBytes,MinimumFreeDiskBytes:_resources.Budget.MinimumFreeDiskBytes),new(MaximumProfileBytes:_resources.Budget.ProfileBytes,MinimumFreeDiskBytes:_resources.Budget.MinimumFreeDiskBytes),ct);
                    _coherence=state.CoherenceId;
                }
                catch{scanFailed=true;scanShutdown.Cancel();throw;}
                finally
                {
                    Interlocked.Exchange(ref done,1);Wake();
                    try{await pumping;}
                    catch(OperationCanceledException)when(scanFailed&&!ct.IsCancellationRequested){/* Preserve the original scan failure after joining workers. */}
                    finally{_wake=null;}
                }
                // The native cut can expose unrelated/hardlink events after
                // enumeration. Close membership with the ordinary guarded
                // reconciliation, on this SAME coherence, before final binding.
                // Workers must have joined; the binding guard remains strict.
                if(_reconcileParents&&state.Status!=LocalCoherenceStatus.Anchored)await Observe(ct);
                await _session.BindCycleCoherenceAsync(ScopeId,_coherence,ct);
                while(await _session.SeedCycleEntriesAsync(ScopeId,ct)>0)ct.ThrowIfCancellationRequested();
                await PlanProgress(ct);
            }
            if(Volatile.Read(ref _pauseNew)!=0)throw PausedWork();
            for(var pass=0;pass<4;pass++)
            {
                _syncProgress.Stage(SyncProgressStage.Verifying);
                await Observe(ct);await SyncRemote(ct);
                await ProcessNamespace(ct);
                // Previous immutable receipts cannot clear later dirty pairs.
                while(true){var receipts=await _session.ReadCycleActiveWorkAsync(ScopeId,true,ct);if(receipts.Count==0)break;await _session.AcknowledgeCycleWorksetAsync(ScopeId,receipts.Select(r=>r.OperationId).ToArray(),_capture.RootProof,ct);}
                await PlanProgress(ct);await ProcessWork(()=>true,ct);if(Volatile.Read(ref _pauseNew)!=0)throw PausedWork();_syncProgress.Stage(SyncProgressStage.Verifying);await Observe(ct);await SyncRemote(ct);
                while(true){var receipts=await _session.ReadCycleActiveWorkAsync(ScopeId,true,ct);if(receipts.Count==0)break;await _session.AcknowledgeCycleWorksetAsync(ScopeId,receipts.Select(r=>r.OperationId).ToArray(),_capture.RootProof,ct);}
                if((await _session.ReadCyclePendingAsync(ScopeId,ct)).Pending==0)break;
            }
            if(_observer is not null)try{await _observer(new(WindowsCycleStage.CycleObserved,ScopeId),ct);}catch{_resources.Diagnostic();}
            await _session.ClearOperationWaitAsync(ScopeId,"cycle",ct);
        }
        catch(Exception e)
        {
            error=e is OperationCanceledException?"Cancelled; original dirty generations, intents and publication evidence retained.":e.GetType().Name+": "+e.Message;
            retryableFailure=!ct.IsCancellationRequested&&(e is OperationNotDueException or ScheduledWorkDeferredException||SyncRetryController.Classify(e)is not null);
            if(e is not OperationNotDueException&&!ct.IsCancellationRequested)
                try
                {
                    if(e is ScheduledWorkDeferredException)await _session.ClearOperationWaitAsync(ScopeId,"cycle",CancellationToken.None);
                    else await RecordRetry("cycle",e,CancellationToken.None);
                }
                catch{_resources.Diagnostic();}
        }
        try
        {
        var scope=await _session.ReadCycleScopeAsync(ScopeId,CancellationToken.None);LocalCoherenceState? local=_coherence is null?null:await _session.ReadCoherenceAsync(_coherence,CancellationToken.None);
        CyclePending pending;try{pending=await _session.ReadCyclePendingAsync(ScopeId,CancellationToken.None);}catch{pending=new(scope.Files,0,0);}
        LocalCycleRemoteProgress? remote;try{remote=await _session.ReadCycleRemoteProgressAsync(ScopeId,CancellationToken.None);}catch{remote=null;}
        var agree=error is null&&scope.SeedComplete&&local?.Status==LocalCoherenceStatus.Anchored&&remote?.Complete==true&&pending.Pending==0;
        _syncProgress.Stage(agree?SyncProgressStage.Completed:ct.IsCancellationRequested?SyncProgressStage.Cancelled:retryableFailure||error is null?SyncProgressStage.Waiting:SyncProgressStage.Failed);
        try{Volatile.Write(ref _schedule,await _session.ReadOperationScheduleAsync(ScopeId,CancellationToken.None));_watcher.ScheduleRetry(_schedule.NextAttemptUnixMilliseconds);if(_schedule.State is null)_coordinator.Diagnostics.ClearWait();}catch{_resources.Diagnostic();}
        return new(ScopeId,_coherence,local?.NamespaceCoverage??"Unknown",agree?"HistoricalAtAppliedEventBoundary":"Unknown",agree?"NoWorkAtAppliedEventBoundary":"PendingOrUncertain",local?.AppliedUsn??0,remote?.Sequence??0,pending.Pending,_capturedFiles-firstFiles,_capturedBytes-firstBytes,_received-firstReceived,_publishedFiles-firstPublished,_publishedDirectories-firstDirectories,pending.Conflicts,pending.Deferred,error){RetryState=error is null||retryableFailure?_schedule.State:null};
        }
        finally{_scheduledOperation.Value=null;_serial.Release();}
    }
    private async Task ProcessWork(Func<bool> finished,CancellationToken ct,bool originalsOnly=false,bool bulk=false)
    {
        try { await ProcessWorkCore(finished,ct,originalsOnly,bulk); }
        catch(LocalSourceGapException e)when(_reconcileParents&&_coherence is not null&&e.Code=="CycleNamespaceUncertain")
        {
            // The journal pump fences new dispatch while existing workers join.
            // Only then may the ordinary native membership proof reconcile an
            // unrelated volume hardlink or a scoped namespace transition. Never
            // classify it Outside from a path lookup or clear historical evidence.
            await Observe(ct);
            if(_backpressure=="Journal observation requires recovery: "+e.Message)
            {
                _backpressure=null;_scheduledPause=null;
                Interlocked.Exchange(ref _pauseNew,0);
            }
        }
    }
    private async Task ProcessWorkCore(Func<bool> finished,CancellationToken ct,bool originalsOnly=false,bool bulk=false)
    {
        var ownedWake=_wake is null?new SemaphoreSlim(0,1):null;if(ownedWake is not null)_wake=ownedWake;
        var queue=Channel.CreateBounded<LocalCycleWork>(new BoundedChannelOptions(32){SingleWriter=true,SingleReader=false,FullMode=BoundedChannelFullMode.Wait});var inflight=0;
        // Retained bootstrap backlogs continue through FreshSyncAsync too.
        // Both paths need overlap; fresh edits retain their 250 ms age bound.
        var pipeline=!originalsOnly&&_smallBatchCapability is not null;
        _rootMutationAdmission=pipeline&&_smallBatchCapability!.ExecutionJob
            ?new(_smallBatchCapability.ParallelJobs,()=>PauseNew("Root original is unresolved; recover before new network work.")):null;
        _smallBatchScheduler=!originalsOnly&&_smallBatchCapability is not null?
            new(bulk,Math.Min(pipeline?SmallFileBatchScheduler.BulkItems:4,_smallBatchCapability.MaximumItems),pipeline?(_smallBatchCapability.ExecutionJob?_smallBatchCapability.ParallelJobs:2):1,DispatchSmallBatch,ct,_rootMutationAdmission is not null):null;
        _resources.DeferCaptureOnSpoolPressure=pipeline;
        using var journalStop=CancellationTokenSource.CreateLinkedTokenSource(ct);
        Exception? journalFailure=null;
        var journal=originalsOnly?Task.CompletedTask:Task.Run(async()=>
        {
            using var timer=new PeriodicTimer(TimeSpan.FromSeconds(1));
            try
            {
                while(await timer.WaitForNextTickAsync(journalStop.Token))
                {
                    var coherence=Volatile.Read(ref _coherence);if(coherence is null)continue;
                    var observed=await _coordinator.ObserveAndRetireChangesAsync(_session,coherence,journalStop.Token);
                    if(observed.Status!=LocalCoherenceStatus.Anchored)throw new LocalSourceGapException("CycleNamespaceUncertain",observed.Error??"Scoped reconciliation required.");
                }
            }
            catch(OperationCanceledException)when(journalStop.IsCancellationRequested){}
            catch(Exception e){journalFailure=e;PauseNew("Journal observation requires recovery: "+e.Message);}
        },CancellationToken.None);
        var workerCount=pipeline?(_smallBatchCapability is {ExecutionJob:true,ParallelJobs:2}?SmallFileBatchScheduler.PipelinedWorkers:SmallFileBatchScheduler.BulkWorkers):_resources.Budget.DataJobs;
        var workers=Enumerable.Range(0,workerCount).Select(_=>Task.Run(async()=>
        {
            try{await foreach(var work in queue.Reader.ReadAllAsync(ct)){try{_workLane.Value=originalsOnly?SyncWorkLane.Recovery:bulk?SyncWorkLane.Bootstrap:SyncWorkLane.Live;if(work.InputJson is not null||Volatile.Read(ref _pauseNew)==0)await Execute(work,ct);}finally{Interlocked.Decrement(ref inflight);Wake();}}}catch(OperationCanceledException)when(ct.IsCancellationRequested){}
        },CancellationToken.None)).ToArray();
        try
        {
            while(true)
            {
                ct.ThrowIfCancellationRequested();var scope=await _session.ReadCycleScopeAsync(ScopeId,ct);
                if(scope.CoherenceId is not null&&(originalsOnly||Volatile.Read(ref _pauseNew)==0))
                {
                    if(pipeline)
                    {
                        if(!await queue.Writer.WaitToWriteAsync(ct))break;
                        if(Volatile.Read(ref _pauseNew)!=0)continue;
                        var workset=await _session.DispatchCycleWorksetAsync(ScopeId,_dispatch,Math.Min(16,Math.Max(1,32-queue.Reader.Count)),ct);
                        if(workset.Count>0)
                        {
                            Interlocked.Add(ref inflight,workset.Count);
                            foreach(var member in workset)await queue.Writer.WriteAsync(member,ct);
                            _resources.Queue(queue.Reader.Count);continue;
                        }
                    }
                    else
                    {
                    var work=originalsOnly?await _session.DispatchCycleOriginalAsync(ScopeId,_dispatch,ct):await _session.DispatchCycleWorkAsync(ScopeId,_dispatch,ct);
                    if(!originalsOnly&&work is null&&Volatile.Read(ref _pauseNew)==0&&await _session.ClaimCycleWorkAsync(ScopeId,ct)is not null)work=await _session.DispatchCycleWorkAsync(ScopeId,_dispatch,ct);
                    if(work is not null){Interlocked.Increment(ref inflight);await queue.Writer.WriteAsync(work,ct);_resources.Queue(queue.Reader.Count);continue;}
                    }
                }
                if((originalsOnly||Volatile.Read(ref _pauseNew)!=0||finished())&&Volatile.Read(ref inflight)==0)break;
                // No polling sleep; parent publication/scan eligibility wakes
                // the bounded producer. A fresh pass has no shared scan signal.
                await _wake!.WaitAsync(ct);
            }
        }
        finally
        {
            queue.Writer.TryComplete();
            try{await Task.WhenAll(workers);}
            finally
            {
                try{if(_smallBatchScheduler is not null)await _smallBatchScheduler.DisposeAsync();}
                finally{_smallBatchScheduler=null;_rootMutationAdmission=null;_resources.DeferCaptureOnSpoolPressure=false;await journalStop.CancelAsync();await journal;if(ownedWake is not null){_wake=null;ownedWake.Dispose();}}
            }
        }
        if(journalFailure is not null)System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(journalFailure).Throw();
    }
    private async Task Execute(LocalCycleWork initial,CancellationToken ct)
    {
        RootMutationAdmission.Lease? rootAdmission=null;
        IDisposable? sharedJob=null;var scheduled=false;
        var started=DateTimeOffset.UtcNow;var failed=false;
        var work=initial;var largeTaken=false;var reserved=false;var capturedHere=false;
        try
        {
            if(!await _session.ScheduleReadyAsync(ScopeId,initial.OperationId,"work",ct))return;
            scheduled=true;_scheduledOperation.Value=initial.OperationId;
            _syncProgress.Start(initial.OperationId,initial.Entry.RelativePath);
            sharedJob=await DataSlot(Math.Min(initial.Size??initial.Entry.EnumeratedSize,8L*1024*1024),ct);
            if(work.InputJson is null&&Volatile.Read(ref _pauseNew)!=0)return;
            if(await TryMetadataWork(work,ct))return;
            if(await TryStreamWork(work,ct))return;
            if(!work.Entry.Directory&&work.InputJson is null&&await _session.ReadContentMutationAsync(ScopeId,work.OperationId,ct) is {State:not("NOT_PUBLISHED" or "CANCELLED")})
            {
                WindowsNativeCycleCapture.ValidatePersisted(work,work.Size!.Value,work.Sha256!,work.CaptureProof!,_capture.RootProof);
                if(await TryContentMutation(work,"",null,ct)){await PostCommit(work,WindowsCycleStage.FilePublished,true,ct);return;}
            }
            _=await _session.ReadCycleScopeAsync(ScopeId,ct);
            // A persisted original must survive later target rename/version
            // changes. Its status/receipt is historical evidence, not current R.
            var target=work.InputJson is null?await _session.ReadCycleTargetAsync(ScopeId,work.Entry,ct):
                (Parent:"",Target:(SyncDescriptor?)null,Topology:(IReadOnlyList<CatalogParentTopology>)[]);
            if(work.Entry.Directory)
            {
                if(work.InputJson is null){await _resources.Reader.WaitAsync(ct);_resources.StartReader();try{await _capture.ValidateDirectoryAsync(work.Entry,ct);}finally{_resources.EndReader();_resources.Reader.Release();}}
                if(target.Target is not null&&target.Target.Type!="folder")throw new CycleConflictException("Original directory conflicts with a remote file.");
                if(work.InputJson is null&&target.Target is { } knownFolder&&work.Entry.BaselineJson is { } directoryBaseline)
                {
                    var prior=JsonSerializer.Deserialize<LocalCycleBaseline>(directoryBaseline,LocalCycleWire.Json)!;
                    if(prior.Description.NodeId==knownFolder.NodeId&&prior.Description.ParentId==knownFolder.ParentId&&prior.Description.Name==knownFolder.Name&&knownFolder.Availability=="AVAILABLE")
                    {await _session.FinishCycleWorkAsync(ScopeId,work.OperationId,LocalCycleWorkPhase.AdoptedEqual,JsonSerializer.Serialize(knownFolder,LocalCycleWire.Json),null,ct);await PostCommit(work,WindowsCycleStage.DirectoryPublished,false,ct);return;}
                }
                if(work.InputJson is null&&target.Target is not null&&await _session.HasReplacementDirectoryPublicationAsync(ScopeId,work.Entry,target.Target,ct))
                {await _session.FinishCycleWorkAsync(ScopeId,work.OperationId,LocalCycleWorkPhase.AdoptedEqual,JsonSerializer.Serialize(target.Target,LocalCycleWire.Json),null,ct);await PostCommit(work,WindowsCycleStage.DirectoryPublished,false,ct);return;}
                if(work.InputJson is null){await _session.SaveCycleFolderInputAsync(ScopeId,work.OperationId,new(work.OperationId,"neptune-cycle-"+work.OperationId,_configuration.Binding.RootId,_configuration.Binding.ServerEpoch,_configuration.Binding.BindingVersion,target.Parent,work.Entry.RawName,"ensure",target.Topology),ct);work=await _session.ReadCycleWorkAsync(ScopeId,work.OperationId,ct);}
                var originalFolder=JsonSerializer.Deserialize<CycleFolderInput>(work.InputJson!,LocalCycleWire.Json)!;
                rootAdmission=await AcquireRootMutation(null,ct);rootAdmission?.Attempt();
                var outcome=await Admission(()=>_remote.EnsureDirectoryAsync(originalFolder,ct),ct);var description=outcome.Description;
                if(description.Type!="folder"||description.ParentId!=originalFolder.ParentId||description.Name!=originalFolder.Name)throw new InvalidDataException("Folder outcome rebinds original topology/type.");
                await _session.FinishCycleWorkAsync(ScopeId,work.OperationId,LocalCycleWorkPhase.Published,outcome.RawJson,null,ct);rootAdmission?.Complete();
                using(var folder=SyncProtocolReader.Parse(System.Text.Encoding.UTF8.GetBytes(outcome.RawJson)))
                    if(folder.RootElement.GetProperty("result").GetProperty("status").GetString()=="CREATED")Interlocked.Increment(ref _publishedDirectories);
                await PostCommit(work,WindowsCycleStage.DirectoryPublished,false,ct);return;
            }
            if(work.Phase==LocalCycleWorkPhase.Queued)
            {
                await _resources.Reader.WaitAsync(ct);_resources.StartReader();long currentSize;
                try{currentSize=await _capture.MeasureFileAsync(work.Entry,ct);}finally{_resources.EndReader();_resources.Reader.Release();}
                if(currentSize>Math.Min(_resources.Budget.SpoolBytes,_resources.Budget.MaximumNewFileBytes??_resources.Budget.SpoolBytes))throw new BootstrapCaptureDeferredException("New file exceeds current capture/remote capability limit; original sealed operations remain recoverable.");
                if(currentSize>=_resources.Budget.LargeFileThreshold){await _resources.Large.WaitAsync(ct);largeTaken=true;_resources.StartLarge();}
                await _resources.ReserveAsync(work.OperationId,currentSize,ct);reserved=true;await _resources.Reader.WaitAsync(ct);_resources.StartReader();
                LocalCycleSeal seal;try{using var cpu=await SyncResourceScheduler.CpuSlot(ct);using var captureMeasurement=_coordinator.Diagnostics.Measure(SyncPhase.Capture);seal=await _capture.CaptureAsync(work,_resources.PathFor(work.OperationId),currentSize,ct);_coordinator.Diagnostics.Items(SyncPhase.Capture,seal.Size);}finally{_resources.EndReader();_resources.Reader.Release();}
                LocalCycleWork? combined=null;
                if(_smallBatchScheduler is not null&&target.Target is null&&work.Entry.BaselineJson is null&&seal.Size>0&&seal.Size<=_smallBatchCapability!.MaximumFileBytes)
                    combined=await _session.TrySealCycleInputAsync(ScopeId,work.OperationId,seal,_capture.RootProof,UploadInput(work,target.Parent,null,target.Topology,seal.Size,seal.Sha256),ct);
                if(combined is not null)work=combined;
                else{await _session.SealCycleWorkAsync(ScopeId,work.OperationId,seal,_capture.RootProof,ct);work=await _session.ReadCycleWorkAsync(ScopeId,work.OperationId,ct);}
                capturedHere=true;Interlocked.Increment(ref _capturedFiles);Interlocked.Add(ref _capturedBytes,seal.Size);
                if(_observer is not null)await _observer(new(WindowsCycleStage.FileCaptured,ScopeId,work.OperationId,work.Entry.RelativePath),ct);
            }
            else{if(work.Size>=_resources.Budget.LargeFileThreshold){await _resources.Large.WaitAsync(ct);largeTaken=true;_resources.StartLarge();}await _resources.ReserveAsync(work.OperationId,work.Size??throw new InvalidDataException("Original sealed size absent."),ct);reserved=true;WindowsNativeCycleCapture.ValidatePersisted(work,work.Size!.Value,work.Sha256!,work.CaptureProof!,_capture.RootProof);}
            await using var spool=await _resources.OpenAsync(work,!capturedHere,ct);
            if(work.InputJson is null&&await TryContentMutation(work,target.Parent,target.Target,ct))
            {await spool.DisposeAsync();await PostCommit(work,WindowsCycleStage.FilePublished,true,ct);return;}
            if(work.InputJson is null)
            {
                var remote=target.Target;LocalCycleBaseline? baseline=work.Entry.BaselineJson is null?null:JsonSerializer.Deserialize<LocalCycleBaseline>(work.Entry.BaselineJson,LocalCycleWire.Json);
                if(remote is not null)
                {
                    if(remote.Type!="file"||remote.Availability!="AVAILABLE")throw new CycleConflictException("Remote target is not an available file.");
                    if(remote.ContentSha256==work.Sha256&&remote.SizeBytes==work.Size!.Value.ToString(CultureInfo.InvariantCulture)&&(baseline is not null||_configuration.AllowVerifiedEqualAdoption||await _session.HasCycleOwnershipAsync(ScopeId,work.Entry,remote,ct)))
                    {await _session.FinishCycleWorkAsync(ScopeId,work.OperationId,LocalCycleWorkPhase.AdoptedEqual,JsonSerializer.Serialize(remote,LocalCycleWire.Json),null,ct);await spool.DisposeAsync();await PostCommit(work,WindowsCycleStage.FileCaptured,true,ct);return;}
                    if(baseline is null||baseline.Description!=remote)throw new CycleConflictException("Unknown/changed remote content cannot be overwritten by a fresh HEAD/current descriptor.");
                }
                await _session.SaveCycleInputAsync(ScopeId,work.OperationId,UploadInput(work,target.Parent,remote,target.Topology,work.Size!.Value,work.Sha256!),ct);work=await _session.ReadCycleWorkAsync(ScopeId,work.OperationId,ct);
            }
            if(capturedHere&&_smallBatchScheduler is { } scheduler&&work.Size<=_smallBatchCapability!.MaximumFileBytes)
            {
                // Aggregation opens originals serially; no worker retains an
                // extra original descriptor while waiting for its age group.
                await spool.DisposeAsync();
                sharedJob.Dispose();sharedJob=null;
                if(await scheduler.SubmitAsync(work))
                {
                    var completed=await _session.ReadCycleWorkAsync(ScopeId,work.OperationId,ct);
                    Interlocked.Add(ref _received,Math.Max(0,completed.ReceivedBytes-work.ReceivedBytes));
                    if(completed.Phase==LocalCycleWorkPhase.Published){Interlocked.Increment(ref _publishedFiles);await PostCommit(completed,WindowsCycleStage.FilePublished,true,ct);}
                    // An explicit per-file pending result retains this original.
                    // FinishScheduledOperation persists its retry deadline; it
                    // must not stop independent files in this bounded pass.
                    return;
                }
                // The unchanged individual fallback reacquires bounded work
                // admission; a sealed waiter never holds a capture slot.
                sharedJob=await DataSlot(work.Size??0,ct);
            }
            rootAdmission=await AcquireRootMutation(null,ct);
            await using var transferSpool=spool.CanRead?spool:await _resources.OpenAsync(work,true,ct);
            var original=JsonSerializer.Deserialize<CycleUploadInput>(work.InputJson!,LocalCycleWire.Json)!;
            using var transferMeasurement=_coordinator.Diagnostics.Measure(SyncPhase.Transfer);
            await _session.MarkCycleSendingAsync(ScopeId,work.OperationId,ct);
            rootAdmission?.Attempt();
            var status=_smallBatchRecoveryStatuses.TryRemove(work.OperationId,out var recoveredStatus)?recoveredStatus:await Admission(()=>_remote.UploadAsync(work.UploadId is null?"prepare":"inspect",original,work.UploadId,work.IntentHash!,ct),ct);await _session.SaveCycleStatusAsync(ScopeId,work.OperationId,status,ct);
            if(status.Phase is "published" or "not_published")rootAdmission?.Complete();
            if(status.Phase=="published"){Interlocked.Increment(ref _publishedFiles);await transferSpool.DisposeAsync();await PostCommit(work,WindowsCycleStage.FilePublished,true,ct);return;}
            if(status.Phase=="not_published")return;
            _syncProgress.TransferStart(work.OperationId,transferSpool.Length,status.ReceivedBytes);
            var buffer=new byte[8*1024*1024];transferSpool.Position=status.ReceivedBytes;
            while(transferSpool.Position<transferSpool.Length)
            {
                var offset=transferSpool.Position;var wanted=(int)Math.Min(buffer.Length,transferSpool.Length-offset);await transferSpool.ReadExactlyAsync(buffer.AsMemory(0,wanted),ct);
                await _session.MarkCycleSendingAsync(ScopeId,work.OperationId,ct);using(var slot=await TransferSlot(wanted,ct))status=await Admission(()=>_remote.AppendAsync(original,status.UploadId,work.IntentHash!,offset,buffer.AsMemory(0,wanted),ct),ct);await _session.SaveCycleStatusAsync(ScopeId,work.OperationId,status,ct);
                if(status.ReceivedBytes!=offset+wanted)throw new InvalidDataException("Server append acknowledged a different immutable offset.");Interlocked.Add(ref _received,wanted);_coordinator.Diagnostics.Items(SyncPhase.Transfer,wanted);
                _syncProgress.Acknowledge(work.OperationId,status.ReceivedBytes);
            }
            await _session.MarkCycleSendingAsync(ScopeId,work.OperationId,ct);status=await Admission(()=>_remote.UploadAsync("complete",original,status.UploadId,work.IntentHash!,ct),ct);await _session.SaveCycleStatusAsync(ScopeId,work.OperationId,status,ct);
            if(status.Phase is "published" or "not_published")rootAdmission?.Complete();
            if(status.Phase!="published")return;Interlocked.Increment(ref _publishedFiles);await transferSpool.DisposeAsync();await PostCommit(work,WindowsCycleStage.FilePublished,true,ct);
        }
        catch(Exception error)
        {
            failed=true;
            // A transport exception can be an applied effect with a lost reply.
            // Never generate another key or recapture attempted bytes.
            try
            {
                work=await _session.ReadCycleWorkAsync(ScopeId,initial.OperationId,CancellationToken.None);
                if(work.Phase is not(LocalCycleWorkPhase.Published or LocalCycleWorkPhase.AdoptedEqual))
                {
                    var extendedOriginal=await _session.ReadStreamAsync(ScopeId,work.OperationId,CancellationToken.None)is not null||await _session.ReadContentMutationAsync(ScopeId,work.OperationId,CancellationToken.None)is not null||await _session.ReadMetadataSealAsync(ScopeId,work.OperationId,CancellationToken.None)is not null;
                    var phase=extendedOriginal&&work.InputJson is null?LocalCycleWorkPhase.Deferred:error is CycleConflictException?LocalCycleWorkPhase.Conflict:work.InputJson is not null?LocalCycleWorkPhase.OutcomeUnknown:SyncRetryController.Classify(error)is not null||error is OperationCanceledException?LocalCycleWorkPhase.Deferred:LocalCycleWorkPhase.Failed;
                    await _session.FinishCycleWorkAsync(ScopeId,work.OperationId,phase,work.ResultJson,error.GetType().Name+": "+error.Message[..Math.Min(error.Message.Length,900)],CancellationToken.None);
                    if(!ct.IsCancellationRequested)await RecordRetry(work.OperationId,error,CancellationToken.None);
                    if(extendedOriginal||phase==LocalCycleWorkPhase.OutcomeUnknown||error is CycleSpoolAdmissionException)PauseNew(error.GetType().Name+": "+error.Message,!ct.IsCancellationRequested&&SyncRetryController.Classify(error)is not null?error:null);
                }
                else if(reserved&&work.CaptureProof is not null)
                {
                    // A later partition can fail after this original's receipt
                    // was saved. Retire only the proven terminal local copy;
                    // unresolved originals keep their normal recovery path.
                    await PostCommit(work,work.Phase==LocalCycleWorkPhase.Published?WindowsCycleStage.FilePublished:WindowsCycleStage.FileCaptured,true,CancellationToken.None);
                }
                else _resources.Diagnostic();
            }
            catch{_resources.Diagnostic();}
            if(reserved&&work.CaptureProof is null)try{_resources.Release(work.OperationId,true);}catch{_resources.Diagnostic();}
        }
        finally
        {
            rootAdmission?.Dispose();
            _syncProgress.Release(initial.OperationId);
            if(scheduled)try{await FinishScheduledOperation(initial.OperationId,started,failed,CancellationToken.None);}catch{_resources.Diagnostic();}
            _scheduledOperation.Value=null;
            if(reserved)try{_resources.Release(initial.OperationId,false);}catch{_resources.Diagnostic();}if(largeTaken){_resources.EndLarge();_resources.Large.Release();}sharedJob?.Dispose();
        }
    }
    private CycleUploadInput UploadInput(LocalCycleWork work,string parent,SyncDescriptor? remote,IReadOnlyList<CatalogParentTopology> topology,long size,string hash)
    {
        var intent=new CycleUploadIntent("saturn.upload-intent.v2","windows_sync",_configuration.DeviceId,_configuration.Binding.RootId,"/"+work.Entry.RelativePath,"neptune-cycle-"+work.OperationId,parent,work.Entry.RawName,remote?.NodeId,size.ToString(CultureInfo.InvariantCulture),hash,null,remote is null?"*":null,remote is null,remote?.ContentVersionId,
            remote is null?new("absent"):new("match",remote.NodeId,remote.Version),topology);
        return new(_configuration.Binding.ServerEpoch,_configuration.Binding.BindingVersion,_configuration.BackendIdentity,intent);
    }
    private async Task PostCommit(LocalCycleWork work,WindowsCycleStage stage,bool delete,CancellationToken ct)
    {
        _syncProgress.Complete(work.OperationId);
        try{if(delete)_resources.Release(work.OperationId,true);}catch{_resources.Diagnostic();}
        if(_observer is not null)try{await _observer(new(stage,ScopeId,work.OperationId,work.Entry.RelativePath),ct);}catch{_resources.Diagnostic();}
    }
    public async ValueTask DisposeAsync()
    {await _serial.WaitAsync();try{if(_disposed)return;_disposed=true;try{await _watcher.DisposeAsync();}finally{await _capture.DisposeAsync();await _coordinator.DisposeAsync();await _session.DisposeAsync();_resources.Dispose();}}finally{_serial.Release();}}
    private sealed class CycleConflictException(string message):IOException(message);
}
