namespace Neptune.Core;

// Opt-in native-only composition. Arbitrary candidate JSON or adapter claims
// cannot call the internal build/alias/cut transitions through this API.
// A complete namespace does not seal bytes or close a writer cycle.
public sealed partial class WindowsLocalCoherenceCoordinator:IAsyncDisposable
{
    private readonly WindowsLocalNamespaceSource _source;
    private readonly WindowsLocalDirectoryAliasSource _aliases;
    private readonly ILocalJournalReader _journal;
    public SyncDiagnostics Diagnostics { get; } = new();
    private readonly Func<LocalCoherenceObservation,CancellationToken,Task>? _observer;
    private readonly SemaphoreSlim _serial=new(1,1);private bool _disposed;
    private Func<string,string,CancellationToken,Task>? _replacementAnchorStarted;
    internal void SetReplacementAnchorStarted(Func<string,string,CancellationToken,Task>? observer)=>_replacementAnchorStarted=observer;
    public LocalNamespaceRootProof RootProof=>_source.RootProof;
    private WindowsLocalCoherenceCoordinator(WindowsLocalNamespaceSource source,ILocalJournalReader journal,Func<LocalCoherenceObservation,CancellationToken,Task>? observer)
    {(_source,_journal,_observer)=(source,journal,observer);_aliases=new(source.RootProof);}
    public static async Task<WindowsLocalCoherenceCoordinator> OpenAsync(string sourceRoot,CancellationToken cancellationToken=default,
        Func<LocalCoherenceObservation,CancellationToken,Task>? observer=null)
        =>await OpenWithModeAsync(sourceRoot,LocalJournalReadMode.Named,cancellationToken,observer);
    // The mode is explicit and persisted by the owned local subscription.
    // Identity-only events never supply names for namespace reconciliation.
    internal static async Task<WindowsLocalCoherenceCoordinator> OpenWithModeAsync(string sourceRoot,LocalJournalReadMode nativeMode,
        CancellationToken cancellationToken=default,Func<LocalCoherenceObservation,CancellationToken,Task>? observer=null)
    {
        UsnRecordParser.ValidateMode(nativeMode);
        var source=await WindowsLocalNamespaceSource.OpenAsync(sourceRoot,cancellationToken);
        try{return new(source,WindowsVolumeJournalSubscription.Open(source.RootProof.VolumeGuid,nativeMode),observer);}catch{await source.DisposeAsync();throw;}
    }
    public ValueTask<LocalJournalBounds> QueryJournalAsync(CancellationToken cancellationToken=default)=>_journal.QueryAsync(cancellationToken);
    private async Task RequireNativeRootBinding(LocalSourceJournalSession session,CancellationToken ct)
    {
        var binding=(await session.ReadStateAsync(ct)).Binding;var proof=_source.RootProof;
        if(!string.Equals(Path.GetFullPath(binding.SourceRoot),Path.GetFullPath(proof.RootPath),StringComparison.OrdinalIgnoreCase)
            ||binding.RootFileId!=proof.Handle.FileId128||binding.RootIncarnation!=proof.Handle.CreationTime
            ||binding.VolumeGuid!=proof.VolumeGuid||binding.VolumeSerial!=proof.Handle.LegacyVolumeSerial.ToString("x8")||binding.FileSystem!="NTFS")
            throw new InvalidDataException("Native coordinator root does not match the owned local subscription.");
    }
    private async Task CaptureThrough(LocalSourceJournalSession session,LocalJournalBounds boundary,CancellationToken ct)
    {
        using var phase=Diagnostics.Measure(SyncPhase.JournalIngest);
        while(true)
        {
            ct.ThrowIfCancellationRequested();var state=await session.ReadStateAsync(ct);
            Diagnostics.Backlog(state.InboxRecords,boundary.NextUsn-state.Cursor.NextReadUsn);
            await ValidateObservedBoundary(session,state,boundary,ct);
            if(state.Cursor.NextReadUsn>=boundary.NextUsn)return;
            WindowsUsnJournalReader.ValidateCursor(boundary,state.Cursor);
            var result=await session.CaptureNextAsync(_journal,cancellationToken:ct);
            Diagnostics.Items(SyncPhase.JournalIngest,result.Records);
            if(result.NextReadUsn<=state.Cursor.NextReadUsn)throw new LocalSourceGapException("NativeNoProgress","Native journal read did not reach its observed boundary.");
        }
    }
    private static async Task ValidateObservedBoundary(LocalSourceJournalSession session,LocalSourceContinuityState state,LocalJournalBounds boundary,CancellationToken ct)
    {
        if(!state.Cursor.Valid)throw new LocalSourceGapException("CoherenceBoundaryGap","Native subscription is already invalid; explicit recovery required.");
        if(state.Cursor.JournalIdHex!=boundary.JournalIdHex||state.Cursor.NextReadUsn<Math.Max(boundary.FirstUsn,boundary.LowestValidUsn))
        {await session.MarkGapAsync("JournalGap",ct);throw new LocalSourceGapException("JournalGap","Native observed boundary no longer retains the subscription cursor.");}
    }
    public async Task<LocalCoherenceState> BuildAnchorAsync(LocalSourceJournalSession session,LocalNamespaceAdmission? namespaceAdmission=null,
        LocalCoherenceAdmission? coherenceAdmission=null,CancellationToken cancellationToken=default)
    {
        var ct=cancellationToken;await _serial.WaitAsync(ct);LocalNamespaceCandidate? candidate=null;LocalCoherenceState? coherence=null;
        try
        {
            ObjectDisposedException.ThrowIf(_disposed,this);
            await RequireNativeRootBinding(session,ct);
            // Older evidence remains durable. S is the resulting validated
            // native read boundary, not an arithmetic offset or lastUSN+1.
            await CaptureThrough(session,await _journal.QueryAsync(ct),ct);
            candidate=await session.BeginNamespaceCandidateAsync(_source.RootProof,namespaceAdmission,ct);
            coherence=await session.BeginCoherenceAsync(candidate.CandidateId,coherenceAdmission??new(),ct);
            // Replacement-only lifecycle hook persists REAL allocated IDs
            // before enumeration; normal observer scheduling is unchanged.
            if(_replacementAnchorStarted is not null)await _replacementAnchorStarted(candidate.CandidateId,coherence.CoherenceId,ct);
            while(true)
            {
                ct.ThrowIfCancellationRequested();while(await session.RefillNamespaceFrontierAsync(candidate.CandidateId,ct)is not null)ct.ThrowIfCancellationRequested();
                var parent=await session.ClaimNamespaceDirectoryAsync(candidate.CandidateId,ct);if(parent is null)break;
                await using var reader=await _source.OpenDirectoryAsync(parent,ct);
                await session.RecordNamespaceHandleAsync(parent,reader.Proof,ct);
                await session.RecordCoherenceParentAsync(coherence.CoherenceId,parent,reader.Proof,ct);
                while(true)
                {
                    var page=await reader.ReadPageAsync(ct);
                    for(var i=0;i<page.Entries.Count;i+=session.NamespaceStageCapacity)
                        await session.StageNamespaceEntriesAsync(parent,page.Entries.Skip(i).Take(session.NamespaceStageCapacity).ToArray(),ct);
                    if(page.End)break;
                    await session.CaptureNextAsync(_journal,cancellationToken:ct);
                }
                if(_observer is not null)await _observer(new(LocalCoherenceStage.Parent128Enumerated,candidate.CandidateId,parent.FileId128),ct);
                await using var aliases=await _aliases.OpenAsync(parent,reader.Proof,ct);
                while(true)
                {
                    var page=await aliases.ReadPageAsync(ct);
                    for(var i=0;i<page.Entries.Count;i+=session.NamespaceStageCapacity)
                        await session.StageCoherenceAliasesAsync(coherence.CoherenceId,parent,page.Entries.Skip(i).Take(session.NamespaceStageCapacity).ToArray(),ct);
                    if(page.End)break;
                    await session.CaptureNextAsync(_journal,cancellationToken:ct);
                }
                if(_observer is not null)await _observer(new(LocalCoherenceStage.ParentAliasesEnumerated,candidate.CandidateId,parent.FileId128),ct);
                await aliases.ValidateUnchangedAsync(ct);await reader.ValidateUnchangedAsync(ct);
                await session.CompleteCoherenceParentAsync(coherence.CoherenceId,parent,ct);
                await session.CompleteNamespaceDirectoryAsync(parent,ct);
            }
            await session.FinishNamespaceAsync(candidate.CandidateId,LocalNamespaceStatus.EnumerationComplete,cancellationToken:ct);
            if(_observer is not null)await _observer(new(LocalCoherenceStage.BeforeCut,candidate.CandidateId,_source.RootProof.Handle.FileId128),ct);
            var cut=await _journal.QueryAsync(ct);await CaptureThrough(session,cut,ct);
            await session.SetCoherenceCutAsync(coherence.CoherenceId,cut,ct);
            while(true)
            {
                var state=await session.ReadCoherenceAsync(coherence.CoherenceId,ct);
                if(state.AppliedUsn>=cut.NextUsn&&state.Status!=LocalCoherenceStatus.Building)
                {
                    if(state.Status==LocalCoherenceStatus.Anchored)while(await session.QualifyHistoricalCoherenceOutsideAsync(coherence.CoherenceId,ct)>0)ct.ThrowIfCancellationRequested();
                    return await session.ReadCoherenceAsync(coherence.CoherenceId,ct);
                }
                var applied=await session.ApplyNextCoherenceAsync(coherence.CoherenceId,state.AppliedUsn,ct);if(applied.AppliedUsn<state.AppliedUsn||applied.AppliedUsn==state.AppliedUsn&&state.AppliedUsn<cut.NextUsn)throw new LocalSourceGapException("CoherenceNoProgress","Complete namespace interval cannot advance.");
            }
        }
        catch(Exception e)
        {
            if(coherence is not null)try{await session.MarkCoherenceUncertainAsync(coherence.CoherenceId,e is OperationCanceledException?"Cancelled native scan; fresh candidate required.":e.Message,CancellationToken.None);}catch(InvalidOperationException){/* fenced owner */}
            if(candidate is not null)try{await session.FinishNamespaceAsync(candidate.CandidateId,LocalNamespaceStatus.Incomplete,e is OperationCanceledException?"Cancelled coherent scan.":e.Message,CancellationToken.None);}catch(InvalidOperationException){/* already terminal/fenced */}
            throw;
        }
        finally{_serial.Release();}
    }
    public Task<LocalCoherenceState> ObserveChangesAsync(LocalSourceJournalSession session,string coherenceId,CancellationToken cancellationToken=default)
        =>ObserveChangesCore(session,coherenceId,false,cancellationToken);
    internal Task<LocalCoherenceState> ObserveAndRetireChangesAsync(LocalSourceJournalSession session,string coherenceId,CancellationToken cancellationToken)
        =>ObserveChangesCore(session,coherenceId,true,cancellationToken);
    private async Task<LocalCoherenceState> ObserveChangesCore(LocalSourceJournalSession session,string coherenceId,bool retire,CancellationToken cancellationToken)
    {
        using var phase=Diagnostics.Measure(SyncPhase.Replay);
        var ct=cancellationToken;await _serial.WaitAsync(ct);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed,this);await RequireNativeRootBinding(session,ct);var boundary=await _journal.QueryAsync(ct);
            while(true)
            {
                var state=await session.ReadCoherenceAsync(coherenceId,ct);var source=await session.ReadStateAsync(ct);
                Diagnostics.Backlog(source.InboxRecords,boundary.NextUsn-source.Cursor.NextReadUsn);
                await ValidateObservedBoundary(session,source,boundary,ct);
                if(state.AppliedUsn<source.Cursor.NextReadUsn)
                {var applied=await session.ApplyNextCoherenceAsync(coherenceId,state.AppliedUsn,ct);if(applied.AppliedUsn<=state.AppliedUsn)throw new LocalSourceGapException("CoherenceNoProgress","Local decision prefix cannot advance.");continue;}
                if(retire&&state.Status==LocalCoherenceStatus.Anchored)
                    while((await session.RetireAvailableCoherenceInboxBatchAsync(coherenceId,ct)).Rows>0)ct.ThrowIfCancellationRequested();
                if(source.Cursor.NextReadUsn>=boundary.NextUsn)return state;
                var captured=await session.CaptureNextAsync(_journal,cancellationToken:ct);
                if(captured.NextReadUsn<=source.Cursor.NextReadUsn)throw new LocalSourceGapException("NativeNoProgress","Native journal read did not reach its observed boundary.");
            }
        }
        finally{_serial.Release();}
    }
    public async ValueTask DisposeAsync()
    {await _serial.WaitAsync();try{if(_disposed)return;_disposed=true;await _journal.DisposeAsync();await _aliases.DisposeAsync();await _source.DisposeAsync();}finally{_serial.Release();}}
}
