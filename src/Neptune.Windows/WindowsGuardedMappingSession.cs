using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Windows;

internal sealed class WindowsGuardedMappingSession : IAsyncDisposable
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly WindowsFreshSyncEngine _engine;
    private readonly SaturnGuardedCycleTransport _transport;
    private readonly string _configurationFingerprint;
    private readonly WindowsGuardedStateLease _stateLease;
    private readonly string _profileStateDirectory;
    private string _token;
    private bool _initial = true;
    private IProgress<string>? _currentProgress;
    private WindowsGuardedMappingSession(WindowsFreshSyncEngine engine, SaturnGuardedCycleTransport transport,
        string fingerprint, string token, WindowsGuardedStateLease stateLease,string profileStateDirectory)
    {
        (_engine, _transport, _configurationFingerprint, _token, _stateLease) = (engine, transport, fingerprint, token, stateLease);
        _profileStateDirectory=profileStateDirectory;
        _engine.WorkAvailable+=()=>WorkAvailable?.Invoke();
        _initial = !engine.HasRetainedAnchor;
    }
    internal string ConfigurationFingerprint => _configurationFingerprint;
    internal event Action? WorkAvailable;
    private readonly CancellationTokenSource _eventsShutdown=new();
    private Task? _events;
    internal SyncDiagnosticSnapshot Diagnostics => _engine.Diagnostics;
    internal Task<MappingSyncPolicy> ReadPolicyAsync(CancellationToken ct)=>_engine.ReadMappingPolicyAsync(ct);
    internal Task<MappingPolicyPreview> PreviewPolicyAsync(MappingSyncPolicy policy,CancellationToken ct)=>_engine.PreviewPolicyAsync(policy,ct);
    internal Task ApplyPolicyAsync(MappingSyncPolicy policy,string digest,CancellationToken ct)=>_engine.ApplyPolicyAsync(policy,digest,ct);
    internal Task<NamespaceDeletePreview> PreviewDeletesAsync(CancellationToken ct)=>_engine.PreviewDeletesAsync(ct);
    internal Task ApproveDeletesAsync(string digest,CancellationToken ct)=>_engine.ApproveDeletesAsync(digest,ct);
    internal Task<IReadOnlyList<LocalParentReconciliationConflict>> ReadConflictsAsync(CancellationToken ct)=>_engine.ReadParentConflictsAsync(cancellationToken:ct);
    internal void UpdateCurrentToken(string token) => _token = token.Length == 43 ? token
        : throw new InvalidDataException("Invalid current Windows device credential.");

    internal static string Fingerprint(WindowsGuardedCapability capability, Uri origin, string clientId,
        WindowsProfileContext profile, SyncMapping mapping)
        => Hash(JsonSerializer.Serialize(new { capability.Binding, capability.DeviceId, capability.BackendIdentity, capability.Budget, capability.MaximumFileBytes, capability.Namespace,capability.NamespaceRevision,features=capability.ExtensionFeatures.OrderBy(x=>x,StringComparer.Ordinal),
            origin = origin.GetLeftPart(UriPartial.Authority), clientId, profile.ProfileId, mapping.MappingId,
            source = Path.TrimEndingDirectorySeparator(Path.GetFullPath(mapping.LocalPath)) }, Json));

    internal static async Task<WindowsGuardedMappingSession> OpenAsync(WindowsProfileContext profile,
        string clientId, SyncMapping mapping, Uri origin, WindowsGuardedCapability capability, string token, string assignedRootName,
        IProgress<string>? progress, CancellationToken ct)
    {
        var fingerprint = Fingerprint(capability, origin, clientId, profile, mapping);
        var namespaceKey = WindowsGuardedCapabilities.NamespaceKey(origin, capability.DeviceId, capability.Binding.RootId);
        // Mapping settings/credentials remain in the original profile database.
        // Each guarded mapping has separately versioned evidence, WAL and spool.
        var directory = Path.Combine(profile.StateDirectory, "guarded-sync", namespaceKey, Hash(mapping.MappingId));
        var database = Path.Combine(directory, "cycle.db");
        var config = new WindowsCycleConfiguration(mapping.MappingId, clientId, profile.ProfileId,
            "windows-guarded-append-only-v1", origin, capability.DeviceId, "windows-device:" + capability.DeviceId,
            capability.BackendIdentity, capability.Binding, AllowVerifiedEqualAdoption: true);
        await using var source = await WindowsGuardedSourceEligibility.OpenAsync(mapping.LocalPath, database, config, ct);
        await source.RequireCurrentAsync(ct);
        var stateLease = WindowsGuardedStateLease.Open(directory, create: true);
        try
        {
        var state = new NeptuneStateStore(database); await state.InitializeAsync(ct);
        stateLease.RequireCurrent();
        var existing = (await state.ListMappingsAsync(ct)).SingleOrDefault();
        if (existing is null) await state.AddMappingAsync(mapping, ct);
        else if (existing.MappingId != mapping.MappingId || existing.ClientInstanceId != mapping.ClientInstanceId
            || !string.Equals(Path.GetFullPath(existing.LocalPath), Path.GetFullPath(mapping.LocalPath), StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("Guarded mapping evidence cannot be rebound to a different source.");
        string currentToken = token;
        using var anchorTransport = new SaturnGuardedCycleTransport(config, _ => Task.FromResult("Bearer " + currentToken));
        var anchorIdentity = Hash(JsonSerializer.Serialize(new { namespaceKey, clientId, profile.ProfileId,
            mapping.MappingId, source = Path.TrimEndingDirectorySeparator(Path.GetFullPath(mapping.LocalPath)) }, Json));
        await source.RequireCurrentAsync(ct);
        var anchor = await ResolveAnchor(directory, anchorIdentity, mapping, capability, assignedRootName, anchorTransport, ct);
        stateLease.RequireCurrent();
        config = config with { RemoteAnchorId = anchor };
        WindowsGuardedMappingSession? session = null;
        var transport = new SaturnGuardedCycleTransport(config, _ => Task.FromResult("Bearer " + (session?._token ?? currentToken)));
        transport.ConfigureExtensions(capability.ExtensionFeatures,capability.Namespace,capability.NamespaceRevision);
        WindowsFreshSyncEngine? engine = null;
        try
        {
            await source.RequireCurrentAsync(ct);
            engine = await WindowsFreshSyncEngine.ResumeOrOpenAsync(new WindowsCycleNativeOptions(LocalJournalReadMode.IdentityOnly, true), state, mapping.LocalPath, Path.Combine(directory, "spool"),
                config, new WindowsMappingAnchorRemote(transport, anchor, capability.Binding.RootId, Path.GetFileName(mapping.LocalPath.TrimEnd(Path.DirectorySeparatorChar)) is { Length: > 0 } name ? name : "root"), capability.Budget, observer: (observation, _) =>
                {
                    if (observation.Stage is WindowsCycleStage.FilePublished or WindowsCycleStage.DirectoryPublished)
                        (session is null?progress:session._currentProgress)?.Report($"{Path.GetFileName(mapping.LocalPath)} · {observation.RelativePath}");
                    return Task.CompletedTask;
                }, subscriptionGeneration: source.SubscriptionGeneration, cancellationToken: ct);
            session = new(engine, transport, fingerprint, token, stateLease,profile.StateDirectory);
            await RequireNoOtherActiveScopeAsync(database, engine.ScopeId, ct, engine);
            stateLease.RequireCurrent();
            return session;
        }
        catch { if (engine is not null) await engine.DisposeAsync(); transport.Dispose(); throw; }
        }
        catch { stateLease.Dispose(); throw; }
    }

    internal async Task<(WindowsCycleResult Result, SyncMappingMirrorState? Stats)> SyncAsync(string mappingId, CancellationToken ct, IProgress<string>? progress=null)
    {
        _currentProgress=progress;
        _engine.ProgressEnabled=progress is IProgress<SyncWorkProgressSnapshot>;
        var previousRevision=_engine.WorkProgress.Revision;
        using var pollingStop=CancellationTokenSource.CreateLinkedTokenSource(ct);
        var polling=_engine.ProgressEnabled?ReportProgressAsync(progress,previousRevision,pollingStop.Token):Task.CompletedTask;
        try
        {
        _stateLease.RequireCurrent();
        await _engine.ResumeAuthorizedOperationsAsync(ct);
        var preferences=new WindowsPreferencesStore(_profileStateDirectory).Read();
        await _engine.ConfigureSmallFileBatchesAsync(preferences.SmallBatchMappings.Contains(mappingId,StringComparer.Ordinal),ct);
        WindowsCycleResult result;
        if (_initial) { result = await _engine.InitialSyncAsync(ct); _initial = !_engine.HasRetainedAnchor; }
        else { await _engine.RetryDeferredAsync(ct); result = await _engine.FreshSyncAsync(ct); }
        _events??=Task.Run(RemoteEventsAsync);
        if (result.Error is not null&&(!_engine.HasRetainedAnchor||result.RetryState is null))
            throw new IOException("Guarded synchronization retained queued work: " + result.Error);
        var stats = result.PendingEntries == 0 && result.Decision == "NoWorkAtAppliedEventBoundary"
            ? await ReadHistoricalStats(mappingId, ct) : null;
        return (result, stats);
        }
        finally
        {
            pollingStop.Cancel();await polling;
            if(_engine.WorkProgress.Revision!=previousRevision)WindowsSyncProgressReporter.Send(progress,_engine.WorkProgress);
            _currentProgress=null;_engine.ProgressEnabled=false;
        }
    }

    private async Task ReportProgressAsync(IProgress<string>? progress,long previousRevision,CancellationToken ct)
    {
        try
        {
            using var timer=new PeriodicTimer(TimeSpan.FromMilliseconds(500));
            while(await timer.WaitForNextTickAsync(ct))
            {
                var snapshot=_engine.WorkProgress;
                if(snapshot.Revision!=previousRevision)WindowsSyncProgressReporter.Send(progress,snapshot);
            }
        }
        catch(OperationCanceledException)when(ct.IsCancellationRequested){}
    }

    private async Task RemoteEventsAsync()
    {
        while(!_eventsShutdown.IsCancellationRequested)
        {
            try{if(await _engine.WaitForRemoteHintAsync(_eventsShutdown.Token))WorkAvailable?.Invoke();await Task.Delay(TimeSpan.FromSeconds(30),_eventsShutdown.Token);}
            catch(OperationCanceledException)when(_eventsShutdown.IsCancellationRequested){return;}
            catch{try{await Task.Delay(TimeSpan.FromSeconds(30),_eventsShutdown.Token);}catch(OperationCanceledException){return;}}
        }
    }
    internal static async Task RequireNoOtherActiveScopeAsync(string database, string currentScope, CancellationToken ct,
        WindowsFreshSyncEngine? owner = null)
    {
        if (owner is not null)
        {
            if (owner.ScopeId != currentScope) throw new InvalidDataException("Retained history requires its current owned scope.");
            await owner.RequireRetainedHistoryAsync(ct);
            return;
        }
        await using var db = new SqliteConnection(new SqliteConnectionStringBuilder { DataSource = NeptuneStateStore.NativeDatabasePath(database),
            Mode = SqliteOpenMode.ReadOnly, Cache = SqliteCacheMode.Private }.ToString());
        await db.OpenAsync(ct);
        using var transaction = db.BeginTransaction(deferred: true);
        var previousScopes = new List<string>(16);
        await using (var scopes = db.CreateCommand())
        {
            scopes.Transaction = transaction;
            scopes.CommandText = "SELECT scope_id FROM local_cycle_scopes WHERE scope_id<>$scope ORDER BY scope_id LIMIT 17";
            scopes.Parameters.AddWithValue("$scope", currentScope);
            await using var reader = await scopes.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct)) previousScopes.Add(reader.GetString(0));
        }
        if (previousScopes.Count > 16) throw new InvalidDataException("Guarded historical scope count exceeds its recognized bound.");
        // The retained active scope recovers its own original attempts before
        // the fresh feed. Older active work requires explicit recovery; never
        // adopt it into a new scope or skip an UNKNOWN/deferred phase.
        foreach (var previous in previousScopes)
        {
            await using var query = db.CreateCommand();
            query.Transaction = transaction;
            query.CommandText = "SELECT 1 FROM local_cycle_work INDEXED BY local_cycle_work_page WHERE scope_id=$scope AND active=1 LIMIT 1";
            query.Parameters.AddWithValue("$scope", previous);
            if (await query.ExecuteScalarAsync(ct) is not null)
                throw new InvalidDataException("Another guarded scope retains active work; its original evidence requires explicit recovery.");
        }
    }

    private async Task<SyncMappingMirrorState> ReadHistoricalStats(string mappingId, CancellationToken ct)
    {
        var totals = await _engine.ReadConfirmedTotalsAsync(ct);
        return new(mappingId, totals.Bytes, checked((int)totals.Files), DateTimeOffset.UtcNow);
    }

    private sealed record AnchorIntent(string Schema, string Fingerprint, CycleFolderInput Input);
    private static async Task<string> ResolveAnchor(string directory, string fingerprint, SyncMapping mapping,
        WindowsGuardedCapability capability, string assignedRootName, IWindowsCycleRemote remote, CancellationToken ct)
    {
        var adopted=await new NeptuneStateStore(Path.Combine(directory,"cycle.db")).ReadAdoptedAnchorAsync(capability.Binding,capability.Namespace,capability.QualificationReceiptSha256,ct);
        if(adopted is not null)return adopted; // Fresh snapshot/feed still verifies current anchor topology.
        var mappingName = Path.GetFileName(mapping.LocalPath.TrimEnd(Path.DirectorySeparatorChar));
        if (string.IsNullOrWhiteSpace(mappingName)) mappingName = "root";
        if (assignedRootName.Length is < 1 or > 255 || assignedRootName.IndexOfAny(['\0', '/', '\\']) >= 0)
            throw new InvalidDataException("Assigned root name is invalid.");
        var intentPath = Path.Combine(directory, "anchor-intent.json"); AnchorIntent original;
        if (File.Exists(intentPath))
        {
            WindowsGuardedCapabilities.RequirePlainAncestors(intentPath);
            if (new FileInfo(intentPath).Length > 40 * 1024) throw new InvalidDataException("Original mapping intent exceeds its fixed limit.");
            using var json = SyncProtocolReader.Parse(await File.ReadAllBytesAsync(intentPath, ct));
            if (json.RootElement.ValueKind != JsonValueKind.Object || json.RootElement.EnumerateObject().Count() != 3
                || json.RootElement.EnumerateObject().Any(v => v.Name is not ("schema" or "fingerprint" or "input")))
                throw new InvalidDataException("Original mapping intent has unknown fields.");
            original = json.RootElement.Deserialize<AnchorIntent>(Json) ?? throw new InvalidDataException("Original mapping intent is missing.");
            if (original.Schema != "neptune.mapping-anchor-intent.v1" || original.Fingerprint != fingerprint
                || original.Input.Name != mappingName || original.Input.RootId != capability.Binding.RootId)
                throw new InvalidDataException("Original mapping intent cannot be silently replaced after rebind.");
        }
        else
        {
            var id = Guid.NewGuid().ToString("D");
            original = new("neptune.mapping-anchor-intent.v1", fingerprint,
                new(id, id, capability.Binding.RootId, capability.Binding.ServerEpoch, capability.Binding.BindingVersion,
                    capability.Binding.RootId, mappingName, "ensure", [new(capability.Binding.RootId, null, assignedRootName)]));
            await PersistNew(intentPath, JsonSerializer.SerializeToUtf8Bytes(original, Json), ct);
        }
        var outcome = await remote.EnsureDirectoryAsync(original.Input, ct);
        // Only an identity is taken from this historical result. The engine's
        // first snapshot/feed plus the mapping anchor wrapper qualify current
        // presence/type/name/parent before source enumeration or byte capture.
        return outcome.Description.NodeId;
    }

    private static async Task PersistNew(string file, byte[] bytes, CancellationToken ct)
    {
        var temp = file + "." + Guid.NewGuid().ToString("N") + ".partial";
        try
        {
            await using (var stream = new FileStream(temp, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            { await stream.WriteAsync(bytes, ct); stream.Flush(true); }
            File.Move(temp, file);
        }
        finally { if (File.Exists(temp)) File.Delete(temp); }
    }
    private static string Hash(string value) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
    public async ValueTask DisposeAsync() { _eventsShutdown.Cancel();if(_events is not null)await _events;try { await _engine.DisposeAsync(); } finally { _transport.Dispose(); _stateLease.Dispose();_eventsShutdown.Dispose(); } }
}
