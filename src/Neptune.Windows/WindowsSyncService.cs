using System.Text.Json;
using System.Net.Http;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Net.Http.Headers;
using System.Reflection;
using Neptune.Core;

namespace Neptune.Windows;

public sealed record WindowsSyncResult(int UploadedFiles, int DeferredFiles, SaturnPalette Palette, IReadOnlyList<SyncMappingMirrorState> Mirrors, bool Guarded = false);
public sealed record WindowsConnectionResult(SaturnPalette Palette, WindowsConnection Connection);

public sealed partial class WindowsSyncService(WindowsProfileContext profile, HttpClient? httpClient = null) : IAsyncDisposable
{
    private const int SourceReadAttempts = 3;
    private static readonly TimeSpan SourceReadRetryDelay = TimeSpan.FromMilliseconds(200);
    internal static readonly TimeSpan LegacyMissingFileGrace = TimeSpan.FromSeconds(30);
    private HttpClient _http = httpClient ?? SyncHttpPolicy.CreateClient();
    private HttpClient _capabilitiesHttp = httpClient ?? SyncHttpPolicy.CreateClient();
    private NetworkRecoveryCoordinator? _network;
    private (string Client, WindowsConnection Connection)? _recoveryConnection;
    private (Uri Origin, string Token)? _recoveryEndpoint;
    private void RememberConnection(string client, WindowsConnection connection)
    {
        if (_recoveryConnection is { } prior && (prior.Connection.KernelOrigin != connection.KernelOrigin || prior.Connection.SaturnToken != connection.SaturnToken))
            _recoveryEndpoint = null;
        _recoveryConnection = (client, connection);
    }
    public string NetworkState => _network?.State ?? "Online";
    private NetworkRecoveryCoordinator Network
    {
        get
        {
            if (_network is null)
            {
                _network = new NetworkRecoveryCoordinator(ProbeNetworkAsync);
                _network.WorkAvailable += () => MappingWorkAvailable?.Invoke("");
            }
            return _network;
        }
    }
    public void NotifyNetworkChanged() { if (_recoveryConnection is not null) Network.Invalidate(); }
    private async Task ProbeNetworkAsync(CancellationToken ct)
    {
        await _syncGate.WaitAsync(ct);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            foreach (var session in _guardedSessions.Values) await session.DisposeAsync();
            _guardedSessions.Clear();
            if (httpClient is null)
            {
                _http.Dispose(); _capabilitiesHttp.Dispose();
                _http = SyncHttpPolicy.CreateClient(); _capabilitiesHttp = SyncHttpPolicy.CreateClient();
            }
            if (_recoveryConnection is not { } connection) throw new NetworkUnavailableException();
            if (connection.Connection.SaturnToken.Trim().Length != 43)
                throw new NetworkUnavailableException("Complete enrollment before automatic network recovery.");
            if (_recoveryEndpoint is { } endpoint) await SendHeartbeatAsync(endpoint.Origin, endpoint.Token, ct);
            else await PrepareAsync(connection.Client, connection.Connection, ct);
        }
        finally { _syncGate.Release(); }
    }
    private readonly SemaphoreSlim _syncGate = new(1, 1);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, WindowsGuardedMappingSession> _guardedSessions = new(StringComparer.Ordinal);
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string,SyncDiagnosticSnapshot> _lastDiagnostics=new(StringComparer.Ordinal);
    private bool _disposed;
    private volatile bool _admissionPaused;
    public async Task PauseAndCheckpointAsync(CancellationToken ct=default)
    {
        _admissionPaused=true;await _syncGate.WaitAsync(ct);
        try{foreach(var session in _guardedSessions.Values)await session.DisposeAsync();_guardedSessions.Clear();await WindowsProfileCommands.CheckpointAsync(profile,ct);}
        finally{_syncGate.Release();}
    }
    public void ResumeAdmission()=>_admissionPaused=false;
    public event Action<string>? MappingWorkAvailable;
    public bool HasGuardedMapping(string mappingId) => _guardedSessions.ContainsKey(mappingId);
    public bool SyncInProgress=>_syncGate.CurrentCount==0;
    public Task<IReadOnlyDictionary<string,SyncDiagnosticSnapshot>> ReadDiagnosticsAsync(CancellationToken ct=default)
    {
        ct.ThrowIfCancellationRequested();var values=_lastDiagnostics.ToDictionary(p=>p.Key,p=>p.Value,StringComparer.Ordinal);foreach(var pair in _guardedSessions)values[pair.Key]=pair.Value.Diagnostics;return Task.FromResult<IReadOnlyDictionary<string,SyncDiagnosticSnapshot>>(values);
    }

    public async Task<WindowsConnectionResult> ConnectAsync(string clientInstanceId, WindowsConnection connection, CancellationToken cancellationToken = default)
    {
        await _syncGate.WaitAsync(cancellationToken);
        try
        {
            RememberConnection(clientInstanceId, connection);
            var prepared = await PrepareAsync(clientInstanceId, connection, cancellationToken);
            var connected = connection with { SaturnToken = prepared.SaturnToken, RemoteFolder = prepared.RemoteFolder };
            _recoveryConnection = (clientInstanceId, connected);
            if (_network is not null && NetworkState != "Online") _network.Invalidate("Recovering operations");
            return new WindowsConnectionResult(prepared.Palette, connected);
        }
        catch (Exception error) when (!cancellationToken.IsCancellationRequested && NetworkFailure.State(error) is { } state)
        { Network.Invalidate(state, (error as GuardedCycleRemoteException)?.RetryAfter); throw; }
        finally { _syncGate.Release(); }
    }

    public async Task<WindowsSyncResult> SyncAsync(
        string clientInstanceId,
        IReadOnlyList<SyncMapping> mappings,
        WindowsConnection connection,
        IProgress<string>? progress = null,
        CancellationToken cancellationToken = default,
        IProgress<WindowsSyncProgress>? detailedProgress = null)
    {
        await _syncGate.WaitAsync(cancellationToken);
        var userCancellation = cancellationToken;
        CancellationTokenSource? routeCancellation = null;
        try
        {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if(_admissionPaused)throw new InvalidOperationException("Profile admission is paused for checkpoint/update.");
        RememberConnection(clientInstanceId, connection);
        routeCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, Network.Admit());
        cancellationToken = routeCancellation.Token;
        if(detailedProgress is not null)progress=new WindowsSyncProgressReporter(progress,detailedProgress);
        WindowsSyncProgressReporter.Send(progress,new(SyncProgressStage.Preparing));
        // Validate saved exclusions before any data recovery. Missing settings adopt
        // the current older Review policy, rather than resetting it.
        var exclusions = new Dictionary<string, MappingSyncPolicy>(StringComparer.Ordinal);
        foreach (var mapping in mappings)
        {
            var paths = FolderExclusions.Read(mapping) ?? await FolderExclusions.ReadRetainedAsync(mapping, cancellationToken);
            exclusions.Add(mapping.MappingId, new MappingSyncPolicy("legacy", paths));
        }
        var prepared = await PrepareAsync(clientInstanceId, connection, cancellationToken);
        // A server upgrade must not bypass an unfinished legacy original by
        // activating a fresh guarded writer. Reconcile it before negotiation.
        var legacyOriginals = Path.Combine(profile.StateDirectory, "legacy-originals");
        if (Directory.Exists(legacyOriginals))
        {
            var legacyState = new NeptuneStateStore(Path.Combine(profile.StateDirectory, "neptune.db"));
            await legacyState.InitializeAsync(cancellationToken);
            progress?.Report("Recovering operations…");
            await new WebDavSyncClient(_http, legacyState) { RetainedOriginalsDirectory = legacyOriginals }
                .RecoverRetainedUploadsAsync(prepared.NamespaceRoot, prepared.SaturnToken, cancellationToken, target =>
                {
                    foreach (var mapping in mappings)
                    {
                        var name = Path.GetFileName(Path.TrimEndingDirectorySeparator(mapping.LocalPath));
                        var root = new Uri(WebDavSyncClient.RelativeTargetUri(prepared.NamespaceRoot, string.IsNullOrWhiteSpace(name) ? "root" : name).AbsoluteUri.TrimEnd('/') + "/");
                        if (root.IsBaseOf(target) && exclusions[mapping.MappingId].Excludes(Uri.UnescapeDataString(root.MakeRelativeUri(target).OriginalString))) return false;
                    }
                    return true;
                });
        }
        var capability = await WindowsGuardedCapabilities.NegotiateAsync(_capabilitiesHttp, prepared.SaturnOrigin,
            prepared.SaturnToken, prepared.DeviceId, prepared.RootId, profile.StateDirectory, cancellationToken);
        if (capability is not null)
            return await SyncGuardedAsync(clientInstanceId, mappings, prepared, capability, progress, cancellationToken);
        {
            var state = new NeptuneStateStore(Path.Combine(profile.StateDirectory, "neptune.db"));
            await state.InitializeAsync(cancellationToken);
            var client = new WebDavSyncClient(_http, state) { RetainedOriginalsDirectory = Path.Combine(profile.StateDirectory, "legacy-originals") };
            var uploaded = 0;
            var deferred = 0;
            var failures = 0;
            var enabledMappings=mappings.Where(item=>item.Enabled).ToArray();var mappingIndex=0;
            foreach (var mapping in enabledMappings)
            {
                var mappingName = Path.GetFileName(mapping.LocalPath.TrimEnd(Path.DirectorySeparatorChar));
                if (string.IsNullOrWhiteSpace(mappingName)) mappingName = "root";
                var mappingProgress=WindowsSyncProgressReporter.ForMapping(progress,mappingName,++mappingIndex,enabledMappings.Length);
                var workProgress=new SyncWorkProgressTracker{Enabled=mappingProgress is IProgress<SyncWorkProgressSnapshot>};workProgress.Begin();
                workProgress.Stage(SyncProgressStage.Scanning);WindowsSyncProgressReporter.Send(mappingProgress,workProgress.Snapshot());
                var mappingKey = WebDavSyncClient.CanonicalPathKey(mappingName);
                if (mappings.Any(other => other.MappingId != mapping.MappingId
                    && WebDavSyncClient.CanonicalPathKey(Path.GetFileName(other.LocalPath.TrimEnd(Path.DirectorySeparatorChar))) == mappingKey))
                    throw new InvalidOperationException($"Two selected directories cannot use the same Saturn folder name '{mappingName}'.");
                var policy = exclusions[mapping.MappingId];
                var snapshot = await ReadLocalSnapshotAsync(mapping.LocalPath, cancellationToken, policy);
                var entries = snapshot.Entries;
                workProgress.Plan(entries.LongCount(e=>!e.IsDirectory));WindowsSyncProgressReporter.Send(mappingProgress,workProgress.Snapshot());
                if (!snapshot.CanReconcileRemote)
                {
                    deferred++;
                    if (snapshot.WasUnstable)
                        progress?.Report($"{mappingName} · local tree changed during scan; deletion reconciliation deferred");
                    foreach (var deniedPath in snapshot.AccessDeniedPaths)
                        progress?.Report($"{mappingName} · access denied: {deniedPath}; scan incomplete, remote deletion and upload cancellation deferred until a complete scan");
                }
                EnsureCanonicalPathsAreUnique(entries);
                await client.EnsureDirectoryAsync(prepared.NamespaceRoot, prepared.SaturnToken, mappingName, cancellationToken);
                var canonicalMapping = WebDavSyncClient.RelativeTargetUri(prepared.NamespaceRoot, mappingName);
                var mappingRoot = new Uri(canonicalMapping.AbsoluteUri.TrimEnd('/') + "/");
                var localFiles = entries.Where(value => !value.IsDirectory).Select(value => value.RelativePath).ToHashSet(StringComparer.Ordinal);
                var localDirectories = entries.Where(value => value.IsDirectory).Select(value => value.RelativePath).ToHashSet(StringComparer.Ordinal);
                var mirroredBytes = 0L;
                var failuresBeforeMapping = failures;
                var deferredBeforeMapping = deferred;
                var sourceWasUnstable = false;
                foreach (var directory in localDirectories.OrderBy(value => value.Count(character => character == '/')))
                    await client.EnsureDirectoryAsync(mappingRoot, prepared.SaturnToken, directory, cancellationToken);
                foreach (var entry in entries.Where(value => !value.IsDirectory))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var file = entry.FullPath;
                    var relative = entry.RelativePath;
                    var fileCompleted=false;workProgress.Start(relative,relative);
                    client.TransferProgress=workProgress.Enabled?new SyncProgressCallback<UploadTransferProgress>(value=>{
                        if(value.Initial)workProgress.TransferStart(relative,value.TotalBytes,value.ReceivedBytes);else workProgress.Acknowledge(relative,value.ReceivedBytes);
                        WindowsSyncProgressReporter.Send(mappingProgress,workProgress.Snapshot());
                    }):null;
                    var previous = await state.GetSyncFileAsync(mapping.MappingId, relative, cancellationToken);
                    for (var attempt = 0; attempt < SourceReadAttempts; attempt++)
                    {
                        long localSize = previous?.LocalSize ?? 0;
                        var modifiedAt = previous?.LocalModifiedAt ?? DateTimeOffset.UnixEpoch;
                        string? hash = null;
                        try
                        {
                            var info = new FileInfo(file);
                            localSize = info.Length;
                            modifiedAt = new DateTimeOffset(info.LastWriteTimeUtc, TimeSpan.Zero);
                            progress?.Report($"{Path.GetFileName(mapping.LocalPath)} · {relative}");
                            await using (var source = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan))
                                hash = Convert.ToHexStringLower(await SHA256.HashDataAsync(source, cancellationToken));
                            var afterHash = new FileInfo(file);
                            if (afterHash.Length != localSize || afterHash.LastWriteTimeUtc != info.LastWriteTimeUtc)
                                throw new SourceChangedDuringReadException();
                            // Legacy has no continuous journal witness. Editors can
                            // preserve size/mtime, so metadata alone cannot prove NOOP.
                            if (previous is not null && previous.Sha256 == hash)
                            {
                                var remoteEtag = await client.ReadEtagAsync(WebDavSyncClient.RelativeTargetUri(mappingRoot, relative), prepared.SaturnToken, cancellationToken);
                                if (remoteEtag == $"\"sha256-{hash}\"")
                                {
                                    await state.UpsertSyncFileAsync(new SyncFileRecord(mapping.MappingId, relative, localSize, modifiedAt, hash, remoteEtag, "synced", DateTimeOffset.UtcNow, null), cancellationToken);
                                    mirroredBytes = checked(mirroredBytes + localSize);
                                    fileCompleted=true;
                                    break;
                                }
                            }
                            var etag = await client.UploadAsync(mappingRoot, prepared.SaturnToken, relative, file, hash, cancellationToken);
                            if (!string.Equals(etag, $"\"sha256-{hash}\"", StringComparison.Ordinal))
                                throw new SourceChangedDuringReadException();
                            var afterUpload = new FileInfo(file);
                            if (afterUpload.Length != localSize || afterUpload.LastWriteTimeUtc != info.LastWriteTimeUtc)
                                throw new SourceChangedDuringReadException();
                            await state.UpsertSyncFileAsync(new SyncFileRecord(mapping.MappingId, relative, localSize, modifiedAt, hash, etag, "synced", DateTimeOffset.UtcNow, null), cancellationToken);
                            mirroredBytes = checked(mirroredBytes + localSize);
                            uploaded++;
                            fileCompleted=true;
                            break;
                        }
                        catch (Exception error) when (IsTransientSourceMutation(error) && attempt + 1 < SourceReadAttempts)
                        {
                            sourceWasUnstable = true;
                            await Task.Delay(SourceReadRetryDelay, cancellationToken);
                        }
                        catch (Exception error) when (IsTransientSourceMutation(error))
                        {
                            sourceWasUnstable = true;
                            deferred++;
                            progress?.Report($"{mappingName} · {relative} changed during scan; retry deferred");
                            if (hash is not null)
                                await state.UpsertSyncFileAsync(new SyncFileRecord(mapping.MappingId, relative, localSize, modifiedAt, hash, previous?.RemoteEtag, "retry-wait", DateTimeOffset.UtcNow, "Source changed while synchronization was reading it."), cancellationToken);
                            break;
                        }
                        catch (Exception error) when (NetworkFailure.State(error) is not null)
                        {
                            // Stop this origin at its first failure. No per-file network storm
                            // and no remote cleanup based on an incomplete observation.
                            throw;
                        }
                        catch (Exception error) when (error is not OperationCanceledException)
                        {
                            failures++;
                            progress?.Report($"{mappingName} · {relative} failed: {error.Message}");
                            if (hash is not null)
                                await state.UpsertSyncFileAsync(new SyncFileRecord(mapping.MappingId, relative, localSize, modifiedAt, hash, previous?.RemoteEtag, "retry-wait", DateTimeOffset.UtcNow, error.Message), cancellationToken);
                            break;
                        }
                    }
                    if(fileCompleted)workProgress.Complete(relative);else workProgress.Release(relative);
                    WindowsSyncProgressReporter.Send(mappingProgress,workProgress.Snapshot());
                }
                workProgress.Stage(SyncProgressStage.Verifying);WindowsSyncProgressReporter.Send(mappingProgress,workProgress.Snapshot());
                // An atomic save can disappear for the entire scan without
                // throwing a read error. Persist the first missing observation,
                // including across restart, before admitting remote deletion.
                var missing = (await state.ListSyncFilesAsync(mapping.MappingId, cancellationToken))
                    .Where(record => record.State != "removed" && !policy.Excludes(record.RelativePath) && !localFiles.Contains(record.RelativePath)).ToArray();
                var observedAt = DateTimeOffset.UtcNow;
                var completeObservation = snapshot.CanReconcileRemote && !sourceWasUnstable
                    && failures == failuresBeforeMapping && deferred == deferredBeforeMapping;
                foreach (var record in missing)
                {
                    if (!completeObservation)
                    {
                        if (record.State == "missing-local")
                            await state.UpsertSyncFileAsync(record with { State = "retry-wait", UpdatedAt = observedAt, Error = "Incomplete source observation; deletion grace must restart." }, cancellationToken);
                    }
                    else if (record.State != "missing-local")
                    {
                        await state.UpsertSyncFileAsync(record with { State = "missing-local", UpdatedAt = observedAt, Error = "Waiting for stable local absence before remote deletion." }, cancellationToken);
                        deferred++;
                    }
                    else if (observedAt - record.UpdatedAt < LegacyMissingFileGrace) deferred++;
                }
                if (missing.Length > 0 && deferred > deferredBeforeMapping)
                    progress?.Report($"{mappingName} · local disappearance pending; remote deletion deferred");
                if (snapshot.CanReconcileRemote && !sourceWasUnstable
                    && failures == failuresBeforeMapping && deferred == deferredBeforeMapping) try
                {
                    // Uploads and remote metadata reads can outlive a short save
                    // gap. Revalidate membership before using the earlier scan.
                    var finalSnapshot = await ReadLocalSnapshotAsync(mapping.LocalPath, cancellationToken, policy);
                    if (!finalSnapshot.CanReconcileRemote
                        || !localFiles.SetEquals(finalSnapshot.Entries.Where(e => !e.IsDirectory).Select(e => e.RelativePath))
                        || !localDirectories.SetEquals(finalSnapshot.Entries.Where(e => e.IsDirectory).Select(e => e.RelativePath)))
                    {
                        deferred++;
                        progress?.Report($"{mappingName} · local tree changed before cleanup; retry deferred");
                        workProgress.Stage(SyncProgressStage.Waiting);WindowsSyncProgressReporter.Send(mappingProgress,workProgress.Snapshot());
                        continue;
                    }
                    await client.CancelMissingUploadsAsync(mappingRoot, prepared.SaturnToken, localFiles, cancellationToken, policy);
                    await client.MirrorAsync(mappingRoot, prepared.SaturnToken, localFiles, localDirectories, protectMassDeletion: true, cancellationToken: cancellationToken, policy: policy);
                    foreach (var record in missing)
                        await state.UpsertSyncFileAsync(record with { State = "removed", UpdatedAt = DateTimeOffset.UtcNow, Error = null }, cancellationToken);
                    if (failures == failuresBeforeMapping && deferred == deferredBeforeMapping)
                        await state.UpsertSyncMappingStatsAsync(new SyncMappingMirrorState(mapping.MappingId, mirroredBytes, localFiles.Count, DateTimeOffset.UtcNow), cancellationToken);
                }
                catch (Exception error) when (NetworkFailure.State(error) is not null) { throw; }
                catch (Exception error) when (error is not OperationCanceledException)
                {
                    failures++;
                    progress?.Report($"{mappingName} · remote cleanup failed: {error.Message}");
                }
                workProgress.Stage(!snapshot.CanReconcileRemote||sourceWasUnstable||failures!=failuresBeforeMapping||deferred!=deferredBeforeMapping
                    ?SyncProgressStage.Waiting:SyncProgressStage.Completed);
                WindowsSyncProgressReporter.Send(mappingProgress,workProgress.Snapshot());
            }
            if (failures > 0) throw new IOException($"{failures} file(s) could not be synchronized and remain queued for retry.");
            Network.Recovered();
            return new WindowsSyncResult(uploaded, deferred, prepared.Palette, await state.ListSyncMappingStatsAsync(clientInstanceId, cancellationToken));
        }
        }
        catch (Exception error) when (!userCancellation.IsCancellationRequested && error is not NetworkUnavailableException && NetworkFailure.State(error) is { } state)
        {
            Network.Invalidate(state, (error as GuardedCycleRemoteException)?.RetryAfter);
            if (error is OperationCanceledException) throw new NetworkUnavailableException(state);
            throw;
        }
        finally { routeCancellation?.Dispose(); _syncGate.Release(); }
    }

    private async Task<WindowsSyncResult> SyncGuardedAsync(string clientId, IReadOnlyList<SyncMapping> mappings,
        PreparedConnection prepared, WindowsGuardedCapability capability, IProgress<string>? progress, CancellationToken ct)
    {
        var enabled = mappings.Where(m => m.Enabled).ToArray();
        if (enabled.Length > 16) throw new InvalidDataException("Guarded synchronization supports at most 16 active mappings per profile.");
        // Include disabled/retained mapping spools in the common quota before
        // any mapping admits a new capture. The application holds profile ownership.
        var guardedRoot=Path.Combine(profile.StateDirectory,"guarded-sync");var spoolDirectories=0;
        if(Directory.Exists(guardedRoot))
        {
            WindowsGuardedCapabilities.RequirePlainAncestors(guardedRoot);
            foreach(var ns in Directory.EnumerateDirectories(guardedRoot))
            {
                WindowsGuardedCapabilities.RequirePlainAncestors(ns);
                foreach(var directory in Directory.EnumerateDirectories(ns))
                {if(++spoolDirectories>256)throw new InvalidDataException("Retained mapping spool inventory exceeds its bound.");WindowsGuardedCapabilities.RequirePlainAncestors(directory);WindowsCycleSharedBudget.ObserveSpoolDirectory(Path.Combine(directory,"spool"));}
            }
        }
        foreach (var group in enabled.GroupBy(m => WebDavSyncClient.CanonicalPathKey(Path.GetFileName(m.LocalPath.TrimEnd(Path.DirectorySeparatorChar)) is { Length: > 0 } name ? name : "root")))
            if (group.Count() > 1) throw new InvalidDataException("Selected directories have colliding Saturn folder names.");
        foreach (var old in _guardedSessions.Keys.Where(id => !enabled.Any(m => m.MappingId == id)).ToArray())
        { await _guardedSessions[old].DisposeAsync(); _guardedSessions.TryRemove(old,out _); }
        var state = new NeptuneStateStore(Path.Combine(profile.StateDirectory, "neptune.db"));
        await state.InitializeAsync(ct); long uploaded = 0, deferred = 0;var mappingIndex=0;
        foreach (var mapping in enabled)
        {
            var name=Path.GetFileName(mapping.LocalPath.TrimEnd(Path.DirectorySeparatorChar));
            var mappingProgress=WindowsSyncProgressReporter.ForMapping(progress,string.IsNullOrWhiteSpace(name)?"root":name,++mappingIndex,enabled.Length);
            WindowsSyncProgressReporter.Send(mappingProgress,new(SyncProgressStage.Preparing));
            var fingerprint = WindowsGuardedMappingSession.Fingerprint(capability, prepared.SaturnOrigin, clientId, profile, mapping);
            if (_guardedSessions.TryGetValue(mapping.MappingId, out var existing) && existing.ConfigurationFingerprint != fingerprint)
            { await existing.DisposeAsync(); _guardedSessions.TryRemove(mapping.MappingId,out _); existing = null; }
            if (existing is null)
            {
                existing = await WindowsGuardedMappingSession.OpenAsync(profile, clientId, mapping, prepared.SaturnOrigin,
                    capability, prepared.SaturnToken, prepared.RemoteFolder, mappingProgress, ct);
                if(!_guardedSessions.TryAdd(mapping.MappingId,existing))throw new InvalidOperationException("Mapping session ownership changed.");
                existing.WorkAvailable += () => MappingWorkAvailable?.Invoke(mapping.MappingId);
            }
            existing.UpdateCurrentToken(prepared.SaturnToken);
            try
            {
                await ApplyFolderExclusionsAsync(mapping, existing, ct);
                var cycle = await existing.SyncAsync(mapping.MappingId, ct, mappingProgress);
                if (cycle.Result.RetryState is "WAIT_CONNECTIVITY" or "AUTH_REQUIRED")
                {
                    var next = existing.Diagnostics.Schedule?.NextAttemptUnixMilliseconds;
                    var delay = next is { } due && due != long.MaxValue ? TimeSpan.FromMilliseconds(Math.Max(0, due - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())) : (TimeSpan?)null;
                    Network.Invalidate(cycle.Result.RetryState == "AUTH_REQUIRED" ? "Server/proxy authorization required" : "Waiting for network", delay);
                    throw new NetworkUnavailableException(NetworkState);
                }
                var wait=cycle.Result.RetryState switch{"AUTH_REQUIRED"=>"Access needs attention; resume after restoring access","SOURCE_BUSY"=>"Waiting for source","WAIT_CONNECTIVITY"=>"Waiting for connection","DEGRADED"=>"Retrying less often","RETRY_WAIT"=>"Retry scheduled",_=>null};
                progress?.Report($"{Path.GetFileName(mapping.LocalPath)} · "+(wait??(cycle.Result.Conflicts>0?$"Needs attention · {cycle.Result.Conflicts} conflicts":cycle.Result.Deferred>0?$"Waiting for source · {cycle.Result.Deferred} deferred":cycle.Result.PendingEntries>0?$"Pending · {cycle.Result.PendingEntries} objects":"No work at the applied event boundary")));
                uploaded = checked(uploaded + cycle.Result.PublishedFiles);
                deferred = checked(deferred + cycle.Result.PendingEntries);
                if (cycle.Stats is not null) await state.UpsertSyncMappingStatsAsync(cycle.Stats, ct);
            }
            catch
            {
                // A failed initial coherence attempt must be reopened from its
                // retained original work; it must not become a fresh clean cycle.
                _lastDiagnostics[mapping.MappingId]=existing.Diagnostics;
                try{await WindowsProfileCommands.WriteJsonAsync(Path.Combine(profile.StateDirectory,"diagnostics",Convert.ToHexStringLower(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(mapping.MappingId)))+".json"),existing.Diagnostics,CancellationToken.None);}catch(Exception diagnostic)when(diagnostic is IOException or UnauthorizedAccessException){}
                await existing.DisposeAsync(); _guardedSessions.TryRemove(mapping.MappingId,out _); throw;
            }
        }
        Network.Recovered();
        return new(checked((int)uploaded), checked((int)deferred), prepared.Palette,
            await state.ListSyncMappingStatsAsync(clientId, ct), Guarded: true);
    }

    private async Task<PreparedConnection> PrepareAsync(string clientInstanceId, WindowsConnection connection, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(SyncHttpPolicy.ControlTimeout);
        cancellationToken = deadline.Token;
        var register = new KernelRegisterClient(_http, Path.Combine(profile.StateDirectory, "register-lkg.json"));
        string[] keys = ["services.saturn.sni", "services.saturn.port", "services.saturn.paths.sync", "services.saturn.paths.sync_preferences"];
        var snapshot = connection.KernelCredentialKind == WindowsKernelCredentialKind.NeptuneOutdoorToken
            ? await register.ResolveOutdoorAsync(connection.KernelOrigin, connection.KernelToken, keys, cancellationToken, allowCachedMetadata: connection.SaturnToken.Trim().Length == 43)
            : await register.GetSnapshotAsync(connection.KernelOrigin, connection.KernelToken, cancellationToken, keys, allowCachedMetadata: connection.SaturnToken.Trim().Length == 43);
        using (snapshot)
        {
            var values = snapshot.RootElement.GetProperty("values");
            var saturnOrigin = RegisterValues.HttpsOrigin(values, "saturn");
            var syncPath = RegisterValues.RequiredString(values, "services.saturn.paths.sync");
            string preferencesPath;
            try { preferencesPath = RegisterValues.RequiredString(values, "services.saturn.paths.sync_preferences"); }
            catch (InvalidDataException) { preferencesPath = "/api/v1/sync/preferences"; }
            var client = new WebDavSyncClient(_http);
            var syncRoot = new Uri(saturnOrigin, syncPath.TrimEnd('/') + "/");
            var saturnToken = connection.SaturnToken.Trim();
            if (saturnToken.Length == 32)
                saturnToken = await RedeemSetupCodeAsync(saturnOrigin, saturnToken, cancellationToken);
            if (saturnToken.Length != 43)
                throw new InvalidDataException("Saturn setup code or stored device credential is invalid.");
            _recoveryConnection = (clientInstanceId, connection with { SaturnToken = saturnToken });
            _recoveryEndpoint = (saturnOrigin, saturnToken);
            var assignment = await SendHeartbeatAsync(saturnOrigin, saturnToken, cancellationToken);
            var remoteFolder = assignment.SyncFolderName!;
            var namespaceRoot = new Uri(syncRoot, Uri.EscapeDataString(remoteFolder) + "/");
            var palette = SaturnPalette.Default;
            try { palette = await client.ReadPaletteAsync(new Uri(saturnOrigin, preferencesPath), saturnToken, cancellationToken); }
            catch (HttpRequestException error) when (error.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                // Older Saturn deployments have no preferences endpoint. The next
                // successful connection after Saturn is upgraded replaces this fallback.
            }
            return new PreparedConnection(namespaceRoot, palette, saturnToken, remoteFolder, saturnOrigin, assignment.Id!, assignment.SyncRootId!);
        }
    }

    private async Task<string> RedeemSetupCodeAsync(Uri saturnOrigin, string code, CancellationToken cancellationToken)
    {
        var version = typeof(WindowsSyncService).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0] ?? "unknown";
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(saturnOrigin, "/api/v1/device-enrollments/redeem")) { Content = JsonContent.Create(new { code, version }) };
        using var response = await SyncHttpPolicy.SendAsync(_http, request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        var redeemed = await response.Content.ReadFromJsonAsync<DeviceEnrollmentResponse>(cancellationToken: cancellationToken)
            ?? throw new InvalidDataException("Saturn returned an empty device enrollment response.");
        if (redeemed.Token is not { Length: 43 }) throw new InvalidDataException("Saturn returned an invalid device credential.");
        return redeemed.Token;
    }

    private async Task<DeviceAssignment> SendHeartbeatAsync(Uri saturnOrigin, string token, CancellationToken cancellationToken)
    {
        var version = typeof(WindowsSyncService).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion.Split('+')[0] ?? "unknown";
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(saturnOrigin, "/api/v1/device-session/heartbeat"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        request.Content = JsonContent.Create(new { platform = "windows", version });
        using var response = await SyncHttpPolicy.SendAsync(_http, request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        await NetworkFailure.EnsureSuccessAsync(response, cancellationToken);
        await using var body = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var limited = new MemoryStream(); var buffer = new byte[4096];
        while (true) { var count = await body.ReadAsync(buffer, cancellationToken); if (count == 0) break; if (limited.Length + count > 16384) throw new InvalidDataException("Heartbeat response exceeds limit."); limited.Write(buffer, 0, count); }
        var device = JsonSerializer.Deserialize<DeviceAssignment>(limited.ToArray(), new JsonSerializerOptions(JsonSerializerDefaults.Web))
            ?? throw new InvalidDataException("Saturn returned no folder assignment.");
        var folder = device.SyncFolderName;
        if (device.Id is null || !Guid.TryParse(device.Id, out _) || device.SyncRootId is null || !Guid.TryParse(device.SyncRootId, out _) || string.IsNullOrWhiteSpace(folder)
            || folder.Length > 80 || folder is "." or ".." || folder.Any(value => char.IsControl(value) || value is '/' or '\\'))
            throw new InvalidDataException("This Windows connection has no isolated Saturn folder. Create a new setup code in Saturn.");
        return device;
    }

    private static async Task<WindowsLocalSnapshot> ReadLocalSnapshotAsync(string root, CancellationToken cancellationToken, MappingSyncPolicy? policy = null)
    {
        var wasUnstable = false;
        for (var attempt = 0; ; attempt++)
        {
            try { return WindowsLocalSnapshot.Read(root, cancellationToken, policy) with { WasUnstable = wasUnstable }; }
            catch (Exception error) when (IsTransientSourceMutation(error) && attempt + 1 < SourceReadAttempts)
            {
                wasUnstable = true;
                await Task.Delay(SourceReadRetryDelay, cancellationToken);
            }
        }
    }

    private static bool IsTransientSourceMutation(Exception error)
    {
        if (error is SourceChangedDuringReadException or LocalSourceChangedException or FileNotFoundException or DirectoryNotFoundException) return true;
        return error is IOException && ((uint)error.HResult & 0xffff) is 32 or 33;
    }

    private static void EnsureCanonicalPathsAreUnique(IEnumerable<WindowsLocalEntry> entries)
    {
        var paths = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var entry in entries)
        {
            var key = WebDavSyncClient.CanonicalPathKey(entry.RelativePath);
            if (paths.TryGetValue(key, out var existing) && !string.Equals(existing, entry.RelativePath, StringComparison.Ordinal))
                throw new InvalidDataException($"Local paths '{existing}' and '{entry.RelativePath}' resolve to the same Saturn path.");
            paths[key] = entry.RelativePath;
        }
    }

    private sealed record PreparedConnection(Uri NamespaceRoot, SaturnPalette Palette, string SaturnToken, string RemoteFolder, Uri SaturnOrigin, string DeviceId, string RootId);
    private sealed record DeviceAssignment(string? Id, string? SyncRootId, string? SyncFolderName);
    private sealed record DeviceEnrollmentResponse(string Token);
    private sealed class SourceChangedDuringReadException : IOException { }
    public async ValueTask DisposeAsync()
    {
        if (_network is not null) await _network.DisposeAsync();
        await _syncGate.WaitAsync();
        try
        {
            if (_disposed) return; _disposed = true;
            foreach (var session in _guardedSessions.Values) await session.DisposeAsync();
            _guardedSessions.Clear();
            if (httpClient is null) { _http.Dispose(); _capabilitiesHttp.Dispose(); }
        }
        finally { _syncGate.Release(); }
    }
}
