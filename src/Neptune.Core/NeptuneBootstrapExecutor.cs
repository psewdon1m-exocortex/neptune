using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace Neptune.Core;

// Explicit opt-in Core executor. WindowsSyncService does not call this engine.
// Partial enumeration never invokes Mirror/CancelMissing or writes trusted B.
public sealed class NeptuneBootstrapExecutor(NeptuneStateStore state, ICreateOnlyBootstrapTransport transport,
    IBootstrapScanSource? scanSource = null, IBootstrapSourceCapture? captureSource = null)
{
    private readonly IBootstrapSourceCapture _capture = captureSource ?? new WindowsBootstrapSourceCapture();

    // Scanner/consumer/capture must not borrow a caller's UI/test scheduling
    // context. In particular a stalled GUI must not stall data publication.
    // The callback runs on the background executor; GUI callers dispatch it.
    public Task<BootstrapExecutionResult> ScanAndTransferAsync(BootstrapExecutionRequest request,
        Func<BootstrapPublicationReceipt, CancellationToken, Task>? publicationConfirmed = null, CancellationToken cancellationToken = default)
        => Task.Run(() => ScanAndTransferCoreAsync(request, publicationConfirmed, cancellationToken), CancellationToken.None);

    private async Task<BootstrapExecutionResult> ScanAndTransferCoreAsync(BootstrapExecutionRequest request,
        Func<BootstrapPublicationReceipt, CancellationToken, Task>? publicationConfirmed, CancellationToken cancellationToken)
    {
        await using var ownership = await state.AcquireBootstrapExecutionOwnerAsync(request.MappingId, cancellationToken);
        using var context = Prepare(request, cancellationToken);
        await EnsureCaptureSchemaAsync(cancellationToken);
        using var shutdown = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var started = new TaskCompletionSource<(string Session, BootstrapScanState Scan)>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var signal = new SemaphoreSlim(0, 1);
        void Wake() { try { signal.Release(); } catch (SemaphoreFullException) { } }
        string? scanError = null;
        var scanFinished = 0;
        BootstrapScanState? finalScan = null;
        var scanning = Task.Run(async () =>
        {
            try
            {
                finalScan = await new NeptuneBootstrapScanner(state, scanSource).ScanAsync(request.MappingId, request.ClientInstanceId, request.SourceRoot,
                    async (directory, ct) =>
                    {
                        if (!started.Task.IsCompleted)
                        {
                            var scan = (await state.GetBootstrapScanAsync(directory.ScanId, ct))!;
                            var session = await state.BeginBootstrapWorkSessionAsync(scan, request.ClientInstanceId, transport.TargetRoot, transport.CredentialScope, transport.ReceiptExpectation, ct);
                            started.TrySetResult((session, scan));
                        }
                        Wake(); // notification only, never awaits all data
                    }, shutdown.Token);
            }
            catch (Exception error)
            {
                scanError = error.Message;
                if (!started.Task.IsCompleted) started.TrySetException(error);
                shutdown.Cancel();
            }
            finally { Interlocked.Exchange(ref scanFinished, 1); Wake(); }
        }, CancellationToken.None);
        (string Session, BootstrapScanState Scan) session;
        try { session = await started.Task.WaitAsync(cancellationToken); }
        catch { shutdown.Cancel(); await scanning; return EmptyResult(scanError, cancellationToken.IsCancellationRequested); }
        var channel = NewChannel(context.Limits);
        var workers = Enumerable.Range(0, context.Limits.DataJobs).Select(_ => WorkerAsync(channel.Reader, context, publicationConfirmed, shutdown.Token)).ToArray();
        var producer = Task.Run(async () =>
        {
            long cursor = 0;
            try
            {
                while (true)
                {
                    shutdown.Token.ThrowIfCancellationRequested();
                    var page = await state.ListBootstrapEligibleEntriesAsync(session.Scan.ScanId, cursor, context.Limits.ReadyQueueCapacity, shutdown.Token);
                    if (page.Count > 0)
                    {
                        await state.EnqueueBootstrapWorkAsync(session.Session, page, shutdown.Token);
                        var works = await state.ListBootstrapWorkAsync(session.Session, context.LastWorkId, context.Limits.ReadyQueueCapacity, shutdown.Token);
                        foreach (var work in works)
                        {
                            await channel.Writer.WriteAsync(work, shutdown.Token);
                            context.LastWorkId = work.WorkId;
                            context.ObserveReady(channel.Reader.Count);
                        }
                        cursor = page[^1].EntryId;
                        continue;
                    }
                    if (Volatile.Read(ref scanFinished) != 0) break;
                    await signal.WaitAsync(shutdown.Token);
                }
            }
            catch (Exception error) { scanError ??= error.Message; shutdown.Cancel(); }
            finally { channel.Writer.TryComplete(); }
        }, CancellationToken.None);
        await Task.WhenAll(producer, scanning, Task.WhenAll(workers));
        finalScan ??= await state.GetBootstrapScanAsync(session.Scan.ScanId, CancellationToken.None);
        return await ResultAsync(session.Session, finalScan, context, cancellationToken.IsCancellationRequested, scanError);
    }

    // Replay only sealed unresolved intents with the same operation ID/bytes.
    // No partial list supplies new source work. Generation/mapping is checked
    // again before each transport; receipts survive later pause/remap/cancel.
    public Task<BootstrapExecutionResult> ResumeSessionAsync(string sessionId, BootstrapExecutionRequest request,
        Func<BootstrapPublicationReceipt, CancellationToken, Task>? publicationConfirmed = null, CancellationToken cancellationToken = default)
        => Task.Run(() => ResumeSessionCoreAsync(sessionId, request, publicationConfirmed, cancellationToken), CancellationToken.None);

    private async Task<BootstrapExecutionResult> ResumeSessionCoreAsync(string sessionId, BootstrapExecutionRequest request,
        Func<BootstrapPublicationReceipt, CancellationToken, Task>? publicationConfirmed, CancellationToken cancellationToken)
    {
        await using var ownership = await state.AcquireBootstrapExecutionOwnerAsync(request.MappingId, cancellationToken);
        using var context = Prepare(request, cancellationToken);
        await EnsureCaptureSchemaAsync(cancellationToken);
        var first = (await state.ListBootstrapWorkAsync(sessionId, limit: 1, cancellationToken: cancellationToken)).FirstOrDefault();
        if (first is null) throw new InvalidDataException("Unknown or empty bootstrap session.");
        if (first.MappingId != request.MappingId || first.ClientInstanceId != request.ClientInstanceId || !string.Equals(first.SourceRoot, Path.TrimEndingDirectorySeparator(Path.GetFullPath(request.SourceRoot)), OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal)
            || first.TargetRoot != transport.TargetRoot.AbsoluteUri || first.CredentialScope != transport.CredentialScope)
            throw new InvalidOperationException("Resume cannot rebind an immutable execution session.");
        BootstrapUploadReceiptParser.RequireBinding(first, transport.ReceiptExpectation);
        var channel = NewChannel(context.Limits);
        var workers = Enumerable.Range(0, context.Limits.DataJobs).Select(_ => WorkerAsync(channel.Reader, context, publicationConfirmed, cancellationToken)).ToArray();
        string? resumeError = null;
        try
        {
            long cursor = 0;
            while (true)
            {
                var page = await state.ListBootstrapWorkAsync(sessionId, cursor, context.Limits.ReadyQueueCapacity, cancellationToken);
                if (page.Count == 0) break;
                foreach (var work in page)
                    if ((work.IsDirectory ? work.SealedSize == 0 && work.SourceIdentity is not null : work.SpoolPath is not null)
                        && work.Phase is BootstrapWorkPhase.Captured or BootstrapWorkPhase.Transferring or BootstrapWorkPhase.Received or BootstrapWorkPhase.Failed)
                    { await channel.Writer.WriteAsync(work, cancellationToken); context.ObserveReady(channel.Reader.Count); }
                cursor = page[^1].WorkId;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { resumeError = "Execution replay cancelled; sealed intents and acknowledged outcomes are retained."; }
        finally { channel.Writer.TryComplete(); await Task.WhenAll(workers); }
        return await ResultAsync(sessionId, await state.GetBootstrapScanAsync(first.ScanId, CancellationToken.None), context, cancellationToken.IsCancellationRequested, resumeError);
    }

    private static Channel<BootstrapWorkRecord> NewChannel(BootstrapExecutionLimits limits) => Channel.CreateBounded<BootstrapWorkRecord>(new BoundedChannelOptions(limits.ReadyQueueCapacity)
        { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = false });

    private async Task WorkerAsync(ChannelReader<BootstrapWorkRecord> queue, ExecutionContext context,
        Func<BootstrapPublicationReceipt, CancellationToken, Task>? published, CancellationToken ct)
    {
        try
        {
            await foreach (var initial in queue.ReadAllAsync(ct))
            {
                var work = initial;
                var isLarge = work.EnumeratedSize >= context.Limits.LargeFileThreshold;
                var largeAcquired = false;
                var reserved = false;
                var capturedHere = false;
                FileStream? spoolGuard = null;
                try
                {
                    if (isLarge) { await context.Large.WaitAsync(ct); largeAcquired = true; context.LargeStarted(); }
                    if (work.Phase == BootstrapWorkPhase.Queued)
                    {
                        if (work.IsDirectory)
                        {
                            await context.Reader.WaitAsync(ct); context.ReaderStarted();
                            try { await _capture.ValidateDirectoryAsync(work.SourceRoot, work.RelativePath, ct); }
                            finally { context.ReaderEnded(); context.Reader.Release(); }
                            await state.SealBootstrapWorkAsync(work, new("", 0, "", "directory-observation"), ct);
                            work = work with { Phase = BootstrapWorkPhase.Captured, SealedSize = 0 };
                        }
                        else
                        {
                            await context.ReserveAsync(work.OperationId, work.EnumeratedSize, ct);
                            reserved = true;
                            var spool = Path.Combine(context.SpoolRoot, work.OperationId + ".spool");
                            await context.Reader.WaitAsync(ct); context.ReaderStarted();
                            BootstrapSealedSource sealedSource;
                            NativeBootstrapSeal? nativeSeal = null;
                            try
                            {
                                if (_capture is IQualifiedBootstrapSourceCapture native)
                                { nativeSeal = await native.CaptureWorkAsync(work, spool, work.EnumeratedSize, ct); sealedSource = nativeSeal.Source; }
                                else sealedSource = await _capture.CaptureAsync(work.SourceRoot, work.RelativePath, spool, work.EnumeratedSize, ct);
                            }
                            finally { context.ReaderEnded(); context.Reader.Release(); }
                            if (sealedSource.SpoolPath != spool || sealedSource.Size != work.EnumeratedSize || sealedSource.Sha256.Length != 64 || !sealedSource.Sha256.All(Uri.IsHexDigit))
                                throw new InvalidDataException("Capture did not return the reserved sealed source.");
                            work = work with { Phase = BootstrapWorkPhase.Captured, SpoolPath = sealedSource.SpoolPath, SealedSize = sealedSource.Size,
                                Sha256 = sealedSource.Sha256, SourceIdentity = sealedSource.SourceIdentity, NativeCaptureProofJson = nativeSeal?.ProofJson };
                            if (nativeSeal is not null) await state.SealNativeBootstrapWorkAsync(initial, nativeSeal, ct);
                            else await state.SealBootstrapWorkAsync(initial, sealedSource, ct);
                            capturedHere = true;
                        }
                    }
                    if (work.IsDirectory && initial.Phase != BootstrapWorkPhase.Queued)
                    {
                        await context.Reader.WaitAsync(ct); context.ReaderStarted();
                        try { await _capture.ValidateDirectoryAsync(work.SourceRoot, work.RelativePath, ct); }
                        finally { context.ReaderEnded(); context.Reader.Release(); }
                    }
                    if (!work.IsDirectory)
                    {
                        var evidence = NativeBootstrapCaptureProof.ValidatePersisted(work);
                        if (_capture is IQualifiedBootstrapSourceCapture && evidence is null)
                            throw new InvalidDataException("Old sealed bytes have no native write-exclusion witness; this unresolved intent is retained for its original legacy recovery policy.");
                        spoolGuard = await context.OpenOwnedSpoolAsync(work, verifyDigest: !capturedHere, ct);
                    }
                    await state.BeginBootstrapWorkAttemptAsync(work, ct);
                    work = work with { Attempt = work.Attempt + 1, Phase = BootstrapWorkPhase.Transferring };
                    context.DataStarted();
                    BootstrapPublicationReceipt receipt;
                    try { receipt = await transport.TransferAsync(work, checkpoint => state.SaveBootstrapWorkCheckpointAsync(work, checkpoint), ct); }
                    finally { context.DataEnded(); }
                    if (receipt.OperationId != work.OperationId || receipt.TargetUri != WebDavSyncClient.RelativeTargetUri(transport.TargetRoot, work.RelativePath).AbsoluteUri
                        || receipt.RelativePath != work.RelativePath || receipt.IsDirectory != work.IsDirectory || receipt.Size != work.SealedSize
                        || !work.IsDirectory && receipt.Sha256 != work.Sha256 || !Enum.IsDefined(receipt.Proof) || receipt.Proof == BootstrapPublicationProof.Uncertain)
                        throw new InvalidDataException("Transport receipt does not match its immutable intent.");
                    var phase = receipt.Proof == BootstrapPublicationProof.ObservedEqual ? BootstrapWorkPhase.ObservedEqual : BootstrapWorkPhase.Published;
                    await state.FinishBootstrapWorkAsync(work, phase, JsonSerializer.Serialize(receipt));
                    // No cleanup/notification error is a publication failure.
                    // Keep the committed receipt even if an owned spool is busy
                    // or a caller callback fails; join the remaining workers.
                    spoolGuard?.Dispose(); spoolGuard = null;
                    async Task PostCommitAsync(Func<Task> action)
                    {
                        try { await action(); }
                        catch (Exception error)
                        {
                            context.PostCommitDiagnostic();
                            try { await state.SaveBootstrapPostCommitDiagnosticAsync(work, error.Message); }
                            catch { context.PostCommitDiagnostic(); } // receipt already committed
                        }
                    }
                    if (!work.IsDirectory) await PostCommitAsync(() => { context.Release(work.OperationId, deleteSealed: true); return Task.CompletedTask; });
                    if (published is not null && receipt.Proof != BootstrapPublicationProof.ObservedEqual)
                        await PostCommitAsync(() => published(receipt, CancellationToken.None));
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
                catch (BootstrapCaptureDeferredException error) { await state.FinishBootstrapWorkAsync(work, BootstrapWorkPhase.Deferred, error: error.Message); }
                catch (BootstrapCreateOnlyConflictException error) { await state.FinishBootstrapWorkAsync(work, BootstrapWorkPhase.Conflict, error: error.Message); }
                catch (Exception error) { await state.FinishBootstrapWorkAsync(work, BootstrapWorkPhase.Failed, error: error.Message); }
                finally
                {
                    spoolGuard?.Dispose();
                    if (reserved && work.SpoolPath is null) context.Release(work.OperationId, deleteSealed: true);
                    else if (reserved) context.Retain(work.OperationId);
                    if (largeAcquired) { context.LargeEnded(); context.Large.Release(); }
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }

    private Task EnsureCaptureSchemaAsync(CancellationToken ct) => _capture is IQualifiedBootstrapSourceCapture
        ? state.EnsureNativeBootstrapWorkSchemaAsync(ct) : state.EnsureBootstrapWorkSchemaAsync(ct);

    private ExecutionContext Prepare(BootstrapExecutionRequest request, CancellationToken ct)
    {
        var limits = request.Limits ?? new(); limits.Validate();
        if (transport.TargetRoot.Scheme is not ("https" or "http") || transport.TargetRoot.UserInfo.Length != 0
            || transport.TargetRoot.Query.Length != 0 || transport.TargetRoot.Fragment.Length != 0 || !transport.TargetRoot.AbsoluteUri.EndsWith('/')
            || transport.CredentialScope.Length != 64 || !transport.CredentialScope.All(Uri.IsHexDigit)) throw new ArgumentException("Transport root/scope is invalid.");
        var sourceRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(request.SourceRoot));
        var spoolRoot = Path.GetFullPath(request.SpoolDirectory);
        if (Within(sourceRoot, spoolRoot) || Within(sourceRoot, state.BootstrapExecutionDatabasePath)) throw new InvalidOperationException("State/spool must be outside the source tree.");
        Directory.CreateDirectory(spoolRoot);
        if ((File.GetAttributes(spoolRoot) & FileAttributes.ReparsePoint) != 0) throw new IOException("Spool root is a reparse point.");
        var scope = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(state.BootstrapExecutionDatabasePath + "\n" + request.MappingId + "\n" + transport.TargetRoot.AbsoluteUri + "\n" + transport.CredentialScope)));
        spoolRoot = Path.Combine(spoolRoot, "bootstrap-" + scope);
        Directory.CreateDirectory(spoolRoot);
        if ((File.GetAttributes(spoolRoot) & FileAttributes.ReparsePoint) != 0) throw new IOException("Owned spool directory is a reparse point.");
        ct.ThrowIfCancellationRequested();
        return new(limits, spoolRoot);
    }
    private static bool Within(string root, string path) => string.Equals(root, path, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal)
        || path.StartsWith(root + Path.DirectorySeparatorChar, OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);
    private async Task<BootstrapExecutionResult> ResultAsync(string session, BootstrapScanState? scan, ExecutionContext context, bool cancelled, string? error)
    {
        long queued = 0, files = 0, directories = 0, equal = 0, received = 0, bytes = 0, conflicts = 0, failures = 0, deferred = 0, cursor = 0;
        while (true)
        {
            var page = await state.ListBootstrapWorkAsync(session, cursor, cancellationToken: CancellationToken.None);
            if (page.Count == 0) break;
            foreach (var work in page)
            {
                queued++; received = checked(received + work.ReceivedBytes);
                if (work.Phase == BootstrapWorkPhase.Published) { if (work.IsDirectory) directories++; else { files++; bytes = checked(bytes + work.SealedSize!.Value); } }
                if (work.Phase == BootstrapWorkPhase.ObservedEqual) equal++;
                if (work.Phase == BootstrapWorkPhase.Conflict) conflicts++;
                if (work.Phase == BootstrapWorkPhase.Failed) failures++;
                if (work.Phase == BootstrapWorkPhase.Deferred) deferred++;
            }
            cursor = page[^1].WorkId;
        }
        return new(session, scan, queued, files, directories, equal, received, bytes, conflicts, failures, deferred, cancelled, error,
            context.MaxData, context.MaxLarge, context.MaxReaders, context.MaxReady, context.MaxSpool, context.PostCommitCount);
    }
    private static BootstrapExecutionResult EmptyResult(string? error, bool cancelled) => new("", null, 0, 0, 0, 0, 0, 0, 0, 0, 0, cancelled, error, 0, 0, 0, 0, 0);

    private sealed class ExecutionContext : IDisposable
    {
        public readonly BootstrapExecutionLimits Limits;
        public readonly string SpoolRoot;
        public readonly SemaphoreSlim Reader = new(1, 1), Large;
        private readonly object _spoolLock = new();
        private readonly Dictionary<string, long> _spools = new();
        private readonly HashSet<string> _activeSpools = new();
        private TaskCompletionSource _spoolChanged = NewSpoolSignal();
        private readonly IDisposable? _directoryGuard;
        private long _spoolBytes;
        private int _data, _large, _readers;
        public int MaxData, MaxLarge, MaxReaders, MaxReady;
        public long MaxSpool, LastWorkId;
        public int PostCommitCount;
        public ExecutionContext(BootstrapExecutionLimits limits, string spoolRoot)
        {
            Limits = limits; SpoolRoot = spoolRoot; Large = new(limits.LargeJobs, limits.LargeJobs);
            _directoryGuard = OperatingSystem.IsWindows() ? WindowsBootstrapSourceCapture.HoldDirectoryPath(spoolRoot) : null;
            try
            {
            foreach (var file in new DirectoryInfo(spoolRoot).EnumerateFiles())
            {
                if (_spools.Count >= limits.MaximumSpoolFiles || file.Name.Length != 70 || !file.Name.EndsWith(".spool", StringComparison.Ordinal)
                    || !file.Name[..64].All(Uri.IsHexDigit) || (file.Attributes & FileAttributes.ReparsePoint) != 0)
                    throw new BootstrapCaptureDeferredException("Spool inventory exceeds its bound or contains an unqualified file.");
                _spools.Add(file.Name[..64], file.Length); _spoolBytes = checked(_spoolBytes + file.Length);
            }
            MaxSpool = _spoolBytes;
            if (_spoolBytes > limits.SpoolBudgetBytes || new DriveInfo(Path.GetPathRoot(SpoolRoot)!).AvailableFreeSpace < limits.DiskFreeFloorBytes)
                throw new BootstrapCaptureDeferredException("Existing sealed inventory exceeds the configured spool/free-space budget; preserve it for explicit recovery.");
            }
            catch { _directoryGuard?.Dispose(); Reader.Dispose(); Large.Dispose(); throw; }
        }
        public async Task ReserveAsync(string operation, long bytes, CancellationToken ct)
        {
            while (true)
            {
                Task change;
                lock (_spoolLock)
                {
                    if (_spools.ContainsKey(operation)) throw new InvalidOperationException("A sealed spool identity cannot be reused for a new capture.");
                    if (bytes < 0 || bytes > Limits.SpoolBudgetBytes) throw new BootstrapCaptureDeferredException("Source exceeds this opt-in spool quota.");
                    var free = new DriveInfo(Path.GetPathRoot(SpoolRoot)!).AvailableFreeSpace;
                    if (free < Limits.DiskFreeFloorBytes) throw new BootstrapCaptureDeferredException("Spool free-space floor is unavailable.");
                    // Reserve free space against other admitted captures too.
                    // Counting active captured bytes again is conservative:
                    // it cannot oversubscribe a floor while writes lag admission.
                    var activeReservation = _activeSpools.Sum(id => _spools[id]);
                    if (bytes <= Limits.SpoolBudgetBytes - _spoolBytes && _spools.Count < Limits.MaximumSpoolFiles
                        && bytes <= free - Limits.DiskFreeFloorBytes - activeReservation)
                    {
                        _spools.Add(operation, bytes); _activeSpools.Add(operation);
                        _spoolBytes = checked(_spoolBytes + bytes); MaxSpool = Math.Max(MaxSpool, _spoolBytes); return;
                    }
                    if (_activeSpools.Count == 0) throw new BootstrapCaptureDeferredException("Unresolved retained spools prevent admission; no active transfer can release capacity.");
                    change = _spoolChanged.Task;
                }
                await change.WaitAsync(ct); // no source-reader permit held
            }
        }
        public async Task<FileStream> OpenOwnedSpoolAsync(BootstrapWorkRecord work, bool verifyDigest, CancellationToken ct)
        {
            if (work.SpoolPath != Path.Combine(SpoolRoot, work.OperationId + ".spool") || !File.Exists(work.SpoolPath)
                || (File.GetAttributes(work.SpoolPath) & FileAttributes.ReparsePoint) != 0 || new FileInfo(work.SpoolPath).Length != work.SealedSize)
                throw new InvalidDataException("The sealed spool cannot be recovered inside its owned scope.");
            var file = new FileStream(work.SpoolPath, FileMode.Open, FileAccess.Read, FileShare.Read, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
            try
            {
                if (OperatingSystem.IsWindows()) WindowsBootstrapSourceCapture.RequireHandlePath(file.SafeFileHandle, work.SpoolPath);
                if (file.Length != work.SealedSize || verifyDigest && Convert.ToHexStringLower(await SHA256.HashDataAsync(file, ct)) != work.Sha256)
                    throw new InvalidDataException("Recovered sealed spool identity/hash changed; transport is forbidden.");
                return file; // deny writes/deletes throughout transfer, including a second read handle
            }
            catch { file.Dispose(); throw; }
        }
        public void Release(string operation, bool deleteSealed)
        {
            lock (_spoolLock)
            {
                if (deleteSealed)
                {
                    var path = Path.Combine(SpoolRoot, operation + ".spool");
                    if (File.Exists(path)) { if ((File.GetAttributes(path) & FileAttributes.ReparsePoint) != 0) throw new IOException("Spool changed into a reparse point."); File.Delete(path); }
                }
                if (_spools.Remove(operation, out var bytes)) _spoolBytes -= bytes;
                _activeSpools.Remove(operation); PulseSpool();
            }
        }
        public void Retain(string operation) { lock (_spoolLock) { _activeSpools.Remove(operation); PulseSpool(); } }
        private static TaskCompletionSource NewSpoolSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
        private void PulseSpool() { var previous = _spoolChanged; _spoolChanged = NewSpoolSignal(); previous.TrySetResult(); }
        public void PostCommitDiagnostic() => Interlocked.Increment(ref PostCommitCount);
        public void Dispose() { _directoryGuard?.Dispose(); Reader.Dispose(); Large.Dispose(); }
        public void DataStarted() { var current = Interlocked.Increment(ref _data); lock (_spoolLock) MaxData = Math.Max(MaxData, current); }
        public void DataEnded() => Interlocked.Decrement(ref _data);
        public void LargeStarted() { var current = Interlocked.Increment(ref _large); lock (_spoolLock) MaxLarge = Math.Max(MaxLarge, current); }
        public void LargeEnded() => Interlocked.Decrement(ref _large);
        public void ReaderStarted() { var current = Interlocked.Increment(ref _readers); lock (_spoolLock) MaxReaders = Math.Max(MaxReaders, current); }
        public void ReaderEnded() => Interlocked.Decrement(ref _readers);
        public void ObserveReady(int count) { lock (_spoolLock) MaxReady = Math.Max(MaxReady, count); }
    }
}
