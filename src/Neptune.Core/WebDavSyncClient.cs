using System.Net;
using System.Security.Cryptography;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Xml;
using System.Text;
using System.Xml.Linq;

namespace Neptune.Core;

public sealed partial class WebDavSyncClient(HttpClient httpClient, NeptuneStateStore? state, TimeProvider? timeProvider)
{
    public WebDavSyncClient(HttpClient httpClient, NeptuneStateStore? state = null) : this(httpClient, state, null) { }
    public string? RetainedOriginalsDirectory { get; init; }
    public IProgress<UploadTransferProgress>? TransferProgress { get; set; }
    private void ReportTransfer(long received,long total,bool initial=false)
    { if(TransferProgress is { } progress)try{progress.Report(new(received,total,initial));}catch{ /* Display observers cannot change publication semantics. */ } }
    public async Task RecoverRetainedUploadsAsync(Uri namespaceRoot, string token, CancellationToken ct, Func<Uri, bool>? mayRecover = null)
    {
        if (RetainedOriginalsDirectory is not { } directory) return;
        var pending = RetainedLegacyUpload.Pending(directory, UploadScope(namespaceRoot, token), namespaceRoot).ToArray();
        if (mayRecover is not null && pending.Any(target => !mayRecover(target)))
            throw new InvalidOperationException("An excluded folder has an unfinished upload. Its original bytes and operation are retained. Restore the previous exclusions to recover it before changing this folder's policy.");
        foreach (var target in pending)
            await UploadTargetAsync(target, token, "", ct);
    }

    private const string OwnerMarker = "_neptune-owner";
    private const int CollectionCacheCapacity = 4096;
    private static readonly TimeSpan CollectionCacheLifetime = TimeSpan.FromMinutes(2);
    private readonly TimeProvider _clock = timeProvider ?? TimeProvider.System;
    private readonly object _collectionCacheLock = new();
    private readonly Dictionary<string, CollectionCacheEntry> _collectionCache = new(StringComparer.Ordinal);
    private readonly LinkedList<string> _collectionCacheOrder = new();
    // Fixed stripes bound synchronization state even when callers prepare many
    // distinct paths. Cache rechecking under the stripe combines one parent.
    private readonly SemaphoreSlim[] _collectionLocks = Enumerable.Range(0, 64).Select(_ => new SemaphoreSlim(1, 1)).ToArray();
    private readonly SemaphoreSlim _collectionRequests = new(4, 4);
    private long _collectionCacheGeneration;

    public async Task<string?> UploadAsync(Uri syncBaseUri, string token, string clientInstanceId, string mappingId, string relativePath, string localPath, CancellationToken cancellationToken = default)
    {
        var segments = TargetSegments(clientInstanceId, mappingId, relativePath);
        await EnsureCollectionsAsync(syncBaseUri, token, segments[..^1], cancellationToken);
        var target = new Uri(EnsureSlash(syncBaseUri), string.Join('/', segments));
        return await UploadPreparedTargetAsync(syncBaseUri, token, segments[..^1], target, localPath, cancellationToken);
    }

    public static Uri TargetUri(Uri syncBaseUri, string clientInstanceId, string mappingId, string relativePath) =>
        new(EnsureSlash(syncBaseUri), string.Join('/', TargetSegments(clientInstanceId, mappingId, relativePath)));

    public static Uri RelativeTargetUri(Uri root, string relativePath) =>
        new(EnsureSlash(root), string.Join('/', CanonicalSegments(relativePath).Select(Uri.EscapeDataString)));

    public static string CanonicalPathKey(string relativePath) =>
        UnicodeNames.Key(string.Join('/', CanonicalSegments(relativePath)));

    public async Task<Uri> ClaimNamespaceAsync(Uri syncBaseUri, string token, string folderName, string clientInstanceId, CancellationToken cancellationToken = default)
    {
        var folderSegments = CanonicalSegments(folderName);
        if (folderSegments.Length != 1) throw new ArgumentException("Folder name must be one path segment.", nameof(folderName));
        var target = new Uri(EnsureSlash(syncBaseUri), Uri.EscapeDataString(folderSegments[0]) + "/");
        if (await CollectionExistsAsync(target, token, cancellationToken))
        {
            var owner = await ReadTextAsync(new Uri(target, OwnerMarker), token, cancellationToken);
            if (!string.Equals(owner?.Trim(), clientInstanceId, StringComparison.Ordinal))
                throw new InvalidOperationException($"Saturn sync folder '{folderName}' already exists and belongs to another client.");
            return target;
        }

        using (var create = new HttpRequestMessage(new HttpMethod("MKCOL"), target))
        {
            create.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
            using var response = await SyncHttpPolicy.SendAsync(httpClient, create, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            if (response.StatusCode != HttpStatusCode.Created)
                throw new InvalidOperationException($"Saturn sync folder '{folderName}' was claimed by another client.");
        }
        using (var marker = new HttpRequestMessage(HttpMethod.Put, new Uri(target, OwnerMarker)))
        {
            marker.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
            marker.Headers.TryAddWithoutValidation("If-None-Match", "*");
            marker.Content = new StringContent(clientInstanceId, Encoding.UTF8, "text/plain");
            using var response = await SyncHttpPolicy.SendAsync(httpClient, marker, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            await NetworkFailure.EnsureLegacySuccessAsync(response, cancellationToken);
        }
        return target;
    }

    public async Task<SaturnPalette> ReadPaletteAsync(Uri preferencesUri, string token, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, preferencesUri);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        using var response = await SyncHttpPolicy.SendAsync(httpClient, request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        await NetworkFailure.EnsureLegacySuccessAsync(response, cancellationToken);
        using var document = await BoundedJson.ReadAsync(response.Content, 4096, cancellationToken);
        var value = document.RootElement.Deserialize<SaturnPreferences>(new JsonSerializerOptions(JsonSerializerDefaults.Web))
            ?? throw new InvalidDataException("Saturn preferences response is empty.");
        var palette = new SaturnPalette(value.AccentColor, value.BlackColor ?? SaturnPalette.Default.BlackColor, value.WhiteColor ?? SaturnPalette.Default.WhiteColor);
        if (!IsColor(palette.AccentColor) || !IsColor(palette.BlackColor) || !IsColor(palette.WhiteColor))
            throw new InvalidDataException("Saturn returned an invalid color palette.");
        return palette;
    }

    private static bool IsColor(string? value) =>
        value is { Length: 7 } && value[0] == '#' && value[1..].All(Uri.IsHexDigit);

    public async Task EnsureDirectoryAsync(Uri mappingRoot, string token, string relativePath, CancellationToken cancellationToken = default)
    {
        var segments = CanonicalSegments(relativePath).Select(Uri.EscapeDataString).ToArray();
        await EnsureCollectionsAsync(mappingRoot, token, segments, cancellationToken);
    }

    public async Task<string?> UploadAsync(Uri mappingRoot, string token, string relativePath, string localPath, CancellationToken cancellationToken = default)
    {
        return await UploadRelativeTargetAsync(mappingRoot, token, relativePath, localPath, null, cancellationToken);
    }

    public async Task<string?> UploadAsync(Uri mappingRoot, string token, string relativePath, string localPath, string expectedSha256, CancellationToken cancellationToken = default)
    {
        if (expectedSha256.Length != 64 || !expectedSha256.All(Uri.IsHexDigit))
            throw new ArgumentException("Expected SHA-256 is invalid.", nameof(expectedSha256));
        return await UploadRelativeTargetAsync(mappingRoot, token, relativePath, localPath, expectedSha256.ToLowerInvariant(), cancellationToken);
    }

    private async Task<string?> UploadRelativeTargetAsync(Uri mappingRoot, string token, string relativePath, string localPath, string? expectedSha256, CancellationToken cancellationToken)
    {
        var segments = CanonicalSegments(relativePath).Select(Uri.EscapeDataString).ToArray();
        if (segments.Length == 0) throw new ArgumentException("Relative file path is empty.", nameof(relativePath));
        await EnsureCollectionsAsync(mappingRoot, token, segments[..^1], cancellationToken);
        var target = new Uri(EnsureSlash(mappingRoot), string.Join('/', segments));
        return await UploadPreparedTargetAsync(mappingRoot, token, segments[..^1], target, localPath, cancellationToken, expectedSha256);
    }

    private async Task<string> UploadPreparedTargetAsync(Uri root, string token, string[] parentSegments, Uri target, string localPath, CancellationToken cancellationToken, string? expectedSha256 = null)
    {
        try { return await UploadTargetAsync(target, token, localPath, cancellationToken, expectedSha256); }
        catch (HttpRequestException error) when (error.StatusCode is HttpStatusCode.NotFound or HttpStatusCode.Conflict)
        {
            // Do not replay a publication or change its precondition after a
            // generic conflict. The next caller re-prepares this branch; durable
            // upload checkpoints and their existing recovery policy stay intact.
            InvalidateCollections(root, token, parentSegments);
            throw;
        }
    }

    private async Task<string> UploadTargetAsync(Uri target, string token, string localPath, CancellationToken cancellationToken, string? expectedSha256 = null)
    {
        if (RetainedOriginalsDirectory is { } directory)
        {
            if (state is null) throw new InvalidOperationException("Retained legacy uploads require durable checkpoint storage.");
            await using var original = await RetainedLegacyUpload.OpenAsync(directory, UploadScope(target, token), target, localPath, expectedSha256, cancellationToken);
            var confirmed = await UploadSealedAsync(target, token, original.Content, original.Sha256, original, cancellationToken);
            await original.ConfirmAsync();
            return confirmed;
        }
        await using var snapshot = await LocalUploadSnapshot.CaptureAsync(localPath, expectedSha256, cancellationToken);
        return await UploadSealedAsync(target, token, snapshot.Content, snapshot.Sha256, null, cancellationToken);
    }

    private async Task<string> UploadSealedAsync(Uri target, string token, FileStream source, string digest, RetainedLegacyUpload? original, CancellationToken cancellationToken)
    {
        var observedEtag = await ReadEtagAsync(target, token, cancellationToken);
        if (original is not null) await original.BindAsync(observedEtag, cancellationToken);
        var currentEtag = original is null ? observedEtag : original.Etag;
        var expectedEtag = $"\"sha256-{digest}\"";
        var fingerprint = digest + "\n" + source.Length + "\n" + currentEtag;
        if (state is not null)
        {
            var previous = await FindCheckpointAsync(target, UploadScope(target,token), cancellationToken);
            if (original is not null && previous is not null && previous.Fingerprint != fingerprint && observedEtag != expectedEtag)
                throw new InvalidDataException("An older legacy operation is unresolved; its checkpoint cannot be replaced by new source bytes.");
            if (previous is not null && (previous.Fingerprint != fingerprint || observedEtag == expectedEtag))
                await CancelCheckpointAsync(previous, token, cancellationToken);
        }
        if (string.Equals(observedEtag, expectedEtag, StringComparison.Ordinal)) return expectedEtag;
        var davPrefix = target.AbsolutePath.StartsWith("/dav/sync/", StringComparison.Ordinal) ? "/dav/"
            : target.AbsolutePath.StartsWith("/webdav/sync/", StringComparison.Ordinal) ? "/webdav/" : null;
        if (source.Length >= 1024 * 1024 && davPrefix is not null)
        {
            var resumed = await UploadResumableAsync(target, target.AbsolutePath[davPrefix.Length..], token, source, digest, currentEtag, cancellationToken);
            if (resumed is not null) return resumed;
            source.Position = 0;
        }
        using var request = new HttpRequestMessage(HttpMethod.Put, target);
        ReportTransfer(0,source.Length,initial:true);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        request.Headers.TryAddWithoutValidation(currentEtag is null ? "If-None-Match" : "If-Match", currentEtag ?? "*");
        request.Headers.TryAddWithoutValidation("X-Content-Sha256", digest);
        request.Content = new StreamContent(source);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        using var response = await SyncHttpPolicy.SendAsync(httpClient, request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        await NetworkFailure.EnsureLegacySuccessAsync(response, cancellationToken);
        var receipt = response.Headers.ETag?.Tag ?? await ReadEtagAsync(target, token, cancellationToken);
        if (receipt is null || !string.Equals(receipt, $"\"sha256-{digest}\"", StringComparison.Ordinal)) throw new InvalidDataException("Saturn did not confirm the uploaded file checksum.");
        ReportTransfer(source.Length,source.Length);
        return receipt;
    }

    private sealed record UploadCheckpoint(string Id, string Status, long ReceivedSize, long ExpectedSize, string? Etag);

    private async Task<string?> UploadResumableAsync(Uri target, string logicalPath, string token, FileStream source, string digest, string? currentEtag, CancellationToken cancellationToken)
    {
        var endpoint = new Uri(target, "/api/v1/sync/uploads");
        var scope = UploadScope(target, token);
        var fingerprint = digest + "\n" + source.Length + "\n" + currentEtag;
        var key = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(target.AbsoluteUri + "\n" + fingerprint)));
        var saved = await FindCheckpointAsync(target, scope, cancellationToken);
        var recovering = saved is not null;
        key = saved?.IdempotencyKey ?? (state is null ? key : Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(key+"\n"+Guid.NewGuid().ToString("N")))));
        // Persist before create: even losing the create response leaves a key
        // with which this device can cancel the old session after a restart.
        saved = new SyncUploadCheckpoint(scope,target.AbsoluteUri,key,fingerprint,saved?.UploadId);
        if (state is not null) await state.SaveSyncUploadCheckpointAsync(saved,cancellationToken);
        using var create = new HttpRequestMessage(HttpMethod.Post, endpoint);
        create.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        create.Content = new StringContent(JsonSerializer.Serialize(new { path = logicalPath, expectedSize = source.Length, expectedSha256 = digest, idempotencyKey = key, ifMatch = currentEtag, ifNoneMatch = currentEtag is null ? "*" : null },
            new JsonSerializerOptions { DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull }), Encoding.UTF8, "application/json");
        using var created = await SyncHttpPolicy.SendAsync(httpClient, create, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        // Older Saturn releases have no resumable endpoint.
        if (created.StatusCode == HttpStatusCode.NotFound)
        {
            if (recovering && RetainedOriginalsDirectory is not null) throw new InvalidDataException("Original resumable endpoint is unavailable; retained operation cannot downgrade to PUT.");
            if (state is not null) await state.RemoveSyncUploadCheckpointAsync(saved,cancellationToken);
            return null;
        }
        if (created.StatusCode == HttpStatusCode.Conflict && state is not null)
        {
            if (RetainedOriginalsDirectory is not null) throw new InvalidDataException("Original legacy upload requires reconciliation; its key and bytes are retained.");
            // Expired or terminal sessions cannot be revived under their old
            // key. Confirm cancellation, then the next attempt gets a new key.
            await CancelCheckpointAsync(saved,token,cancellationToken);
        }
        await NetworkFailure.EnsureLegacySuccessAsync(created, cancellationToken);
        var checkpoint = await ReadCheckpointAsync(created, source.Length, cancellationToken);
        ReportTransfer(checkpoint.ReceivedSize,source.Length,initial:true);
        saved = saved with { UploadId=checkpoint.Id };
        if (state is not null) await state.SaveSyncUploadCheckpointAsync(saved,cancellationToken);
        var uploadUri = new Uri(endpoint.AbsoluteUri + "/" + checkpoint.Id);
        var buffer = new byte[1024 * 1024];
        while (checkpoint.Status != "active" && checkpoint.ReceivedSize < source.Length)
        {
            source.Position = checkpoint.ReceivedSize;
            var count = (int)Math.Min(buffer.Length, source.Length - source.Position);
            await source.ReadExactlyAsync(buffer.AsMemory(0, count), cancellationToken);
            using var append = new HttpRequestMessage(HttpMethod.Patch, uploadUri);
            append.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
            append.Headers.TryAddWithoutValidation("Upload-Offset", checkpoint.ReceivedSize.ToString(System.Globalization.CultureInfo.InvariantCulture));
            append.Content = new ByteArrayContent(buffer, 0, count);
            append.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            using var appended = await SyncHttpPolicy.SendAsync(httpClient, append, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            await NetworkFailure.EnsureLegacySuccessAsync(appended, cancellationToken);
            var next = await ReadCheckpointAsync(appended, source.Length, cancellationToken);
            if (next.Id != checkpoint.Id || next.ReceivedSize != checkpoint.ReceivedSize + count) throw new InvalidDataException("Saturn returned an invalid upload checkpoint.");
            checkpoint = next;
            ReportTransfer(checkpoint.ReceivedSize,source.Length);
        }
        if (checkpoint.Status != "active")
        {
            using var complete = new HttpRequestMessage(HttpMethod.Post, new Uri(uploadUri.AbsoluteUri + "/complete"));
            complete.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
            using var completed = await SyncHttpPolicy.SendAsync(httpClient, complete, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            await NetworkFailure.EnsureLegacySuccessAsync(completed, cancellationToken);
            checkpoint = await ReadCheckpointAsync(completed, source.Length, cancellationToken);
        }
        if (checkpoint.Status != "active" || !string.Equals(checkpoint.Etag, $"\"sha256-{digest}\"", StringComparison.Ordinal))
            throw new InvalidDataException("Saturn did not confirm the completed file checksum.");
        if (state is not null) await state.RemoveSyncUploadCheckpointAsync(saved,cancellationToken);
        return checkpoint.Etag;
    }

    private static string UploadScope(Uri target, string token) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(target.GetLeftPart(UriPartial.Authority)+"\n"+token.Trim())));

    private async Task<SyncUploadCheckpoint?> FindCheckpointAsync(Uri target, string scope, CancellationToken cancellationToken)
    {
        if (state is null) return null;
        var key = CanonicalTargetKey(target);
        var matches = (await state.ListSyncUploadCheckpointsAsync(scope, cancellationToken))
            .Where(item => CanonicalTargetKey(new Uri(item.TargetUri)) == key).ToArray();
        if (matches.Length > 1) throw new InvalidDataException("Multiple upload checkpoints resolve to the same Saturn path.");
        var match = matches.SingleOrDefault();
        // Releases before NFC path canonicalization persisted the exact local
        // spelling in this key. Move it transactionally and retain the same
        // idempotency key/upload id so an in-flight upload resumes after update.
        return match is not null && match.TargetUri != target.AbsoluteUri
            ? await state.MoveSyncUploadCheckpointAsync(match, target.AbsoluteUri, cancellationToken)
            : match;
    }

    private static string CanonicalTargetKey(Uri target) =>
        target.GetLeftPart(UriPartial.Authority).ToLowerInvariant() + "\n" + CanonicalPathKey(Uri.UnescapeDataString(target.AbsolutePath));

    private async Task CancelCheckpointAsync(SyncUploadCheckpoint checkpoint, string token, CancellationToken cancellationToken)
    {
        using var cancel = new HttpRequestMessage(HttpMethod.Post,new Uri(new Uri(checkpoint.TargetUri),"/api/v1/sync/uploads/cancel"));
        cancel.Headers.Authorization = new AuthenticationHeaderValue("Bearer",token.Trim());
        cancel.Content = new StringContent(JsonSerializer.Serialize(new { idempotencyKey=checkpoint.IdempotencyKey }),Encoding.UTF8,"application/json");
        using var response = await SyncHttpPolicy.SendAsync(httpClient, cancel,HttpCompletionOption.ResponseHeadersRead,cancellationToken);
        await NetworkFailure.EnsureLegacySuccessAsync(response, cancellationToken);
        using var document = await BoundedJson.ReadAsync(response.Content,4096,cancellationToken);
        var status = document.RootElement.GetProperty("status").GetString();
        if (status is not ("absent" or "abandoned" or "active")) throw new InvalidDataException("Saturn did not confirm checkpoint cancellation.");
        if (state is not null) await state.RemoveSyncUploadCheckpointAsync(checkpoint,cancellationToken);
    }

    public async Task CancelMissingUploadsAsync(Uri mappingRoot, string token, IReadOnlySet<string> localFiles, CancellationToken cancellationToken = default, MappingSyncPolicy? policy = null)
    {
        policy?.Validate();
        if (state is null) return;
        var root = EnsureSlash(mappingRoot);
        var localKeys = localFiles.Select(CanonicalPathKey).ToHashSet(StringComparer.Ordinal);
        foreach (var checkpoint in await state.ListSyncUploadCheckpointsAsync(UploadScope(root,token),cancellationToken))
        {
            var target = new Uri(checkpoint.TargetUri);
            if (!root.IsBaseOf(target)) continue;
            var relative = Uri.UnescapeDataString(root.MakeRelativeUri(target).OriginalString);
            if (policy?.Excludes(relative) != true && !localKeys.Contains(CanonicalPathKey(relative))) await CancelCheckpointAsync(checkpoint,token,cancellationToken);
        }
    }

    private static async Task<UploadCheckpoint> ReadCheckpointAsync(HttpResponseMessage response, long expectedSize, CancellationToken cancellationToken)
    {
        using var document = await BoundedJson.ReadAsync(response.Content, 4096, cancellationToken);
        var value = document.RootElement.Deserialize<UploadCheckpoint>(new JsonSerializerOptions(JsonSerializerDefaults.Web));
        if (value is null || !Guid.TryParseExact(value.Id, "D", out _) || value.ExpectedSize != expectedSize || value.ReceivedSize < 0 || value.ReceivedSize > expectedSize
            || value.Status is not ("created" or "uploading" or "failed_retryable" or "verifying" or "committing" or "active"))
            throw new InvalidDataException("Saturn returned an invalid upload checkpoint.");
        return value;
    }

    public async Task<int> MirrorAsync(Uri mappingRoot, string token, IReadOnlySet<string> localFiles, IReadOnlySet<string> localDirectories, bool protectMassDeletion = true, CancellationToken cancellationToken = default, MappingSyncPolicy? policy = null)
    {
        policy?.Validate();
        var remote = await EnumerateAsync(mappingRoot, token, "", cancellationToken, policy);
        var localFileKeys = localFiles.Select(CanonicalPathKey).ToHashSet(StringComparer.Ordinal);
        var localDirectoryKeys = localDirectories.Select(CanonicalPathKey).ToHashSet(StringComparer.Ordinal);
        var removedFiles = remote.Where(value => !value.IsCollection && !localFileKeys.Contains(CanonicalPathKey(value.RelativePath))).ToArray();
        var removedDirectories = remote.Where(value => value.IsCollection && policy?.ProtectsSubtree(value.RelativePath) != true && !localDirectoryKeys.Contains(CanonicalPathKey(value.RelativePath))).OrderByDescending(value => value.RelativePath.Count(c => c == '/')).ToArray();
        var removed = removedFiles.Length + removedDirectories.Length;
        if (protectMassDeletion && localFiles.Count == 0 && removedFiles.Length > 0)
            throw new InvalidOperationException("Mirror deletion guard stopped an empty local snapshot from removing stored files.");
        if (protectMassDeletion && removed > 20 && removed * 4 > Math.Max(1, remote.Count))
            throw new InvalidOperationException($"Mirror deletion guard stopped removal of {removed} out of {remote.Count} remote entries.");
        foreach (var entry in removedFiles)
            await DeleteAsync(entry.Uri, token, cancellationToken);
        foreach (var entry in removedDirectories)
            await DeleteAsync(entry.Uri, token, cancellationToken);
        return removed;
    }

    public async Task<string?> ReadEtagAsync(Uri target, string token, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Head, target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        using var response = await SyncHttpPolicy.SendAsync(httpClient, request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound) return null;
        await NetworkFailure.EnsureLegacySuccessAsync(response, cancellationToken);
        return response.Headers.ETag?.Tag;
    }

    private async Task EnsureCollectionsAsync(Uri root, string token, IEnumerable<string> segments, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var parts = segments.ToArray();
        var scope = CollectionScope(root, token);
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                var current = EnsureSlash(root);
                foreach (var segment in parts)
                {
                    current = new Uri(current, segment + "/");
                    await EnsureCollectionAsync(current, scope, token, cancellationToken);
                }
                return;
            }
            catch (CollectionParentMissingException)
            {
                InvalidateCollections(root, token, parts);
                if (attempt != 0) throw new HttpRequestException("Saturn directory parent is missing after preparation.", null, HttpStatusCode.NotFound);
                // A cached ancestor may have disappeared. Re-prepare at most
                // once, starting below the caller's root; never recreate a root.
            }
        }
    }

    private async Task EnsureCollectionAsync(Uri target, string scope, string token, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var key = scope + target.AbsoluteUri;
        if (CollectionCached(key)) return;
        var gate = _collectionLocks[(uint)StringComparer.Ordinal.GetHashCode(key) % (uint)_collectionLocks.Length];
        await gate.WaitAsync(cancellationToken);
        try
        {
            if (CollectionCached(key)) return;
            await _collectionRequests.WaitAsync(cancellationToken);
            try
            {
                long generation;
                lock (_collectionCacheLock) generation = _collectionCacheGeneration;
                using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                deadline.CancelAfter(TimeSpan.FromSeconds(15));
                using var request = new HttpRequestMessage(new HttpMethod("MKCOL"), target);
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
                using var response = await SyncHttpPolicy.SendAsync(httpClient, request, HttpCompletionOption.ResponseHeadersRead, deadline.Token);
                if (response.StatusCode is HttpStatusCode.MethodNotAllowed or HttpStatusCode.Conflict or HttpStatusCode.NotFound)
                {
                    var kind = await ReadCollectionKindAsync(target, token, deadline.Token);
                    if (kind == CollectionKind.Missing) throw new CollectionParentMissingException();
                    if (kind != CollectionKind.Collection) throw new InvalidDataException("A file occupies a required Saturn directory path.");
                }
                else if (response.StatusCode != HttpStatusCode.Created)
                {
                    await NetworkFailure.EnsureLegacySuccessAsync(response, cancellationToken);
                    throw new InvalidDataException("Saturn did not confirm directory creation.");
                }
                CacheCollection(key, generation);
            }
            finally { _collectionRequests.Release(); }
        }
        finally { gate.Release(); }
    }

    private bool CollectionCached(string key)
    {
        lock (_collectionCacheLock)
        {
            if (!_collectionCache.TryGetValue(key, out var entry)) return false;
            _collectionCacheOrder.Remove(entry.Node);
            if (entry.ExpiresAt <= _clock.GetUtcNow())
            {
                _collectionCache.Remove(key);
                return false;
            }
            _collectionCacheOrder.AddLast(entry.Node);
            return true;
        }
    }

    private void CacheCollection(string key, long generation)
    {
        lock (_collectionCacheLock)
        {
            if (_collectionCacheGeneration != generation) return;
            if (_collectionCache.Remove(key, out var old)) _collectionCacheOrder.Remove(old.Node);
            var node = _collectionCacheOrder.AddLast(key);
            _collectionCache.Add(key, new CollectionCacheEntry(_clock.GetUtcNow() + CollectionCacheLifetime, node));
            while (_collectionCache.Count > CollectionCacheCapacity)
            {
                var first = _collectionCacheOrder.First!;
                _collectionCache.Remove(first.Value);
                _collectionCacheOrder.RemoveFirst();
            }
        }
    }

    private void InvalidateCollections(Uri root, string token, IReadOnlyList<string> segments)
    {
        if (segments.Count == 0) return;
        var branch = CollectionScope(root, token) + new Uri(EnsureSlash(root), segments[0] + "/").AbsoluteUri;
        lock (_collectionCacheLock)
        {
            _collectionCacheGeneration++;
            foreach (var key in _collectionCache.Keys.Where(key => key.StartsWith(branch, StringComparison.Ordinal)).ToArray())
            {
                _collectionCacheOrder.Remove(_collectionCache[key].Node);
                _collectionCache.Remove(key);
            }
        }
    }

    private static string CollectionScope(Uri root, string token) => UploadScope(root, token) + "\n" + EnsureSlash(root).AbsoluteUri + "\n";

    private async Task<CollectionKind> ReadCollectionKindAsync(Uri target, string token, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(new HttpMethod("PROPFIND"), target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        request.Headers.TryAddWithoutValidation("Depth", "0");
        using var response = await SyncHttpPolicy.SendAsync(httpClient, request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound) return CollectionKind.Missing;
        await NetworkFailure.EnsureLegacySuccessAsync(response, cancellationToken);
        if ((int)response.StatusCode != 207) throw new InvalidDataException("Saturn returned an invalid directory metadata status.");
        using var text = new StringReader(await ReadBoundedTextAsync(response.Content, 64 * 1024, cancellationToken));
        using var reader = XmlReader.Create(text, new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit, XmlResolver = null, MaxCharactersInDocument = 64 * 1024 });
        var document = XDocument.Load(reader);
        XNamespace dav = "DAV:";
        var rows = document.Root?.Name == dav + "multistatus" ? document.Root.Elements(dav + "response").ToArray() : [];
        if (rows.Length != 1) throw new InvalidDataException("Saturn did not identify exactly one required directory.");
        var href = rows[0].Element(dav + "href")?.Value;
        if (href is null || href.Length > 4096 || href.Any(char.IsControl) || !Uri.TryCreate(target, href, out var actual)
            || actual.GetLeftPart(UriPartial.Authority) != target.GetLeftPart(UriPartial.Authority)
            || actual.UserInfo.Length != 0 || actual.Query.Length != 0 || actual.Fragment.Length != 0 || !SameCollectionPath(actual, target))
            throw new InvalidDataException("Saturn returned metadata outside the required directory path.");
        var types = rows[0].Elements(dav + "propstat").Where(value =>
                value.Element(dav + "status")?.Value is "HTTP/1.1 200 OK" or "HTTP/1.0 200 OK")
            .SelectMany(value => value.Elements(dav + "prop")).SelectMany(value => value.Elements(dav + "resourcetype")).ToArray();
        if (types.Length != 1) throw new InvalidDataException("Saturn did not confirm the required directory type.");
        return types[0].Elements(dav + "collection").Any() ? CollectionKind.Collection : CollectionKind.File;
    }

    private static bool SameCollectionPath(Uri actual, Uri target)
    {
        if (SamePath(actual, target)) return true;
        // Saturn's legacy /webdav route emits canonical /dav hrefs. This exact
        // route alias does not relax the root, path, case or origin checks.
        return target.AbsolutePath.StartsWith("/webdav/", StringComparison.Ordinal)
            && SamePath(actual, new Uri(target, "/dav/" + target.AbsolutePath[8..]));
    }

    private enum CollectionKind { Missing, File, Collection }
    private sealed record CollectionCacheEntry(DateTimeOffset ExpiresAt, LinkedListNode<string> Node);
    private sealed class CollectionParentMissingException : Exception { }

    private async Task<bool> CollectionExistsAsync(Uri target, string token, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(new HttpMethod("PROPFIND"), target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        request.Headers.TryAddWithoutValidation("Depth", "0");
        using var response = await SyncHttpPolicy.SendAsync(httpClient, request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound) return false;
        if ((int)response.StatusCode == 207) return true;
        await NetworkFailure.EnsureLegacySuccessAsync(response, cancellationToken);
        return true;
    }

    private async Task<string?> ReadTextAsync(Uri target, string token, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        using var response = await SyncHttpPolicy.SendAsync(httpClient, request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound) return null;
        await NetworkFailure.EnsureLegacySuccessAsync(response, cancellationToken);
        return await ReadBoundedTextAsync(response.Content, 4096, cancellationToken);
    }

    private static async Task<string> ReadBoundedTextAsync(HttpContent content, int maximum, CancellationToken cancellationToken)
    {
        if (content.Headers.ContentLength > maximum || content.Headers.ContentEncoding.Count != 0)
            throw new InvalidDataException("WebDAV metadata exceeds its bound.");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(15));
        await using var input = await content.ReadAsStreamAsync(deadline.Token);
        using var output = new MemoryStream();
        var buffer = new byte[16 * 1024];
        while (true)
        {
            var count = await input.ReadAsync(buffer, deadline.Token);
            if (count == 0) break;
            if (output.Length + count > maximum) throw new InvalidDataException("WebDAV metadata exceeds its bound.");
            output.Write(buffer, 0, count);
        }
        return new UTF8Encoding(false, true).GetString(output.GetBuffer(), 0, checked((int)output.Length));
    }

    private async Task<IReadOnlyList<RemoteEntry>> EnumerateAsync(Uri directory, string token, string prefix, CancellationToken cancellationToken, MappingSyncPolicy? policy = null)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(15));
        var pending = new Queue<(Uri Directory, string Prefix, int Depth)>();
        pending.Enqueue((EnsureSlash(directory), prefix, 0));
        var result = new List<RemoteEntry>();
        var names = new HashSet<string>(StringComparer.Ordinal);
        var metadataBytes = 0L;
        XNamespace dav = "DAV:";
        while (pending.Count > 0)
        {
            var next = pending.Dequeue();
            if (next.Depth >= 64) throw new InvalidDataException("WebDAV tree exceeds its depth limit.");
            using var requestDeadline = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
            requestDeadline.CancelAfter(TimeSpan.FromSeconds(15));
            using var request = new HttpRequestMessage(new HttpMethod("PROPFIND"), next.Directory);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
            request.Headers.TryAddWithoutValidation("Depth", "1");
            using var response = await SyncHttpPolicy.SendAsync(httpClient, request, HttpCompletionOption.ResponseHeadersRead, requestDeadline.Token);
            await NetworkFailure.EnsureLegacySuccessAsync(response, requestDeadline.Token);
            using var text = new StringReader(await ReadBoundedTextAsync(response.Content, 8 * 1024 * 1024, requestDeadline.Token));
            using var reader = XmlReader.Create(text, new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit, XmlResolver = null, MaxCharactersInDocument = 8 * 1024 * 1024 });
            var xml = XDocument.Load(reader);
            foreach (var item in xml.Descendants(dav + "response"))
            {
                var href = item.Element(dav + "href")?.Value ?? throw new InvalidDataException("WebDAV response has no href.");
                if (href.Length > 4096 || href.Any(char.IsControl) || !Uri.TryCreate(next.Directory, href, out var child)
                    || child.GetLeftPart(UriPartial.Authority) != directory.GetLeftPart(UriPartial.Authority)
                    || child.UserInfo.Length != 0 || child.Query.Length != 0 || child.Fragment.Length != 0)
                    throw new InvalidDataException("WebDAV returned an entry outside the synchronized origin.");
                if (SamePath(child, next.Directory)) continue;
                var properties = item.Elements(dav + "propstat").Where(value =>
                    value.Element(dav + "status")?.Value is "HTTP/1.1 200 OK" or "HTTP/1.0 200 OK")
                    .Select(value => value.Element(dav + "prop")).Where(value => value is not null).ToArray();
                var name = properties.SelectMany(value => value!.Elements(dav + "displayname")).SingleOrDefault()?.Value
                    ?? throw new InvalidDataException("WebDAV response has no successful displayname.");
                if (name is "" or "." or ".." || name.IndexOfAny(['/', '\\', ':']) >= 0 || name.Any(char.IsControl))
                    throw new InvalidDataException("WebDAV returned an unsafe child name.");
                var collection = properties.SelectMany(value => value!.Elements(dav + "resourcetype")).Elements(dav + "collection").Any();
                var expected = new Uri(next.Directory, Uri.EscapeDataString(name) + (collection ? "/" : ""));
                if (!SamePath(child, expected))
                    throw new InvalidDataException("WebDAV child path does not match its declared name.");
                child = expected;
                var relative = next.Prefix.Length == 0 ? name : next.Prefix + "/" + name;
                if (policy?.Excludes(relative) == true) continue;
                metadataBytes += Encoding.UTF8.GetByteCount(relative) + Encoding.UTF8.GetByteCount(child.AbsoluteUri);
                if (result.Count >= 100_000 || metadataBytes > 32L * 1024 * 1024 || !names.Add(UnicodeNames.Key(relative)))
                    throw new InvalidDataException("WebDAV tree exceeds its metadata limit or contains duplicate names.");
                result.Add(new RemoteEntry(child, relative, collection));
                if (collection) pending.Enqueue((EnsureSlash(child), relative, next.Depth + 1));
            }
        }
        return result;
    }

    private async Task DeleteAsync(Uri target, string token, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Delete, target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        using var response = await SyncHttpPolicy.SendAsync(httpClient, request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        if (response.StatusCode != HttpStatusCode.NotFound) await NetworkFailure.EnsureLegacySuccessAsync(response, cancellationToken);
        if (target.AbsolutePath.EndsWith("/", StringComparison.Ordinal))
        {
            lock (_collectionCacheLock)
            {
                _collectionCacheGeneration++;
                foreach (var key in _collectionCache.Keys.Where(key =>
                {
                    var cached = new Uri(key[(key.LastIndexOf('\n') + 1)..]);
                    var canonical = cached.AbsolutePath.StartsWith("/webdav/", StringComparison.Ordinal)
                        ? new Uri(cached, "/dav/" + cached.AbsolutePath[8..]) : cached;
                    return target.IsBaseOf(cached) || target.IsBaseOf(canonical);
                }).ToArray())
                {
                    _collectionCacheOrder.Remove(_collectionCache[key].Node);
                    _collectionCache.Remove(key);
                }
            }
        }
    }

    private static bool SamePath(Uri left, Uri right)
    {
        // Servers may spell parentheses and other permitted characters literally
        // or percent-encoded. Compare once-decoded segments, never whole paths:
        // encoded separators must not acquire directory semantics.
        var a = left.AbsolutePath.TrimEnd('/').Split('/');
        var b = right.AbsolutePath.TrimEnd('/').Split('/');
        if (a.Length != b.Length) return false;
        for (var index = 0; index < a.Length; index++)
        {
            var decoded = Uri.UnescapeDataString(a[index]);
            if (decoded.IndexOfAny(['/', '\\']) >= 0 || decoded.Any(char.IsControl)
                || decoded != Uri.UnescapeDataString(b[index])) return false;
        }
        return true;
    }

    private static Uri EnsureSlash(Uri uri) => uri.AbsoluteUri.EndsWith("/", StringComparison.Ordinal)
        ? uri : new Uri(uri.AbsoluteUri + "/");

    private static string[] TargetSegments(string clientInstanceId, string mappingId, string relativePath) =>
        new[] { clientInstanceId.Normalize(NormalizationForm.FormC), mappingId.Normalize(NormalizationForm.FormC) }
            .Concat(CanonicalSegments(relativePath))
            .Select(Uri.EscapeDataString).ToArray();

    private static string[] CanonicalSegments(string relativePath)
    {
        var segments = relativePath.Replace('\\', '/').Split('/', StringSplitOptions.RemoveEmptyEntries)
            .Select(segment => segment.Normalize(NormalizationForm.FormC).Trim()).ToArray();
        if (segments.Any(segment => segment is "" or "." or ".." || segment.Length > 255 || segment.Any(char.IsControl)))
            throw new InvalidDataException("Relative path contains an invalid segment.");
        return segments;
    }

    private sealed record SaturnPreferences(string AccentColor, string? BlackColor = null, string? WhiteColor = null);
    private sealed record RemoteEntry(Uri Uri, string RelativePath, bool IsCollection);
}
