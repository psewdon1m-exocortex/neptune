using System.Net;
using System.Security.Cryptography;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Xml;
using System.Text;
using System.Xml.Linq;

namespace Neptune.Core;

public sealed class WebDavSyncClient(HttpClient httpClient, NeptuneStateStore? state = null)
{
    private const string OwnerMarker = "_neptune-owner";

    public async Task<string?> UploadAsync(Uri syncBaseUri, string token, string clientInstanceId, string mappingId, string relativePath, string localPath, CancellationToken cancellationToken = default)
    {
        var segments = TargetSegments(clientInstanceId, mappingId, relativePath);
        await EnsureCollectionsAsync(syncBaseUri, token, segments[..^1], cancellationToken);
        var target = new Uri(EnsureSlash(syncBaseUri), string.Join('/', segments));
        return await UploadTargetAsync(target, token, localPath, cancellationToken);
    }

    public static Uri TargetUri(Uri syncBaseUri, string clientInstanceId, string mappingId, string relativePath) =>
        new(EnsureSlash(syncBaseUri), string.Join('/', TargetSegments(clientInstanceId, mappingId, relativePath)));

    public static Uri RelativeTargetUri(Uri root, string relativePath) =>
        new(EnsureSlash(root), string.Join('/', relativePath.Replace('\\', '/').Split('/', StringSplitOptions.RemoveEmptyEntries).Select(Uri.EscapeDataString)));

    public async Task<Uri> ClaimNamespaceAsync(Uri syncBaseUri, string token, string folderName, string clientInstanceId, CancellationToken cancellationToken = default)
    {
        var target = new Uri(EnsureSlash(syncBaseUri), Uri.EscapeDataString(folderName) + "/");
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
            using var response = await httpClient.SendAsync(create, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            if (response.StatusCode != HttpStatusCode.Created)
                throw new InvalidOperationException($"Saturn sync folder '{folderName}' was claimed by another client.");
        }
        using (var marker = new HttpRequestMessage(HttpMethod.Put, new Uri(target, OwnerMarker)))
        {
            marker.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
            marker.Headers.TryAddWithoutValidation("If-None-Match", "*");
            marker.Content = new StringContent(clientInstanceId, Encoding.UTF8, "text/plain");
            using var response = await httpClient.SendAsync(marker, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            response.EnsureSuccessStatusCode();
        }
        return target;
    }

    public async Task<string> ReadAccentAsync(Uri preferencesUri, string token, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, preferencesUri);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        using var document = await BoundedJson.ReadAsync(response.Content, 4096, cancellationToken);
        var value = document.RootElement.Deserialize<SaturnPreferences>(new JsonSerializerOptions(JsonSerializerDefaults.Web))
            ?? throw new InvalidDataException("Saturn preferences response is empty.");
        if (value.AccentColor is not { Length: 7 } accent || accent[0] != '#' || !accent[1..].All(Uri.IsHexDigit))
            throw new InvalidDataException("Saturn returned an invalid accent color.");
        return accent;
    }

    public async Task EnsureDirectoryAsync(Uri mappingRoot, string token, string relativePath, CancellationToken cancellationToken = default)
    {
        var segments = relativePath.Replace('\\', '/').Split('/', StringSplitOptions.RemoveEmptyEntries).Select(Uri.EscapeDataString).ToArray();
        await EnsureCollectionsAsync(mappingRoot, token, segments, cancellationToken);
    }

    public async Task<string?> UploadAsync(Uri mappingRoot, string token, string relativePath, string localPath, CancellationToken cancellationToken = default)
    {
        var segments = relativePath.Replace('\\', '/').Split('/', StringSplitOptions.RemoveEmptyEntries).Select(Uri.EscapeDataString).ToArray();
        if (segments.Length == 0) throw new ArgumentException("Relative file path is empty.", nameof(relativePath));
        await EnsureCollectionsAsync(mappingRoot, token, segments[..^1], cancellationToken);
        var target = new Uri(EnsureSlash(mappingRoot), string.Join('/', segments));
        return await UploadTargetAsync(target, token, localPath, cancellationToken);
    }

    private async Task<string> UploadTargetAsync(Uri target, string token, string localPath, CancellationToken cancellationToken)
    {
        await using var source = new FileStream(localPath, FileMode.Open, FileAccess.Read, FileShare.Read, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
        var digest = Convert.ToHexStringLower(await SHA256.HashDataAsync(source, cancellationToken));
        source.Position = 0;
        var currentEtag = await ReadEtagAsync(target, token, cancellationToken);
        var expectedEtag = $"\"sha256-{digest}\"";
        var fingerprint = digest + "\n" + source.Length + "\n" + currentEtag;
        if (state is not null)
        {
            var previous = (await state.ListSyncUploadCheckpointsAsync(UploadScope(target,token), cancellationToken)).SingleOrDefault(item=>item.TargetUri==target.AbsoluteUri);
            if (previous is not null && (previous.Fingerprint != fingerprint || currentEtag == expectedEtag))
                await CancelCheckpointAsync(previous, token, cancellationToken);
        }
        if (string.Equals(currentEtag, expectedEtag, StringComparison.Ordinal)) return expectedEtag;
        var davPrefix = target.AbsolutePath.StartsWith("/dav/sync/", StringComparison.Ordinal) ? "/dav/"
            : target.AbsolutePath.StartsWith("/webdav/sync/", StringComparison.Ordinal) ? "/webdav/" : null;
        if (source.Length >= 1024 * 1024 && davPrefix is not null)
        {
            var resumed = await UploadResumableAsync(target, target.AbsolutePath[davPrefix.Length..], token, source, digest, currentEtag, cancellationToken);
            if (resumed is not null) return resumed;
            source.Position = 0;
        }
        using var request = new HttpRequestMessage(HttpMethod.Put, target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        request.Headers.TryAddWithoutValidation(currentEtag is null ? "If-None-Match" : "If-Match", currentEtag ?? "*");
        request.Headers.TryAddWithoutValidation("X-Content-Sha256", digest);
        request.Content = new StreamContent(source);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        var receipt = response.Headers.ETag?.Tag ?? await ReadEtagAsync(target, token, cancellationToken);
        if (receipt is null || !string.Equals(receipt, $"\"sha256-{digest}\"", StringComparison.Ordinal)) throw new InvalidDataException("Saturn did not confirm the uploaded file checksum.");
        return receipt;
    }

    private sealed record UploadCheckpoint(string Id, string Status, long ReceivedSize, long ExpectedSize, string? Etag);

    private async Task<string?> UploadResumableAsync(Uri target, string logicalPath, string token, FileStream source, string digest, string? currentEtag, CancellationToken cancellationToken)
    {
        var endpoint = new Uri(target, "/api/v1/sync/uploads");
        var scope = UploadScope(target, token);
        var fingerprint = digest + "\n" + source.Length + "\n" + currentEtag;
        var key = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(target.AbsoluteUri + "\n" + fingerprint)));
        var saved = state is null ? null : (await state.ListSyncUploadCheckpointsAsync(scope,cancellationToken)).SingleOrDefault(item=>item.TargetUri==target.AbsoluteUri);
        key = saved?.IdempotencyKey ?? (state is null ? key : Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(key+"\n"+Guid.NewGuid().ToString("N")))));
        // Persist before create: even losing the create response leaves a key
        // with which this device can cancel the old session after a restart.
        saved = new SyncUploadCheckpoint(scope,target.AbsoluteUri,key,fingerprint,saved?.UploadId);
        if (state is not null) await state.SaveSyncUploadCheckpointAsync(saved,cancellationToken);
        using var create = new HttpRequestMessage(HttpMethod.Post, endpoint);
        create.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        create.Content = new StringContent(JsonSerializer.Serialize(new { path = logicalPath, expectedSize = source.Length, expectedSha256 = digest, idempotencyKey = key, ifMatch = currentEtag, ifNoneMatch = currentEtag is null ? "*" : null },
            new JsonSerializerOptions { DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull }), Encoding.UTF8, "application/json");
        using var created = await httpClient.SendAsync(create, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        // Older Saturn releases have no resumable endpoint.
        if (created.StatusCode == HttpStatusCode.NotFound)
        {
            if (state is not null) await state.RemoveSyncUploadCheckpointAsync(saved,cancellationToken);
            return null;
        }
        if (created.StatusCode == HttpStatusCode.Conflict && state is not null)
        {
            // Expired or terminal sessions cannot be revived under their old
            // key. Confirm cancellation, then the next attempt gets a new key.
            await CancelCheckpointAsync(saved,token,cancellationToken);
        }
        created.EnsureSuccessStatusCode();
        var checkpoint = await ReadCheckpointAsync(created, source.Length, cancellationToken);
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
            using var appended = await httpClient.SendAsync(append, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            appended.EnsureSuccessStatusCode();
            var next = await ReadCheckpointAsync(appended, source.Length, cancellationToken);
            if (next.Id != checkpoint.Id || next.ReceivedSize != checkpoint.ReceivedSize + count) throw new InvalidDataException("Saturn returned an invalid upload checkpoint.");
            checkpoint = next;
        }
        if (checkpoint.Status != "active")
        {
            using var complete = new HttpRequestMessage(HttpMethod.Post, new Uri(uploadUri.AbsoluteUri + "/complete"));
            complete.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
            using var completed = await httpClient.SendAsync(complete, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            completed.EnsureSuccessStatusCode();
            checkpoint = await ReadCheckpointAsync(completed, source.Length, cancellationToken);
        }
        if (checkpoint.Status != "active" || !string.Equals(checkpoint.Etag, $"\"sha256-{digest}\"", StringComparison.Ordinal))
            throw new InvalidDataException("Saturn did not confirm the completed file checksum.");
        if (state is not null) await state.RemoveSyncUploadCheckpointAsync(saved,cancellationToken);
        return checkpoint.Etag;
    }

    private static string UploadScope(Uri target, string token) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(target.GetLeftPart(UriPartial.Authority)+"\n"+token.Trim())));

    private async Task CancelCheckpointAsync(SyncUploadCheckpoint checkpoint, string token, CancellationToken cancellationToken)
    {
        using var cancel = new HttpRequestMessage(HttpMethod.Post,new Uri(new Uri(checkpoint.TargetUri),"/api/v1/sync/uploads/cancel"));
        cancel.Headers.Authorization = new AuthenticationHeaderValue("Bearer",token.Trim());
        cancel.Content = new StringContent(JsonSerializer.Serialize(new { idempotencyKey=checkpoint.IdempotencyKey }),Encoding.UTF8,"application/json");
        using var response = await httpClient.SendAsync(cancel,HttpCompletionOption.ResponseHeadersRead,cancellationToken);
        response.EnsureSuccessStatusCode();
        using var document = await BoundedJson.ReadAsync(response.Content,4096,cancellationToken);
        var status = document.RootElement.GetProperty("status").GetString();
        if (status is not ("absent" or "abandoned" or "active")) throw new InvalidDataException("Saturn did not confirm checkpoint cancellation.");
        if (state is not null) await state.RemoveSyncUploadCheckpointAsync(checkpoint,cancellationToken);
    }

    public async Task CancelMissingUploadsAsync(Uri mappingRoot, string token, IReadOnlySet<string> localFiles, CancellationToken cancellationToken = default)
    {
        if (state is null) return;
        var root = EnsureSlash(mappingRoot);
        foreach (var checkpoint in await state.ListSyncUploadCheckpointsAsync(UploadScope(root,token),cancellationToken))
        {
            var target = new Uri(checkpoint.TargetUri);
            if (!root.IsBaseOf(target)) continue;
            var relative = Uri.UnescapeDataString(root.MakeRelativeUri(target).OriginalString);
            if (!localFiles.Contains(relative)) await CancelCheckpointAsync(checkpoint,token,cancellationToken);
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

    public async Task<int> MirrorAsync(Uri mappingRoot, string token, IReadOnlySet<string> localFiles, IReadOnlySet<string> localDirectories, bool protectMassDeletion = true, CancellationToken cancellationToken = default)
    {
        var remote = await EnumerateAsync(mappingRoot, token, "", cancellationToken);
        var removedFiles = remote.Where(value => !value.IsCollection && !localFiles.Contains(value.RelativePath)).ToArray();
        var removedDirectories = remote.Where(value => value.IsCollection && !localDirectories.Contains(value.RelativePath)).OrderByDescending(value => value.RelativePath.Count(c => c == '/')).ToArray();
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
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound) return null;
        response.EnsureSuccessStatusCode();
        return response.Headers.ETag?.Tag;
    }

    private async Task EnsureCollectionsAsync(Uri root, string token, IEnumerable<string> segments, CancellationToken cancellationToken)
    {
        var current = EnsureSlash(root);
        foreach (var segment in segments)
        {
            current = new Uri(current, segment + "/");
            using var request = new HttpRequestMessage(new HttpMethod("MKCOL"), current);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
            using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            if (response.StatusCode is not (HttpStatusCode.Created or HttpStatusCode.MethodNotAllowed or HttpStatusCode.Conflict))
                response.EnsureSuccessStatusCode();
        }
    }

    private async Task<bool> CollectionExistsAsync(Uri target, string token, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(new HttpMethod("PROPFIND"), target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        request.Headers.TryAddWithoutValidation("Depth", "0");
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound) return false;
        if ((int)response.StatusCode == 207) return true;
        response.EnsureSuccessStatusCode();
        return true;
    }

    private async Task<string?> ReadTextAsync(Uri target, string token, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound) return null;
        response.EnsureSuccessStatusCode();
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

    private async Task<IReadOnlyList<RemoteEntry>> EnumerateAsync(Uri directory, string token, string prefix, CancellationToken cancellationToken)
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
            using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, requestDeadline.Token);
            response.EnsureSuccessStatusCode();
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
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        if (response.StatusCode != HttpStatusCode.NotFound) response.EnsureSuccessStatusCode();
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
        new[] { clientInstanceId, mappingId }
            .Concat(relativePath.Replace('\\', '/').Split('/', StringSplitOptions.RemoveEmptyEntries))
            .Select(Uri.EscapeDataString).ToArray();

    private sealed record SaturnPreferences(string AccentColor);
    private sealed record RemoteEntry(Uri Uri, string RelativePath, bool IsCollection);
}
