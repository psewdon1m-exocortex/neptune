using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Xml;
using System.Text;
using System.Xml.Linq;

namespace Neptune.Core;

public sealed class WebDavSyncClient(HttpClient httpClient)
{
    private const string OwnerMarker = "_neptune-owner";

    public async Task<string?> UploadAsync(Uri syncBaseUri, string token, string clientInstanceId, string mappingId, string relativePath, string localPath, CancellationToken cancellationToken = default)
    {
        var segments = TargetSegments(clientInstanceId, mappingId, relativePath);
        await EnsureCollectionsAsync(syncBaseUri, token, segments[..^1], cancellationToken);
        var target = new Uri(EnsureSlash(syncBaseUri), string.Join('/', segments));
        var currentEtag = await ReadEtagAsync(target, token, cancellationToken);
        using var request = new HttpRequestMessage(HttpMethod.Put, target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        request.Headers.TryAddWithoutValidation(currentEtag is null ? "If-None-Match" : "If-Match", currentEtag ?? "*");
        request.Content = new StreamContent(new FileStream(localPath, FileMode.Open, FileAccess.Read, FileShare.Read, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan));
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        return response.Headers.ETag?.Tag ?? await ReadEtagAsync(target, token, cancellationToken);
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
        var currentEtag = await ReadEtagAsync(target, token, cancellationToken);
        using var request = new HttpRequestMessage(HttpMethod.Put, target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        request.Headers.TryAddWithoutValidation(currentEtag is null ? "If-None-Match" : "If-Match", currentEtag ?? "*");
        request.Content = new StreamContent(new FileStream(localPath, FileMode.Open, FileAccess.Read, FileShare.Read, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan));
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        return response.Headers.ETag?.Tag ?? await ReadEtagAsync(target, token, cancellationToken);
    }

    public async Task<int> MirrorAsync(Uri mappingRoot, string token, IReadOnlySet<string> localFiles, IReadOnlySet<string> localDirectories, bool protectMassDeletion = true, CancellationToken cancellationToken = default)
    {
        var remote = await EnumerateAsync(mappingRoot, token, "", cancellationToken);
        var removedFiles = remote.Where(value => !value.IsCollection && !localFiles.Contains(value.RelativePath)).ToArray();
        var removedDirectories = remote.Where(value => value.IsCollection && !localDirectories.Contains(value.RelativePath)).OrderByDescending(value => value.RelativePath.Count(c => c == '/')).ToArray();
        var removed = removedFiles.Length + removedDirectories.Length;
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
