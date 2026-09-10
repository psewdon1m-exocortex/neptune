using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
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
        using var response = await httpClient.SendAsync(request, cancellationToken);
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
            using var response = await httpClient.SendAsync(create, cancellationToken);
            if (response.StatusCode != HttpStatusCode.Created)
                throw new InvalidOperationException($"Saturn sync folder '{folderName}' was claimed by another client.");
        }
        using (var marker = new HttpRequestMessage(HttpMethod.Put, new Uri(target, OwnerMarker)))
        {
            marker.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
            marker.Headers.TryAddWithoutValidation("If-None-Match", "*");
            marker.Content = new StringContent(clientInstanceId, Encoding.UTF8, "text/plain");
            using var response = await httpClient.SendAsync(marker, cancellationToken);
            response.EnsureSuccessStatusCode();
        }
        return target;
    }

    public async Task<string> ReadAccentAsync(Uri preferencesUri, string token, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, preferencesUri);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        using var response = await httpClient.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();
        var value = await response.Content.ReadFromJsonAsync<SaturnPreferences>(cancellationToken)
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
        using var response = await httpClient.SendAsync(request, cancellationToken);
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
        using var response = await httpClient.SendAsync(request, cancellationToken);
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
            using var response = await httpClient.SendAsync(request, cancellationToken);
            if (response.StatusCode is not (HttpStatusCode.Created or HttpStatusCode.MethodNotAllowed or HttpStatusCode.Conflict))
                response.EnsureSuccessStatusCode();
        }
    }

    private async Task<bool> CollectionExistsAsync(Uri target, string token, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(new HttpMethod("PROPFIND"), target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        request.Headers.TryAddWithoutValidation("Depth", "0");
        using var response = await httpClient.SendAsync(request, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound) return false;
        if ((int)response.StatusCode == 207) return true;
        response.EnsureSuccessStatusCode();
        return true;
    }

    private async Task<string?> ReadTextAsync(Uri target, string token, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        using var response = await httpClient.SendAsync(request, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound) return null;
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStringAsync(cancellationToken);
    }

    private async Task<IReadOnlyList<RemoteEntry>> EnumerateAsync(Uri directory, string token, string prefix, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(new HttpMethod("PROPFIND"), directory);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        request.Headers.TryAddWithoutValidation("Depth", "1");
        using var response = await httpClient.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();
        var xml = XDocument.Parse(await response.Content.ReadAsStringAsync(cancellationToken));
        XNamespace dav = "DAV:";
        var result = new List<RemoteEntry>();
        foreach (var item in xml.Descendants(dav + "response").Skip(1))
        {
            var href = item.Element(dav + "href")?.Value ?? throw new InvalidDataException("WebDAV response has no href.");
            var name = item.Descendants(dav + "displayname").FirstOrDefault()?.Value ?? throw new InvalidDataException("WebDAV response has no displayname.");
            var child = new Uri(directory, href);
            if (!child.AbsolutePath.StartsWith(directory.AbsolutePath, StringComparison.Ordinal))
                throw new InvalidDataException("WebDAV returned an entry outside the synchronized folder.");
            var relative = string.IsNullOrEmpty(prefix) ? name : $"{prefix}/{name}";
            var collection = item.Descendants(dav + "collection").Any();
            result.Add(new RemoteEntry(child, relative, collection));
            if (collection) result.AddRange(await EnumerateAsync(EnsureSlash(child), token, relative, cancellationToken));
        }
        return result;
    }

    private async Task DeleteAsync(Uri target, string token, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Delete, target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        using var response = await httpClient.SendAsync(request, cancellationToken);
        if (response.StatusCode != HttpStatusCode.NotFound) response.EnsureSuccessStatusCode();
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
