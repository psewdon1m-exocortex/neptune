using System.Net;
using System.Net.Http.Headers;

namespace Neptune.Core;

public sealed class WebDavSyncClient(HttpClient httpClient)
{
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

    private static Uri EnsureSlash(Uri uri) => uri.AbsoluteUri.EndsWith("/", StringComparison.Ordinal)
        ? uri : new Uri(uri.AbsoluteUri + "/");

    private static string[] TargetSegments(string clientInstanceId, string mappingId, string relativePath) =>
        new[] { clientInstanceId, mappingId }
            .Concat(relativePath.Replace('\\', '/').Split('/', StringSplitOptions.RemoveEmptyEntries))
            .Select(Uri.EscapeDataString).ToArray();
}
