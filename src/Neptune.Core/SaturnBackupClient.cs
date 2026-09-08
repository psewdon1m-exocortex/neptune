using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;

namespace Neptune.Core;

public sealed class SaturnBackupClient(HttpClient httpClient, int chunkSizeBytes = 4 * 1024 * 1024)
{
    public async Task<BackupReceipt> UploadAsync(
        Uri backupBaseUri,
        string serviceSlug,
        string token,
        BackupRun run,
        string idempotencyKey,
        string sourceVersion,
        CancellationToken cancellationToken = default)
    {
        if (run.SpoolPath is null || run.Size is null || run.Sha256 is null)
            throw new InvalidOperationException("Backup run is not spooled.");

        var normalizedBaseUri = EnsureTrailingSlash(backupBaseUri);
        var capabilities = await httpClient.GetFromJsonAsync<SaturnBackupCapabilities>(new Uri(normalizedBaseUri, "capabilities"), cancellationToken)
            ?? throw new InvalidDataException("Saturn returned empty backup capabilities.");
        if (capabilities.Schema != "saturn.backup-ingest.capabilities.v1" || capabilities.ProtocolVersion != 1 || !capabilities.Resumable || capabilities.Checksum != "sha256" || capabilities.MaxChunkBytes < 1)
            throw new InvalidDataException("Saturn backup capabilities are incompatible with Neptune.");
        var actualChunkSize = Math.Min(chunkSizeBytes, capabilities.MaxChunkBytes);
        var serviceUri = new Uri(normalizedBaseUri, Uri.EscapeDataString(serviceSlug) + "/");
        using var create = new HttpRequestMessage(HttpMethod.Post, new Uri(serviceUri, "runs"));
        Authorize(create, token);
        create.Headers.TryAddWithoutValidation("Idempotency-Key", idempotencyKey);
        create.Content = JsonContent.Create(new
        {
            filename = $"{run.ProjectId}-{run.CreatedAt:yyyyMMddTHHmmssZ}.zip",
            createdAt = run.CreatedAt.ToString("O"),
            backupType = "full",
            expectedSize = run.Size.Value,
            sha256 = run.Sha256,
            sourceVersion,
            encrypted = false
        });
        using var createResponse = await httpClient.SendAsync(create, cancellationToken);
        createResponse.EnsureSuccessStatusCode();
        var created = await createResponse.Content.ReadFromJsonAsync<SaturnRunCreated>(cancellationToken: cancellationToken)
            ?? throw new InvalidDataException("Saturn returned an empty backup run response.");

        var uploadUri = new Uri(serviceUri, $"runs/{Uri.EscapeDataString(created.Id)}/upload");
        var offset = await ReadOffsetAsync(uploadUri, token, cancellationToken);
        if (offset < 0 || offset > run.Size.Value)
            throw new InvalidDataException("Saturn returned an invalid upload offset.");

        await using var file = new FileStream(run.SpoolPath, FileMode.Open, FileAccess.Read, FileShare.Read, actualChunkSize, FileOptions.Asynchronous | FileOptions.SequentialScan);
        file.Position = offset;
        var buffer = new byte[actualChunkSize];
        while (offset < run.Size.Value)
        {
            var count = await file.ReadAsync(buffer.AsMemory(0, (int)Math.Min(buffer.Length, run.Size.Value - offset)), cancellationToken);
            if (count == 0) throw new EndOfStreamException("Backup spool ended before its declared size.");
            using var patch = new HttpRequestMessage(HttpMethod.Patch, uploadUri);
            Authorize(patch, token);
            patch.Headers.TryAddWithoutValidation("Upload-Offset", offset.ToString(System.Globalization.CultureInfo.InvariantCulture));
            patch.Content = new ByteArrayContent(buffer, 0, count);
            patch.Content.Headers.ContentType = new MediaTypeHeaderValue("application/offset+octet-stream");
            using var patchResponse = await httpClient.SendAsync(patch, cancellationToken);
            patchResponse.EnsureSuccessStatusCode();
            var expectedOffset = offset + count;
            offset = ParseOffset(patchResponse.Headers, expectedOffset);
            if (offset != expectedOffset) throw new InvalidDataException("Saturn acknowledged an unexpected upload offset.");
        }

        using var complete = new HttpRequestMessage(HttpMethod.Post, new Uri(serviceUri, $"runs/{Uri.EscapeDataString(created.Id)}/complete"));
        Authorize(complete, token);
        complete.Content = new ByteArrayContent([]);
        using var completeResponse = await httpClient.SendAsync(complete, cancellationToken);
        completeResponse.EnsureSuccessStatusCode();
        using var json = await JsonDocument.ParseAsync(await completeResponse.Content.ReadAsStreamAsync(cancellationToken), cancellationToken: cancellationToken);
        var receipt = json.RootElement.GetProperty("receipt");
        return JsonSerializer.Deserialize<BackupReceipt>(receipt, new JsonSerializerOptions(JsonSerializerDefaults.Web))
            ?? throw new InvalidDataException("Saturn completion response has no receipt.");
    }

    private async Task<long> ReadOffsetAsync(Uri uploadUri, string token, CancellationToken cancellationToken)
    {
        using var head = new HttpRequestMessage(HttpMethod.Head, uploadUri);
        Authorize(head, token);
        using var response = await httpClient.SendAsync(head, cancellationToken);
        response.EnsureSuccessStatusCode();
        return ParseOffset(response.Headers, 0);
    }

    private static long ParseOffset(HttpResponseHeaders headers, long fallback) =>
        headers.TryGetValues("Upload-Offset", out var values) && long.TryParse(values.SingleOrDefault(), out var parsed)
            ? parsed
            : fallback;

    private static void Authorize(HttpRequestMessage request, string token) =>
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());

    private static Uri EnsureTrailingSlash(Uri uri) => uri.AbsoluteUri.EndsWith("/", StringComparison.Ordinal)
        ? uri
        : new Uri(uri.AbsoluteUri + "/", UriKind.Absolute);
}
