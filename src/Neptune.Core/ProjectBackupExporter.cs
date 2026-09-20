using System.Globalization;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Cryptography;

namespace Neptune.Core;

public sealed record ExportArtifactReceipt(long Generation, long Size, string Sha256);

public sealed class ProjectBackupExporter(HttpClient httpClient)
{
    private const long MaximumBytes = 8L * 1024 * 1024 * 1024;

    public async Task<ExportArtifactReceipt?> ExportAsync(Uri exportUri, string token, string destinationPath,
        CancellationToken cancellationToken = default, string? purpose = null)
    {
        Validate(exportUri, purpose);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromHours(1));
        using var request = new HttpRequestMessage(HttpMethod.Post, exportUri);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        request.Headers.AcceptEncoding.ParseAdd("identity");
        if (purpose is not null) request.Headers.Add("X-Neptune-Purpose", purpose);
        using var headers = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
        headers.CancelAfter(TimeSpan.FromMinutes(5)); // snapshot copying and encryption precede the response
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, headers.Token);
        response.EnsureSuccessStatusCode();
        if (response.Content.Headers.ContentType?.MediaType is not "application/zip" || response.Content.Headers.ContentEncoding.Count != 0)
            throw new InvalidDataException("Project export has an unsupported content type or encoding.");
        var declared = response.Content.Headers.ContentLength;
        if (declared is < 1 or > MaximumBytes || purpose is not null && declared is null)
            throw new InvalidDataException("Project export has an invalid size.");
        ExportArtifactReceipt? receipt = null;
        if (purpose is not null)
        {
            var rawGeneration = SingleHeader(response, "X-Mastermind-Generation");
            var hash = SingleHeader(response, "X-Content-SHA256");
            if (!long.TryParse(rawGeneration, NumberStyles.None, CultureInfo.InvariantCulture, out var generation)
                || generation < 0 || hash.Length != 64 || hash.Any(c => !char.IsAsciiHexDigit(c)))
                throw new InvalidDataException("Project export receipt metadata is invalid.");
            receipt = new(generation, declared!.Value, hash.ToLowerInvariant());
        }
        await using var source = await response.Content.ReadAsStreamAsync(deadline.Token);
        await using var destination = new FileStream(destinationPath, FileMode.CreateNew, FileAccess.Write,
            FileShare.None, 256 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
        if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(destinationPath, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        using var digest = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = new byte[256 * 1024];
        long total = 0;
        while (true)
        {
            using var progress = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
            progress.CancelAfter(TimeSpan.FromSeconds(60));
            var count = await source.ReadAsync(buffer, progress.Token);
            if (count == 0) break;
            total += count;
            if (total > MaximumBytes || declared is not null && total > declared)
                throw new InvalidDataException("Project export exceeds its declared size.");
            digest.AppendData(buffer, 0, count);
            await destination.WriteAsync(buffer.AsMemory(0, count), deadline.Token);
        }
        var actual = Convert.ToHexStringLower(digest.GetHashAndReset());
        if (total == 0 || declared is not null && total != declared || receipt is not null && receipt.Sha256 != actual)
            throw new InvalidDataException("Project export is incomplete or its SHA-256 does not match.");
        await destination.FlushAsync(deadline.Token);
        destination.Flush(flushToDisk: true);
        return receipt;
    }

    public async Task AcknowledgeAsync(Uri exportUri, string token, string purpose, ExportArtifactReceipt receipt,
        CancellationToken cancellationToken = default)
    {
        Validate(exportUri, purpose);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(15));
        using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(exportUri.AbsoluteUri.TrimEnd('/') + "/receipt"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        request.Headers.Add("X-Neptune-Purpose", purpose);
        request.Content = JsonContent.Create(new { generation = receipt.Generation, size = receipt.Size, sha256 = receipt.Sha256 });
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, deadline.Token);
        response.EnsureSuccessStatusCode();
        using var body = await BoundedJson.ReadAsync(response.Content, 4096, deadline.Token);
        if (body.RootElement.GetProperty("accepted").ValueKind != System.Text.Json.JsonValueKind.True
            || body.RootElement.GetProperty("generation").GetInt64() != receipt.Generation)
            throw new InvalidDataException("Core did not accept the exact completed export receipt.");
    }

    private static string SingleHeader(HttpResponseMessage response, string name)
    {
        if (!response.Headers.TryGetValues(name, out var values)) throw new InvalidDataException("Project export metadata is missing.");
        var list = values.Take(2).ToArray();
        if (list.Length != 1 || list[0].Length > 128) throw new InvalidDataException("Project export metadata is invalid.");
        return list[0];
    }

    private static void Validate(Uri uri, string? purpose)
    {
        if (!uri.IsLoopback || uri.Scheme is not ("http" or "https") || !string.IsNullOrEmpty(uri.UserInfo)
            || !string.IsNullOrEmpty(uri.Query) || !string.IsNullOrEmpty(uri.Fragment)
            || purpose is not (null or "archive" or "mirror"))
            throw new InvalidDataException("Project export destination or purpose is invalid.");
    }
}

