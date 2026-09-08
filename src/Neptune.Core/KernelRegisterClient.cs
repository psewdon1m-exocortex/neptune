using System.Net.Http.Headers;
using System.Text.Json;

namespace Neptune.Core;

public sealed class KernelRegisterClient(HttpClient httpClient, string cachePath)
{
    public async Task<JsonDocument> GetSnapshotAsync(Uri kernelOrigin, string token, CancellationToken cancellationToken = default)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(kernelOrigin, "/api/v1/register/snapshot"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        await using var body = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var downloaded = await JsonDocument.ParseAsync(body, cancellationToken: cancellationToken);
        Validate(downloaded.RootElement);

        Directory.CreateDirectory(Path.GetDirectoryName(cachePath)!);
        var temporary = $"{cachePath}.{Guid.NewGuid():N}.tmp";
        await File.WriteAllTextAsync(temporary, downloaded.RootElement.GetRawText(), cancellationToken);
        File.Move(temporary, cachePath, overwrite: true);
        return JsonDocument.Parse(downloaded.RootElement.GetRawText());
    }

    public JsonDocument GetLastKnownGood()
    {
        using var cached = JsonDocument.Parse(File.ReadAllText(cachePath));
        Validate(cached.RootElement);
        return JsonDocument.Parse(cached.RootElement.GetRawText());
    }

    public static void Validate(JsonElement root)
    {
        if (root.GetProperty("schema").GetString() != "exocortex.register.snapshot.v1")
            throw new InvalidDataException("Unsupported Kernel Register snapshot schema.");
        var values = root.GetProperty("values");
        var expected = root.GetProperty("checksum").GetString();
        var actual = CanonicalJson.ComputeRegisterChecksum(values);
        if (!string.Equals(expected, actual, StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("Kernel Register snapshot checksum mismatch.");
    }
}
