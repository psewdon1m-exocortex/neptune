using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Neptune.Core;

public sealed class KernelRegisterClient(HttpClient httpClient, string cachePath)
{
    private static readonly Regex VoltReference = new(
        "^volt://[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

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
        return await ResolveAsync(kernelOrigin, token, downloaded.RootElement.GetRawText(), cancellationToken);
    }

    private async Task<JsonDocument> ResolveAsync(Uri kernelOrigin, string token, string rawSnapshot, CancellationToken cancellationToken)
    {
        var root = JsonNode.Parse(rawSnapshot)?.AsObject() ?? throw new InvalidDataException("Invalid Kernel Register snapshot.");
        var values = root["values"]?.AsObject() ?? throw new InvalidDataException("Kernel Register values are missing.");
        var references = new Dictionary<string, string>(StringComparer.Ordinal);
        CollectReferences(values, "", references);
        var keys = references.Keys.OrderBy(key => key, StringComparer.Ordinal).ToArray();
        for (var offset = 0; offset < keys.Length; offset += 20)
        {
            var batch = keys.Skip(offset).Take(20).ToArray();
            using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(kernelOrigin, "/api/v1/register/resolve"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Content = JsonContent.Create(new { keys = batch });
            using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            response.EnsureSuccessStatusCode();
            await using var body = await response.Content.ReadAsStreamAsync(cancellationToken);
            using var resolved = await JsonDocument.ParseAsync(body, cancellationToken: cancellationToken);
            if (resolved.RootElement.GetProperty("schema").GetString() != "exocortex.register.resolution.v1")
                throw new InvalidDataException("Unsupported Kernel resolution schema.");
            foreach (var key in batch)
            {
                var value = resolved.RootElement.GetProperty("values").GetProperty(key).GetProperty("value").GetString()
                    ?? throw new InvalidDataException($"Kernel omitted Register key {key}.");
                SetDotted(values, key, value);
            }
        }
        return JsonDocument.Parse(root.ToJsonString());
    }

    private static void CollectReferences(JsonObject node, string prefix, IDictionary<string, string> output)
    {
        foreach (var (name, child) in node)
        {
            var key = string.IsNullOrEmpty(prefix) ? name : $"{prefix}.{name}";
            if (child is JsonObject nested) CollectReferences(nested, key, output);
            else if (child is JsonValue value && value.TryGetValue<string>(out var reference) && VoltReference.IsMatch(reference)) output[key] = reference;
            else throw new InvalidDataException($"Kernel Register key {key} is not mapped to Volt.");
        }
    }

    private static void SetDotted(JsonObject values, string key, string value)
    {
        var parts = key.Split('.');
        var current = values;
        foreach (var part in parts[..^1]) current = current[part]?.AsObject() ?? throw new InvalidDataException($"Kernel Register key {key} is invalid.");
        current[parts[^1]] = value;
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
