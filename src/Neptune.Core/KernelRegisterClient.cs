using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Security.Cryptography;
using System.Text;

namespace Neptune.Core;

public sealed class KernelRegisterClient(HttpClient httpClient, string cachePath)
{
    private static readonly Regex VoltReference = new(
        "^volt://[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/[1-5]$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private sealed record MetadataCache(string Binding, DateTimeOffset SavedAt, string Snapshot);
    private static readonly HashSet<string> CacheableKeys = ["services.saturn.sni", "services.saturn.port", "services.saturn.paths.sync", "services.saturn.paths.sync_preferences"];

    public async Task<JsonDocument> GetSnapshotAsync(Uri kernelOrigin, string token, CancellationToken cancellationToken = default, IReadOnlyCollection<string>? requestedKeys = null, bool allowCachedMetadata = false)
    {
        var callerCancellation = cancellationToken;
        var cacheAllowed = allowCachedMetadata && requestedKeys is { Count: > 0 } && requestedKeys.All(CacheableKeys.Contains);
        var binding = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(kernelOrigin.AbsoluteUri + "\0" + token + "\0" + string.Join('\n', (requestedKeys ?? Array.Empty<string>()).Order(StringComparer.Ordinal)))));
        var metadataPath = cachePath + ".resolved.json";
        var discoveryComplete = false;
        try {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(15));
        cancellationToken = deadline.Token;
        using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(kernelOrigin, "/api/v1/register/snapshot"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        using var downloaded = await BoundedJson.ReadAsync(response.Content, 1024 * 1024, cancellationToken);
        Validate(downloaded.RootElement);

        Directory.CreateDirectory(Path.GetDirectoryName(cachePath)!);
        var temporary = $"{cachePath}.{Guid.NewGuid():N}.tmp";
        await File.WriteAllTextAsync(temporary, downloaded.RootElement.GetRawText(), cancellationToken);
        File.Move(temporary, cachePath, overwrite: true);
        var resolved = await ResolveAsync(kernelOrigin, token, downloaded.RootElement.GetRawText(), requestedKeys, cancellationToken);
        discoveryComplete = true;
        if (cacheAllowed)
        {
            var cacheTemporary = $"{metadataPath}.{Guid.NewGuid():N}.tmp";
            try { await File.WriteAllTextAsync(cacheTemporary, JsonSerializer.Serialize(new MetadataCache(binding, DateTimeOffset.UtcNow, resolved.RootElement.GetRawText())), cancellationToken); File.Move(cacheTemporary,metadataPath,overwrite:true); }
            catch { resolved.Dispose(); throw; }
            finally { if(File.Exists(cacheTemporary)) File.Delete(cacheTemporary); }
        }
        return resolved;
        }
        catch (HttpRequestException error) when (error.StatusCode is System.Net.HttpStatusCode.Unauthorized or System.Net.HttpStatusCode.Forbidden)
        {
            if (cacheAllowed && File.Exists(metadataPath)) File.Delete(metadataPath);
            throw;
        }
        catch (Exception error) when (cacheAllowed && !discoveryComplete && !callerCancellation.IsCancellationRequested && RegisterFallback.CanUse(error))
        {
            if (!File.Exists(metadataPath) || new FileInfo(metadataPath).Length > 1024 * 1024) throw;
            var saved = JsonSerializer.Deserialize<MetadataCache>(await File.ReadAllTextAsync(metadataPath, callerCancellation));
            if (saved is null || saved.Binding != binding || saved.SavedAt > DateTimeOffset.UtcNow || DateTimeOffset.UtcNow-saved.SavedAt > TimeSpan.FromHours(24)) throw;
            return JsonDocument.Parse(saved.Snapshot);
        }
    }

    private async Task<JsonDocument> ResolveAsync(Uri kernelOrigin, string token, string rawSnapshot, IReadOnlyCollection<string>? requestedKeys, CancellationToken cancellationToken)
    {
        var root = JsonNode.Parse(rawSnapshot)?.AsObject() ?? throw new InvalidDataException("Invalid Kernel Register snapshot.");
        var values = root["values"]?.AsObject() ?? throw new InvalidDataException("Kernel Register values are missing.");
        var references = new Dictionary<string, string>(StringComparer.Ordinal);
        CollectReferences(values, "", references);
        // Optional keys can be absent in older Registers; consumers validate their required values.
        var keys = references.Keys.Where(key => requestedKeys is null || requestedKeys.Contains(key))
            .OrderBy(key => key, StringComparer.Ordinal).ToArray();
        var selectedValues = new JsonObject();
        for (var offset = 0; offset < keys.Length; offset += 20)
        {
            var batch = keys.Skip(offset).Take(20).ToArray();
            using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(kernelOrigin, "/api/v1/register/resolve"));
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Content = JsonContent.Create(new { keys = batch });
            using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            response.EnsureSuccessStatusCode();
            using var resolved = await BoundedJson.ReadAsync(response.Content, 1024 * 1024, cancellationToken);
            if (resolved.RootElement.GetProperty("schema").GetString() != "exocortex.register.resolution.v1")
                throw new InvalidDataException("Unsupported Kernel resolution schema.");
            foreach (var key in batch)
            {
                var value = resolved.RootElement.GetProperty("values").GetProperty(key).GetProperty("value").GetString()
                    ?? throw new InvalidDataException($"Kernel omitted Register key {key}.");
                SetDotted(selectedValues, key, value);
            }
        }
        root["values"] = selectedValues;
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
        foreach (var part in parts[..^1])
        {
            current[part] ??= new JsonObject();
            current = current[part]!.AsObject();
        }
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
