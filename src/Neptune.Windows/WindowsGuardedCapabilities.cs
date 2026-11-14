using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Windows;

internal sealed record WindowsGuardedCapability(SyncRootBinding Binding, string DeviceId,
    string BackendIdentity, string QualificationReceiptSha256, WindowsCycleBudget Budget,
    long MaximumFileBytes)
{
    public IReadOnlySet<string> ExtensionFeatures { get; init; }=new HashSet<string>();
    public SyncNamespacePolicy? Namespace { get; init; }
    public int NamespaceRevision { get; init; }=1;
}

// Negotiation is separate from snapshot/receipt parsing. Once a namespace has
// opted in, a downgrade or rejected admission must never turn into DAV writes.
internal static class WindowsGuardedCapabilities
{
    private const int MaximumBytes = 16 * 1024;
    private static readonly string[] Features = ["snapshot-v1", "changes-v1", "folder-v2", "catalog-upload-v2"];
    internal static string NamespaceKey(Uri origin, string deviceId, string rootId)
        => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(
            JsonSerializer.Serialize(new[] { origin.GetLeftPart(UriPartial.Authority), deviceId, rootId }))));

    internal static WindowsGuardedCapability Parse(ReadOnlyMemory<byte> raw, string deviceId, string rootId)
    {
        if (raw.Length > MaximumBytes) throw new InvalidDataException("Sync capability exceeds its fixed limit.");
        using var doc = SyncProtocolReader.Parse(raw); var body = doc.RootElement;
        var hasExtensions=body.TryGetProperty("extensions",out var extensions);
        Exact(body, hasExtensions?["schema", "binding", "device_id", "backend_identity", "qualification_receipt_sha256", "coverage", "features", "limits","extensions"]:["schema", "binding", "device_id", "backend_identity", "qualification_receipt_sha256", "coverage", "features", "limits"]);
        if (Text(body, "schema") != "saturn.sync-capabilities.v1"
            || !(Text(body, "coverage") == "append-only-directories/file-cas"
                || hasExtensions && Text(body, "coverage") == "namespace-mutations-v1")
            || Text(body, "device_id") != deviceId || !Uuid(deviceId) || !Uuid(rootId))
            throw new InvalidDataException("Sync capability is not the assigned supported scope.");
        var bindingJson = body.GetProperty("binding");
        Exact(bindingJson, "protocol", "descriptor_schema", "server_identity", "root_id", "server_epoch", "policy_version", "binding_version");
        var binding = SyncProtocolReader.Binding(bindingJson);
        if (binding.RootId != rootId) throw new InvalidDataException("Sync capability changed the assigned root.");
        var backend = Text(body, "backend_identity");
        var receipt = Text(body, "qualification_receipt_sha256");
        if (backend.Length is < 1 or > 128 || backend.Any(c => c is < '!' or > '~')
            || receipt.Length != 64 || receipt.Any(c => c is not (>= '0' and <= '9' or >= 'a' and <= 'f')))
            throw new InvalidDataException("Sync capability identity is invalid.");
        var features = body.GetProperty("features");
        var names=features.ValueKind==JsonValueKind.Array?features.EnumerateArray().Select(v=>v.ValueKind==JsonValueKind.String?v.GetString()!:"").ToArray():[];
        string[] optional=["changes-wait-v1","folder-batch-v1","namespace-mutations-v1","stream-upload-v1","metadata-gzip-v1","clone-content-v1","scale-transfer-v2"];
        var scaled=names.Contains("scale-transfer-v2",StringComparer.Ordinal);
        if(scaled&&!names.Contains("stream-upload-v1",StringComparer.Ordinal))throw new InvalidDataException("Parallel stream requires base stream capability.");
        if (names.Length!=names.Distinct(StringComparer.Ordinal).Count()||Features.Any(f=>!names.Contains(f,StringComparer.Ordinal))||names.Any(f=>!Features.Contains(f,StringComparer.Ordinal)&&(!hasExtensions||!optional.Contains(f,StringComparer.Ordinal))))
            throw new InvalidDataException("Required sync features are missing, duplicated or unsupported.");
        SyncNamespacePolicy? policy=null;
        if(hasExtensions)
        {
            Exact(extensions,"revisions","namespace","minimum_writer","backend_policy","limits");var revisions=extensions.GetProperty("revisions");Exact(revisions,"K1","K2","K3","K4");
            if(revisions.GetProperty("K1").GetInt32() is not(1 or 2)||revisions.GetProperty("K2").GetInt32()!=2||revisions.GetProperty("K3").GetInt32()!=2||revisions.GetProperty("K4").GetInt32()!=1
                ||Text(extensions,"minimum_writer")!="saturn.namespace-mutation.v1"||Text(extensions,"backend_policy")!="managed-exclusive")throw new InvalidDataException("Unsupported sync extension revision/writer policy.");
            var ns=extensions.GetProperty("namespace");Exact(ns,"codec","policyHash");policy=new(Text(ns,"codec"),Text(ns,"policyHash"));
            if(policy.Codec is not("legacy" or "portable_v1")||policy.PolicyHash.Length!=64||policy.PolicyHash.Any(c=>c is not(>= '0' and <= '9' or >= 'a' and <= 'f')))throw new InvalidDataException("Invalid namespace capability.");
            var bounds=extensions.GetProperty("limits");Exact(bounds,"metadata_items","metadata_bytes","long_poll_ms","gzip_threshold_bytes","stream_chunk_bytes","stream_maximum_chunks");
            if(bounds.GetProperty("metadata_items").GetInt32()!=500||bounds.GetProperty("metadata_bytes").GetInt32()!=4194304||bounds.GetProperty("long_poll_ms").GetInt32()!=25000||bounds.GetProperty("gzip_threshold_bytes").GetInt32()!=16384||bounds.GetProperty("stream_chunk_bytes").GetInt32()!=4194304||bounds.GetProperty("stream_maximum_chunks").GetInt64()!=(scaled?2147483648L:256))throw new InvalidDataException("Unsupported sync extension resource bounds.");
        }
        var limits = body.GetProperty("limits");
        Exact(limits, "data_jobs", "large_jobs", "spool_bytes", "maximum_file_bytes");
        var dataJobs = limits.GetProperty("data_jobs").GetInt32();
        var largeJobs = limits.GetProperty("large_jobs").GetInt32();
        var spool = Decimal(Text(limits, "spool_bytes"));
        var fileMax = Decimal(Text(limits, "maximum_file_bytes"));
        if (dataJobs<1||dataJobs>(scaled?8:4) || largeJobs is < 1 or > 2 || largeJobs > dataJobs
            || spool is < 1 or > 1024L * 1024 * 1024 || fileMax < 1 || fileMax > (scaled?9007199254740991L:spool))
            throw new InvalidDataException("Sync capability requests resources outside client ceilings.");
        // A lowered admission limit applies to new capture. Retained originals
        // still need the negotiated physical spool budget for recovery.
        return new(binding, deviceId, backend, receipt,
            new(DataJobs: dataJobs, LargeJobs: largeJobs, SpoolBytes: spool, MaximumNewFileBytes: fileMax), fileMax)
            {Namespace=policy,NamespaceRevision=hasExtensions?extensions.GetProperty("revisions").GetProperty("K1").GetInt32():1,ExtensionFeatures=new HashSet<string>(names.Where(f=>optional.Contains(f,StringComparer.Ordinal)),StringComparer.Ordinal)};
    }

    internal static async Task<HttpResponseMessage> ReadWhilePublishingAsync(HttpClient http, Uri uri, string token, CancellationToken ct)
    {
        var elapsed = System.Diagnostics.Stopwatch.StartNew();
        while (true)
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, uri);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            var response = await SyncHttpPolicy.SendAsync(http, request, HttpCompletionOption.ResponseHeadersRead, ct);
            try
            {
                if (response.RequestMessage?.RequestUri != uri || (int)response.StatusCode is >= 300 and < 400)
                    throw new InvalidDataException("Sync capability redirected outside its original request.");
                if (response.StatusCode != HttpStatusCode.ServiceUnavailable) return response;
                if (response.Content.Headers.ContentLength > 4096) return response;
                await using var body = await response.Content.ReadAsStreamAsync(ct);
                var bytes = new byte[4097]; var count = 0;
                while (count < bytes.Length) { var n = await body.ReadAsync(bytes.AsMemory(count), ct); if (n == 0) break; count += n; }
                var pending = false;
                if (count <= 4096) try {
                    using var json = SyncProtocolReader.Parse(bytes.AsMemory(0,count));
                    var value = json.RootElement;
                    pending = value.ValueKind == JsonValueKind.Object
                        && value.EnumerateObject().All(p => p.Name is "code" or "retry_after_ms")
                        && value.TryGetProperty("code", out var code) && code.ValueKind == JsonValueKind.String
                        && code.GetString() == "publication_pending";
                } catch (SyncWireException) { }
                // Ordinary 409, changed authority and unrelated 503 retain
                // their existing fail-closed behavior. This loop admits no work.
                if (!pending) { var old = response.Content; response.Content = new ByteArrayContent(bytes.AsSpan(0,Math.Min(count,4096)).ToArray()); old.Dispose(); return response; }
                if (elapsed.Elapsed >= TimeSpan.FromMinutes(10)) throw new TimeoutException("Server publication remains pending; profile and originals retained.");
            }
            catch { response.Dispose(); throw; }
            response.Dispose();
            await Task.Delay(TimeSpan.FromSeconds(2), ct);
        }
    }

    internal static async Task<WindowsGuardedCapability?> NegotiateAsync(HttpClient safeHttp, Uri origin,
        string token, string deviceId, string rootId, string stateDirectory, CancellationToken ct)
    {
        if (!Uuid(deviceId) || !Uuid(rootId) || token.Length != 43)
            throw new InvalidDataException("Guarded negotiation requires the actual assigned device and root.");
        var key = NamespaceKey(origin, deviceId, rootId);
        var root = Path.Combine(stateDirectory, "guarded-sync");
        var marker = Path.Combine(root, key + ".json");
        using var profileLease = WindowsGuardedStateLease.Open(stateDirectory);
        RequirePlainAncestors(stateDirectory);
        RequirePlainAncestors(marker);
        var enrolled = File.Exists(marker);
        if (!enrolled && Directory.Exists(Path.Combine(root, key)))
            throw new InvalidDataException("Guarded state exists without its enrollment marker; legacy fallback is refused.");
        if (enrolled) RequireMarker(marker, origin, deviceId, rootId);
        var uri = new Uri(origin, "/sync/v1/roots/" + rootId + "/capabilities");
        using var response = await ReadWhilePublishingAsync(safeHttp, uri, token, ct);
        profileLease.RequireCurrent();
        if (response.RequestMessage?.RequestUri != uri)
            throw new InvalidDataException("Sync capability redirected outside its original request.");
        if ((int)response.StatusCode is >= 300 and < 400)
            throw new InvalidDataException("Sync capability redirects are refused.");
        if (response.StatusCode == HttpStatusCode.NotFound && !enrolled) return null;
        await NetworkFailure.EnsureLegacySuccessAsync(response, ct);
        if (response.Content.Headers.ContentLength > MaximumBytes) throw new InvalidDataException("Sync capability exceeds its fixed limit.");
        await using var input = await response.Content.ReadAsStreamAsync(ct);
        using var bytes = new MemoryStream(); var buffer = new byte[4096];
        while (true)
        {
            var count = await input.ReadAsync(buffer, ct); if (count == 0) break;
            if (bytes.Length > MaximumBytes - count) throw new InvalidDataException("Sync capability exceeds its fixed limit.");
            bytes.Write(buffer, 0, count);
        }
        var result = Parse(bytes.ToArray(), deviceId, rootId);
        profileLease.RequireCurrent();
        using var markerLease = WindowsGuardedStateLease.Open(root, create: true);
        if (!enrolled)
        {
            var raw = JsonSerializer.SerializeToUtf8Bytes(new { schema = "neptune.guarded-enrollment.v1",
                origin = origin.GetLeftPart(UriPartial.Authority), device_id = deviceId, root_id = rootId });
            // This marker precedes writable migration and the first modern effect.
            var temp = marker + "." + Guid.NewGuid().ToString("N") + ".partial";
            try
            {
                markerLease.RequireCurrent();
                await using (var file = new FileStream(temp, FileMode.CreateNew, FileAccess.Write, FileShare.None))
                { await file.WriteAsync(raw, ct); file.Flush(true); }
                markerLease.RequireCurrent();
                try { File.Move(temp, marker); }
                catch (IOException) when (File.Exists(marker)) { RequireMarker(marker, origin, deviceId, rootId); }
            }
            finally { if (File.Exists(temp)) File.Delete(temp); }
        }
        return result;
    }

    private static void RequireMarker(string file, Uri origin, string deviceId, string rootId)
    {
        RequirePlainAncestors(file);
        if (new FileInfo(file).Length > MaximumBytes) throw new InvalidDataException("Guarded enrollment marker is invalid.");
        using var doc = SyncProtocolReader.Parse(File.ReadAllBytes(file)); var body = doc.RootElement;
        Exact(body, "schema", "origin", "device_id", "root_id");
        if (Text(body, "schema") != "neptune.guarded-enrollment.v1"
            || Text(body, "origin") != origin.GetLeftPart(UriPartial.Authority)
            || Text(body, "device_id") != deviceId || Text(body, "root_id") != rootId)
            throw new InvalidDataException("Guarded enrollment marker cannot be rebound or ignored.");
    }
    internal static void RequirePlainAncestors(string path)
    {
        var current = Path.GetFullPath(path);
        while (!string.IsNullOrEmpty(current))
        {
            try
            {
                if ((File.GetAttributes(current) & FileAttributes.ReparsePoint) != 0)
                    throw new InvalidDataException("Guarded state cannot use a reparse path.");
            }
            catch (FileNotFoundException) { }
            catch (DirectoryNotFoundException) { }
            current = Path.GetDirectoryName(current)!;
        }
    }
    private static void Exact(JsonElement body, params string[] fields)
    {
        if (body.ValueKind != JsonValueKind.Object || body.EnumerateObject().Count() != fields.Length
            || body.EnumerateObject().Any(v => !fields.Contains(v.Name, StringComparer.Ordinal)))
            throw new InvalidDataException("Sync capability or durable enrollment has an invalid shape.");
    }
    private static string Text(JsonElement body, string field) => body.GetProperty(field).ValueKind == JsonValueKind.String
        ? body.GetProperty(field).GetString()! : throw new InvalidDataException("Expected a sync string.");
    private static bool Uuid(string value) => value.Length == 36 && Guid.TryParseExact(value, "D", out _)
        && value == value.ToLowerInvariant();
    private static long Decimal(string value)
    {
        if (value.Length is < 1 or > 19 || value.Length > 1 && value[0] == '0'
            || value.Any(c => c is < '0' or > '9') || !long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var result))
            throw new InvalidDataException("Expected an exact nonnegative decimal sync limit.");
        return result;
    }
}
