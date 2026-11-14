using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Neptune.Core;

internal sealed record BootstrapUploadReply(string Id, string Status, long ReceivedSize, long ExpectedSize, string? Etag,
    string? IntentHash, string? ReceiptState, SaturnUploadPublicationReceipt? PublicationReceipt, string? RawReceiptJson);

internal static class BootstrapUploadReceiptParser
{
    private static readonly string[] ReceiptFields = ["schema", "uploadId", "operationId", "intentHash", "rootId", "resourceId",
        "versionId", "parentId", "name", "sizeBytes", "sha256", "etag", "publishedAt"];
    internal static bool IsDigest(string? value) => value is { Length: 64 } && value.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f');
    internal static bool IsUuid(string? value) => value is { Length: 36 } && Guid.TryParseExact(value, "D", out _)
        && value[14] is >= '1' and <= '8' && char.ToLowerInvariant(value[19]) is '8' or '9' or 'a' or 'b';
    internal static bool SameUuid(string? a, string? b) => IsUuid(a) && IsUuid(b) && Guid.Parse(a!) == Guid.Parse(b!);

    internal static string RequestPath(Uri root, string relativePath)
    {
        var target = WebDavSyncClient.RelativeTargetUri(root, relativePath);
        var prefix = target.AbsolutePath.StartsWith("/dav/", StringComparison.Ordinal) ? "/dav/"
            : target.AbsolutePath.StartsWith("/webdav/", StringComparison.Ordinal) ? "/webdav/" : null;
        if (prefix is null) throw new InvalidDataException("Receipt-bound sessions require an explicit DAV route.");
        return target.AbsolutePath[prefix.Length..];
    }
    internal static void RequireBinding(BootstrapWorkRecord work, BootstrapReceiptExpectation? expectation)
    {
        expectation = expectation?.Validated();
        if (work.ExpectedServerRootId != expectation?.AssignedRootId || work.ReceiptPolicy != (expectation?.Policy ?? BootstrapReceiptPolicy.LegacyOnly))
            throw new InvalidDataException("Receipt qualification cannot rebind an execution session.");
    }

    internal static async Task<BootstrapUploadReply> ReadAsync(HttpResponseMessage response, long expectedSize, CancellationToken ct)
    {
        using var document = await BoundedJson.ReadAsync(response.Content, 4096, ct);
        var root = document.RootElement;
        RejectDuplicates(root, 0);
        if (root.ValueKind != JsonValueKind.Object) throw Invalid();
        var id = Text(root, "id", 36); var status = Text(root, "status", 32);
        var received = Integer(root, "receivedSize"); var expected = Integer(root, "expectedSize");
        if (!IsUuid(id) || expected != expectedSize || received < 0 || received > expected
            || status is not ("created" or "uploading" or "failed_retryable" or "verifying" or "committing" or "active")) throw Invalid();
        var etag = root.TryGetProperty("etag", out var etagValue) && etagValue.ValueKind != JsonValueKind.Null ? Text(root, "etag", 100) : null;
        var hasState = root.TryGetProperty("receiptState", out _); var hasHash = root.TryGetProperty("intentHash", out _);
        var hasReceipt = root.TryGetProperty("publicationReceipt", out var raw);
        if (!hasState && !hasHash && !hasReceipt) return new(id, status, received, expected, etag, null, null, null, null);
        if (!hasState) throw Invalid();
        var state = Text(root, "receiptState", 32);
        if (state == "legacy_unqualified")
        {
            if (hasHash || hasReceipt) throw Invalid();
            return new(id, status, received, expected, etag, null, state, null, null);
        }
        var hash = Text(root, "intentHash", 64);
        if (!IsDigest(hash)) throw Invalid();
        if (state == "pending")
        {
            if (hasReceipt || status == "active") throw Invalid();
            return new(id, status, received, expected, etag, hash, state, null, null);
        }
        if (state != "published" || !hasReceipt || status != "active" || received != expected) throw Invalid();
        var receipt = ParseReceipt(raw);
        if (!SameUuid(receipt.UploadId, id) || receipt.IntentHash != hash) throw Invalid();
        return new(id, status, received, expected, etag, hash, state, receipt, raw.GetRawText());
    }

    internal static SaturnUploadPublicationReceipt ParseReceipt(JsonElement raw)
    {
        RejectDuplicates(raw, 0);
        if (raw.ValueKind != JsonValueKind.Object || raw.EnumerateObject().Count() != ReceiptFields.Length
            || ReceiptFields.Any(field => !raw.TryGetProperty(field, out _))) throw Invalid();
        var result = new SaturnUploadPublicationReceipt(Text(raw,"schema",64), Text(raw,"uploadId",36), Text(raw,"operationId",36),
            Text(raw,"intentHash",64), Text(raw,"rootId",36), Text(raw,"resourceId",36), Text(raw,"versionId",36), Text(raw,"parentId",36),
            Text(raw,"name",255), Text(raw,"sizeBytes",20), Text(raw,"sha256",64), Text(raw,"etag",100), Text(raw,"publishedAt",40));
        if (result.Schema != "saturn.upload-publication.v1"
            || new[] {result.UploadId,result.OperationId,result.RootId,result.ResourceId,result.VersionId,result.ParentId}.Any(value => !IsUuid(value))
            || !SameUuid(result.UploadId,result.OperationId) || !IsDigest(result.IntentHash) || !IsDigest(result.Sha256)
            || result.Name is "." or ".." || result.Name.Any(c => char.IsControl(c) || c is '/' or '\\')
            || !Regex.IsMatch(result.SizeBytes, "^(0|[1-9][0-9]*)$", RegexOptions.CultureInvariant)
            || !long.TryParse(result.SizeBytes, NumberStyles.None, CultureInfo.InvariantCulture, out _)
            || result.Etag != $"\"sha256-{result.Sha256}\""
            || !DateTimeOffset.TryParseExact(result.PublishedAt, "yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out _)) throw Invalid();
        return result;
    }

    internal static void ValidateReply(BootstrapUploadReply reply, BootstrapWorkRecord work, string? uploadId, string? originalHash)
    {
        if (uploadId is not null && !SameUuid(reply.Id, uploadId) || reply.ReceivedSize < work.ReceivedBytes
            || originalHash is not null && reply.IntentHash != originalHash) throw Invalid();
        if (reply.PublicationReceipt is { } receipt) ValidateReceipt(receipt, work, reply.Id, originalHash ?? reply.IntentHash);
    }
    internal static void ValidateReceipt(SaturnUploadPublicationReceipt receipt, BootstrapWorkRecord work, string? uploadId, string? originalHash)
    {
        var request = RequestPath(new Uri(work.TargetRoot), work.RelativePath);
        var name = Uri.UnescapeDataString(request.Split('/')[^1]);
        if (!SameUuid(receipt.UploadId,uploadId) || receipt.IntentHash != originalHash || receipt.Sha256 != work.Sha256
            || long.Parse(receipt.SizeBytes,CultureInfo.InvariantCulture) != work.SealedSize || receipt.Name != name
            || work.ReceiptRequestPath is not null && work.ReceiptRequestPath != request
            || work.ExpectedServerRootId is not null && !SameUuid(receipt.RootId,work.ExpectedServerRootId)) throw Invalid();
    }
    internal static void ValidateCommittedReceipt(BootstrapPublicationReceipt receipt, BootstrapWorkRecord work)
    {
        if (receipt.Proof != BootstrapPublicationProof.QualifiedImmutableReceipt)
        {
            if (receipt.ServerReceipt is not null || receipt.ImmutableReceiptJson is not null) throw Invalid();
            return;
        }
        if (work.IsDirectory || work.ReceiptPolicy == BootstrapReceiptPolicy.LegacyOnly || !IsUuid(work.ExpectedServerRootId)
            || !IsDigest(work.OriginalIntentHash) || !IsUuid(work.UploadId) || receipt.ServerReceipt is null || receipt.ImmutableReceiptJson is null
            || System.Text.Encoding.UTF8.GetByteCount(receipt.ImmutableReceiptJson) > 4096) throw Invalid();
        using var document = JsonDocument.Parse(receipt.ImmutableReceiptJson, new JsonDocumentOptions { MaxDepth = 8 });
        var parsed = ParseReceipt(document.RootElement);
        if (parsed != receipt.ServerReceipt) throw Invalid();
        ValidateReceipt(parsed, work, work.UploadId, work.OriginalIntentHash);
        if (receipt.Etag != parsed.Etag || receipt.Size != work.SealedSize || receipt.Sha256 != work.Sha256
            || receipt.OperationId != work.OperationId || receipt.RelativePath != work.RelativePath || receipt.IsDirectory
            || receipt.TargetUri != WebDavSyncClient.RelativeTargetUri(new Uri(work.TargetRoot),work.RelativePath).AbsoluteUri) throw Invalid();
    }
    private static void RejectDuplicates(JsonElement value, int depth)
    {
        if (depth > 8) throw Invalid();
        if (value.ValueKind == JsonValueKind.Object)
        {
            var keys = new HashSet<string>(StringComparer.Ordinal);
            foreach (var item in value.EnumerateObject()) { if (!keys.Add(item.Name)) throw Invalid(); RejectDuplicates(item.Value,depth+1); }
        }
        else if (value.ValueKind == JsonValueKind.Array) foreach (var item in value.EnumerateArray()) RejectDuplicates(item,depth+1);
    }
    private static string Text(JsonElement obj,string key,int maximum)
    {
        if (!obj.TryGetProperty(key,out var field) || field.ValueKind != JsonValueKind.String
            || field.GetString() is not { Length: > 0 } result || result.Length > maximum) throw Invalid();
        return result;
    }
    private static long Integer(JsonElement obj,string key)
    {
        if (!obj.TryGetProperty(key,out var field) || field.ValueKind != JsonValueKind.Number || !field.TryGetInt64(out var value)) throw Invalid();
        return value;
    }
    private static InvalidDataException Invalid() => new("Immutable upload receipt/checkpoint is malformed or does not bind to the original sealed intent.");
}
