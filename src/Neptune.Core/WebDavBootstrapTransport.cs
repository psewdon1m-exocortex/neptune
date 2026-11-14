using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Neptune.Core;

public sealed class CreateOnlyWebDavBootstrapTransport(WebDavSyncClient client, Uri targetRoot, string token,
    bool allowLegacyPutFallback = false, BootstrapReceiptExpectation? receiptExpectation = null) : ICreateOnlyBootstrapTransport
{
    private readonly SemaphoreSlim _rootGate = new(1, 1);
    private bool _rootVerified;
    public Uri TargetRoot { get; } = new(targetRoot.AbsoluteUri.TrimEnd('/') + "/");
    // A credential identity, never raw credentials in the durable work module.
    public string CredentialScope { get; } = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(targetRoot.GetLeftPart(UriPartial.Authority) + "\n" + token.Trim())));
    public BootstrapReceiptExpectation? ReceiptExpectation { get; } = receiptExpectation?.Validated();
    public async Task<BootstrapPublicationReceipt> TransferAsync(BootstrapWorkRecord work,
        Func<BootstrapTransferCheckpoint, Task> checkpoint, CancellationToken cancellationToken)
    {
        BootstrapUploadReceiptParser.RequireBinding(work, ReceiptExpectation);
        await _rootGate.WaitAsync(cancellationToken);
        try
        {
            // Historical same-key recovery is authorized by the explicit
            // assigned UUID/server registry. Its old DAV parent may be gone.
            var boundReplay = work.TransportMode == "session" && work.ExpectedServerRootId is not null && work.Attempt > 1;
            if (!_rootVerified && !boundReplay)
            {
                await client.VerifyBootstrapRootAsync(TargetRoot, token, cancellationToken);
                _rootVerified = true;
            }
        }
        finally { _rootGate.Release(); }
        return await client.TransferBootstrapCreateOnlyAsync(TargetRoot, token, CredentialScope, work, checkpoint, allowLegacyPutFallback, cancellationToken);
    }
}

public sealed partial class WebDavSyncClient
{
    internal async Task VerifyBootstrapRootAsync(Uri root, string token, CancellationToken cancellationToken)
    {
        if (await ReadCollectionKindAsync(root, token, cancellationToken) != CollectionKind.Collection)
            throw new BootstrapCreateOnlyConflictException("The supplied mapping root is not an existing Saturn directory.");
    }
    internal async Task<BootstrapPublicationReceipt> TransferBootstrapCreateOnlyAsync(Uri root, string token, string credentialScope,
        BootstrapWorkRecord work, Func<BootstrapTransferCheckpoint, Task> checkpoint, bool allowLegacyPutFallback, CancellationToken cancellationToken)
    {
        if (root.AbsoluteUri != work.TargetRoot || credentialScope != work.CredentialScope || work.OperationId.Length != 64
            || !work.OperationId.All(Uri.IsHexDigit) || work.Attempt < 1)
            throw new InvalidDataException("CREATE_ONLY work binding is invalid.");
        var segments = CanonicalSegments(work.RelativePath).Select(Uri.EscapeDataString).ToArray();
        if (segments.Length == 0) throw new InvalidDataException("Work cannot publish over the mapping root.");
        var target = RelativeTargetUri(root, work.RelativePath);
        try
        {
            if (work.IsDirectory)
            {
                await EnsureCollectionsAsync(root, token, segments, cancellationToken);
                // A cached parent observation is not a fresh observation of
                // this explicit empty-directory job. Verify its own type/path.
                if (await ReadCollectionKindAsync(target, token, cancellationToken) != CollectionKind.Collection)
                    throw new BootstrapCreateOnlyConflictException("Required directory is absent or conflicts with a file.");
                return new(work.OperationId, target.AbsoluteUri, work.RelativePath, true, 0, null, null, BootstrapPublicationProof.LegacyObservedActive);
            }
            // Never recreate mutable parents before observing a sent intent.
            // An attempt that failed before choosing its transport mode may
            // still prepare parents; no CREATE was sent for that work yet.
            if (work.TransportMode is null && work.UploadId is null)
                await EnsureCollectionsAsync(root, token, segments[..^1], cancellationToken);
            if (work.SpoolPath is null || work.Sha256 is not { Length: 64 } || !work.Sha256.All(Uri.IsHexDigit) || work.SealedSize is null)
                throw new InvalidDataException("File work was not sealed before transport.");
            await using var source = new FileStream(work.SpoolPath, FileMode.Open, FileAccess.Read, FileShare.Read, 128 * 1024,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            if (source.Length != work.SealedSize) throw new InvalidDataException("The immutable spool size changed.");
            // Recovery may only observe a previous non-idempotent PUT. Absence
            // does not fence a delayed request and never authorizes a new PUT.
            if (work.TransportMode == "put") return await ObserveLegacyPutAsync(target, token, work, cancellationToken);
            var prefix = target.AbsolutePath.StartsWith("/dav/", StringComparison.Ordinal) ? "/dav/"
                : target.AbsolutePath.StartsWith("/webdav/", StringComparison.Ordinal) ? "/webdav/" : null;
            if (prefix is null) throw new InvalidDataException("CREATE_ONLY legacy sessions require an explicit DAV route.");
            var endpoint = new Uri(target, "/api/v1/sync/uploads");
            var requestPath = BootstrapUploadReceiptParser.RequestPath(root,work.RelativePath);
            if (work.ReceiptRequestPath is not null && work.ReceiptRequestPath != requestPath)
                throw new InvalidDataException("CREATE_ONLY request path changed after sealing.");
            await checkpoint(new("session", work.UploadId, work.ReceivedBytes, work.OriginalIntentHash));
            using var create = new HttpRequestMessage(HttpMethod.Post, endpoint);
            create.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
            create.Content = new StringContent(JsonSerializer.Serialize(new { path = requestPath,
                expectedSize = source.Length, expectedSha256 = work.Sha256, idempotencyKey = work.OperationId, ifNoneMatch = "*" }), Encoding.UTF8, "application/json");
            using var created = await httpClient.SendAsync(create, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            if (created.StatusCode == HttpStatusCode.NotFound && allowLegacyPutFallback && work.ReceiptPolicy != BootstrapReceiptPolicy.RequireImmutable
                && work.Attempt == 1 && work.UploadId is null && work.OriginalIntentHash is null)
            {
                await checkpoint(new("put", null, 0)); // persist before the non-idempotent effect
                using var put = new HttpRequestMessage(HttpMethod.Put, target);
                put.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
                put.Headers.TryAddWithoutValidation("If-None-Match", "*");
                put.Headers.TryAddWithoutValidation("X-Content-Sha256", work.Sha256);
                put.Content = new StreamContent(source, 128 * 1024);
                put.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
                using var result = await httpClient.SendAsync(put, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                CheckCreateOnlyResponse(result);
                if (result.Headers.ETag?.Tag != ExpectedBootstrapEtag(work)) throw new BootstrapPublicationUncertainException("Legacy PUT did not confirm sealed bytes.");
                await checkpoint(new("put", null, source.Length));
                return BootstrapReceipt(target, work, result.Headers.ETag.Tag, BootstrapPublicationProof.LegacyObservedActive);
            }
            CheckCreateOnlyResponse(created);
            var current = await BootstrapUploadReceiptParser.ReadAsync(created, source.Length, cancellationToken);
            BootstrapUploadReceiptParser.ValidateReply(current,work,work.UploadId,work.OriginalIntentHash);
            // Original server fingerprint and UUID commit before any PATCH.
            // Lost CREATE replies are recovered by the same unchanged key.
            await checkpoint(new("session", current.Id, current.ReceivedSize, current.IntentHash));
            var originalHash = work.OriginalIntentHash ?? current.IntentHash;
            if (work.ReceiptPolicy == BootstrapReceiptPolicy.RequireImmutable && originalHash is null)
                throw new BootstrapPublicationUncertainException("This accepted session has no immutable receipt capability; preserve its original key.");
            if (current.Status == "committing") throw new BootstrapPublicationUncertainException("Pending committing publication is UNKNOWN; same identity reconciliation is required.");
            var upload = new Uri(endpoint.AbsoluteUri + "/" + Uri.EscapeDataString(current.Id));
            var buffer = new byte[1024 * 1024];
            while (current.Status != "active" && current.ReceivedSize < source.Length)
            {
                source.Position = current.ReceivedSize;
                var count = (int)Math.Min(buffer.Length, source.Length - source.Position);
                await source.ReadExactlyAsync(buffer.AsMemory(0, count), cancellationToken);
                using var append = new HttpRequestMessage(HttpMethod.Patch, upload);
                append.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
                append.Headers.TryAddWithoutValidation("Upload-Offset", current.ReceivedSize.ToString(System.Globalization.CultureInfo.InvariantCulture));
                append.Content = new ByteArrayContent(buffer, 0, count);
                append.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
                using var appended = await httpClient.SendAsync(append, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                CheckCreateOnlyResponse(appended);
                var next = await BootstrapUploadReceiptParser.ReadAsync(appended, source.Length, cancellationToken);
                BootstrapUploadReceiptParser.ValidateReply(next,work,current.Id,originalHash);
                if (next.Id != current.Id || next.ReceivedSize != current.ReceivedSize + count) throw new InvalidDataException("Server upload offset is invalid.");
                current = next;
                await checkpoint(new("session", current.Id, current.ReceivedSize, current.IntentHash));
                if (current.Status == "committing") throw new BootstrapPublicationUncertainException("Pending committing publication remains UNKNOWN.");
            }
            if (current.Status != "active")
            {
                using var complete = new HttpRequestMessage(HttpMethod.Post, new Uri(upload.AbsoluteUri + "/complete"));
                complete.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
                complete.Headers.TryAddWithoutValidation("If-None-Match", "*");
                using var completed = await httpClient.SendAsync(complete, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                CheckCreateOnlyResponse(completed);
                var next = await BootstrapUploadReceiptParser.ReadAsync(completed, source.Length, cancellationToken);
                BootstrapUploadReceiptParser.ValidateReply(next,work,current.Id,originalHash);
                if (next.Id != current.Id) throw new InvalidDataException("Server completion changed upload identity.");
                current = next;
                await checkpoint(new("session", current.Id, current.ReceivedSize, current.IntentHash));
            }
            if (work.ReceiptPolicy != BootstrapReceiptPolicy.LegacyOnly && work.ExpectedServerRootId is not null
                && originalHash is not null && current.PublicationReceipt is not null && current.RawReceiptJson is not null)
                return new(work.OperationId,target.AbsoluteUri,work.RelativePath,false,work.SealedSize.Value,work.Sha256,
                    current.PublicationReceipt.Etag,BootstrapPublicationProof.QualifiedImmutableReceipt,current.PublicationReceipt,current.RawReceiptJson);
            if (work.ReceiptPolicy == BootstrapReceiptPolicy.RequireImmutable)
                throw new BootstrapPublicationUncertainException("The session has no qualified historical publication receipt.");
            if (current.Status != "active" || current.ReceivedSize != source.Length || current.Etag != ExpectedBootstrapEtag(work))
                throw new BootstrapPublicationUncertainException("Legacy active response does not qualify the captured publication; current resource may have changed.");
            // Legacy Saturn resolves active ETag from CURRENT resource. This is
            // an observed active result, not an immutable historic receipt/B.
            return BootstrapReceipt(target, work, current.Etag, BootstrapPublicationProof.LegacyObservedActive);
        }
        catch (HttpRequestException error) when (error.StatusCode is HttpStatusCode.NotFound or HttpStatusCode.Conflict)
        {
            InvalidateCollections(root, token, segments[..^1]);
            throw;
        }
    }

    private async Task<BootstrapPublicationReceipt> ObserveLegacyPutAsync(Uri target, string token, BootstrapWorkRecord work, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, target);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Trim());
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound) throw new BootstrapPublicationUncertainException("A lost PUT remains uncertain; observed absence does not fence its delayed effect.");
        response.EnsureSuccessStatusCode();
        await using var content = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = new byte[128 * 1024]; long size = 0;
        while (true)
        {
            var count = await content.ReadAsync(buffer, cancellationToken); if (count == 0) break;
            size = checked(size + count);
            if (size > work.SealedSize) throw new BootstrapPublicationUncertainException("Observed remote differs from the captured PUT bytes.");
            hash.AppendData(buffer, 0, count);
        }
        if (size != work.SealedSize || Convert.ToHexStringLower(hash.GetHashAndReset()) != work.Sha256)
            throw new BootstrapPublicationUncertainException("Observed remote differs from the captured PUT bytes.");
        return BootstrapReceipt(target, work, response.Headers.ETag?.Tag, BootstrapPublicationProof.ObservedEqual);
    }
    private static string ExpectedBootstrapEtag(BootstrapWorkRecord work) => $"\"sha256-{work.Sha256}\"";
    private static BootstrapPublicationReceipt BootstrapReceipt(Uri target, BootstrapWorkRecord work, string? etag, BootstrapPublicationProof proof)
        => new(work.OperationId, target.AbsoluteUri, work.RelativePath, false, work.SealedSize!.Value, work.Sha256, etag, proof);
    private static void CheckCreateOnlyResponse(HttpResponseMessage response)
    {
        if (response.StatusCode is HttpStatusCode.PreconditionFailed or HttpStatusCode.Conflict || (int)response.StatusCode == 428)
            throw new BootstrapCreateOnlyConflictException("CREATE_ONLY target/session conflicts; unknown remote is never overwritten.");
        response.EnsureSuccessStatusCode();
    }
}
