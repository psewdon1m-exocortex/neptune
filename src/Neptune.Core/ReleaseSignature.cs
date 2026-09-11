using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Neptune.Core;

public static class ReleaseSignature
{
    public static void Verify(ReadOnlySpan<byte> manifest, ReadOnlySpan<byte> envelope, string publicKeyPem)
    {
        if (manifest.Length > 2 * 1024 * 1024 || envelope.Length > 16384 || publicKeyPem.Length > 16384)
            throw new InvalidDataException("Release signature input exceeds limit.");
        using var document = JsonDocument.Parse(envelope.ToArray());
        var root = document.RootElement;
        if (root.GetProperty("schema").GetString() != "exocortex.release-signature.v1" || root.GetProperty("algorithm").GetString() != "RSA-PSS-SHA256")
            throw new InvalidDataException("Unsupported release signature.");
        var pem = publicKeyPem.Trim();
        if (!pem.StartsWith("-----BEGIN PUBLIC KEY-----", StringComparison.Ordinal) || !pem.EndsWith("-----END PUBLIC KEY-----", StringComparison.Ordinal))
            throw new InvalidDataException("Release trust must contain a SPKI public key.");
        using var key = RSA.Create();
        key.ImportFromPem(pem);
        if (key.KeySize < 3072) throw new InvalidDataException("Release trust requires RSA with at least 3072 bits.");
        var keyId = Convert.ToHexStringLower(SHA256.HashData(key.ExportSubjectPublicKeyInfo()));
        if (root.GetProperty("key_id").GetString() != keyId) throw new InvalidDataException("Release signer is not trusted.");
        var signature = Convert.FromBase64String(root.GetProperty("signature").GetString() ?? "");
        if (!key.VerifyData(manifest, signature, HashAlgorithmName.SHA256, RSASignaturePadding.Pss))
            throw new InvalidDataException("Release signature verification failed.");
    }

    public static async Task<byte[]> DownloadManifestAsync(HttpClient http, Uri manifestUri, Uri signatureUri, string trustFile, CancellationToken cancellationToken)
    {
        var key = await File.ReadAllTextAsync(trustFile, cancellationToken);
        var manifest = await DownloadBoundedAsync(http, manifestUri, 2 * 1024 * 1024, cancellationToken);
        var envelope = await DownloadBoundedAsync(http, signatureUri, 16384, cancellationToken);
        Verify(manifest, envelope, key);
        return manifest;
    }

    private static async Task<byte[]> DownloadBoundedAsync(HttpClient http, Uri uri, int limit, CancellationToken cancellationToken)
    {
        if (uri.Scheme != Uri.UriSchemeHttps) throw new InvalidDataException("Release signature transport requires HTTPS.");
        using var response = await http.GetAsync(uri, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        response.EnsureSuccessStatusCode();
        if (response.Content.Headers.ContentLength > limit) throw new InvalidDataException("Release metadata exceeds limit.");
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var output = new MemoryStream();
        var buffer = new byte[16384];
        int count;
        while ((count = await stream.ReadAsync(buffer, cancellationToken)) != 0)
        {
            if (output.Length + count > limit) throw new InvalidDataException("Release metadata exceeds limit.");
            output.Write(buffer, 0, count);
        }
        return output.ToArray();
    }
}
