using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Neptune.Core;

// A seal binds bytes observed under a live write/delete-excluding file handle
// to one bootstrap job. ScanGeneration is not a local dirty generation, and
// this historical evidence grants no continuous cut, current R or B.
public interface IQualifiedBootstrapSourceCapture : IBootstrapSourceCapture
{
    Task<NativeBootstrapSeal> CaptureWorkAsync(BootstrapWorkRecord work, string spoolPath,
        long maximumBytes, CancellationToken cancellationToken);
}
public sealed class NativeBootstrapSeal
{
    internal NativeBootstrapSeal(BootstrapSealedSource source, string proofJson)
        => (Source, ProofJson) = (source, proofJson);
    public BootstrapSealedSource Source { get; }
    public string ProofJson { get; }
}
public enum NativeCaptureStage { GuardOpened, SpoolFlushed }
public sealed record NativeBootstrapCaptureEvidence(string Schema, string SessionId, string ScanId,
    long ScanGeneration, long EntryId, string OperationId, string MappingId, string ClientInstanceId,
    string SourceRoot, string RelativePath, string TargetRoot, string CredentialScope, string? ExpectedServerRootId,
    string VolumeGuid, string VolumeSerial64, string LegacyVolumeSerial, string RootFileId128, long RootCreated,
    string ParentFileId128, long ParentCreated, bool ParentCaseSensitive, string FileId128, string LegacyFileId64,
    long FileCreated, uint FileAttributes, uint Links, string SealedSize, string Sha256, string LastFileUsn, string Nonce)
{
    public bool HistoricalOnly => true;
    public string LocalCompleteness => "Unknown";
    public string SourceCycle => "Unknown";
}

public static class NativeBootstrapCaptureProof
{
    public const string Schema = "neptune.native-bootstrap-capture.v1";
    public const int MaximumJsonBytes = 32 * 1024;
    private const string IdentityPrefix = "native-bootstrap-v1:";
    private static readonly JsonSerializerOptions Options = new()
    { PropertyNamingPolicy = JsonNamingPolicy.CamelCase, UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
      MaxDepth = 4, IgnoreReadOnlyProperties = true };
    private static readonly HashSet<string> Fields = new(StringComparer.Ordinal)
    {
        "schema", "sessionId", "scanId", "scanGeneration", "entryId", "operationId", "mappingId", "clientInstanceId",
        "sourceRoot", "relativePath", "targetRoot", "credentialScope", "expectedServerRootId", "volumeGuid", "volumeSerial64",
        "legacyVolumeSerial", "rootFileId128", "rootCreated", "parentFileId128", "parentCreated", "parentCaseSensitive",
        "fileId128", "legacyFileId64", "fileCreated", "fileAttributes", "links", "sealedSize", "sha256", "lastFileUsn", "nonce"
    };
    internal static string Serialize(NativeBootstrapCaptureEvidence evidence) => JsonSerializer.Serialize(evidence, Options);
    internal static string Identity(string json) => IdentityPrefix + Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(json)));

    // Parsing stored evidence validates a historical ledger binding. It does
    // not mint NativeBootstrapSeal or promote JSON supplied by an old capture.
    public static NativeBootstrapCaptureEvidence? ValidatePersisted(BootstrapWorkRecord work)
    {
        if (work.NativeCaptureProofJson is null)
        {
            if (work.SourceIdentity?.StartsWith(IdentityPrefix, StringComparison.Ordinal) == true)
                throw new InvalidDataException("A recognized native capture proof is missing.");
            return null;
        }
        if (work.SealedSize is null || work.Sha256 is null || work.SourceIdentity is null)
            throw new InvalidDataException("Native capture lacks its original sealed bytes.");
        return Validate(work.NativeCaptureProofJson, work, work.SealedSize.Value, work.Sha256, work.SourceIdentity);
    }
    internal static NativeBootstrapCaptureEvidence Validate(string json, BootstrapWorkRecord work, long size, string hash, string identity)
    {
        try
        {
            if (json.Length > MaximumJsonBytes || Encoding.UTF8.GetByteCount(json) > MaximumJsonBytes)
                throw new InvalidDataException("Native capture proof exceeds its fixed limit.");
            using var doc = JsonDocument.Parse(json, new JsonDocumentOptions { MaxDepth = 4 });
            if (doc.RootElement.ValueKind != JsonValueKind.Object) throw new InvalidDataException("Native capture proof is not an object.");
            var fields = new HashSet<string>(StringComparer.Ordinal);
            foreach (var property in doc.RootElement.EnumerateObject())
                if (!Fields.Contains(property.Name) || !fields.Add(property.Name)) throw new InvalidDataException("Native capture proof contains unknown or duplicate fields.");
            if (!fields.SetEquals(Fields)) throw new InvalidDataException("Native capture proof is incomplete.");
            var e = JsonSerializer.Deserialize<NativeBootstrapCaptureEvidence>(json, Options)
                ?? throw new InvalidDataException("Native capture proof is missing.");
            if (e.Schema != Schema || e.SessionId != work.SessionId || e.ScanId != work.ScanId
                || e.ScanGeneration != work.ScanGeneration || e.ScanGeneration <= 0 || e.EntryId != work.EntryId || e.EntryId <= 0
                || e.OperationId != work.OperationId || !Hex(e.OperationId, 64)
                || e.MappingId != work.MappingId || e.ClientInstanceId != work.ClientInstanceId
                || e.SourceRoot != work.SourceRoot || e.RelativePath != work.RelativePath || e.TargetRoot != work.TargetRoot
                || e.CredentialScope != work.CredentialScope || e.ExpectedServerRootId != work.ExpectedServerRootId
                || work.IsDirectory || size < 0 || e.SealedSize != size.ToString(CultureInfo.InvariantCulture)
                || e.Sha256 != hash || !Hex(e.Sha256, 64) || identity != Identity(json))
                throw new InvalidDataException("Native capture proof cannot rebind its original work or bytes.");
            if (!Guid.TryParseExact(e.VolumeGuid, "D", out var volume) || volume == Guid.Empty
                || !Guid.TryParseExact(e.Nonce, "N", out var nonce) || nonce == Guid.Empty
                || !Hex(e.VolumeSerial64, 16) || !Hex(e.LegacyVolumeSerial, 8) || !NonzeroHex(e.RootFileId128, 32)
                || !NonzeroHex(e.ParentFileId128, 32) || !NonzeroHex(e.FileId128, 32) || !NonzeroHex(e.LegacyFileId64, 16)
                || e.RootCreated <= 0 || e.ParentCreated <= 0 || e.FileCreated <= 0 || e.Links != 1
                || (e.FileAttributes & (0x10u | 0x400u | 0x1000u | 0x40000u | 0x400000u)) != 0
                || !long.TryParse(e.LastFileUsn, NumberStyles.None, CultureInfo.InvariantCulture, out var usn) || usn < 0
                || e.LastFileUsn != usn.ToString(CultureInfo.InvariantCulture)
                || e.SourceRoot.Length is < 1 or > 4096 || e.RelativePath.Length is < 1 or > 4096
                || e.TargetRoot.Length is < 1 or > 8192 || e.MappingId.Length is < 1 or > 256
                || e.ClientInstanceId.Length is < 1 or > 256 || e.CredentialScope.Length is < 1 or > 256)
                throw new InvalidDataException("Native capture identity or policy is unsupported.");
            return e;
        }
        catch (JsonException error) { throw new InvalidDataException("Native capture proof is malformed.", error); }
    }
    private static bool Hex(string? value, int length) => value is not null && value.Length == length
        && value.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f');
    private static bool NonzeroHex(string? value, int length) => Hex(value, length) && value!.Any(c => c != '0');
}
