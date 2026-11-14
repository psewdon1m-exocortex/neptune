using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

// Explicit root-relative exclusions only; there are no implicit dot-directory rules.
public sealed record MappingSyncPolicy(string Codec,IReadOnlyList<string> ExcludedPrefixes)
{
    public static MappingSyncPolicy Default { get; }=new("legacy",Array.Empty<string>());
    public string Hash=>Convert.ToHexStringLower(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(new{codec=Codec,excludedPrefixes=ExcludedPrefixes.Order(StringComparer.Ordinal)})));
    public void Validate()
    {
        if(Codec is not("legacy" or "portable_v1")||ExcludedPrefixes is null||ExcludedPrefixes.Count>128||ExcludedPrefixes.Distinct(StringComparer.Ordinal).Count()!=ExcludedPrefixes.Count)throw new InvalidDataException("Invalid mapping policy.");
        foreach(var path in ExcludedPrefixes)
        {if(path is null||path.Length is <1 or >4096||Path.IsPathRooted(path)||path.Contains('\\')||path.Contains(':')||path.Any(char.IsControl)||path.Split('/').Any(n=>n is "" or "." or ".."))throw new InvalidDataException("Excludes must use explicit relative path prefixes.");foreach(var name in path.Split('/'))_=SyncNameCodec.Key(name,"legacy");}
    }
    // Windows paths are compared by whole segments, including non-ASCII case.
    public static bool IsWithin(string path,string prefix)
    {
        // DAV canonicalizes NFC. Protect the same spelling on both sides even
        // when the excluded local directory itself cannot use the server codec.
        path=path.Normalize(NormalizationForm.FormC);prefix=prefix.Normalize(NormalizationForm.FormC);
        return path.Equals(prefix,StringComparison.OrdinalIgnoreCase)||path.StartsWith(prefix+"/",StringComparison.OrdinalIgnoreCase);
    }
    public bool Excludes(string path)=>ExcludedPrefixes.Any(p=>IsWithin(path,p));
    // Deleting/moving an ancestor would also mutate its excluded descendants.
    public bool ProtectsSubtree(string path)=>Excludes(path)||ExcludedPrefixes.Any(p=>IsWithin(p,path));
}
public sealed record MappingPolicyPreview(string ScopeId,string PreviousHash,string ProposedHash,string Digest,long Affected,long Collisions,IReadOnlyList<string> Examples);
