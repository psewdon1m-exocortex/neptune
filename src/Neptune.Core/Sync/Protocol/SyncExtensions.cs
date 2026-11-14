using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Neptune.Core.Sync.Protocol;

public sealed record SyncNamespacePolicy(string Codec,string PolicyHash);
public sealed record SyncContentReference(string NodeId,string ContentVersionId,string MetadataVersion,string SizeBytes,string Sha256);
public sealed record SyncMutationReceipt(string OperationId,string IntentHash,string State,SyncDescriptor? Description,long? FirstSequence,long? LastSequence,bool Tombstone,string RawJson);
public sealed record SyncMutationRequest(string OperationId,string Kind,JsonElement Input,SyncNamespacePolicy Namespace);
public sealed record SyncDeletePlanItem(string NodeId,string Version);
public sealed record SyncDeletePlan(string PlanId,string PlanDigest,IReadOnlyList<SyncDeletePlanItem> Items,SyncNamespacePolicy Namespace);
public interface IWindowsCycleExtensions
{
    int NamespaceRevision=>1;
    Task<string> ApproveDeletePlanAsync(SyncDeletePlan original,CancellationToken ct)=>throw new NotSupportedException("Server delete-plan registration requires K1 r2.");
    SyncNamespacePolicy? NamespacePolicy { get; }
    bool Supports(string feature);
    Task<SyncMutationReceipt> MutateAsync(SyncMutationRequest original,bool inspect,CancellationToken ct);
    Task<SyncContentReference?> LookupContentAsync(long size,string sha256,CancellationToken ct);
    Task<SyncFeedPage> WaitForChangesAsync(string cursor,long after,CancellationToken ct);
}
public static partial class SyncNameCodec
{
    public static string Key(string name,string codec)
    {
        var scalars=0;
        for(var i=0;i<name.Length;i++)
        {
            var c=name[i];if(char.IsHighSurrogate(c)){if(++i>=name.Length||!char.IsLowSurrogate(name[i]))throw Invalid();}
            else if(char.IsLowSurrogate(c)||char.IsControl(c)||c is '/' or '\\')throw Invalid();
            scalars++;
        }
        if(scalars is <1 or >255||Encoding.UTF8.GetByteCount(name)>1020||name is "." or "..")throw Invalid();
        if(codec=="legacy")return name;
        if(codec!="portable_v1"||!name.IsNormalized(NormalizationForm.FormC)||name.IndexOfAny(['<','>',':','"','|','?','*'])>=0||name.EndsWith('.')||name.EndsWith(' ')||DeviceName().IsMatch(name))throw Invalid();
        return string.Concat(name.Select(c=>c is >= 'A' and <= 'Z'?(char)(c+32):c));
    }
    private static InvalidDataException Invalid()=>new("Name is incompatible with the selected codec; an explicit migration is required.");
    [GeneratedRegex(@"^(CON|PRN|AUX|NUL|COM[1-9¹²³]|LPT[1-9¹²³])(?:\.|$)",RegexOptions.IgnoreCase|RegexOptions.CultureInvariant)]private static partial Regex DeviceName();
}

public static class SyncMutationWire
{
    public static SyncMutationRequest Create(SyncRootBinding binding,SyncNamespacePolicy policy,string operation,string kind,SyncDescriptor? expected,IReadOnlyDictionary<string,object?> fields)
    {
        if(!Guid.TryParseExact(operation,"D",out _)||!LocalCycleWire.Hash(policy.PolicyHash))throw new InvalidDataException("Mutation identity/policy missing.");
        var input=new Dictionary<string,object?>
        {
            ["schema"]="saturn.namespace-mutation.v1",["operationId"]=operation,["idempotencyKey"]="neptune-namespace-"+operation,
            ["rootId"]=binding.RootId,["epoch"]=binding.ServerEpoch,["bindingVersion"]=binding.BindingVersion,["kind"]=kind,
            ["expected"]=expected is null?new{kind="absent"}:(object)new{kind="version",node_id=expected.NodeId,version=expected.Version}
        };
        foreach(var field in fields)input.Add(field.Key,field.Value);
        if(input.TryGetValue("name",out var name))_=SyncNameCodec.Key((string)name!,policy.Codec);
        return new(operation,kind,JsonSerializer.SerializeToElement(input,LocalCycleWire.Json),policy);
    }
    internal static SyncMutationReceipt Read(ReadOnlyMemory<byte> raw,SyncMutationRequest original)
    {
        using var document=SyncProtocolReader.Parse(raw);var root=document.RootElement;
        if(root.GetProperty("schema").GetString()!="saturn.namespace-mutation-status.v1"||root.GetProperty("operationId").GetString()!=original.OperationId)throw new InvalidDataException("Mutation receipt identity mismatch.");
        var hash=root.GetProperty("intentHash").GetString()!;var state=root.GetProperty("state").GetString()!;
        if(!LocalCycleWire.Hash(hash)||state is not("QUEUED" or "PREPARED" or "PHYSICAL_PENDING" or "UNKNOWN" or "PUBLISHED" or "NOT_PUBLISHED"))throw new InvalidDataException("Mutation status is invalid.");
        SyncDescriptor? description=null;long? first=null,last=null;var tombstone=false;
        if(root.TryGetProperty("result",out var result))
        {
            description=SyncProtocolReader.Descriptor(result.GetProperty("description"));first=SyncProtocolReader.Integer64(result.GetProperty("firstSeq"));last=SyncProtocolReader.Integer64(result.GetProperty("lastSeq"));tombstone=result.GetProperty("tombstone").GetBoolean();
            if(state!="PUBLISHED"||first<1||last<first||tombstone!=(original.Kind=="quarantine-delete"))throw new InvalidDataException("Mutation publication receipt is inconsistent.");
            var expected=original.Input.GetProperty("expected");
            if(expected.GetProperty("kind").GetString()=="version"&&description.NodeId!=expected.GetProperty("node_id").GetString())throw new InvalidDataException("Mutation changed node identity.");
            if(original.Input.TryGetProperty("parentId",out var parent)&&(description.ParentId!=parent.GetString()||description.Name!=original.Input.GetProperty("name").GetString()))throw new InvalidDataException("Mutation changed destination.");
        }
        if(state=="PUBLISHED"&&description is null)throw new InvalidDataException("Published mutation lacks a receipt.");
        return new(original.OperationId,hash,state,description,first,last,tombstone,root.GetRawText());
    }
}
