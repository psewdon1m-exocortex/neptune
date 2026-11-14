using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

public sealed partial class SaturnGuardedCycleTransport : IWindowsCycleExtensions
{
    private IReadOnlySet<string> _extensionFeatures=new HashSet<string>();
    public SyncNamespacePolicy? NamespacePolicy { get; private set; }
    public int NamespaceRevision { get; private set; }=1;
    public void ConfigureExtensions(IReadOnlySet<string> features,SyncNamespacePolicy? policy,int namespaceRevision=1)
    {
        if(policy is not null&&(!LocalCycleWire.Hash(policy.PolicyHash)||policy.Codec is not("legacy" or "portable_v1")))throw new InvalidDataException("Invalid negotiated namespace policy.");
        if(namespaceRevision is not(1 or 2))throw new InvalidDataException("Unsupported namespace revision.");
        if(NamespacePolicy!=policy||NamespaceRevision!=namespaceRevision)InvalidateDirectoryFailures();
        _extensionFeatures=new HashSet<string>(features,StringComparer.Ordinal);NamespacePolicy=policy;NamespaceRevision=namespaceRevision;
        MetadataGzipEnabled=Supports("metadata-gzip-v1");ChangesWaitEnabled=Supports("changes-wait-v1");
    }
    public bool Supports(string feature)=>_extensionFeatures.Contains(feature);
    public async Task<SyncMutationReceipt> MutateAsync(SyncMutationRequest original,bool inspect,CancellationToken ct)
    {
        if(!Supports("namespace-mutations-v1")||original.Namespace!=NamespacePolicy||original.Input.GetProperty("rootId").GetString()!=Binding.RootId)throw new InvalidOperationException("Namespace mutation capability/binding unavailable.");
        if(!inspect&&NamespaceRevision>=2&&original.Kind is "create-empty" or "metadata-patch" or "clone-content")return await QueueMetadataMutation(original,ct);
        return await MutateIndividual(original,inspect,ct);
    }
    private async Task<SyncMutationReceipt> MutateIndividual(SyncMutationRequest original,bool inspect,CancellationToken ct)
    {
        var raw=await Send("mutations/"+(inspect?"status":"execute"),HttpMethod.Post,Json(new{binding=LocalCycleWire.BindingObject(Binding),@namespace=new{codec=original.Namespace.Codec,policyHash=original.Namespace.PolicyHash},input=original.Input}),64*1024,ct);
        return SyncMutationWire.Read(raw,original);
    }
    public async Task<string> ApproveDeletePlanAsync(SyncDeletePlan original,CancellationToken ct)
    {
        if(!Supports("namespace-mutations-v1")||NamespaceRevision<2||original.Namespace!=NamespacePolicy||original.Items.Count is <1 or >500||original.Items.Select(x=>x.NodeId).Distinct(StringComparer.Ordinal).Count()!=original.Items.Count)throw new InvalidOperationException("Delete-plan capability or identity unavailable.");
        var raw=await Send("delete-plans/approve",HttpMethod.Post,Json(new{binding=LocalCycleWire.BindingObject(Binding),@namespace=new{codec=original.Namespace.Codec,policyHash=original.Namespace.PolicyHash},plan=new{schema="saturn.delete-plan.v1",planId=original.PlanId,planDigest=original.PlanDigest,items=original.Items.Select(x=>new{nodeId=x.NodeId,version=x.Version})}},4*1024*1024),16*1024,ct);
        using var doc=SyncProtocolReader.Parse(raw);var result=doc.RootElement;
        if(result.GetProperty("schema").GetString()!="saturn.delete-plan-approved.v1"||result.GetProperty("planId").GetString()!=original.PlanId||result.GetProperty("planDigest").GetString()!=original.PlanDigest||result.GetProperty("itemCount").GetInt32()!=original.Items.Count)throw new InvalidDataException("Delete-plan approval receipt changed the original preview.");
        return result.GetRawText();
    }
    public async Task<SyncContentReference?> LookupContentAsync(long size,string sha256,CancellationToken ct)
    {
        if(!Supports("clone-content-v1"))return null;
        if(size<0||!LocalCycleWire.Hash(sha256))throw new InvalidDataException("Content lookup needs an already known sealed hash.");
        var raw=await Send("content/lookup",HttpMethod.Post,Json(new{binding=LocalCycleWire.BindingObject(Binding),sizeBytes=size.ToString(System.Globalization.CultureInfo.InvariantCulture),sha256},16*1024),16*1024,ct);
        using var json=SyncProtocolReader.Parse(raw);var value=json.RootElement.GetProperty("reference");if(value.ValueKind==JsonValueKind.Null)return null;
        var reference=JsonSerializer.Deserialize<SyncContentReference>(value.GetRawText(),LocalCycleWire.Json)??throw new InvalidDataException("Content reference missing.");
        if(reference.SizeBytes!=size.ToString(System.Globalization.CultureInfo.InvariantCulture)||reference.Sha256!=sha256||!LocalCycleWire.Uuid(reference.NodeId)||!LocalCycleWire.Uuid(reference.ContentVersionId)||!LocalCycleWire.Uuid(reference.MetadataVersion))throw new InvalidDataException("Content reference changed the sealed identity.");
        return reference;
    }
    public async Task<IReadOnlyList<CycleFolderOutcome?>> EnsureDirectoriesAsync(IReadOnlyList<CycleFolderInput> originals,CancellationToken ct)
    {
        if(originals.Count is <1 or >500)throw new ArgumentOutOfRangeException(nameof(originals));
        if(!Supports("folder-batch-v1")){var individual=new List<CycleFolderOutcome?>();foreach(var original in originals)individual.Add(await EnsureDirectoryIndividualAsync(original,ct));return individual;}
        foreach(var original in originals){LocalCycleWire.FolderInput(original);if(original.RootId!=Binding.RootId)throw new InvalidDataException("Folder batch root mismatch.");}
        var raw=await Send("directories/batch",HttpMethod.Post,Json(new{binding=LocalCycleWire.BindingObject(Binding),items=originals.Select((input,i)=>new{@ref=i.ToString(System.Globalization.CultureInfo.InvariantCulture),input=LocalCycleWire.FolderObject(input)})},4*1024*1024),4*1024*1024,ct);
        using var json=SyncProtocolReader.Parse(raw);var root=json.RootElement;
        if(root.GetProperty("schema").GetString()!="saturn.folder-batch-result.v1"||root.GetProperty("items").GetArrayLength()!=originals.Count)throw new InvalidDataException("Folder batch membership changed.");
        var output=new List<CycleFolderOutcome?>();var n=0;
        foreach(var row in root.GetProperty("items").EnumerateArray())
        {if(row.GetProperty("ref").GetString()!=(n++).ToString(System.Globalization.CultureInfo.InvariantCulture))throw new InvalidDataException("Folder batch order changed.");output.Add(row.TryGetProperty("outcome",out var outcome)?LocalCycleWire.Folder(System.Text.Encoding.UTF8.GetBytes(outcome.GetRawText()),originals[n-1]):null);}
        return output;
    }
}
