using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

public sealed record SmallFileBatchCapability(SyncRootBinding Binding, string DeviceId, string BackendIdentity,
    string QualificationReceiptSha256, int MaximumItems, int MaximumFileBytes, int MaximumDataBytes, int MaximumMetadataBytes, bool ExecutionJob = false, int ParallelJobs = 1, bool SingleRequest = false)
{
    public static SmallFileBatchCapability Parse(ReadOnlyMemory<byte> bytes, WindowsCycleConfiguration expected)
    {
        if (bytes.Length > 16 * 1024) throw Invalid(); using var doc = SyncProtocolReader.Parse(bytes); var body = doc.RootElement;
        Exact(body, "schema", "transport", "binding", "device_id", "backend_identity", "qualification_receipt_sha256", "limits");
        var single = Text(body, "schema") == "saturn.small-execution-job-single-request-capabilities.v1";
        var pipeline = single || Text(body, "schema") == "saturn.small-execution-job-pipeline-capabilities.v1";
        var job = pipeline || Text(body, "schema") == "saturn.small-execution-job-capabilities.v1";
        if ((!job && Text(body, "schema") != "saturn.small-file-batch-capabilities.v1") || Text(body, "transport") != (job ? "saturn.small-execution-job.v1" : "saturn.small-file-batch.v1")
            || Text(body, "device_id") != expected.DeviceId || Text(body, "backend_identity") != expected.BackendIdentity) throw Invalid();
        var bindingJson = body.GetProperty("binding"); Exact(bindingJson, "protocol", "descriptor_schema", "server_identity", "root_id", "server_epoch", "policy_version", "binding_version");
        var binding = SyncProtocolReader.Binding(bindingJson); if (binding != expected.Binding) throw Invalid();
        var receipt = Text(body, "qualification_receipt_sha256"); if (!LocalCycleWire.Hash(receipt)) throw Invalid();
        var limits = body.GetProperty("limits");
        if (pipeline) Exact(limits, "items", "file_bytes", "data_bytes", "manifest_bytes", "jobs");
        else Exact(limits, "items", "file_bytes", "data_bytes", "manifest_bytes");
        if (pipeline && (!limits.GetProperty("jobs").TryGetInt32(out var jobs) || jobs != 2)) throw Invalid();
        if (!limits.GetProperty("items").TryGetInt32(out var items) || items is < 1 or > SmallFileBatchPlan.MaximumItems) throw Invalid();
        var file = LocalCycleWire.Decimal(Text(limits, "file_bytes")); var data = LocalCycleWire.Decimal(Text(limits, "data_bytes"));
        var metadata = LocalCycleWire.Decimal(Text(limits, "manifest_bytes"));
        if (file is < 1 or > SmallFileBatchPlan.MaximumFileBytes || data < file || data > SmallFileBatchPlan.MaximumDataBytes
            || metadata is < 1024 or > SmallFileBatchPlan.MaximumMetadataBytes) throw Invalid();
        if (job && (items > 16 || data > 4194304 || metadata > 1048576)) throw Invalid();
        return new(binding, expected.DeviceId, expected.BackendIdentity, receipt, items, (int)file, (int)data, (int)metadata, job, pipeline ? 2 : 1, single);
    }
    public bool Admits(SmallFileBatchPlan plan) => plan.ExecutionJob == ExecutionJob && (!ExecutionJob || plan.CanExecuteAsJob)
        && plan.Binding == Binding && plan.Entries.Count <= MaximumItems && plan.DataBytes <= MaximumDataBytes
        && plan.EncodePlan().Length <= MaximumMetadataBytes && plan.Entries.All(e => LocalCycleWire.Decimal(e.Original.Intent.SizeBytes) <= MaximumFileBytes
            && e.Original.Intent.DeviceId == DeviceId && e.Original.BackendIdentity == BackendIdentity);
    private static InvalidDataException Invalid() => new("Unqualified, changed or unsupported small-file-batch capability.");
    private static string Text(JsonElement element, string name) => element.GetProperty(name).ValueKind == JsonValueKind.String ? element.GetProperty(name).GetString()! : throw Invalid();
    private static void Exact(JsonElement element, params string[] fields)
    {
        if (element.ValueKind != JsonValueKind.Object || element.EnumerateObject().Count() != fields.Length || element.EnumerateObject().Any(p => !fields.Contains(p.Name, StringComparer.Ordinal))) throw Invalid();
    }
}
