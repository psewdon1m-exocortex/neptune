using System.Globalization;
using System.Text;
using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

// These declarations supplement already durable original operations. Neither
// construction nor a transport outcome qualifies current L/R or updates B.
public sealed record SmallFileBatchEntry(CycleUploadInput Original, long SourceGeneration,
    string SourceIdentity, string? OriginalUploadId = null);
public sealed record SmallFileBatchItemOutcome(string OperationId, long SourceGeneration,
    string SourceIdentity, string State, CycleUploadStatus? Status, string? Code);
public sealed record SmallFileBatchOutcome(string BatchId, IReadOnlyList<SmallFileBatchItemOutcome> Items);

/// <summary>Immutable, bounded transport plan. The caller must persist its ID,
/// original operations and sealed payloads before sending any request.</summary>
public sealed class SmallFileBatchPlan
{
    public const int MaximumItems = 500, MaximumFileBytes = 256 * 1024;
    public const int MaximumDataBytes = 32 * 1024 * 1024, MaximumMetadataBytes = 4 * 1024 * 1024;
    private const string EmptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private readonly byte[] _manifest, _plan;
    private readonly string[] _hashes;
    public SyncRootBinding Binding { get; }
    public string BatchId { get; }
    public string RulesVersion { get; }
    public long DataBytes { get; }
    public IReadOnlyList<SmallFileBatchEntry> Entries { get; }
    public bool ExecutionJob { get; }

    private SmallFileBatchPlan(SyncRootBinding binding, string batchId, string rules,
        SmallFileBatchEntry[] entries, string[] hashes, long size, byte[] manifest, byte[] plan, bool executionJob = false)
        => (Binding, BatchId, RulesVersion, Entries, _hashes, DataBytes, _manifest, _plan, ExecutionJob)
            = (binding, batchId, rules, Array.AsReadOnly(entries), hashes, size, manifest, plan, executionJob);

    public byte[] EncodeManifest() => (byte[])_manifest.Clone();
    public byte[] EncodePlan() => (byte[])_plan.Clone();
    public bool CanExecuteAsJob => Entries.Count <= 16 && _plan.Length <= 1048576 && Entries.All(e =>
        e.Original.Intent.ParentId == Entries[0].Original.Intent.ParentId && IsExecutionJobInput(e.Original));
    internal static bool IsExecutionJobInput(CycleUploadInput original) => original.Intent.Condition.Kind == "absent"
        && original.Intent.RequireAbsent && original.Intent.TargetId is null && original.Intent.ExpectedVersionId is null
        && LocalCycleWire.Decimal(original.Intent.SizeBytes) > 0;
    public SmallFileBatchPlan AsExecutionJob() => CanExecuteAsJob
        ? new(Binding, BatchId, RulesVersion, Entries.ToArray(), _hashes, DataBytes, _manifest, _plan, true) : throw Invalid();
    // Stored inside the existing immutable, hashed plan_json cell. The wire
    // plan remains byte-for-byte unchanged and old rows retain their protocol.
    internal byte[] EncodePersisted() => ExecutionJob
        ? Encode(new { schema = "neptune.small-execution-job.v1", plan_base64 = Convert.ToBase64String(_plan) }) : EncodePlan();
    internal static SmallFileBatchPlan RestorePersisted(ReadOnlyMemory<byte> bytes, SyncRootBinding binding)
    {
        using var doc = SyncProtocolReader.Parse(bytes); var top = doc.RootElement;
        if (!top.TryGetProperty("schema", out _)) return Restore(bytes, binding);
        Fields(top, "schema", "plan_base64");
        if (Text(top, "schema") != "neptune.small-execution-job.v1") throw Invalid();
        var text = Text(top, "plan_base64"); if (text.Length > 1398104) throw Invalid();
        var plan = Convert.FromBase64String(text); if (Convert.ToBase64String(plan) != text) throw Invalid();
        return Restore(plan, binding).AsExecutionJob();
    }
    public byte[] EncodeStatusRequest() => Encode(new { binding = LocalCycleWire.BindingObject(Binding), batch_id = BatchId });
    private static InvalidDataException Invalid() => new("Invalid small file batch binding, bounds or original outcome.");
    private static bool Opaque(string value) => value.Length is > 0 and <= 128 && value.All(c => c is >= '!' and <= '~');
    private static bool Token(string value, int minimum) => value.Length >= minimum && value.Length <= 128
        && char.IsAsciiLetterOrDigit(value[0]) && value.All(c => char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or ':' or '-');
    private static string Decimal(long value) => value.ToString(CultureInfo.InvariantCulture);

    /// <summary>Reopens a persisted exact wire plan. Stored server UUIDs remain
    /// in the original operation store and must be rebound separately.</summary>
    public static SmallFileBatchPlan Restore(ReadOnlyMemory<byte> bytes, SyncRootBinding expectedBinding)
    {
        using var document = SyncProtocolReader.Parse(bytes); var top = document.RootElement;
        Fields(top, "manifest", "inputs"); var manifest = top.GetProperty("manifest");
        Fields(manifest, "protocol", "descriptor_schema", "server_identity", "root_id", "server_epoch", "policy_version", "binding_version", "schema", "batch_id", "rules_version", "items");
        if (Text(manifest, "schema") != "saturn.small-file-batch.v1" || SyncProtocolReader.Binding(manifest) != expectedBinding) throw Invalid();
        var items = manifest.GetProperty("items"); var inputs = top.GetProperty("inputs");
        if (items.ValueKind != JsonValueKind.Array || inputs.ValueKind != JsonValueKind.Array || items.GetArrayLength() is < 1 or > MaximumItems
            || items.GetArrayLength() != inputs.GetArrayLength()) throw Invalid();
        var entries = new SmallFileBatchEntry[items.GetArrayLength()];
        for (var n = 0; n < entries.Length; n++)
        {
            var item = items[n]; Fields(item, "operation_id", "source_generation", "source_identity", "parent_id", "name", "size_bytes", "sha256", "expected");
            var input = JsonSerializer.Deserialize<CycleUploadInput>(inputs[n], LocalCycleWire.Json) ?? throw Invalid();
            entries[n] = new(input, LocalCycleWire.Decimal(Text(item, "source_generation")), Text(item, "source_identity"));
        }
        var result = Create(expectedBinding, Text(manifest, "batch_id"), Text(manifest, "rules_version"), entries);
        // Exact value equality also rejects unmapped/defaulted fields, wrong
        // condition shape and any drift between item and original V2 input.
        using var canonical = SyncProtocolReader.Parse(result._plan);
        if (!JsonElement.DeepEquals(top, canonical.RootElement)) throw Invalid();
        // The server can pin the exact accepted bytes. Validation may compare
        // values, but restoring an original must never re-encode its identity.
        return new(result.Binding, result.BatchId, result.RulesVersion, result.Entries.ToArray(),
            result._hashes, result.DataBytes, result._manifest, bytes.ToArray());
    }

    public SmallFileBatchPlan WithOriginalUploadIds(IReadOnlyDictionary<string, string> originals)
    {
        if (originals.Count > Entries.Count || originals.Keys.Any(key => !Entries.Any(e => e.Original.Intent.IdempotencyKey == key))) throw Invalid();
        var bound = Create(Binding, BatchId, RulesVersion, Entries.Select(e => originals.TryGetValue(e.Original.Intent.IdempotencyKey, out var id)
            ? e.OriginalUploadId is not null && e.OriginalUploadId != id ? throw Invalid() : e with { OriginalUploadId = id }
            : e).ToArray());
        return new(Binding, BatchId, RulesVersion, bound.Entries.ToArray(), bound._hashes, DataBytes, _manifest, _plan, ExecutionJob);
    }

    public static SmallFileBatchPlan Create(SyncRootBinding binding, string batchId, string rulesVersion,
        IReadOnlyList<SmallFileBatchEntry> originals)
    {
        using var parsedBinding = SyncProtocolReader.Parse(LocalCycleWire.BindingBytes(binding));
        if (SyncProtocolReader.Binding(parsedBinding.RootElement) != binding || !LocalCycleWire.Uuid(binding.RootId)
            || !LocalCycleWire.Uuid(binding.ServerEpoch) || !LocalCycleWire.Uuid(binding.BindingVersion)
            || !Token(batchId, 1) || !Opaque(rulesVersion) || originals.Count is < 1 or > MaximumItems) throw Invalid();
        var entries = new SmallFileBatchEntry[originals.Count]; var hashes = new string[entries.Length];
        var operations = new HashSet<string>(StringComparer.Ordinal); var destinations = new HashSet<(string, string)>();
        long total = 0; string? device = null, backend = null;
        for (var n = 0; n < entries.Length; n++)
        {
            var entry = originals[n]; var input = entry.Original;
            // Freeze the one potentially mutable collection before fingerprints
            // or encoded bytes are retained; record strings/conditions are immutable.
            input = input with { Intent = input.Intent with { ParentTopology = Array.AsReadOnly(input.Intent.ParentTopology.ToArray()) } };
            var i = input.Intent; var size = LocalCycleWire.Decimal(i.SizeBytes);
            hashes[n] = LocalCycleWire.IntentHash(input);
            // The individual server intent retains its own 32 KiB ceiling.
            // Counting our encoded representation is conservative for escaped
            // Unicode and never admits a larger original into the batch path.
            using var originalJson = JsonDocument.Parse(Encode(LocalCycleWire.InputObject(input)));
            if (Encoding.UTF8.GetByteCount(originalJson.RootElement.GetProperty("intent").GetRawText()) > 32768
                || i.IfMatch?.Length > 100) throw Invalid();
            ValidateScalar(i.RequestPath); if (i.IfMatch is not null) ValidateScalar(i.IfMatch);
            if (entry.SourceGeneration < 1 || !Opaque(entry.SourceIdentity) || !Token(i.IdempotencyKey, 8)
                || entry.OriginalUploadId is not null && !LocalCycleWire.Uuid(entry.OriginalUploadId)
                || input.Epoch != binding.ServerEpoch || input.BindingVersion != binding.BindingVersion || i.RootId != binding.RootId
                || !Opaque(input.BackendIdentity) || input.BackendIdentity.Any(c => !(char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or ':' or '-'))
                || device is not null && device != i.DeviceId || backend is not null && backend != input.BackendIdentity
                || size > MaximumFileBytes || size == 0 && i.Sha256 != EmptyHash
                || !operations.Add(i.IdempotencyKey) || !destinations.Add((i.ParentId, i.Name))) throw Invalid();
            total = checked(total + size); if (total > MaximumDataBytes) throw Invalid();
            device = i.DeviceId; backend = input.BackendIdentity; entries[n] = entry with { Original = input };
        }
        var manifest = new { protocol = binding.Protocol, descriptor_schema = binding.DescriptorSchema, server_identity = binding.ServerIdentity,
            root_id = binding.RootId, server_epoch = binding.ServerEpoch, policy_version = binding.PolicyVersion, binding_version = binding.BindingVersion,
            schema = "saturn.small-file-batch.v1", batch_id = batchId, rules_version = rulesVersion,
            items = entries.Select(entry => new { operation_id = entry.Original.Intent.IdempotencyKey, source_generation = Decimal(entry.SourceGeneration),
                source_identity = entry.SourceIdentity, parent_id = entry.Original.Intent.ParentId, name = entry.Original.Intent.Name,
                size_bytes = entry.Original.Intent.SizeBytes, sha256 = entry.Original.Intent.Sha256,
                expected = entry.Original.Intent.Condition.Kind == "absent" ? (object)new { kind = "absent" }
                    : new { kind = "version", node_id = entry.Original.Intent.Condition.NodeId, version = entry.Original.Intent.Condition.MetadataVersion } }).ToArray() };
        var manifestBytes = Encode(manifest);
        var planBytes = Encode(new { manifest, inputs = entries.Select(entry => LocalCycleWire.InputObject(entry.Original)).ToArray() });
        return new(binding, batchId, rulesVersion, entries, hashes, total, manifestBytes, planBytes);
    }

    /// <summary>Decodes transport evidence only. Original receipt validation is
    /// shared with individual uploads; the existing engine still owns adoption.</summary>
    public SmallFileBatchOutcome ReadOutcome(ReadOnlyMemory<byte> bytes)
    {
        using var document = SyncProtocolReader.Parse(bytes); var top = document.RootElement;
        Fields(top, "protocol", "descriptor_schema", "server_identity", "root_id", "server_epoch", "policy_version", "binding_version", "schema", "batch_id", "items");
        if (SyncProtocolReader.Binding(top) != Binding || Text(top, "schema") != "saturn.small-file-batch-result.v1" || Text(top, "batch_id") != BatchId) throw Invalid();
        var items = top.GetProperty("items"); if (items.ValueKind != JsonValueKind.Array || items.GetArrayLength() != Entries.Count) throw Invalid();
        var result = new SmallFileBatchItemOutcome[Entries.Count];
        for (var n = 0; n < result.Length; n++)
        {
            var raw = items[n]; var entry = Entries[n]; var state = Text(raw, "state");
            if (Text(raw, "operation_id") != entry.Original.Intent.IdempotencyKey || Text(raw, "source_generation") != Decimal(entry.SourceGeneration)
                || Text(raw, "source_identity") != entry.SourceIdentity) throw Invalid();
            CycleUploadStatus? status = null; string? code = null;
            if (raw.TryGetProperty("status", out var wireStatus))
            {
                Fields(raw, "operation_id", "source_generation", "source_identity", "state", "status");
                // This checks server UUID, full original intent, all receipt and
                // historical descriptor fields, content SHA and exact byte count.
                status = LocalCycleWire.Status(Encoding.UTF8.GetBytes(wireStatus.GetRawText()), entry.Original, _hashes[n], entry.OriginalUploadId);
                var expected = status.Phase switch { "published" => "PUBLISHED", "not_published" => "NOT_PUBLISHED",
                    "physical_pending" or "reconciliation_required" => "UNRESOLVED", _ => "PENDING" };
                if (state != expected) throw Invalid();
            }
            else if (state == "MISSING")
            {
                Fields(raw, "operation_id", "source_generation", "source_identity", "state");
                if (entry.OriginalUploadId is not null) throw Invalid();
            }
            else
            {
                Fields(raw, "operation_id", "source_generation", "source_identity", "state", "code"); code = Text(raw, "code");
                var conflict = code is "operation_reused" or "membership_changed" or "precondition_failed";
                if (state != (conflict ? "CONFLICT" : "UNRESOLVED") || !conflict && code is not ("reconciliation_required" or "admission_full"
                    or "transfer_unresolved" or "verification_failed" or "upload_too_large" or "counter_exhausted" or "sync_internal_error")) throw Invalid();
            }
            result[n] = new(entry.Original.Intent.IdempotencyKey, entry.SourceGeneration, entry.SourceIdentity, state, status, code);
        }
        return new(BatchId, Array.AsReadOnly(result));
    }

    private static string Text(JsonElement element, string key) => element.ValueKind == JsonValueKind.Object
        && element.TryGetProperty(key, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString()! : throw Invalid();
    private static void Fields(JsonElement element, params string[] names)
    {
        if (element.ValueKind != JsonValueKind.Object || element.EnumerateObject().Count() != names.Length
            || element.EnumerateObject().Any(p => !names.Contains(p.Name, StringComparer.Ordinal))) throw Invalid();
    }
    private static byte[] Encode(object value)
    {
        using var stream = new MetadataBuffer(); JsonSerializer.Serialize(stream, value); return stream.ToArray();
    }
    private static void ValidateScalar(string value)
    {
        for (var n = 0; n < value.Length; n++)
            if (char.IsHighSurrogate(value[n])) { if (++n >= value.Length || !char.IsLowSurrogate(value[n])) throw Invalid(); }
            else if (char.IsLowSurrogate(value[n])) throw Invalid();
    }
    private sealed class MetadataBuffer : MemoryStream
    {
        private void Require(int count) { if (Length > MaximumMetadataBytes - count) throw Invalid(); }
        public override void Write(byte[] buffer, int offset, int count) { Require(count); base.Write(buffer, offset, count); }
        public override void Write(ReadOnlySpan<byte> buffer) { Require(buffer.Length); base.Write(buffer); }
        public override void WriteByte(byte value) { Require(1); base.WriteByte(value); }
    }
}
