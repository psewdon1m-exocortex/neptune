using System.Globalization;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class SmallFileBatchProtocolTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static string Sha(string text) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(text)));
    private static string Hash(CycleUploadInput input) => (string)typeof(NeptuneStateStore).Assembly.GetType("Neptune.Core.LocalCycleWire")!
        .GetMethod("IntentHash", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [input])!;
    private static SmallFileBatchEntry Entry(WindowsFreshCycleTests.FakeRemote r, int index = 0, int size = 3) => new(
        new(r.Binding.ServerEpoch, r.Binding.BindingVersion, "local-test", new("saturn.upload-intent.v2", "windows_sync", r.Device, r.Binding.RootId,
            "/café😀-" + index, "neptune-cycle-" + Guid.NewGuid().ToString("D"), r.Binding.RootId, "café😀-" + index, null,
            size.ToString(CultureInfo.InvariantCulture), Sha(size == 0 ? "" : "abc"), null, "*", true, null, new("absent"), [new(r.Binding.RootId, null, "root")])),
        7, "native-capture-" + index);
    private static SmallFileBatchPlan Plan(WindowsFreshCycleTests.FakeRemote r, params SmallFileBatchEntry[] entries)
        => SmallFileBatchPlan.Create(r.Binding, "batch-" + Guid.NewGuid().ToString("D"), "rules-42", entries.Length == 0 ? [Entry(r)] : entries);
    private static JsonObject Outcome(SmallFileBatchPlan plan, params JsonObject[] items)
    {
        var result = JsonNode.Parse(plan.EncodeManifest())!.AsObject(); result.Remove("rules_version");
        result["schema"] = "saturn.small-file-batch-result.v1";
        result["items"] = new JsonArray((items.Length == 0 ? plan.Entries.Select(e => Item(e)).ToArray() : items).Select(i => (JsonNode)i).ToArray());
        return result;
    }
    private static JsonObject Item(SmallFileBatchEntry entry, string state = "MISSING") => new()
    {
        ["operation_id"] = entry.Original.Intent.IdempotencyKey, ["source_generation"] = entry.SourceGeneration.ToString(CultureInfo.InvariantCulture),
        ["source_identity"] = entry.SourceIdentity, ["state"] = state,
    };
    private static object Status(CycleUploadStatus s)
    {
        var session = new { uploadId = s.UploadId, operationId = s.OperationId, rootId = s.RootId, intentHash = s.IntentHash,
            expectedSizeBytes = s.ExpectedBytes.ToString(CultureInfo.InvariantCulture), receivedSizeBytes = s.ReceivedBytes.ToString(CultureInfo.InvariantCulture),
            phase = s.Phase, transferPending = s.TransferPending };
        return s.ResultJson is null ? new { session, replayed = s.Replayed }
            : (object)new { session, result = JsonSerializer.Deserialize<JsonElement>(s.ResultJson), replayed = s.Replayed };
    }
    private static byte[] Bytes(JsonNode node) => JsonSerializer.SerializeToUtf8Bytes(node);

    [Fact]
    public void RestoreAndServerIdentityBindingPreserveExactWireBytes()
    {
        var remote = new WindowsFreshCycleTests.FakeRemote(); var plan = Plan(remote);
        var json = JsonNode.Parse(plan.EncodePlan())!.AsObject();
        var reordered = new JsonObject { ["inputs"] = json["inputs"]!.DeepClone(), ["manifest"] = json["manifest"]!.DeepClone() };
        var bytes = JsonSerializer.SerializeToUtf8Bytes(reordered, new JsonSerializerOptions { WriteIndented = true });
        var expected = bytes.ToArray(); var restored = SmallFileBatchPlan.Restore(bytes, remote.Binding);
        Array.Fill<byte>(bytes, 0);
        Assert.Equal(expected, restored.EncodePlan());
        var bound = restored.WithOriginalUploadIds(new Dictionary<string, string>
            { [plan.Entries[0].Original.Intent.IdempotencyKey] = Guid.NewGuid().ToString("D") });
        Assert.Equal(expected, bound.EncodePlan());
        Assert.Equal(restored.EncodeManifest(), bound.EncodeManifest());
        Assert.NotNull(bound.Entries[0].OriginalUploadId);
        var job = bound.AsExecutionJob();
        var persisted = (byte[])typeof(SmallFileBatchPlan).GetMethod("EncodePersisted", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(job, null)!;
        var reopened = (SmallFileBatchPlan)typeof(SmallFileBatchPlan).GetMethod("RestorePersisted", BindingFlags.Static | BindingFlags.NonPublic)!
            .Invoke(null, [new ReadOnlyMemory<byte>(persisted), remote.Binding])!;
        Assert.True(reopened.ExecutionJob); Assert.Equal(expected, reopened.EncodePlan());
        Assert.True(reopened.WithOriginalUploadIds(new Dictionary<string, string>()).ExecutionJob);
    }

    [Fact]
    public void ExactImmutablePlanPreservesFullIntentAndFreezesCallerTopologyAndReturnedBytes()
    {
        var remote = new WindowsFreshCycleTests.FakeRemote(); var source = Entry(remote);
        var topology = source.Original.Intent.ParentTopology.ToList();
        source = source with { Original = source.Original with { Intent = source.Original.Intent with { ParentTopology = topology } }, SourceGeneration = long.MaxValue };
        var plan = Plan(remote, source); var original = plan.EncodePlan(); topology.Clear();
        var tampered = plan.EncodePlan(); Array.Fill<byte>(tampered, 0);
        Assert.Equal(original, plan.EncodePlan()); Assert.Single(plan.Entries[0].Original.Intent.ParentTopology);
        using var parsed = JsonDocument.Parse(original); var item = parsed.RootElement.GetProperty("manifest").GetProperty("items")[0];
        Assert.Equal("9223372036854775807", item.GetProperty("source_generation").GetString()); Assert.Equal("café😀-0", item.GetProperty("name").GetString());
        var intent = parsed.RootElement.GetProperty("inputs")[0].GetProperty("intent"); Assert.Equal(17, intent.EnumerateObject().Count());
        Assert.Equal(source.Original.Intent.IdempotencyKey, intent.GetProperty("idempotencyKey").GetString()); Assert.Single(intent.GetProperty("parentTopology").EnumerateArray());
        Assert.Equal(3, plan.DataBytes); Assert.Single(plan.ReadOutcome(Bytes(Outcome(plan))).Items);
        var artifact = Environment.GetEnvironmentVariable("NEPTUNE_BATCH_WIRE_FIXTURE");
        if (artifact is not null)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(artifact)!); using var file = new FileStream(artifact, FileMode.CreateNew); file.Write(original);
            using var hashFile = new FileStream(artifact + ".intent-sha256", FileMode.CreateNew); hashFile.Write(Encoding.ASCII.GetBytes(Hash(plan.Entries[0].Original)));
        }
    }

    [Theory]
    [InlineData("generation")][InlineData("identity")][InlineData("rules")][InlineData("batch")][InlineData("operation")]
    [InlineData("epoch")][InlineData("binding")][InlineData("root")][InlineData("backend")][InlineData("upload")]
    [InlineData("oversize")][InlineData("zero-hash")][InlineData("name")][InlineData("path-scalar")]
    public void RejectsInvalidOriginalBeforeEncoding(string change)
    {
        var remote = new WindowsFreshCycleTests.FakeRemote(); var entry = Entry(remote); var input = entry.Original; var intent = input.Intent;
        if (change == "generation") entry = entry with { SourceGeneration = 0 };
        if (change == "identity") entry = entry with { SourceIdentity = "not canonical" };
        if (change == "operation") intent = intent with { IdempotencyKey = "neptune\r\nContent-ID: leaked" };
        if (change == "epoch") input = input with { Epoch = Guid.NewGuid().ToString("D") };
        if (change == "binding") input = input with { BindingVersion = Guid.NewGuid().ToString("D") };
        if (change == "root") intent = intent with { RootId = Guid.NewGuid().ToString("D") };
        if (change == "backend") input = input with { BackendIdentity = "malformed/backend" };
        if (change == "upload") entry = entry with { OriginalUploadId = "local-operation-id" };
        if (change == "oversize") intent = intent with { SizeBytes = "262145" };
        if (change == "zero-hash") intent = intent with { SizeBytes = "0" };
        if (change == "name") intent = intent with { Name = "a\ud800b" };
        if (change == "path-scalar") intent = intent with { RequestPath = "a\ud800b" };
        entry = entry with { Original = input with { Intent = intent } };
        Assert.Throws<InvalidDataException>(() => SmallFileBatchPlan.Create(remote.Binding, change == "batch" ? "bad batch" : "batch-1",
            change == "rules" ? "rules\n2" : "rules-v1", [entry]));
    }

    [Fact]
    public void BoundsItemsBytesDuplicatesAndCredentialMixBeforeTransport()
    {
        var r = new WindowsFreshCycleTests.FakeRemote();
        Assert.Throws<InvalidDataException>(() => SmallFileBatchPlan.Create(r.Binding, "batch", "rules", []));
        Assert.Equal(500, Plan(r, Enumerable.Range(0, 500).Select(i => Entry(r, i, 0)).ToArray()).Entries.Count);
        Assert.Throws<InvalidDataException>(() => Plan(r, Enumerable.Range(0, 501).Select(i => Entry(r, i, 0)).ToArray()));
        Assert.Equal(32 * 1024 * 1024, Plan(r, Enumerable.Range(0, 128).Select(i => Entry(r, i, 256 * 1024)).ToArray()).DataBytes);
        Assert.Throws<InvalidDataException>(() => Plan(r, Enumerable.Range(0, 129).Select(i => Entry(r, i, 256 * 1024)).ToArray()));
        var a = Entry(r); var b = Entry(r, 1);
        Assert.Throws<InvalidDataException>(() => Plan(r, a, a));
        Assert.Throws<InvalidDataException>(() => Plan(r, a, b with { Original = b.Original with { Intent = b.Original.Intent with { Name = a.Original.Intent.Name } } }));
        Assert.Throws<InvalidDataException>(() => Plan(r, a, b with { Original = b.Original with { BackendIdentity = "another-backend" } }));
        Assert.Throws<InvalidDataException>(() => Plan(r, a, b with { Original = b.Original with { Intent = b.Original.Intent with { DeviceId = Guid.NewGuid().ToString("D") } } }));
    }

    [Fact]
    public void StopsPlanEncodingAtMetadataBudgetEvenWithinFileAndCountLimits()
    {
        var r = new WindowsFreshCycleTests.FakeRemote(); var ids = Enumerable.Range(0, 128).Select(_ => Guid.NewGuid().ToString("D")).Append(r.Binding.RootId).ToArray();
        var topology = ids.Select((id, n) => new CatalogParentTopology(id, n == ids.Length - 1 ? null : ids[n + 1], new string('x', 128))).ToArray();
        var entries = Enumerable.Range(0, 160).Select(n => { var e = Entry(r, n, 0); return e with { Original = e.Original with { Intent = e.Original.Intent with { ParentId = ids[0], ParentTopology = topology } } }; }).ToArray();
        Assert.Single(Plan(r, entries[0]).Entries); // Each original remains individually admissible.
        Assert.Throws<InvalidDataException>(() => Plan(r, entries));
    }

    [Theory]
    [InlineData("valid")][InlineData("manifest-hash")][InlineData("generation")][InlineData("input-extra")]
    [InlineData("input-missing-null")][InlineData("condition-extra")][InlineData("topology-extra")][InlineData("rules-missing")]
    [InlineData("expected")][InlineData("binding")]
    public void PersistedPlanRestoresOnlyItsCompleteCanonicalOriginals(string mutation)
    {
        var r = new WindowsFreshCycleTests.FakeRemote(); var plan = Plan(r); var json = JsonNode.Parse(plan.EncodePlan())!;
        var manifest = json["manifest"]!; var input = json["inputs"]![0]!;
        switch (mutation)
        {
            case "manifest-hash": manifest["items"]![0]!["sha256"] = Sha("other"); break;
            case "generation": manifest["items"]![0]!["source_generation"] = "07"; break;
            case "input-extra": input["unrecognized"] = 1; break;
            case "input-missing-null": input["intent"]!.AsObject().Remove("ifMatch"); break;
            case "condition-extra": input["intent"]!["condition"]!["nodeId"] = null; break;
            case "topology-extra": input["intent"]!["parentTopology"]![0]!["unexpected"] = false; break;
            case "rules-missing": manifest.AsObject().Remove("rules_version"); break;
            case "expected": manifest["items"]![0]!["expected"]!["kind"] = "present"; break;
            case "binding": manifest["binding_version"] = Guid.NewGuid().ToString("D"); break;
        }
        if (mutation == "valid")
        {
            var restored = SmallFileBatchPlan.Restore(Bytes(json), r.Binding); Assert.Equal(plan.EncodePlan(), restored.EncodePlan());
            var key = restored.Entries[0].Original.Intent.IdempotencyKey; var serverId = Guid.NewGuid().ToString("D");
            var bound = restored.WithOriginalUploadIds(new Dictionary<string, string> { [key] = serverId });
            Assert.Equal(restored.EncodePlan(), bound.EncodePlan()); Assert.Equal(serverId, bound.Entries[0].OriginalUploadId);
            Assert.Throws<InvalidDataException>(() => bound.WithOriginalUploadIds(new Dictionary<string, string> { [key] = Guid.NewGuid().ToString("D") }));
            Assert.Throws<InvalidDataException>(() => restored.WithOriginalUploadIds(new Dictionary<string, string> { ["unknown-original"] = serverId }));
        }
        else Assert.ThrowsAny<Exception>(() => SmallFileBatchPlan.Restore(Bytes(json), r.Binding));
    }

    [Theory]
    [InlineData("generation")][InlineData("generation-number")][InlineData("identity")][InlineData("operation")][InlineData("batch")]
    [InlineData("binding")][InlineData("schema")][InlineData("extra")][InlineData("item-extra")][InlineData("missing")]
    [InlineData("reorder")][InlineData("duplicate")][InlineData("synthetic-published")][InlineData("missing-with-status")]
    [InlineData("unknown-code")][InlineData("wrong-conflict-state")]
    public void ResponseCannotRebindAnyOriginalOrInventSuccess(string mutation)
    {
        var r = new WindowsFreshCycleTests.FakeRemote(); var plan = Plan(r, Entry(r), Entry(r, 1)); var body = Outcome(plan); var item = body["items"]![0]!;
        switch (mutation)
        {
            case "generation": item["source_generation"] = "8"; break;
            case "generation-number": item["source_generation"] = 7; break;
            case "identity": item["source_identity"] = "another-native-capture"; break;
            case "operation": item["operation_id"] = "neptune-cycle-another"; break;
            case "batch": body["batch_id"] = "another-batch"; break;
            case "binding": body["binding_version"] = Guid.NewGuid().ToString("D"); break;
            case "schema": body["schema"] = "saturn.small-file-batch-result.v2"; break;
            case "extra": body["future"] = true; break;
            case "item-extra": item["accepted"] = true; break;
            case "missing": body["items"]!.AsArray().RemoveAt(1); break;
            case "reorder": body["items"] = new JsonArray(Item(plan.Entries[1]), Item(plan.Entries[0])); break;
            case "duplicate": body["items"] = new JsonArray(Item(plan.Entries[0]), Item(plan.Entries[0])); break;
            case "synthetic-published": item["state"] = "PUBLISHED"; break;
            case "missing-with-status": item["status"] = new JsonObject(); break;
            case "unknown-code": item["state"] = "UNRESOLVED"; item["code"] = "silently_accepted"; break;
            case "wrong-conflict-state": item["state"] = "PUBLISHED"; item["code"] = "precondition_failed"; break;
        }
        Assert.ThrowsAny<Exception>(() => plan.ReadOutcome(Bytes(body)));
    }

    [Fact]
    public async Task PreservesPublishedReceiptAlongsideMissingConflictAndPendingOriginals()
    {
        var r = new WindowsFreshCycleTests.FakeRemote(); var entries = Enumerable.Range(0, 4).Select(i => Entry(r, i)).ToArray();
        var a = entries[0].Original; var prepare = await r.UploadAsync("prepare", a, null, Hash(a), Ct);
        await r.AppendAsync(a, prepare.UploadId, Hash(a), 0, Encoding.UTF8.GetBytes("abc"), Ct);
        var published = await r.UploadAsync("complete", a, prepare.UploadId, Hash(a), Ct);
        entries[0] = entries[0] with { OriginalUploadId = prepare.UploadId };
        var d = entries[3].Original; var pending = await r.UploadAsync("prepare", d, null, Hash(d), Ct);
        var plan = Plan(r, entries); var items = entries.Select(e => Item(e)).ToArray();
        items[0]["state"] = "PUBLISHED"; items[0]["status"] = JsonSerializer.SerializeToNode(Status(published));
        items[2]["state"] = "CONFLICT"; items[2]["code"] = "precondition_failed";
        items[3]["state"] = "PENDING"; items[3]["status"] = JsonSerializer.SerializeToNode(Status(pending));
        var outcome = plan.ReadOutcome(Bytes(Outcome(plan, items)));
        Assert.Equal(new[] { "PUBLISHED", "MISSING", "CONFLICT", "PENDING" }, outcome.Items.Select(i => i.State));
        Assert.Equal(published.ResultJson, outcome.Items[0].Status!.ResultJson); Assert.Equal(published.UploadId, outcome.Items[0].Status!.UploadId);
        Assert.Null(outcome.Items[1].Status); Assert.Null(outcome.Items[2].Status);
        Assert.Throws<InvalidDataException>(() => plan.ReadOutcome(Bytes(Outcome(plan)))); // Known server UUID cannot become MISSING.
        items[0] = Item(entries[0], "PUBLISHED"); items[0]["status"] = JsonSerializer.SerializeToNode(Status(published));
        items[0]["status"]!["result"]!["publicationReceipt"]!["sha256"] = Sha("changed");
        Assert.Throws<InvalidDataException>(() => plan.ReadOutcome(Bytes(Outcome(plan, items.Select(i => (JsonObject)i.DeepClone()).ToArray()))));
    }

    [Theory]
    [InlineData("duplicate")][InlineData("utf8")][InlineData("overflow")][InlineData("depth")]
    public void StrictJsonAndResponseBudgetApplyBeforeProjection(string mutation)
    {
        var r = new WindowsFreshCycleTests.FakeRemote(); var plan = Plan(r);
        var raw = mutation switch { "duplicate" => Encoding.UTF8.GetBytes("{\"schema\":\"x\",\"schema\":\"x\"}"), "utf8" => [123, 34, 255, 34, 58, 49, 125],
            "overflow" => new byte[SmallFileBatchPlan.MaximumMetadataBytes + 1], _ => Encoding.UTF8.GetBytes(new string('[', 40) + new string(']', 40)) };
        Assert.ThrowsAny<Exception>(() => plan.ReadOutcome(raw));
    }
}
