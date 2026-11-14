using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    private static string ReorderNative18Receipt(string raw)
    {
        using var doc = JsonDocument.Parse(raw);
        using var bytes = new MemoryStream();
        using (var writer = new Utf8JsonWriter(bytes, new() { Indented = true, Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping }))
            Write(doc.RootElement, writer);
        return Encoding.UTF8.GetString(bytes.ToArray());
        static void Write(JsonElement value, Utf8JsonWriter writer)
        {
            if (value.ValueKind == JsonValueKind.Object)
            {
                writer.WriteStartObject();
                foreach (var p in value.EnumerateObject().Reverse()) { writer.WritePropertyName(p.Name); Write(p.Value, writer); }
                writer.WriteEndObject();
            }
            else if (value.ValueKind == JsonValueKind.Array)
            { writer.WriteStartArray(); foreach (var item in value.EnumerateArray()) Write(item, writer); writer.WriteEndArray(); }
            else value.WriteTo(writer);
        }
    }

    [Fact]
    public async Task Native18FourteenPublishedSixMissingRecoverWithoutRecaptureAndRejectChangedReceipts()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Requires Windows native filesystem fixture");
        var clock = new Stage3Clock();
        await using var f = await WindowsFreshCycleTests.Fixture.Create(clock);
        for (var n = 0; n < 20; n++)
            File.WriteAllText(Path.Combine(f.Source, $"Агентность-{n:D2}.md"), $"original {n}");
        await Medium(async () =>
        {
            var remote = new BatchRemote(f.Remote)
            {
                PublicationPrefix = plan => plan.Entries.Count == 16 ? 14 : 0,
                WireJsonEscaping = true
            };
            string scope;
            object? originals, receipts;
            await using (var engine = await OpenBatchEngine(f, remote))
            {
                scope = engine.ScopeId;
                Assert.True(await engine.EnableSmallFileBatchQualificationAsync(Ct));
                var first = await engine.InitialSyncAsync(Ct);
                Assert.Equal(20, first.CapturedFiles);
                Assert.Equal(14, first.PublishedFiles);
                Assert.Equal(2L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches WHERE phase=2"));
                Assert.Equal(6L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE input_json IS NOT NULL AND upload_id IS NULL"));
                Assert.Equal(6L, await f.Scalar("SELECT COUNT(*) FROM client_operation_schedule WHERE reason='outcome_pending'"));
                Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM client_operation_schedule WHERE operation_id='cycle'"));
                originals = await f.Scalar("SELECT json_group_array(json_array(operation_id,input_json,intent_hash)) FROM(SELECT * FROM local_cycle_work ORDER BY work_no)");
                receipts = await f.Scalar("SELECT json_group_array(json_array(operation_id,result_json)) FROM(SELECT * FROM local_cycle_work WHERE result_json IS NOT NULL ORDER BY work_no)");
                await f.Evidence("native18-exact-partial", new { first, remote.BatchSizes });
            }
            await using var resumed = await OpenBatchEngine(f, remote, resume: true);
            Assert.Equal(scope, resumed.ScopeId);
            remote.PublicationPrefix = null;
            var mutations = new Dictionary<string, Action<JsonNode>>
            {
                ["sha256"] = n => n["publicationReceipt"]!["sha256"] = new string('f', 64),
                ["size"] = n => n["publicationReceipt"]!["sizeBytes"] = "999999",
                ["uploadId"] = n => n["uploadId"] = Guid.NewGuid().ToString("D"),
                ["operationId"] = n => n["operationId"] = Guid.NewGuid().ToString("D"),
                ["rootId"] = n => n["rootId"] = Guid.NewGuid().ToString("D"),
                ["intentHash"] = n => n["intentHash"] = new string('e', 64),
                ["contentVersion"] = n => n["publicationReceipt"]!["versionId"] = Guid.NewGuid().ToString("D"),
                ["metadataVersion"] = n => n["publishedDescription"]!["version"] = Guid.NewGuid().ToString("D"),
                ["sequence"] = n => n["lastSeq"] = "999999",
                ["receipt"] = n => n["publicationReceipt"]!["publishedAt"] = "2030-01-01T00:00:00.000Z",
                ["unknownField"] = n => n["unexpected"] = true
            };
            foreach (var (name, mutate) in mutations)
            {
                remote.ReceiptRewrite = raw => { var node = JsonNode.Parse(raw)!; mutate(node); return node.ToJsonString(); };
                var rejected = await f.FreshWhenDueAsync(resumed, Ct);
                Assert.Contains("Batch status changed a durable terminal original", rejected.Error);
                Assert.Equal(14, f.Remote.Prepares);
                Assert.Equal(receipts, await f.Scalar("SELECT json_group_array(json_array(operation_id,result_json)) FROM(SELECT * FROM local_cycle_work WHERE result_json IS NOT NULL ORDER BY work_no)"));
                await f.Evidence("native18-reject-" + name, rejected);
            }
            remote.ReceiptRewrite = raw => raw.Insert(1, "\"schema\":\"saturn.catalog-upload-result.v2\",");
            Assert.NotNull((await f.FreshWhenDueAsync(resumed, Ct)).Error);
            Assert.Equal(14, f.Remote.Prepares);
            remote.ReceiptRewrite = ReorderNative18Receipt;
            var recovered = await f.FreshWhenDueAsync(resumed, Ct);
            Assert.Null(recovered.Error);
            Assert.Equal("NoWorkAtAppliedEventBoundary", recovered.Decision);
            Assert.Equal(0, recovered.CapturedFiles);
            Assert.Equal(20, f.Remote.Prepares);
            Assert.Equal(20, f.Remote.Completes);
            Assert.Equal(originals, await f.Scalar("SELECT json_group_array(json_array(operation_id,input_json,intent_hash)) FROM(SELECT * FROM local_cycle_work ORDER BY work_no)"));
            Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_batches"));
            Assert.Equal(20, Directory.GetFiles(f.Source).Length);
            await f.Evidence("native18-exact-recovered", new { recovered, changedReceiptCases = mutations.Count + 1, localCopies = 0 });
            return true;
        });
    }

    [Fact]
    public void Native18RecordedReceiptsPassProductValidationWithoutChangingOriginals()
    {
        var directory = Environment.GetEnvironmentVariable("NEPTUNE_NATIVE18_EVIDENCE_ROOT");
        Assert.SkipUnless(directory is not null, "Requires the retained native18 diagnostic fixture");
        using var local = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(directory!, "local-batch-originals.json")));
        using var server = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(directory!, "server-batch-originals.json")));
        var rows = server.RootElement.EnumerateArray().ToDictionary(r => r.GetProperty("idempotency_key").GetString()!);
        var verified = 0; var missing = 0;
        foreach (var item in local.RootElement.GetProperty("items").EnumerateArray())
        {
            var input = JsonSerializer.Deserialize<CycleUploadInput>(item.GetProperty("input_json").GetString()!, LocalCycleWire.Json)!;
            if (!rows.TryGetValue(input.Intent.IdempotencyKey, out var result)) { missing++; continue; }
            var raw = result.GetProperty("sync_result").GetRawText();
            var saved = item.GetProperty("result_json").GetString()!;
            Assert.NotEqual(saved, raw);
            Assert.True(LocalCycleWire.SameResult(saved, raw));
            Assert.True(LocalCycleWire.SameResult(saved, ReorderNative18Receipt(raw)));
            var hash = item.GetProperty("intent_hash").GetString()!;
            var upload = item.GetProperty("upload_id").GetString()!;
            var body = JsonSerializer.SerializeToUtf8Bytes(new
            {
                session = new { uploadId = upload, operationId = upload, rootId = input.Intent.RootId, intentHash = hash,
                    expectedSizeBytes = input.Intent.SizeBytes, receivedSizeBytes = item.GetProperty("received").GetInt64().ToString(System.Globalization.CultureInfo.InvariantCulture), phase = "published", transferPending = false },
                result = result.GetProperty("sync_result"), replayed = true
            });
            var validated = LocalCycleWire.Status(body, input, hash, upload);
            Assert.True(LocalCycleWire.SameResult(saved, validated.ResultJson));
            verified++;
        }
        Assert.Equal(14, verified);
        Assert.Equal(6, missing);
    }
}
