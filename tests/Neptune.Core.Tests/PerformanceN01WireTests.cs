using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Nodes;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class PerformanceN01WireTests
{
    private static byte[] PlutoBytes() => File.ReadAllBytes(Path.Combine(AppContext.BaseDirectory,
        "TestData", "performance-n01", "pluto-small-job.json"));

    private static SyncRootBinding Binding(JsonNode plan)
    {
        using var manifest = JsonDocument.Parse(plan["manifest"]!.ToJsonString());
        return SyncProtocolReader.Binding(manifest.RootElement);
    }

    private static JsonNode NeptuneVariant()
    {
        var plan = JsonNode.Parse(PlutoBytes())!;
        foreach (var input in plan["inputs"]!.AsArray()) input!["intent"]!["deviceKind"] = "windows_sync";
        return plan;
    }

    [Fact]
    public void OriginalGoFixtureCannotBorrowNeptuneActorAuthority()
    {
        var bytes = PlutoBytes();
        Assert.Equal("e25ae4db54b2f076e10a2ffc0bd784aab62854a1dc389c4313f5d82b228527df",
            Convert.ToHexStringLower(SHA256.HashData(bytes)));
        Assert.Throws<InvalidDataException>(() => SmallFileBatchPlan.Restore(bytes, Binding(JsonNode.Parse(bytes)!)));
    }

    [Fact]
    public void ExplicitNeptuneVariantPreservesExactPlanBytesAndParentTopology()
    {
        var input = NeptuneVariant(); var bytes = JsonSerializer.SerializeToUtf8Bytes(input);
        var plan = SmallFileBatchPlan.Restore(bytes, Binding(input));
        Assert.Equal(2, plan.Entries.Count);
        Assert.True(plan.CanExecuteAsJob);
        Assert.Equal(bytes, plan.AsExecutionJob().EncodePlan());
        Assert.All(plan.Entries, entry =>
        {
            Assert.Equal("windows_sync", entry.Original.Intent.DeviceKind);
            Assert.Equal(3, entry.Original.Intent.ParentTopology.Count);
        });
    }

    [Theory]
    [InlineData("name", "unrelated")]
    [InlineData("sha256", "0000000000000000000000000000000000000000000000000000000000000000")]
    [InlineData("parentId", "00000000-0000-4000-8000-000000001003")]
    [InlineData("sizeBytes", "4")]
    public void LastMemberCannotDriftFromItsManifest(string field, string value)
    {
        var input = NeptuneVariant(); var binding = Binding(input);
        input["inputs"]![1]!["intent"]![field] = value;
        Assert.Throws<InvalidDataException>(() => SmallFileBatchPlan.Restore(JsonSerializer.SerializeToUtf8Bytes(input), binding));
    }
}
