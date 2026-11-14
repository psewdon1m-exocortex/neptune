using System.Text;
using System.Text.Json;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class SyncSnapshotEntriesReaderTests
{
    private static string CorpusPath => Path.Combine(AppContext.BaseDirectory, "TestData", "SyncWire", "snapshot-pages.v1.json");
    public static TheoryData<string, string?, string, bool> PageCases
    {
        get
        {
            using var document = JsonDocument.Parse(File.ReadAllBytes(CorpusPath));
            var result = new TheoryData<string, string?, string, bool>();
            foreach (var row in document.RootElement.GetProperty("cases").EnumerateArray())
                result.Add(row.GetProperty("name").GetString()!, row.GetProperty("afterNodeId").GetString(),
                    row.GetProperty("inputHex").GetString()!, row.GetProperty("accepted").GetBoolean());
            return result;
        }
    }
    private static SyncSnapshotReady Context(JsonDocument corpus) => SyncProtocolReader.ReadSnapshot(Encoding.UTF8.GetBytes(corpus.RootElement.GetProperty("snapshot").GetRawText()));

    [Theory]
    [MemberData(nameof(PageCases))]
    public void SharedExactRawVector(string name, string? after, string hex, bool expected)
    {
        using var corpus = JsonDocument.Parse(File.ReadAllBytes(CorpusPath));
        var snapshot = Context(corpus); var accepted = true;
        try { _ = SyncSnapshotEntriesReader.Read(Convert.FromHexString(hex), snapshot, after); }
        catch (SyncWireException) { accepted = false; }
        Assert.True(accepted == expected, $"{name}: expected {expected}, got {accepted}");
    }

    [Fact]
    public void RejectsUnqualifiedContextAndAppliesReceivedByteBudget()
    {
        using var corpus = JsonDocument.Parse(File.ReadAllBytes(CorpusPath));
        var snapshot = Context(corpus);
        var input = Convert.FromHexString(corpus.RootElement.GetProperty("cases")[0].GetProperty("inputHex").GetString()!);
        Assert.Throws<SyncWireException>(() => SyncSnapshotEntriesReader.Read(input, snapshot with { LeaseSeconds = 0 }, null));
        var oversized = Assert.Throws<SyncWireException>(() => SyncSnapshotEntriesReader.Read(new byte[SyncProtocolReader.MaxPageBytes + 1], snapshot, null));
        Assert.Equal("page_limit", oversized.Code);
    }

    [Fact]
    public void IgnoredNumericFieldsAreChargedAsReceivedBytes()
    {
        using var corpus = JsonDocument.Parse(File.ReadAllBytes(CorpusPath));
        var snapshot = Context(corpus);
        var row = corpus.RootElement.GetProperty("cases").EnumerateArray().Single(c => c.GetProperty("name").GetString() == "nonempty terminal page");
        var original = Encoding.UTF8.GetString(Convert.FromHexString(row.GetProperty("inputHex").GetString()!));
        var raw = Encoding.UTF8.GetBytes(original[..^1] + ",\"future\":[" + string.Concat(Enumerable.Repeat("1e4,", 710000)).TrimEnd(',') + "]}");
        Assert.True(raw.Length < SyncProtocolReader.MaxPageBytes);
        var page = SyncSnapshotEntriesReader.Read(raw, snapshot, row.GetProperty("afterNodeId").GetString());
        Assert.Equal(2, page.Items.Count); Assert.Equal("9007199254740993", page.Items[0].SizeBytes);
    }
}
