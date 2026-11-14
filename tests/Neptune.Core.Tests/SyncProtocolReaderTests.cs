using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class SyncProtocolReaderTests
{
    private static string CorpusPath => Environment.GetEnvironmentVariable("SYNC_WIRE_CORPUS_PATH")
        ?? Path.Combine(AppContext.BaseDirectory, "TestData", "SyncWire", "boundaries.v1.json");

    public static TheoryData<string, string, string, bool> BoundaryCases
    {
        get
        {
            using var corpus = JsonDocument.Parse(File.ReadAllBytes(CorpusPath));
            var cases = new TheoryData<string, string, string, bool>();
            foreach (var row in corpus.RootElement.GetProperty("cases").EnumerateArray())
                cases.Add(row.GetProperty("name").GetString()!, row.GetProperty("kind").GetString()!,
                    row.GetProperty("inputHex").GetString()!, row.GetProperty("accepted").GetBoolean());
            return cases;
        }
    }

    [Theory]
    [MemberData(nameof(BoundaryCases))]
    public void SharedQualifiedBoundaryHasSameAcceptance(string name, string kind, string hex, bool expected)
    {
        using var corpus = JsonDocument.Parse(File.ReadAllBytes(CorpusPath));
        var binding = SyncProtocolReader.Binding(corpus.RootElement.GetProperty("binding"));
        var after = corpus.RootElement.GetProperty("after").GetString()!;
        var accepted = true;
        try
        {
            using var document = SyncProtocolReader.Parse(Convert.FromHexString(hex));
            switch (kind)
            {
                case "parse": break;
                case "integer64": _ = SyncProtocolReader.Integer64(document.RootElement); break;
                case "descriptor": _ = SyncProtocolReader.Descriptor(document.RootElement); break;
                case "expected": SyncProtocolReader.Expected(document.RootElement); break;
                case "snapshot": _ = SyncProtocolReader.Snapshot(document.RootElement); break;
                case "feed": _ = SyncProtocolReader.Feed(document.RootElement, binding, after); break;
                default: throw new InvalidOperationException("Unknown qualified corpus kind: " + kind);
            }
        }
        catch (SyncWireException) { accepted = false; }
        Assert.True(accepted == expected, $"{name}: {kind} expected {expected}, got {accepted}");
    }

    [Fact]
    public void GeneratedExactCountersMatchIndependentArbitraryPrecisionOracle()
    {
        var canonical = new Regex("^(0|[1-9][0-9]*)$", RegexOptions.CultureInvariant | RegexOptions.NonBacktracking);
        var maximum = new BigInteger(long.MaxValue);
        ulong seed = 0x6a09e667f3bcc909;
        for (var i = 0; i < 5000; i++)
        {
            seed = unchecked(seed * 6364136223846793005 + 1442695040888963407);
            var value = seed.ToString(CultureInfo.InvariantCulture);
            value = (i % 7) switch
            {
                1 => "0" + value, 2 => "-" + value, 3 => value + ".0",
                4 => value + "e0", 5 => " " + value, _ => value
            };
            var expected = canonical.IsMatch(value)
                && BigInteger.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var number)
                && number >= BigInteger.Zero && number <= maximum;
            using var document = SyncProtocolReader.Parse(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value)));
            long? actual = null;
            try { actual = SyncProtocolReader.Integer64(document.RootElement); }
            catch (SyncWireException) { }
            Assert.Equal(expected, actual.HasValue);
            if (expected) Assert.Equal((long)BigInteger.Parse(value, CultureInfo.InvariantCulture), actual!.Value);
        }
        using var numeric = SyncProtocolReader.Parse("1"u8.ToArray());
        Assert.Throws<SyncWireException>(() => SyncProtocolReader.Integer64(numeric.RootElement));
    }

    [Fact]
    public void IncomingBudgetCountsReceivedBytesWithoutExpandingFutureFields()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", "SyncWire", "golden.v1.json");
        using var golden = JsonDocument.Parse(File.ReadAllBytes(path));
        var binding = SyncProtocolReader.Binding(golden.RootElement.GetProperty("binding"));
        var feed = golden.RootElement.GetProperty("feed");
        var original = feed.GetRawText().TrimEnd();
        Assert.EndsWith("}", original);
        var raw = Encoding.UTF8.GetBytes(original[..^1] + ",\"future\":["
            + string.Concat(Enumerable.Repeat("1e4,", 710000)).TrimEnd(',') + "]}");
        Assert.True(raw.Length < SyncProtocolReader.MaxPageBytes);
        using var document = SyncProtocolReader.Parse(raw);
        _ = SyncProtocolReader.Feed(document.RootElement, binding, feed.GetProperty("from_exclusive").GetString()!);
        var failure = Assert.Throws<SyncWireException>(() => SyncProtocolReader.Parse(new byte[SyncProtocolReader.MaxPageBytes + 1]));
        Assert.Equal("page_limit", failure.Code);
    }
}
