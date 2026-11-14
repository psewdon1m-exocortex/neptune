using System.Text.Json;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class CompletionCorpusObservationTests
{
    private static readonly string Hash = new('a', 64);
    private static readonly string IdA = Guid.NewGuid().ToString(), IdB = Guid.NewGuid().ToString();
    private static readonly string VersionA = Guid.NewGuid().ToString(), VersionB = Guid.NewGuid().ToString();
    private static CompletionCorpusOracle Oracle() => new(new Dictionary<string, CompletionExpectedFile>
    { ["a"] = new(3, Hash), ["b"] = new(3, Hash) });
    private static object Row(string path = "a", string? id = null, string? version = null, int versionCount = 1, string? hash = null) => new
    { path, id = id ?? IdA, version = version ?? VersionA, catalogSha256 = hash ?? Hash, catalogSize = "3", versionCount, sha256 = Hash, size = 3 };
    private static JsonElement Snapshot(params object[] rows) => JsonSerializer.SerializeToElement(new { files = rows, folders = Array.Empty<string>() });

    [Fact]
    public void CountAloneCannotProveCompletionAndPhysicalEvidenceIsRequired()
    {
        var oracle = Oracle();
        Assert.False(oracle.Observe(Snapshot(Row()), false));
        Assert.True(oracle.Observe(Snapshot(Row(), Row("b", IdB, VersionB)), false));
        Assert.ThrowsAny<Exception>(() => oracle.Observe(Snapshot(new { path="a", id=IdA, version=VersionA, catalogSha256=Hash, catalogSize="3", versionCount=1 }, Row("b", IdB, VersionB)), true));
        Assert.True(oracle.Observe(Snapshot(Row(), Row("b", IdB, VersionB)), true));
    }

    [Fact]
    public void DuplicatePathsCannotHideMissingFileAtTheSameCount() =>
        Assert.Throws<InvalidDataException>(() => Oracle().Observe(Snapshot(Row(), Row()), false));

    [Fact]
    public void WrongPathHashAndExtraVersionsFail()
    {
        Assert.Throws<InvalidDataException>(() => Oracle().Observe(Snapshot(Row("wrong"), Row("b", IdB, VersionB)), false));
        Assert.Throws<InvalidDataException>(() => Oracle().Observe(Snapshot(Row(hash:new string('b',64)), Row("b", IdB, VersionB)), false));
        Assert.Throws<InvalidDataException>(() => Oracle().Observe(Snapshot(Row(versionCount:2), Row("b", IdB, VersionB)), false));
    }

    [Fact]
    public void RestartMustPreserveOriginalResourceAndVersionIdentity()
    {
        var oracle = Oracle();
        Assert.False(oracle.Observe(Snapshot(Row()), false));
        Assert.True(oracle.Observe(Snapshot(Row(), Row("b", IdB, VersionB)), true));
        Assert.Throws<InvalidDataException>(() => oracle.Observe(Snapshot(Row(version:Guid.NewGuid().ToString()), Row("b", IdB, VersionB)), false));
        Assert.Throws<InvalidDataException>(() => oracle.Observe(Snapshot(Row(id:Guid.NewGuid().ToString()), Row("b", IdB, VersionB)), false));
    }

    [Fact]
    public void DuplicateIdsAndEmptyVersionsFail()
    {
        Assert.Throws<InvalidDataException>(() => Oracle().Observe(Snapshot(Row(), Row("b", IdA, VersionB)), false));
        Assert.Throws<InvalidDataException>(() => Oracle().Observe(Snapshot(Row(version:""), Row("b", IdB, VersionB)), false));
    }

    [Fact]
    public void CorrectCatalogCannotHideCorruptPhysicalBytesOrDisappearedOriginal()
    {
        var oracle=Oracle();
        Assert.True(oracle.Observe(Snapshot(Row(),Row("b",IdB,VersionB)),false));
        Assert.Throws<InvalidDataException>(()=>oracle.Observe(Snapshot(new {path="a",id=IdA,version=VersionA,versionCount=1,catalogSize="3",catalogSha256=Hash,size=3,sha256=new string('b',64)},Row("b",IdB,VersionB)),true));
        Assert.Throws<InvalidDataException>(()=>oracle.Observe(Snapshot(Row()),false));
        Assert.True(oracle.Observe(Snapshot(Row(),Row("b",IdB,VersionB)),true));
    }

    [Fact]
    public void RarePollsAndUnchangedCountsCannotResetNoGrowthWatchdog()
    {
        var schedule = new CompletionObservationSchedule(10);
        Assert.True(schedule.AggregateDue(TimeSpan.Zero));
        schedule.Observed(10, TimeSpan.Zero);
        Assert.False(schedule.AggregateDue(TimeSpan.FromSeconds(9)));
        Assert.True(schedule.AggregateDue(TimeSpan.FromSeconds(10)));
        Assert.False(schedule.FullDue(TimeSpan.FromMinutes(4)));
        Assert.True(schedule.FullDue(TimeSpan.FromMinutes(5)));
        schedule.FullObserved(TimeSpan.FromMinutes(5));
        schedule.Observed(10, TimeSpan.FromMinutes(14));
        Assert.Throws<InvalidOperationException>(() => schedule.CheckGrowth(TimeSpan.FromMinutes(15)));
        schedule.Observed(11, TimeSpan.FromMinutes(15));
        schedule.CheckGrowth(TimeSpan.FromMinutes(29));
        // A count regression followed by recovery to the same high water is not growth.
        schedule.Observed(9, TimeSpan.FromMinutes(28));
        schedule.Observed(11, TimeSpan.FromMinutes(29));
        Assert.Throws<InvalidOperationException>(() => schedule.CheckGrowth(TimeSpan.FromMinutes(30)));
    }
}
