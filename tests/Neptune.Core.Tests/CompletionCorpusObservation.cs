using System.Globalization;
using System.Text.Json;

namespace Neptune.Core.Tests;

internal sealed record CompletionExpectedFile(long Size, string Sha256);

// Independent test oracle for an immutable create-only corpus. Neither a client
// receipt nor an aggregate count can prove completion.
internal sealed class CompletionCorpusOracle(IReadOnlyDictionary<string, CompletionExpectedFile> expected)
{
    private readonly Dictionary<string, (string Id, string Version)> identities = new(StringComparer.Ordinal);

    internal bool Observe(JsonElement snapshot, bool physical)
    {
        var files = snapshot.GetProperty("files");
        var observed = new Dictionary<string, (string Id, string Version)>(StringComparer.Ordinal);
        var ids = new HashSet<string>(StringComparer.Ordinal);
        var versions = new HashSet<string>(StringComparer.Ordinal);
        foreach (var file in files.EnumerateArray())
        {
            var path = file.GetProperty("path").GetString()!;
            if (!expected.TryGetValue(path, out var row) || file.GetProperty("catalogSha256").GetString() != row.Sha256
                || file.GetProperty("catalogSize").GetString() != row.Size.ToString(CultureInfo.InvariantCulture))
                throw new InvalidDataException("Corpus catalog mismatch: " + path);
            var id = file.GetProperty("id").GetString()!;
            var version = file.GetProperty("version").GetString()!;
            if (!Guid.TryParse(id, out var resourceId) || resourceId == Guid.Empty
                || !Guid.TryParse(version, out var versionId) || versionId == Guid.Empty
                || file.GetProperty("versionCount").GetInt32() != 1
                || !ids.Add(id) || !versions.Add(version) || !observed.TryAdd(path, (id, version))
                || (identities.TryGetValue(path, out var original) && original != (id, version)))
                throw new InvalidDataException("Corpus identity/version mismatch: " + path);
            if (physical && (file.GetProperty("sha256").GetString() != row.Sha256 || file.GetProperty("size").GetInt64() != row.Size))
                throw new InvalidDataException("Corpus physical mismatch: " + path);
        }
        if (identities.Keys.Any(path => !observed.ContainsKey(path)))
            throw new InvalidDataException("Previously observed corpus original disappeared");
        // A failed snapshot must not replace the baseline used for restart checks.
        foreach (var (path, identity) in observed) identities[path] = identity;
        return files.GetArrayLength() == expected.Count;
    }
}

internal sealed class CompletionObservationSchedule(int initialCount)
{
    private TimeSpan lastAggregate = -TimeSpan.FromSeconds(10);
    private TimeSpan lastFull;
    private TimeSpan lastGrowth;
    private int highWater = initialCount;
    internal bool AggregateDue(TimeSpan elapsed) => elapsed - lastAggregate >= TimeSpan.FromSeconds(10);
    internal bool FullDue(TimeSpan elapsed) => elapsed - lastFull >= TimeSpan.FromMinutes(5);
    internal void Observed(int count, TimeSpan elapsed)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        lastAggregate = elapsed;
        if (count > highWater) { highWater = count; lastGrowth = elapsed; }
    }
    internal void FullObserved(TimeSpan elapsed) => lastFull = elapsed;
    internal void CheckGrowth(TimeSpan elapsed)
    {
        if (elapsed - lastGrowth >= TimeSpan.FromMinutes(15))
            throw new InvalidOperationException("No publication growth for 15 minutes; retain state and diagnose");
    }
}
