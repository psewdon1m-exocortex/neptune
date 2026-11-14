using System.Diagnostics;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Threading.Channels;
using Neptune.Core;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

/// <summary>Opt-in native acceptance driver. Uses the public service, real TLS,
/// native capture, ordinary watcher wakeups and a separate read-only oracle.</summary>
[Collection("Native fresh cycle")]
[Trait("Lane", "Live")]
public sealed class PerformanceN01LiveTests
{
    private sealed record Spec(string Scenario, int Files, int Size, int Directories = 0,
        string Shape = "flat", int Seed = 271828, double MinimumFilesPerSecond = 50,
        double MinimumDirectoriesPerSecond = 6, int DeadlineMinutes = 20, string SourceName = "source");
    private sealed record Expected(string Sha256, long Size, int Versions);
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web) { WriteIndented = true };

    [Fact]
    public async Task PublicServiceCompletesOwnedCorpusWithPhysicalIntegrity()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires owned NTFS and Windows service"); return; }
        if (Environment.GetEnvironmentVariable("N01_EVIDENCE") is null) { Assert.Skip("Requires opt-in owned N01 stand"); return; }
        var output = Path.GetFullPath(Environment.GetEnvironmentVariable("N01_EVIDENCE")
            ?? throw new InvalidOperationException("N01_EVIDENCE must identify a new owned case"));
        var owned = Path.GetFullPath(Environment.GetEnvironmentVariable("N01_RUN_ROOT")
            ?? throw new InvalidOperationException("N01_RUN_ROOT required"));
        Assert.StartsWith(owned + Path.DirectorySeparatorChar, output);
        var spec = JsonSerializer.Deserialize<Spec>(await File.ReadAllTextAsync(Path.Combine(output, "run-spec.json"), TestContext.Current.CancellationToken), Json)!;
        Assert.InRange(spec.Files, 0, 100000); Assert.InRange(spec.Directories, 0, 100000);
        Assert.InRange(spec.Size, 0, 2 * 1024 * 1024);
        Assert.Contains(spec.Scenario, new[] { "initial", "fresh-add", "fresh-overwrite", "fresh-mix", "fresh-directories" });
        Assert.Contains(spec.Shape, new[] { "flat", "wide", "tree", "mixed" });
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(spec.DeadlineMinutes)); var ct = deadline.Token;
        using var volume = JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(owned, "native-volume.json"), ct));
        Assert.Equal("ready", volume.RootElement.GetProperty("state").GetString());
        var native = volume.RootElement.GetProperty("nativePath").GetString()!;
        var retained = Environment.GetEnvironmentVariable("N01_RETAIN_NATIVE_CASE");
        var directory = retained is null ? Path.Combine(native, "n01-" + Guid.NewGuid().ToString("N")) : Path.GetFullPath(retained);
        Assert.StartsWith(Path.GetFullPath(native).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar, Path.GetFullPath(directory));
        Assert.Matches("^[a-z0-9-]{1,80}$", spec.SourceName);
        var source = Path.Combine(directory, spec.SourceName); Directory.CreateDirectory(source);
        await using (var ns = await WindowsLocalNamespaceSource.OpenAsync(source, ct))
        {
            Assert.Equal("NTFS", ns.RootProof.FileSystem);
            Assert.Equal(volume.RootElement.GetProperty("volumeUniqueId").GetString()!.Substring(11, 36), ns.RootProof.VolumeGuid);
        }
        var profile = new WindowsProfileContext("N01", Path.Combine(directory, "profile"), false);
        Directory.CreateDirectory(profile.StateDirectory);
        new WindowsPreferencesStore(profile.StateDirectory).Write(WindowsPreferences.Default with { SmallBatchMappings = ["n01-mapping"] });
        var expected = new SortedDictionary<string, Expected>(StringComparer.Ordinal);
        var directories = new SortedSet<string>(StringComparer.Ordinal);
        await File.WriteAllTextAsync(Path.Combine(output, "native-case.json"), JsonSerializer.Serialize(new { directory, source, profile = profile.StateDirectory, spec }, Json), ct);

        async Task WriteFile(string relative, int index, int generation)
        {
            var bytes = new byte[spec.Size]; new Random(unchecked(spec.Seed + index * 7919 + generation * 104729)).NextBytes(bytes);
            var file = Path.Combine(source, relative.Replace('/', Path.DirectorySeparatorChar));
            Directory.CreateDirectory(Path.GetDirectoryName(file)!);
            await File.WriteAllBytesAsync(file, bytes, ct);
            expected[relative] = new(Convert.ToHexStringLower(SHA256.HashData(bytes)), bytes.Length,
                expected.TryGetValue(relative, out var prior) ? prior.Versions + 1 : 1);
        }
        void AddDirectories(string prefix)
        {
            for (var i = 0; i < spec.Directories; i++)
            {
                var relative = spec.Shape == "tree"
                    ? string.Join('/', Enumerable.Range(0, i % 16 + 1).Select(depth => depth == 0 ? $"{prefix}tree-{i / 16:D4}" : $"level-{depth:D2}"))
                    : $"{prefix}dir-{i:D5}";
                Directory.CreateDirectory(Path.Combine(source, relative.Replace('/', Path.DirectorySeparatorChar)));
                directories.Add(relative);
            }
        }
        string FileName(int index, string prefix = "") => spec.Shape == "mixed"
            ? $"{prefix}dir-{index / 8:D5}/file-{index:D6}.bin" : $"{prefix}file-{index:D6}.bin";

        var fresh = spec.Scenario != "initial";
        if (!fresh) { AddDirectories(""); for (var i = 0; i < spec.Files; i++) await WriteFile(FileName(i), i, 0); }
        else if (spec.Scenario != "fresh-directories")
            for (var i = 0; i < spec.Files; i++) await WriteFile($"file-{i:D6}.bin", i, 0);

        using var config = JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(output, "client.private.json"), ct));
        var c = config.RootElement;
        var origin = new Uri(c.GetProperty("origin").GetString()!);
        Assert.True(origin.Scheme == "https" || origin.Scheme == "http" && origin.IsLoopback && c.GetProperty("diagnosticLoopbackHttp").GetBoolean());
        using var ca = X509CertificateLoader.LoadCertificateFromFile(c.GetProperty("caCertificate").GetString()!);
        var chain = new X509ChainPolicy { TrustMode = X509ChainTrustMode.CustomRootTrust, RevocationMode = X509RevocationMode.NoCheck };
        chain.CustomTrustStore.Add(ca);
        using var handler = new SocketsHttpHandler
        {
            ConnectTimeout = TimeSpan.FromSeconds(10),
            SslOptions = new SslClientAuthenticationOptions { CertificateChainPolicy = chain }
        };
        using var http = new HttpClient(handler) { Timeout = Timeout.InfiniteTimeSpan };
        using var oracle = new HttpClient { BaseAddress = new Uri(c.GetProperty("oracleOrigin").GetString()!), Timeout = TimeSpan.FromSeconds(45) };
        var connection = new WindowsConnection(origin, c.GetProperty("kernelToken").GetString()!,
            c.GetProperty("authorization").GetString()![7..], c.GetProperty("rootName").GetString()!);
        var mapping = new SyncMapping("n01-mapping", "n01-client", source, true, DateTimeOffset.UtcNow);
        // The ordinary UI persists its mapping before invoking the service. The
        // legacy path records files against that foreign key, just like guarded.
        var stateStore = new NeptuneStateStore(Path.Combine(profile.StateDirectory, "neptune.db"));
        await stateStore.InitializeAsync(ct);
        var savedMappings = await stateStore.ListMappingsAsync(ct);
        if (savedMappings.Count == 0) await stateStore.AddMappingAsync(mapping, ct);
        else Assert.Contains(savedMappings, saved => saved.MappingId == mapping.MappingId && saved.ClientInstanceId == mapping.ClientInstanceId && saved.LocalPath == mapping.LocalPath);
        await using var service = new WindowsSyncService(profile, http);
        var wake = Channel.CreateBounded<bool>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.DropOldest });
        service.MappingWorkAvailable += _ => wake.Writer.TryWrite(true);
        using var watcher = new FileSystemWatcher(source) { IncludeSubdirectories = true,
            NotifyFilter = NotifyFilters.FileName | NotifyFilters.DirectoryName | NotifyFilters.LastWrite | NotifyFilters.Size };
        watcher.Created += (_, _) => wake.Writer.TryWrite(true); watcher.Changed += (_, _) => wake.Writer.TryWrite(true);
        watcher.Renamed += (_, _) => wake.Writer.TryWrite(true); watcher.Deleted += (_, _) => wake.Writer.TryWrite(true);
        watcher.Error += (_, _) => wake.Writer.TryWrite(true); watcher.EnableRaisingEvents = true;
        var progress = new SampledProgress(Path.Combine(output, "client-progress.json"));
        double observeMs = 0, clientMs = 0; var cycles = 0;
        async Task<JsonElement> Observe(string endpoint)
        {
            var watch = Stopwatch.StartNew();
            try { using var response = await oracle.GetAsync(endpoint, ct); response.EnsureSuccessStatusCode();
                using var document = JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync(ct)); return document.RootElement.Clone(); }
            finally { observeMs += watch.Elapsed.TotalMilliseconds; }
        }
        async Task Converge(string phase)
        {
            var noGrowth = Stopwatch.StartNew(); long lastGrowth = -1;
            while (true)
            {
                var watch = Stopwatch.StartNew();
                var result = await service.SyncAsync("n01-client", [mapping], connection, progress, ct);
                clientMs += watch.Elapsed.TotalMilliseconds; cycles++;
                var state = await Observe("progress");
                var files = state.GetProperty("files").GetInt32(); var dirs = state.GetProperty("directories").GetInt32();
                var versions = state.GetProperty("versions").GetInt64(); var targetVersions = expected.Values.Sum(e => (long)e.Versions);
                await File.AppendAllTextAsync(Path.Combine(output, "cycles.jsonl"), JsonSerializer.Serialize(new { at = DateTimeOffset.UtcNow,
                    phase, cycles, files, dirs, versions, expectedFiles = expected.Count, targetVersions,
                    result.UploadedFiles, result.DeferredFiles, clientMs, observeMs, diagnostics = await service.ReadDiagnosticsAsync(ct) }) + "\n", ct);
                Assert.InRange(files, 0, expected.Count); Assert.InRange(versions, 0, targetVersions);
                if (files == expected.Count && versions == targetVersions && dirs == directories.Count + 1) return;
                var total = versions + dirs; if (total > lastGrowth) { lastGrowth = total; noGrowth.Restart(); }
                Assert.True(noGrowth.Elapsed < TimeSpan.FromMinutes(2), "No independently confirmed publication for two minutes");
                while (wake.Reader.TryRead(out _)) { }
                if (result.DeferredFiles > 0) await Task.Delay(750, ct);
                else { using var wait = CancellationTokenSource.CreateLinkedTokenSource(ct); wait.CancelAfter(TimeSpan.FromSeconds(5));
                    try { await wake.Reader.ReadAsync(wait.Token); } catch (OperationCanceledException) when (!ct.IsCancellationRequested) { } }
            }
        }
        async Task<JsonElement> Verify(string name)
        {
            var state = await Observe("readback"); var actual = state.GetProperty("files").EnumerateArray().ToArray();
            Assert.Equal(expected.Count, actual.Length); var ids = new HashSet<string>(); var paths = new HashSet<string>();
            foreach (var f in actual)
            {
                var remote = f.GetProperty("path").GetString()!; Assert.StartsWith("source/", remote); var relative = remote[7..];
                Assert.True(paths.Add(relative)); Assert.True(expected.TryGetValue(relative, out var e));
                Assert.Equal(e.Sha256, f.GetProperty("sha256").GetString()); Assert.Equal(e.Sha256, f.GetProperty("catalogSha256").GetString());
                Assert.Equal(e.Size, f.GetProperty("size").GetInt64()); Assert.Equal(e.Size.ToString(), f.GetProperty("catalogSize").GetString());
                Assert.Equal(e.Versions, f.GetProperty("versionCount").GetInt32()); Assert.True(ids.Add(f.GetProperty("id").GetString()!));
            }
            var remoteDirs = state.GetProperty("directories").EnumerateArray().Select(d => d.GetString()!).Where(d => d != "source").Select(d => d[7..]).Order(StringComparer.Ordinal);
            Assert.Equal(directories, remoteDirs);
            Assert.Equal(expected.Count, Directory.GetFiles(source, "*", SearchOption.AllDirectories).Length);
            foreach (var (relative, e) in expected) { await using var stream = File.OpenRead(Path.Combine(source, relative.Replace('/', Path.DirectorySeparatorChar)));
                Assert.Equal(e.Sha256, Convert.ToHexStringLower(await SHA256.HashDataAsync(stream, ct))); }
            await File.WriteAllTextAsync(Path.Combine(output, name + "-readback.json"), state.GetRawText(), ct); return state;
        }
        JsonElement? before = null;
        if (fresh)
        {
            await Converge("precondition"); before = await Verify("precondition");
            await service.PauseAndCheckpointAsync(ct);
            if (spec.Scenario == "fresh-directories") AddDirectories("fresh-");
            else for (var i = 0; i < spec.Files; i++)
            {
                var overwrite = spec.Scenario == "fresh-overwrite" || spec.Scenario == "fresh-mix" && i < spec.Files / 2;
                await WriteFile(overwrite ? $"file-{i:D6}.bin" : $"new-file-{i:D6}.bin", i, 1);
            }
        }
        await File.WriteAllTextAsync(Path.Combine(output, "corpus-manifest.json"), JsonSerializer.Serialize(new { spec, expected, directories }, Json), ct);
        var wall = Stopwatch.StartNew(); if (fresh) service.ResumeAdmission();
        await Converge("measured"); wall.Stop();
        var verification = Stopwatch.StartNew(); var after = await Verify("final"); verification.Stop();
        if (before is { } prior)
        {
            var priorIds = prior.GetProperty("files").EnumerateArray().ToDictionary(f => f.GetProperty("path").GetString()!, f => f.GetProperty("id").GetString());
            foreach (var f in after.GetProperty("files").EnumerateArray())
                if (priorIds.TryGetValue(f.GetProperty("path").GetString()!, out var id)) Assert.Equal(id, f.GetProperty("id").GetString());
        }
        var fps = spec.Files / wall.Elapsed.TotalSeconds; var dps = spec.Directories / wall.Elapsed.TotalSeconds;
        var pass = (spec.Files == 0 || fps >= spec.MinimumFilesPerSecond) && (spec.Directories == 0 || dps >= spec.MinimumDirectoriesPerSecond);
        await File.WriteAllTextAsync(Path.Combine(output, "metrics.json"), JsonSerializer.Serialize(new { status = pass ? "PASS" : "FAIL", spec,
            wallSeconds = wall.Elapsed.TotalSeconds, verificationSeconds = verification.Elapsed.TotalSeconds,
            filesPerSecond = fps, directoriesPerSecond = dps, observeMs, clientMs, cycles, publicService = true,
            physicalSha = true, exactTree = true, individualVersions = true, resourceIdsPreserved = true,
            fullDiskReserve = true, tlsChainValidated = origin.Scheme == "https", automaticSingleEventQualified = false, processId = Environment.ProcessId }, Json), ct);
        Assert.True(pass, $"N01 target failed: {fps:F3} files/s, {dps:F3} directories/s");
    }

    private sealed class SampledProgress(string file) : IProgress<string>
    {
        private readonly object _gate = new(); private DateTimeOffset _last;
        public void Report(string value) { lock (_gate) { var now = DateTimeOffset.UtcNow; if (now - _last < TimeSpan.FromSeconds(1)) return;
            _last = now; File.WriteAllText(file, JsonSerializer.Serialize(new { at = now, progress = value })); } }
    }
}
