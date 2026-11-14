using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class FolderExclusionTests
{
    private sealed class NoRequests : HttpMessageHandler
    {
        internal int Calls;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        { Calls++; throw new InvalidOperationException("Excluded original must not issue a request: " + request.Method); }
    }

    [Fact]
    public async Task ExcludedLegacyOriginalIsNeitherRecoveredNorCancelledAndKeepsSealedBytes()
    {
        var ct = TestContext.Current.CancellationToken;
        var root = Path.Combine(Path.GetTempPath(), "neptune-exclusion-original-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            var state = new NeptuneStateStore(Path.Combine(root, "state.db")); await state.InitializeAsync(ct);
            var mapping = new Uri("https://fixture.test/mirror/"); var target = new Uri(mapping, "private/file");
            const string token = "fixture";
            var scope = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes("https://fixture.test\n" + token)));
            var checkpoint = new SyncUploadCheckpoint(scope, target.AbsoluteUri, "same-key", "same-intent", "same-upload");
            await state.SaveSyncUploadCheckpointAsync(checkpoint, ct);
            var source = Path.Combine(root, "file"); File.WriteAllText(source, "valuable original");
            var spools = Path.Combine(root, "originals");
            await using (var original = await RetainedLegacyUpload.OpenAsync(spools, scope, target, source, null, ct))
                await original.BindAsync(null, ct);
            var before = Directory.GetFiles(spools).ToDictionary(p => p, File.ReadAllBytes);
            using var handler = new NoRequests(); using var http = new HttpClient(handler);
            var client = new WebDavSyncClient(http, state) { RetainedOriginalsDirectory = spools };
            var policy = new MappingSyncPolicy("legacy", ["PRIVATE"]);
            await client.CancelMissingUploadsAsync(mapping, token, new HashSet<string>(), ct, policy);
            await Assert.ThrowsAsync<InvalidOperationException>(() => client.RecoverRetainedUploadsAsync(mapping, token, ct, _ => false));
            Assert.Equal(0, handler.Calls);
            Assert.Equal(checkpoint, Assert.Single(await state.ListSyncUploadCheckpointsAsync(scope, ct)));
            Assert.All(before, pair => Assert.Equal(pair.Value, File.ReadAllBytes(pair.Key)));
            Assert.Equal("valuable original", File.ReadAllText(source));
        }
        finally { SqliteConnection.ClearAllPools(); Directory.Delete(root, true); }
    }

    [Theory]
    [InlineData("project-2", true)] [InlineData("PROJECT-2/deep/file", true)]
    [InlineData("project-20/file", false)] [InlineData("project-1/file", false)]
    [InlineData("ПРОЕКТ/файл", true)] [InlineData("другой/ПРОЕКТ/файл", false)]
    public void ExclusionsMatchWholeWindowsSegments(string path, bool expected)
        => Assert.Equal(expected, new MappingSyncPolicy("legacy", ["project-2", "проект"]).Excludes(path));

    [Theory]
    [InlineData("")] [InlineData("../secret")] [InlineData("project/../secret")]
    [InlineData("C:/secret")] [InlineData("/secret")] [InlineData("project//child")]
    [InlineData("project:stream")] [InlineData("project/.")] [InlineData("project\nsecret")]
    public void InvalidPolicyCannotEscapeRoot(string path)
        => Assert.Throws<InvalidDataException>(() => new MappingSyncPolicy("legacy", [path]).Validate());

    [Fact]
    public void ExcludedDescendantProtectsItsAncestorsButNotSiblings()
    {
        var policy = new MappingSyncPolicy("legacy", ["parent/private"]);
        Assert.True(policy.ProtectsSubtree("parent"));
        Assert.True(policy.ProtectsSubtree("parent/private/child"));
        Assert.False(policy.Excludes("parent"));
        Assert.False(policy.ProtectsSubtree("parent/public"));
        Assert.Equal(new[] { "folder", "Папка/Дочь" }, WindowsFolderExclusions.Normalize(["folder/", "FOLDER", "Папка\\Дочь"]));
        var nonCanonical = new MappingSyncPolicy("portable_v1", ["cafe\u0301"]);
        nonCanonical.Validate();
        Assert.True(nonCanonical.Excludes("CAFÉ/file"));
    }

    [Fact]
    public async Task SettingsSurviveRestartAreRootBoundAndFailClosed()
    {
        var root = Path.Combine(Path.GetTempPath(), "neptune-exclusions-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var mapping = new SyncMapping("mapping", "client", Path.Combine(root, "source"), true, DateTimeOffset.UtcNow);
        var profile = new WindowsProfileContext("test", root, false);
        try
        {
            var store = new WindowsFolderExclusions(root);
            await using (var service = new WindowsSyncService(profile))
            {
                Assert.Empty(await service.ReadExcludedFoldersAsync(mapping, TestContext.Current.CancellationToken));
                Assert.True((await service.SetExcludedFoldersAsync(mapping, ["private"], [], TestContext.Current.CancellationToken)).Applied);
                await Assert.ThrowsAsync<InvalidOperationException>(() => service.SetExcludedFoldersAsync(mapping, [], [], TestContext.Current.CancellationToken));
            }
            using var noRequests = new NoRequests(); using var http = new HttpClient(noRequests);
            await using var reopened = new WindowsSyncService(profile, http);
            Assert.Equal(new[] { "private" }, await reopened.ReadExcludedFoldersAsync(mapping, TestContext.Current.CancellationToken));
            Assert.Throws<InvalidDataException>(() => store.Read(mapping with { LocalPath = root }));
            Assert.Throws<InvalidDataException>(() => store.Read(mapping with { ClientInstanceId = "other" }));
            var good = File.ReadAllBytes(store.FileName(mapping));
            await Assert.ThrowsAsync<InvalidDataException>(() => reopened.SetExcludedFoldersAsync(mapping, ["../escape"], ["private"], TestContext.Current.CancellationToken));
            Assert.Equal(good, File.ReadAllBytes(store.FileName(mapping)));
            File.WriteAllText(store.FileName(mapping), "{broken");
            await Assert.ThrowsAnyAsync<JsonException>(() => reopened.ReadExcludedFoldersAsync(mapping, TestContext.Current.CancellationToken));
            var connection = new WindowsConnection(new Uri("https://fixture.invalid"), "fixture", new string('b', 43), "fixture");
            await Assert.ThrowsAnyAsync<JsonException>(() => reopened.SyncAsync("client", [mapping], connection, cancellationToken: TestContext.Current.CancellationToken));
            Assert.Equal(0, noRequests.Calls);
        }
        finally { Directory.Delete(root, true); }
    }

    [Fact]
    public void PickerAndLegacyScanOmitSubtreesWithoutTouchingTheirBytes()
    {
        var root = Path.Combine(Path.GetTempPath(), "neptune-exclusion-picker-" + Guid.NewGuid().ToString("N"));
        var source = Path.Combine(root, "projects");
        Directory.CreateDirectory(Path.Combine(source, "project-1"));
        Directory.CreateDirectory(Path.Combine(source, "project-2", "nested"));
        Directory.CreateDirectory(Path.Combine(source, "project-20"));
        Directory.CreateDirectory(Path.Combine(root, "projects-other"));
        var privateFile = Path.Combine(source, "project-2", "nested", "valuable.txt");
        File.WriteAllText(privateFile, "original bytes");
        File.WriteAllText(Path.Combine(source, "project-1", "visible.txt"), "public");
        try
        {
            Assert.Equal("project-2", WindowsFolderExclusions.SelectedFolder(source, Path.Combine(source, "project-2")));
            Assert.Throws<InvalidOperationException>(() => WindowsFolderExclusions.SelectedFolder(source, source));
            Assert.Throws<InvalidOperationException>(() => WindowsFolderExclusions.SelectedFolder(source, Path.Combine(root, "projects-other")));
            var snapshot = WindowsLocalSnapshot.Read(source, TestContext.Current.CancellationToken, new("legacy", ["PROJECT-2"]));
            Assert.True(snapshot.CanReconcileRemote);
            Assert.Equal(new[] { "project-1", "project-1/visible.txt", "project-20" }, snapshot.Entries.Select(e => e.RelativePath).Order());
            Assert.Equal("original bytes", File.ReadAllText(privateFile));
        }
        finally { Directory.Delete(root, true); }
    }

    [Fact]
    public async Task FolderExclusionsImportOlderReviewWithoutChangingRetainedDatabase()
    {
        var ct = TestContext.Current.CancellationToken;
        var root = Path.Combine(Path.GetTempPath(), "neptune-exclusion-import-" + Guid.NewGuid().ToString("N"));
        var mapping = new SyncMapping("mapping", "client", Path.Combine(root, "source"), true, DateTimeOffset.UtcNow);
        var key = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(mapping.MappingId)));
        var database = Path.Combine(root, "guarded-sync", "namespace", key, "cycle.db");
        Directory.CreateDirectory(Path.GetDirectoryName(database)!);
        try
        {
            var policy = new MappingSyncPolicy("portable_v1", ["private"]);
            await using (var db = new SqliteConnection($"Data Source={database};Pooling=False"))
            {
                await db.OpenAsync(ct); await using var q = db.CreateCommand();
                q.CommandText = "CREATE TABLE local_cycle_active(scope_id TEXT); CREATE TABLE client_mapping_policies(scope_id TEXT,policy_json TEXT,policy_hash TEXT); INSERT INTO local_cycle_active VALUES('current'); INSERT INTO client_mapping_policies VALUES('current',$json,$hash),('retired','invalid','invalid')";
                q.Parameters.AddWithValue("$json", JsonSerializer.Serialize(policy, new JsonSerializerOptions(JsonSerializerDefaults.Web)));
                q.Parameters.AddWithValue("$hash", policy.Hash); await q.ExecuteNonQueryAsync(ct);
            }
            var before = File.ReadAllBytes(database);
            await using var service = new WindowsSyncService(new("test", root, false));
            Assert.Equal(new[] { "private" }, await service.ReadExcludedFoldersAsync(mapping, ct));
            Assert.True((await service.SetExcludedFoldersAsync(mapping, ["private", "other"], ["private"], ct)).Applied);
            Assert.Equal(new[] { "other", "private" }, await service.ReadExcludedFoldersAsync(mapping, ct));
            Assert.Equal(before, File.ReadAllBytes(database));
        }
        finally { Directory.Delete(root, true); }
    }
}

public sealed partial class WindowsEnrollmentTests
{
    [Fact]
    public async Task FolderExclusionsApplyBeforeFirstSyncAndPreserveRemoteAcrossRestartAndReinclude()
    {
        var ct = TestContext.Current.CancellationToken;
        var root = Path.Combine(Path.GetTempPath(), "neptune-excluded-service-" + Guid.NewGuid().ToString("N"));
        var source = Path.Combine(root, "projects");
        Directory.CreateDirectory(Path.Combine(source, "project-1"));
        Directory.CreateDirectory(Path.Combine(source, "project-2"));
        Directory.CreateDirectory(Path.Combine(source, "project-20"));
        foreach (var dir in new[] { "project-1", "project-2", "project-20" }) File.WriteAllText(Path.Combine(source, dir, "file.txt"), dir);
        var profile = new WindowsProfileContext("test", root, false);
        var mapping = new SyncMapping("mapping", "client", source, true, DateTimeOffset.UtcNow);
        var state = new NeptuneStateStore(Path.Combine(root, "neptune.db"));
        var remote = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var requests = new List<(string Method, string Path)>();
        using var handler = new Handler("Assigned");
        handler.DavOverride = request =>
        {
            var path = Uri.UnescapeDataString(request.RequestUri!.AbsolutePath);
            requests.Add((request.Method.Method, path));
            if (request.Method == HttpMethod.Put)
                remote[path] = request.Content!.ReadAsByteArrayAsync(ct).GetAwaiter().GetResult();
            if (request.Method == HttpMethod.Head || request.Method == HttpMethod.Put)
            {
                var response = new HttpResponseMessage(remote.ContainsKey(path) ? HttpStatusCode.OK : HttpStatusCode.NotFound);
                if (remote.TryGetValue(path, out var bytes)) response.Headers.ETag = new($"\"sha256-{Convert.ToHexStringLower(SHA256.HashData(bytes))}\"");
                return response;
            }
            if (request.Method.Method == "PROPFIND" && path.EndsWith("/projects/"))
                return new((HttpStatusCode)207) { Content = new StringContent("<d:multistatus xmlns:d=\"DAV:\"><d:response><d:href>project-2/</d:href><d:propstat><d:prop><d:displayname>project-2</d:displayname><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>") };
            Assert.NotEqual(HttpMethod.Delete, request.Method);
            return null;
        };
        using var http = new HttpClient(handler);
        var connection = new WindowsConnection(new Uri("https://kernel.test"), "fixture", new string('b', 43), "Assigned");
        WindowsSyncService service = new(profile, http);
        try
        {
            await state.InitializeAsync(ct); await state.AddMappingAsync(mapping, ct);
            Assert.True((await service.SetExcludedFoldersAsync(mapping, ["project-2"], [], ct)).Applied);
            Assert.Equal(2, (await service.SyncAsync("client", [mapping], connection, cancellationToken: ct)).UploadedFiles);
            Assert.DoesNotContain(requests, r => r.Path.Contains("/project-2/"));
            Assert.Equal("project-2", File.ReadAllText(Path.Combine(source, "project-2", "file.txt")));
            await service.DisposeAsync(); service = new(profile, http);
            requests.Clear();
            Assert.Equal(0, (await service.SyncAsync("client", [mapping], connection, cancellationToken: ct)).UploadedFiles);
            Assert.DoesNotContain(requests, r => r.Path.Contains("/project-2/"));
            Assert.True((await service.SetExcludedFoldersAsync(mapping, [], ["project-2"], ct)).Applied);
            Assert.Equal(1, (await service.SyncAsync("client", [mapping], connection, cancellationToken: ct)).UploadedFiles);
            var prior = await state.GetSyncFileAsync("mapping", "project-2/file.txt", ct);
            Assert.True((await service.SetExcludedFoldersAsync(mapping, ["project-2"], [], ct)).Applied);
            // Local absence inside an excluded subtree must not become a deletion.
            File.Move(Path.Combine(source, "project-2", "file.txt"), Path.Combine(root, "retained.txt"));
            requests.Clear();
            Assert.Equal(0, (await service.SyncAsync("client", [mapping], connection, cancellationToken: ct)).UploadedFiles);
            Assert.Equal(prior, await state.GetSyncFileAsync("mapping", "project-2/file.txt", ct));
            Assert.DoesNotContain(requests, r => r.Path.Contains("/project-2/"));
            Assert.Equal(3, remote.Count);
            Assert.Equal("project-2", File.ReadAllText(Path.Combine(root, "retained.txt")));
        }
        finally { await service.DisposeAsync(); SqliteConnection.ClearAllPools(); Directory.Delete(root, true); }
    }
}

public sealed partial class Stage3ChatBTests
{
    [Fact]
    public async Task FolderExclusionsNativePreserveExistingRemoteAndAncestorDeletion()
    {
        RequireWindows(); var clock = new Stage3Clock(); await using var f = await WindowsFreshCycleTests.Fixture.Create(clock);
        f.Remote.ExtensionsEnabled = true;
        var folder = Path.Combine(f.Source, "parent", "private"); Directory.CreateDirectory(folder);
        File.WriteAllText(Path.Combine(folder, "file.txt"), "valuable original");
        await using var engine = await NamespaceEngine(f);
        await ManagedBaseline(engine, f, clock, 3);
        var original = f.Remote.CurrentFixtureNode("file.txt");
        var count = f.Remote.Count;
        var policy = (await engine.ReadMappingPolicyAsync(Ct)) with { ExcludedPrefixes = ["parent/PRIVATE"] };
        await engine.ApplyPolicyAsync(policy, (await engine.PreviewPolicyAsync(policy, Ct)).Digest, Ct);
        File.WriteAllText(Path.Combine(folder, "file.txt"), "edited but excluded");
        var changed = await f.FreshWhenDueAsync(engine, Ct);
        Assert.Null(changed.Error); Assert.Equal(0, changed.PublishedFiles); Assert.Equal(0, changed.CapturedBytes);
        Assert.Equal(original, f.Remote.CurrentFixtureNode("file.txt"));
        // Move the source aside so the native observer proves ancestor absence.
        Directory.Move(Path.Combine(f.Source, "parent"), Path.Combine(f.Directory, "saved-parent"));
        await AwaitHint(f); clock.Advance(TimeSpan.FromSeconds(1));
        var absent = await engine.FreshSyncAsync(Ct);
        var deletes = await engine.PreviewDeletesAsync(Ct);
        await f.Evidence("exclusions-protected-ancestor", new { changed, absent, deletes });
        Assert.Empty(deletes.Operations); Assert.Empty(f.Remote.Mutations); Assert.Equal(count, f.Remote.Count);
        Assert.Equal(original, f.Remote.CurrentFixtureNode("file.txt"));
        Assert.Equal("edited but excluded", File.ReadAllText(Path.Combine(f.Directory, "saved-parent", "private", "file.txt")));
    }

    [Fact]
    public async Task FolderExclusionsNativeInitialChangeRestartAndReinclude()
    {
        RequireWindows(); var clock = new Stage3Clock(); await using var f = await WindowsFreshCycleTests.Fixture.Create(clock);
        foreach (var folder in new[] { "project-1", "project-2", "project-20" })
        { Directory.CreateDirectory(Path.Combine(f.Source, folder)); File.WriteAllText(Path.Combine(f.Source, folder, "file.txt"), folder); }
        string scope;
        await using (var engine = await NamespaceEngine(f))
        {
            var policy = (await engine.ReadMappingPolicyAsync(Ct)) with { ExcludedPrefixes = ["PROJECT-2"] };
            var preview = await engine.PreviewPolicyAsync(policy, Ct);
            await engine.ApplyPolicyAsync(policy, preview.Digest, Ct);
            var initial = await f.InitialWhenDueAsync(engine, Ct);
            await f.Evidence("exclusions-initial", initial);
            Assert.Null(initial.Error); Assert.Equal(2, initial.PublishedFiles); Assert.Equal(0, initial.PendingEntries);
            Assert.Equal(0L, await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE json_extract(entry_json,'$.relativePath') LIKE 'project-2/%'"));
            File.WriteAllText(Path.Combine(f.Source, "project-2", "file.txt"), "excluded later bytes");
            var changed = await f.FreshWhenDueAsync(engine, Ct);
            Assert.Null(changed.Error); Assert.Equal(0, changed.PublishedFiles); Assert.Equal(0, changed.CapturedFiles);
            scope = engine.ScopeId;
        }
        f.Store = new(f.Database, clock);
        await using var reopened = await NamespaceEngine(f);
        Assert.Equal(scope, reopened.ScopeId);
        Assert.Equal(new[] { "PROJECT-2" }, (await reopened.ReadMappingPolicyAsync(Ct)).ExcludedPrefixes);
        var included = (await reopened.ReadMappingPolicyAsync(Ct)) with { ExcludedPrefixes = [] };
        await reopened.ApplyPolicyAsync(included, (await reopened.PreviewPolicyAsync(included, Ct)).Digest, Ct);
        var next = await f.FreshWhenDueAsync(reopened, Ct);
        await f.Evidence("exclusions-reinclude", next);
        Assert.Null(next.Error); Assert.Equal(1, next.PublishedFiles); Assert.Equal(0, next.PendingEntries);
        Assert.Equal("excluded later bytes", File.ReadAllText(Path.Combine(f.Source, "project-2", "file.txt")));
        Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes("excluded later bytes"))), f.Remote.LastInput!.Intent.Sha256);
    }
}
