using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsEnrollmentTests
{
    [Fact, Trait("Lane", "Live")]
    public async Task InPlaceClientUpdatePreservesConnectionIdentityAndMappings()
    {
        // DPAPI is the production credential boundary and can only be exercised
        // on the Windows CI lane. The rest of the suite still compiles/runs on Linux.
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        var root = Path.Combine(Path.GetTempPath(),"neptune-windows-upgrade-"+Guid.NewGuid().ToString("N"));
        var profile = WindowsProfileContext.FromArguments(["--profile", "upgrade-" + Guid.NewGuid().ToString("N")]);
        var stateDirectory = profile.StateDirectory;
        var source = Path.Combine(root,"new-release");
        var target = Path.Combine(root,"installed-application");
        Directory.CreateDirectory(source); Directory.CreateDirectory(target);
        var payload = Environment.GetEnvironmentVariable("ENDURANCE_WINDOWS_PAYLOAD");
        Assert.True(payload is not null && File.Exists(Path.Combine(payload, "Neptune.Windows.exe")), "A published Windows payload is required in ENDURANCE_WINDOWS_PAYLOAD for the real update health process.");
        foreach(var file in Directory.EnumerateFiles(payload!, "*", SearchOption.AllDirectories))
        {
            var destination = Path.Combine(source, Path.GetRelativePath(payload!, file));
            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            File.Copy(file, destination);
        }
        await File.WriteAllTextAsync(Path.Combine(target,"Neptune.Windows.exe"),"old",TestContext.Current.CancellationToken);
        var connection = new WindowsConnection(new Uri("https://kernel.test"),"kernel-token",new string('s',43),"Assigned PC");
        var connections = new WindowsConnectionStore(profile.StateDirectory);
        var state = new NeptuneStateStore(Path.Combine(profile.StateDirectory,"neptune.db"));
        try
        {
            connections.Write(connection);
            var connectionPath = Path.Combine(profile.StateDirectory, "connection.json");
            var legacyJson = JsonNode.Parse(await File.ReadAllTextAsync(connectionPath, TestContext.Current.CancellationToken))!.AsObject();
            legacyJson.Remove("KernelCredentialKind");
            await File.WriteAllTextAsync(connectionPath, legacyJson.ToJsonString(), TestContext.Current.CancellationToken);
            Assert.Equal(WindowsKernelCredentialKind.LegacyServiceToken, connections.Read()!.KernelCredentialKind);
            var outdoorConnection = connection with { KernelCredentialKind = WindowsKernelCredentialKind.NeptuneOutdoorToken };
            connections.Write(outdoorConnection);
            Assert.Equal(outdoorConnection, connections.Read());
            connections.Write(connection);
            var clientId = await new ClientIdentityStore(profile.StateDirectory).GetOrCreateAsync(TestContext.Current.CancellationToken);
            await state.InitializeAsync(TestContext.Current.CancellationToken);
            var mapping = new SyncMapping("mapping-existing",clientId,Path.Combine(root,"Documents"),true,DateTimeOffset.UtcNow);
            await state.AddMappingAsync(mapping,TestContext.Current.CancellationToken);

            // The updater normally runs after the application exits. Release
            // pooled test connections to reproduce that closed-process boundary.
            SqliteConnection.ClearAllPools();
            WindowsUpdateTransaction.ApplyPortable(source,target,profile.ProfileId,relaunch:false);
            var relaunch = UpdateApplyMode.CreateRelaunchStartInfo(target,profile.ProfileId);

            Assert.Equal(System.Security.Cryptography.SHA256.HashData(await File.ReadAllBytesAsync(Path.Combine(source,"Neptune.Windows.exe"),TestContext.Current.CancellationToken)),
                System.Security.Cryptography.SHA256.HashData(await File.ReadAllBytesAsync(Path.Combine(target,"Neptune.Windows.exe"),TestContext.Current.CancellationToken)));
            Assert.Equal(["--profile",profile.ProfileId],relaunch.ArgumentList);
            Assert.Equal(connection,connections.Read());
            Assert.Equal(clientId,await new ClientIdentityStore(profile.StateDirectory).GetOrCreateAsync(TestContext.Current.CancellationToken));
            Assert.Equal(mapping,Assert.Single(await new NeptuneStateStore(Path.Combine(profile.StateDirectory,"neptune.db"))
                .ListMappingsAsync(TestContext.Current.CancellationToken)));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if(Directory.Exists(root)) Directory.Delete(root,true);
            if(Directory.Exists(stateDirectory)) Directory.Delete(stateDirectory,true);
        }
    }

    [Theory]
    [InlineData("Office PC")]
    [InlineData("Домашний ПК")]
    public async Task UsesServerAssignedFolderForEnrollmentAndEveryDavRequest(string folder)
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-windows-enrollment-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var mappingDirectory = Path.Combine(directory, "Documents"); Directory.CreateDirectory(mappingDirectory);
        using var handler = new Handler(folder);
        using var http = new HttpClient(handler);
        var service = new WindowsSyncService(new WindowsProfileContext("test", directory, false), http);
        var connection = new WindowsConnection(new Uri("https://kernel.test"), "kernel-fixture", new string('a', 32), "caller-selected-folder");
        try
        {
            var result = await service.ConnectAsync("client-id", connection, TestContext.Current.CancellationToken);
            Assert.Equal(folder, result.Connection.RemoteFolder);
            Assert.Equal(new string('b', 43), result.Connection.SaturnToken);
            Assert.Equal(1,handler.EnrollmentRequests);
            Assert.Empty(handler.DavRequests);
            var mapping = new SyncMapping("mapping-id", "client-id", mappingDirectory, true, DateTimeOffset.UtcNow);
            await service.SyncAsync("client-id", [mapping], result.Connection, cancellationToken: TestContext.Current.CancellationToken);
            Assert.NotEmpty(handler.DavRequests);
            Assert.All(handler.DavRequests, uri => Assert.StartsWith("/webdav/sync/" + Uri.EscapeDataString(folder) + "/Documents/", uri.AbsolutePath));
        }
        finally { SqliteConnection.ClearAllPools(); Directory.Delete(directory, true); }
    }

    [Theory]
    [InlineData(50)]
    [InlineData(-1)]
    public async Task DefersOrRetriesAnObsidianStyleAtomicReplacementWithoutFailingTheWholeSync(int recreateAfterMilliseconds)
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-windows-replace-" + Guid.NewGuid().ToString("N"));
        var mappingDirectory = Path.Combine(directory, "obsidian");
        Directory.CreateDirectory(mappingDirectory);
        var file = Path.Combine(mappingDirectory, "Агентность, допуск контроля, инициативность.md");
        const string content = "valuable markdown";
        await File.WriteAllTextAsync(file, content, TestContext.Current.CancellationToken);
        using var handler = new Handler("Assigned");
        Task replacement = Task.CompletedTask;
        var replaced = false;
        var uploaded = false;
        var digest = Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(content)));
        handler.DavOverride = request =>
        {
            var route = request.RequestUri!.AbsolutePath;
            if (!replaced && request.Method.Method == "MKCOL" && route.EndsWith("/obsidian/", StringComparison.Ordinal))
            {
                replaced = true;
                File.Delete(file);
                if (recreateAfterMilliseconds >= 0)
                    replacement = Task.Run(async () =>
                    {
                        await Task.Delay(recreateAfterMilliseconds);
                        await File.WriteAllTextAsync(file, content);
                    });
                return new(HttpStatusCode.Created);
            }
            if (request.Method == HttpMethod.Head && route.EndsWith(".md", StringComparison.Ordinal)) return new(HttpStatusCode.NotFound);
            if (request.Method == HttpMethod.Put && route.EndsWith(".md", StringComparison.Ordinal))
            {
                uploaded = true;
                var response = new HttpResponseMessage(HttpStatusCode.Created);
                response.Headers.ETag = new System.Net.Http.Headers.EntityTagHeaderValue($"\"sha256-{digest}\"");
                return response;
            }
            return null;
        };
        using var http = new HttpClient(handler);
        var service = new WindowsSyncService(new WindowsProfileContext("test", directory, false), http);
        var connection = new WindowsConnection(new Uri("https://kernel.test"), "fixture", new string('b', 43), "Assigned");
        try
        {
            var mapping = new SyncMapping("mapping-id", "client", mappingDirectory, true, DateTimeOffset.UtcNow);
            var state = new NeptuneStateStore(Path.Combine(directory, "neptune.db"));
            await state.InitializeAsync(TestContext.Current.CancellationToken);
            await state.AddMappingAsync(mapping, TestContext.Current.CancellationToken);
            var result = await service.SyncAsync("client", [mapping], connection, cancellationToken: TestContext.Current.CancellationToken);
            Assert.True(replaced);
            Assert.Equal(recreateAfterMilliseconds >= 0, uploaded);
            Assert.Equal(recreateAfterMilliseconds >= 0 ? 1 : 0, result.UploadedFiles);
            Assert.Equal(recreateAfterMilliseconds >= 0 ? 0 : 1, result.DeferredFiles);
            if (recreateAfterMilliseconds >= 0) Assert.Equal(content, await File.ReadAllTextAsync(file, TestContext.Current.CancellationToken));
            else Assert.False(File.Exists(file));
        }
        finally
        {
            await replacement;
            SqliteConnection.ClearAllPools();
            Directory.Delete(directory, true);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LegacyRechecksContentAndRetainsAFileMissingAcrossScanAndRestart(bool temporarilyMissing)
    {
        var ct=TestContext.Current.CancellationToken;
        var directory=Path.Combine(Path.GetTempPath(),"neptune-editor-regression-"+Guid.NewGuid().ToString("N"));
        var source=Path.Combine(directory,"vault");Directory.CreateDirectory(source);
        var file=Path.Combine(source,"note.md");await File.WriteAllTextAsync(file,"version1",ct);
        string? remoteDigest=null;var puts=0;var listings=0;
        using var handler=new Handler("Assigned");
        handler.DavOverride=request=>
        {
            if(request.Method.Method=="PROPFIND")listings++;
            if(!request.RequestUri!.AbsolutePath.EndsWith("/note.md",StringComparison.Ordinal))return null;
            if(request.Method==HttpMethod.Put)
            {
                var bytes=request.Content!.ReadAsByteArrayAsync(ct).GetAwaiter().GetResult();
                remoteDigest=Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(bytes));puts++;
            }
            if(request.Method==HttpMethod.Head||request.Method==HttpMethod.Put)
            {
                var reply=new HttpResponseMessage(remoteDigest is null?HttpStatusCode.NotFound:HttpStatusCode.OK);
                if(remoteDigest is not null)reply.Headers.ETag=new($"\"sha256-{remoteDigest}\"");
                return reply;
            }
            return null;
        };
        using var http=new HttpClient(handler);
        var profile=new WindowsProfileContext("test",directory,false);
        var service=new WindowsSyncService(profile,http);
        var connection=new WindowsConnection(new Uri("https://kernel.test"),"fixture",new string('b',43),"Assigned");
        var mapping=new SyncMapping("mapping-id","client",source,true,DateTimeOffset.UtcNow);
        var state=new NeptuneStateStore(Path.Combine(directory,"neptune.db"));
        try
        {
            await state.InitializeAsync(ct);await state.AddMappingAsync(mapping,ct);
            Assert.Equal(1,(await service.SyncAsync("client",[mapping],connection,cancellationToken:ct)).UploadedFiles);
            if(temporarilyMissing)
            {
                File.Move(file,Path.Combine(directory,"retained.md"));var previousListings=listings;
                Assert.Equal(1,(await service.SyncAsync("client",[mapping],connection,cancellationToken:ct)).DeferredFiles);
                var missing=await state.GetSyncFileAsync(mapping.MappingId,"note.md",ct);Assert.Equal("missing-local",missing!.State);
                Assert.Equal(previousListings,listings);
                await service.DisposeAsync();service=new(profile,http);
                Assert.Equal(1,(await service.SyncAsync("client",[mapping],connection,cancellationToken:ct)).DeferredFiles);
                Assert.Equal(missing.UpdatedAt,(await state.GetSyncFileAsync(mapping.MappingId,"note.md",ct))!.UpdatedAt);
                Assert.Equal(previousListings,listings);
                File.Move(Path.Combine(directory,"retained.md"),file);
                Assert.Equal(0,(await service.SyncAsync("client",[mapping],connection,cancellationToken:ct)).DeferredFiles);
                Assert.Equal("synced",(await state.GetSyncFileAsync(mapping.MappingId,"note.md",ct))!.State);
                Assert.Equal(1,puts);
            }
            else
            {
                var before=new FileInfo(file);var modified=before.LastWriteTimeUtc;var length=before.Length;
                await File.WriteAllTextAsync(file,"version2",ct);File.SetLastWriteTimeUtc(file,modified);
                Assert.Equal(length,new FileInfo(file).Length);Assert.Equal(modified,File.GetLastWriteTimeUtc(file));
                Assert.Equal(1,(await service.SyncAsync("client",[mapping],connection,cancellationToken:ct)).UploadedFiles);
                Assert.Equal(Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes("version2"))),remoteDigest);
                Assert.Equal(0,(await service.SyncAsync("client",[mapping],connection,cancellationToken:ct)).UploadedFiles);
                Assert.Equal(2,puts);
            }
        }
        finally{await service.DisposeAsync();SqliteConnection.ClearAllPools();Directory.Delete(directory,true);}
    }

    [Theory]
    [InlineData(null)]
    [InlineData("../other")]
    [InlineData("sync/other")]
    [InlineData("..")]
    public async Task FailsBeforeDavWhenServerHasNoSafeFolderAssignment(string? folder)
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-windows-unbound-" + Guid.NewGuid().ToString("N"));
        using var handler = new Handler(folder); using var http = new HttpClient(handler);
        var service = new WindowsSyncService(new WindowsProfileContext("test", directory, false), http);
        try
        {
            await Assert.ThrowsAsync<InvalidDataException>(() => service.ConnectAsync("client", new WindowsConnection(new Uri("https://kernel.test"), "fixture", new string('b', 43), "old-folder"), TestContext.Current.CancellationToken));
            Assert.Empty(handler.DavRequests);
        }
        finally { SqliteConnection.ClearAllPools(); if (Directory.Exists(directory)) Directory.Delete(directory, true); }
    }

    [Fact]
    public async Task UsesBoundedMetadataCacheDuringKernelOutageButStopsOnAuthorizationRejection()
    {
        var directory=Path.Combine(Path.GetTempPath(),"neptune-cache-"+Guid.NewGuid().ToString("N"));
        using var handler=new Handler("Assigned");using var http=new HttpClient(handler);
        var service=new WindowsSyncService(new WindowsProfileContext("test",directory,false),http);
        var connection=new WindowsConnection(new Uri("https://kernel.test"),"fixture",new string('b',43),"Assigned");
        try {
            await service.ConnectAsync("client",connection,TestContext.Current.CancellationToken);
            Assert.Equal(0,handler.EnrollmentRequests);
            handler.KernelFailure=HttpStatusCode.ServiceUnavailable;
            Assert.Equal("Assigned",(await service.ConnectAsync("client",connection,TestContext.Current.CancellationToken)).Connection.RemoteFolder);
            await Assert.ThrowsAsync<HttpRequestException>(()=>service.ConnectAsync("client",connection with{KernelToken="different"},TestContext.Current.CancellationToken));
            handler.KernelFailure=HttpStatusCode.Forbidden;
            await Assert.ThrowsAsync<HttpRequestException>(()=>service.ConnectAsync("client",connection,TestContext.Current.CancellationToken));
            handler.KernelFailure=HttpStatusCode.ServiceUnavailable;
            await Assert.ThrowsAsync<HttpRequestException>(()=>service.ConnectAsync("client",connection,TestContext.Current.CancellationToken));
        } finally {SqliteConnection.ClearAllPools();if(Directory.Exists(directory))Directory.Delete(directory,true);}
    }

    [Fact]
    public async Task NeptuneOutdoorConnectionUsesOutdoorDiscoveryWithoutDownloadingRegisterSnapshot()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-outdoor-enrollment-" + Guid.NewGuid().ToString("N"));
        using var handler = new Handler("Assigned"); using var http = new HttpClient(handler);
        var service = new WindowsSyncService(new WindowsProfileContext("test", directory, false), http);
        var connection = new WindowsConnection(new Uri("https://kernel.test"), "outdoor-fixture", new string('b', 43), "Assigned", WindowsKernelCredentialKind.NeptuneOutdoorToken);
        try
        {
            var result = await service.ConnectAsync("client", connection, TestContext.Current.CancellationToken);
            Assert.Equal(1, handler.OutdoorRequests);
            Assert.Equal(0, handler.SnapshotRequests);
            Assert.Equal(WindowsKernelCredentialKind.NeptuneOutdoorToken, result.Connection.KernelCredentialKind);
        }
        finally { SqliteConnection.ClearAllPools(); if (Directory.Exists(directory)) Directory.Delete(directory, true); }
    }

    private sealed class Handler(string? folder) : HttpMessageHandler
    {
        public Func<HttpRequestMessage, HttpResponseMessage?>? DavOverride { get; set; }
        public HttpStatusCode? KernelFailure {get;set;}
        public int EnrollmentRequests {get;private set;}
        public int OutdoorRequests { get; private set; }
        public int SnapshotRequests { get; private set; }
        public List<Uri> DavRequests { get; } = [];
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var route = request.RequestUri!.AbsolutePath;
            if (request.RequestUri.Host=="kernel.test" && KernelFailure is { } failure) return Task.FromResult(new HttpResponseMessage(failure));
            HttpResponseMessage Json(object body) => new(HttpStatusCode.OK) { Content = JsonContent.Create(body) };
            if (route == "/api/v1/register/snapshot")
            {
                SnapshotRequests++;
                using var values = JsonDocument.Parse("""{"services":{"saturn":{"sni":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/1","port":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/2","paths":{"sync":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/3","sync_preferences":"volt://3518462b-bb66-459a-a1ab-c38a837740ab/4"}}}}""");
                return Task.FromResult(Json(new { schema = "exocortex.register.snapshot.v1", revision = 1, checksum = CanonicalJson.ComputeRegisterChecksum(values.RootElement), values = values.RootElement.Clone() }));
            }
            if (route == "/api/v1/register/resolve") return Task.FromResult(Json(new { schema = "exocortex.register.resolution.v1", values = new Dictionary<string, object> {
                ["services.saturn.sni"] = new { value = "saturn.test" }, ["services.saturn.port"] = new { value = "443" },
                ["services.saturn.paths.sync"] = new { value = "/webdav/sync/" }, ["services.saturn.paths.sync_preferences"] = new { value = "/preferences" }
            } }));
            if (route == "/api/v1/outdoor/resolve")
            {
                OutdoorRequests++;
                return Task.FromResult(Json(new { schema = "exocortex.register.resolution.v1", register_revision = "register-1", values = new Dictionary<string, object> {
                    ["services.saturn.sni"] = new { value = "saturn.test", secret = false }, ["services.saturn.port"] = new { value = "443", secret = false },
                    ["services.saturn.paths.sync"] = new { value = "/webdav/sync/", secret = false }, ["services.saturn.paths.sync_preferences"] = new { value = "/preferences", secret = false }
                } }));
            }
            if (route == "/api/v1/device-enrollments/redeem") { EnrollmentRequests++; return Task.FromResult(Json(new { token = new string('b', 43) })); }
            if (route == "/api/v1/device-session/heartbeat")
            {
                Assert.Equal(new string('b', 43), request.Headers.Authorization?.Parameter);
                return Task.FromResult(Json(new { id = "82b44ad9-6360-45cc-9a0c-a36cce13b559", syncRootId = "d3eeae64-79d8-492b-b4b7-3b535ebd2a48", syncFolderName = folder }));
            }
            if (route == "/sync/v1/roots/d3eeae64-79d8-492b-b4b7-3b535ebd2a48/capabilities")
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound) { RequestMessage = request });
            if (route == "/preferences") return Task.FromResult(Json(new { accentColor = "#00A8FF" }));
            if (route.StartsWith("/webdav/", StringComparison.Ordinal))
            {
                DavRequests.Add(request.RequestUri);
                Assert.Equal(new string('b', 43), request.Headers.Authorization?.Parameter);
                var selected = DavOverride?.Invoke(request);
                if (selected is not null) return Task.FromResult(selected);
                return Task.FromResult(request.Method.Method == "PROPFIND"
                    ? new HttpResponseMessage((HttpStatusCode)207) { Content = new StringContent("<d:multistatus xmlns:d=\"DAV:\" />") }
                    : new HttpResponseMessage(HttpStatusCode.Created));
            }
            throw new InvalidOperationException("Unexpected request: " + request);
        }
    }
}
