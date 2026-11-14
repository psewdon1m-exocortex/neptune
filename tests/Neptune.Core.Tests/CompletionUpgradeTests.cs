using System.Net;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text.Json;
using Neptune.Core;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class CompletionUpgradeTests
{
    [Fact]
    public async Task PublicServiceRecoversLegacyBeforeGuardedNegotiation()
    {
        var ct=TestContext.Current.CancellationToken;var directory=Path.Combine(Path.GetTempPath(),"completion-upgrade-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        try
        {
            var source=Path.Combine(directory,"original.bin");var bytes=new byte[1024*1024+17];RandomNumberGenerator.Fill(bytes);await File.WriteAllBytesAsync(source,bytes,ct);
            var profile=new WindowsProfileContext("upgrade",directory,false);Directory.CreateDirectory(profile.StateDirectory);
            var spool=Path.Combine(profile.StateDirectory,"legacy-originals");var database=Path.Combine(profile.StateDirectory,"neptune.db");var store=new NeptuneStateStore(database);await store.InitializeAsync(ct);
            using var handler=new UpgradeHandler(bytes,spool);using var http=new HttpClient(handler);var token=new string('b',43);
            var client=new WebDavSyncClient(http,store){RetainedOriginalsDirectory=spool};
            await Assert.ThrowsAsync<HttpRequestException>(()=>client.UploadAsync(new Uri("https://saturn.test/webdav/sync/Assigned/"),token,"original.bin",source,ct));
            File.Delete(source);Assert.Single(Directory.GetFiles(spool,"*.bin"));
            await using var service=new WindowsSyncService(profile,http);
            var error=await Assert.ThrowsAsync<InvalidDataException>(()=>service.SyncAsync("client",[],new WindowsConnection(new Uri("https://kernel.test"),"outdoor",token,"Assigned",WindowsKernelCredentialKind.NeptuneOutdoorToken),null,ct));
            Assert.Equal("qualified-capability-boundary",error.Message);Assert.True(handler.CapabilityReached);Assert.Equal(1,handler.Publications);Assert.Equal(bytes,handler.Accepted.ToArray());Assert.False(File.Exists(source));
        }
        finally{Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();Directory.Delete(directory,true);}
    }
    private sealed class UpgradeHandler(byte[] bytes,string spool):HttpMessageHandler
    {
        internal bool CapabilityReached;internal int Publications;internal MemoryStream Accepted=new();
        private bool lost;private string? key;private readonly string id=Guid.NewGuid().ToString("D");
        private HttpResponseMessage Status()=>new(HttpStatusCode.OK){Content=JsonContent.Create(new{id,status=Publications>0?"active":"uploading",receivedSize=Accepted.Length,expectedSize=bytes.Length,etag=Publications>0?"\"sha256-"+Convert.ToHexStringLower(SHA256.HashData(bytes))+"\"":null})};
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken ct)
        {
            var path=request.RequestUri!.AbsolutePath;
            if(path=="/api/v1/outdoor/resolve")return new(HttpStatusCode.OK){Content=JsonContent.Create(new{schema="exocortex.register.resolution.v1",register_revision="register-1",values=new Dictionary<string,object>{["services.saturn.sni"]=new{value="saturn.test",secret=false},["services.saturn.port"]=new{value="443",secret=false},["services.saturn.paths.sync"]=new{value="/webdav/sync/",secret=false},["services.saturn.paths.sync_preferences"]=new{value="/preferences",secret=false}}})};
            if(path=="/api/v1/device-session/heartbeat")return new(HttpStatusCode.OK){Content=JsonContent.Create(new{id="82b44ad9-6360-45cc-9a0c-a36cce13b559",syncRootId="d3eeae64-79d8-492b-b4b7-3b535ebd2a48",syncFolderName="Assigned"})};
            if(path=="/preferences"||request.Method==HttpMethod.Head)return new(HttpStatusCode.NotFound);
            if(path.EndsWith("/capabilities")){Assert.Empty(Directory.GetFiles(spool,"*.bin"));Assert.Empty(Directory.GetFiles(spool,"*.json"));CapabilityReached=true;throw new InvalidDataException("qualified-capability-boundary");}
            if(path.EndsWith("/complete")){Assert.Equal(bytes,Accepted.ToArray());Publications++;return Status();}
            if(request.Method==HttpMethod.Post){using var json=JsonDocument.Parse(await request.Content!.ReadAsByteArrayAsync(ct));var current=json.RootElement.GetProperty("idempotencyKey").GetString();key??=current;Assert.Equal(key,current);if(!lost){lost=true;throw new HttpRequestException("committed prepare response lost");}return Status();}
            Assert.Equal(HttpMethod.Patch,request.Method);Assert.Equal(Accepted.Length.ToString(),request.Headers.GetValues("Upload-Offset").Single());await request.Content!.CopyToAsync(Accepted,ct);return Status();
        }
    }
    [Fact]
    public async Task LongProfileDatabaseReopensAndBacksUpCommittedState()
    {
        var ct=TestContext.Current.CancellationToken;var root=Path.Combine(Path.GetTempPath(),"completion-db-"+Guid.NewGuid().ToString("N"));
        var profile=new WindowsProfileContext("long",Path.Combine(root,new string('a',90),new string('b',90),new string('c',90)),false);Directory.CreateDirectory(profile.StateDirectory);
        try
        {
            var database=Path.Combine(profile.StateDirectory,"neptune.db");var store=new NeptuneStateStore(database);await store.InitializeAsync(ct);await store.AddMappingAsync(new("mapping","client",root,true,DateTimeOffset.UtcNow),ct);
            await new NeptuneStateStore(database).InitializeAsync(ct);Assert.Single(await WindowsProfileCommands.InspectAsync(profile,ct));
            var backup=Path.Combine(root,"backup");await WindowsProfileCommands.BackupAsync(profile,backup,ct);
            var copy=new NeptuneStateStore(Path.Combine(backup,"neptune.db"));await copy.InitializeAsync(ct);Assert.Single(await copy.ListMappingsAsync(ct));
        }
        finally{Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();Directory.Delete(root,true);}
    }
}
