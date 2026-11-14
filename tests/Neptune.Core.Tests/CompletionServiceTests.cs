using System.Net;
using System.Net.Http.Json;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("Native fresh cycle")]
[Trait("Lane", "Live")]
public sealed class CompletionServiceTests
{
    [Fact]
    public void LongGuardedProfilePathPinsAndRevalidatesNativeIdentity()
    {
        Assert.True(OperatingSystem.IsWindows());
        var root=Path.Combine(Path.GetTempPath(),"completion-lease-"+Guid.NewGuid().ToString("N"));
        var directory=Path.Combine(root,new string('a',90),new string('b',90),new string('c',90));
        try { using var lease=WindowsGuardedStateLease.Open(directory,true);lease.RequireCurrent();Assert.True(directory.Length>260);Assert.True(Directory.Exists(directory)); }
        finally { if(Directory.Exists(root))Directory.Delete(root,true); }
    }
    [Theory]
    [InlineData("prepare","unchanged")][InlineData("append","unchanged")][InlineData("complete","unchanged")]
    [InlineData("prepare","changed")][InlineData("append","changed")][InlineData("complete","changed")]
    [InlineData("prepare","deleted")][InlineData("append","deleted")][InlineData("complete","deleted")]
    public async Task GuardedServiceReopensDurableQueueOnNetworkWake(string phase,string sourceChange)
    {
        var ct=TestContext.Current.CancellationToken;
        var evidence=Environment.GetEnvironmentVariable("COMPLETION_EVIDENCE")??throw new InvalidOperationException("Own live API required");
        using var document=JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(evidence,"clients.private.json"),ct));var c=document.RootElement[0];
        var origin=new Uri(c.GetProperty("origin").GetString()!);var token=c.GetProperty("authorization").GetString()![7..];var device=c.GetProperty("deviceId").GetString()!;var root=c.GetProperty("binding").GetProperty("root_id").GetString()!;var rootName=c.GetProperty("rootName").GetString()!;
        using var actual=SyncHttpPolicy.CreateClient();using var request=new HttpRequestMessage(HttpMethod.Get,new Uri(origin,"sync/v1/roots/"+root+"/capabilities"));request.Headers.TryAddWithoutValidation("Authorization","Bearer "+token);
        using var response=await actual.SendAsync(request,ct);response.EnsureSuccessStatusCode();
        var capability=WindowsGuardedCapabilities.Parse(await response.Content.ReadAsByteArrayAsync(ct),device,root);
        // Exercise the service's guarded queue on a tiny owned source. Admission
        // ceilings are unchanged; only its configurable disk reserve is zero.
        capability=capability with{Budget=capability.Budget with{MinimumFreeDiskBytes=0},ExtensionFeatures=new HashSet<string>(capability.ExtensionFeatures.Where(x=>x!="clone-content-v1"&&x!="stream-upload-v1"))};
        var directory=Path.Combine(evidence,"service",phase+"-"+sourceChange+"-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(directory);
        var source=Path.Combine(directory,"source-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(source);
        var file=Path.Combine(source,"Агентность.md");var bytes=new byte[1048576+17];new Random(314159).NextBytes(bytes);await File.WriteAllBytesAsync(file,bytes,ct);var hash=Convert.ToHexStringLower(SHA256.HashData(bytes));
        var profile=new WindowsProfileContext("completion",Path.Combine(directory,"profile"),false);Directory.CreateDirectory(profile.StateDirectory);
        var mapping=new SyncMapping("mapping","client",source,true,DateTimeOffset.UtcNow);
        using var probe=new ProbeHandler(actual,origin,root,device,rootName,token);using var probeClient=new HttpClient(probe){Timeout=Timeout.InfiniteTimeSpan};
        await using var service=new WindowsSyncService(profile,probeClient);
        var connection=new WindowsConnection(origin,"fixture-kernel",token,rootName,WindowsKernelCredentialKind.NeptuneOutdoorToken);
        typeof(WindowsSyncService).GetMethod("RememberConnection",BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(service,["client",connection]);
        typeof(WindowsSyncService).GetField("_recoveryEndpoint",BindingFlags.Instance|BindingFlags.NonPublic)!.SetValue(service,((Uri Origin,string Token)?)(origin,token));
        var prepared=Activator.CreateInstance(typeof(WindowsSyncService).GetNestedType("PreparedConnection",BindingFlags.NonPublic)!,new object[]{new Uri(origin,"webdav/sync/"),SaturnPalette.Default,token,rootName,origin,device,root})!;
        var method=typeof(WindowsSyncService).GetMethod("SyncGuardedAsync",BindingFlags.Instance|BindingFlags.NonPublic)!;
        var gate=(SemaphoreSlim)typeof(WindowsSyncService).GetField("_syncGate",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(service)!;
        async Task<WindowsSyncResult> Run(){await gate.WaitAsync(ct);try{return await (Task<WindowsSyncResult>)method.Invoke(service,["client",new[]{mapping},prepared,capability,null,ct])!;}finally{gate.Release();}}
        var wake=new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        service.MappingWorkAvailable+=id=>{if(id.Length==0)wake.TrySetResult();};
        using(var fault=await actual.PostAsJsonAsync(new Uri(new Uri(c.GetProperty("faultOrigin").GetString()!),"__completion/fault"),new{tail="catalog-uploads/"+phase,kind="lost-ack-offline"},ct))fault.EnsureSuccessStatusCode();
        Exception? first=null;for(var initial=0;initial<30&&first is null;initial++){first=await Record.ExceptionAsync(async()=>await Run());if(first is null)await Task.Delay(250,ct);}Assert.NotNull(first);
        await File.WriteAllTextAsync(Path.Combine(directory,"first-error.log"),first.ToString(),ct);
        Assert.True(Directory.GetFiles(profile.StateDirectory,"cycle.db",SearchOption.AllDirectories).Length==1,first.ToString());
        var database=Directory.GetFiles(profile.StateDirectory,"cycle.db",SearchOption.AllDirectories).Single();
        async Task<object?> Scalar(string sql){await using var db=new SqliteConnection(new SqliteConnectionStringBuilder{DataSource=NeptuneStateStore.NativeDatabasePath(database),Pooling=false}.ToString());await db.OpenAsync(ct);await using var q=db.CreateCommand();q.CommandText=sql;return await q.ExecuteScalarAsync(ct);}
        var operation=(string)(await Scalar("SELECT operation_id FROM local_cycle_work WHERE input_json IS NOT NULL ORDER BY work_no LIMIT 1"))!;
        Assert.False(string.IsNullOrEmpty(operation));var original=await Scalar("SELECT input_json FROM local_cycle_work WHERE operation_id='"+operation+"'");
        if(sourceChange=="changed")await File.WriteAllTextAsync(file,"new generation",ct);else if(sourceChange=="deleted")File.Delete(file);
        using(var restored=await actual.PostAsJsonAsync(new Uri(new Uri(c.GetProperty("faultOrigin").GetString()!),"__completion/restore"),new{},ct))restored.EnsureSuccessStatusCode();
        probe.Restored=true;
        await wake.Task.WaitAsync(TimeSpan.FromSeconds(20),ct);
        var history=new List<object>();
        for(var attempt=0;attempt<30;attempt++)
        {
            var error=await Record.ExceptionAsync(async()=>{var result=await Run();history.Add(new{attempt,result.UploadedFiles,result.DeferredFiles});});
            if(error is not null)history.Add(new{attempt,error=error.GetType().Name});
            var published=await Scalar("SELECT phase FROM local_cycle_work WHERE operation_id='"+operation+"'");
            if(Convert.ToInt64(published)==5)break;
            Assert.True(attempt<29,"Service queue did not recover original after route restoration");await Task.Delay(250,ct);
        }
        Assert.Equal(original,await Scalar("SELECT input_json FROM local_cycle_work WHERE operation_id='"+operation+"'"));
        Assert.Equal(hash,await Scalar("SELECT sha256 FROM local_cycle_work WHERE operation_id='"+operation+"'"));
        Assert.True(probe.Probes>0);if(sourceChange=="deleted")Assert.False(File.Exists(file));if(sourceChange=="changed")Assert.Equal("new generation",await File.ReadAllTextAsync(file,ct));
        await File.WriteAllTextAsync(Path.Combine(directory,"result.json"),JsonSerializer.Serialize(new{phase,sourceChange,operation,hash,probe.Probes,history,networkWakeWithoutNewFileEvent=true}),ct);
    }
    private sealed class ProbeHandler(HttpClient actual,Uri origin,string root,string device,string folder,string token):HttpMessageHandler
    {
        public volatile bool Restored;
        public int Probes;
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken ct)
        {
            Assert.Equal("/api/v1/device-session/heartbeat",request.RequestUri!.AbsolutePath);Interlocked.Increment(ref Probes);
            if(!Restored)throw new HttpRequestException("Owned route temporarily unavailable");
            using var check=new HttpRequestMessage(HttpMethod.Get,new Uri(origin,"sync/v1/roots/"+root+"/capabilities"));check.Headers.TryAddWithoutValidation("Authorization","Bearer "+token);
            using var response=await actual.SendAsync(check,ct);response.EnsureSuccessStatusCode();
            return new(HttpStatusCode.OK){RequestMessage=request,Content=JsonContent.Create(new{id=device,syncRootId=root,syncFolderName=folder})};
        }
    }
}
