using System.Net.Http.Json;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class EditingVaultStressTests
{
    [Fact(Skip="Requires explicitly owned retained failure fixture",SkipWhen=nameof(NoFixture))]
    public async Task GuardedRetainedAtomicFailureRecoversWithoutResetOrSourceWrites()
    {
        using var timeout=CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);timeout.CancelAfter(TimeSpan.FromMinutes(4));var ct=timeout.Token;
        var root=Path.GetFullPath(Environment.GetEnvironmentVariable("EDITING_STRESS_ROOT")!);
        Assert.Equal(@"C:\.projects\exocortex\.audit\2026-10-15\guarded-atomic-repair",root,StringComparer.OrdinalIgnoreCase);
        var previous=Path.Combine(root,"attempt-002","guarded");var output=Path.Combine(root,"retained-recovery-02");Directory.CreateDirectory(output);
        using var identity=JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(previous,"identity.json"),ct));
        var source=Path.GetFullPath(identity.RootElement.GetProperty("source").GetString()!);var profilePath=Path.GetFullPath(identity.RootElement.GetProperty("profile").GetString()!);
        Assert.StartsWith(previous+Path.DirectorySeparatorChar,source,StringComparison.OrdinalIgnoreCase);Assert.StartsWith(previous+Path.DirectorySeparatorChar,profilePath,StringComparison.OrdinalIgnoreCase);
        var expected=JsonSerializer.Deserialize<Dictionary<string,string>>(await File.ReadAllTextAsync(Path.Combine(previous,"editor-expected.json"),ct))!;
        Dictionary<string,string> Local()=>Directory.GetFiles(source,"*",SearchOption.AllDirectories).ToDictionary(p=>Path.GetRelativePath(source,p).Replace('\\','/'),p=>Convert.ToHexStringLower(SHA256.HashData(File.ReadAllBytes(p))));
        void CheckLocal(){var actual=Local();Assert.Equal(expected.Count,actual.Count);foreach(var item in expected)Assert.Equal(item.Value,actual[item.Key]);}
        CheckLocal();
        foreach(var database in Directory.GetFiles(profilePath,"*.db",SearchOption.AllDirectories).Where(p=>Path.GetFileName(p) is "neptune.db" or "cycle.db"))
        {
            var backup=Path.Combine(output,"before",Guid.NewGuid().ToString("N")+"-"+Path.GetFileName(database));Directory.CreateDirectory(Path.GetDirectoryName(backup)!);
            using var src=new SqliteConnection(new SqliteConnectionStringBuilder{DataSource=database,Mode=SqliteOpenMode.ReadOnly,Pooling=false}.ToString());await src.OpenAsync(ct);
            using var dst=new SqliteConnection(new SqliteConnectionStringBuilder{DataSource=backup,Pooling=false}.ToString());await dst.OpenAsync(ct);src.BackupDatabase(dst);
        }
        using var config=JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(root,"clients/clients.private.json"),ct));var c=config.RootElement[0];var origin=new Uri(c.GetProperty("origin").GetString()!);
        var token=c.GetProperty("authorization").GetString()![7..];var device=c.GetProperty("deviceId").GetString()!;var rootId=c.GetProperty("binding").GetProperty("root_id").GetString()!;
        using var http=SyncHttpPolicy.CreateClient();using var req=new HttpRequestMessage(HttpMethod.Get,new Uri(origin,"sync/v1/roots/"+rootId+"/capabilities"));req.Headers.TryAddWithoutValidation("Authorization","Bearer "+token);
        using var caps=await http.SendAsync(req,ct);caps.EnsureSuccessStatusCode();var capability=WindowsGuardedCapabilities.Parse(await caps.Content.ReadAsByteArrayAsync(ct),device,rootId);capability=capability with{Budget=capability.Budget with{MinimumFreeDiskBytes=2L*1024*1024*1024}};
        var prepared=Activator.CreateInstance(typeof(WindowsSyncService).GetNestedType("PreparedConnection",BindingFlags.NonPublic)!,[new Uri(origin,"dav/sync/"),SaturnPalette.Default,token,c.GetProperty("rootName").GetString()!,origin,device,rootId]);
        var method=typeof(WindowsSyncService).GetMethod("SyncGuardedAsync",BindingFlags.NonPublic|BindingFlags.Instance)!;
        var state=new NeptuneStateStore(Path.Combine(profilePath,"neptune.db"));var mapping=Assert.Single(await state.ListMappingsAsync(ct));Assert.Equal(source,mapping.LocalPath);
        var profile=new WindowsProfileContext("editing-stress",profilePath,false);await using var service=new WindowsSyncService(profile,http);
        var gate=(SemaphoreSlim)typeof(WindowsSyncService).GetField("_syncGate",BindingFlags.NonPublic|BindingFlags.Instance)!.GetValue(service)!;
        async Task<JsonElement> Readback(){using var response=await http.PostAsJsonAsync(new Uri(origin,"__completion/readback"),new{kind="windows",folder=Path.GetFileName(source)},ct);response.EnsureSuccessStatusCode();using var d=JsonDocument.Parse(await response.Content.ReadAsByteArrayAsync(ct));return d.RootElement.Clone();}
        bool Matches(JsonElement readback){var files=readback.GetProperty("files").EnumerateArray().ToArray();return files.Length==expected.Count&&files.All(f=>expected.TryGetValue(f.GetProperty("path").GetString()!,out var h)&&h==f.GetProperty("sha256").GetString()&&h==f.GetProperty("catalogSha256").GetString());}
        using var initial=JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(previous,"initial-remote.json"),ct));var ids=initial.RootElement.GetProperty("files").EnumerateArray().ToDictionary(f=>f.GetProperty("path").GetString()!,f=>f.GetProperty("id").GetString());
        var before=await Readback();await File.WriteAllTextAsync(Path.Combine(output,"before-remote.json"),before.GetRawText(),ct);Assert.False(Matches(before));
        var cycles=0;JsonElement final=default;
        for(;cycles<100;cycles++)
        {
            await gate.WaitAsync(ct);WindowsSyncResult result;
            try{result=await (Task<WindowsSyncResult>)method.Invoke(service,["editing-client",new[]{mapping},prepared,capability,new CallbackProgress(_=>{}),ct])!;}finally{gate.Release();}
            await File.AppendAllTextAsync(Path.Combine(output,"cycles.jsonl"),JsonSerializer.Serialize(new{at=DateTimeOffset.UtcNow,cycle=cycles+1,result.UploadedFiles,result.DeferredFiles})+"\n",ct);
            final=await Readback();if(Matches(final)){cycles++;break;}await Task.Delay(500,ct);
        }
        await File.WriteAllTextAsync(Path.Combine(output,"final-remote.json"),final.GetRawText(),ct);Assert.True(Matches(final),"Retained failed profile did not converge without reset");
        foreach(var file in final.GetProperty("files").EnumerateArray())Assert.Equal(ids[file.GetProperty("path").GetString()!],file.GetProperty("id").GetString());CheckLocal();
        await File.WriteAllTextAsync(Path.Combine(output,"result.json"),JsonSerializer.Serialize(new{at=DateTimeOffset.UtcNow,status="PASS",cycles,files=expected.Count,sourceWrites=0,profileReset=false,sameRemoteIds=true}),ct);
    }
}
