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
public sealed class CompletionConflictTests
{
    [Fact]
    public async Task ActualIndependentServerAndLocalEditsPreserveBothAfterRestart()
    {
        var ct=TestContext.Current.CancellationToken;
        var evidence=Environment.GetEnvironmentVariable("COMPLETION_EVIDENCE")??throw new InvalidOperationException("Owned live API required");
        using var config=JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(evidence,"clients.private.json"),ct));var c=config.RootElement[0];
        var origin=new Uri(c.GetProperty("origin").GetString()!);var token=c.GetProperty("authorization").GetString()![7..];var device=c.GetProperty("deviceId").GetString()!;var root=c.GetProperty("binding").GetProperty("root_id").GetString()!;var rootName=c.GetProperty("rootName").GetString()!;
        using var http=SyncHttpPolicy.CreateClient();using var request=new HttpRequestMessage(HttpMethod.Get,new Uri(origin,"sync/v1/roots/"+root+"/capabilities"));request.Headers.TryAddWithoutValidation("Authorization","Bearer "+token);
        using var response=await http.SendAsync(request,ct);response.EnsureSuccessStatusCode();
        var capability=WindowsGuardedCapabilities.Parse(await response.Content.ReadAsByteArrayAsync(ct),device,root);
        capability=capability with {Budget=capability.Budget with {MinimumFreeDiskBytes=0}};
        var reuse=Environment.GetEnvironmentVariable("COMPLETION_CONFLICT_STATE");
        var directory=reuse??Path.Combine(evidence,"windows-conflict");
        var source=reuse is null?Path.Combine(directory,"vault-"+Guid.NewGuid().ToString("N")):Directory.GetDirectories(directory,"vault-*").Single();Directory.CreateDirectory(source);
        const string initialText="original shared baseline",remoteText="independent server edit",localText="independent local edit",legalText="pre-existing numbered file";
        static string Hash(string text)=>Convert.ToHexStringLower(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(text)));
        if(reuse is null){await File.WriteAllTextAsync(Path.Combine(source,"conflict.md"),initialText,ct);await File.WriteAllTextAsync(Path.Combine(source,"conflict (2).md"),legalText,ct);}
        else {Assert.Equal(initialText,await File.ReadAllTextAsync(Path.Combine(source,"conflict.md"),ct));Assert.Equal(legalText,await File.ReadAllTextAsync(Path.Combine(source,"conflict (2).md"),ct));}
        var profile=new WindowsProfileContext("conflict",Path.Combine(directory,"profile"),false);Directory.CreateDirectory(profile.StateDirectory);
        var mapping=new SyncMapping("conflict-mapping","conflict-client",source,true,DateTimeOffset.UtcNow);
        var prepared=Activator.CreateInstance(typeof(WindowsSyncService).GetNestedType("PreparedConnection",BindingFlags.NonPublic)!,new object[]{new Uri(origin,"webdav/sync/"),SaturnPalette.Default,token,rootName,origin,device,root})!;
        var method=typeof(WindowsSyncService).GetMethod("SyncGuardedAsync",BindingFlags.Instance|BindingFlags.NonPublic)!;
        var messages=new System.Collections.Concurrent.ConcurrentQueue<string>();var progress=new ImmediateProgress(messages.Enqueue);
        async Task Run(WindowsSyncService service){
            for(var retry=0;;retry++){
                var gate=(SemaphoreSlim)typeof(WindowsSyncService).GetField("_syncGate",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(service)!;await gate.WaitAsync(ct);
                try{await (Task<WindowsSyncResult>)method.Invoke(service,["conflict-client",new[]{mapping},prepared,capability,progress,ct])!;return;}
                catch(IOException error) when(retry<150&&error.Message.EndsWith("GuardedCycleRemoteException: reconciliation_required",StringComparison.Ordinal)){
                    await File.AppendAllTextAsync(Path.Combine(evidence,"admission-retries.jsonl"),JsonSerializer.Serialize(new{at=DateTimeOffset.UtcNow,retry,statePreserved=true})+"\n",ct);
                }finally{gate.Release();}
                await Task.Delay(2000,ct);
            }
        }
        async Task<JsonElement> Remote(){using var r=await http.PostAsJsonAsync(new Uri(origin,"__completion/readback"),new{kind="windows",folder=Path.GetFileName(source),includeVersions=true},ct);r.EnsureSuccessStatusCode();using var d=JsonDocument.Parse(await r.Content.ReadAsByteArrayAsync(ct));return d.RootElement.Clone();}
        static JsonElement Conflict(JsonElement snapshot)=>snapshot.GetProperty("files").EnumerateArray().Single(x=>x.GetProperty("path").GetString()=="conflict.md");
        string? database=null;
        async Task<object?> Scalar(string sql){database??=Directory.GetFiles(profile.StateDirectory,"cycle.db",SearchOption.AllDirectories).Single();await using var db=new SqliteConnection(new SqliteConnectionStringBuilder{DataSource=NeptuneStateStore.NativeDatabasePath(database),Pooling=false}.ToString());await db.OpenAsync(ct);await using var q=db.CreateCommand();q.CommandText=sql;return await q.ExecuteScalarAsync(ct);}
        try {
            await using(var service=new WindowsSyncService(profile)){
                for(var n=0;;n++){await Run(service);var s=await Remote();if(s.GetProperty("files").GetArrayLength()==2)break;Assert.True(n<100,"initial publication missing");await Task.Delay(100,ct);}
            }
            var before=await Remote();Assert.Equal(Hash(initialText),Conflict(before).GetProperty("sha256").GetString());
            var baseline=Assert.IsType<string>(await Scalar("SELECT baseline FROM local_cycle_files WHERE name='conflict.md'"));Assert.Contains(Hash(initialText),baseline);
            using(var edit=await http.PostAsJsonAsync(new Uri(origin,"__completion/remote-edit"),new{kind="windows",folder=Path.GetFileName(source),name="conflict.md",expectedSha256=Hash(initialText),content=remoteText},ct)){edit.EnsureSuccessStatusCode();}
            await File.WriteAllTextAsync(Path.Combine(source,"conflict.md"),localText,ct);
            async Task Verify(){var snapshot=await Remote();var file=Conflict(snapshot);Assert.Equal(Conflict(before).GetProperty("id").GetString(),file.GetProperty("id").GetString());Assert.Equal(Hash(remoteText),file.GetProperty("sha256").GetString());Assert.Equal(2,file.GetProperty("versionCount").GetInt32());
                var hashes=file.GetProperty("versions").EnumerateArray().Select(x=>x.GetProperty("sha256").GetString()).Order().ToArray();Assert.Equal(new[]{Hash(initialText),Hash(remoteText)}.Order().ToArray(),hashes);
                Assert.Equal(baseline,await Scalar("SELECT baseline FROM local_cycle_files WHERE name='conflict.md'"));Assert.Equal(2L,Convert.ToInt64(await Scalar("SELECT phase FROM local_cycle_files WHERE name='conflict.md'")));
                Assert.Equal(localText,await File.ReadAllTextAsync(Path.Combine(source,"conflict.md"),ct));Assert.Equal(legalText,await File.ReadAllTextAsync(Path.Combine(source,"conflict (2).md"),ct));Assert.Equal(2,Directory.GetFiles(source,"*",SearchOption.AllDirectories).Length);
                await File.WriteAllTextAsync(Path.Combine(directory,"server-readback.json"),snapshot.GetRawText(),ct);
            }
            for(var reopen=0;reopen<2;reopen++)await using(var service=new WindowsSyncService(profile)){
                for(var n=0;n<20;n++){await Run(service);if(Convert.ToInt64(await Scalar("SELECT phase FROM local_cycle_files WHERE name='conflict.md'"))==2)break;await Task.Delay(100,ct);}
                await Verify();await Run(service);await Run(service);await Verify();
            }
            Assert.Contains(messages,x=>x.Contains(" conflicts",StringComparison.OrdinalIgnoreCase));
            await File.WriteAllTextAsync(Path.Combine(directory,"result.json"),JsonSerializer.Serialize(new{status="PASS",baselinePreserved=true,serverWinnerPreserved=true,localEditPreserved=true,originalVersionPreserved=true,noLocalCopies=true,reopened=true,messages}),ct);
        } finally {await File.WriteAllTextAsync(Path.Combine(directory,"progress.json"),JsonSerializer.Serialize(messages),ct);}
    }
    private sealed class ImmediateProgress(Action<string> action):IProgress<string>{public void Report(string value)=>action(value);}
}
