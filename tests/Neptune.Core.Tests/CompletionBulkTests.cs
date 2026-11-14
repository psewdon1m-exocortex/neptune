using System.Net.Http.Json;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using Neptune.Core;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("Native fresh cycle")]
[Trait("Lane", "Live")]
public sealed class CompletionBulkTests
{
    [Fact]
    public async Task ActualWindowsTreeReadbackRestartAndEditsDoNotCreateLocalCopies()
    {
        var ct=TestContext.Current.CancellationToken;
        var evidence=Environment.GetEnvironmentVariable("COMPLETION_EVIDENCE")??throw new InvalidOperationException("Own live API required");
        var count=Math.Clamp(int.Parse(Environment.GetEnvironmentVariable("ENDURANCE_SYNC_FILES")??"32"),8,4096);
        using var config=JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(evidence,"clients.private.json"),ct));var c=config.RootElement[0];
        var origin=new Uri(c.GetProperty("origin").GetString()!);var token=c.GetProperty("authorization").GetString()![7..];var device=c.GetProperty("deviceId").GetString()!;var root=c.GetProperty("binding").GetProperty("root_id").GetString()!;var rootName=c.GetProperty("rootName").GetString()!;
        using var http=SyncHttpPolicy.CreateClient();using var request=new HttpRequestMessage(HttpMethod.Get,new Uri(origin,"sync/v1/roots/"+root+"/capabilities"));request.Headers.TryAddWithoutValidation("Authorization","Bearer "+token);
        using var response=await http.SendAsync(request,ct);response.EnsureSuccessStatusCode();
        var capability=WindowsGuardedCapabilities.Parse(await response.Content.ReadAsByteArrayAsync(ct),device,root);
        capability=capability with {Budget=capability.Budget with {MinimumFreeDiskBytes=0}};
        var directory=Path.Combine(evidence,"windows-bulk");
        var resumeExisting=Environment.GetEnvironmentVariable("COMPLETION_RESUME_EXISTING")=="1";
        var sharedFault=Environment.GetEnvironmentVariable("COMPLETION_SHARED_FAULT")=="1";
        var source=resumeExisting?Assert.Single(Directory.GetDirectories(directory,"vault-*")):Path.Combine(directory,"vault-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(source);
        var expected=new SortedDictionary<string,string>(StringComparer.Ordinal);
        async Task Write(string name,byte[] bytes){var file=Path.Combine(source,name.Replace('/',Path.DirectorySeparatorChar));Directory.CreateDirectory(Path.GetDirectoryName(file)!);await File.WriteAllBytesAsync(file,bytes,ct);expected[name]=Convert.ToHexStringLower(SHA256.HashData(bytes));}
        for(var i=0;i<count;i++)
        {
            var name=i==0?"example.md":i==1?"example (2).md":$"дерево-{i%8}/level/файл-{i:D5}.bin";
            var bytes=i==2?[]:System.Text.Encoding.UTF8.GetBytes($"{i:D5}: deterministic initial bytes");
            if(!resumeExisting)await Write(name,bytes);
            else {expected[name]=Convert.ToHexStringLower(SHA256.HashData(bytes));Assert.Equal(bytes,await File.ReadAllBytesAsync(Path.Combine(source,name.Replace('/',Path.DirectorySeparatorChar)),ct));}
        }
        Directory.CreateDirectory(Path.Combine(source,"empty"));
        var profile=new WindowsProfileContext("bulk",Path.Combine(directory,"profile"),false);Directory.CreateDirectory(profile.StateDirectory);
        var mapping=new SyncMapping("bulk-mapping","bulk-client",source,true,DateTimeOffset.UtcNow);
        var prepared=Activator.CreateInstance(typeof(WindowsSyncService).GetNestedType("PreparedConnection",BindingFlags.NonPublic)!,new object[]{new Uri(origin,"webdav/sync/"),SaturnPalette.Default,token,rootName,origin,device,root})!;
        var method=typeof(WindowsSyncService).GetMethod("SyncGuardedAsync",BindingFlags.Instance|BindingFlags.NonPublic)!;
        var events=new System.Collections.Concurrent.ConcurrentQueue<object>();
        using var watcher=new FileSystemWatcher(source){IncludeSubdirectories=true,EnableRaisingEvents=true};
        watcher.Created+=(_,e)=>events.Enqueue(new{kind="created",e.FullPath});watcher.Deleted+=(_,e)=>events.Enqueue(new{kind="deleted",e.FullPath});watcher.Renamed+=(_,e)=>events.Enqueue(new{kind="renamed",e.FullPath,e.OldFullPath});watcher.Changed+=(_,e)=>events.Enqueue(new{kind="changed",e.FullPath});
        async Task<JsonElement> Remote(){using var r=await http.PostAsJsonAsync(new Uri(origin,"__completion/readback"),new{kind="windows",folder=Path.GetFileName(source)},ct);r.EnsureSuccessStatusCode();using var d=JsonDocument.Parse(await r.Content.ReadAsByteArrayAsync(ct));return d.RootElement.Clone();}
        void Local(){var actual=Directory.GetFiles(source,"*",SearchOption.AllDirectories).ToDictionary(f=>Path.GetRelativePath(source,f).Replace('\\','/'),f=>Convert.ToHexStringLower(SHA256.HashData(File.ReadAllBytes(f))));Assert.Equal(expected.Count,actual.Count);foreach(var p in expected)Assert.Equal(p.Value,actual[p.Key]);}
        bool Matches(JsonElement snapshot){var files=snapshot.GetProperty("files").EnumerateArray().ToArray();return files.Length==expected.Count&&files.All(f=>expected.TryGetValue(f.GetProperty("path").GetString()!,out var hash)&&hash==f.GetProperty("sha256").GetString()&&hash==f.GetProperty("catalogSha256").GetString()&&f.GetProperty("size").GetInt64().ToString()==f.GetProperty("catalogSize").GetString());}
        var progress=new SampledProgress(Path.Combine(directory,"progress.json"));
        async Task<bool> Run(WindowsSyncService service){var gate=(SemaphoreSlim)typeof(WindowsSyncService).GetField("_syncGate",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(service)!;await gate.WaitAsync(ct);try{await (Task<WindowsSyncResult>)method.Invoke(service,["bulk-client",new[]{mapping},prepared,capability,progress,ct])!;return true;}
            catch(Exception error)when(sharedFault&&!ct.IsCancellationRequested&&NetworkFailure.State(error)=="Waiting for network")
            {await File.AppendAllTextAsync(Path.Combine(directory,"scheduled-retries.jsonl"),JsonSerializer.Serialize(new{at=DateTimeOffset.UtcNow,error=error.GetType().Name,originalStateRetained=true})+"\n",ct);return false;}
            finally{gate.Release();}}
        async Task<JsonElement> Converge(WindowsSyncService service,string stage){for(var n=0;n<120;n++){if(!await Run(service)){await Task.Delay(500,ct);continue;}var actual=await Remote();await File.WriteAllTextAsync(Path.Combine(directory,stage+"-remote.json"),actual.GetRawText(),ct);if(Matches(actual)){Local();return actual;}await Task.Delay(250,ct);}throw new InvalidOperationException("Bulk source did not converge: "+stage);}
        try{
            JsonElement initial;
            await using(var service=new WindowsSyncService(profile)){initial=await Converge(service,"initial");}
            await using(var service=new WindowsSyncService(profile)){
                await Run(service);await Run(service);var unchanged=await Remote();Assert.Equal(initial.GetRawText(),unchanged.GetRawText());Local();
                await Write("example.md",System.Text.Encoding.UTF8.GetBytes("updated generation preserved"));
                await Converge(service,"edited");
                Assert.Equal(expected["example (2).md"],Convert.ToHexStringLower(SHA256.HashData(await File.ReadAllBytesAsync(Path.Combine(source,"example (2).md"),ct))));
                if(int.Parse(Environment.GetEnvironmentVariable("ENDURANCE_TIER")??"0")>=6){
                    var file=Path.Combine(source,"example.md");var originalTime=File.GetLastWriteTimeUtc(file);var replacement=await File.ReadAllBytesAsync(file,ct);replacement[0]^=1;
                    await Write("example.md",replacement);File.SetLastWriteTimeUtc(file,originalTime);await Converge(service,"same-size-mtime");
                    var before=await Remote();var ids=before.GetProperty("files").EnumerateArray().ToDictionary(x=>x.GetProperty("path").GetString()!,x=>x.GetProperty("id").GetString()!);
                    Directory.Move(Path.Combine(source,"дерево-0"),Path.Combine(source,"moved-tree"));
                    foreach(var name in expected.Keys.Where(x=>x.StartsWith("дерево-0/",StringComparison.Ordinal)).ToArray()){expected["moved-tree/"+name[9..]]=expected[name];expected.Remove(name);}
                    var moved=await Converge(service,"tree-moved");
                    foreach(var item in moved.GetProperty("files").EnumerateArray()){var name=item.GetProperty("path").GetString()!;if(name.StartsWith("moved-tree/",StringComparison.Ordinal))Assert.Equal(ids["дерево-0/"+name[11..]],item.GetProperty("id").GetString());}
                    File.Move(Path.Combine(source,"example (2).md"),Path.Combine(source,"EXAMPLE (2).md"));expected["EXAMPLE (2).md"]=expected["example (2).md"];expected.Remove("example (2).md");
                    var renamed=await Converge(service,"case-renamed");Assert.Equal(ids["example (2).md"],renamed.GetProperty("files").EnumerateArray().Single(x=>x.GetProperty("path").GetString()=="EXAMPLE (2).md").GetProperty("id").GetString());
                    var removed=expected.Keys.First(x=>x.StartsWith("дерево-1/",StringComparison.Ordinal));File.Delete(Path.Combine(source,removed.Replace('/',Path.DirectorySeparatorChar)));
                    WindowsMappingReview? review=null;
                    for(var n=0;n<120;n++){await Run(service);review=await service.ReviewMappingAsync(mapping.MappingId,ct);if(review.Deletes.Operations.Count>0)break;await Task.Delay(250,ct);}
                    Assert.NotNull(review);var pending=Assert.Single(review.Deletes.Operations);Assert.Equal(removed,pending.Path);Assert.False(pending.Approved);
                    var retained=await Remote();Assert.True(Matches(retained));await File.WriteAllTextAsync(Path.Combine(directory,"delete-review.json"),JsonSerializer.Serialize(review.Deletes),ct);
                    await service.ApproveDeletesAsync(mapping.MappingId,review.Deletes.Digest,ct);expected.Remove(removed);await Converge(service,"local-delete-approved");
                }
            }
            await File.WriteAllTextAsync(Path.Combine(directory,"result.json"),JsonSerializer.Serialize(new{status="PASS",count,initialAndRestartAndEdit=true,namespaceChangesVerified=int.Parse(Environment.GetEnvironmentVariable("ENDURANCE_TIER")??"0")>=6,expected}),ct);
        }finally{await File.WriteAllTextAsync(Path.Combine(directory,"watcher.json"),JsonSerializer.Serialize(events),ct);}
    }
    private sealed class SampledProgress(string path):IProgress<string>
    {
        private readonly object _gate=new();private long _events;private DateTimeOffset _last;
        public void Report(string value){lock(_gate){_events++;var now=DateTimeOffset.UtcNow;if(now-_last<TimeSpan.FromSeconds(10))return;_last=now;File.WriteAllText(path,JsonSerializer.Serialize(new{at=now,events=_events,progress=value}));}}
    }
}
