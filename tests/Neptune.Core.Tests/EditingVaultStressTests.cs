using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Json;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

// Explicit live fixture only. The editor's expected bytes are generated before
// writes; the oracle reads actual backend bytes independently of client ACKs.
[Collection("Native fresh cycle")]
public sealed partial class EditingVaultStressTests
{
    public static bool NoFixture => !OperatingSystem.IsWindows() || string.IsNullOrEmpty(Environment.GetEnvironmentVariable("EDITING_STRESS_ROOT"));
    [Fact(Skip = "Requires an explicitly owned Windows live fixture", SkipWhen = nameof(NoFixture))]
    public Task LegacyVaultUnderContinuousEdits() => Run(false);
    [Fact(Skip = "Requires an explicitly owned Windows live fixture", SkipWhen = nameof(NoFixture))]
    public Task GuardedVaultUnderContinuousEdits() => Run(true);

    private static async Task Run(bool guarded)
    {
        using var deadline=CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        deadline.CancelAfter(TimeSpan.FromMinutes(15));var ct=deadline.Token;
        var root=Path.GetFullPath(Environment.GetEnvironmentVariable("EDITING_STRESS_ROOT")!);
        if(!new[]{@"C:\.projects\exocortex\.audit\2026-10-15\editing-vault-stress",@"C:\.projects\exocortex\.audit\2026-10-15\guarded-atomic-repair",@"C:\.projects\exocortex\.audit\2026-10-18\endurance-6h\editing"}.Contains(root,StringComparer.OrdinalIgnoreCase))throw new InvalidOperationException("Owned fixture required");
        var mode=Environment.GetEnvironmentVariable("EDITING_STRESS_MODE")??"mixed";
        if(mode is not ("mixed" or "in-place"))throw new InvalidDataException("Unknown editor mode");
        var attempt=Environment.GetEnvironmentVariable("EDITING_STRESS_ATTEMPT")??"attempt-001";
        if(attempt.Length>64||attempt.Any(c=>!char.IsAsciiLetterOrDigit(c)&&c!='-'))throw new InvalidDataException("Unsafe attempt");
        var output=Path.Combine(root,attempt,guarded?"guarded":"legacy");Directory.CreateDirectory(output);
        var source=Path.Combine(output,"vault-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(source);
        var profile=new WindowsProfileContext("editing-stress",Path.Combine(output,"profile"),false);Directory.CreateDirectory(profile.StateDirectory);
        var mapping=new SyncMapping("editing-mapping","editing-client",source,true,DateTimeOffset.UtcNow);
        var state=new NeptuneStateStore(Path.Combine(profile.StateDirectory,"neptune.db"));await state.InitializeAsync(ct);await state.AddMappingAsync(mapping,ct);
        var expected=new ConcurrentDictionary<string,string>(StringComparer.Ordinal);
        var findings=new List<string>();var unknownPaths=new ConcurrentDictionary<string,byte>();var events=new ConcurrentQueue<object>();
        var logGate=new object();int edits=0,cycles=0,deferred=0,uploads=0,writerRetries=0,writerAccessDenied=0,cycleErrors=0;string stage="initial";
        string Hash(byte[] bytes)=>Convert.ToHexStringLower(SHA256.HashData(bytes));
        void Log(object value){lock(logGate)File.AppendAllText(Path.Combine(output,"events.jsonl"),JsonSerializer.Serialize(value)+"\n");}
        void Finding(string message){findings.Add(message);Log(new{at=DateTimeOffset.UtcNow,stage,failure=message});}
        byte[] Bytes(string name,int revision,int size=2048){var head=Encoding.UTF8.GetBytes($"{name}\nrevision={revision:D8}\n");var bytes=new byte[Math.Max(size,head.Length)];for(var i=0;i<bytes.Length;i++)bytes[i]=(byte)('a'+revision%26);head.CopyTo(bytes,0);return bytes;}
        var hot=new[]{"example.md","Агентность, локус контроля, инициативность.md","notes/live-0.md","notes/live-1.md","notes/live-2.md","attachment.bin"};
        var names=hot.Concat(new[]{"example (2).md","empty.bin","same-size.md"}).Concat(Enumerable.Range(0,23).Select(i=>$"cold/раздел-{i%3}/file-{i:D2}.md")).ToArray();
        foreach(var name in names){var file=Path.Combine(source,name);Directory.CreateDirectory(Path.GetDirectoryName(file)!);var bytes=name=="empty.bin"?[]:Bytes(name,0,name=="attachment.bin"?1152*1024:2048);await File.WriteAllBytesAsync(file,bytes,ct);expected[name]=Hash(bytes);}
        var preservedCopy=expected["example (2).md"];
        await File.WriteAllTextAsync(Path.Combine(output,"identity.json"),JsonSerializer.Serialize(new{at=DateTimeOffset.UtcNow,source,profile=profile.StateDirectory,guarded,files=names.Length}),ct);
        using var watcher=new FileSystemWatcher(source){IncludeSubdirectories=true,NotifyFilter=NotifyFilters.FileName|NotifyFilters.DirectoryName|NotifyFilters.LastWrite|NotifyFilters.Size};
        void Observe(string path,string kind){var relative=Path.GetRelativePath(source,path).Replace('\\','/');if(events.Count<20000)events.Enqueue(new{at=DateTimeOffset.UtcNow,kind,path=relative});if(kind=="created"&&File.Exists(path)&&!expected.ContainsKey(relative))unknownPaths.TryAdd(relative,0);}
        watcher.Created+=(_,e)=>Observe(e.FullPath,"created");watcher.Renamed+=(_,e)=>Observe(e.FullPath,"renamed");watcher.Changed+=(_,e)=>Observe(e.FullPath,"changed");watcher.Deleted+=(_,e)=>Observe(e.FullPath,"deleted");watcher.Error+=(_,e)=>Log(new{at=DateTimeOffset.UtcNow,watcherError=e.GetException().Message});watcher.EnableRaisingEvents=true;
        using var config=JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(root,guarded?"clients/clients.private.json":"private/legacy.json"),ct));
        var c=guarded?config.RootElement[0]:config.RootElement;
        var origin=new Uri(c.GetProperty("origin").GetString()!);
        using var http=guarded?SyncHttpPolicy.CreateClient():new HttpClient(new FixtureTransport(origin)){Timeout=TimeSpan.FromSeconds(90)};
        using var oracle=SyncHttpPolicy.CreateClient();
        WindowsGuardedCapability? capability=null;object? prepared=null;MethodInfo? method=null;WindowsConnection? connection=null;
        if(guarded){
            var token=c.GetProperty("authorization").GetString()![7..];var device=c.GetProperty("deviceId").GetString()!;var rootId=c.GetProperty("binding").GetProperty("root_id").GetString()!;
            using var req=new HttpRequestMessage(HttpMethod.Get,new Uri(origin,"sync/v1/roots/"+rootId+"/capabilities"));req.Headers.TryAddWithoutValidation("Authorization","Bearer "+token);
            using var res=await http.SendAsync(req,ct);res.EnsureSuccessStatusCode();capability=WindowsGuardedCapabilities.Parse(await res.Content.ReadAsByteArrayAsync(ct),device,rootId);
            // This isolated fixture has <4MiB of source data and retains a
            // 2GiB free-space reserve; production defaults remain unchanged.
            capability=capability with{Budget=capability.Budget with{MinimumFreeDiskBytes=2L*1024*1024*1024}};
            prepared=Activator.CreateInstance(typeof(WindowsSyncService).GetNestedType("PreparedConnection",BindingFlags.NonPublic)!,[new Uri(origin,"dav/sync/"),SaturnPalette.Default,token,c.GetProperty("rootName").GetString()!,origin,device,rootId]);
            method=typeof(WindowsSyncService).GetMethod("SyncGuardedAsync",BindingFlags.NonPublic|BindingFlags.Instance)!;
        }else connection=new(new Uri("https://editing-kernel.invalid"),"owned-fixture",c.GetProperty("token").GetString()!,c.GetProperty("folder").GetString()!,WindowsKernelCredentialKind.NeptuneOutdoorToken);
        WindowsSyncService service=new(profile,http);
        async Task Cycle(){
            cycles++;var start=DateTimeOffset.UtcNow;
            try{
                WindowsSyncResult result;
                var progress=new CallbackProgress(value=>Log(new{at=DateTimeOffset.UtcNow,stage,progress=value}));
                if(guarded){var gate=(SemaphoreSlim)typeof(WindowsSyncService).GetField("_syncGate",BindingFlags.NonPublic|BindingFlags.Instance)!.GetValue(service)!;await gate.WaitAsync(ct);try{result=await (Task<WindowsSyncResult>)method!.Invoke(service,["editing-client",new[]{mapping},prepared,capability,progress,ct])!;}finally{gate.Release();}}
                else result=await service.SyncAsync("editing-client",[mapping],connection!,progress,ct);
                deferred+=result.DeferredFiles;uploads+=result.UploadedFiles;Log(new{at=DateTimeOffset.UtcNow,stage,cycle=cycles,result.UploadedFiles,result.DeferredFiles,elapsedMs=(DateTimeOffset.UtcNow-start).TotalMilliseconds});
            }catch(Exception error)when(!ct.IsCancellationRequested){cycleErrors++;Log(new{at=DateTimeOffset.UtcNow,stage,cycle=cycles,error=error.ToString()});}
            await File.WriteAllTextAsync(Path.Combine(output,"progress.json"),JsonSerializer.Serialize(new{at=DateTimeOffset.UtcNow,stage,cycles,edits,deferred,uploads,writerRetries,cycleErrors}),ct);
        }
        async Task<JsonElement> Remote(){
            using var request=new HttpRequestMessage(HttpMethod.Post,guarded?new Uri(origin,"__completion/readback"):new Uri(c.GetProperty("oracleOrigin").GetString()!+"/readback"));
            if(!guarded)request.Headers.TryAddWithoutValidation("Authorization","Bearer "+c.GetProperty("oracleKey").GetString());
            request.Content=JsonContent.Create(new{kind="windows",folder=Path.GetFileName(source)});
            using var res=await oracle.SendAsync(request,ct);res.EnsureSuccessStatusCode();using var doc=JsonDocument.Parse(await res.Content.ReadAsByteArrayAsync(ct));return doc.RootElement.Clone();
        }
        bool Matches(JsonElement snapshot){var files=snapshot.GetProperty("files").EnumerateArray().ToArray();return files.Length==expected.Count&&files.Select(f=>f.GetProperty("path").GetString()).Distinct().Count()==expected.Count&&files.All(f=>expected.TryGetValue(f.GetProperty("path").GetString()!,out var hash)&&hash==f.GetProperty("sha256").GetString()&&hash==f.GetProperty("catalogSha256").GetString());}
        void Local(){var files=Directory.GetFiles(source,"*",SearchOption.AllDirectories).ToDictionary(f=>Path.GetRelativePath(source,f).Replace('\\','/'),f=>Hash(File.ReadAllBytes(f)));if(files.Count!=expected.Count||files.Any(p=>!expected.TryGetValue(p.Key,out var h)||p.Value!=h))Finding("Local names or final bytes differ from editor manifest");if(expected["example (2).md"]!=preservedCopy)Finding("Pre-existing indexed copy changed");}
        async Task<JsonElement> Converge(string name,int limit=80){stage=name;JsonElement snapshot=default;for(var i=0;i<limit;i++){await Cycle();snapshot=await Remote();if(Matches(snapshot))break;await Task.Delay(500,ct);}await File.WriteAllTextAsync(Path.Combine(output,name+"-remote.json"),snapshot.GetRawText(),ct);if(!Matches(snapshot))Finding("Remote did not converge to final editor bytes: "+name);Local();return snapshot;}
        async Task Edit(string name,int revision,string mode){
            var file=Path.Combine(source,name);var bytes=Bytes(name,revision,name=="attachment.bin"?1152*1024:2048+revision%17);
            var temporary=Path.Combine(output,"editor-"+Guid.NewGuid().ToString("N")+".tmp");
            for(var attemptNo=0;;attemptNo++){
                try{
                    if(mode=="atomic"){await File.WriteAllBytesAsync(temporary,bytes,ct);File.Move(temporary,file,true);}
                    else {await using var writer=new FileStream(file,FileMode.Create,FileAccess.Write,mode=="lock"?FileShare.None:FileShare.ReadWrite|FileShare.Delete,65536,true);var half=bytes.Length/2;await writer.WriteAsync(bytes.AsMemory(0,half),ct);await writer.FlushAsync(ct);await Task.Delay(mode=="lock"?140:20,ct);await writer.WriteAsync(bytes.AsMemory(half),ct);await writer.FlushAsync(ct);}
                    expected[name]=Hash(bytes);Interlocked.Increment(ref edits);Log(new{at=DateTimeOffset.UtcNow,editor=true,name,revision,mode,sha256=expected[name],size=bytes.Length});return;
                }catch(IOException error)when((error.HResult&0xffff)is 32 or 33 && attemptNo<200){Interlocked.Increment(ref writerRetries);await Task.Delay(20,ct);}
                catch(UnauthorizedAccessException error)when(mode=="atomic"&&attemptNo<200){Interlocked.Increment(ref writerAccessDenied);Log(new{at=DateTimeOffset.UtcNow,stage,editorAccessDenied=true,name,error.HResult,attemptNo});await Task.Delay(20,ct);}
                finally{if(File.Exists(temporary))File.Delete(temporary);}
            }
        }
        async Task Churn(string label,string mode,int count,bool restart){
            stage=label;
            var editor=Task.Run(async()=>{for(var i=0;i<count;i++){await Edit(hot[i%hot.Length],1000*count+i,mode);await Task.Delay(45,ct);}},ct);
            var rounds=0;
            while(!editor.IsCompleted){await Cycle();if(restart&&++rounds==1){await service.DisposeAsync();service=new(profile,http);Log(new{at=DateTimeOffset.UtcNow,stage,restartedService=true});}foreach(var f in Directory.GetFiles(source,"*",SearchOption.AllDirectories)){var n=Path.GetRelativePath(source,f).Replace('\\','/');if(!expected.ContainsKey(n))unknownPaths.TryAdd(n,0);}await Task.Delay(50,ct);}
            await editor;await Converge(label+"-settled");
        }
        try{
            var initial=await Converge("initial");if(!Matches(initial))throw new InvalidOperationException("Initial fixture failed");
            var originalIds=initial.GetProperty("files").EnumerateArray().ToDictionary(f=>f.GetProperty("path").GetString()!,f=>f.GetProperty("id").GetString());
            await Churn(mode=="mixed"?"atomic":"in-place", mode=="mixed"?"atomic":"partial",120,false);
            await Churn("partial-writes", "partial",90,false);
            await Churn("exclusive-locks", "lock",36,false);
            if(mode=="mixed"){
            stage="gap-across-scan";var beforeGap=await Remote();var gapPath=Path.Combine(source,hot[1]);var retained=Path.Combine(output,"editor-gap-retained.md");File.Move(gapPath,retained);var gapAt=DateTimeOffset.UtcNow;
            try{await Cycle();var missing=await Remote();await File.WriteAllTextAsync(Path.Combine(output,"gap-remote.json"),missing.GetRawText(),ct);var oldFile=beforeGap.GetProperty("files").EnumerateArray().Single(f=>f.GetProperty("path").GetString()==hot[1]);var newFile=missing.GetProperty("files").EnumerateArray().SingleOrDefault(f=>f.GetProperty("path").GetString()==hot[1]);if(newFile.ValueKind==JsonValueKind.Undefined||oldFile.GetRawText()!=newFile.GetRawText())Finding("Temporary missing file was removed/changed remotely during one scan");}
            finally{File.Move(retained,gapPath);Log(new{at=DateTimeOffset.UtcNow,stage,gapMilliseconds=(DateTimeOffset.UtcNow-gapAt).TotalMilliseconds});}
            var restored=await Converge("gap-restored");
            foreach(var f in restored.GetProperty("files").EnumerateArray())if(originalIds[f.GetProperty("path").GetString()!]!=f.GetProperty("id").GetString())Finding("Stable target identity changed: "+f.GetProperty("path").GetString());
            }
            stage="same-size-mtime";var same=Path.Combine(source,"same-size.md");var oldTime=File.GetLastWriteTimeUtc(same);var changed=await File.ReadAllBytesAsync(same,ct);changed[^1]^=1;await File.WriteAllBytesAsync(same,changed,ct);File.SetLastWriteTimeUtc(same,oldTime);expected["same-size.md"]=Hash(changed);edits++;
            await Converge("same-size-mtime",8);
            await Churn("service-restart", mode=="mixed"?"atomic":"partial",90,true);
            if(!guarded){
                stage="permanent-delete";var name=names[^1];File.Delete(Path.Combine(source,name));
                await Cycle();var retainedRemote=await Remote();
                if(!Matches(retainedRemote))Finding("First permanent deletion observation was not deferred");
                var pending=await state.GetSyncFileAsync(mapping.MappingId,name,ct);
                if(pending?.State!="missing-local")Finding("Missing observation was not durable");
                await service.DisposeAsync();service=new(profile,http);
                await Cycle();var restartPending=await state.GetSyncFileAsync(mapping.MappingId,name,ct);
                if(restartPending?.UpdatedAt!=pending?.UpdatedAt)Finding("Restart lost the original missing observation");
                Log(new{at=DateTimeOffset.UtcNow,stage,waitingForDeletionGrace=true});
                await Task.Delay(WindowsSyncService.LegacyMissingFileGrace+TimeSpan.FromSeconds(1),ct);
                expected.TryRemove(name,out _);await Converge("permanent-delete-confirmed");
            }
            var final=await Converge("final");stage="idle";for(var n=0;n<3;n++)await Cycle();var idle=await Remote();await File.WriteAllTextAsync(Path.Combine(output,"idle-remote.json"),idle.GetRawText(),ct);
            if(final.GetRawText()!=idle.GetRawText())Finding("Idle sync changed versions/IDs/bytes");Local();
            if(unknownPaths.Count>0)Finding("Unexpected local names: "+string.Join(", ",unknownPaths.Keys));
            if(cycleErrors>0)Finding("Whole sync cycles raised errors during editing: "+cycleErrors);
        }
        catch(Exception error){Finding("Test interrupted: "+error);throw;}
        finally{
            await service.DisposeAsync();watcher.EnableRaisingEvents=false;
            await File.WriteAllTextAsync(Path.Combine(output,"editor-expected.json"),JsonSerializer.Serialize(expected),CancellationToken.None);
            await File.WriteAllTextAsync(Path.Combine(output,"watcher.json"),JsonSerializer.Serialize(events),CancellationToken.None);
            await File.WriteAllTextAsync(Path.Combine(output,"result.json"),JsonSerializer.Serialize(new{at=DateTimeOffset.UtcNow,status=findings.Count==0&&stage=="idle"?"PASS":"FAIL",guarded,mode,source,files=names.Length,finalFiles=expected.Count,edits,cycles,deferred,uploads,writerRetries,writerAccessDenied,cycleErrors,unexpectedLocalNames=unknownPaths.Keys,findings}),CancellationToken.None);
        }
        Assert.Empty(findings);
    }
    private sealed class CallbackProgress(Action<string> action):IProgress<string>{public void Report(string value)=>action(value);}
    private sealed class FixtureTransport(Uri origin):DelegatingHandler(new SocketsHttpHandler{AllowAutoRedirect=false})
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken ct){
            if(request.RequestUri!.Host=="editing-kernel.invalid")return new(HttpStatusCode.OK){Content=JsonContent.Create(new{schema="exocortex.register.resolution.v1",register_revision="editing",values=new Dictionary<string,object>{["services.saturn.sni"]=new{value="editing-saturn.invalid",secret=false},["services.saturn.port"]=new{value="443",secret=false},["services.saturn.paths.sync"]=new{value="/dav/sync/",secret=false},["services.saturn.paths.sync_preferences"]=new{value="/api/v1/sync/preferences",secret=false}}})};
            if(request.RequestUri.Host!="editing-saturn.invalid")throw new InvalidDataException("Unexpected fixture destination");
            using var forwarded=new HttpRequestMessage(request.Method,new Uri(origin,request.RequestUri.PathAndQuery)){Content=request.Content};foreach(var header in request.Headers)forwarded.Headers.TryAddWithoutValidation(header.Key,header.Value);
            var response=await base.SendAsync(forwarded,ct);response.RequestMessage=request;return response;
        }
    }
}
