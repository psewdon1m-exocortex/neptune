using System.Diagnostics;
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
public sealed class SmallFileThroughputLiveTests
{
    // Opt-in, owned NTFS volume only. Aggregate observations never establish integrity.
    [Fact]
    public async Task NativeSmallFilesRetainIntegrityAndOriginalVersionsAcrossRestart()
    {
        var output=Environment.GetEnvironmentVariable("SMALL_FILE_NATIVE_EVIDENCE")??throw new InvalidOperationException("Owned receiver required");
        var volumeFile=Environment.GetEnvironmentVariable("SMALL_FILE_NATIVE_VOLUME")??throw new InvalidOperationException("Owned NTFS manifest required");
        using var deadline=CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);deadline.CancelAfter(TimeSpan.FromMinutes(15));var ct=deadline.Token;
        var count=Math.Clamp(int.Parse(Environment.GetEnvironmentVariable("SMALL_FILE_NATIVE_COUNT")??"128"),16,4096);
        var resume=Environment.GetEnvironmentVariable("SMALL_FILE_NATIVE_PHASE")=="resume";
        using var volume=JsonDocument.Parse(await File.ReadAllBytesAsync(volumeFile,ct));
        var nativeRoot=volume.RootElement.GetProperty("nativePath").GetString()!;
        var volumeId=volume.RootElement.GetProperty("volumeUniqueId").GetString()!.Substring(11,36);
        Assert.Equal("ready",volume.RootElement.GetProperty("state").GetString());
        Assert.NotEqual(Path.GetPathRoot(output),Path.GetPathRoot(nativeRoot));
        var casePath=Path.Combine(output,"native-case.json");
        using var previous=resume?JsonDocument.Parse(await File.ReadAllBytesAsync(casePath,ct)):null;
        var directory=resume?previous!.RootElement.GetProperty("directory").GetString()!:Path.Combine(nativeRoot,"smallfile-"+Guid.NewGuid().ToString("N"));
        Assert.StartsWith(nativeRoot+"smallfile-",directory);
        var source=Path.Combine(directory,"source");Directory.CreateDirectory(source);
        await using(var ns=await WindowsLocalNamespaceSource.OpenAsync(source,ct)){Assert.Equal(volumeId,ns.RootProof.VolumeGuid);Assert.Equal("NTFS",ns.RootProof.FileSystem);}
        var expected=new SortedDictionary<string,string>(StringComparer.Ordinal);
        const int size=48*1024;
        async Task Write(int index){var name=$"file-{index:D6}.bin";var bytes=new byte[size];new Random(index+17).NextBytes(bytes);await File.WriteAllBytesAsync(Path.Combine(source,name),bytes,ct);expected.Add(name,Convert.ToHexStringLower(SHA256.HashData(bytes)));}
        if(resume){foreach(var entry in previous!.RootElement.GetProperty("expected").EnumerateObject())expected.Add(entry.Name,entry.Value.GetString()!);}
        else for(var i=0;i<count;i++)await Write(i);
        var profile=new WindowsProfileContext("smallfile-native",Path.Combine(directory,"profile"),false);Directory.CreateDirectory(profile.StateDirectory);
        var smallBatches=Environment.GetEnvironmentVariable("SMALL_FILE_NATIVE_BATCHES")!="0";
        new WindowsPreferencesStore(profile.StateDirectory).Write(WindowsPreferences.Default with{SmallBatchMappings=smallBatches?["smallfile-mapping"]:[]});
        async Task SaveCase()=>await File.WriteAllTextAsync(casePath,JsonSerializer.Serialize(new{directory,source,profile=profile.StateDirectory,volumeId,count,size,expected}),ct);
        if(!resume)await SaveCase();
        using var config=JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(output,"client.private.json"),ct));var c=config.RootElement;
        var origin=new Uri(c.GetProperty("origin").GetString()!);var token=c.GetProperty("authorization").GetString()![7..];var device=c.GetProperty("deviceId").GetString()!;var root=c.GetProperty("binding").GetProperty("root_id").GetString()!;
        using var http=SyncHttpPolicy.CreateClient();
        using var request=new HttpRequestMessage(HttpMethod.Get,new Uri(origin,"sync/v1/roots/"+root+"/capabilities"));request.Headers.TryAddWithoutValidation("Authorization","Bearer "+token);
        using var response=await http.SendAsync(request,ct);response.EnsureSuccessStatusCode();
        var capability=WindowsGuardedCapabilities.Parse(await response.Content.ReadAsByteArrayAsync(ct),device,root);
        // The dedicated 8 GiB scratch volume cannot satisfy the desktop 40 GiB reserve.
        // Keep all negotiated concurrency/transfer/spool limits; use a 1 GiB scratch reserve.
        capability=capability with{Budget=capability.Budget with{MinimumFreeDiskBytes=1024L*1024*1024}};
        var mapping=new SyncMapping("smallfile-mapping","smallfile-client",source,true,DateTimeOffset.UtcNow);
        var prepared=Activator.CreateInstance(typeof(WindowsSyncService).GetNestedType("PreparedConnection",BindingFlags.NonPublic)!,new object[]{new Uri(origin,"webdav/sync/"),SaturnPalette.Default,token,c.GetProperty("rootName").GetString()!,origin,device,root})!;
        var method=typeof(WindowsSyncService).GetMethod("SyncGuardedAsync",BindingFlags.Instance|BindingFlags.NonPublic)!;
        var progress=new SampledProgress(Path.Combine(output,"client-progress.json"));
        double clientMs=0,observationMs=0,physicalReadbackMs=0,schedulerMs=0;var cycles=0;var physicalChecks=0;var lastProgress=0;var lastGrowth=Stopwatch.StartNew();
        async Task<WindowsSyncResult> Run(WindowsSyncService service)
        {
            var gate=(SemaphoreSlim)typeof(WindowsSyncService).GetField("_syncGate",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(service)!;
            await gate.WaitAsync(ct);var watch=Stopwatch.StartNew();
            try{return await (Task<WindowsSyncResult>)method.Invoke(service,["smallfile-client",new[]{mapping},prepared,capability,progress,ct])!;}
            finally{clientMs+=watch.Elapsed.TotalMilliseconds;cycles++;gate.Release();}
        }
        async Task<JsonElement> Observe(string endpoint)
        {
            var watch=Stopwatch.StartNew();
            try{using var r=await http.GetAsync(new Uri(origin,"__smallfile/"+endpoint),ct);r.EnsureSuccessStatusCode();using var d=JsonDocument.Parse(await r.Content.ReadAsByteArrayAsync(ct));return d.RootElement.Clone();}
            finally{if(endpoint=="readback")physicalReadbackMs+=watch.Elapsed.TotalMilliseconds;else observationMs+=watch.Elapsed.TotalMilliseconds;}
        }
        async Task Converge(WindowsSyncService service,string stage)
        {
            var sinceObservation=Stopwatch.StartNew();var observed=-1;
            while(observed!=expected.Count)
            {
                var result=await Run(service);
                if(sinceObservation.Elapsed>=TimeSpan.FromSeconds(2)||result.DeferredFiles==0)
                {
                    var state=await Observe("progress");observed=state.GetProperty("files").GetInt32();
                    if(observed>lastProgress){lastProgress=observed;lastGrowth.Restart();}
                    await File.AppendAllTextAsync(Path.Combine(output,"cycles.jsonl"),JsonSerializer.Serialize(new{at=DateTimeOffset.UtcNow,stage,cycles,observed,result.UploadedFiles,result.DeferredFiles,clientMs,observationMs,schedulerMs,diagnostics=await service.ReadDiagnosticsAsync(ct)})+"\n",ct);
                    Assert.InRange(observed,0,expected.Count);Assert.True(lastGrowth.Elapsed<TimeSpan.FromMinutes(2),"No increase in independently published files for 2 minutes");sinceObservation.Restart();
                }
                if(result.UploadedFiles==0&&observed!=expected.Count){var watch=Stopwatch.StartNew();await Task.Delay(50,ct);schedulerMs+=watch.Elapsed.TotalMilliseconds;}
            }
        }
        async Task<JsonElement> Verify()
        {
            var state=await Observe("readback");var files=state.GetProperty("files").EnumerateArray().ToArray();
            Assert.Equal(expected.Count,files.Length);var names=new HashSet<string>();var ids=new HashSet<string>();var versions=new HashSet<string>();
            foreach(var f in files)
            {
                var remote=f.GetProperty("path").GetString()!;Assert.StartsWith("source/",remote);var name=remote[7..];Assert.True(names.Add(name));Assert.True(expected.TryGetValue(name,out var hash));
                Assert.Equal(hash,f.GetProperty("sha256").GetString());Assert.Equal(hash,f.GetProperty("catalogSha256").GetString());Assert.Equal(size,f.GetProperty("size").GetInt64());Assert.Equal(size.ToString(),f.GetProperty("catalogSize").GetString());
                Assert.Equal(1,f.GetProperty("versionCount").GetInt32());Assert.True(ids.Add(f.GetProperty("id").GetString()!));Assert.True(versions.Add(f.GetProperty("versionId").GetString()!));
            }
            var local=Directory.GetFiles(source,"*",SearchOption.AllDirectories);Assert.Equal(expected.Count,local.Length);
            foreach(var file in local)Assert.Equal(expected[Path.GetRelativePath(source,file)],Convert.ToHexStringLower(SHA256.HashData(await File.ReadAllBytesAsync(file,ct))));
            physicalChecks++;await File.WriteAllTextAsync(Path.Combine(output,$"{(resume?"resume-":"")}readback-{physicalChecks}.json"),state.GetRawText(),ct);return state;
        }
        var elapsed=Stopwatch.StartNew();
        var verifiedElapsed=Stopwatch.StartNew();
        JsonElement initial;
        await using(var service=new WindowsSyncService(profile)){await Converge(service,resume?"process-resume":"initial");elapsed.Stop();initial=await Verify();}
        verifiedElapsed.Stop();
        var initialPhases=new{clientMs,observationMs,schedulerMs,cycles};
        if(resume)
        {
            using var before=JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(output,"fault-originals.json"),ct));
            var after=await Observe("originals");
            var originals=after.GetProperty("originals").EnumerateArray().ToDictionary(x=>x.GetProperty("id").GetString()!);
            foreach(var original in before.RootElement.GetProperty("originals").EnumerateArray())Assert.Equal(original.GetRawText(),originals[original.GetProperty("id").GetString()!].GetRawText());
            using var fault=JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(output,"fault-state.json"),ct));Assert.NotEqual(Environment.ProcessId,fault.RootElement.GetProperty("processId").GetInt32());
            await File.WriteAllTextAsync(Path.Combine(output,"resume-result.json"),JsonSerializer.Serialize(new{status="PASS",count=expected.Count,physicalChecks,elapsedMs=elapsed.Elapsed.TotalMilliseconds,initialPhases,actualProcessRestart=true,originalsUnchanged=true,lostResponseAfterCommit=initial.GetProperty("lostResponses").GetInt32(),processId=Environment.ProcessId}),ct);
            return;
        }
        await using(var service=new WindowsSyncService(profile))
        {await Run(service);await Run(service);var replay=await Verify();Assert.Equal(initial.GetProperty("files").GetRawText(),replay.GetProperty("files").GetRawText());}
        await File.WriteAllTextAsync(Path.Combine(output,"native-result.json"),JsonSerializer.Serialize(new{count,size,smallBatches,elapsedMs=elapsed.Elapsed.TotalMilliseconds,filesPerSecond=count/elapsed.Elapsed.TotalSeconds,verifiedTotalMs=verifiedElapsed.Elapsed.TotalMilliseconds,verifiedTotalFilesPerSecond=count/verifiedElapsed.Elapsed.TotalSeconds,initialPhases,totalPhases=new{clientMs,observationMs,physicalReadbackMs,schedulerMs,cycles},physicalChecks,fullPhysicalSha=true,noExtraLocalFiles=true,newServiceReplayUnchangedVersions=true,actualProcessRestart=false,targetFilesPerSecond=50,targetAchieved=count/elapsed.Elapsed.TotalSeconds>=50}),ct);
        if(Environment.GetEnvironmentVariable("SMALL_FILE_NATIVE_FAULT")=="1")
        {
            for(var i=count;i<count+16;i++)await Write(i);await SaveCase();
            using(var armed=await http.PostAsJsonAsync(new Uri(origin,"__smallfile/lose-response"),new{},ct))armed.EnsureSuccessStatusCode();
            string? failure=null;
            await using(var service=new WindowsSyncService(profile))
            {
                for(var attempt=0;attempt<8;attempt++)
                {
                    try{await Run(service);}catch(Exception e)when(!ct.IsCancellationRequested&&NetworkFailure.State(e)=="Waiting for network"){failure=e.GetType().Name;}
                    var state=await Observe("progress");if(state.GetProperty("lostResponses").GetInt32()==1)break;
                }
            }
            var fault=await Observe("progress");Assert.Equal(1,fault.GetProperty("lostResponses").GetInt32());
            var originals=await Observe("originals");await File.WriteAllTextAsync(Path.Combine(output,"fault-originals.json"),originals.GetRawText(),ct);
            await File.WriteAllTextAsync(Path.Combine(output,"fault-state.json"),JsonSerializer.Serialize(new{processId=Environment.ProcessId,failure,expected=expected.Count,published=fault.GetProperty("files").GetInt32(),lostResponses=1}),ct);
        }
        Assert.True(count/elapsed.Elapsed.TotalSeconds>=50,$"Native throughput {count/elapsed.Elapsed.TotalSeconds:F3} files/sec is below 50");
    }
    private sealed class SampledProgress(string path):IProgress<string>
    {
        private readonly object _gate=new();private DateTimeOffset _last;
        public void Report(string value){lock(_gate){var now=DateTimeOffset.UtcNow;if(now-_last<TimeSpan.FromSeconds(1))return;_last=now;File.WriteAllText(path,JsonSerializer.Serialize(new{at=now,progress=value}));}}
    }
}
