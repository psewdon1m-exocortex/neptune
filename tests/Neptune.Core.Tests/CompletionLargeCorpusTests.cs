using System.Diagnostics;
using System.Net.Http.Json;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using Neptune.Core;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

// Explicit live campaign only. Source, state and API are owned disposable fixtures.
[Collection("Native fresh cycle")]
[Trait("Lane", "Live")]
public sealed class CompletionLargeCorpusTests
{
    private sealed record Input(int index, string path, long size, string sha256);
    [Fact]
    public async Task ActualLargeWindowsSourceConvergesWithoutCopies()
    {
        var ct = TestContext.Current.CancellationToken;
        var campaign = Environment.GetEnvironmentVariable("ENDURANCE_LARGE_ROOT") ?? throw new InvalidOperationException("Owned campaign required");
        Assert.StartsWith("C:/.projects/exocortex/.audit/2026-10-18/endurance-6h", campaign.Replace('\\','/'), StringComparison.OrdinalIgnoreCase);
        var evidence = Environment.GetEnvironmentVariable("COMPLETION_EVIDENCE")!;
        var target = int.Parse(Environment.GetEnvironmentVariable("ENDURANCE_LARGE_FILES")!);
        Assert.InRange(target, 8, 500000);
        var stage = Environment.GetEnvironmentVariable("ENDURANCE_LARGE_STAGE") ?? "initial";
        var observationMode = Environment.GetEnvironmentVariable("ENDURANCE_LARGE_OBSERVATION") ?? "full-per-cycle";
        Assert.Contains(observationMode, new[] { "full-per-cycle", "aggregate-v1" });
        var aggregateObservation = observationMode == "aggregate-v1";
        var physicalTimeoutSeconds = int.Parse(Environment.GetEnvironmentVariable("ENDURANCE_LARGE_READBACK_TIMEOUT_SECONDS") ?? "7200");
        Assert.InRange(physicalTimeoutSeconds, 60, 43200);
        var phaseMs = new Dictionary<string, double>(StringComparer.Ordinal);
        async Task<T> Measure<T>(string phase, Func<Task<T>> action)
        {
            var started = Stopwatch.GetTimestamp();
            try { return await action(); }
            finally { phaseMs[phase] = phaseMs.GetValueOrDefault(phase) + Stopwatch.GetElapsedTime(started).TotalMilliseconds; }
        }
        async Task Wait(int milliseconds)
        {
            await Measure("schedulerWait", async () => { await Task.Delay(milliseconds, ct); return true; });
        }
        var source = Path.Combine(campaign, "corpus", "active-vault"); Directory.CreateDirectory(source);
        var manifest = Path.Combine(campaign,"corpus",$"manifest-{target}.jsonl");
        var expected = new Dictionary<string,Input>(StringComparer.Ordinal);
        await foreach(var line in File.ReadLinesAsync(manifest,ct)) { var row=JsonSerializer.Deserialize<Input>(line)!;expected.Add(row.path,row); }
        Assert.Equal(target,expected.Count);
        var expectedBytes=expected.Values.Sum(f=>f.size);
        var progressPath=Path.Combine(evidence,"large-progress.json");
        async Task Evidence(string name,object value)=>await File.WriteAllTextAsync(Path.Combine(evidence,name+".json"),JsonSerializer.Serialize(value),ct);
        var timer=Stopwatch.StartNew(); var admitted=0;
        foreach(var row in expected.Values)
        {
            var dest=Path.Combine(source,row.path.Replace('/',Path.DirectorySeparatorChar));
            if(!File.Exists(dest)){Directory.CreateDirectory(Path.GetDirectoryName(dest)!);File.Move(Path.Combine(campaign,"corpus","prepared",row.path.Replace('/',Path.DirectorySeparatorChar)),dest);}
            if(++admitted%10000==0)await Evidence("large-progress",new{at=DateTimeOffset.UtcNow,stage="admitting",admitted,target});
        }
        Directory.CreateDirectory(Path.Combine(source,"empty/deep"));
        using var doc=JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(evidence,"clients.private.json"),ct));var c=doc.RootElement[0];
        var origin=new Uri(c.GetProperty("origin").GetString()!);var oracleOrigin=c.TryGetProperty("oracleOrigin",out var oracle)?new Uri(oracle.GetString()!):origin;var token=c.GetProperty("authorization").GetString()![7..];var device=c.GetProperty("deviceId").GetString()!;var root=c.GetProperty("binding").GetProperty("root_id").GetString()!;
        using var http=SyncHttpPolicy.CreateClient();using var req=new HttpRequestMessage(HttpMethod.Get,new Uri(origin,"sync/v1/roots/"+root+"/capabilities"));req.Headers.TryAddWithoutValidation("Authorization","Bearer "+token);
        using var response=await http.SendAsync(req,ct);response.EnsureSuccessStatusCode();var capability=WindowsGuardedCapabilities.Parse(await response.Content.ReadAsByteArrayAsync(ct),device,root);
        var profile=new WindowsProfileContext("endurance-large",Path.Combine(campaign,"large-state"),false);Directory.CreateDirectory(profile.StateDirectory);
        var mapping=new SyncMapping("large-mapping","large-client",source,true,DateTimeOffset.UtcNow);
        var prepared=Activator.CreateInstance(typeof(WindowsSyncService).GetNestedType("PreparedConnection",BindingFlags.NonPublic)!,new object[]{new Uri(origin,"webdav/sync/"),SaturnPalette.Default,token,c.GetProperty("rootName").GetString()!,origin,device,root})!;
        var method=typeof(WindowsSyncService).GetMethod("SyncGuardedAsync",BindingFlags.Instance|BindingFlags.NonPublic)!;
        var progress=new ProgressSink(progressPath);long cycles=0;
        async Task<JsonElement> Remote(bool metadataOnly)
        {
            if(metadataOnly)return await CompletionOracleReadback.ReadMetadataAsync(http,oracleOrigin,"windows",Path.GetFileName(source),ct,
                onRetry:async(attempt,reason)=>await File.AppendAllTextAsync(Path.Combine(evidence,"large-oracle-retries.jsonl"),JsonSerializer.Serialize(new{at=DateTimeOffset.UtcNow,attempt,reason})+"\n",ct));
            if(aggregateObservation)return await CompletionOracleReadback.ReadPhysicalAsync(http,oracleOrigin,"windows",Path.GetFileName(source),ct,TimeSpan.FromSeconds(physicalTimeoutSeconds));
            using var r=await http.PostAsJsonAsync(new Uri(oracleOrigin,"__completion/readback"),new{kind="windows",folder=Path.GetFileName(source),metadataOnly},ct);r.EnsureSuccessStatusCode();
            using var result=JsonDocument.Parse(await r.Content.ReadAsStreamAsync(ct));return result.RootElement.Clone();
        }
        bool Matches(JsonElement snapshot,bool metadata)
        {
            var files=snapshot.GetProperty("files");if(files.GetArrayLength()!=target)return false;
            foreach(var f in files.EnumerateArray())if(!expected.TryGetValue(f.GetProperty("path").GetString()!,out var row)||f.GetProperty("catalogSha256").GetString()!=row.sha256||f.GetProperty("catalogSize").GetString()!=row.size.ToString()||(!metadata&&(f.GetProperty("sha256").GetString()!=row.sha256||f.GetProperty("size").GetInt64()!=row.size)))return false;
            return true;
        }
        async Task<JsonElement> Metadata(string phase)
        {
            await Evidence("large-oracle-phase",new{at=DateTimeOffset.UtcNow,phase,state="started",observationMode,attemptTimeoutSeconds=120});
            var snapshot=await Measure(phase,()=>Remote(true));
            await Evidence("large-oracle-phase",new{at=DateTimeOffset.UtcNow,phase,state="finished",observationMode});
            return snapshot;
        }
        var strictOracle = aggregateObservation?new CompletionCorpusOracle(expected.ToDictionary(p=>p.Key,p=>new CompletionExpectedFile(p.Value.size,p.Value.sha256),StringComparer.Ordinal)):null;
        var before=await Metadata("baselineMetadata");long beforeBytes=before.GetProperty("files").EnumerateArray().Sum(f=>long.Parse(f.GetProperty("catalogSize").GetString()!));
        if(aggregateObservation)strictOracle!.Observe(before,false);
        var schedule=new CompletionObservationSchedule(before.GetProperty("files").GetArrayLength());
        var syncStart=Stopwatch.StartNew();JsonElement initial=default;
        await using(var service=new WindowsSyncService(profile))
        {
            var lastCount=-1;var lastGrowth=Stopwatch.StartNew();
            for(;;)
            {
                WindowsSyncResult? cycleResult=null;
                var gate=(SemaphoreSlim)typeof(WindowsSyncService).GetField("_syncGate",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(service)!;await gate.WaitAsync(ct);
                try{cycleResult=await Measure("clientActive",async()=>await (Task<WindowsSyncResult>)method.Invoke(service,["large-client",new[]{mapping},prepared,capability,progress,ct])!);}
                catch(Exception error) when(!ct.IsCancellationRequested && NetworkFailure.State(error)=="Waiting for network") { await File.AppendAllTextAsync(Path.Combine(evidence,"large-network-retries.jsonl"),JsonSerializer.Serialize(new{at=DateTimeOffset.UtcNow,type=error.GetType().Name})+"\n",ct); }
                catch(IOException error) when(error.Message.EndsWith("GuardedCycleRemoteException: reconciliation_required",StringComparison.Ordinal))
                {
                    // Model the next scheduled service run when another owned writer
                    // temporarily closes root admission. Never clear UNKNOWN/state or
                    // claim success: the ordinary service must recover original work,
                    // and the unchanged no-growth watchdog still makes this finite.
                    await File.AppendAllTextAsync(Path.Combine(evidence,"large-admission-retries.jsonl"),JsonSerializer.Serialize(new{at=DateTimeOffset.UtcNow,error=error.Message,statePreserved=true})+"\n",ct);
                    await Wait(2000);
                }
                finally{gate.Release();}
                cycles++;
                if(aggregateObservation)
                {
                    var fullDue=schedule.FullDue(syncStart.Elapsed);
                    if(schedule.AggregateDue(syncStart.Elapsed))
                    {
                        await Evidence("large-oracle-phase",new{at=DateTimeOffset.UtcNow,phase="aggregateObservation",state="started",observationMode,attemptTimeoutSeconds=15});
                        var observed=await Measure("aggregateObservation",()=>CompletionOracleReadback.ReadProgressAsync(http,oracleOrigin,"windows",Path.GetFileName(source),root,ct));
                        if(observed.Files>target||observed.Bytes>expectedBytes)throw new InvalidDataException("Aggregate exceeds immutable corpus bounds");
                        schedule.Observed(observed.Files,syncStart.Elapsed);
                        await Evidence("large-checkpoint",new{at=DateTimeOffset.UtcNow,stage,target,published=observed.Files,bytes=observed.Bytes,cycles,elapsedMs=syncStart.ElapsedMilliseconds,observationMode,phaseMs});
                        await Evidence("large-oracle-phase",new{at=DateTimeOffset.UtcNow,phase="aggregateObservation",state="finished",observationMode});
                        fullDue|=observed.Files==target;
                    }
                    if(fullDue)
                    {
                        var snapshot=await Metadata("syncMetadata");
                        var complete=strictOracle!.Observe(snapshot,false);
                        schedule.Observed(snapshot.GetProperty("files").GetArrayLength(),syncStart.Elapsed);
                        schedule.FullObserved(syncStart.Elapsed);
                        if(complete){initial=snapshot;break;}
                    }
                    schedule.CheckGrowth(syncStart.Elapsed);
                    // Product receipts choose backoff only; independent oracle alone
                    // authorizes completion. Productive passes need no fixed sleep.
                    if(cycleResult is null||cycleResult.UploadedFiles==0)await Wait(500);
                }
                else
                {
                    var snapshot=await Metadata("syncMetadata");var published=snapshot.GetProperty("files").GetArrayLength();
                    await Evidence("large-checkpoint",new{at=DateTimeOffset.UtcNow,stage,target,published,cycles,elapsedMs=syncStart.ElapsedMilliseconds,observationMode,phaseMs});
                    if(published>lastCount){lastCount=published;lastGrowth.Restart();}
                    if(Matches(snapshot,true)){initial=snapshot;break;}
                    if(lastGrowth.Elapsed>TimeSpan.FromMinutes(15))throw new InvalidOperationException("No publication growth for 15 minutes; retain state and diagnose");
                    await Wait(500);
                }
            }
        }
        var syncMs=syncStart.ElapsedMilliseconds;var readback=Stopwatch.StartNew();
        await Evidence("large-oracle-phase",new{at=DateTimeOffset.UtcNow,phase="finalPhysicalReadback",state="started",observationMode,attemptTimeoutSeconds=aggregateObservation?(int?)physicalTimeoutSeconds:null});
        var full=await Measure("finalPhysicalReadback",()=>Remote(false));Assert.True(aggregateObservation?strictOracle!.Observe(full,true):Matches(full,false),"Independent backend bytes/catalog/source manifest mismatch");
        await Evidence("large-oracle-phase",new{at=DateTimeOffset.UtcNow,phase="finalPhysicalReadback",state="finished",observationMode});
        await Evidence("large-remote",full);
        var localVerification=Stopwatch.StartNew();
        var actualCount=0;long actualBytes=0;foreach(var file in Directory.EnumerateFiles(source,"*",SearchOption.AllDirectories))
        {
            var rel=Path.GetRelativePath(source,file).Replace('\\','/');Assert.True(expected.TryGetValue(rel,out var row),"Unexpected local file: "+rel);
            using var input=File.OpenRead(file);Assert.Equal(row!.sha256,Convert.ToHexStringLower(await SHA256.HashDataAsync(input,ct)));Assert.Equal(row.size,input.Length);actualCount++;actualBytes+=input.Length;
        }
        Assert.Equal(target,actualCount);
        phaseMs["localVerification"]=localVerification.Elapsed.TotalMilliseconds;
        var readbackMs=readback.ElapsedMilliseconds;
        // A new service instance must not publish unchanged data again.
        var repeat=Stopwatch.StartNew();await using(var service=new WindowsSyncService(profile))
        {
            var gate=(SemaphoreSlim)typeof(WindowsSyncService).GetField("_syncGate",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(service)!;await gate.WaitAsync(ct);
            try{await Measure("repeatClient",async()=>await (Task<WindowsSyncResult>)method.Invoke(service,["large-client",new[]{mapping},prepared,capability,progress,ct])!);}finally{gate.Release();}
        }
        var unchanged=await Metadata("repeatMetadata");if(aggregateObservation)Assert.True(strictOracle!.Observe(unchanged,false));Assert.Equal(initial.GetRawText(),unchanged.GetRawText());
        await Evidence("large-result",new{status="PASS",at=DateTimeOffset.UtcNow,target,files=actualCount,bytes=actualBytes,newlyPublishedBytes=actualBytes-beforeBytes,directories=Directory.EnumerateDirectories(source,"*",SearchOption.AllDirectories).Count(),syncMs,readbackMs,repeatMs=repeat.ElapsedMilliseconds,cycles,localCopies=0,independentFullHashReadback=true,unchangedVersions=true,totalMs=timer.ElapsedMilliseconds,uploadMiBps=(actualBytes-beforeBytes)/1048576d/(syncMs/1000d),observationMode,phaseMs});
    }
    private sealed class ProgressSink(string path):IProgress<string>
    {
        private readonly object _gate=new();private long _events;private DateTimeOffset _last;
        public void Report(string value){lock(_gate){_events++;var now=DateTimeOffset.UtcNow;if(now-_last<TimeSpan.FromSeconds(5))return;_last=now;File.WriteAllText(path,JsonSerializer.Serialize(new{at=now,events=_events,progress=value}));}}
    }
}
