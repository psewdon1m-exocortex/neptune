using System.Net.Http.Json;
using System.Reflection;
using System.Text.Json;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;
[Collection("Native fresh cycle")]
[Trait("Lane", "Live")]
public sealed class CompletionAdoptionTests
{
    [Fact]
    public async Task RealOperatorArtifactAdoptsRetainedWindowsCycleAndFreshApiFeed()
    {
        var ct=TestContext.Current.CancellationToken;
        var evidence=Environment.GetEnvironmentVariable("COMPLETION_EVIDENCE")??throw new InvalidOperationException("Own API required");
        using var json=JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(evidence,"clients.private.json"),ct));var c=json.RootElement[0];
        var b=c.GetProperty("binding");string B(string key)=>b.GetProperty(key).GetString()!;
        var binding=new SyncRootBinding(B("protocol"),B("descriptor_schema"),B("server_identity"),B("root_id"),B("server_epoch"),B("policy_version"),B("binding_version"));
        var ns=new SyncNamespacePolicy(c.GetProperty("namespace").GetProperty("codec").GetString()!,c.GetProperty("namespace").GetProperty("policyHash").GetString()!);
        var origin=new Uri(c.GetProperty("origin").GetString()!);using var http=SyncHttpPolicy.CreateClient();
        await using var f=await WindowsFreshCycleTests.Fixture.Create();
        await File.WriteAllTextAsync(Path.Combine(f.Source,"Агентность.md"),"retained adoption bytes",ct);
        await File.WriteAllTextAsync(Path.Combine(f.Source,"empty.md"),"",ct);
        var cfg=new WindowsCycleConfiguration("mapping","client","audit","rules-v1",origin,c.GetProperty("deviceId").GetString()!,"fixture",c.GetProperty("backendIdentity").GetString()!,binding);
        SaturnGuardedCycleTransport Remote(WindowsCycleConfiguration conf){var r=(SaturnGuardedCycleTransport)typeof(SaturnGuardedCycleTransport).GetConstructor(BindingFlags.Instance|BindingFlags.NonPublic,null,[typeof(HttpClient),typeof(WindowsCycleConfiguration),typeof(Func<CancellationToken,Task<string>>)],null)!.Invoke([http,conf,(Func<CancellationToken,Task<string>>)(_=>Task.FromResult(c.GetProperty("authorization").GetString()!))]);r.ConfigureExtensions(new HashSet<string>{"namespace-mutations-v1","metadata-gzip-v1","folder-batch-v1"},ns,2);return r;}
        string oldScope;using(var remote=Remote(cfg))
        {
            await using var engine=await WindowsFreshSyncEngine.ResumeOrOpenAsync(new WindowsCycleNativeOptions(LocalJournalReadMode.Named,true),f.Store,f.Source,Path.Combine(f.Directory,"spool"),cfg,remote,new(MinimumFreeDiskBytes:0),cancellationToken:ct);
            oldScope=engine.ScopeId;var first=await engine.InitialSyncAsync(ct);
            for(var n=0;n<30&&Convert.ToInt64(await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE phase=4"))!=2;n++){await Task.Delay(250,ct);first=await engine.FreshSyncAsync(ct);}
            Assert.Null(first.Error);Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE phase=4"));
        }
        var originals=await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)");
        using var adopt=await http.PostAsJsonAsync(new Uri(origin,"__completion/adopt"),new{kind="windows"},ct);
        var raw=await adopt.Content.ReadAsStringAsync(ct);Assert.True(adopt.IsSuccessStatusCode,raw);
        var plan=await f.Store.PreviewServerBindingAsync(raw,ct);Assert.Empty(plan.Blockers);Assert.NotEqual(binding,plan.Configuration.Binding);
        await f.Store.ApplyServerBindingAsync(plan,ct);Assert.Equal("QUALIFYING",await f.Scalar("SELECT state FROM client_binding_migrations"));
        using var artifact=JsonDocument.Parse(raw);var receipt=artifact.RootElement.GetProperty("qualification_receipt_sha256").GetString()!;
        await f.Store.ReadAdoptedAnchorAsync(plan.Configuration.Binding,plan.Namespace,receipt,ct);
        using var qualifiedRequest=new HttpRequestMessage(HttpMethod.Get,new Uri(origin,"sync/v1/roots/"+binding.RootId+"/capabilities"));qualifiedRequest.Headers.TryAddWithoutValidation("Authorization",c.GetProperty("authorization").GetString());
        using var qualifiedResponse=await http.SendAsync(qualifiedRequest,ct);qualifiedResponse.EnsureSuccessStatusCode();var capability=WindowsGuardedCapabilities.Parse(await qualifiedResponse.Content.ReadAsByteArrayAsync(ct),cfg.DeviceId,binding.RootId);Assert.Equal(plan.Configuration.Binding,capability.Binding);
        f.Store=new NeptuneStateStore(f.Database);
        using var adopted=Remote(plan.Configuration);
        await using var resumed=await WindowsFreshSyncEngine.ResumeOrOpenAsync(new WindowsCycleNativeOptions(LocalJournalReadMode.Named,true),f.Store,f.Source,Path.Combine(f.Directory,"spool"),plan.Configuration,adopted,new(MinimumFreeDiskBytes:0),cancellationToken:ct);
        var result=await resumed.FreshSyncAsync(ct);Assert.Null(result.Error);Assert.Equal(0,result.CapturedBytes);Assert.Equal(0,result.PublishedFiles);Assert.Equal(0,result.Conflicts);Assert.Equal("APPLIED",await f.Scalar("SELECT state FROM client_binding_migrations"));
        Assert.Equal(originals,await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"));
        Assert.Equal(2L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE scope_id='"+oldScope+"'"));
        await File.WriteAllTextAsync(Path.Combine(evidence,"neptune-real-adoption.json"),JsonSerializer.Serialize(new{oldScope,plan.NextScope,result,receipt,originalsPreserved=true}),ct);
    }
}
