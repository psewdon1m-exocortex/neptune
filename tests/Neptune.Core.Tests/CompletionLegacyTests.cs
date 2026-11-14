using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class CompletionLegacyTests
{
    [Theory]
    [InlineData("prepare","unchanged")][InlineData("append","unchanged")][InlineData("complete","unchanged")]
    [InlineData("prepare","changed")][InlineData("append","changed")][InlineData("complete","changed")]
    [InlineData("prepare","deleted")][InlineData("append","deleted")][InlineData("complete","deleted")]
    public async Task ThreeLegacyJobsSurviveActualProcessExit(string phase,string change)
    {
        var ct=TestContext.Current.CancellationToken;
        var root=Path.Combine(Path.GetTempPath(),"completion-legacy-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(root);
        using var port=new TcpListener(IPAddress.Loopback,0);port.Start();var number=((IPEndPoint)port.LocalEndpoint).Port;port.Stop();
        var origin=new Uri($"http://127.0.0.1:{number}/dav/sync/test/");
        using var listener=new HttpListener();listener.Prefixes.Add($"http://127.0.0.1:{number}/");listener.Start();
        using var stop=CancellationTokenSource.CreateLinkedTokenSource(ct);
        var originals=new Dictionary<string,LegacyReceipt>();var effects=0;var requests=new List<string>();
        var names=new[]{"one.bin","two.bin","Агентность.md"};
        var bytes=new byte[1024*1024+17];RandomNumberGenerator.Fill(bytes);var hash=Convert.ToHexStringLower(SHA256.HashData(bytes));
        foreach(var name in names)await File.WriteAllBytesAsync(Path.Combine(root,name),bytes,ct);
        var server=Task.Run(async()=>
        {
            try{while(!stop.IsCancellationRequested)
            {
                var context=await listener.GetContextAsync().WaitAsync(stop.Token);var request=context.Request;var response=context.Response;
                var path=request.Url!.AbsolutePath;requests.Add(request.HttpMethod+" "+path);
                if(request.HttpMethod=="HEAD"){response.StatusCode=404;response.Close();continue;}
                LegacyReceipt receipt;string current;
                if(request.HttpMethod=="POST"&&!path.EndsWith("/complete"))
                {
                    using var json=await JsonDocument.ParseAsync(request.InputStream,cancellationToken:stop.Token);
                    var key=json.RootElement.GetProperty("idempotencyKey").GetString()!;
                    Assert.Equal(hash,json.RootElement.GetProperty("expectedSha256").GetString());
                    if(!originals.TryGetValue(key,out receipt!)){receipt=new(Guid.NewGuid().ToString("D"));originals.Add(key,receipt);}
                    current="prepare";
                }
                else
                {
                    receipt=originals.Values.Single(r=>path.Contains(r.Id,StringComparison.Ordinal));
                    if(path.EndsWith("/complete")){current="complete";Assert.Equal(bytes,receipt.Bytes.ToArray());if(!receipt.Published){effects++;receipt.Published=true;}}
                    else{current="append";Assert.Equal(receipt.Bytes.Length.ToString(),request.Headers["Upload-Offset"]);await request.InputStream.CopyToAsync(receipt.Bytes,stop.Token);}
                }
                var payload=JsonSerializer.SerializeToUtf8Bytes(new{id=receipt.Id,status=receipt.Published?"active":"uploading",receivedSize=receipt.Bytes.Length,expectedSize=bytes.Length,etag=receipt.Published?"\"sha256-"+hash+"\"":null});
                response.StatusCode=200;response.ContentType="application/json";response.ContentLength64=payload.Length;
                if(current==phase&&!receipt.Lost){receipt.Lost=true;await response.OutputStream.WriteAsync(payload.AsMemory(0,1),stop.Token);response.Abort();}
                else{await response.OutputStream.WriteAsync(payload,stop.Token);response.Close();}
            }}catch(OperationCanceledException)when(stop.IsCancellationRequested){}
        },ct);
        try
        {
            var manifest=Path.Combine(root,"helper.json");
            await File.WriteAllTextAsync(manifest,JsonSerializer.Serialize(new{profile=root,origin=origin.AbsoluteUri,recover=false,names}),ct);
            var first=await Child(manifest,23,ct);Assert.Equal(3,first.GetProperty("failures").GetInt32());
            Assert.Equal(3,Directory.GetFiles(Path.Combine(root,"originals"),"*.bin").Length);
            Assert.Equal(3,originals.Count);var ids=originals.Values.Select(r=>r.Id).Order().ToArray();
            foreach(var name in names){if(change=="deleted")File.Delete(Path.Combine(root,name));else if(change=="changed")await File.WriteAllTextAsync(Path.Combine(root,name),"next generation",ct);}
            await File.WriteAllTextAsync(manifest,JsonSerializer.Serialize(new{profile=root,origin=origin.AbsoluteUri,recover=true,names}),ct);
            var second=await Child(manifest,0,ct);Assert.NotEqual(first.GetProperty("pid").GetInt32(),second.GetProperty("pid").GetInt32());
            Assert.Equal(ids,originals.Values.Select(r=>r.Id).Order());Assert.Equal(3,effects);
            Assert.All(originals.Values,r=>Assert.Equal(hash,Convert.ToHexStringLower(SHA256.HashData(r.Bytes.ToArray()))));
            Assert.Empty(Directory.GetFiles(Path.Combine(root,"originals"),"*.bin"));
            Assert.Empty(Directory.GetFiles(Path.Combine(root,"originals"),"*.json"));
            foreach(var name in names){if(change=="deleted")Assert.False(File.Exists(Path.Combine(root,name)));else if(change=="changed")Assert.Equal("next generation",await File.ReadAllTextAsync(Path.Combine(root,name),ct));}
            var evidence=Environment.GetEnvironmentVariable("COMPLETION_EVIDENCE");if(evidence is not null)await File.WriteAllTextAsync(Path.Combine(evidence,$"legacy-process-{phase}-{change}.json"),JsonSerializer.Serialize(new{first,second,ids,effects,hash,requests}),ct);
        }
        finally{stop.Cancel();listener.Stop();await server;Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();Directory.Delete(root,true);}
    }
    private sealed class LegacyReceipt(string id)
    {internal string Id=id;internal MemoryStream Bytes=new();internal bool Published,Lost;}
    private static async Task<JsonElement> Child(string manifest,int exit,CancellationToken ct)
    {
        var helper=Environment.GetEnvironmentVariable("NEPTUNE_NATIVE_HELPER");
        if(string.IsNullOrWhiteSpace(helper))
        {
            var directory=new DirectoryInfo(AppContext.BaseDirectory);while(directory is not null&&!Directory.Exists(Path.Combine(directory.FullName,"tests","Neptune.Stage3.NativeHelper")))directory=directory.Parent;
            Assert.NotNull(directory);
            var configuration = new DirectoryInfo(AppContext.BaseDirectory).Parent!.Name;
            helper=Path.Combine(directory.FullName,"tests","Neptune.Stage3.NativeHelper","bin",configuration,"net10.0","Neptune.Stage3.NativeHelper.dll");
        }
        Assert.True(Path.IsPathFullyQualified(helper), "Native helper must use an absolute path");
        var dotnet=Environment.GetEnvironmentVariable("DOTNET_HOST_PATH")??"dotnet";
        var start=new ProcessStartInfo(dotnet){UseShellExecute=false,CreateNoWindow=true,RedirectStandardOutput=true,RedirectStandardError=true};
        Assert.True(File.Exists(helper), "Build the native helper in the same configuration: " + helper);
        start.ArgumentList.Add(helper);start.ArgumentList.Add("legacy-upload");start.ArgumentList.Add(manifest);
        using var child=Process.Start(start)!;var stdout=child.StandardOutput.ReadToEndAsync(ct);var stderr=child.StandardError.ReadToEndAsync(ct);
        try{await child.WaitForExitAsync(ct).WaitAsync(TimeSpan.FromSeconds(45),ct);Assert.True(child.ExitCode==exit,await stderr+" exit="+child.ExitCode);return JsonSerializer.Deserialize<JsonElement>(await stdout);}
        finally{if(!child.HasExited)child.Kill(true);}
    }
    [Fact]
    public async Task EightGiBSpoolCeilingRefusesNewBytesWithoutDiscardingEvidence()
    {
        var ct=TestContext.Current.CancellationToken;var root=Path.Combine(Path.GetTempPath(),"completion-quota-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(root);
        try
        {
            var retained=Path.Combine(root,"existing.bin");await using(var file=File.Create(retained)){file.SetLength(8L*1024*1024*1024);}
            var source=Path.Combine(root,"new");await File.WriteAllTextAsync(source,"new generation",ct);
            await Assert.ThrowsAsync<IOException>(async()=>{await using var refused=await RetainedLegacyUpload.OpenAsync(root,"scope",new Uri("https://saturn.test/new"),source,null,ct);});
            Assert.Equal(8L*1024*1024*1024,new FileInfo(retained).Length);Assert.Single(Directory.GetFiles(root,"*.bin"));Assert.Empty(Directory.GetFiles(root,"*.json"));
        }finally{Directory.Delete(root,true);}
    }
}
