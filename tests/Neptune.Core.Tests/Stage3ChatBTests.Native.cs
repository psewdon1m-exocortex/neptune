using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class Stage3ChatBTests
{
    [Fact]
    public async Task B02_ProcessDeathAfterDurableEnqueueBeforeCallbackRetainsHintAndGap()
    {
        RequireWindows();await using var f=await WindowsFreshCycleTests.Fixture.Create();var manifest=Path.Combine(f.Directory,"enqueue-crash.json");await File.WriteAllTextAsync(manifest,JsonSerializer.Serialize(new{profile=f.Directory}),Ct);
        var child=await CacheChild("enqueue-crash",manifest,23);Assert.True(child.GetProperty("committed").GetBoolean());Assert.False(child.GetProperty("callback").GetBoolean());var binding=child.GetProperty("binding").Deserialize<LocalSourceBinding>()!;var cursor=child.GetProperty("cursor").Deserialize<LocalJournalCursor>()!;
        f.Store=new(f.Database);await using var reader=new WindowsUsnJournalReader(binding.VolumeGuid);var bounds=await reader.QueryAsync(Ct);await using var session=await f.Store.OpenLocalJournalSessionAsync(binding,bounds,bounds.NextUsn,new(MinimumFreeDiskBytes:0),Ct);
        var pending=await session.ReadEventHintsAsync(Ct);var hint=Assert.Single(pending.Hints);Assert.Equal("durable-before-callback",hint.Path);Assert.Equal(1,hint.Generation);Assert.True(pending.Gap);Assert.Equal(1,pending.GapGeneration);Assert.Equal(cursor.NextReadUsn,(await session.ReadStateAsync(Ct)).Cursor.NextReadUsn);
        await f.Evidence("B02-process-death",new{child,pending,leaseReacquired=true});
    }
    private static async Task<JsonElement> CacheChild(string mode,string manifest,int expectedExit)
    {
        var dotnet=Environment.GetEnvironmentVariable("DOTNET_HOST_PATH")??Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),".dotnet","dotnet.exe");
        var configuration=typeof(Stage3ChatBTests).Assembly.GetCustomAttribute<AssemblyConfigurationAttribute>()?.Configuration;
        if(configuration is not("Debug" or "Release"))throw new InvalidDataException("Native helper requires the exact test build configuration.");
        var helper=RepositoryFile($"neptune/tests/Neptune.Stage3.NativeHelper/bin/{configuration}/net10.0/Neptune.Stage3.NativeHelper.dll");
        var start=new ProcessStartInfo(dotnet){UseShellExecute=false,CreateNoWindow=true,RedirectStandardOutput=true,RedirectStandardError=true};start.ArgumentList.Add(helper);start.ArgumentList.Add(mode);start.ArgumentList.Add(manifest);
        using var child=Process.Start(start)??throw new IOException("Fixture helper failed to start");var stdout=child.StandardOutput.ReadToEndAsync(Ct);var stderr=child.StandardError.ReadToEndAsync(Ct);
        try{await child.WaitForExitAsync(Ct).WaitAsync(TimeSpan.FromSeconds(30),Ct);var errors=await stderr;Assert.True(child.ExitCode==expectedExit,$"Native helper exit {child.ExitCode}, expected {expectedExit}: {errors}");Assert.Equal("",errors);return JsonSerializer.Deserialize<JsonElement>(await stdout);}
        finally{if(!child.HasExited&&string.Equals(child.MainModule!.FileName,Path.GetFullPath(dotnet),StringComparison.OrdinalIgnoreCase))child.Kill(entireProcessTree:true);}
    }
    [Fact]
    public async Task B03_TwoProcessesReuseEightPagesEvictNinthCorruptAndAbandonedCache()
    {
        RequireWindows();await using var f=await WindowsFreshCycleTests.Fixture.Create();await using var coordinator=await WindowsLocalCoherenceCoordinator.OpenAsync(f.Source,Ct);
        var volume=coordinator.RootProof.VolumeGuid;await using var reader=new WindowsUsnJournalReader(volume);var bounds=await reader.QueryAsync(Ct);
        await using var engine=await Open(f);var cursor=(await Session(engine).ReadStateAsync(Ct)).Cursor;
        for(var i=0;i<180;i++)File.WriteAllText(Path.Combine(f.Source,"record-"+i),"native");
        // A private cache namespace prevents fault injection from touching any
        // normal profile's shared cache on the host volume.
        var key=volume+".stage3-fixture-"+Guid.NewGuid().ToString("N");using var cache=new WindowsVolumeJournalCache(key,LocalJournalReadMode.Named);
        var cursors=new List<LocalJournalCursor>();var pages=new List<LocalJournalPage>();
        for(var i=0;i<9;i++){cursors.Add(cursor);var page=cache.Read(reader,cursor,1024,Ct);Assert.True(page.NextReadUsn>cursor.NextReadUsn);pages.Add(page);cursor=cursor with{NextReadUsn=page.NextReadUsn};}
        var view=(MemoryMappedViewAccessor)typeof(WindowsVolumeJournalCache).GetField("_view",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(cache)!;Assert.Equal(1,view.ReadInt32(8));
        var manifest=Path.Combine(f.Directory,"cache.json");await File.WriteAllTextAsync(manifest,JsonSerializer.Serialize(new{key,volume,cursor=cursors[8],profile=Path.Combine(f.Directory,"second-profile")}),Ct);
        var child=await CacheChild("read",manifest,0);Assert.Equal(1,child.GetProperty("before").GetInt32());Assert.Equal(1,child.GetProperty("after").GetInt32());Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(pages[8].Payload)),child.GetProperty("sha256").GetString());Assert.True(child.GetProperty("initialCursor").GetInt64()>cursors[0].NextReadUsn);Assert.True(child.GetProperty("durableCursor").GetInt64()>child.GetProperty("initialCursor").GetInt64());Assert.NotEqual(coordinator.RootProof.Handle.FileId128,child.GetProperty("root").GetString());
        cache.Read(reader,cursors[0],1024,Ct);Assert.Equal(2,view.ReadInt32(8)); // ninth evicted first
        view.Write(16+40,(byte)(view.ReadByte(16+40)^0xff));cache.Read(reader,cursors[8],1024,Ct);Assert.Equal(3,view.ReadInt32(8)); // bad SHA forces physical read
        var death=await CacheChild("abandon",manifest,17);Assert.True(death.GetProperty("acquired").GetBoolean());cache.Read(reader,cursors[8],1024,Ct);Assert.Equal(1,view.ReadInt32(8));
        Assert.Throws<LocalSourceGapException>(()=>cache.Read(reader,cursors[8] with{JournalIdHex="0000000000000000"},1024,Ct));
        var state=await Session(engine).ReadStateAsync(Ct);Assert.Equal(cursors[0].NextReadUsn,state.Cursor.NextReadUsn);Assert.True(state.Cursor.Valid);
        await f.Evidence("B03-cache",new{bounds,child,death,cursors,physicalReads=12,cacheHits=1});
    }
    private sealed class NoopLease:IDisposable{public void Dispose(){}}
    [Theory]
    [InlineData("unchanged")][InlineData("replace")][InlineData("delete")][InlineData("modify")]
    public async Task B10_NativeChunkBeforeWholeSealAndDenyWriterCustody(string change)
    {
        RequireWindows();await using var f=await WindowsFreshCycleTests.Fixture.Create();var data=new byte[12*1024*1024+17];new Random(314159).NextBytes(data);var file=Path.Combine(f.Source,"stream");await File.WriteAllBytesAsync(file,data,Ct);
        await using var engine=await Open(f);Assert.Null((await engine.InitialSyncAsync(Ct)).Error);var op=(string)(await f.Scalar("SELECT operation_id FROM local_cycle_work WHERE phase=5"))!;var work=await Session(engine).ReadCycleWorkAsync(engine.ScopeId,op,Ct);
        await using var capture=await WindowsNativeCycleCapture.OpenAsync(f.Source,Ct);var chunks=new List<string>();var reads=0;var began=false;
        var task=capture.CaptureStreamAsync(work,Path.Combine(f.Directory,"chunk"),(witness,size,ct)=>{Assert.Equal(data.Length,size);Assert.Equal(64,witness.Length);began=true;return Task.CompletedTask;},
            (index,bytes,ct)=>
            {
                Assert.True(began);Assert.Equal(index+1,reads);if(index==0)
                {
                    Assert.Equal(4194304,bytes.Length);Assert.True(reads<4);
                    if(change=="replace")Assert.Throws<IOException>(()=>File.Move(file,file+"-moved"));
                    if(change=="delete")Assert.Throws<IOException>(()=>File.Delete(file));
                    if(change=="modify")Assert.Throws<IOException>(()=>File.WriteAllText(file,"different"));
                }
                chunks.Add(Convert.ToHexStringLower(SHA256.HashData(bytes.Span)));return Task.CompletedTask;
            },ct=>{reads++;return Task.FromResult<IDisposable>(new NoopLease());},Ct);
        var seal=await task;Assert.Equal(4,chunks.Count);Assert.Equal(data.Length,seal.Size);Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(data)),seal.Sha256);
        await f.Evidence("B10-native-"+change,new{seal.Size,seal.Sha256,chunks,reads});
    }
}
