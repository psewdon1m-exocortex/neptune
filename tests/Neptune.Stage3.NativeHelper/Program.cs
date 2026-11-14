using System.IO.MemoryMappedFiles;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using Neptune.Core;

if(args.Length!=2)throw new ArgumentException("mode and fixture manifest required");
using var manifest=JsonDocument.Parse(File.ReadAllBytes(args[1]));var value=manifest.RootElement;
if(args[0]=="legacy-upload")
{
    var legacyProfile=Path.GetFullPath(value.GetProperty("profile").GetString()!);
    var endpoint=new Uri(value.GetProperty("origin").GetString()!);
    if(!endpoint.IsLoopback||endpoint.Scheme!="http")throw new InvalidDataException("Legacy helper requires an owned loopback listener.");
    var state=new NeptuneStateStore(Path.Combine(legacyProfile,"legacy.db"));await state.InitializeAsync();
    using var http=SyncHttpPolicy.CreateClient();
    var client=new WebDavSyncClient(http,state){RetainedOriginalsDirectory=Path.Combine(legacyProfile,"originals")};
    var failures=0;
    if(value.GetProperty("recover").GetBoolean())await client.RecoverRetainedUploadsAsync(endpoint,"completion-fixture",CancellationToken.None);
    else foreach(var entry in value.GetProperty("names").EnumerateArray())
    {
        var name=entry.GetString()!;if(Path.GetFileName(name)!=name)throw new InvalidDataException("Simple owned filename required.");
        try{await client.UploadAsync(endpoint,"completion-fixture",name,Path.Combine(legacyProfile,name),CancellationToken.None);}
        catch(Exception error)when(error is HttpRequestException or HttpIOException or TimeoutException or IOException { InnerException: System.Net.Sockets.SocketException }){failures++;}
    }
    Console.WriteLine(JsonSerializer.Serialize(new{pid=Environment.ProcessId,failures}));Console.Out.Flush();
    Environment.Exit(failures>0?23:0);return;
}
if(args[0]=="old-writer")
{
    var oldCore=Path.GetFullPath(value.GetProperty("core").GetString()!);var expected=value.GetProperty("sha256").GetString()!;
    if(Convert.ToHexStringLower(SHA256.HashData(File.ReadAllBytes(oldCore)))!=expected)throw new InvalidDataException("Qualified predecessor hash changed.");
    var database=Path.GetFullPath(value.GetProperty("database").GetString()!);var owned=Path.GetFullPath(value.GetProperty("profile").GetString()!);
    if(!database.StartsWith(Path.TrimEndingDirectorySeparator(owned)+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))throw new InvalidDataException("Only the owned fixture database may be inspected.");
    var context=new System.Runtime.Loader.AssemblyLoadContext("stage3-predecessor",isCollectible:true);var assembly=context.LoadFromAssemblyPath(oldCore);var stateType=assembly.GetType("Neptune.Core.NeptuneStateStore")!;var state=Activator.CreateInstance(stateType,[database])!;string? refusal=null;
    try{await (Task)stateType.GetMethod("InitializeAsync")!.Invoke(state,[CancellationToken.None])!;}catch(InvalidDataException e){refusal=e.Message;}
    if(refusal is null)throw new InvalidDataException("Old writer accepted the current profile.");
    Console.WriteLine(JsonSerializer.Serialize(new{pid=Environment.ProcessId,oldCoreSha256=expected,refusal,refused=true}));return;
}
if(args[0]=="enqueue-crash")
{
    var ownedProfile=value.GetProperty("profile").GetString()!;var ownedSource=Path.Combine(ownedProfile,"source");Directory.CreateDirectory(ownedSource);var ownedStore=new NeptuneStateStore(Path.Combine(ownedProfile,"state.db"));await ownedStore.InitializeAsync();await ownedStore.AddMappingAsync(new("crash","crash",ownedSource,true,DateTimeOffset.UtcNow));
    await using var native=await WindowsLocalCoherenceCoordinator.OpenAsync(ownedSource);var nativeProof=native.RootProof;await using var usn=new WindowsUsnJournalReader(nativeProof.VolumeGuid);var nativeBounds=await usn.QueryAsync();
    var ownedBinding=new LocalSourceBinding("crash","crash","crash",ownedSource,nativeProof.VolumeGuid,nativeProof.Handle.LegacyVolumeSerial.ToString("x8"),"NTFS",nativeProof.Handle.FileId128,nativeProof.Handle.CreationTime,1,"rules","windows-raw-nfc-v1");
    await using var ownedSession=await ownedStore.OpenLocalJournalSessionAsync(ownedBinding,nativeBounds,nativeBounds.NextUsn,new(MinimumFreeDiskBytes:0));
    await (Task)typeof(NeptuneStateStore).GetMethod("EnsureEventInboxAsync",BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(ownedStore,[CancellationToken.None])!;
    await (Task)typeof(LocalSourceJournalSession).GetMethod("SaveEventHintsAsync",BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(ownedSession,[new[]{"durable-before-callback"},true,CancellationToken.None])!;
    Console.WriteLine(JsonSerializer.Serialize(new{pid=Environment.ProcessId,binding=ownedBinding,cursor=(await ownedSession.ReadStateAsync()).Cursor,committed=true,callback=false}));Console.Out.Flush();
    // Exit skips all using/finally blocks, abandoning the real session lease
    // immediately after the durable enqueue and before any watcher callback.
    Environment.Exit(23);return;
}
var cacheType=typeof(WindowsUsnJournalReader).Assembly.GetType("Neptune.Core.WindowsVolumeJournalCache")!;
using var cache=(IDisposable)Activator.CreateInstance(cacheType,BindingFlags.Instance|BindingFlags.NonPublic,null,[value.GetProperty("key").GetString()!,LocalJournalReadMode.Named],null)!;
if(args[0]=="abandon")
{
    var mutex=(Mutex)cacheType.GetField("_mutex",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(cache)!;mutex.WaitOne();
    Console.WriteLine(JsonSerializer.Serialize(new{pid=Environment.ProcessId,acquired=true}));Console.Out.Flush();Environment.Exit(17);
}
await using var reader=new WindowsUsnJournalReader(value.GetProperty("volume").GetString()!);
var cursor=JsonSerializer.Deserialize<LocalJournalCursor>(value.GetProperty("cursor"))!;
var view=(MemoryMappedViewAccessor)cacheType.GetField("_view",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(cache)!;
var before=view.ReadInt32(8);
var page=(LocalJournalPage)cacheType.GetMethod("Read",BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(cache,[reader,cursor,1024,CancellationToken.None])!;
var payload=(byte[])typeof(LocalJournalPage).GetProperty("Payload",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(page)!;
var profile=value.GetProperty("profile").GetString()!;var source=Path.Combine(profile,"source");Directory.CreateDirectory(source);var store=new NeptuneStateStore(Path.Combine(profile,"child.db"));await store.InitializeAsync();await store.AddMappingAsync(new("child","child",source,true,DateTimeOffset.UtcNow));
await using var coordinator=await WindowsLocalCoherenceCoordinator.OpenAsync(source);var proof=coordinator.RootProof;var binding=new LocalSourceBinding("child","child","child",source,proof.VolumeGuid,proof.Handle.LegacyVolumeSerial.ToString("x8"),"NTFS",proof.Handle.FileId128,proof.Handle.CreationTime,1,"rules","windows-raw-nfc-v1");
var observed=await reader.QueryAsync();await using var session=await store.OpenLocalJournalSessionAsync(binding,observed,observed.NextUsn,new(MinimumFreeDiskBytes:0));var initial=await session.ReadStateAsync();File.WriteAllText(Path.Combine(source,"child-event"),"child event");var ownPage=await reader.ReadAsync(initial.Cursor,1024);await session.CapturePageAsync(initial.Cursor,ownPage);var durable=await session.ReadStateAsync();
Console.WriteLine(JsonSerializer.Serialize(new{pid=Environment.ProcessId,before,after=view.ReadInt32(8),page.StartUsn,page.NextReadUsn,sha256=Convert.ToHexStringLower(SHA256.HashData(payload)),initialCursor=initial.Cursor.NextReadUsn,durableCursor=durable.Cursor.NextReadUsn,root=proof.Handle.FileId128}));
