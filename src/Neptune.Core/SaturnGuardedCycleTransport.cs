using System.Buffers.Binary;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using System.IO.Compression;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

public sealed class GuardedCycleRemoteException : IOException
{
    public GuardedCycleRemoteException(HttpStatusCode status,string code):this(status,code,false){}
    public GuardedCycleRemoteException(HttpStatusCode status,string code,bool structuredWireCode):base(code)
        =>(Status,Code,StructuredWireCode)=(status,code,structuredWireCode);
    public HttpStatusCode Status { get; }
    public string Code { get; }
    public bool StructuredWireCode { get; }
    public TimeSpan? RetryAfter { get; init; }
}

// Guarded adapter admitted by the Windows capability/binding lifecycle.
// Syntactically valid READY alone is not production capability activation.
public sealed partial class SaturnGuardedCycleTransport : IWindowsCycleRemote, IDisposable, IAsyncDisposable
{
    private readonly HttpClient _http;
    private TimeSpan _controlTimeout = SyncHttpPolicy.ControlTimeout;
    private bool _ownsHttp;private readonly object _counterGate=new();private readonly Dictionary<string,long> _requests=new(StringComparer.Ordinal);
    private readonly Uri _origin;
    private readonly WindowsCycleConfiguration _configuration;
    private readonly Func<CancellationToken,Task<string>> _authorization;
    public SyncRootBinding Binding { get; }
    public bool MetadataGzipEnabled { get; set; }
    public bool ChangesWaitEnabled { get; set; }
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string,SemaphoreSlim> OriginSlots=new(StringComparer.Ordinal);
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string,SemaphoreSlim> OriginWaitSlots=new(StringComparer.Ordinal);
    // The public path owns its handler: a caller-supplied HttpClient cannot
    // prove that a 307/308 did not forward credentials and sealed bytes.
    public SaturnGuardedCycleTransport(WindowsCycleConfiguration configuration,Func<CancellationToken,Task<string>> authorization,TimeSpan? timeout=null)
        :this(SafeClient(configuration,timeout),configuration,authorization)
    {_ownsHttp=true;_controlTimeout=timeout??SyncHttpPolicy.ControlTimeout;}
    private static HttpClient SafeClient(WindowsCycleConfiguration configuration,TimeSpan? timeout)
    {LocalCycleWire.Configuration(configuration);if(timeout is { } duration&&(duration<=TimeSpan.Zero||duration>TimeSpan.FromMinutes(5)))throw new ArgumentOutOfRangeException(nameof(timeout));return SyncHttpPolicy.CreateClient();}
    private SaturnGuardedCycleTransport(HttpClient http, WindowsCycleConfiguration configuration,
        Func<CancellationToken,Task<string>> authorization)
    {
        LocalCycleWire.Configuration(configuration);
        _configuration=configuration;
        (_http,_origin,_authorization,Binding)=(http,configuration.ServerOrigin,authorization,configuration.Binding);
    }
    public IReadOnlyDictionary<string,long> RequestCounts{get{lock(_counterGate)return new Dictionary<string,long>(_requests,StringComparer.Ordinal);}}
    private Task? _disposeTask;
    // Both APIs join the same shutdown. Recovery uses the asynchronous form;
    // the synchronous contract may wait while an in-flight handler unwinds.
    public void Dispose()=>DisposeAsync().AsTask().GetAwaiter().GetResult();
    public ValueTask DisposeAsync()
    {
        lock(_folderGate)return new(_disposeTask??=Task.Run(DisposeCoreAsync));
    }
    private async Task DisposeCoreAsync()
    {
        Task? folders,mutations;
        lock(_folderGate){folders=_folderPump;mutations=_mutationPump;}
        _folders.Writer.TryComplete();_mutations.Writer.TryComplete();
        try
        {
            try{_folderShutdown.Cancel();}
            finally{if(_ownsHttp)_http.Dispose();}
        }
        finally
        {
            try{await Task.WhenAll(new[]{folders,mutations}.OfType<Task>()).ConfigureAwait(false);}
            finally{_folderShutdown.Dispose();}
        }
    }
    private string Route(string tail)=>"sync/v1/roots/"+Binding.RootId+"/"+tail;
    private async Task<byte[]> Send(string tail,HttpMethod method,HttpContent content,int maximum,CancellationToken ct,bool longPoll=false)
    {
        var slots=OriginSlots.GetOrAdd(_origin.GetLeftPart(UriPartial.Authority),_=>new(8,8));
        try{if(!await slots.WaitAsync(_controlTimeout,ct))throw new TimeoutException("Origin admission deadline exceeded; request was not sent.");}catch{content.Dispose();throw;}
        try
        {
        if(MetadataGzipEnabled&&content.Headers.ContentType?.MediaType=="application/json")
        {
            var bytes=await content.ReadAsByteArrayAsync(ct);
            if(bytes.Length>4*1024*1024)throw new InvalidDataException("Expanded metadata exceeds its fixed limit.");
            if(bytes.Length>16*1024)
            {
                using var cpu=await SyncResourceScheduler.CpuSlot(ct);
                using var packed=new MemoryStream();using(var gzip=new GZipStream(packed,CompressionLevel.Fastest,true))gzip.Write(bytes);
                if(packed.Length>4*1024*1024)throw new InvalidDataException("Compressed metadata exceeds its fixed limit.");
                content.Dispose();content=new ByteArrayContent(packed.ToArray());content.Headers.ContentType=new("application/json");content.Headers.ContentEncoding.Add("gzip");
            }
        }
        using var request=new HttpRequestMessage(method,new Uri(_origin,Route(tail))){Content=content};
        var authorization=await _authorization(ct);
        if(authorization.Length is <1 or >8192||authorization.IndexOfAny(['\r','\n'])>=0)throw new InvalidDataException("Invalid current authorization header.");
        request.Headers.TryAddWithoutValidation("Authorization",authorization);
        lock(_counterGate){var key=method.Method+" "+tail;_requests.TryGetValue(key,out var count);_requests[key]=checked(count+1);}
        using var response=await SyncHttpPolicy.SendAsync(_http,request,HttpCompletionOption.ResponseHeadersRead,ct,controlTimeout:longPoll?SyncHttpPolicy.LongPollTimeout:_controlTimeout,longPoll:longPoll);
        if(response.RequestMessage?.RequestUri!=request.RequestUri)throw new InvalidDataException("Guarded transport response changed the exact origin/route.");
        if((int)response.StatusCode is >=300 and <=399)throw new GuardedCycleRemoteException(response.StatusCode,"guarded_redirect_refused");
        byte[] raw;
        try
        {
            var limit=response.IsSuccessStatusCode?maximum:16*1024;
            if(response.Content.Headers.ContentLength>limit)throw new InvalidDataException("Guarded response exceeds fixed limit.");
            await using var stream=await response.Content.ReadAsStreamAsync(ct);using var memory=new MemoryStream();var buffer=new byte[16*1024];
            while(true){var count=await stream.ReadAsync(buffer,ct);if(count==0)break;if(memory.Length>limit-count)throw new InvalidDataException("Guarded response exceeds fixed limit.");memory.Write(buffer,0,count);}
            raw=memory.ToArray();
        }
        catch(Exception error)when(!response.IsSuccessStatusCode&&!ct.IsCancellationRequested&&error is IOException or InvalidDataException or HttpRequestException or TimeoutException or OperationCanceledException)
        {throw new GuardedCycleRemoteException(response.StatusCode,"http_"+(int)response.StatusCode){RetryAfter=NetworkFailure.RetryAfter(response)};}
        if(!response.IsSuccessStatusCode)
        {
            string code="http_"+(int)response.StatusCode;var structured=false;
            TimeSpan? retry=response.Headers.RetryAfter?.Delta;
            if(retry is null&&response.Headers.RetryAfter?.Date is { } retryDate)retry=retryDate-DateTimeOffset.UtcNow;
            if(raw.Length<=16*1024)try
            {
                using var body=SyncProtocolReader.Parse(raw);var root=body.RootElement;
                if(root.ValueKind==JsonValueKind.Object&&root.EnumerateObject().All(p=>p.Name is "code" or "retry_after_ms")&&root.TryGetProperty("code",out var value)&&value.ValueKind==JsonValueKind.String&&value.GetString()!.Length is >0 and <=128)
                {
                    code=value.GetString()!;structured=true;
                    if(root.TryGetProperty("retry_after_ms",out var ms)&&ms.TryGetInt64(out var delay)&&delay>=0&&delay<=86_400_000)retry=TimeSpan.FromMilliseconds(Math.Max(retry?.TotalMilliseconds??0,delay));
                }
            }catch(Exception e)when(e is SyncWireException or InvalidOperationException){/* retain status without trusting invalid body */}
            throw new GuardedCycleRemoteException(response.StatusCode,code,structured){RetryAfter=retry<TimeSpan.Zero?TimeSpan.Zero:retry};
        }
        return raw;
        }
        finally{content.Dispose();slots.Release();}
    }
    private static HttpContent Json(object body,int maximum=LocalCycleWire.MetadataLimit)
    {
        var raw=JsonSerializer.SerializeToUtf8Bytes(body);if(raw.Length>maximum)throw new InvalidDataException("Guarded request metadata exceeds fixed limit.");
        var content=new ByteArrayContent(raw);content.Headers.ContentType=new("application/json");return content;
    }
    public Task<byte[]> StartSnapshotAsync(string id,CancellationToken ct)
        =>Snapshot("start",id,ct);
    public Task<byte[]> StepSnapshotAsync(string id,CancellationToken ct)
        =>Snapshot("step",id,ct);
    private Task<byte[]> Snapshot(string command,string id,CancellationToken ct)
    {
        if(!LocalCycleWire.Uuid(id))throw new InvalidDataException("Snapshot UUID must be persisted before I/O.");
        return Send("snapshots/"+command,HttpMethod.Post,Json(new {binding=LocalCycleWire.BindingObject(Binding),snapshot_id=id},16*1024),16*1024,ct);
    }
    public async Task<SyncSnapshotReady> ReadySnapshotAsync(string id,CancellationToken ct)
    {
        var raw=await Snapshot("ready",id,ct);var ready=SyncProtocolReader.ReadSnapshot(raw);
        if(ready.Binding!=Binding||ready.SnapshotId!=id)throw new InvalidDataException("READY cannot rebind persisted snapshot/enrollment.");return ready;
    }
    public async Task<SyncSnapshotEntriesPage> ReadEntriesAsync(SyncSnapshotReady ready,string cursor,string? after,CancellationToken ct)
    {
        if(ready.Binding!=Binding)throw new InvalidDataException("Snapshot binding changed.");
        var bytes=await Send("entries",HttpMethod.Post,Json(new {binding=LocalCycleWire.BindingObject(Binding),cursor},16*1024),SyncProtocolReader.MaxPageBytes,ct);
        return SyncSnapshotEntriesReader.Read(bytes,ready,after);
    }
    public async Task<SyncFeedPage> ReadChangesAsync(string cursor,long after,CancellationToken ct)
    {
        var bytes=await Send("changes",HttpMethod.Post,Json(new {binding=LocalCycleWire.BindingObject(Binding),cursor},16*1024),SyncProtocolReader.MaxPageBytes,ct);
        return ObserveDirectoryFeed(SyncProtocolReader.ReadFeed(bytes,Binding,after.ToString(CultureInfo.InvariantCulture)));
    }
    public async Task<SyncFeedPage> WaitForChangesAsync(string cursor,long after,CancellationToken ct)
    {
        if(!ChangesWaitEnabled)return await ReadChangesAsync(cursor,after,ct);
        var waits=OriginWaitSlots.GetOrAdd(_origin.GetLeftPart(UriPartial.Authority),_=>new(2,2));if(!await waits.WaitAsync(_controlTimeout,ct))throw new TimeoutException("Long-poll admission deadline exceeded.");
        try{var bytes=await Send("changes",HttpMethod.Post,Json(new {binding=LocalCycleWire.BindingObject(Binding),cursor,wait_ms=25000},16*1024),SyncProtocolReader.MaxPageBytes,ct,longPoll:true);return ObserveDirectoryFeed(SyncProtocolReader.ReadFeed(bytes,Binding,after.ToString(CultureInfo.InvariantCulture)));}
        finally{waits.Release();}
    }
    private async Task<CycleFolderOutcome> EnsureDirectoryIndividualAsync(CycleFolderInput original,CancellationToken ct)
    {
        LocalCycleWire.FolderInput(original);if(original.RootId!=Binding.RootId)throw new InvalidDataException("Folder root changed.");
        var raw=await Send("directories/ensure-v2",HttpMethod.Post,Json(new {binding=LocalCycleWire.BindingObject(Binding),input=LocalCycleWire.FolderObject(original)},LocalCycleWire.MetadataLimit),LocalCycleWire.MetadataLimit,ct);
        return LocalCycleWire.Folder(raw,original);
    }
    public async Task<CycleUploadStatus> UploadAsync(string command,CycleUploadInput original,string? upload,string hash,CancellationToken ct)
    {
        if(command is not ("prepare" or "inspect" or "complete")||command!="prepare"&&upload is null||upload is not null&&!LocalCycleWire.Uuid(upload)||original.Intent.RootId!=Binding.RootId)
            throw new InvalidDataException("Invalid original guarded command binding.");
        object body=upload is null?new {binding=LocalCycleWire.BindingObject(Binding),input=LocalCycleWire.InputObject(original)}:
            new {binding=LocalCycleWire.BindingObject(Binding),input=LocalCycleWire.InputObject(original),upload_id=upload};
        var raw=await Send("catalog-uploads/"+command,HttpMethod.Post,Json(body),LocalCycleWire.MetadataLimit,ct);
        return LocalCycleWire.Status(raw,original,hash,upload);
    }
    public async Task<CycleUploadStatus> AppendAsync(CycleUploadInput original,string upload,string hash,long offset,ReadOnlyMemory<byte> bytes,CancellationToken ct)
    {
        if(!LocalCycleWire.Uuid(upload)||original.Intent.RootId!=Binding.RootId||offset<0||bytes.Length is <1 or >8*1024*1024)
            throw new InvalidDataException("Invalid fixed-budget guarded append.");
        var metadata=JsonSerializer.SerializeToUtf8Bytes(new {binding=LocalCycleWire.BindingObject(Binding),input=LocalCycleWire.InputObject(original),upload_id=upload,offset=offset.ToString(CultureInfo.InvariantCulture)});
        if(metadata.Length>LocalCycleWire.MetadataLimit)throw new InvalidDataException("Append metadata exceeds fixed budget.");
        var raw=await Send("catalog-uploads/append",HttpMethod.Patch,new FramedContent(metadata,bytes),LocalCycleWire.MetadataLimit,ct);
        return LocalCycleWire.Status(raw,original,hash,upload);
    }
    private sealed class FramedContent:HttpContent
    {
        private readonly byte[] _metadata;private readonly ReadOnlyMemory<byte> _bytes;
        internal FramedContent(byte[] metadata,ReadOnlyMemory<byte> bytes)
        {(_metadata,_bytes)=(metadata,bytes);Headers.ContentType=new MediaTypeHeaderValue("application/vnd.saturn.sync-upload.v1");}
        protected override bool TryComputeLength(out long length){length=4L+_metadata.Length+_bytes.Length;return true;}
        protected override Task SerializeToStreamAsync(Stream stream,TransportContext? context)=>Serialize(stream,CancellationToken.None);
        protected override Task SerializeToStreamAsync(Stream stream,TransportContext? context,CancellationToken ct)=>Serialize(stream,ct);
        private async Task Serialize(Stream stream,CancellationToken ct)
        {var header=new byte[4];BinaryPrimitives.WriteUInt32BigEndian(header,(uint)_metadata.Length);await stream.WriteAsync(header,ct);await stream.WriteAsync(_metadata,ct);await stream.WriteAsync(_bytes,ct);}
    }
}
