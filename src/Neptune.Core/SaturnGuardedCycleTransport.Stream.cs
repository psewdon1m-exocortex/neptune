using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;
public sealed partial class SaturnGuardedCycleTransport:IWindowsStreamRemote
{
    public async Task<SyncStreamStatus> StreamStatusPageAsync(SyncStreamInput original,int afterIndex,CancellationToken ct)
    {
        if(!Supports("scale-transfer-v2")||afterIndex< -1||original.Input.GetProperty("schema").GetString()!="saturn.stream-upload.v2")throw new InvalidOperationException("Paged stream capability unavailable.");
        var status=ReadStream(await Send("stream-uploads/status",HttpMethod.Post,Json(new{binding=LocalCycleWire.BindingObject(Binding),input=original.Input,afterIndex}),256*1024,ct),original);
        if(status.AfterIndex!=afterIndex)throw new InvalidDataException("Stream ACK page cursor changed.");return status;
    }
    public async Task<SyncStreamStatus> StreamCommandAsync(string command,SyncStreamInput original,string? wholeHash,CancellationToken ct)
    {
        if(!Supports("stream-upload-v1")||command is not("prepare" or "status" or "seal" or "complete"))throw new InvalidOperationException("Stream capability/command unavailable.");
        var body=new Dictionary<string,object?>{{"binding",LocalCycleWire.BindingObject(Binding)},{"input",original.Input}};
        if(command=="seal"){if(wholeHash is null||!LocalCycleWire.Hash(wholeHash))throw new InvalidDataException("Whole SHA is required at seal.");body.Add("seal",new{sha256=wholeHash,sourceGeneration=original.Generation,sourceWitnessSha256=original.WitnessSha256});}
        var status = ReadStream(await Send("stream-uploads/"+command,HttpMethod.Post,Json(body),256*1024,ct),original);
        if (command is not ("seal" or "complete")) return status;
        if (command == "seal" && status.FinalSha256 != wholeHash)
            throw new InvalidDataException("Stream completion changed the original seal.");
        var sealedSha = status.FinalSha256;
        // The server can finish verification/rename beyond one HTTP deadline.
        // Stay with this retained original; never recapture, allocate or resend
        // parts while the authoritative result is pending. Each request retains
        // the ordinary bounded timeout, identity/parser and redirect guards.
        var until = System.Diagnostics.Stopwatch.StartNew();
        while (status.State is "SEALED" or "PUBLISHING")
        {
            if (until.Elapsed >= TimeSpan.FromMinutes(10))
                throw new TimeoutException("Original stream publication remains pending; original operation retained.");
            await Task.Delay(TimeSpan.FromSeconds(1), ct);
            status = ReadStream(await Send("stream-uploads/complete",HttpMethod.Post,
                Json(new{binding=LocalCycleWire.BindingObject(Binding),input=original.Input}),256*1024,ct),original);
            if (status.FinalSha256 != sealedSha)
                throw new InvalidDataException("Stream completion changed the original seal.");
        }
        return status;
    }
    public async Task<SyncStreamStatus> StreamChunkAsync(SyncStreamInput original,SyncStreamChunk chunk,ReadOnlyMemory<byte> bytes,CancellationToken ct)
    {
        if(!Supports("stream-upload-v1")||bytes.Length is <1 or >4194304||LocalCycleWire.Decimal(chunk.Length)!=bytes.Length||Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(bytes.Span))!=chunk.Sha256)throw new InvalidDataException("Stream chunk identity/limit mismatch.");
        var metadata=JsonSerializer.SerializeToUtf8Bytes(new{binding=LocalCycleWire.BindingObject(Binding),input=original.Input,chunk},LocalCycleWire.Json);
        if(metadata.Length>40*1024)throw new InvalidDataException("Stream metadata limit.");
        using var content=new FramedContent(metadata,bytes);content.Headers.ContentType=new("application/vnd.saturn.sync-stream.v1");
        return ReadStream(await Send("stream-uploads/chunk",HttpMethod.Patch,content,256*1024,ct),original);
    }
    internal static SyncStreamStatus ReadStream(ReadOnlyMemory<byte> bytes,SyncStreamInput original)
    {
        using var doc=SyncProtocolReader.Parse(bytes);var body=doc.RootElement;
        if(original.Input.TryGetProperty("schema",out var schema)&&schema.GetString()=="saturn.stream-upload.v2")return ReadParallelStream(body,original);
        if(body.GetProperty("schema").GetString()!="saturn.stream-upload-status.v1"||body.GetProperty("operationId").GetString()!=original.OperationId)throw new InvalidDataException("Original stream identity changed.");
        var state=body.GetProperty("state").GetString()!;if(state is not("RECEIVING" or "SEALED" or "PUBLISHING" or "UNKNOWN" or "PUBLISHED"))throw new InvalidDataException("Invalid stream state.");
        var chunks=JsonSerializer.Deserialize<SyncStreamChunk[]>(body.GetProperty("chunks").GetRawText(),LocalCycleWire.Json)!;
        var size=LocalCycleWire.Decimal(original.Input.GetProperty("prepare").GetProperty("intent").GetProperty("sizeBytes").GetString()!);long offset=0;
        if(chunks.Length>256)throw new InvalidDataException("Stream ACK bound exceeded.");
        foreach(var chunk in chunks)
        {var length=LocalCycleWire.Decimal(chunk.Length);if(chunk.Index!=offset/4194304||LocalCycleWire.Decimal(chunk.Offset)!=offset||length!=Math.Min(4194304,size-offset)||length<=0||!LocalCycleWire.Hash(chunk.Sha256))throw new InvalidDataException("Invalid contiguous stream ACK map.");offset+=length;}
        var received=SyncProtocolReader.Integer64(body.GetProperty("receivedSizeBytes"));if(offset!=received||received>size)throw new InvalidDataException("Stream ACK offset changed.");
        var pending=body.GetProperty("pendingChunk").ValueKind==JsonValueKind.Null?null:JsonSerializer.Deserialize<SyncStreamChunk>(body.GetProperty("pendingChunk").GetRawText(),LocalCycleWire.Json);
        var hash=body.GetProperty("finalSha256").GetString();if(hash is not null&&!LocalCycleWire.Hash(hash))throw new InvalidDataException("Stream final hash invalid.");
        var publication=body.GetProperty("publication");
        if(pending is not null&&(pending.Index!=chunks.Length||LocalCycleWire.Decimal(pending.Offset)!=received||LocalCycleWire.Decimal(pending.Length)!=Math.Min(4194304,size-received)||received==size||!LocalCycleWire.Hash(pending.Sha256)))throw new InvalidDataException("Pending chunk does not extend the original ACK map.");
        if(hash is not null&&(received!=size||pending is not null)||state is "SEALED" or "PUBLISHING" or "PUBLISHED"&&hash is null||state=="RECEIVING"&&hash is not null)throw new InvalidDataException("Stream seal/state is inconsistent.");
        if(state=="PUBLISHED"&&publication.ValueKind!=JsonValueKind.Object||publication.ValueKind!=JsonValueKind.Null&&hash is null)throw new InvalidDataException("Stream publication has no final seal.");
        if(publication.ValueKind!=JsonValueKind.Null&&(publication.GetProperty("session").GetProperty("phase").GetString()=="published")!=(state=="PUBLISHED"))throw new InvalidDataException("Stream/catalog publication states disagree.");
        return new(state,received,chunks,pending,hash,publication.ValueKind==JsonValueKind.Null?null:publication.Clone(),body.GetRawText());
    }
}
