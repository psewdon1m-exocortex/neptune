using System.Text.Json;
using Neptune.Core.Sync.Protocol;
namespace Neptune.Core;

public sealed partial class SaturnGuardedCycleTransport
{
    private static SyncStreamStatus ReadParallelStream(JsonElement body,SyncStreamInput original)
    {
        string[] fields=["schema","operationId","state","receivedSizeBytes","chunks","pendingChunks","afterIndex","nextIndex","finalSha256","publication"];
        if(body.ValueKind!=JsonValueKind.Object||body.EnumerateObject().Count()!=fields.Length||body.EnumerateObject().Any(p=>!fields.Contains(p.Name,StringComparer.Ordinal))
            ||body.GetProperty("schema").GetString()!="saturn.stream-upload-status.v2"||body.GetProperty("operationId").GetString()!=original.OperationId)throw new InvalidDataException("Original parallel stream identity/shape changed.");
        var state=body.GetProperty("state").GetString()!;if(state is not("RECEIVING" or "SEALED" or "PUBLISHING" or "UNKNOWN" or "PUBLISHED"))throw new InvalidDataException("Invalid stream state.");
        SyncStreamChunk[] ReadParts(string key,int maximum)
        {
            var array=body.GetProperty(key);string[] partFields=["index","offset","length","sha256"];
            if(array.ValueKind!=JsonValueKind.Array||array.GetArrayLength()>maximum)throw new InvalidDataException("Stream part list exceeds its bound.");
            foreach(var part in array.EnumerateArray())if(part.ValueKind!=JsonValueKind.Object||part.EnumerateObject().Count()!=4||part.EnumerateObject().Any(p=>!partFields.Contains(p.Name,StringComparer.Ordinal)))throw new InvalidDataException("Unexpected stream part fields.");
            return JsonSerializer.Deserialize<SyncStreamChunk[]>(array,LocalCycleWire.Json)!;
        }
        var chunks=ReadParts("chunks",256);var pending=ReadParts("pendingChunks",4);
        var size=LocalCycleWire.Decimal(original.Input.GetProperty("prepare").GetProperty("intent").GetProperty("sizeBytes").GetString()!);
        var after=body.GetProperty("afterIndex").GetInt32();int? next=body.GetProperty("nextIndex").ValueKind==JsonValueKind.Null?null:body.GetProperty("nextIndex").GetInt32();
        if(chunks.Length>256||pending.Length>4||after< -1||size>9007199254740991L)throw new InvalidDataException("Stream ACK page bound exceeded.");
        var seen=new HashSet<int>();long acknowledged=0;var previous=after;
        foreach(var chunk in chunks.Concat(pending))
        {
            var offset=(long)chunk.Index*4194304;var length=LocalCycleWire.Decimal(chunk.Length);
            if(chunk.Index<0||!seen.Add(chunk.Index)||LocalCycleWire.Decimal(chunk.Offset)!=offset||offset>=size||length!=Math.Min(4194304,size-offset)||!LocalCycleWire.Hash(chunk.Sha256))throw new InvalidDataException("Invalid indexed stream ACK.");
        }
        foreach(var chunk in chunks){if(chunk.Index<=previous)throw new InvalidDataException("Stream ACK ordering changed.");previous=chunk.Index;acknowledged=checked(acknowledged+LocalCycleWire.Decimal(chunk.Length));}
        if(next is not null&&(chunks.Length!=256||next!=previous))throw new InvalidDataException("Stream ACK continuation changed.");
        var received=SyncProtocolReader.Integer64(body.GetProperty("receivedSizeBytes"));if(received>size||received<acknowledged)throw new InvalidDataException("Stream ACK byte count changed.");
        var hash=body.GetProperty("finalSha256").GetString();if(hash is not null&&!LocalCycleWire.Hash(hash))throw new InvalidDataException("Stream final hash invalid.");
        var publication=body.GetProperty("publication");
        if(hash is not null&&(received!=size||pending.Length!=0)||state is "SEALED" or "PUBLISHING" or "PUBLISHED"&&hash is null||state=="RECEIVING"&&hash is not null)throw new InvalidDataException("Stream seal/state is inconsistent.");
        if(state=="PUBLISHED"&&publication.ValueKind!=JsonValueKind.Object||publication.ValueKind!=JsonValueKind.Null&&hash is null)throw new InvalidDataException("Stream publication has no final seal.");
        if(publication.ValueKind!=JsonValueKind.Null&&(publication.GetProperty("session").GetProperty("phase").GetString()=="published")!=(state=="PUBLISHED"))throw new InvalidDataException("Stream/catalog publication states disagree.");
        return new(state,received,chunks,pending.FirstOrDefault(),hash,publication.ValueKind==JsonValueKind.Null?null:publication.Clone(),body.GetRawText())
            {Parallel=true,AfterIndex=after,NextIndex=next,PendingChunks=pending};
    }
}
