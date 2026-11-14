using System.Text.Json;

namespace Neptune.Core.Sync.Protocol;

public sealed record SyncStreamInput(string OperationId,string Generation,string WitnessSha256,JsonElement Input);
public sealed record SyncStreamChunk(int Index,string Offset,string Length,string Sha256);
public sealed record SyncStreamStatus(string State,long Received,IReadOnlyList<SyncStreamChunk> Chunks,SyncStreamChunk? Pending,string? FinalSha256,JsonElement? Publication,string RawJson)
{
    public bool Parallel {get;init;}
    public int AfterIndex {get;init;}=-1;
    public int? NextIndex {get;init;}
    public IReadOnlyList<SyncStreamChunk> PendingChunks {get;init;}=[];
}
public interface IWindowsStreamRemote
{
    Task<SyncStreamStatus> StreamCommandAsync(string command,SyncStreamInput original,string? wholeHash,CancellationToken ct);
    Task<SyncStreamStatus> StreamChunkAsync(SyncStreamInput original,SyncStreamChunk chunk,ReadOnlyMemory<byte> bytes,CancellationToken ct);
    Task<SyncStreamStatus> StreamStatusPageAsync(SyncStreamInput original,int afterIndex,CancellationToken ct)=>throw new NotSupportedException("Paged stream capability unavailable.");
}
