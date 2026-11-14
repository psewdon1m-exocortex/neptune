using Neptune.Core;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Windows;

internal sealed class WindowsMappingAnchorRemote(IWindowsCycleRemote remote, string anchorId, string rootId,
    string mappingName) : IWindowsCycleRemote, ISmallFileBatchRemote,IWindowsCycleExtensions,IWindowsStreamRemote
{
    public SyncRootBinding Binding => remote.Binding;
    private void Validate(SyncDescriptor value)
    {
        if (value.NodeId == anchorId && (value.ParentId != rootId || value.Name != mappingName
            || value.Type != "folder" || value.Availability != "AVAILABLE"))
            throw new InvalidDataException("The mapping's current anchor changed, moved or became unavailable.");
        if (value.ParentId == rootId && value.NodeId != anchorId
            && WebDavSyncClient.CanonicalPathKey(value.Name) == WebDavSyncClient.CanonicalPathKey(mappingName))
            throw new InvalidDataException("A different current node collides with the selected mapping.");
    }
    public Task<byte[]> StartSnapshotAsync(string id, CancellationToken ct) => remote.StartSnapshotAsync(id, ct);
    public Task<byte[]> StepSnapshotAsync(string id, CancellationToken ct) => remote.StepSnapshotAsync(id, ct);
    public Task<SyncSnapshotReady> ReadySnapshotAsync(string id, CancellationToken ct) => remote.ReadySnapshotAsync(id, ct);
    public async Task<SyncSnapshotEntriesPage> ReadEntriesAsync(SyncSnapshotReady ready, string cursor, string? after, CancellationToken ct)
    { var page = await remote.ReadEntriesAsync(ready, cursor, after, ct); foreach (var item in page.Items) Validate(item); return page; }
    public async Task<SyncFeedPage> ReadChangesAsync(string cursor, long after, CancellationToken ct)
    { var page = await remote.ReadChangesAsync(cursor, after, ct); foreach (var item in page.Items) Validate(item.Descriptor); return page; }
    public Task<CycleFolderOutcome> EnsureDirectoryAsync(CycleFolderInput original, CancellationToken ct) => remote.EnsureDirectoryAsync(original, ct);
    public Task<CycleUploadStatus> UploadAsync(string command, CycleUploadInput original, string? uploadId, string hash, CancellationToken ct)
        => remote.UploadAsync(command, original, uploadId, hash, ct);
    public Task<CycleUploadStatus> AppendAsync(CycleUploadInput original, string uploadId, string hash, long offset, ReadOnlyMemory<byte> bytes, CancellationToken ct)
        => remote.AppendAsync(original, uploadId, hash, offset, bytes, ct);
    public Task<SmallFileBatchCapability?> ReadSmallBatchCapabilitiesAsync(CancellationToken ct)
        =>remote is ISmallFileBatchRemote batch?batch.ReadSmallBatchCapabilitiesAsync(ct):Task.FromResult<SmallFileBatchCapability?>(null);
    public Task<SmallFileBatchCapability?> ReadLegacySmallBatchCapabilitiesAsync(CancellationToken ct)
        =>remote is ISmallFileBatchRemote batch?batch.ReadLegacySmallBatchCapabilitiesAsync(ct):Task.FromResult<SmallFileBatchCapability?>(null);
    private ISmallFileBatchRemote Batch=>remote as ISmallFileBatchRemote??throw new NotSupportedException("Batch transport unavailable.");
    private IWindowsCycleExtensions Extensions=>remote as IWindowsCycleExtensions??throw new NotSupportedException("Extensions unavailable.");
    public SyncNamespacePolicy? NamespacePolicy=>(remote as IWindowsCycleExtensions)?.NamespacePolicy;
    public int NamespaceRevision=>Extensions.NamespaceRevision;
    public Task<string> ApproveDeletePlanAsync(SyncDeletePlan original,CancellationToken ct)=>Extensions.ApproveDeletePlanAsync(original,ct);
    public bool Supports(string feature)=>(remote as IWindowsCycleExtensions)?.Supports(feature)==true;
    public Task<SyncMutationReceipt> MutateAsync(SyncMutationRequest request,bool inspect,CancellationToken ct)=>Extensions.MutateAsync(request,inspect,ct);
    public Task<SyncContentReference?> LookupContentAsync(long size,string hash,CancellationToken ct)=>Extensions.LookupContentAsync(size,hash,ct);
    public Task<SyncStreamStatus> StreamCommandAsync(string command,SyncStreamInput input,string? hash,CancellationToken ct)=>((IWindowsStreamRemote)remote).StreamCommandAsync(command,input,hash,ct);
    public Task<SyncStreamStatus> StreamStatusPageAsync(SyncStreamInput input,int afterIndex,CancellationToken ct)=>((IWindowsStreamRemote)remote).StreamStatusPageAsync(input,afterIndex,ct);
    public Task<SyncStreamStatus> StreamChunkAsync(SyncStreamInput input,SyncStreamChunk chunk,ReadOnlyMemory<byte> bytes,CancellationToken ct)=>((IWindowsStreamRemote)remote).StreamChunkAsync(input,chunk,bytes,ct);
    public async Task<SyncFeedPage> WaitForChangesAsync(string cursor,long after,CancellationToken ct)
    {var page=await Extensions.WaitForChangesAsync(cursor,after,ct);foreach(var item in page.Items)Validate(item.Descriptor);return page;}
    public Task<SmallFileBatchOutcome> PrepareSmallBatchAsync(SmallFileBatchPlan plan,CancellationToken ct)=>Batch.PrepareSmallBatchAsync(plan,ct);
    public Task<SmallFileBatchOutcome> ReadSmallBatchAsync(SmallFileBatchPlan plan,CancellationToken ct)=>Batch.ReadSmallBatchAsync(plan,ct);
    public Task<SmallFileBatchOutcome> SendSmallBatchAsync(SmallFileBatchPlan plan,Stream bytes,CancellationToken ct)=>Batch.SendSmallBatchAsync(plan,bytes,ct);
    public Task<SmallFileBatchOutcome> ExecuteSmallJobAsync(SmallFileBatchPlan plan,Stream bytes,CancellationToken ct)=>Batch.ExecuteSmallJobAsync(plan,bytes,ct);
}
