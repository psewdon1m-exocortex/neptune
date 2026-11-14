using Neptune.Core;

namespace Neptune.Windows;
public sealed record WindowsMappingReview(SyncDiagnosticSnapshot Diagnostics,MappingSyncPolicy Policy,NamespaceDeletePreview Deletes,IReadOnlyList<LocalParentReconciliationConflict> Conflicts);
public sealed partial class WindowsSyncService
{
    private async Task<T> WithMapping<T>(string mapping,Func<WindowsGuardedMappingSession,Task<T>> action,CancellationToken ct)
    {
        await _syncGate.WaitAsync(ct);try
        {ObjectDisposedException.ThrowIf(_disposed,this);if(!_guardedSessions.TryGetValue(mapping,out var session))throw new InvalidOperationException("Run guarded synchronization to obtain a current mapping observation first.");return await action(session);}
        finally{_syncGate.Release();}
    }
    public Task<WindowsMappingReview> ReviewMappingAsync(string mapping,CancellationToken ct=default)=>WithMapping(mapping,async s=>new WindowsMappingReview(s.Diagnostics,await s.ReadPolicyAsync(ct),await s.PreviewDeletesAsync(ct),await s.ReadConflictsAsync(ct)),ct);
    public Task<MappingPolicyPreview> PreviewPolicyAsync(string mapping,MappingSyncPolicy policy,CancellationToken ct=default)=>WithMapping(mapping,s=>s.PreviewPolicyAsync(policy,ct),ct);
    public Task ApplyPolicyAsync(string mapping,MappingSyncPolicy policy,string digest,CancellationToken ct=default)=>WithMapping(mapping,async s=>
    {
        var preview=await s.PreviewPolicyAsync(policy,ct);
        var current=await s.ReadPolicyAsync(ct);
        if(preview.Digest!=digest||preview.Collisions!=0||policy.Codec!=current.Codec)throw new InvalidOperationException("Policy review changed or requires a server codec migration.");
        var state=new NeptuneStateStore(Path.Combine(profile.StateDirectory,"neptune.db"));
        var selected=(await state.ListMappingsAsync(ct)).Single(m=>m.MappingId==mapping);
        FolderExclusions.Write(selected,policy.ExcludedPrefixes);
        await s.ApplyPolicyAsync(policy,digest,ct);MappingWorkAvailable?.Invoke(mapping);return true;
    },ct);
    public Task ApproveDeletesAsync(string mapping,string digest,CancellationToken ct=default)=>WithMapping(mapping,async s=>{await s.ApproveDeletesAsync(digest,ct);MappingWorkAvailable?.Invoke(mapping);return true;},ct);
}
