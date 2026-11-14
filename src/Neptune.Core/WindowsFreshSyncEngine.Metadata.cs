using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;
public sealed partial class WindowsFreshSyncEngine
{
    private async Task<bool> TryMetadataWork(LocalCycleWork work,CancellationToken ct)
    {
        var seal=await _session.ReadMetadataSealAsync(ScopeId,work.OperationId,ct);
        if(seal is null)
        {
            if(_remote is not IWindowsCycleExtensions remote||!remote.Supports("namespace-mutations-v1"))return false;
            var boundary=await _session.ReadMetadataBoundaryAsync(ScopeId,work,ct);if(boundary is null)return false;
            var target=await _session.ReadCycleTargetAsync(ScopeId,work.Entry,ct);var baseline=JsonSerializer.Deserialize<LocalCycleBaseline>(work.Entry.BaselineJson!,LocalCycleWire.Json)!;
            if(target.Target!=baseline.Description)return false;
            await _resources.Reader.WaitAsync(ct);_resources.StartReader();
            try{seal=_capture.CaptureMetadata(work,_resources.PathFor(work.OperationId),boundary.Value);}finally{_resources.EndReader();_resources.Reader.Release();}
            if(seal is null)return false;await _session.SaveMetadataSealAsync(ScopeId,work,seal,ct);
        }
        if(work.Phase==LocalCycleWorkPhase.Queued)
        {await _session.SealCycleWorkAsync(ScopeId,work.OperationId,new(seal.Seal.Path,seal.Seal.Size,seal.Seal.Sha256,seal.Seal.Proof),_capture.RootProof,ct);work=await _session.ReadCycleWorkAsync(ScopeId,work.OperationId,ct);}
        var original=JsonSerializer.Deserialize<LocalCycleBaseline>(work.Entry.BaselineJson!,LocalCycleWire.Json)!;
        if(!await TryContentMutation(work,original.Description.ParentId!,original.Description,ct))
        {await _session.FinishCycleWorkAsync(ScopeId,work.OperationId,LocalCycleWorkPhase.Conflict,null,"Metadata publication was refused; refresh the conflict before a new byte capture.",ct);return true;}
        await PostCommit(work,WindowsCycleStage.FilePublished,false,ct);return true;
    }
}
