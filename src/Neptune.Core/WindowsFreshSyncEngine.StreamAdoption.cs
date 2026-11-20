using System.Globalization;
using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

public sealed partial class WindowsFreshSyncEngine
{
    private async Task<bool> TryAdoptVerifiedEqualStreamAsync(LocalCycleWork work,SyncDescriptor target,string path,CancellationToken ct)
    {
        // A replacement scan has no old local baseline. It still must not
        // prepare an overwrite to discover whether an existing file is equal.
        if(!_configuration.AllowVerifiedEqualAdoption||target.Type!="file"
            ||!long.TryParse(target.SizeBytes,NumberStyles.None,CultureInfo.InvariantCulture,out var expected)
            ||expected<0||target.ContentSha256 is null)return false;
        if(await _capture.MeasureFileAsync(work.Entry,ct)!=expected)return false;
        var seal=await _capture.CaptureStreamAsync(work,path,(witness,size,token)=>
        {
            if(size!=expected)throw new BootstrapCaptureDeferredException("Source size changed before verified stream adoption.");
            return Task.CompletedTask;
        },(index,bytes,token)=>
        {
            _coordinator.Diagnostics.StreamRead(bytes.Length,false);
            return Task.CompletedTask;
        },SourceReadSlot,ct);
        if(seal.Size!=expected||seal.Sha256!=target.ContentSha256)return false;
        var current=await _session.ReadCycleTargetAsync(ScopeId,work.Entry,ct);
        if(current.Target!=target)throw new CycleConflictException("Remote target changed during verified stream adoption.");
        // The native whole-file seal uses bounded read buffers, without a
        // payload spool, stream input, prepare request, or remote mutation.
        await _session.SealCycleWorkAsync(ScopeId,work.OperationId,seal,_capture.RootProof,ct);
        await _session.FinishCycleWorkAsync(ScopeId,work.OperationId,LocalCycleWorkPhase.AdoptedEqual,
            JsonSerializer.Serialize(target,LocalCycleWire.Json),null,ct);
        Interlocked.Increment(ref _capturedFiles);Interlocked.Add(ref _capturedBytes,seal.Size);
        await PostCommit(work,WindowsCycleStage.FileCaptured,true,ct);
        return true;
    }
}
