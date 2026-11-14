using System.Globalization;
using System.Text.Json;

namespace Neptune.Core;
public sealed partial class WindowsNativeCycleCapture
{
    internal MetadataOnlySeal? CaptureMetadata(LocalCycleWork work,string path,long appliedUsn)
    {
        var entry=work.Entry;var baseline=JsonSerializer.Deserialize<LocalCycleBaseline>(entry.BaselineJson!,LocalCycleWire.Json)!;
        var previous=JsonSerializer.Deserialize<LocalCycleCaptureEvidence>(baseline.CaptureProof,LocalCycleWire.Json);
        if(previous is null||previous.NodeId!=entry.SourceNodeId||previous.Created!=entry.Created||previous.RootFileId!=RootProof.Handle.FileId128||previous.RootCreated!=RootProof.Handle.CreationTime||previous.VolumeGuid!=RootProof.VolumeGuid||previous.Sha256!=baseline.Description.ContentSha256||previous.SizeBytes!=baseline.Description.SizeBytes)return null;
        var full=Resolve(entry.RelativePath);var parentPath=Path.GetDirectoryName(full)!;
        using var ancestry=WindowsBootstrapSourceCapture.HoldDirectoryPath(parentPath);using var root=Open(RootProof.RootPath,0x80,3);ValidateRoot(root);
        using var parent=Open(parentPath,0x80,3);WindowsBootstrapSourceCapture.RequireHandlePath(parent,parentPath);var p=ReadIdentity(parent,true);if(p.FileId128!=entry.ParentId)return null;
        using var handle=Open(full,0x80000000u|0x80u,1);WindowsBootstrapSourceCapture.RequireHandlePath(handle,full);RequireExactLeaf(handle,entry.RawName);var before=ReadIdentity(handle,false);var usn=ReadLastFileUsn(handle,before);
        if(before.FileId128!=entry.SourceNodeId||before.Created!=entry.Created||before.Links!=1||before.Volume64!=RootProof.Handle.VolumeSerial64||before.Volume32!=RootProof.Handle.LegacyVolumeSerial||before.Size.ToString(CultureInfo.InvariantCulture)!=previous.SizeBytes||usn>=appliedUsn||usn<LocalCycleWire.Decimal(previous.LastFileUsn))return null;
        var evidence=previous with{ScopeId=work.ScopeId,OperationId=work.OperationId,ParentId=entry.ParentId,RelativePath=entry.RelativePath,JournalGeneration=entry.Generation.Journal,WatcherGeneration=entry.Generation.Watcher,LastFileUsn=usn.ToString(CultureInfo.InvariantCulture),Nonce=Guid.NewGuid().ToString("N")};
        var proof=JsonSerializer.Serialize(evidence,LocalCycleWire.Json);ValidatePersisted(work,before.Size,previous.Sha256,proof,RootProof);
        ValidateRoot(root);if(before!=ReadIdentity(handle,false)||ReadLastFileUsn(handle,before)!=usn||p.Witness!=ReadIdentity(parent,true).Witness)return null;
        return new(new(path,before.Size,previous.Sha256,proof),DateTime.FromFileTimeUtc(before.Write).ToString("O",CultureInfo.InvariantCulture));
    }
}
