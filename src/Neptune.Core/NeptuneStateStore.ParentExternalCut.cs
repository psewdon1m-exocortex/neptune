using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;
internal sealed record ParentExternalClaims(string Scope,string Coherence,string Journal,long Anchor,long Cut,IReadOnlyList<WindowsOutsideHardlinkProof.Claim> Files);
public sealed partial class NeptuneStateStore
{
    private const string ExternalCutTable="local_parent_reconcile_external_cuts";
    private const string ExternalCutDdl="CREATE TABLE local_parent_reconcile_external_cuts(scope_id TEXT NOT NULL,coherence_id TEXT NOT NULL,journal TEXT NOT NULL,cut_usn INTEGER NOT NULL,witness TEXT NOT NULL,PRIMARY KEY(scope_id,cut_usn)) WITHOUT ROWID";
    private static Dictionary<string,string> ParentExternalCutSchema=>new(ParentOrphanCloseSchema,StringComparer.Ordinal){[ExternalCutTable]=ExternalCutDdl};
    private async Task EnsureParentExternalCut(LocalJournalAdmission admission,CancellationToken ct)
    {
        await EnsureParentReconcileAsync(admission,ct);if(await PreflightParentReconcileAsync(ct)>=5)return;
        await using var migration=await AcquireSyncMigrationLockAsync(ct);if(await PreflightParentReconcileAsync(ct)>=5)return;
        var reserve=NamespaceReserve(8);var copy=checked(new FileInfo(databasePath).Length+(File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0));CheckLocalDiskAdmission(admission,checked(copy+reserve));
        var temporary=databasePath+".parent-external-cut-"+Guid.NewGuid().ToString("N")+".partial";
        try
        {
            await CreateConsistentSyncDatabaseCopyAsync(databasePath,temporary,ct);var hash=await HashSyncBackupAsync(temporary,ct);var backup=Path.GetFullPath(databasePath)+".pre-local-parent-reconcile-v5-"+hash+".db";
            if(File.Exists(backup)){if(await HashSyncBackupAsync(backup,ct)!=hash)throw new InvalidDataException("External-cut backup changed.");File.Delete(temporary);}else File.Move(temporary,backup);
            var manifest=JsonSerializer.Serialize(new ParentReconcileBackup(Path.GetFullPath(databasePath),backup,hash,5),SyncStateJson);var pending=databasePath+".local-parent-reconcile-v5-backup.json."+Guid.NewGuid().ToString("N")+".partial";
            try{await File.WriteAllTextAsync(pending,manifest,ct);using(var f=new FileStream(pending,FileMode.Open,FileAccess.Write,FileShare.None))f.Flush(true);File.Move(pending,databasePath+".local-parent-reconcile-v5-backup.json",true);}finally{if(File.Exists(pending))File.Delete(pending);}
            await CheckLocalAdmissionAsync(admission,0,reserve,ct);await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);
            await ExecuteSyncAsync(c,t,ExternalCutDdl,ct);await ExecuteSyncAsync(c,t,"UPDATE local_parent_reconcile_format SET version=5 WHERE singleton=1 AND version=4",ct);await t.CommitAsync(ct);
        }
        finally{if(File.Exists(temporary))File.Delete(temporary);}
    }
    internal async Task<ParentExternalClaims?> ReadParentExternalClaims(string root,string owner,string scope,LocalJournalAdmission local,CancellationToken ct)
    {
        // A completed native anchor is sufficient to try a current outside
        // witness. Requiring a failed full-tree scan first defeats this proof.
        LocalJournalAdmission budget;
        await using(var db=await OpenCycleConnection(ct))using(var tx=db.BeginTransaction(deferred:true))
        {
            var cycle=await RequireParentCycle(db,tx,root,owner,scope,ct);
            var coherent=await ReadCoherenceAsync(db,tx,cycle.CoherenceId!,ct);
            if(cycle.Status!=0||coherent.State.Status!=LocalCoherenceStatus.Uncertain||coherent.State.Error!="HardlinkMembershipUnresolved")return null;
            budget=CycleBudget(local,cycle);
            await tx.CommitAsync(ct);
        }
        await EnsureParentReconcileAsync(budget,ct);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);
        return await ExternalClaims(c,t,root,owner,scope,ct);
    }
    private static async Task<ParentExternalClaims?> ExternalClaims(SqliteConnection c,SqliteTransaction t,string root,string owner,string scope,CancellationToken ct)
    {
        var s=await RequireParentCycle(c,t,root,owner,scope,ct);var source=await ReadLocalStateAsync(c,t,root,owner,ct);var coherence=await ReadCoherenceAsync(c,t,s.CoherenceId!,ct);
        if(s.Status!=0||coherence.State.Status!=LocalCoherenceStatus.Uncertain||coherence.State.Error!="HardlinkMembershipUnresolved"||coherence.State.AppliedUsn!=source.Cursor.NextReadUsn)return null;
        bool extended;await using(var q=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='local_parent_reconcile_external_cuts'"))extended=await q.ExecuteScalarAsync(ct)is not null;
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_parent_reconcile_plans WHERE scope_id=$scope AND status<3 LIMIT 1")){q.Parameters.AddWithValue("$scope",scope);if(await q.ExecuteScalarAsync(ct)is not null)return null;}
        // Only an already seeded, complete native anchor can be advanced.
        // Pending namespace plans and any other unresolved transition still
        // require their ordinary reconciliation; this cut cannot clear them.
        long anchor=coherence.State.CutUsn;
        await using(var q=SyncCommand(c,t,"SELECT max(cut_usn) FROM local_parent_reconcile_plans WHERE coherence_id=$coherence AND status=3")){q.Parameters.AddWithValue("$coherence",s.CoherenceId!);if(await q.ExecuteScalarAsync(ct)is long cut)anchor=Math.Max(anchor,cut);}
        if(extended)await using(var q=SyncCommand(c,t,"SELECT max(cut_usn) FROM local_parent_reconcile_external_cuts WHERE scope_id=$scope AND coherence_id=$coherence AND journal=$journal")){q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$coherence",s.CoherenceId!);q.Parameters.AddWithValue("$journal",source.Cursor.JournalIdHex);if(await q.ExecuteScalarAsync(ct)is long cut)anchor=Math.Max(anchor,cut);}
        if(anchor<=0||anchor>=coherence.State.AppliedUsn)return null;
        await using(var q=SyncCommand(c,t,"SELECT 1 FROM local_coherence_unresolved WHERE coherence_id=$coherence AND usn>=$anchor AND usn<$cut AND code NOT IN('HardlinkMembershipUnresolved','PriorMembershipUncertainty') LIMIT 1"))
        {q.Parameters.AddWithValue("$coherence",s.CoherenceId!);q.Parameters.AddWithValue("$anchor",anchor);q.Parameters.AddWithValue("$cut",coherence.State.AppliedUsn);if(await q.ExecuteScalarAsync(ct)is not null)return null;}
        var records=new List<LocalUsnRecord>();
        await using(var q=SyncCommand(c,t,"SELECT usn,payload,sha256 FROM local_coherence_unresolved WHERE coherence_id=$coherence AND usn>=$anchor AND usn<$cut AND code='HardlinkMembershipUnresolved' ORDER BY usn LIMIT 4097"))
        {
            q.Parameters.AddWithValue("$coherence",s.CoherenceId!);q.Parameters.AddWithValue("$anchor",anchor);q.Parameters.AddWithValue("$cut",coherence.State.AppliedUsn);
            await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct)){var raw=(byte[])r.GetValue(1);if(ProjectionHash(raw)!=r.GetString(2))throw new InvalidDataException("Hardlink history hash changed.");records.Add(ProjectionRecord(source.Cursor.JournalIdHex,r.GetInt64(0),source.Cursor.NextReadUsn,raw));}
        }
        if(records.Count is 0 or >4096)return null;
        var p=new LocalParentReconciliation("external-current-proof",scope,s.CoherenceId!,LocalParentReconciliationStatus.Scanning,anchor,coherence.State.AppliedUsn,0,0,null);
        var files=new Dictionary<(ushort,string),WindowsOutsideHardlinkProof.Claim>();
        foreach(var record in records)
        {
            if((record.Reason&0x10000)==0||await KnownParentCloseIdentity(c,t,p,record,ct))return null;
            files[(record.MajorVersion,record.FileIdHex)]=new(record.MajorVersion,record.FileIdHex,record.Usn);if(files.Count>128)return null;
        }
        // Every event after the first hardlink uncertainty is retained here.
        // Find each identity's LAST record, not merely its last hardlink event.
        // SQL only narrows the candidate bytes; the native parser and digest
        // still validate the complete envelope and identity before use.
        foreach(var key in files.Keys.ToArray())
        {
            var claim=files[key];
            await using var q=SyncCommand(c,t,"SELECT usn,payload,sha256 FROM local_coherence_unresolved WHERE coherence_id=$coherence AND usn>=$start AND usn<$cut AND substr(payload,1,4)=X'4e55534e' AND lower(hex(substr(payload,17,$width)))=$wire ORDER BY usn DESC LIMIT 1");
            q.Parameters.AddWithValue("$coherence",s.CoherenceId!);q.Parameters.AddWithValue("$start",claim.LastEventUsn);q.Parameters.AddWithValue("$cut",coherence.State.AppliedUsn);q.Parameters.AddWithValue("$width",claim.Major==3?16:8);q.Parameters.AddWithValue("$wire",claim.Wire);
            await using var r=await q.ExecuteReaderAsync(ct);
            if(await r.ReadAsync(ct))
            {
                var raw=(byte[])r.GetValue(1);if(ProjectionHash(raw)!=r.GetString(2))throw new InvalidDataException("Hardlink terminal history hash changed.");
                var last=ProjectionRecord(source.Cursor.JournalIdHex,r.GetInt64(0),source.Cursor.NextReadUsn,raw);
                if(last.MajorVersion!=claim.Major||last.FileIdHex!=claim.Wire)return null;
                files[key]=claim with{LastEventUsn=last.Usn,ClosedDeleted=(last.Reason&0x80000300)==0x80000200};
            }
        }
        return new(scope,s.CoherenceId!,source.Cursor.JournalIdHex,anchor,coherence.State.AppliedUsn,files.Values.ToArray());
    }
    internal async Task<bool> InstallParentExternalCut(string root,string owner,ParentExternalClaims initial,WindowsOutsideHardlinkProof proof,LocalJournalAdmission local,CancellationToken ct)
    {
        LocalCycleScope s;await using(var c=await OpenCycleConnection(ct))using(var t=c.BeginTransaction(deferred:true))s=await RequireParentCycle(c,t,root,owner,initial.Scope,ct);
        await EnsureParentExternalCut(CycleBudget(local,s),ct);await ParentAdmission(root,owner,initial.Scope,local,4,65536,ct);
        await using var db=await OpenCycleConnection(ct);using var tx=db.BeginTransaction(deferred:false);var before=await NamespaceChanges(db,tx,ct);
        var current=await ExternalClaims(db,tx,root,owner,initial.Scope,ct);
        if(current is null||current.Coherence!=initial.Coherence||current.Journal!=initial.Journal||current.Anchor!=initial.Anchor||current.Cut<initial.Cut||current.Files.Any(x=>!proof.Covers(x)))return false;
        // Validate AFTER the retained cut, while the root is still natively
        // pinned. No historical disposition, baseline or original is rewritten.
        proof.Validate(ct);var receipt=JsonSerializer.Serialize(new{schema="neptune.current-external-hardlink-cut.v1",current.Anchor,current.Cut,current.Journal,evidence=proof.Evidence},SyncStateJson);
        if(Encoding.UTF8.GetByteCount(receipt)>65536)throw new InvalidDataException("Unbounded native witness.");
        await using(var q=SyncCommand(db,tx,"INSERT INTO local_parent_reconcile_external_cuts VALUES($scope,$coherence,$journal,$cut,$witness);UPDATE local_coherence_roots SET status=1,error=NULL WHERE coherence_id=$coherence AND status=2 AND applied_usn=$cut;UPDATE local_parent_reconcile_format SET bytes=bytes+$bytes WHERE singleton=1"))
        {q.Parameters.AddWithValue("$scope",initial.Scope);q.Parameters.AddWithValue("$coherence",current.Coherence);q.Parameters.AddWithValue("$journal",current.Journal);q.Parameters.AddWithValue("$cut",current.Cut);q.Parameters.AddWithValue("$witness",receipt);q.Parameters.AddWithValue("$bytes",Encoding.UTF8.GetByteCount(receipt)+256);await q.ExecuteNonQueryAsync(ct);}
        await CommitParent(db,tx,before,ct);return true;
    }
    public Task<string> CreateParentExternalCutRollbackCopyAsync(string destination,CancellationToken cancellationToken=default)=>CreateParentRollbackCopy(destination,5,cancellationToken);
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<ParentExternalClaims?> ReadParentExternalClaimsAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.ReadParentExternalClaims(_root,_owner,scope,_admission,ct),ct);
    internal Task<bool> InstallParentExternalCutAsync(ParentExternalClaims claims,WindowsOutsideHardlinkProof proof,CancellationToken ct)=>SerialAsync(()=>_store.InstallParentExternalCut(_root,_owner,claims,proof,_admission,ct),ct);
}
