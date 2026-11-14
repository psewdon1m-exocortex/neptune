using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    internal async Task<bool> HasPendingReplacement(string root,string owner,string scope,CancellationToken ct)
    {
        if(await PreflightCycleAsync(ct)<5)return false;await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,scope,true,ct);await using var q=SyncCommand(c,t,"SELECT 1 FROM local_cycle_replacement_requests WHERE new_scope=$scope AND status<>3");q.Parameters.AddWithValue("$scope",scope);return await q.ExecuteScalarAsync(ct)is not null;
    }
    private static void ReplacementId(string id)
    {if(!Guid.TryParseExact(id,"N",out var parsed)||parsed.ToString("N")!=id)throw new ArgumentException("Replacement identity requires a canonical persisted UUID.");}
    private static async Task<bool> HasReplacementLineage(SqliteConnection c,SqliteTransaction t,string current,string previous,string root,string old,WindowsCycleConfiguration config,bool installed,CancellationToken ct)
    {
        await using(var present=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='local_cycle_replacement_requests'"))if(await present.ExecuteScalarAsync(ct)is null)return false;
        var unsuperseded=await CycleFormat(c,t,ct)>=6?" AND superseded_by IS NULL":"";
        await using var q=SyncCommand(c,t,"SELECT 1 FROM local_cycle_replacement_requests WHERE new_scope=$scope AND original_scope=$previous AND new_root=$root AND original_root=$old AND configuration=$configuration AND fingerprint=$hash"+(installed?" AND status=3":" AND status<4")+unsuperseded);q.Parameters.AddWithValue("$scope",current);q.Parameters.AddWithValue("$previous",previous);q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$old",old);var raw=JsonSerializer.Serialize(config,LocalCycleWire.Json);q.Parameters.AddWithValue("$configuration",raw);q.Parameters.AddWithValue("$hash",CycleHash(raw));return await q.ExecuteScalarAsync(ct)is not null;
    }
    private sealed record ReplacementOriginal(LocalCycleScope Scope,LocalSourceBinding Binding,string BindingJson,string Journal,long Cursor,long Gap,string Code,long Revision,long Owner,bool Valid);
    private static async Task<ReplacementOriginal> ReadReplacementOriginal(SqliteConnection c,SqliteTransaction t,string scope,CancellationToken ct)
    {
        var old=await CycleScope(c,t,scope,ct);await using var q=SyncCommand(c,t,"SELECT binding_json,journal_id,cursor,gap_epoch,gap_code,legacy_revision,owner_epoch,valid FROM local_source_roots WHERE root_key=$root");q.Parameters.AddWithValue("$root",old.Root);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))throw new InvalidDataException("Original source evidence is absent.");
        var raw=r.GetString(0);if(System.Text.Encoding.UTF8.GetByteCount(raw)>16*1024)throw new InvalidDataException("Original source binding is unbounded.");return new(old,JsonSerializer.Deserialize<LocalSourceBinding>(raw,SyncStateJson)!,raw,r.GetString(1),r.GetInt64(2),r.GetInt64(3),r.IsDBNull(4)?"":r.GetString(4),r.GetInt64(5),r.GetInt64(6),r.GetInt64(7)!=0);
    }
    private static async Task RequireReplacementOriginal(SqliteConnection c,SqliteTransaction t,ReplacementOriginal old,WindowsCycleConfiguration config,LocalNamespaceRootProof proof,CancellationToken ct)
    {
        LocalCycleWire.Configuration(config);ValidateLocalBinding(old.Binding);ValidateNamespaceProof(proof.Handle);var b=old.Binding;
        if(JsonSerializer.Serialize(config,LocalCycleWire.Json)!=JsonSerializer.Serialize(old.Scope.Configuration,LocalCycleWire.Json)
            ||old.Revision!=await LocalRevisionAsync(c,t,ct)||old.Scope.Revision!=old.Revision
            ||proof.RootPath!=b.SourceRoot||proof.VolumeGuid!=b.VolumeGuid||proof.Handle.FileId128!=b.RootFileId||proof.Handle.CreationTime!=b.RootIncarnation||proof.Handle.LegacyVolumeSerial.ToString("x8")!=b.VolumeSerial)
            throw new LocalSourceGapException("ReplacementIdentity","Explicit replacement cannot borrow another native incarnation, principal, authority, rules or legacy revision.");
        await RequireLocalMappingAsync(c,t,b,ct);
        if(await HasClientOriginals(c,t,old.Scope.ScopeId,true,ct))throw new LocalSourceGapException("ReplacementExtendedOriginalUnresolved","Recover the original namespace/stream outcome before replacing its journal binding; extended evidence is pinned.");
        if(old.Valid||old.Code!="JournalGap")throw new LocalSourceGapException("ReplacementCause","Only an already recorded typed JournalGap can be replaced; ordinary lifecycle uncertainty is preserved.");
    }
    private static async Task<LocalJournalReplacement> ReadReplacement(SqliteConnection c,SqliteTransaction t,string id,CancellationToken ct)
    {
        await using var q=SyncCommand(c,t,"SELECT original_scope,new_scope,status,original_journal,original_cursor,original_gap,journal,start_usn,new_binding,candidate_attempts,error FROM local_cycle_replacement_requests WHERE replacement_id=$id");q.Parameters.AddWithValue("$id",id);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct))throw new InvalidOperationException("Replacement request is absent.");var b=JsonSerializer.Deserialize<LocalSourceBinding>(r.GetString(8),SyncStateJson)!;
        return new(id,r.GetString(0),r.GetString(1),(LocalJournalReplacementStatus)r.GetInt32(2),r.GetString(3),r.GetInt64(4),r.GetString(5),r.GetString(6),r.GetInt64(7),b.SubscriptionGeneration,r.GetInt32(9),r.IsDBNull(10)?null:r.GetString(10));
    }
    // Called only by the native engine while its current root handles remain
    // owned. One lifetime OS lock guards the complete request/source/scope
    // handoff. The generic Rebound transition is deliberately not invoked.
    internal async Task<LocalJournalReplacementOpen> OpenJournalReplacement(JournalGapReplacementRequest request,WindowsCycleConfiguration config,
        LocalNamespaceRootProof proof,LocalJournalBounds bounds,WindowsCycleBudget budget,LocalJournalReadMode mode,CancellationToken ct)
    {
        ReplacementId(request.ReplacementId);ReplacementId(request.OriginalScopeId);UsnRecordParser.ValidateMode(mode);WindowsUsnJournalReader.ValidateBounds(bounds);ValidateCycleBudget(budget);
        await InitializeAsync(ct);var format=await PreflightCycleAsync(ct);if(format is not(3 or 4 or 5 or 6 or 7))throw new InvalidDataException("Existing recognized guarded cycle is required for replacement.");
        var admission=new LocalJournalAdmission(MaximumDatabaseBytes:budget.ProfileBytes,MinimumFreeDiskBytes:budget.MinimumFreeDiskBytes);
        var owned=new FileStream(Path.GetFullPath(databasePath)+".local-source-owner.lock",FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None);
        try
        {
            await using(var preflight=new SqliteConnection(ReadOnlySyncConnectionString(databasePath)))
            {await preflight.OpenAsync(ct);using var t=preflight.BeginTransaction(deferred:true);var old=await ReadReplacementOriginal(preflight,t,request.OriginalScopeId,ct);await RequireReplacementOriginal(preflight,t,old,config,proof,ct);await ReadReplacementSupersession(preflight,t,request,old,bounds,ct);await using var page=SyncCommand(preflight,t,"PRAGMA page_size");var size=Convert.ToInt64(await page.ExecuteScalarAsync(ct));ValidateLocalPageSize(size);System.Threading.Interlocked.Exchange(ref _localPageSize,size);}
            // Recognized Named-v1 profiles predate the read-mode column. The
            // explicit replacement owns the profile lock and has validated the
            // old gap/identity; upgrade that dependency before cycle-v5 writes.
            if(await PreflightLocalSourceAsync(ct)!=2)
            {CheckLocalDiskAdmission(admission,NamespaceReserve(64),includeMigrationBackup:true);await EnsureLocalSourceAsync(ct);}
            await EnsureCycleReplacementAsync(admission,ct);await CheckLocalAdmissionAsync(admission,16*1024,NamespaceReserve(16),ct);
            if(request.SupersedesReplacementId is not null)await EnsureCycleSupersessionAsync(admission,ct);
            await using var c=await OpenCycleConnection(ct);using var tx=c.BeginTransaction(deferred:false);var before=await NamespaceChanges(c,tx,ct);var original=await ReadReplacementOriginal(c,tx,request.OriginalScopeId,ct);await RequireReplacementOriginal(c,tx,original,config,proof,ct);
            var supersession=await ReadReplacementSupersession(c,tx,request,original,bounds,ct);
            string? saved;await using(var q=SyncCommand(c,tx,"SELECT new_binding FROM local_cycle_replacement_requests WHERE replacement_id=$id")){q.Parameters.AddWithValue("$id",request.ReplacementId);saved=await q.ExecuteScalarAsync(ct)as string;}
            var binding=saved is null?supersession?.Binding??original.Binding with{SubscriptionGeneration=checked(original.Binding.SubscriptionGeneration+1)}:JsonSerializer.Deserialize<LocalSourceBinding>(saved,SyncStateJson)!;
            if(supersession is not null&&binding!=supersession.Binding)throw new InvalidDataException("Supersession binding changed.");
            var newJson=JsonSerializer.Serialize(binding,SyncStateJson);var root=LocalHash(newJson);var token=Guid.NewGuid().ToString("N");var configuration=JsonSerializer.Serialize(config,LocalCycleWire.Json);var fingerprint=CycleHash(configuration);string scope;
            if(saved is not null)
            {
                await using var q=SyncCommand(c,tx,"SELECT new_scope FROM local_cycle_replacement_requests WHERE replacement_id=$id AND original_scope=$old AND original_root=$original AND original_binding=$binding AND new_root=$root AND configuration=$configuration AND fingerprint=$fingerprint AND original_journal=$journal AND original_cursor=$cursor AND original_gap_epoch=$gap AND revision=$revision AND original_owner=$owner");
                q.Parameters.AddWithValue("$id",request.ReplacementId);q.Parameters.AddWithValue("$old",request.OriginalScopeId);q.Parameters.AddWithValue("$original",original.Scope.Root);q.Parameters.AddWithValue("$binding",original.BindingJson);q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$configuration",configuration);q.Parameters.AddWithValue("$fingerprint",fingerprint);q.Parameters.AddWithValue("$journal",original.Journal);q.Parameters.AddWithValue("$cursor",original.Cursor);q.Parameters.AddWithValue("$gap",original.Gap);q.Parameters.AddWithValue("$revision",original.Revision);q.Parameters.AddWithValue("$owner",original.Owner);
                scope=await q.ExecuteScalarAsync(ct)as string??throw new InvalidDataException("Immutable replacement request changed.");
            }
            else
            {
                await using(var q=SyncCommand(c,tx,"SELECT 1 FROM local_cycle_replacement_requests WHERE mapping_id=$mapping AND status<>3 AND replacement_id<>$previous"+(await CycleFormat(c,tx,ct)>=6?" AND superseded_by IS NULL":""))){q.Parameters.AddWithValue("$mapping",binding.MappingId);q.Parameters.AddWithValue("$previous",supersession?.PreviousId??"");if(await q.ExecuteScalarAsync(ct)is not null)throw new LocalSourceGapException("ReplacementPending","Resume the original pending replacement UUID, do not allocate another.");}
                await using(var q=SyncCommand(c,tx,"SELECT COUNT(*) FROM(SELECT 1 FROM local_source_roots LIMIT 65)"))if(Convert.ToInt64(await q.ExecuteScalarAsync(ct))>=64)throw new LocalSourceGapException("SubscriptionCapacity","Replacement preserves pinned subscriptions; history capacity reached.");
                await using(var q=SyncCommand(c,tx,"SELECT COUNT(*) FROM(SELECT 1 FROM local_cycle_scopes LIMIT 17)"))if(Convert.ToInt64(await q.ExecuteScalarAsync(ct))>=16)throw new LocalSourceGapException("CycleCapacity","Replacement preserves pinned original scopes; capacity reached.");scope=Guid.NewGuid().ToString("N");
            }
            string? active;await using(var q=SyncCommand(c,tx,"SELECT root_key FROM local_source_active WHERE mapping_id=$mapping")){q.Parameters.AddWithValue("$mapping",binding.MappingId);active=await q.ExecuteScalarAsync(ct)as string;}
            if(active!=(supersession?.Root??original.Scope.Root)&&(saved is null||active!=root))throw new LocalSourceGapException("ReplacementSuperseded","Another explicit binding superseded this request; reopen its original UUID.");
            if(saved is not null)
            {
                await using var q=SyncCommand(c,tx,"SELECT journal_id,cursor,owner_epoch,valid,legacy_revision,read_mode FROM local_source_roots WHERE root_key=$root");q.Parameters.AddWithValue("$root",root);await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)||r.GetInt64(3)!=1||r.GetInt64(4)!=original.Revision||r.GetInt32(5)!=(int)mode)throw new LocalSourceGapException("ReplacementNewGap","Current replacement continuity was lost; original evidence retained, no new generation allocated.");WindowsUsnJournalReader.ValidateCursor(bounds,new(r.GetString(0),r.GetInt64(1),r.GetInt64(2),true));
            }
            await using(var q=SyncCommand(c,tx,"INSERT INTO local_source_roots(root_key,mapping_id,binding_json,volume_key,journal_id,cursor,owner_token,owner_epoch,valid,legacy_revision,read_mode) VALUES($root,$mapping,$binding,$volume,$journal,$cursor,$token,1,1,$revision,$mode) ON CONFLICT(root_key) DO UPDATE SET owner_token=$token,owner_epoch=owner_epoch+1;INSERT INTO local_source_active(mapping_id,root_key) VALUES($mapping,$root) ON CONFLICT(mapping_id) DO UPDATE SET root_key=$root"))
            {q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$mapping",binding.MappingId);q.Parameters.AddWithValue("$binding",newJson);q.Parameters.AddWithValue("$volume",LocalHash(binding.VolumeGuid+"/"+binding.VolumeSerial));q.Parameters.AddWithValue("$journal",bounds.JournalIdHex);q.Parameters.AddWithValue("$cursor",bounds.NextUsn);q.Parameters.AddWithValue("$token",token);q.Parameters.AddWithValue("$revision",original.Revision);q.Parameters.AddWithValue("$mode",(int)mode);await q.ExecuteNonQueryAsync(ct);}
            var state=await ReadLocalStateAsync(c,tx,root,token,ct);
            if(saved is null)
            {
                if(supersession is not null)
                {await using var previous=SyncCommand(c,tx,"UPDATE local_cycle_replacement_requests SET superseded_by=$next WHERE replacement_id=$previous AND superseded_by IS NULL AND status<>3");previous.Parameters.AddWithValue("$next",request.ReplacementId);previous.Parameters.AddWithValue("$previous",supersession.PreviousId);if(await previous.ExecuteNonQueryAsync(ct)!=1)throw new InvalidDataException("Supersession raced another explicit request.");}
                await using(var scopeInsert=SyncCommand(c,tx,"INSERT INTO local_cycle_scopes(scope_id,root_key,configuration,fingerprint,owner_epoch,legacy_revision,gap_epoch,max_profile,free_floor) VALUES($scope,$root,$configuration,$fingerprint,$owner,$revision,0,$cap,$floor);INSERT INTO local_cycle_active(root_key,scope_id) VALUES($root,$scope)"))
                {scopeInsert.Parameters.AddWithValue("$scope",scope);scopeInsert.Parameters.AddWithValue("$root",root);scopeInsert.Parameters.AddWithValue("$configuration",configuration);scopeInsert.Parameters.AddWithValue("$fingerprint",fingerprint);scopeInsert.Parameters.AddWithValue("$owner",state.Cursor.OwnerEpoch);scopeInsert.Parameters.AddWithValue("$revision",original.Revision);scopeInsert.Parameters.AddWithValue("$cap",budget.ProfileBytes);scopeInsert.Parameters.AddWithValue("$floor",budget.MinimumFreeDiskBytes);await scopeInsert.ExecuteNonQueryAsync(ct);}
                await using var q=SyncCommand(c,tx,"INSERT INTO local_cycle_replacement_requests(replacement_id,mapping_id,original_scope,new_scope,original_root,new_root,configuration,fingerprint,original_binding,new_binding,original_journal,original_cursor,original_gap,original_gap_epoch,revision,original_owner,new_owner,journal,start_usn) VALUES($id,$mapping,$old,$scope,$original,$root,$configuration,$fingerprint,$binding,$new,$oldjournal,$oldcursor,$code,$gap,$revision,$oldowner,$owner,$journal,$start);UPDATE local_cycle_format SET bytes=bytes+16384 WHERE singleton=1");
                q.Parameters.AddWithValue("$id",request.ReplacementId);q.Parameters.AddWithValue("$mapping",binding.MappingId);q.Parameters.AddWithValue("$old",request.OriginalScopeId);q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$original",original.Scope.Root);q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$configuration",configuration);q.Parameters.AddWithValue("$fingerprint",fingerprint);q.Parameters.AddWithValue("$binding",original.BindingJson);q.Parameters.AddWithValue("$new",newJson);q.Parameters.AddWithValue("$oldjournal",original.Journal);q.Parameters.AddWithValue("$oldcursor",original.Cursor);q.Parameters.AddWithValue("$code",original.Code);q.Parameters.AddWithValue("$gap",original.Gap);q.Parameters.AddWithValue("$revision",original.Revision);q.Parameters.AddWithValue("$oldowner",original.Owner);q.Parameters.AddWithValue("$owner",state.Cursor.OwnerEpoch);q.Parameters.AddWithValue("$journal",bounds.JournalIdHex);q.Parameters.AddWithValue("$start",bounds.NextUsn);await q.ExecuteNonQueryAsync(ct);
                if(supersession is not null)
                {await using var link=SyncCommand(c,tx,"UPDATE local_cycle_replacement_requests SET supersedes_id=$previous,replacement_gap_json=$gap WHERE replacement_id=$id");link.Parameters.AddWithValue("$previous",supersession.PreviousId);link.Parameters.AddWithValue("$gap",supersession.GapJson);link.Parameters.AddWithValue("$id",request.ReplacementId);await link.ExecuteNonQueryAsync(ct);}
            }
            else
            {
                var current=await CycleScope(c,tx,scope,ct);
                if(current.CoherenceId is null)
                {
                    // A crash between native allocation and recording the
                    // lifecycle hook is not evidence of an empty source. The
                    // active candidate must not be silently replaced on reopen.
                    await using var activeBuild=SyncCommand(c,tx,"SELECT 1 FROM local_namespace_active a JOIN local_namespace_candidates n ON n.candidate_id=a.candidate_id WHERE a.mapping_id=$mapping AND n.root_key=$root AND n.status<>2 LIMIT 1");activeBuild.Parameters.AddWithValue("$mapping",binding.MappingId);activeBuild.Parameters.AddWithValue("$root",root);if(await activeBuild.ExecuteScalarAsync(ct)is not null)throw new LocalSourceGapException("ReplacementBuildingRestart","Unbound native candidate requires explicit diagnosis; immutable request retained.");
                }
                if(current.CoherenceId is not null)
                {
                    var cohere=await ReadCoherenceAsync(c,tx,current.CoherenceId,ct);var candidate=await ReadNamespaceAsync(c,tx,cohere.State.CandidateId,ct);
                    var scoped=current.SeedComplete&&mode==LocalJournalReadMode.IdentityOnly&&cohere.State.Status==LocalCoherenceStatus.Uncertain&&cohere.State.Error is("ScopedNamespaceTransitionUnresolved" or "HardlinkMembershipUnresolved");
                    if((cohere.State.Status==LocalCoherenceStatus.Anchored||scoped)&&candidate.State.Status==LocalNamespaceStatus.EnumerationComplete
                        &&cohere.Root==root&&candidate.RootKey==root&&cohere.State.OwnerEpoch==current.OwnerEpoch&&candidate.State.OwnerEpoch==current.OwnerEpoch&&candidate.State.RootFileId128==binding.RootFileId&&candidate.Mapping==binding.MappingId&&candidate.Client==binding.ClientId&&cohere.State.JournalId==state.Cursor.JournalIdHex&&cohere.State.LegacyRevision==state.LegacyRevision&&candidate.State.LegacyRevision==state.LegacyRevision&&cohere.State.GapEpoch==state.GapEpoch&&cohere.State.AppliedUsn>=cohere.State.CutUsn&&cohere.State.AppliedUsn<=state.Cursor.NextReadUsn)
                    {
                        await using(var activeCandidate=SyncCommand(c,tx,"SELECT 1 FROM local_coherence_active a JOIN local_namespace_active n ON n.candidate_id=$candidate WHERE a.root_key=$root AND a.coherence_id=$cohere AND n.mapping_id=$mapping")){activeCandidate.Parameters.AddWithValue("$candidate",cohere.State.CandidateId);activeCandidate.Parameters.AddWithValue("$root",root);activeCandidate.Parameters.AddWithValue("$cohere",current.CoherenceId);activeCandidate.Parameters.AddWithValue("$mapping",binding.MappingId);if(await activeCandidate.ExecuteScalarAsync(ct)is null)throw new LocalSourceGapException("ReplacementCandidateSuperseded","Completed replacement candidate is not the active retained native cut.");}
                        await using var handoff=SyncCommand(c,tx,"UPDATE local_coherence_roots SET owner_epoch=$owner WHERE coherence_id=$cohere;UPDATE local_namespace_candidates SET owner_epoch=$owner WHERE candidate_id=$candidate AND root_created=$created");handoff.Parameters.AddWithValue("$owner",state.Cursor.OwnerEpoch);handoff.Parameters.AddWithValue("$cohere",current.CoherenceId);handoff.Parameters.AddWithValue("$candidate",cohere.State.CandidateId);handoff.Parameters.AddWithValue("$created",binding.RootIncarnation);if(await handoff.ExecuteNonQueryAsync(ct)!=2)throw new InvalidDataException("Replacement completed candidate incarnation changed.");
                        if(await HasParentModule(c,tx,ct))
                        {await using var parents=SyncCommand(c,tx,"UPDATE local_parent_reconcile_plans SET owner_epoch=$owner WHERE scope_id=$scope AND root_key=$root AND coherence_id=$cohere AND owner_epoch=$previous AND revision=$revision AND gap=$gap AND journal=$journal AND status<3");parents.Parameters.AddWithValue("$owner",state.Cursor.OwnerEpoch);parents.Parameters.AddWithValue("$scope",scope);parents.Parameters.AddWithValue("$root",root);parents.Parameters.AddWithValue("$cohere",current.CoherenceId);parents.Parameters.AddWithValue("$previous",current.OwnerEpoch);parents.Parameters.AddWithValue("$revision",state.LegacyRevision);parents.Parameters.AddWithValue("$gap",state.GapEpoch);parents.Parameters.AddWithValue("$journal",state.Cursor.JournalIdHex);await parents.ExecuteNonQueryAsync(ct);}
                    }
                    else if(candidate.State.Status!=LocalNamespaceStatus.Incomplete)throw new LocalSourceGapException("ReplacementBuildingRestart","Unqualified Building candidate cannot be silently reused; original request/IDs retained.");
                }
                await using var q=SyncCommand(c,tx,"UPDATE local_cycle_scopes SET owner_epoch=$owner WHERE scope_id=$scope;UPDATE local_cycle_replacement_requests SET new_owner=$owner WHERE replacement_id=$id");q.Parameters.AddWithValue("$scope",scope);q.Parameters.AddWithValue("$id",request.ReplacementId);q.Parameters.AddWithValue("$owner",state.Cursor.OwnerEpoch);await q.ExecuteNonQueryAsync(ct);
            }
            // The explicit request preserves the mapping, including its last
            // verified raw anchor ancestry. Carry that authority into new and
            // retained unbound scopes before any remote refresh can install.
            // This is a constraint on current R, never adoption of old B.
            string? originalAnchor,currentAnchor;
            await using(var q=SyncCommand(c,tx,"SELECT anchor_json FROM local_cycle_scopes WHERE scope_id=$scope")){CycleId(q,request.OriginalScopeId);originalAnchor=await q.ExecuteScalarAsync(ct)as string;q.Parameters["$scope"].Value=scope;currentAnchor=await q.ExecuteScalarAsync(ct)as string;}
            if(originalAnchor is not null)
            {
                if(currentAnchor is not null&&currentAnchor!=originalAnchor)throw new InvalidDataException("Replacement changed original mapping anchor/ancestry; explicit remapping required.");
                if(currentAnchor is null)
                {await using var q=SyncCommand(c,tx,"UPDATE local_cycle_scopes SET anchor_json=$anchor WHERE scope_id=$scope AND anchor_json IS NULL");CycleId(q,scope);q.Parameters.AddWithValue("$anchor",originalAnchor);if(await q.ExecuteNonQueryAsync(ct)!=1)throw new InvalidDataException("Replacement anchor changed during handoff.");await AddCycleBytes(c,tx,scope,System.Text.Encoding.UTF8.GetByteCount(originalAnchor),ct);}
            }
            var result=await ReadReplacement(c,tx,request.ReplacementId,ct);var currentScope=await CycleScope(c,tx,scope,ct);string? retained=null;
            if(currentScope.CoherenceId is not null)
            {var stateToResume=(await ReadCoherenceAsync(c,tx,currentScope.CoherenceId,ct)).State;if(stateToResume.Status==LocalCoherenceStatus.Anchored||currentScope.SeedComplete&&mode==LocalJournalReadMode.IdentityOnly&&stateToResume.Status==LocalCoherenceStatus.Uncertain&&stateToResume.Error is("ScopedNamespaceTransitionUnresolved" or "HardlinkMembershipUnresolved"))retained=currentScope.CoherenceId;}
            await CommitCycle(c,tx,before,ct);return new(await CreateOwnedJournalSessionAsync(owned,root,token,admission,ct),result,retained);
        }
        catch{await owned.DisposeAsync();throw;}
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<bool> HasPendingReplacementAsync(string scope,CancellationToken ct)=>SerialAsync(()=>_store.HasPendingReplacement(_root,_owner,scope,ct),ct);
}
