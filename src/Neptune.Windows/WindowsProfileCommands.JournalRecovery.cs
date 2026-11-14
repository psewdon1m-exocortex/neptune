using System.Security.Cryptography;
using System.Security.Principal;
using System.Text.Json;
using Neptune.Core;

namespace Neptune.Windows;

internal static partial class WindowsProfileCommands
{
    // Keep the existing ExecuteAsync entry point unambiguous for reflection callers.
    internal static async Task ExecuteJournalRecoveryWithCancellationAsync(
        WindowsProfileContext profile,string[] args,CancellationToken ct)
    {
        var command=Required(args,"--profile-command");
        var output=Required(args,"--output");
        if(command!="journal-recover")
            throw new ArgumentException("This operator accepts only journal-recover.",nameof(args));
        ct.ThrowIfCancellationRequested();
        var result=await ApplyJournalRecoveryAsync(profile,Required(args,"--plan"),
            Required(args,"--plan-sha256"),Required(args,"--destination"),ct);
        // Await Apply and its owned transport joins before serializing the result.
        await WriteJsonAsync(output,result,ct);
    }

    internal sealed record JournalRecoveryPlan(string Schema,string RequestId,string Sid,string Profile,string StateDirectory,
        string Database,ClientRecoveryTarget Target,string SourceIdentitySha256,long SubscriptionGeneration,MigrationPlan Inventory);
    private sealed record JournalRecoveryProgress(string Schema,string PlanSha256,string Backup,string Stage,string? CurrentScope);
    private static string JournalHash<T>(T value)=>Convert.ToHexStringLower(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(value)));
    private static string CurrentSid()
    {
        if(!OperatingSystem.IsWindows())throw new PlatformNotSupportedException("Journal recovery requires Windows.");
        using var identity=WindowsIdentity.GetCurrent();return identity.User?.Value??throw new InvalidOperationException("Windows user identity unavailable.");
    }
    private static string JournalDatabase(WindowsProfileContext profile,string relative)
    {
        var file=Path.GetFullPath(Path.Combine(profile.StateDirectory,relative));
        if(!Databases(profile).Select(Path.GetFullPath).Contains(file,StringComparer.OrdinalIgnoreCase)||Path.GetFileName(file)!="cycle.db")
            throw new InvalidDataException("Choose an existing guarded profile cycle database.");
        return file;
    }
    private static void RequirePaused(WindowsProfileContext profile)
    {if(new WindowsPreferencesStore(profile.StateDirectory).Read().SynchronizationEnabled)throw new InvalidOperationException("Pause the profile before journal recovery.");}
    private static async Task<long> OriginalGeneration(string file,string scope,CancellationToken ct)
    {
        await using var db=Connection(file);await db.OpenAsync(ct);await using var q=db.CreateCommand();
        q.CommandText="SELECT json_extract(r.binding_json,'$.subscriptionGeneration') FROM local_cycle_scopes s JOIN local_source_roots r ON r.root_key=s.root_key WHERE s.scope_id=$scope";q.Parameters.AddWithValue("$scope",scope);
        return await q.ExecuteScalarAsync(ct)is long generation&&generation>0?generation:throw new InvalidDataException("Original source generation absent.");
    }
    internal static async Task<bool> HasRecordedJournalGapAsync(string file,string scope,CancellationToken ct)
    {
        await using var db=Connection(file);await db.OpenAsync(ct);await using var query=db.CreateCommand();
        query.CommandText="SELECT EXISTS(SELECT 1 FROM local_source_roots r JOIN local_cycle_scopes s ON s.root_key=r.root_key WHERE s.scope_id=$scope AND r.valid=0 AND r.gap_code='JournalGap')";
        query.Parameters.AddWithValue("$scope",scope);return Convert.ToInt64(await query.ExecuteScalarAsync(ct))==1;
    }
    internal static async Task<JournalRecoveryPlan> PreviewJournalRecoveryAsync(WindowsProfileContext profile,string database,string scope,CancellationToken ct=default)
    {
        RequirePaused(profile);var file=JournalDatabase(profile,database);using var owner=new DatabaseOwners(Databases(profile));
        var target=await new NeptuneStateStore(file).ReadClientRecoveryTargetAsync(scope,ct);
        if(target.Configuration.ProfileId!=profile.ProfileId)throw new InvalidDataException("Original profile differs.");
        try{await using var valid=await WindowsGuardedSourceEligibility.OpenAsync(target.SourceRoot,file,target.Configuration,ct);throw new InvalidOperationException("Current source has no journal gap.");}
        catch(LocalSourceGapException gap)when(gap.Code=="JournalGap"){}
        await using var native=await WindowsNativeCycleCapture.OpenAsync(target.SourceRoot,ct);
        var inventory=await InspectAsync(profile,ct);
        return new("neptune.journal-recovery-plan.v1",Guid.NewGuid().ToString("N"),CurrentSid(),profile.ProfileId,Path.GetFullPath(profile.StateDirectory),
            Path.GetRelativePath(profile.StateDirectory,file),target,JournalHash(native.RootProof),await OriginalGeneration(file,scope,ct),
            new("neptune.profile-migration.v1",profile.ProfileId,Digest(inventory),inventory));
    }
    internal static void ValidateJournalPlan(WindowsProfileContext profile,JournalRecoveryPlan plan,string sid)
    {
        if(plan.Schema!="neptune.journal-recovery-plan.v1"||!Guid.TryParseExact(plan.RequestId,"N",out var request)||request.ToString("N")!=plan.RequestId
            ||plan.Sid!=sid||plan.Profile!=profile.ProfileId||plan.Target.Configuration.ProfileId!=profile.ProfileId
            ||plan.StateDirectory!=Path.GetFullPath(profile.StateDirectory)||plan.SubscriptionGeneration<1
            ||plan.Inventory.Profile!=profile.ProfileId||plan.Inventory.Digest!=Digest(plan.Inventory.Databases))
            throw new InvalidDataException("Reviewed journal plan identity or inventory changed.");
    }
    internal static async Task<object> ApplyJournalRecoveryAsync(WindowsProfileContext profile,string planPath,string planSha,string backup,CancellationToken ct=default)
    {
        if(!OperatingSystem.IsWindows())throw new PlatformNotSupportedException("Journal recovery requires Windows enrollment.");
        RequirePaused(profile);WindowsGuardedCapabilities.RequirePlainAncestors(planPath);
        if(new FileInfo(planPath).Length>4*1024*1024)throw new InvalidDataException("Journal plan exceeds limit.");
        WindowsUpdateTransaction.RequireHash(planPath,planSha);
        var plan=JsonSerializer.Deserialize<JournalRecoveryPlan>(await File.ReadAllTextAsync(planPath,ct))??throw new InvalidDataException("Journal plan absent.");
        ValidateJournalPlan(profile,plan,CurrentSid());var database=JournalDatabase(profile,plan.Database);
        using var lease=WindowsGuardedStateLease.Open(Path.GetDirectoryName(database)!);
        var store=new NeptuneStateStore(database);var original=await store.ReadClientRecoveryTargetAsync(plan.Target.ScopeId,ct);
        if(JournalHash(original)!=JournalHash(plan.Target))throw new InvalidDataException("Original configuration changed after preview.");
        await using var native=await WindowsNativeCycleCapture.OpenAsync(original.SourceRoot,ct);
        if(JournalHash(native.RootProof)!=plan.SourceIdentitySha256)throw new InvalidDataException("Native source incarnation changed after preview.");
        var connection=new WindowsConnectionStore(profile.StateDirectory).Read()??throw new InvalidOperationException("Existing Windows enrollment is required.");
        using var http=new System.Net.Http.HttpClient(new System.Net.Http.SocketsHttpHandler{AllowAutoRedirect=false}){Timeout=TimeSpan.FromSeconds(30)};
        var config=original.Configuration;
        var capability=await WindowsGuardedCapabilities.NegotiateAsync(http,config.ServerOrigin,connection.SaturnToken,config.DeviceId,config.Binding.RootId,profile.StateDirectory,ct)
            ??throw new InvalidOperationException("Current original capability unavailable.");
        if(capability.Binding!=config.Binding||capability.BackendIdentity!=config.BackendIdentity||capability.DeviceId!=config.DeviceId)
            throw new InvalidDataException("Recover against the original full server authority.");
        backup=Path.GetFullPath(backup);var journal=planPath+".recovery";
        var progress=File.Exists(journal)?JsonSerializer.Deserialize<JournalRecoveryProgress>(await File.ReadAllTextAsync(journal,ct)):null;
        if(progress is null)
        {
            if(Digest(await InspectAsync(profile,ct))!=plan.Inventory.Digest)throw new InvalidOperationException("Profile changed after preview.");
            // A completed backup may survive a process exit just before the
            // progress record. Reuse only its exact verified inventory and
            // every saved SHA; incomplete or unrelated destinations stay closed.
            if(Directory.Exists(backup))await VerifyMigrationBackup(backup,plan.Inventory);
            else await BackupAsync(profile,backup,ct);
            progress=new("neptune.journal-recovery-progress.v1",planSha,backup,"BACKED_UP",null);await WriteJsonAsync(journal,progress,ct);
        }
        if(progress.Schema!="neptune.journal-recovery-progress.v1"||progress.PlanSha256!=planSha||progress.Backup!=backup)
            throw new InvalidDataException("Resume the same reviewed recovery and backup.");
        await VerifyMigrationBackup(backup,plan.Inventory);lease.RequireCurrent();
        await using var transport=new SaturnGuardedCycleTransport(config,_=>Task.FromResult("Bearer "+connection.SaturnToken));
        transport.ConfigureExtensions(capability.ExtensionFeatures,capability.Namespace,capability.NamespaceRevision);
        var remote=new WindowsMappingAnchorRemote(transport,config.RemoteAnchorId??throw new InvalidDataException("Original anchor absent."),config.Binding.RootId,Path.GetFileName(original.SourceRoot.TrimEnd(Path.DirectorySeparatorChar)));
        var options=new WindowsCycleNativeOptions(LocalJournalReadMode.IdentityOnly,true);var spool=Path.Combine(Path.GetDirectoryName(database)!,"spool");
        // Let the normal native cursor validator persist its typed gap. Never
        // manufacture a gap, set a cursor, or discard originals in this command.
        await using(var db=Connection(database))
        {
            await db.OpenAsync(ct);await using var query=db.CreateCommand();query.CommandText="SELECT r.valid FROM local_source_roots r JOIN local_cycle_scopes s ON s.root_key=r.root_key WHERE s.scope_id=$scope";query.Parameters.AddWithValue("$scope",plan.Target.ScopeId);
            if(Convert.ToInt64(await query.ExecuteScalarAsync(ct))==1)
            {
                await db.CloseAsync();
                try{await using var opened=await WindowsFreshSyncEngine.ResumeOrOpenAsync(options,store,original.SourceRoot,spool,config,remote,capability.Budget,plan.SubscriptionGeneration,cancellationToken:ct);throw new InvalidOperationException("Native continuity unexpectedly valid; recovery refused.");}
                catch(LocalSourceGapException gap)when(gap.Code=="JournalGap"){}
                catch(LocalSourceGapException gap)when(gap.Code=="CycleResumeGap")
                {
                    // OpenLocalJournalSession has already persisted a native
                    // cursor gap before retained-cycle resume notices it. Only
                    // that exact recorded cause may enter reviewed recovery.
                    // Other revision/mapping/lifecycle uncertainty stays closed.
                    if(!await HasRecordedJournalGapAsync(database,plan.Target.ScopeId,ct))throw;
                }
            }
        }
        await using var replacement=await WindowsFreshSyncEngine.OpenJournalReplacementAsync(options,new(plan.RequestId,original.ScopeId),store,original.SourceRoot,spool,config,remote,capability.Budget,cancellationToken:ct);
        await WriteJsonAsync(journal,progress with{Stage="QUALIFYING",CurrentScope=replacement.ScopeId},ct);
        await replacement.QualifyJournalReplacementAsync(ct);lease.RequireCurrent();
        await replacement.RequireRetainedHistoryAsync(ct);
        await WriteJsonAsync(journal,progress with{Stage="INSTALLED_AWAITING_PUBLIC_SYNC",CurrentScope=replacement.ScopeId},ct);
        return new{schema="neptune.journal-recovery-result.v1",plan.RequestId,originalScope=original.ScopeId,currentScope=replacement.ScopeId,replacement.JournalReplacement,
            state="installed-awaiting-public-sync",automaticResume=false};
    }
}
