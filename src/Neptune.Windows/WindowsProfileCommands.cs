using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;

namespace Neptune.Windows;

// Offline profile operations run under the same per-profile application mutex.
// No command writes the source tree or invents a new root/operation identity.
internal static partial class WindowsProfileCommands
{
    internal sealed record DatabaseInspection(string Database,IReadOnlyDictionary<string,long> Formats,long ActiveOriginals,long UnknownOriginals,long DirtyEntries,long Revision,string EvidenceSha256);
    internal sealed record MigrationPlan(string Schema,string Profile,string Digest,DatabaseInspection[] Databases);
    internal sealed record MigrationProgress(string Schema,string Profile,string Digest,string Backup,string[] Completed);
    internal sealed record BackupFile(string Database,string Sha256);
    internal sealed record BackupManifest(string Schema,string Profile,DateTimeOffset CreatedAt,DatabaseInspection[] Databases,BackupFile[] Files);
    internal sealed record ProfileBindingPlan(string Schema,string Profile,string Database,ClientBindingPlan Plan,MigrationPlan Inventory);
    private sealed class DatabaseOwners : IDisposable
    {
        private readonly List<FileStream> _locks=[];
        internal DatabaseOwners(IEnumerable<string> databases)
        {try{foreach(var database in databases){var file=database+".local-source-owner.lock";WindowsGuardedCapabilities.RequirePlainAncestors(file);_locks.Add(new(file,FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None));}}catch{Dispose();throw;}}
        public void Dispose(){foreach(var file in _locks)file.Dispose();_locks.Clear();}
    }
    internal static bool IsRequested(string[] args)=>args.Contains("--profile-command",StringComparer.Ordinal);
    internal static string Required(string[] args,string name)
    {var i=Array.IndexOf(args,name);return i>=0&&i+1<args.Length&&!string.IsNullOrWhiteSpace(args[i+1])?args[i+1]:throw new ArgumentException("Missing "+name);}
    private static string[] Databases(WindowsProfileContext profile)
    {
        if(!Directory.Exists(profile.StateDirectory))return [];
        WindowsGuardedCapabilities.RequirePlainAncestors(profile.StateDirectory);
        var databases=new List<string>();var main=Path.Combine(profile.StateDirectory,"neptune.db");if(File.Exists(main))databases.Add(main);
        var root=Path.Combine(profile.StateDirectory,"guarded-sync");
        if(Directory.Exists(root))foreach(var ns in Directory.EnumerateDirectories(root))
        {
            WindowsGuardedCapabilities.RequirePlainAncestors(ns);
            foreach(var mapping in Directory.EnumerateDirectories(ns))
            {
                WindowsGuardedCapabilities.RequirePlainAncestors(mapping);var db=Path.Combine(mapping,"cycle.db");if(File.Exists(db))databases.Add(db);
                if(databases.Count>257)throw new InvalidDataException("Profile database inventory exceeds limit.");
            }
        }
        return databases.Order(StringComparer.Ordinal).ToArray();
    }
    private static SqliteConnection Connection(string path,bool write=false)=>new(new SqliteConnectionStringBuilder
    {DataSource=NeptuneStateStore.NativeDatabasePath(path),Mode=write?SqliteOpenMode.ReadWrite:SqliteOpenMode.ReadOnly,Pooling=false,Cache=SqliteCacheMode.Private}.ToString());
    internal static async Task<DatabaseInspection[]> InspectAsync(WindowsProfileContext profile,CancellationToken ct=default)
    {
        var results=new List<DatabaseInspection>();
        foreach(var file in Databases(profile))
        {
            await using var db=Connection(file);await db.OpenAsync(ct);using var transaction=db.BeginTransaction(deferred:true);
            async Task<long> Scalar(string sql){await using var q=db.CreateCommand();q.Transaction=transaction;q.CommandText=sql;return Convert.ToInt64(await q.ExecuteScalarAsync(ct));}
            var tables=new HashSet<string>(StringComparer.Ordinal);
            await using(var q=db.CreateCommand()){q.Transaction=transaction;q.CommandText="SELECT name FROM sqlite_master WHERE type='table'";await using var r=await q.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct))tables.Add(r.GetString(0));}
            var formats=new SortedDictionary<string,long>(StringComparer.Ordinal);
            foreach(var table in new[]{"sync_state_format","local_source_format","local_namespace_format","local_projection_format","local_coherence_format","local_cycle_format","local_parent_reconcile_format","client_event_format","client_evolution_format"})
                if(tables.Contains(table))formats.Add(table,await Scalar("SELECT version FROM "+table+" WHERE singleton=1"));
            var active=tables.Contains("local_cycle_work")?await Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE active=1"):0;
            var unknown=tables.Contains("local_cycle_work")?await Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE active=1 AND phase=4"):0;
            if(tables.Contains("client_namespace_operations")){active+=await Scalar("SELECT COUNT(*) FROM client_namespace_operations WHERE state NOT IN('PUBLISHED','NOT_PUBLISHED','CANCELLED')");unknown+=await Scalar("SELECT COUNT(*) FROM client_namespace_operations WHERE state NOT IN('QUEUED','PUBLISHED','NOT_PUBLISHED','CANCELLED')");}
            if(tables.Contains("client_stream_operations"))unknown+=await Scalar("SELECT COUNT(*) FROM client_stream_operations WHERE COALESCE(json_extract(status_json,'$.state'),'UNKNOWN')<>'PUBLISHED' AND COALESCE(json_extract(status_json,'$.publication.session.phase'),'')<>'not_published'");
            results.Add(new(Path.GetRelativePath(profile.StateDirectory,file),formats,active,unknown,
                tables.Contains("local_cycle_files")?await Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE phase<>4"):0,
                tables.Contains("local_source_revision")?await Scalar("SELECT revision FROM local_source_revision WHERE singleton=1"):0,await EvidenceHash(db,transaction,tables,ct)));
        }
        return results.ToArray();
    }
    private static async Task<string> EvidenceHash(SqliteConnection db,SqliteTransaction transaction,HashSet<string> tables,CancellationToken ct)
    {
        using var hash=IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        void Add(string value){var bytes=System.Text.Encoding.UTF8.GetBytes(value);var size=new byte[4];System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(size,bytes.Length);hash.AppendData(size);hash.AppendData(bytes);}
        // Original immutable identities, bytes/proofs, cursor/binding rows and
        // current baselines. No values leave the process; only a digest does.
        foreach(var table in new[]{"local_cycle_work","local_cycle_scopes","local_cycle_files","local_source_roots","client_namespace_operations","client_stream_operations","client_metadata_seals","client_mapping_policies","client_namespace_bindings","client_policy_changes","client_delete_plans","client_binding_migrations","client_operation_schedule"})
        {
            if(!tables.Contains(table))continue;
            var columns=new List<string>();var keys=new SortedDictionary<int,string>();await using(var schema=db.CreateCommand()){schema.Transaction=transaction;schema.CommandText="PRAGMA table_info("+table+")";await using var r=await schema.ExecuteReaderAsync(ct);while(await r.ReadAsync(ct)){var name=r.GetString(1);if(r.GetInt32(5)>0)keys.Add(r.GetInt32(5),name);if(name is not("bytes" or "confirmed_files" or "confirmed_bytes" or "work_retire_after" or "retire_phase"))columns.Add(name);}}
            columns.Sort(StringComparer.Ordinal);var projection=string.Join(',',columns.Select(name=>'"'+name+'"'));
            await using var q=db.CreateCommand();q.Transaction=transaction;q.CommandText="SELECT "+projection+" FROM "+table+" ORDER BY "+string.Join(',',keys.Values.Select(name=>'"'+name+'"'));
            var first=true;await using var reader=await q.ExecuteReaderAsync(ct);while(await reader.ReadAsync(ct)){ct.ThrowIfCancellationRequested();if(first){Add(table);first=false;}for(var i=0;i<columns.Count;i++){Add(columns[i]);Add(reader.IsDBNull(i)?"null":reader.GetValue(i) is byte[] blob?"blob:"+Convert.ToBase64String(blob):"value:"+Convert.ToString(reader.GetValue(i),System.Globalization.CultureInfo.InvariantCulture)!);}}
        }
        return Convert.ToHexStringLower(hash.GetHashAndReset());
    }
    internal static async Task BackupAsync(WindowsProfileContext profile,string destination,CancellationToken ct=default)
    {
        var target=Path.GetFullPath(destination);var source=Path.TrimEndingDirectorySeparator(Path.GetFullPath(profile.StateDirectory));
        if(target.Equals(source,StringComparison.OrdinalIgnoreCase)||target.StartsWith(source+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))throw new InvalidDataException("Profile backup must be outside the active profile.");
        if(Directory.Exists(target))throw new IOException("Backup destination already exists.");
        Directory.CreateDirectory(target);WindowsGuardedCapabilities.RequirePlainAncestors(target);
        var databases=Databases(profile);using var owners=new DatabaseOwners(databases);
        var files=new List<BackupFile>();
        foreach(var file in databases)
        {
            var path=Path.Combine(target,Path.GetRelativePath(source,file));Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            await using var from=Connection(file);await from.OpenAsync(ct);
            await using var to=new SqliteConnection(new SqliteConnectionStringBuilder{DataSource=NeptuneStateStore.NativeDatabasePath(path),Mode=SqliteOpenMode.ReadWriteCreate,Pooling=false}.ToString());await to.OpenAsync(ct);
            ct.ThrowIfCancellationRequested();from.BackupDatabase(to);
            await to.CloseAsync();
            await using var copied=File.OpenRead(path);files.Add(new(Path.GetRelativePath(source,file),Convert.ToHexStringLower(await SHA256.HashDataAsync(copied,ct))));
        }
        // SQLite copies carry the WAL boundary. Copy the remaining encrypted
        // enrollment, settings, originals and immutable spools byte-for-byte.
        var skipped=new HashSet<string>(databases.Select(Path.GetFullPath).SelectMany(path=>new[]{path,path+"-wal",path+"-shm"}),StringComparer.OrdinalIgnoreCase);
        var pending=new Stack<string>();pending.Push(source);
        while(pending.Count>0)
        {
            var directory=pending.Pop();WindowsGuardedCapabilities.RequirePlainAncestors(directory);
            foreach(var file in Directory.EnumerateFileSystemEntries(directory))
            {
                ct.ThrowIfCancellationRequested();WindowsGuardedCapabilities.RequirePlainAncestors(file);
                if(Directory.Exists(file)){if(pending.Count>=8192)throw new InvalidDataException("Profile backup directory bound exceeded.");pending.Push(file);continue;}
                if(skipped.Contains(Path.GetFullPath(file))||file.EndsWith(".lock",StringComparison.OrdinalIgnoreCase)||file.EndsWith(".partial",StringComparison.OrdinalIgnoreCase))continue;
                if(files.Count>=8192)throw new InvalidDataException("Profile backup file bound exceeded.");
                var relative=Path.GetRelativePath(source,file);var output=Path.Combine(target,relative);Directory.CreateDirectory(Path.GetDirectoryName(output)!);
                await using(var input=new FileStream(file,FileMode.Open,FileAccess.Read,FileShare.Read))
                await using(var copy=new FileStream(output,FileMode.CreateNew,FileAccess.Write,FileShare.None))
                {await input.CopyToAsync(copy,ct);copy.Flush(true);}
                await using var copied=File.OpenRead(output);files.Add(new(relative,Convert.ToHexStringLower(await SHA256.HashDataAsync(copied,ct))));
            }
        }
        await WriteJsonAsync(Path.Combine(target,"backup.json"),new BackupManifest("neptune.profile-backup.v1",profile.ProfileId,DateTimeOffset.UtcNow,await InspectAsync(profile,ct),files.ToArray()),ct);
    }
    private static async Task VerifyMigrationBackup(string destination,MigrationPlan plan)
    {
        WindowsGuardedCapabilities.RequirePlainAncestors(destination);
        var manifestPath=Path.Combine(destination,"backup.json");WindowsGuardedCapabilities.RequirePlainAncestors(manifestPath);
        if(!File.Exists(manifestPath)||new FileInfo(manifestPath).Length>40*1024*1024)throw new InvalidDataException("Migration backup manifest is missing or unbounded.");
        var manifest=JsonSerializer.Deserialize<BackupManifest>(await File.ReadAllTextAsync(manifestPath))??throw new InvalidDataException("Migration backup manifest is invalid.");
        if(manifest.Schema!="neptune.profile-backup.v1"||manifest.Profile!=plan.Profile||Digest(manifest.Databases)!=plan.Digest||manifest.Files.Length>8192||manifest.Files.Select(file=>file.Database).Distinct(StringComparer.OrdinalIgnoreCase).Count()!=manifest.Files.Length||plan.Databases.Any(database=>!manifest.Files.Any(file=>file.Database==database.Database)))throw new InvalidDataException("Migration backup no longer matches the preview.");
        foreach(var backup in manifest.Files)
        {
            var path=Path.GetFullPath(Path.Combine(destination,backup.Database));
            if(!path.StartsWith(Path.TrimEndingDirectorySeparator(destination)+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase))throw new InvalidDataException("Backup path escaped its directory.");
            WindowsUpdateTransaction.RequireHash(path,backup.Sha256);
        }
    }
    internal static async Task CheckpointAsync(WindowsProfileContext profile,CancellationToken ct=default)
    {
        var databases=Databases(profile);using var owners=new DatabaseOwners(databases);
        foreach(var file in databases)
        {
            await using var db=Connection(file,true);await db.OpenAsync(ct);await using var q=db.CreateCommand();q.CommandText="PRAGMA wal_checkpoint(TRUNCATE)";
            await using var r=await q.ExecuteReaderAsync(ct);if(!await r.ReadAsync(ct)||r.GetInt64(0)!=0)throw new IOException("Profile checkpoint has an active owner; retained WAL was not discarded.");
        }
    }
    internal static async Task ValidateCompatibilityAsync(WindowsProfileContext profile,CancellationToken ct=default)
    {foreach(var file in Databases(profile))await new NeptuneStateStore(file).ValidateProfileCompatibilityAsync(ct);}
    internal static async Task<bool> UsesClientEvolutionAsync(WindowsProfileContext profile,CancellationToken ct=default)
    {foreach(var file in Databases(profile)){await using var db=Connection(file);await db.OpenAsync(ct);await using var q=db.CreateCommand();q.CommandText="SELECT 1 FROM sqlite_master WHERE name='client_evolution_format'";if(await q.ExecuteScalarAsync(ct)is not null)return true;}return false;}
    internal static async Task ExecuteAsync(WindowsProfileContext profile,string[] args)
    {
        var command=Required(args,"--profile-command");var output=Required(args,"--output");object result;
        switch(command)
        {
            case "journal-plan":
                result=await PreviewJournalRecoveryAsync(profile,Required(args,"--database"),Required(args,"--scope"));break;
            case "journal-recover":
                result=await ApplyJournalRecoveryAsync(profile,Required(args,"--plan"),Required(args,"--plan-sha256"),Required(args,"--destination"));break;
            case "recover-extensions":
                if(!OperatingSystem.IsWindows())throw new PlatformNotSupportedException("Historical recovery requires Windows enrollment credentials.");
                if(new WindowsPreferencesStore(profile.StateDirectory).Read().SynchronizationEnabled)throw new InvalidOperationException("Pause the profile before historical recovery.");
                var recoveryDatabase=Path.GetFullPath(Path.Combine(profile.StateDirectory,Required(args,"--database")));
                if(!Databases(profile).Contains(recoveryDatabase,StringComparer.OrdinalIgnoreCase))throw new InvalidDataException("Choose an existing profile cycle database.");
                var recoveryStore=new NeptuneStateStore(recoveryDatabase);var scopeIndex=Array.IndexOf(args,"--scope");
                var recoveryTarget=await recoveryStore.ReadClientRecoveryTargetAsync(scopeIndex<0?null:Required(args,"--scope"));
                var connection=new WindowsConnectionStore(profile.StateDirectory).Read()??throw new InvalidOperationException("Existing Windows enrollment is required.");
                using(var http=new System.Net.Http.HttpClient(new System.Net.Http.SocketsHttpHandler{AllowAutoRedirect=false}){Timeout=TimeSpan.FromSeconds(30)})
                {
                    var capability=await WindowsGuardedCapabilities.NegotiateAsync(http,recoveryTarget.Configuration.ServerOrigin,connection.SaturnToken,recoveryTarget.Configuration.DeviceId,recoveryTarget.Configuration.Binding.RootId,profile.StateDirectory,CancellationToken.None)??throw new InvalidOperationException("Original guarded capability unavailable.");
                    if(capability.Binding!=recoveryTarget.Configuration.Binding||capability.BackendIdentity!=recoveryTarget.Configuration.BackendIdentity)throw new InvalidOperationException("Recover against the original server binding before adoption.");
                    using var remote=new SaturnGuardedCycleTransport(recoveryTarget.Configuration,_=>Task.FromResult("Bearer "+connection.SaturnToken));remote.ConfigureExtensions(capability.ExtensionFeatures,capability.Namespace,capability.NamespaceRevision);
                    result=await recoveryStore.RecoverExtendedOriginalsAsync(recoveryTarget,remote);
                }
                break;
            case "binding-plan":
                var bindingDatabase=Path.GetFullPath(Path.Combine(profile.StateDirectory,Required(args,"--database")));
                if(!Databases(profile).Contains(bindingDatabase,StringComparer.OrdinalIgnoreCase))throw new InvalidDataException("Choose an existing profile cycle database.");
                var artifact=Required(args,"--server-adoption");WindowsGuardedCapabilities.RequirePlainAncestors(artifact);
                if(new FileInfo(artifact).Length>64*1024)throw new InvalidDataException("Server artifact exceeds limit.");
                var bindingInventory=await InspectAsync(profile);var bindingPlan=await new NeptuneStateStore(bindingDatabase).PreviewServerBindingAsync(await File.ReadAllTextAsync(artifact));
                result=new ProfileBindingPlan("neptune.profile-binding-plan.v1",profile.ProfileId,Path.GetRelativePath(profile.StateDirectory,bindingDatabase),bindingPlan,new("neptune.profile-migration.v1",profile.ProfileId,Digest(bindingInventory),bindingInventory));break;
            case "binding-adopt":
                var bindingPlanPath=Required(args,"--plan");WindowsGuardedCapabilities.RequirePlainAncestors(bindingPlanPath);if(new FileInfo(bindingPlanPath).Length>1024*1024)throw new InvalidDataException("Binding plan exceeds limit.");
                var reviewed=JsonSerializer.Deserialize<ProfileBindingPlan>(await File.ReadAllTextAsync(bindingPlanPath))??throw new InvalidDataException("Reviewed binding plan missing.");
                var bindingTarget=Path.GetFullPath(Path.Combine(profile.StateDirectory,reviewed.Database));
                if(reviewed.Schema!="neptune.profile-binding-plan.v1"||reviewed.Profile!=profile.ProfileId||reviewed.Inventory.Profile!=profile.ProfileId||reviewed.Inventory.Digest!=Digest(reviewed.Inventory.Databases)||!Databases(profile).Contains(bindingTarget,StringComparer.OrdinalIgnoreCase)||reviewed.Plan.Blockers.Count!=0)throw new InvalidDataException("Binding plan identity/blockers changed.");
                if(new WindowsPreferencesStore(profile.StateDirectory).Read().SynchronizationEnabled)throw new InvalidOperationException("Pause the profile before binding adoption.");
                var bindingBackup=Path.GetFullPath(Required(args,"--destination"));var bindingJournal=output+".journal";
                var bindingProgress=File.Exists(bindingJournal)?JsonSerializer.Deserialize<MigrationProgress>(await File.ReadAllTextAsync(bindingJournal)):null;
                if(bindingProgress is null)
                {
                    if(Digest(await InspectAsync(profile))!=reviewed.Inventory.Digest)throw new InvalidOperationException("Profile changed after binding preview.");
                    await BackupAsync(profile,bindingBackup);bindingProgress=new("neptune.binding-adoption-progress.v1",profile.ProfileId,reviewed.Plan.PlanId,bindingBackup,[]);await WriteJsonAsync(bindingJournal,bindingProgress);
                }
                if(bindingProgress.Schema!="neptune.binding-adoption-progress.v1"||bindingProgress.Profile!=profile.ProfileId||bindingProgress.Digest!=reviewed.Plan.PlanId||bindingProgress.Backup!=bindingBackup)throw new InvalidDataException("Binding adoption progress changed.");
                await VerifyMigrationBackup(bindingBackup,reviewed.Inventory);
                await new NeptuneStateStore(bindingTarget).ApplyServerBindingAsync(reviewed.Plan);
                await WriteJsonAsync(bindingJournal,bindingProgress with{Completed=[reviewed.Database]});
                result=new{schema="neptune.binding-adoption-result.v1",reviewed.Plan.PlanId,state="awaiting-https-capability-and-fresh-feed",automaticResume=false};break;
            case "pause":case "resume":
                if(command=="resume")foreach(var file in Databases(profile))await new NeptuneStateStore(file).ResumeClientAuthorizationAsync();
                var preferences=new WindowsPreferencesStore(profile.StateDirectory);preferences.Write(preferences.Read()with{SynchronizationEnabled=command=="resume"});
                result=new{schema="neptune.profile-control.v1",profile=profile.ProfileId,paused=command=="pause"};break;
            case "inspect":case "diagnostics":result=new{schema="neptune.profile-diagnostics.v1",profile=profile.ProfileId,databases=await InspectAsync(profile)};break;
            case "checkpoint":await CheckpointAsync(profile);result=new{status="checkpointed"};break;
            case "backup":await BackupAsync(profile,Required(args,"--destination"));result=new{status="backed_up"};break;
            case "migrate-plan":
                var inventory=await InspectAsync(profile);result=new MigrationPlan("neptune.profile-migration.v1",profile.ProfileId,Digest(inventory),inventory);break;
            case "adopt":
                var plan=JsonSerializer.Deserialize<MigrationPlan>(await File.ReadAllTextAsync(Required(args,"--plan")))??throw new InvalidDataException("Missing migration plan.");
                var destination=Path.GetFullPath(Required(args,"--destination"));var progressPath=output+".journal";
                var progress=File.Exists(progressPath)?JsonSerializer.Deserialize<MigrationProgress>(await File.ReadAllTextAsync(progressPath)):null;
                var current=await InspectAsync(profile);
                if(plan.Schema!="neptune.profile-migration.v1"||plan.Profile!=profile.ProfileId||plan.Digest!=Digest(plan.Databases))throw new InvalidDataException("Migration plan identity changed.");
                if(progress is null?plan.Digest!=Digest(current):progress.Schema!="neptune.profile-migration-progress.v1"||progress.Profile!=profile.ProfileId||progress.Digest!=plan.Digest||progress.Backup!=destination||current.Length!=plan.Databases.Length||current.Any(d=>!plan.Databases.Any(p=>p.Database==d.Database&&p.EvidenceSha256==d.EvidenceSha256)))throw new InvalidDataException("Profile originals changed since migration preview; create a new plan.");
                if(new WindowsPreferencesStore(profile.StateDirectory).Read().SynchronizationEnabled)throw new InvalidOperationException("Pause the profile before adoption.");
                if(plan.Databases.Any(d=>d.UnknownOriginals>0))throw new InvalidOperationException("Original UNKNOWN operations require recovery before profile adoption.");
                await ValidateCompatibilityAsync(profile);
                if(progress is null){await BackupAsync(profile,destination);progress=new("neptune.profile-migration-progress.v1",profile.ProfileId,plan.Digest,destination,[]);await WriteJsonAsync(progressPath,progress);}
                await VerifyMigrationBackup(destination,plan);
                if(progress.Completed.Distinct(StringComparer.Ordinal).Count()!=progress.Completed.Length||progress.Completed.Any(done=>!plan.Databases.Any(database=>database.Database==done)))throw new InvalidDataException("Migration progress contains an unknown database.");
                foreach(var file in Databases(profile))
                {var relative=Path.GetRelativePath(profile.StateDirectory,file);if(progress.Completed.Contains(relative,StringComparer.Ordinal))continue;await new NeptuneStateStore(file).AdoptClientProfileAsync();progress=progress with{Completed=progress.Completed.Append(relative).ToArray()};await WriteJsonAsync(progressPath,progress);}
                result=new{schema="neptune.profile-migration-progress.v1",plan.Digest,state="complete",serverActivation="separate-qualified-operation"};break;
            default:throw new ArgumentException("Unsupported profile command.");
        }
        await WriteJsonAsync(output,result);
    }
    private static string Digest(DatabaseInspection[] inventory)=>Convert.ToHexStringLower(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(inventory)));
    internal static async Task WriteJsonAsync(string path,object value,CancellationToken ct=default)
    {
        path=Path.GetFullPath(path);Directory.CreateDirectory(Path.GetDirectoryName(path)!);WindowsGuardedCapabilities.RequirePlainAncestors(path);
        var temp=path+"."+Guid.NewGuid().ToString("N")+".partial";
        try{await using(var file=new FileStream(temp,FileMode.CreateNew,FileAccess.Write,FileShare.None)){await JsonSerializer.SerializeAsync(file,value,cancellationToken:ct);file.Flush(true);}File.Move(temp,path,true);}
        finally{if(File.Exists(temp))File.Delete(temp);}
    }
}
