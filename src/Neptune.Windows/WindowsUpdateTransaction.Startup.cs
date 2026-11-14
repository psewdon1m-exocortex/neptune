using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Win32;

namespace Neptune.Windows;

internal static partial class WindowsUpdateTransaction
{
    private sealed record RecoveryEntry(string Target,string Profile,string HealthToken);
    private const string RecoveryRegistryPath=@"Software\Microsoft\Windows\CurrentVersion\Run";
    private static string LeasePath(string target)=>JournalPath(target)+".startup.lock";

    private static FileStream AcquireUpdateLease(string target)
    {
        var path=LeasePath(target);WindowsGuardedCapabilities.RequirePlainAncestors(path);
        return new FileStream(path,FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None);
    }

    // Kept for the complete GUI/profile-command lifetime, including async work.
    // The lock lives outside target, so swapping a portable directory preserves it.
    internal static FileStream? EnterStartup(string profile)
    {
        var target=Path.TrimEndingDirectorySeparator(Path.GetFullPath(AppContext.BaseDirectory));
        if(File.Exists(Path.Combine(target,"update-recovery-target.json")))
            throw new InvalidOperationException("This is a retained update helper. Start it with --recover-update --target <installation> --profile <profile>; profile writers are disabled here.");
        var path=LeasePath(target);WindowsGuardedCapabilities.RequirePlainAncestors(path);
        if(!File.Exists(path))using(new FileStream(path,FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.ReadWrite)){}
        var lease=new FileStream(path,FileMode.Open,FileAccess.Read,FileShare.Read);
        try
        {
            var journal=ReadJournal(target);
            if(journal is null)return lease;
            if(journal.Phase is "healthy" or "rolled-back"){UnregisterRecovery(journal);return lease;}
            var helper=journal.RecoveryHelper;
            // Old journals have no retained recovery helper. Create one from the
            // running payload before exiting; never try to swap our own image.
            if(helper is null)
            {
                helper=target+".recovery-"+Guid.NewGuid().ToString("N");
                CopyPayload(AppContext.BaseDirectory,helper);
                WriteRecoveryEntry(helper,journal);
            }
            var start=UpdateApplyMode.CreateRelaunchStartInfo(helper,profile);
            start.ArgumentList.Add("--recover-update");start.ArgumentList.Add("--target");start.ArgumentList.Add(target);
            start.ArgumentList.Add("--wait-pid");start.ArgumentList.Add(Environment.ProcessId.ToString(System.Globalization.CultureInfo.InvariantCulture));
            _=Process.Start(start)??throw new IOException("Update recovery helper did not start.");
            lease.Dispose();return null;
        }
        catch{lease.Dispose();throw;}
    }

    private static WindowsUpdateJournal? ReadJournal(string target)
    {
        var path=JournalPath(target);WindowsGuardedCapabilities.RequirePlainAncestors(path);
        if(!File.Exists(path))return null;
        if(new FileInfo(path).Length>64*1024)throw new InvalidDataException("Update journal exceeds limit.");
        var journal=JsonSerializer.Deserialize<WindowsUpdateJournal>(File.ReadAllText(path))??throw new InvalidDataException("Update journal is invalid.");
        Validate(journal,target);return journal;
    }

    private static WindowsUpdateJournal PrepareRecovery(WindowsUpdateJournal journal)
    {
        var helper=journal.Target+".recovery-"+journal.HealthToken;
        CopyPayload(AppContext.BaseDirectory,helper);
        WriteRecoveryEntry(helper,journal);
        return journal with{RecoveryHelper=helper,ProfileStateSha256=ProfileStateFingerprint(2),PreviousPayloadSha256=FingerprintTree(journal.Target,false),ProfileFingerprintVersion=2};
    }

    private static string RecoveryValue(WindowsUpdateJournal journal)=>"Neptune.Update."+journal.HealthToken;
    private static void WriteRecoveryEntry(string helper,WindowsUpdateJournal journal)=>WindowsProfileCommands.WriteJsonAsync(
        Path.Combine(helper,"update-recovery-target.json"),new RecoveryEntry(journal.Target,journal.Profile,journal.HealthToken)).GetAwaiter().GetResult();
    private static void RegisterRecovery(WindowsUpdateJournal journal)
    {
        if(!OperatingSystem.IsWindows())throw new PlatformNotSupportedException("Login recovery requires Windows.");
        // RunOnce can lose its entry before a crashed helper completes. Keep
        // our own Run entry until a durable terminal receipt, then remove it.
        // Target/profile live in the helper marker to keep the command bounded.
        var command=$"\"{Path.Combine(journal.RecoveryHelper!,"Neptune.Windows.exe")}\" --recover-update";
        if(command.Length>260)throw new InvalidDataException("The recovery helper path exceeds the Windows login command limit. Move this portable installation to a shorter path before updating.");
        using var key=Registry.CurrentUser.CreateSubKey(RecoveryRegistryPath,true);
        key.SetValue(RecoveryValue(journal),command);
        key.Flush();
    }
    private static void UnregisterRecovery(WindowsUpdateJournal journal)
    {
        if(!OperatingSystem.IsWindows())throw new PlatformNotSupportedException("Login recovery requires Windows.");
        using var key=Registry.CurrentUser.OpenSubKey(RecoveryRegistryPath,true);
        key?.DeleteValue(RecoveryValue(journal),false);
    }

    internal static void ResumeInterrupted(string[] args)
    {
        UpdateApplyMode.WaitForParent(args,required:false);
        var marker=Path.Combine(AppContext.BaseDirectory,"update-recovery-target.json");WindowsGuardedCapabilities.RequirePlainAncestors(marker);
        RecoveryEntry? entry=null;
        if(File.Exists(marker))
        {
            if(new FileInfo(marker).Length>64*1024)throw new InvalidDataException("Recovery entry exceeds limit.");
            entry=JsonSerializer.Deserialize<RecoveryEntry>(File.ReadAllText(marker))??throw new InvalidDataException("Recovery entry is invalid.");
        }
        var target=Path.TrimEndingDirectorySeparator(Path.GetFullPath(args.Contains("--target",StringComparer.Ordinal)?WindowsProfileCommands.Required(args,"--target"):entry?.Target??throw new InvalidDataException("Recovery target missing.")));
        var profile=args.Contains("--profile",StringComparer.Ordinal)?WindowsProfileCommands.Required(args,"--profile"):entry?.Profile??throw new InvalidDataException("Recovery profile missing.");
        profile=WindowsProfileContext.FromArguments(["--profile",profile]).ProfileId;
        using var gate=AcquireUpdateLease(target);
        var journal=ReadJournal(target)??throw new InvalidDataException("Update journal missing.");
        if(entry is not null&&(entry.HealthToken!=journal.HealthToken||!string.Equals(entry.Target,target,StringComparison.OrdinalIgnoreCase)))throw new InvalidDataException("Recovery entry belongs to a different update.");
        if(journal.RecoveryHelper is not null&&!string.Equals(Path.TrimEndingDirectorySeparator(Path.GetFullPath(AppContext.BaseDirectory)),journal.RecoveryHelper,StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("This recovery helper belongs to a different update. Use the RecoveryHelper path in the current journal.");
        // A previous helper may have died after the terminal journal write but
        // before removing its login entry. Cleanup needs no application restart.
        if(journal.Phase is "healthy" or "rolled-back"){UnregisterRecovery(journal);return;}
        RequireClosedProfiles(target);
        if(journal.Phase is not("healthy" or "rolled-back"))
        {
            // Prefer completing the read-only health check. In particular, an
            // old journal without a baseline cannot authorize automatic rollback.
            if(journal.Phase=="installed"&&File.Exists(Path.Combine(target,"Neptune.Windows.exe")))
            {
                try{RequireHealthy(journal,false);}
                catch{Recover(target);}
            }
            else Recover(target);
        }
        UnregisterRecovery(journal);gate.Dispose();
        Process.Start(UpdateApplyMode.CreateRelaunchStartInfo(target,profile));
    }

    private static void RequireUnchangedProfileState(WindowsUpdateJournal journal)
    {
        if(journal.ProfileStateSha256 is null||journal.ProfileStateSha256!=ProfileStateFingerprint(journal.ProfileFingerprintVersion))
            throw new InvalidOperationException("Automatic rollback is blocked: profile state changed or the update has no compatibility baseline. Both payloads were retained; complete recovery with a compatible Neptune version.");
    }

    private static void RequirePreviousPayload(WindowsUpdateJournal journal,string directory)
    {
        if(journal.PreviousPayloadSha256 is null||journal.PreviousPayloadSha256!=FingerprintTree(directory,false))
            throw new InvalidDataException("Retained rollback payload identity is unavailable or changed. Automatic rollback is blocked.");
    }

    private static string ProfileStateFingerprint(int version)
    {
        if(version is not(1 or 2))throw new InvalidDataException("Unknown update profile fingerprint version.");
        var root=Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),"Neptune","profiles");
        return FingerprintTree(root,true,normalizeEmptyWal:version==2);
    }

    private static string FingerprintTree(string root,bool profiles)
        =>FingerprintTree(root,profiles,normalizeEmptyWal:profiles);

    private static string FingerprintTree(string root,bool profiles,bool normalizeEmptyWal)
    {
        using var hash=IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        void Add(string value){var bytes=Encoding.UTF8.GetBytes(value);hash.AppendData(BitConverter.GetBytes(bytes.Length));hash.AppendData(bytes);}
        if(!Directory.Exists(root)){Add("absent");return Convert.ToHexStringLower(hash.GetHashAndReset());}
        WindowsGuardedCapabilities.RequirePlainAncestors(root);
        var pending=new Stack<string>();pending.Push(root);var count=0;
        while(pending.Count>0)
        {
            var directory=pending.Pop();Add("directory:"+Path.GetRelativePath(root,directory));
            foreach(var path in Directory.EnumerateFileSystemEntries(directory).Order(StringComparer.Ordinal))
            {
                WindowsGuardedCapabilities.RequirePlainAncestors(path);
                if(profiles&&directory==root&&Path.GetFileName(path)==".update-backups")continue;
                if(++count>100_000)throw new InvalidDataException("Update compatibility inventory exceeds bound.");
                if(Directory.Exists(path)){pending.Push(path);continue;}
                // SHM and lock handles carry process coordination, not profile data.
                if(profiles&&(path.EndsWith("-shm",StringComparison.OrdinalIgnoreCase)||path.EndsWith(".lock",StringComparison.OrdinalIgnoreCase)))continue;
                using var file=new FileStream(path,FileMode.Open,FileAccess.Read,FileShare.Read);
                // A read-only SQLite health probe may create an empty WAL.
                // V2 treats only a qualified empty sidecar like its absence;
                // every nonempty WAL remains part of the rollback fence.
                // The read handle excludes concurrent writers while checking length.
                if(profiles&&normalizeEmptyWal&&IsEmptyProfileWal(root,path,file))continue;
                Add("file:"+Path.GetRelativePath(root,path));
                hash.AppendData(SHA256.HashData(file));
            }
        }
        return Convert.ToHexStringLower(hash.GetHashAndReset());
    }

    private static bool IsEmptyProfileWal(string root,string path,FileStream file)
    {
        if(file.Length!=0)return false;
        var parts=Path.GetRelativePath(root,path).Split(Path.DirectorySeparatorChar);
        var main=parts.Length==2&&string.Equals(parts[1],"neptune.db-wal",StringComparison.OrdinalIgnoreCase);
        var cycle=parts.Length==5&&string.Equals(parts[1],"guarded-sync",StringComparison.OrdinalIgnoreCase)
            &&string.Equals(parts[4],"cycle.db-wal",StringComparison.OrdinalIgnoreCase);
        if(!main&&!cycle)return false;
        var database=path[..^4];
        WindowsGuardedCapabilities.RequirePlainAncestors(database);
        if(!File.Exists(database))return false;
        using var sibling=new FileStream(database,FileMode.Open,FileAccess.Read,FileShare.Read);
        if(sibling.Length<16)return false;
        Span<byte> header=stackalloc byte[16];sibling.ReadExactly(header);
        return header.SequenceEqual("SQLite format 3\0"u8);
    }
}
