using System.Diagnostics;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Microsoft.Win32.SafeHandles;

namespace Neptune.Windows;

internal sealed record WindowsStagedUpdate(string Packaging,string Package,string Sha256,string Version,string? PreviousPackage=null,string? PreviousSha256=null);
internal sealed record WindowsUpdateJournal(string Schema,string Target,string Previous,string Candidate,string Profile,string HealthToken,string Phase,string Packaging,WindowsStagedUpdate? Installer=null,string? RecoveryHelper=null,string? ProfileStateSha256=null,string? PreviousPayloadSha256=null,int ProfileFingerprintVersion=1);

internal static partial class WindowsUpdateTransaction
{
    private static string JournalPath(string target)=>Path.TrimEndingDirectorySeparator(Path.GetFullPath(target))+".neptune-update.json";
    internal static void CopyPayload(string source,string destination)
    {
        WindowsGuardedCapabilities.RequirePlainAncestors(source);WindowsGuardedCapabilities.RequirePlainAncestors(destination);
        Directory.CreateDirectory(destination);
        var pending=new Stack<string>();pending.Push(source);var count=0;
        while(pending.Count>0)
        {
            var current=pending.Pop();WindowsGuardedCapabilities.RequirePlainAncestors(current);
            foreach(var child in Directory.EnumerateFileSystemEntries(current))
            {
                if(++count>100_000)throw new InvalidDataException("Update payload entry bound exceeded.");
                WindowsGuardedCapabilities.RequirePlainAncestors(child);var output=Path.Combine(destination,Path.GetRelativePath(source,child));
                if(Directory.Exists(child)){Directory.CreateDirectory(output);pending.Push(child);}else File.Copy(child,output,false);
            }
        }
    }
    private static void Save(WindowsUpdateJournal journal)=>WindowsProfileCommands.WriteJsonAsync(JournalPath(journal.Target),journal).GetAwaiter().GetResult();
    private static void RequireClosedProfiles(string target)
    {
        var executable=Path.Combine(Path.GetFullPath(target),"Neptune.Windows.exe");
        foreach(var process in Process.GetProcessesByName("Neptune.Windows"))
        {
            using(process)
            {
                if(process.Id==Environment.ProcessId)continue;
                if(string.Equals(ReadProcessImagePath(process.Id),executable,StringComparison.OrdinalIgnoreCase))
                    throw new IOException("Close every Neptune profile using this installation before applying the update.");
            }
        }
    }

    internal static string? ReadProcessImagePath(int processId)
    {
        // MainModule requests module/VM access, which a standard user may not
        // have for an unrelated Neptune belonging to another Windows user.
        // Image-path inspection needs only QUERY_LIMITED_INFORMATION. Keep
        // access-denied and other unknown results fatal; only an exited PID
        // may be omitted from the installation's running-process fence.
        using var process = OpenProcess(0x1000, false, processId);
        if (process.IsInvalid)
        {
            var error = Marshal.GetLastPInvokeError();
            if (error == 87 && processId > 0) return null; // PID no longer exists.
            throw new Win32Exception(error, $"Cannot inspect Neptune process {processId} before updating.");
        }
        if (!GetExitCodeProcess(process, out var exitCode))
            throw new Win32Exception(Marshal.GetLastPInvokeError(), $"Cannot inspect Neptune process {processId} before updating.");
        if (exitCode != 259) return null; // STILL_ACTIVE, including retained handles to exited processes.
        var capacity = 32768;
        var image = new StringBuilder(capacity);
        if (!QueryFullProcessImageName(process, 0, image, ref capacity))
            throw new Win32Exception(Marshal.GetLastPInvokeError(), $"Cannot resolve Neptune process {processId} before updating.");
        return image.ToString();
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern SafeProcessHandle OpenProcess(uint desiredAccess, [MarshalAs(UnmanagedType.Bool)] bool inheritHandle, int processId);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetExitCodeProcess(SafeProcessHandle process, out uint exitCode);

    [DllImport("kernel32.dll", EntryPoint = "QueryFullProcessImageNameW", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool QueryFullProcessImageName(SafeProcessHandle process, uint flags, StringBuilder imageName, ref int size);
    internal static void Recover(string target)
    {
        target=Path.TrimEndingDirectorySeparator(Path.GetFullPath(target));var path=JournalPath(target);if(!File.Exists(path))return;
        WindowsGuardedCapabilities.RequirePlainAncestors(path);if(new FileInfo(path).Length>64*1024)throw new InvalidDataException("Update journal exceeds limit.");
        var journal=JsonSerializer.Deserialize<WindowsUpdateJournal>(File.ReadAllText(path))??throw new InvalidDataException("Update journal is invalid.");
        Validate(journal,target);
        if(journal.Phase is "healthy" or "rolled-back"){UnregisterRecovery(journal);return;}
        // A rollback may not silently put an older writer over new receipts or
        // schema. The health path is read-only; any other profile change blocks
        // automatic rollback and preserves both payloads for explicit recovery.
        RequireUnchangedProfileState(journal);
        if(journal.Packaging=="per-user-msi")
        {
            var installer=journal.Installer??throw new InvalidOperationException("Interrupted installer journal lacks retained package identities.");
            if(installer.PreviousPackage is null||installer.PreviousSha256 is null)throw new InvalidDataException("Previous installer identity missing.");
            RequireHash(installer.Package,installer.Sha256);RequireHash(installer.PreviousPackage,installer.PreviousSha256);
            WindowsInstallerTrust.RequireTrusted(installer.Package);WindowsInstallerTrust.RequireTrusted(installer.PreviousPackage);
            RequirePreviousPayload(journal,journal.Previous);
            Save(journal with{Phase="rollback-pending"});
            RunInstaller(installer.Package,true,target);
            RunInstaller(installer.PreviousPackage,false,target);
            if(!WindowsPackagingPolicy.IsInstalled(target))
                throw new IOException("Windows Installer did not restore registration for the previous installation.");
            RequirePreviousPayload(journal,target);
            CompleteRollback(journal);
            return;
        }
        if(journal.Packaging!="portable-zip")throw new InvalidDataException("Unknown update packaging.");
        if((journal.Phase is "prepared" or "rollback-pending")&&Directory.Exists(target)&&!Directory.Exists(journal.Previous))
        {
            RequirePreviousPayload(journal,target);CompleteRollback(journal);return;
        }
        if(!Directory.Exists(journal.Previous))throw new IOException("Previous payload is unavailable for interrupted update recovery.");
        RequirePreviousPayload(journal,journal.Previous);Save(journal with{Phase="rollback-pending"});
        if(Directory.Exists(target))Directory.Move(target,target+".failed-"+Guid.NewGuid().ToString("N"));
        Directory.Move(journal.Previous,target);CompleteRollback(journal);
    }
    private static void CompleteRollback(WindowsUpdateJournal journal)
    {
        // Persist the terminal receipt before removing our login recovery entry.
        // If registry cleanup fails, a later recovery can safely retry it.
        Save(journal with{Phase="rolled-back"});
        UnregisterRecovery(journal);
    }
    private static void Validate(WindowsUpdateJournal journal,string target)
    {
        target=Path.TrimEndingDirectorySeparator(Path.GetFullPath(target));
        bool Sibling(string value,string suffix)=>string.Equals(value,Path.GetFullPath(value),StringComparison.OrdinalIgnoreCase)&&value.StartsWith(target+suffix,StringComparison.OrdinalIgnoreCase)&&Guid.TryParseExact(value[(target.Length+suffix.Length)..],"N",out _);
        if(journal.Schema!="neptune.windows-update.v1"||!string.Equals(Path.GetFullPath(journal.Target),Path.GetFullPath(target),StringComparison.OrdinalIgnoreCase)
            ||WindowsProfileContext.FromArguments(["--profile",journal.Profile]).ProfileId!=journal.Profile
            ||!Sibling(journal.Previous,".previous-")||!Sibling(journal.Candidate,".candidate-")
            ||journal.RecoveryHelper is not null&&!Sibling(journal.RecoveryHelper,".recovery-")
            ||journal.Phase is not("prepared" or "previous-retained" or "installed" or "rollback-pending" or "healthy" or "rolled-back")
            ||!Guid.TryParseExact(journal.HealthToken,"N",out _)
            ||journal.ProfileFingerprintVersion is not(1 or 2))throw new InvalidDataException("Update journal paths or identity changed.");
        WindowsGuardedCapabilities.RequirePlainAncestors(target);WindowsGuardedCapabilities.RequirePlainAncestors(journal.Previous);WindowsGuardedCapabilities.RequirePlainAncestors(journal.Candidate);
        if(journal.RecoveryHelper is not null)WindowsGuardedCapabilities.RequirePlainAncestors(journal.RecoveryHelper);
    }
    internal static void ApplyPortable(string source,string target,string profile,bool relaunch)
    {
        target=Path.TrimEndingDirectorySeparator(Path.GetFullPath(target));using var gate=AcquireUpdateLease(target);
        WindowsPackagingPolicy.RequirePortableDirectory(target);RequireClosedProfiles(target);Recover(target);
        var id=Guid.NewGuid().ToString("N");var journal=new WindowsUpdateJournal("neptune.windows-update.v1",target,target+".previous-"+id,target+".candidate-"+id,profile,id,"prepared","portable-zip");
        Validate(journal,target);CopyPayload(source,journal.Candidate);journal=PrepareRecovery(journal);Save(journal);RegisterRecovery(journal);
        try
        {
            Directory.Move(target,journal.Previous);Save(journal with{Phase="previous-retained"});
            Directory.Move(journal.Candidate,target);Save(journal with{Phase="installed"});
            RequireHealthy(journal,false);
        }
        catch{Recover(target);throw;}
        UnregisterRecovery(journal);gate.Dispose();
        if(relaunch)Process.Start(UpdateApplyMode.CreateRelaunchStartInfo(target,profile));
    }
    internal static void ApplyInstaller(WindowsStagedUpdate update,string target,string profile)
    {
        target=Path.TrimEndingDirectorySeparator(Path.GetFullPath(target));using var gate=AcquireUpdateLease(target);RequireClosedProfiles(target);Recover(target);
        if(!WindowsPackagingPolicy.IsInstalled(target))throw new InvalidOperationException("The update target does not match the registered Windows Installer directory.");
        if(update.PreviousPackage is null||update.PreviousSha256 is null)throw new InvalidOperationException("Installer rollback requires the verified previous MSI package.");
        RequireHash(update.Package,update.Sha256);RequireHash(update.PreviousPackage,update.PreviousSha256);
        WindowsInstallerTrust.RequireTrusted(update.Package);WindowsInstallerTrust.RequireTrusted(update.PreviousPackage);
        var id=Guid.NewGuid().ToString("N");var journal=new WindowsUpdateJournal("neptune.windows-update.v1",target,target+".previous-"+id,target+".candidate-"+id,profile,id,"prepared","per-user-msi",update);Validate(journal,target);
        CopyPayload(target,journal.Previous);journal=PrepareRecovery(journal);Save(journal);RegisterRecovery(journal);
        try{RunInstaller(update.Package,false,target);Save(journal with{Phase="installed"});RequireHealthy(journal,false);}
        catch
        {
            // MSI owns registration and component files throughout rollback.
            Recover(target);gate.Dispose();Process.Start(UpdateApplyMode.CreateRelaunchStartInfo(target,profile));throw;
        }
        UnregisterRecovery(journal);gate.Dispose();Process.Start(UpdateApplyMode.CreateRelaunchStartInfo(target,profile));
    }
    internal static ProcessStartInfo CreateInstallerStartInfo(string package,bool remove,string target)
    {
        var start=new ProcessStartInfo(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.System),"msiexec.exe")){UseShellExecute=false,CreateNoWindow=true};
        start.ArgumentList.Add(remove?"/x":"/i");start.ArgumentList.Add(package);start.ArgumentList.Add("/qn");start.ArgumentList.Add("/norestart");
        // Rollback removes the newer package (and its registry location) first.
        // The retained installer must therefore receive the original location.
        if(!remove)start.ArgumentList.Add("INSTALLDIR="+Path.GetFullPath(target));
        return start;
    }
    private static void RunInstaller(string package,bool remove,string target)
    {
        var start=CreateInstallerStartInfo(package,remove,target);
        using var process=Process.Start(start)??throw new IOException("Windows Installer did not start.");process.WaitForExit();
        if(process.ExitCode is not(0 or 3010)&&!(remove&&process.ExitCode is 1605 or 1614))throw new IOException("Windows Installer transaction failed: "+process.ExitCode);
    }
    internal static void RequireHash(string path,string expected)
    {WindowsGuardedCapabilities.RequirePlainAncestors(path);using var file=File.OpenRead(path);if(Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(file))!=expected)throw new InvalidDataException("Staged package digest changed.");}
    private static void RequireHealthy(WindowsUpdateJournal journal,bool relaunch=true)
    {
        var health=JournalPath(journal.Target)+".health-"+journal.HealthToken;
        WindowsGuardedCapabilities.RequirePlainAncestors(health);
        if(File.Exists(health))File.Delete(health);
        var start=UpdateApplyMode.CreateRelaunchStartInfo(journal.Target,journal.Profile);
        start.ArgumentList.Add("--update-health");start.ArgumentList.Add(journal.HealthToken);
        using var process=Process.Start(start)??throw new IOException("Updated Neptune did not start.");
        try
        {
            for(var n=0;n<120;n++)
            {
                if(File.Exists(health)&&File.ReadAllText(health)==journal.HealthToken)
                {if(!process.WaitForExit(10_000)||process.ExitCode!=0)throw new IOException("Update health process did not release the profile successfully.");Save(journal with{Phase="healthy"});if(relaunch)Process.Start(UpdateApplyMode.CreateRelaunchStartInfo(journal.Target,journal.Profile));return;}
                if(process.HasExited)break;Thread.Sleep(500);
            }
            throw new IOException("Updated Neptune did not complete its offline profile health check. Previous payload retained.");
        }
        finally{if(!process.HasExited){process.Kill();process.WaitForExit();}}
    }
    internal static async Task AcknowledgeHealthAsync(WindowsProfileContext profile,string token)
    {
        var target=Path.TrimEndingDirectorySeparator(Path.GetFullPath(AppContext.BaseDirectory));var path=JournalPath(target);
        var journal=JsonSerializer.Deserialize<WindowsUpdateJournal>(await File.ReadAllTextAsync(path))??throw new InvalidDataException("Update journal missing.");
        Validate(journal,target);if(journal.HealthToken!=token||journal.Profile!=profile.ProfileId||journal.Phase!="installed")throw new InvalidDataException("Unexpected update health identity.");
        // Read-only: no new writer/migration before binary rollback is possible.
        await WindowsProfileCommands.ValidateCompatibilityAsync(profile);
        var profiles=Path.GetDirectoryName(profile.StateDirectory)!;var count=0;
        if(Directory.Exists(profiles))foreach(var directory in Directory.EnumerateDirectories(profiles))
        {
            WindowsGuardedCapabilities.RequirePlainAncestors(directory);
            // The updater's own full-profile backups are deliberately outside
            // each active profile; they are not additional profile instances.
            if(string.Equals(Path.GetFileName(directory),".update-backups",StringComparison.OrdinalIgnoreCase))continue;
            if(++count>64)throw new InvalidDataException("Profile health inventory exceeds bound.");
            var other=WindowsProfileContext.FromArguments(["--profile",Path.GetFileName(directory)]);
            if(!string.Equals(other.StateDirectory,directory,StringComparison.OrdinalIgnoreCase))throw new InvalidDataException("Unexpected profile location during update health.");
            if(other.ProfileId!=profile.ProfileId)await WindowsProfileCommands.ValidateCompatibilityAsync(other);
        }
        await File.WriteAllTextAsync(path+".health-"+token,token);
    }
}
