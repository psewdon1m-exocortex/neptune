using System.Diagnostics;

namespace Neptune.Windows;

public static class UpdateApplyMode
{
    public static bool IsRequested(string[] arguments) => arguments.Contains("--apply-update", StringComparer.Ordinal);

    public static void Apply(string[] arguments)
    {
        var source = Required(arguments, "--source");
        var target = Required(arguments, "--target");
        var profile = Required(arguments, "--profile");
        source = Path.GetFullPath(source);
        target = Path.GetFullPath(target);
        if (!Directory.Exists(source) || !File.Exists(Path.Combine(source, "Neptune.Windows.exe"))) throw new InvalidDataException("Update staging directory is invalid.");
        if (string.Equals(source, target, StringComparison.OrdinalIgnoreCase)) throw new InvalidDataException("Update source and target must differ.");

        WaitForParent(arguments,required:true);
        var metadata=Path.Combine(source,"update-package.json");
        var package=File.Exists(metadata)?System.Text.Json.JsonSerializer.Deserialize<WindowsStagedUpdate>(File.ReadAllText(metadata)):null;
        if(package is not null)WindowsUpdateTransaction.RequireHash(package.Package,package.Sha256);
        if(package?.Packaging=="per-user-msi")WindowsUpdateTransaction.ApplyInstaller(package,target,profile);
        else WindowsUpdateTransaction.ApplyPortable(source,target,profile,relaunch:true);
    }

    internal static void WaitForParent(string[] arguments,bool required)
    {
        if(!required&&!arguments.Contains("--wait-pid",StringComparer.Ordinal))return;
        if(!int.TryParse(Required(arguments,"--wait-pid"),out var processId)||processId<=0||processId==Environment.ProcessId)throw new InvalidDataException("Update process ID is invalid.");
        try{using var process=Process.GetProcessById(processId);if(!process.WaitForExit(60_000))throw new TimeoutException("The running Neptune process did not stop in time.");}
        catch(ArgumentException){}
    }

    private static string Required(string[] arguments, string name)
    {
        var index = Array.FindIndex(arguments, item => item == name);
        if (index < 0 || index + 1 >= arguments.Length || string.IsNullOrWhiteSpace(arguments[index + 1])) throw new InvalidDataException($"Update argument {name} is missing.");
        return arguments[index + 1];
    }

    internal static ProcessStartInfo CreateRelaunchStartInfo(string target, string profile)
    {
        var executable = Path.Combine(target, "Neptune.Windows.exe");
        var start = new ProcessStartInfo(executable) { UseShellExecute = false, WorkingDirectory = target };
        start.ArgumentList.Add("--profile"); start.ArgumentList.Add(profile);
        return start;
    }

    internal static void ReplaceApplicationFiles(string source, string target)
    {
        WindowsUpdateTransaction.ApplyPortable(source,target,"default",relaunch:false);
    }
}
