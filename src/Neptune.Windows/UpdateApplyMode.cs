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
        if (!int.TryParse(Required(arguments, "--wait-pid"), out var processId) || processId <= 0) throw new InvalidDataException("Update process ID is invalid.");
        source = Path.GetFullPath(source);
        target = Path.GetFullPath(target);
        if (!Directory.Exists(source) || !File.Exists(Path.Combine(source, "Neptune.Windows.exe"))) throw new InvalidDataException("Update staging directory is invalid.");
        if (string.Equals(source, target, StringComparison.OrdinalIgnoreCase)) throw new InvalidDataException("Update source and target must differ.");

        try
        {
            using var process = Process.GetProcessById(processId);
            if (!process.WaitForExit(60_000)) throw new TimeoutException("The running Neptune process did not stop in time.");
        }
        catch (ArgumentException) { }
        CopyTree(source, target);

        var executable = Path.Combine(target, "Neptune.Windows.exe");
        var start = new ProcessStartInfo(executable) { UseShellExecute = false, WorkingDirectory = target };
        start.ArgumentList.Add("--profile"); start.ArgumentList.Add(profile);
        Process.Start(start);
    }

    private static string Required(string[] arguments, string name)
    {
        var index = Array.FindIndex(arguments, item => item == name);
        if (index < 0 || index + 1 >= arguments.Length || string.IsNullOrWhiteSpace(arguments[index + 1])) throw new InvalidDataException($"Update argument {name} is missing.");
        return arguments[index + 1];
    }

    private static void CopyTree(string source, string target)
    {
        foreach (var directory in Directory.EnumerateDirectories(source, "*", SearchOption.AllDirectories))
            Directory.CreateDirectory(Path.Combine(target, Path.GetRelativePath(source, directory)));
        foreach (var file in Directory.EnumerateFiles(source, "*", SearchOption.AllDirectories))
        {
            var destination = Path.Combine(target, Path.GetRelativePath(source, file));
            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            var copied = false;
            for (var attempt = 0; attempt < 20 && !copied; attempt++)
            {
                try { File.Copy(file, destination, overwrite: true); copied = true; }
                catch (IOException) when (attempt < 19) { Thread.Sleep(500); }
            }
            if (!copied) throw new IOException($"Could not replace {Path.GetFileName(destination)}.");
        }
    }
}
