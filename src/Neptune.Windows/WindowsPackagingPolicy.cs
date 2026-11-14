using Microsoft.Win32;

namespace Neptune.Windows;

/// <summary>Prevents the legacy portable updater from bypassing MSI ownership.</summary>
public static class WindowsPackagingPolicy
{
    internal const string InstallerRegistryKey = @"Software\Exocortex\Neptune\Installer";
    public static bool IsInstalled(string applicationDirectory)
    {
        if(!OperatingSystem.IsWindows())return false;
        using var registration=Registry.CurrentUser.OpenSubKey(InstallerRegistryKey);
        var installed=registration?.GetValue("InstallDirectory") as string;
        return !string.IsNullOrWhiteSpace(installed)&&string.Equals(Path.TrimEndingDirectorySeparator(Path.GetFullPath(applicationDirectory)),Path.TrimEndingDirectorySeparator(Path.GetFullPath(installed)),StringComparison.OrdinalIgnoreCase);
    }

    public static void RequirePortableDirectory(string applicationDirectory)
    {
        if (!OperatingSystem.IsWindows()) return;
        using var registration = Registry.CurrentUser.OpenSubKey(InstallerRegistryKey);
        RequirePortableDirectory(applicationDirectory, registration?.GetValue("InstallDirectory") as string);
    }

    internal static void RequirePortableDirectory(string applicationDirectory, string? installedDirectory)
    {
        if (string.IsNullOrWhiteSpace(installedDirectory)) return;
        var application = Path.TrimEndingDirectorySeparator(Path.GetFullPath(applicationDirectory));
        var installed = Path.TrimEndingDirectorySeparator(Path.GetFullPath(installedDirectory));
        if (string.Equals(application, installed, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("This Neptune installation is managed by Windows Installer. Portable ZIP updates are disabled. Close Neptune and use the verified MSI installer to update it; your profile is preserved.");
    }
}
