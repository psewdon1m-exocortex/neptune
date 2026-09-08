using Microsoft.Win32;

namespace Neptune.Windows;

public sealed class WindowsAutostartService(WindowsProfileContext profile)
{
    private const string RunKeyPath = @"Software\Microsoft\Windows\CurrentVersion\Run";
    private string ValueName => profile.ProfileId == "default" ? "Neptune" : $"Neptune-{profile.ProfileId}";

    public bool IsEnabled()
    {
        using var key = Registry.CurrentUser.OpenSubKey(RunKeyPath, writable: false);
        return string.Equals(key?.GetValue(ValueName) as string, Command(), StringComparison.Ordinal);
    }

    public void SetEnabled(bool enabled)
    {
        using var key = Registry.CurrentUser.CreateSubKey(RunKeyPath, writable: true)
            ?? throw new InvalidOperationException("Windows autostart registry key is unavailable.");
        if (enabled) key.SetValue(ValueName, Command(), RegistryValueKind.String);
        else key.DeleteValue(ValueName, throwOnMissingValue: false);
    }

    private string Command()
    {
        var executable = Environment.ProcessPath;
        if (string.IsNullOrWhiteSpace(executable)) throw new InvalidOperationException("Neptune executable path is unavailable.");
        return $"\"{executable}\" --profile \"{profile.ProfileId}\" --minimized";
    }
}
