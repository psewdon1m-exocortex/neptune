using System.Text.Json;
using Neptune.Core;

namespace Neptune.Windows;

public sealed record WindowsPreferences(string Theme, bool SynchronizationEnabled, bool SynchronizeSaturnPalette)
{
    // Explicit per-mapping opt-in; automatic benefit qualification remains OFF.
    public string[] SmallBatchMappings { get; init; } = [];
    // Local, per-profile admission policy; server transport ceilings stay intact.
    public long MinimumFreeDiskBytes { get; init; } = 40L * 1024 * 1024 * 1024;
    public static WindowsPreferences Default { get; } = new("dark", true, true);

    internal WindowsCycleBudget ApplyLocalResourcePolicy(WindowsCycleBudget negotiated)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(MinimumFreeDiskBytes);
        return negotiated with { MinimumFreeDiskBytes = MinimumFreeDiskBytes };
    }
}

public sealed class WindowsPreferencesStore(string stateDirectory)
{
    private readonly string _path = Path.Combine(stateDirectory, "preferences.json");

    public WindowsPreferences Read()
    {
        try
        {
            if (!File.Exists(_path)) return WindowsPreferences.Default;
            var json = File.ReadAllText(_path);
            var value = JsonSerializer.Deserialize<WindowsPreferences>(json);
            if (value is null || value.Theme is not ("dark" or "light")) return WindowsPreferences.Default;
            if(value.SmallBatchMappings is null||value.SmallBatchMappings.Length>16||value.SmallBatchMappings.Any(x=>string.IsNullOrWhiteSpace(x)||x.Length>128))value=value with{SmallBatchMappings=[]};
            if (value.MinimumFreeDiskBytes < 0)
                value = value with { MinimumFreeDiskBytes = WindowsPreferences.Default.MinimumFreeDiskBytes };
            using var document = JsonDocument.Parse(json);
            if (!document.RootElement.TryGetProperty(nameof(WindowsPreferences.SynchronizeSaturnPalette), out _))
                value = value with { SynchronizeSaturnPalette = true };
            return value;
        }
        catch (JsonException) { return WindowsPreferences.Default; }
        catch (IOException) { return WindowsPreferences.Default; }
        catch (UnauthorizedAccessException) { return WindowsPreferences.Default; }
    }

    public void Write(WindowsPreferences value)
    {
        if (value.Theme is not ("dark" or "light")) throw new ArgumentOutOfRangeException(nameof(value));
        ArgumentOutOfRangeException.ThrowIfNegative(value.MinimumFreeDiskBytes);
        Directory.CreateDirectory(stateDirectory);
        var temporary = _path + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            File.WriteAllText(temporary, JsonSerializer.Serialize(value, new JsonSerializerOptions { WriteIndented = true }));
            File.Move(temporary, _path, overwrite: true);
        }
        finally
        {
            if (File.Exists(temporary)) File.Delete(temporary);
        }
    }
}
