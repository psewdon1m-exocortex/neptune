using System.Text.Json;

namespace Neptune.Windows;

public sealed record WindowsPreferences(string Theme, bool SynchronizationEnabled, bool SynchronizeSaturnPalette)
{
    public static WindowsPreferences Default { get; } = new("dark", true, true);
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
