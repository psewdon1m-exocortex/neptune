using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class WindowsPreferencesTests
{
    [Fact]
    public void PreferencesPersistThemeAndGlobalSynchronizationState()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-preferences-" + Guid.NewGuid().ToString("N"));
        try
        {
            var store = new WindowsPreferencesStore(directory);
            Assert.Equal(WindowsPreferences.Default, store.Read());

            var expected = new WindowsPreferences("light", false, false);
            store.Write(expected);
            Assert.Equal(expected, store.Read());
        }
        finally
        {
            if (Directory.Exists(directory)) Directory.Delete(directory, true);
        }
    }

    [Fact]
    public void LegacyPreferencesKeepSaturnPaletteSynchronizationEnabled()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-preferences-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            File.WriteAllText(Path.Combine(directory, "preferences.json"), """{"Theme":"light","SynchronizationEnabled":true}""");
            Assert.True(new WindowsPreferencesStore(directory).Read().SynchronizeSaturnPalette);
        }
        finally { Directory.Delete(directory, true); }
    }

    [Fact]
    public void InvalidPreferencesFallBackWithoutChangingFiles()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-preferences-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            File.WriteAllText(Path.Combine(directory, "preferences.json"), "{not-json");
            Assert.Equal(WindowsPreferences.Default, new WindowsPreferencesStore(directory).Read());
        }
        finally { Directory.Delete(directory, true); }
    }
}
