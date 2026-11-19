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

            var expected = new WindowsPreferences("light", false, false) { SmallBatchMappings = ["mapping-one", "mapping-two"] };
            store.Write(expected);
            var actual = store.Read();
            Assert.Equal(expected with { SmallBatchMappings = actual.SmallBatchMappings }, actual);
            Assert.Equal(expected.SmallBatchMappings, actual.SmallBatchMappings);
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

    [Fact]
    public void LegacyPreferencesRetainFortyGiBLocalDiskReserve()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-preferences-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            File.WriteAllText(Path.Combine(directory, "preferences.json"), """{"Theme":"dark","SynchronizationEnabled":false,"SynchronizeSaturnPalette":false}""");
            var actual = new WindowsPreferencesStore(directory).Read();
            Assert.Equal(40L * 1024 * 1024 * 1024, actual.MinimumFreeDiskBytes);
            Assert.False(actual.SynchronizationEnabled);
            Assert.False(actual.SynchronizeSaturnPalette);
        }
        finally { Directory.Delete(directory, true); }
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1073741824L)]
    public void ExplicitDiskReserveRoundTripsWithoutChangingOtherPreferences(long reserve)
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-preferences-" + Guid.NewGuid().ToString("N"));
        try
        {
            var store = new WindowsPreferencesStore(directory);
            store.Write(new("light", false, false) { MinimumFreeDiskBytes = reserve, SmallBatchMappings = ["mapping-one"] });
            var actual = store.Read();
            Assert.Equal(reserve, actual.MinimumFreeDiskBytes);
            Assert.Equal("light", actual.Theme);
            Assert.False(actual.SynchronizationEnabled);
            Assert.False(actual.SynchronizeSaturnPalette);
            Assert.Equal(["mapping-one"], actual.SmallBatchMappings);
        }
        finally { if (Directory.Exists(directory)) Directory.Delete(directory, true); }
    }

    [Fact]
    public void NegativeDiskReserveCannotCreateOrModifyPreferences()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-preferences-" + Guid.NewGuid().ToString("N"));
        Assert.Throws<ArgumentOutOfRangeException>(() => new WindowsPreferencesStore(directory).Write(WindowsPreferences.Default with { MinimumFreeDiskBytes = -1 }));
        Assert.False(Directory.Exists(directory));
    }

    [Fact]
    public void InvalidStoredReserveFallsBackWithoutReenablingPausedProfileOrRewritingFile()
    {
        var directory = Path.Combine(Path.GetTempPath(), "neptune-preferences-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var file = Path.Combine(directory, "preferences.json");
            const string original = """{"Theme":"light","SynchronizationEnabled":false,"SynchronizeSaturnPalette":false,"MinimumFreeDiskBytes":-1}""";
            File.WriteAllText(file, original);
            var actual = new WindowsPreferencesStore(directory).Read();
            Assert.Equal(WindowsPreferences.Default.MinimumFreeDiskBytes, actual.MinimumFreeDiskBytes);
            Assert.False(actual.SynchronizationEnabled);
            Assert.False(actual.SynchronizeSaturnPalette);
            Assert.Equal(original, File.ReadAllText(file));
        }
        finally { Directory.Delete(directory, true); }
    }

    [Fact]
    public void LocalDiskPolicyPreservesEveryNegotiatedTransportCeiling()
    {
        var negotiated = new WindowsCycleBudget(DataJobs: 2, LargeJobs: 1,
            SpoolBytes: 64L * 1024 * 1024, ProfileBytes: 512L * 1024 * 1024,
            MaximumNewFileBytes: 32L * 1024 * 1024);
        var preferences = WindowsPreferences.Default with { MinimumFreeDiskBytes = 1024L * 1024 * 1024 };
        var actual = preferences.ApplyLocalResourcePolicy(negotiated);
        Assert.Equal(negotiated with { MinimumFreeDiskBytes = preferences.MinimumFreeDiskBytes }, actual);
        Assert.Equal(40L * 1024 * 1024 * 1024, negotiated.MinimumFreeDiskBytes);
        Assert.Equal(negotiated, WindowsPreferences.Default.ApplyLocalResourcePolicy(negotiated));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            (preferences with { MinimumFreeDiskBytes = -1 }).ApplyLocalResourcePolicy(negotiated));
    }
}
