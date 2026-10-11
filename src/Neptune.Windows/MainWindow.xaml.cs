using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Windows;
using System.Windows.Interop;
using System.Windows.Media;
using System.Windows.Threading;
using Microsoft.Win32;
using Neptune.Core;

namespace Neptune.Windows;

public sealed class MappingViewModel(SyncMapping mapping) : INotifyPropertyChanged
{
    public SyncMapping Mapping { get; private set; } = mapping;
    public string DisplayName => Path.GetFileName(Mapping.LocalPath.TrimEnd(Path.DirectorySeparatorChar)) is { Length: > 0 } name ? name : Mapping.LocalPath;
    public string LocalPath => Mapping.LocalPath;
    public bool IsEnabled => Mapping.Enabled;
    public long MirroredBytes { get; private set; }
    public int MirroredFiles { get; private set; }
    public DateTimeOffset? LastSuccessfulSyncAt { get; private set; }
    public string MirroredSizeLabel => $"{FormatBytes(MirroredBytes)} mirrored";
    public string FileCountLabel => $"{MirroredFiles:N0} file{(MirroredFiles == 1 ? "" : "s")}";
    public string LastSyncLabel => LastSuccessfulSyncAt is { } value ? $"confirmed {value.LocalDateTime:g}" : "not synchronized";

    public event PropertyChangedEventHandler? PropertyChanged;

    public void SetEnabled(bool enabled)
    {
        if (Mapping.Enabled == enabled) return;
        Mapping = Mapping with { Enabled = enabled };
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(IsEnabled)));
    }

    public void ApplyStats(SyncMappingMirrorState? stats)
    {
        MirroredBytes = stats?.MirroredBytes ?? 0;
        MirroredFiles = stats?.MirroredFiles ?? 0;
        LastSuccessfulSyncAt = stats?.LastSuccessfulSyncAt;
        foreach (var property in new[] { nameof(MirroredBytes), nameof(MirroredFiles), nameof(LastSuccessfulSyncAt), nameof(MirroredSizeLabel), nameof(FileCountLabel), nameof(LastSyncLabel) })
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(property));
    }

    public static string FormatBytes(long value)
    {
        string[] units = ["B", "KiB", "MiB", "GiB", "TiB"];
        var size = (double)Math.Max(0, value);
        var unit = 0;
        while (size >= 1024 && unit < units.Length - 1) { size /= 1024; unit++; }
        return unit == 0 ? $"{size:0} {units[unit]}" : $"{size:0.##} {units[unit]}";
    }
}

public partial class MainWindow : Window
{
    private readonly WindowsProfileContext _profile;
    private readonly WindowsConnectionStore _connections;
    private readonly Func<WindowsSyncService> _syncServiceFactory;
    private readonly WindowsAutostartService _autostart;
    private readonly WindowsUpdateService _updates;
    private readonly WindowsPreferencesStore _preferencesStore;
    private readonly WindowsTrayIcon _tray;
    private readonly NeptuneStateStore _state;
    private readonly ObservableCollection<MappingViewModel> _mappings = [];
    private WindowsPreferences _preferences;
    private SaturnPalette _saturnPalette = SaturnPalette.Default;
    private string _clientInstanceId = "initializing";
    private CancellationTokenSource? _syncCancellation;
    private bool _changingAutostart = true;
    private bool _allowExit;
    private bool _backgroundHintShown;
    private bool _checkingUpdates;
    private bool _changingPreferences = true;
    private DateTimeOffset? _lastRunAt;
    private readonly List<FileSystemWatcher> _watchers = [];
    private readonly DispatcherTimer _reconciliationTimer = new() { Interval = TimeSpan.FromMinutes(5) };
    private readonly DispatcherTimer _watchDebounce = new() { Interval = TimeSpan.FromSeconds(5) };

    public MainWindow(WindowsProfileContext profile, Func<WindowsSyncService>? syncServiceFactory = null)
    {
        _profile = profile;
        _syncServiceFactory = syncServiceFactory ?? (() => new WindowsSyncService(profile));
        _connections = new WindowsConnectionStore(profile.StateDirectory);
        _autostart = new WindowsAutostartService(profile);
        _updates = new WindowsUpdateService(profile);
        _preferencesStore = new WindowsPreferencesStore(profile.StateDirectory);
        _preferences = _preferencesStore.Read();
        _state = new NeptuneStateStore(Path.Combine(profile.StateDirectory, "neptune.db"));
        InitializeComponent();
        ApplyAppearance();
        _tray = new WindowsTrayIcon(profile.ProfileId, RestoreFromTray, SyncFromTrayAsync, CheckForUpdatesAsync, ExitFromTray);
        MappingsList.ItemsSource = _mappings;
        ActivityList.ItemsSource = _mappings;
        ProfileLabel.Text = $"profile / {profile.ProfileId}";
        SourceInitialized += (_, _) =>
        {
            SetClientSize(500, 800);
            WindowsTitleBar.Apply(this);
        };
        Loaded += async (_, _) => await LoadAsync();
        Closing += Window_Closing;
        StateChanged += (_, _) => { if (WindowState == WindowState.Minimized) HideToTray(showHint: false); };
        Application.Current.SessionEnding += (_, _) => _allowExit = true;
        Closed += (_, _) =>
        {
            _reconciliationTimer.Stop();
            _watchDebounce.Stop();
            _syncCancellation?.Cancel();
            DisposeWatchers();
            _tray.Dispose();
        };
        _reconciliationTimer.Tick += async (_, _) => await StartSyncAsync(interactive: false);
        _watchDebounce.Tick += async (_, _) => { _watchDebounce.Stop(); await StartSyncAsync(interactive: false); };
    }

    private async Task LoadAsync()
    {
        _changingAutostart = true;
        try { AutostartToggle.IsChecked = _autostart.IsEnabled(); }
        finally { _changingAutostart = false; }
        _changingPreferences = true;
        try
        {
            ThemeToggle.IsChecked = _preferences.Theme == "light";
            SaturnPaletteToggle.IsChecked = _preferences.SynchronizeSaturnPalette;
            GlobalSyncToggle.IsChecked = _preferences.SynchronizationEnabled;
        }
        finally { _changingPreferences = false; }
        await _state.InitializeAsync();
        _clientInstanceId = await new ClientIdentityStore(_profile.StateDirectory).GetOrCreateAsync();
        ClientLabel.Text = _clientInstanceId;
        foreach (var mapping in await _state.ListMappingsAsync()) _mappings.Add(new MappingViewModel(mapping));
        await RefreshMirrorStatsAsync();
        RefreshWatchers();
        _reconciliationTimer.Start();
        ShowConnectionState();
        UpdateSummary();
        var connection = _connections.Read();
        if (connection is not null)
        {
            try
            {
                var connected = await _syncServiceFactory().ConnectAsync(_clientInstanceId, connection);
                _connections.Write(connected.Connection);
                _saturnPalette = connected.Palette;
                ApplyAppearance();
            }
            catch (Exception error) { FooterStatus.Text = $"Connection check failed · {error.Message}"; }
        }
        if (_profile.StartMinimized) HideToTray(showHint: false);
    }

    private void Window_Closing(object? sender, CancelEventArgs e)
    {
        if (_allowExit) return;
        e.Cancel = true;
        HideToTray(showHint: true);
    }

    private void HideToTray(bool showHint)
    {
        Hide();
        if (showHint && !_backgroundHintShown) { _tray.ShowBackgroundHint(); _backgroundHintShown = true; }
    }

    private void RestoreFromTray()
    {
        Show();
        WindowState = WindowState.Normal;
        Activate();
    }

    private async Task SyncFromTrayAsync()
    {
        RestoreFromTray();
        await StartSyncAsync(interactive: true);
    }

    private void ExitFromTray()
    {
        _allowExit = true;
        Close();
    }

    private void Dashboard_Click(object sender, RoutedEventArgs e) => ShowScreen(DashboardScreen);
    private void Mirrors_Click(object sender, RoutedEventArgs e) => ShowScreen(MirrorsScreen);
    private void Activity_Click(object sender, RoutedEventArgs e) => ShowScreen(ActivityScreen);
    private void Settings_Click(object sender, RoutedEventArgs e) => ShowScreen(SettingsScreen);

    private void ShowScreen(UIElement selected)
    {
        foreach (var screen in new UIElement[] { DashboardScreen, MirrorsScreen, ActivityScreen, SettingsScreen })
            screen.Visibility = ReferenceEquals(screen, selected) ? Visibility.Visible : Visibility.Collapsed;
    }

    private void Theme_Changed(object sender, RoutedEventArgs e)
    {
        if (_changingPreferences) return;
        var theme = ThemeToggle.IsChecked == true ? "light" : "dark";
        var previous = _preferences;
        try
        {
            _preferences = _preferences with { Theme = theme };
            _preferencesStore.Write(_preferences);
            ApplyAppearance();
            FooterStatus.Text = theme == "light" ? "Light theme enabled" : "Dark theme enabled";
        }
        catch (Exception error)
        {
            _preferences = previous;
            _changingPreferences = true;
            try { ThemeToggle.IsChecked = previous.Theme == "light"; }
            finally { _changingPreferences = false; }
            MessageBox.Show(this, error.Message, "Neptune appearance", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private async void SaturnPalette_Changed(object sender, RoutedEventArgs e)
    {
        if (_changingPreferences) return;
        var enabled = SaturnPaletteToggle.IsChecked == true;
        var previous = _preferences;
        try
        {
            _preferences = _preferences with { SynchronizeSaturnPalette = enabled };
            _preferencesStore.Write(_preferences);
            if (enabled && _connections.Read() is { } connection)
            {
                try
                {
                    var connected = await _syncServiceFactory().ConnectAsync(_clientInstanceId, connection);
                    _connections.Write(connected.Connection);
                    _saturnPalette = connected.Palette;
                    FooterStatus.Text = "Saturn color palette synchronized";
                }
                catch (Exception error)
                {
                    FooterStatus.Text = $"Saturn palette pending · {error.Message}";
                }
            }
            else FooterStatus.Text = enabled ? "Saturn palette will apply after connection" : "Local color palette enabled";
            ApplyAppearance();
        }
        catch (Exception error)
        {
            _preferences = previous;
            _changingPreferences = true;
            try { SaturnPaletteToggle.IsChecked = previous.SynchronizeSaturnPalette; }
            finally { _changingPreferences = false; }
            MessageBox.Show(this, error.Message, "Neptune appearance", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void GlobalSync_Changed(object sender, RoutedEventArgs e)
    {
        if (_changingPreferences) return;
        var enabled = GlobalSyncToggle.IsChecked == true;
        var previous = _preferences;
        try
        {
            _preferences = _preferences with { SynchronizationEnabled = enabled };
            _preferencesStore.Write(_preferences);
            if (!enabled) _syncCancellation?.Cancel();
            RefreshWatchers();
            UpdateSummary();
            FooterStatus.Text = enabled ? "Synchronization enabled" : "Synchronization paused for this profile";
        }
        catch (Exception error)
        {
            _preferences = previous;
            _changingPreferences = true;
            try { GlobalSyncToggle.IsChecked = previous.SynchronizationEnabled; }
            finally { _changingPreferences = false; }
            MessageBox.Show(this, error.Message, "Neptune synchronization", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private async void MappingEnabled_Changed(object sender, RoutedEventArgs e)
    {
        if (sender is not System.Windows.Controls.CheckBox { Tag: MappingViewModel item } toggle) return;
        var enabled = toggle.IsChecked == true;
        if (item.IsEnabled == enabled) return;
        try
        {
            await _state.SetMappingEnabledAsync(item.Mapping.MappingId, _clientInstanceId, enabled);
            item.SetEnabled(enabled);
            RefreshWatchers();
            UpdateSummary();
            FooterStatus.Text = enabled ? $"Mirror enabled · {item.DisplayName}" : $"Mirror paused · {item.DisplayName}";
            if (enabled && _preferences.SynchronizationEnabled) ScheduleHintSync();
        }
        catch (Exception error)
        {
            item.SetEnabled(!enabled);
            MessageBox.Show(this, error.Message, "Neptune mirror", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void Autostart_Changed(object sender, RoutedEventArgs e)
    {
        if (_changingAutostart) return;
        try
        {
            var enabled = AutostartToggle.IsChecked == true;
            _autostart.SetEnabled(enabled);
            FooterStatus.Text = enabled ? "Autostart enabled for this profile" : "Autostart disabled for this profile";
        }
        catch (Exception error)
        {
            _changingAutostart = true;
            try { AutostartToggle.IsChecked = _autostart.IsEnabled(); }
            finally { _changingAutostart = false; }
            MessageBox.Show(this, error.Message, "Neptune autostart", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void ShowConnectionState()
    {
        var configured = _connections.Read() is not null;
        ConnectionStatus.Text = configured ? "Saturn configured" : "Not configured";
        ConnectionDot.Fill = configured ? (Brush)Application.Current.Resources["AccentBrush"] : new SolidColorBrush((Color)ColorConverter.ConvertFromString("#697680"));
    }

    private async void Connection_Click(object sender, RoutedEventArgs e) => await PromptConnectionAsync();

    private async void Update_Click(object sender, RoutedEventArgs e) => await CheckForUpdatesAsync();

    private async Task CheckForUpdatesAsync()
    {
        if (_checkingUpdates) return;
        RestoreFromTray();
        var connection = _connections.Read();
        if (connection is null)
        {
            RestoreFromTray();
            connection = await PromptConnectionAsync();
            if (connection is null) return;
        }
        _checkingUpdates = true;
        UpdateButton.IsEnabled = false;
        UpdateButton.Content = "Checking…";
        try
        {
            var release = await _updates.CheckAsync(connection);
            if (release is null)
            {
                MessageBox.Show(this, $"Neptune {_updates.CurrentVersion.ToString(3)} is up to date.", "Neptune update", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }
            RestoreFromTray();
            if (WindowsUpdateService.HasOtherInstances())
            {
                MessageBox.Show(this, "Close other Neptune profiles before installing the update.", "Neptune update", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }
            var answer = MessageBox.Show(this, $"Neptune {release.VersionText} is available. Download and install it now?", "Neptune update", MessageBoxButton.YesNo, MessageBoxImage.Question);
            if (answer != MessageBoxResult.Yes) return;
            UpdateButton.Content = "Downloading…";
            var progress = new Progress<int>(value => { UpdateButton.Content = $"Downloading {value}%"; FooterStatus.Text = $"Downloading update · {value}%"; });
            var staging = await _updates.DownloadAsync(release, progress);
            FooterStatus.Text = "Applying update…";
            _updates.BeginApply(staging);
            _allowExit = true;
            Application.Current.Shutdown();
        }
        catch (Exception error)
        {
            RestoreFromTray();
            MessageBox.Show(this, error.Message, "Neptune update failed", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            _checkingUpdates = false;
            UpdateButton.IsEnabled = true;
            UpdateButton.Content = "Check updates";
        }
    }

    private async Task<WindowsConnection?> PromptConnectionAsync()
    {
        var dialog = new ConnectionWindow(_connections.Read()) { Owner = this };
        if (dialog.ShowDialog() != true || dialog.Connection is null) return null;
        try
        {
            var connected = await _syncServiceFactory().ConnectAsync(_clientInstanceId, dialog.Connection);
            _connections.Write(connected.Connection);
            _saturnPalette = connected.Palette;
            ApplyAppearance();
            ShowConnectionState();
            FooterStatus.Text = $"Connected · sync/{connected.Connection.RemoteFolder}";
            return connected.Connection;
        }
        catch (Exception error)
        {
            MessageBox.Show(this, error.Message, "Neptune connection failed", MessageBoxButton.OK, MessageBoxImage.Error);
            return null;
        }
    }

    private async void AddDirectory_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new OpenFolderDialog { Title = "Select a directory to sync with Saturn", Multiselect = false };
        if (dialog.ShowDialog(this) != true) return;
        var fullPath = Path.GetFullPath(dialog.FolderName).TrimEnd(Path.DirectorySeparatorChar);
        if (_mappings.Any(item => string.Equals(item.LocalPath, fullPath, StringComparison.OrdinalIgnoreCase))) return;
        var folderName = Path.GetFileName(fullPath);
        if (_mappings.Any(item => string.Equals(Path.GetFileName(item.LocalPath), folderName, StringComparison.OrdinalIgnoreCase)))
        {
            MessageBox.Show(this, $"Another selected directory already uses the Saturn folder name '{folderName}'.", "Neptune", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }
        var mapping = new SyncMapping($"mapping-{Guid.NewGuid():N}", _clientInstanceId, fullPath, true, DateTimeOffset.UtcNow);
        await _state.AddMappingAsync(mapping);
        _mappings.Add(new MappingViewModel(mapping));
        RefreshWatchers();
        UpdateSummary();
    }

    private async void Remove_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not FrameworkElement { Tag: MappingViewModel item }) return;
        await _state.RemoveMappingAsync(item.Mapping.MappingId, _clientInstanceId);
        _mappings.Remove(item);
        RefreshWatchers();
        UpdateSummary();
    }

    private async void Sync_Click(object sender, RoutedEventArgs e)
    {
        if (_syncCancellation is not null)
        {
            _syncCancellation.Cancel();
            return;
        }
        await StartSyncAsync(interactive: true);
    }

    private async Task StartSyncAsync(bool interactive)
    {
        if (_syncCancellation is not null || _mappings.Count == 0) return;
        if (!_preferences.SynchronizationEnabled)
        {
            FooterStatus.Text = "Synchronization is paused";
            return;
        }
        if (!_mappings.Any(item => item.IsEnabled))
        {
            FooterStatus.Text = "No mirror entities are enabled";
            return;
        }
        var connection = _connections.Read();
        if (connection is null)
        {
            if (!interactive) return;
            connection = await PromptConnectionAsync();
            if (connection is null) return;
        }
        _syncCancellation = new CancellationTokenSource();
        SyncButton.Content = "Cancel";
        DashboardSyncButton.Content = "Cancel  →";
        FooterStatus.Text = "Starting synchronization…";
        try
        {
            var progress = new Progress<string>(value => FooterStatus.Text = "Uploading · " + value);
            var result = await _syncServiceFactory().SyncAsync(_clientInstanceId, _mappings.Select(item => item.Mapping).ToArray(), connection, progress, _syncCancellation.Token);
            _saturnPalette = result.Palette;
            ApplyAppearance();
            ApplyMirrorStats(result.Mirrors);
            _lastRunAt = DateTimeOffset.Now;
            FooterStatus.Text = $"Up to date · {result.UploadedFiles} file(s) uploaded · {DateTime.Now:t}";
        }
        catch (OperationCanceledException) { FooterStatus.Text = "Synchronization cancelled"; }
        catch (Exception error)
        {
            FooterStatus.Text = "Synchronization failed";
            MessageBox.Show(this, error.Message, "Neptune sync failed", MessageBoxButton.OK, MessageBoxImage.Error);
        }
        finally
        {
            await RefreshMirrorStatsAsync();
            _syncCancellation.Dispose();
            _syncCancellation = null;
            SyncButton.Content = "Sync now";
            DashboardSyncButton.Content = "Sync now  →";
            UpdateSummary();
        }
    }

    private void RefreshWatchers()
    {
        DisposeWatchers();
        if (!_preferences.SynchronizationEnabled) return;
        foreach (var mapping in _mappings.Where(item => item.IsEnabled && Directory.Exists(item.LocalPath)))
        {
            var watcher = new FileSystemWatcher(mapping.LocalPath)
            {
                IncludeSubdirectories = true,
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.DirectoryName | NotifyFilters.LastWrite | NotifyFilters.Size,
                EnableRaisingEvents = true
            };
            FileSystemEventHandler changed = (_, _) => Dispatcher.Invoke(ScheduleHintSync);
            RenamedEventHandler renamed = (_, _) => Dispatcher.Invoke(ScheduleHintSync);
            ErrorEventHandler error = (_, _) => Dispatcher.Invoke(ScheduleHintSync);
            watcher.Changed += changed;
            watcher.Created += changed;
            watcher.Deleted += changed;
            watcher.Renamed += renamed;
            watcher.Error += error;
            _watchers.Add(watcher);
        }
    }

    private void ScheduleHintSync()
    {
        _watchDebounce.Stop();
        _watchDebounce.Start();
    }

    private void DisposeWatchers()
    {
        foreach (var watcher in _watchers) watcher.Dispose();
        _watchers.Clear();
    }

    private void ApplyAppearance()
    {
        var palette = _preferences.SynchronizeSaturnPalette ? _saturnPalette : SaturnPalette.Default;
        ApplyTheme(_preferences.Theme, palette, _preferences.SynchronizeSaturnPalette);
        if (new WindowInteropHelper(this).Handle != nint.Zero) WindowsTitleBar.Apply(this);
        if (_connections.Read() is not null) ConnectionDot.Fill = (Brush)Application.Current.Resources["AccentBrush"];
    }

    private static void ApplyTheme(string theme, SaturnPalette palette, bool synchronized)
    {
        var light = theme == "light";
        var black = synchronized ? palette.BlackColor : SaturnPalette.Default.BlackColor;
        var white = synchronized ? palette.WhiteColor : SaturnPalette.Default.WhiteColor;
        var accent = synchronized ? palette.AccentColor : SaturnPalette.Default.AccentColor;
        var background = light ? white : black;
        var foreground = light ? black : white;
        SetColor("AccentColor", accent);
        SetBrush("AccentBrush", accent);
        SetBrush("BlackBrush", synchronized ? background : light ? "#F3F1EA" : black);
        SetBrush("PanelBrush", synchronized ? background : light ? white : "#050505");
        SetBrush("WhiteBrush", synchronized ? foreground : light ? "#151515" : white);
        SetBrush("White80Brush", light ? Blend(white, black, 0.72) : Blend(black, white, 0.80));
        SetBrush("DecorativeDotBrush", light ? Blend(white, black, 0.58) : Blend(black, white, 0.85));
        SetBrush("SuccessBrush", light ? "#147A3A" : "#62FF8C");
        SetBrush("DangerBrush", light ? "#C72929" : "#F83D3D");
    }

    private static string Blend(string first, string second, double secondWeight)
    {
        var left = (Color)ColorConverter.ConvertFromString(first);
        var right = (Color)ColorConverter.ConvertFromString(second);
        static byte Channel(byte left, byte right, double weight) =>
            (byte)Math.Round(left + (right - left) * weight, MidpointRounding.AwayFromZero);
        return $"#{Channel(left.R, right.R, secondWeight):X2}{Channel(left.G, right.G, secondWeight):X2}{Channel(left.B, right.B, secondWeight):X2}";
    }

    private static void SetColor(string key, string value) =>
        Application.Current.Resources[key] = (Color)ColorConverter.ConvertFromString(value);

    private static void SetBrush(string key, string value) =>
        Application.Current.Resources[key] = new SolidColorBrush((Color)ColorConverter.ConvertFromString(value));

    private async Task RefreshMirrorStatsAsync()
    {
        if (_clientInstanceId == "initializing") return;
        try { ApplyMirrorStats(await _state.ListSyncMappingStatsAsync(_clientInstanceId)); }
        catch (Exception error) when (error is IOException or Microsoft.Data.Sqlite.SqliteException)
        {
            FooterStatus.Text = $"Mirror statistics unavailable · {error.Message}";
        }
    }

    private void ApplyMirrorStats(IReadOnlyList<SyncMappingMirrorState> values)
    {
        var byMapping = values.ToDictionary(value => value.MappingId, StringComparer.Ordinal);
        foreach (var mapping in _mappings)
            mapping.ApplyStats(byMapping.GetValueOrDefault(mapping.Mapping.MappingId));
        UpdateSummary();
    }

    private void UpdateSummary()
    {
        var totalBytes = _mappings.Sum(item => item.MirroredBytes);
        var totalFiles = _mappings.Sum(item => (long)item.MirroredFiles);
        var confirmed = _mappings.Count(item => item.LastSuccessfulSyncAt is not null);
        var enabled = _mappings.Count(item => item.IsEnabled);
        var formatted = MappingViewModel.FormatBytes(totalBytes);
        MirrorTotalText.Text = formatted;
        ActivityTotalText.Text = formatted;
        DashboardSummaryText.Text = $"Confirmed mirror size across {confirmed} of {_mappings.Count} entities";
        ActivitySummaryText.Text = $"{totalFiles:N0} files · {enabled} enabled · {_mappings.Count - enabled} paused";
        LastRunText.Text = _lastRunAt is { } last ? $"Last run · {last.LocalDateTime:g}" : "Last run · this session has not synchronized yet";
        HeaderSyncState.Text = _preferences.SynchronizationEnabled ? "SYNC ENABLED" : "SYNC PAUSED";
        HeaderSyncState.Foreground = _preferences.SynchronizationEnabled
            ? (Brush)Application.Current.Resources["AccentBrush"]
            : (Brush)Application.Current.Resources["White80Brush"];
        var canSync = _preferences.SynchronizationEnabled && enabled > 0;
        SyncButton.IsEnabled = canSync;
        DashboardSyncButton.IsEnabled = canSync;
    }

    private void SetClientSize(int width, int height)
    {
        var handle = new WindowInteropHelper(this).Handle;
        if (!GetClientRect(handle, out var client) || !GetWindowRect(handle, out var window)) return;
        Width = width + (window.Right - window.Left) - (client.Right - client.Left);
        Height = height + (window.Bottom - window.Top) - (client.Bottom - client.Top);
    }

    [LibraryImport("user32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool GetClientRect(nint window, out RectNative rect);
    [LibraryImport("user32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool GetWindowRect(nint window, out RectNative rect);
    private struct RectNative { public int Left; public int Top; public int Right; public int Bottom; }
}
