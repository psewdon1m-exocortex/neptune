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

public sealed record MappingViewModel(SyncMapping Mapping)
{
    public string DisplayName => Path.GetFileName(Mapping.LocalPath.TrimEnd(Path.DirectorySeparatorChar)) is { Length: > 0 } name ? name : Mapping.LocalPath;
    public string LocalPath => Mapping.LocalPath;
}

public partial class MainWindow : Window
{
    private readonly WindowsProfileContext _profile;
    private readonly WindowsConnectionStore _connections;
    private readonly Func<WindowsSyncService> _syncServiceFactory;
    private readonly WindowsAutostartService _autostart;
    private readonly WindowsUpdateService _updates;
    private readonly WindowsTrayIcon _tray;
    private readonly NeptuneStateStore _state;
    private readonly ObservableCollection<MappingViewModel> _mappings = [];
    private string _clientInstanceId = "initializing";
    private CancellationTokenSource? _syncCancellation;
    private bool _changingAutostart = true;
    private bool _allowExit;
    private bool _backgroundHintShown;
    private bool _checkingUpdates;
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
        _state = new NeptuneStateStore(Path.Combine(profile.StateDirectory, "neptune.db"));
        InitializeComponent();
        _tray = new WindowsTrayIcon(profile.ProfileId, RestoreFromTray, SyncFromTrayAsync, CheckForUpdatesAsync, ExitFromTray);
        MappingsList.ItemsSource = _mappings;
        ProfileLabel.Text = $"profile / {profile.ProfileId}";
        SourceInitialized += (_, _) => SetClientSize(800, 500);
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
        await _state.InitializeAsync();
        _clientInstanceId = await new ClientIdentityStore(_profile.StateDirectory).GetOrCreateAsync();
        ClientLabel.Text = _clientInstanceId;
        foreach (var mapping in await _state.ListMappingsAsync()) _mappings.Add(new MappingViewModel(mapping));
        RefreshWatchers();
        _reconciliationTimer.Start();
        ShowConnectionState();
        var connection = _connections.Read();
        if (connection is not null)
        {
            try { ApplyAccent(await _syncServiceFactory().ConnectAsync(_clientInstanceId, connection)); }
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
            var accent = await _syncServiceFactory().ConnectAsync(_clientInstanceId, dialog.Connection);
            _connections.Write(dialog.Connection);
            ApplyAccent(accent);
            ShowConnectionState();
            FooterStatus.Text = $"Connected · sync/{dialog.Connection.RemoteFolder}";
            return dialog.Connection;
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
    }

    private async void Remove_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not FrameworkElement { Tag: MappingViewModel item }) return;
        await _state.RemoveMappingAsync(item.Mapping.MappingId, _clientInstanceId);
        _mappings.Remove(item);
        RefreshWatchers();
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
        var connection = _connections.Read();
        if (connection is null)
        {
            if (!interactive) return;
            connection = await PromptConnectionAsync();
            if (connection is null) return;
        }
        _syncCancellation = new CancellationTokenSource();
        SyncButton.Content = "Cancel";
        FooterStatus.Text = "Starting synchronization…";
        try
        {
            var progress = new Progress<string>(value => FooterStatus.Text = "Uploading · " + value);
            var result = await _syncServiceFactory().SyncAsync(_clientInstanceId, _mappings.Select(item => item.Mapping).ToArray(), connection, progress, _syncCancellation.Token);
            ApplyAccent(result.AccentColor);
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
            _syncCancellation.Dispose();
            _syncCancellation = null;
            SyncButton.Content = "Sync now";
        }
    }

    private void RefreshWatchers()
    {
        DisposeWatchers();
        foreach (var mapping in _mappings.Where(item => Directory.Exists(item.LocalPath)))
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

    private void ApplyAccent(string value)
    {
        var color = (Color)ColorConverter.ConvertFromString(value);
        Application.Current.Resources["AccentColor"] = color;
        Application.Current.Resources["AccentBrush"] = new SolidColorBrush(color);
        if (_connections.Read() is not null) ConnectionDot.Fill = (Brush)Application.Current.Resources["AccentBrush"];
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
