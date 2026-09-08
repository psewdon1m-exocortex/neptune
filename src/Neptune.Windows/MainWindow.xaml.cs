using System.Collections.ObjectModel;
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
    private readonly NeptuneStateStore _state;
    private readonly ObservableCollection<MappingViewModel> _mappings = [];
    private string _clientInstanceId = "initializing";
    private CancellationTokenSource? _syncCancellation;
    private readonly List<FileSystemWatcher> _watchers = [];
    private readonly DispatcherTimer _reconciliationTimer = new() { Interval = TimeSpan.FromMinutes(5) };
    private readonly DispatcherTimer _watchDebounce = new() { Interval = TimeSpan.FromSeconds(5) };

    public MainWindow(WindowsProfileContext profile)
    {
        _profile = profile;
        _connections = new WindowsConnectionStore(profile.StateDirectory);
        _state = new NeptuneStateStore(Path.Combine(profile.StateDirectory, "neptune.db"));
        InitializeComponent();
        MappingsList.ItemsSource = _mappings;
        ProfileLabel.Text = $"profile / {profile.ProfileId}";
        SourceInitialized += (_, _) => SetClientSize(800, 500);
        Loaded += async (_, _) => await LoadAsync();
        Closed += (_, _) =>
        {
            _reconciliationTimer.Stop();
            _watchDebounce.Stop();
            _syncCancellation?.Cancel();
            DisposeWatchers();
        };
        _reconciliationTimer.Tick += async (_, _) => await StartSyncAsync(interactive: false);
        _watchDebounce.Tick += async (_, _) => { _watchDebounce.Stop(); await StartSyncAsync(interactive: false); };
    }

    private async Task LoadAsync()
    {
        await _state.InitializeAsync();
        _clientInstanceId = await new ClientIdentityStore(_profile.StateDirectory).GetOrCreateAsync();
        ClientLabel.Text = _clientInstanceId;
        foreach (var mapping in await _state.ListMappingsAsync()) _mappings.Add(new MappingViewModel(mapping));
        RefreshWatchers();
        _reconciliationTimer.Start();
        ShowConnectionState();
    }

    private void ShowConnectionState()
    {
        var configured = _connections.Read() is not null;
        ConnectionStatus.Text = configured ? "Saturn configured" : "Not configured";
        ConnectionDot.Fill = new SolidColorBrush((Color)ColorConverter.ConvertFromString(configured ? "#00A8FF" : "#697680"));
    }

    private void Connection_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new ConnectionWindow(_connections.Read()) { Owner = this };
        if (dialog.ShowDialog() == true && dialog.Connection is not null)
        {
            _connections.Write(dialog.Connection);
            ShowConnectionState();
        }
    }

    private async void AddDirectory_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new OpenFolderDialog { Title = "Select a directory to sync with Saturn", Multiselect = false };
        if (dialog.ShowDialog(this) != true) return;
        var fullPath = Path.GetFullPath(dialog.FolderName).TrimEnd(Path.DirectorySeparatorChar);
        if (_mappings.Any(item => string.Equals(item.LocalPath, fullPath, StringComparison.OrdinalIgnoreCase))) return;
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
            Connection_Click(this, new RoutedEventArgs());
            connection = _connections.Read();
            if (connection is null) return;
        }
        _syncCancellation = new CancellationTokenSource();
        SyncButton.Content = "Cancel";
        FooterStatus.Text = "Starting synchronization…";
        try
        {
            var progress = new Progress<string>(value => FooterStatus.Text = "Uploading · " + value);
            var count = await new WindowsSyncService(_profile).SyncAsync(_clientInstanceId, _mappings.Select(item => item.Mapping).ToArray(), connection, progress, _syncCancellation.Token);
            FooterStatus.Text = $"Up to date · {count} file(s) uploaded · {DateTime.Now:t}";
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
