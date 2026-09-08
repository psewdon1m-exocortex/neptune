using System.Windows;

namespace Neptune.Windows;

public partial class App : System.Windows.Application
{
    private Mutex? _instanceMutex;
    private bool _ownsInstanceMutex;

    protected override void OnStartup(StartupEventArgs e)
    {
        base.OnStartup(e);
        if (UpdateApplyMode.IsRequested(e.Args))
        {
            try { UpdateApplyMode.Apply(e.Args); }
            catch (Exception error) { MessageBox.Show(error.Message, "Neptune update failed", MessageBoxButton.OK, MessageBoxImage.Error); }
            Shutdown();
            return;
        }
        var profile = WindowsProfileContext.FromArguments(e.Args);
        _instanceMutex = new Mutex(initiallyOwned: true, $"Local\\Neptune.Windows.{profile.ProfileId}", out _ownsInstanceMutex);
        if (!_ownsInstanceMutex)
        {
            MessageBox.Show($"Neptune profile '{profile.ProfileId}' is already running.", "Neptune", MessageBoxButton.OK, MessageBoxImage.Information);
            Shutdown();
            return;
        }
        new MainWindow(profile).Show();
    }

    protected override void OnExit(ExitEventArgs e)
    {
        if (_ownsInstanceMutex) _instanceMutex?.ReleaseMutex();
        _instanceMutex?.Dispose();
        base.OnExit(e);
    }
}
