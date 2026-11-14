using System.Windows;

namespace Neptune.Windows;

public partial class App : System.Windows.Application
{
    private Mutex? _instanceMutex;
    private bool _ownsInstanceMutex;
    private FileStream? _installationLease;

    protected override async void OnStartup(StartupEventArgs e)
    {
        base.OnStartup(e);
        if(e.Args.Contains("--recover-update",StringComparer.Ordinal))
        {
            try{WindowsUpdateTransaction.ResumeInterrupted(e.Args);Shutdown(0);}
            catch(Exception error){MessageBox.Show(error.Message,"Neptune update recovery",MessageBoxButton.OK,MessageBoxImage.Error);Shutdown(1);}
            return;
        }
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
        if(e.Args.Contains("--update-health",StringComparer.Ordinal))
        {
            try{await WindowsUpdateTransaction.AcknowledgeHealthAsync(profile,WindowsProfileCommands.Required(e.Args,"--update-health"));Shutdown(0);}
            catch{Shutdown(1);}
            return;
        }
        try
        {
            _installationLease=WindowsUpdateTransaction.EnterStartup(profile.ProfileId);
            // The requested profile command has not run; callers must retry
            // after recovery instead of treating a zero exit as completion.
            if(_installationLease is null){Shutdown(75);return;}
        }
        catch(Exception error){MessageBox.Show(error.Message,"Neptune update recovery",MessageBoxButton.OK,MessageBoxImage.Error);Shutdown(1);return;}
        if(WindowsProfileCommands.IsRequested(e.Args))
        {
            try{await WindowsProfileCommands.ExecuteAsync(profile,e.Args);Shutdown(0);}
            catch(Exception error){try{await WindowsProfileCommands.WriteJsonAsync(WindowsProfileCommands.Required(e.Args,"--output"),new{status="failed",code=error.GetType().Name,message=error.Message});}catch(Exception reportError){System.Diagnostics.Debug.WriteLine(reportError.GetType().Name);}finally{Shutdown(1);}}
            return;
        }
        new MainWindow(profile).Show();
    }

    protected override void OnExit(ExitEventArgs e)
    {
        _installationLease?.Dispose();
        if (_ownsInstanceMutex) _instanceMutex?.ReleaseMutex();
        _instanceMutex?.Dispose();
        base.OnExit(e);
    }
}
