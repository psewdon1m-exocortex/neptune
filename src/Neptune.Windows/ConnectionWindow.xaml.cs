using System.Windows;

namespace Neptune.Windows;

public partial class ConnectionWindow : Window
{
    private string _existingFolder = "";
    public WindowsConnection? Connection { get; private set; }
    public ConnectionWindow(WindowsConnection? existing)
    {
        InitializeComponent();
        SourceInitialized += (_, _) => WindowsTitleBar.Apply(this);
        if (existing is null) return;
        _existingFolder = existing.RemoteFolder;
        KernelOrigin.Text = existing.KernelOrigin.AbsoluteUri;
        if (existing.KernelCredentialKind == WindowsKernelCredentialKind.NeptuneOutdoorToken) KernelToken.Password = existing.KernelToken;
        else
        {
            ConnectionInstructions.Text = "This profile still uses the retired shared Kernel service token. Paste a Neptune outdoor token to migrate it safely; the existing Saturn credential will be preserved.";
            ConnectionInstructions.SetResourceReference(ForegroundProperty, "DangerBrush");
        }
        SaturnToken.Password = existing.SaturnToken;
    }

    private void Save_Click(object sender, RoutedEventArgs e)
    {
        if (!Uri.TryCreate(KernelOrigin.Text.Trim(), UriKind.Absolute, out var origin) || origin.Scheme != Uri.UriSchemeHttps)
        {
            MessageBox.Show(this, "Enter a valid HTTPS Kernel origin.", "Neptune", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }
        if (string.IsNullOrWhiteSpace(KernelToken.Password) || string.IsNullOrWhiteSpace(SaturnToken.Password))
        {
            MessageBox.Show(this, "The Kernel Neptune outdoor token and Saturn setup code or device credential are required.", "Neptune", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }
        Connection = new WindowsConnection(origin, KernelToken.Password, SaturnToken.Password, _existingFolder, WindowsKernelCredentialKind.NeptuneOutdoorToken);
        DialogResult = true;
    }
}
