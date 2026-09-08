using System.Windows;

namespace Neptune.Windows;

public partial class ConnectionWindow : Window
{
    public WindowsConnection? Connection { get; private set; }
    public ConnectionWindow(WindowsConnection? existing)
    {
        InitializeComponent();
        if (existing is null) return;
        KernelOrigin.Text = existing.KernelOrigin.AbsoluteUri;
        KernelToken.Password = existing.KernelToken;
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
            MessageBox.Show(this, "Both tokens are required.", "Neptune", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }
        Connection = new WindowsConnection(origin, KernelToken.Password, SaturnToken.Password);
        DialogResult = true;
    }
}
