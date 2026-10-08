using System.Xml.Linq;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class WindowsUiContractTests
{
    [Fact]
    public void Main_window_uses_the_requested_portrait_client_contract()
    {
        var root = RepositoryRoot();
        var xaml = XDocument.Load(Path.Combine(root, "src", "Neptune.Windows", "MainWindow.xaml"));
        var window = Assert.IsType<XElement>(xaml.Root);

        Assert.Equal("500", window.Attribute("Width")?.Value);
        Assert.Equal("800", window.Attribute("Height")?.Value);
        Assert.Equal("500", window.Attribute("MinWidth")?.Value);

        var codeBehind = File.ReadAllText(Path.Combine(root, "src", "Neptune.Windows", "MainWindow.xaml.cs"));
        Assert.Contains("SetClientSize(500, 800)", codeBehind, StringComparison.Ordinal);
    }

    [Fact]
    public void Windows_ui_uses_the_shared_palette_instead_of_the_legacy_panel_colors()
    {
        var root = RepositoryRoot();
        var app = File.ReadAllText(Path.Combine(root, "src", "Neptune.Windows", "App.xaml"));
        var windows = string.Join('\n', new[] { "MainWindow.xaml", "ConnectionWindow.xaml" }
            .Select(name => File.ReadAllText(Path.Combine(root, "src", "Neptune.Windows", name))));

        Assert.Contains("#000000", app, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("#FFFFFF", app, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("#CCCCCC", app, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("#00A8FF", app, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("#62FF8C", app, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("#F83D3D", app, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("#090B0E", windows, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("#33404A", windows, StringComparison.OrdinalIgnoreCase);
    }

    private static string RepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "Neptune.slnx"))) directory = directory.Parent;
        return directory?.FullName ?? throw new DirectoryNotFoundException("Neptune repository root was not found.");
    }
}
