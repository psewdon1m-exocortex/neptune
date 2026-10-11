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

    [Fact]
    public void Windows_ui_exposes_english_navigation_themes_and_mirror_controls()
    {
        var root = RepositoryRoot();
        var xaml = File.ReadAllText(Path.Combine(root, "src", "Neptune.Windows", "MainWindow.xaml"));
        var code = File.ReadAllText(Path.Combine(root, "src", "Neptune.Windows", "MainWindow.xaml.cs"));

        foreach (var screen in new[] { "DashboardScreen", "MirrorsScreen", "ActivityScreen", "SettingsScreen" })
            Assert.Contains($"x:Name=\"{screen}\"", xaml, StringComparison.Ordinal);
        Assert.Contains("Light theme", xaml, StringComparison.Ordinal);
        Assert.Contains("Synchronize colors with Saturn", xaml, StringComparison.Ordinal);
        Assert.Contains("SaturnPalette_Changed", xaml, StringComparison.Ordinal);
        Assert.Contains("Synchronization enabled", xaml, StringComparison.Ordinal);
        Assert.Contains("MappingEnabled_Changed", xaml, StringComparison.Ordinal);
        Assert.Contains("Confirmed mirror size", xaml, StringComparison.Ordinal);
        Assert.Contains("ApplyTheme", code, StringComparison.Ordinal);
        Assert.DoesNotMatch("[А-Яа-яЁё]", xaml);
    }

    [Fact]
    public void Native_title_bars_follow_the_active_application_theme()
    {
        var root = RepositoryRoot();
        var titleBar = File.ReadAllText(Path.Combine(root, "src", "Neptune.Windows", "WindowsTitleBar.cs"));
        var main = File.ReadAllText(Path.Combine(root, "src", "Neptune.Windows", "MainWindow.xaml.cs"));
        var connection = File.ReadAllText(Path.Combine(root, "src", "Neptune.Windows", "ConnectionWindow.xaml.cs"));

        Assert.Contains("DwmwaCaptionColor = 35", titleBar, StringComparison.Ordinal);
        Assert.Contains("DwmwaTextColor = 36", titleBar, StringComparison.Ordinal);
        Assert.Contains("WindowsTitleBar.Apply(this)", main, StringComparison.Ordinal);
        Assert.Contains("WindowsTitleBar.Apply(this)", connection, StringComparison.Ordinal);
    }

    [Fact]
    public void Interactive_states_stay_inside_their_control_bounds()
    {
        var root = RepositoryRoot();
        var app = File.ReadAllText(Path.Combine(root, "src", "Neptune.Windows", "App.xaml"));

        Assert.DoesNotContain("ScaleX=\"1.02\"", app, StringComparison.Ordinal);
        Assert.DoesNotContain("ScaleY=\"1.05\"", app, StringComparison.Ordinal);
        Assert.DoesNotContain("Margin=\"-4\"", app, StringComparison.Ordinal);
        Assert.Contains("Property=\"IsPressed\"", app, StringComparison.Ordinal);
    }

    [Fact]
    public void Scrollable_surfaces_hide_scrollbars_without_disabling_scrolling()
    {
        var root = RepositoryRoot();
        var app = File.ReadAllText(Path.Combine(root, "src", "Neptune.Windows", "App.xaml"));
        var windows = string.Join('\n', new[] { "MainWindow.xaml", "ConnectionWindow.xaml" }
            .Select(name => File.ReadAllText(Path.Combine(root, "src", "Neptune.Windows", name))));

        Assert.Contains("<Style TargetType=\"ScrollBar\">", app, StringComparison.Ordinal);
        Assert.Contains("VerticalScrollBarVisibility=\"Hidden\"", windows, StringComparison.Ordinal);
        Assert.Contains("PanningMode=\"VerticalOnly\"", windows, StringComparison.Ordinal);
        Assert.DoesNotContain("ScrollBarVisibility=\"Auto\"", windows, StringComparison.Ordinal);
        Assert.DoesNotContain("ScrollBarVisibility=\"Visible\"", windows, StringComparison.Ordinal);
    }

    private static string RepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "Neptune.slnx"))) directory = directory.Parent;
        return directory?.FullName ?? throw new DirectoryNotFoundException("Neptune repository root was not found.");
    }
}
