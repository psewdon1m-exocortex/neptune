using System.Runtime.InteropServices;
using System.Windows;
using System.Windows.Interop;
using System.Windows.Media;

namespace Neptune.Windows;

internal static partial class WindowsTitleBar
{
    private const int DwmwaUseImmersiveDarkModeBefore20H1 = 19;
    private const int DwmwaUseImmersiveDarkMode = 20;
    private const int DwmwaBorderColor = 34;
    private const int DwmwaCaptionColor = 35;
    private const int DwmwaTextColor = 36;

    public static void Apply(Window window)
    {
        if (!OperatingSystem.IsWindows()) return;
        var handle = new WindowInteropHelper(window).Handle;
        if (handle == 0) return;
        if (window.FindResource("BlackBrush") is not SolidColorBrush background
            || window.FindResource("WhiteBrush") is not SolidColorBrush foreground) return;

        var dark = IsDark(background.Color) ? 1 : 0;
        if (DwmSetWindowAttribute(handle, DwmwaUseImmersiveDarkMode, ref dark, sizeof(int)) != 0)
            _ = DwmSetWindowAttribute(handle, DwmwaUseImmersiveDarkModeBefore20H1, ref dark, sizeof(int));

        var caption = ColorRef(background.Color);
        var text = ColorRef(foreground.Color);
        var border = caption;
        _ = DwmSetWindowAttribute(handle, DwmwaCaptionColor, ref caption, sizeof(int));
        _ = DwmSetWindowAttribute(handle, DwmwaTextColor, ref text, sizeof(int));
        _ = DwmSetWindowAttribute(handle, DwmwaBorderColor, ref border, sizeof(int));
    }

    private static bool IsDark(Color color) =>
        (color.R * 299 + color.G * 587 + color.B * 114) / 1000 < 128;

    private static int ColorRef(Color color) => color.R | color.G << 8 | color.B << 16;

    [LibraryImport("dwmapi.dll")]
    private static partial int DwmSetWindowAttribute(nint window, int attribute, ref int value, int valueSize);
}
