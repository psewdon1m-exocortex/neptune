using DrawingIcon = System.Drawing.Icon;
using Forms = System.Windows.Forms;

namespace Neptune.Windows;

public sealed class WindowsTrayIcon : IDisposable
{
    private readonly DrawingIcon? _icon;
    private readonly Forms.ContextMenuStrip _menu;
    private readonly Forms.NotifyIcon _notify;

    public WindowsTrayIcon(string profileId, Action open, Func<Task> synchronize, Func<Task> checkUpdates, Action exit)
    {
        _icon = Environment.ProcessPath is { } path ? DrawingIcon.ExtractAssociatedIcon(path) : null;
        _menu = new Forms.ContextMenuStrip();
        _menu.Items.Add("Open Neptune", null, (_, _) => Dispatch(open));
        _menu.Items.Add("Synchronize now", null, (_, _) => Dispatch(synchronize));
        _menu.Items.Add("Check for updates", null, (_, _) => Dispatch(checkUpdates));
        _menu.Items.Add(new Forms.ToolStripSeparator());
        _menu.Items.Add("Exit", null, (_, _) => Dispatch(exit));
        _notify = new Forms.NotifyIcon
        {
            ContextMenuStrip = _menu,
            Icon = _icon,
            Text = $"Neptune · {profileId}",
            Visible = true
        };
        _notify.DoubleClick += (_, _) => Dispatch(open);
    }

    public void ShowBackgroundHint()
    {
        _notify.BalloonTipTitle = "Neptune is still running";
        _notify.BalloonTipText = "Synchronization continues in the notification area.";
        _notify.ShowBalloonTip(3_000);
    }

    public void Dispose()
    {
        _notify.Visible = false;
        _notify.Dispose();
        _menu.Dispose();
        _icon?.Dispose();
    }

    private static void Dispatch(Action action) => System.Windows.Application.Current.Dispatcher.BeginInvoke(action);
    private static void Dispatch(Func<Task> action) => System.Windows.Application.Current.Dispatcher.BeginInvoke(async () => await action());
}
