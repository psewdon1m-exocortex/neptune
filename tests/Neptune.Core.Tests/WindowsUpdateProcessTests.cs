using System.ComponentModel;
using System.Diagnostics;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class WindowsUpdateProcessTests
{
    [Fact]
    public void LimitedQueryResolvesRunningNativeImage()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows process inventory"); return; }
        Assert.Equal(Environment.ProcessPath, WindowsUpdateTransaction.ReadProcessImagePath(Environment.ProcessId), ignoreCase: true);
    }

    [Fact]
    public void ExitedProcessCanLeaveTheInventory()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows process inventory"); return; }
        var start = new ProcessStartInfo(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.System), "where.exe"))
        { UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add("/?");
        using var child = Process.Start(start)!;
        child.StandardOutput.ReadToEnd();
        child.StandardError.ReadToEnd();
        Assert.True(child.WaitForExit(10_000));
        Assert.Null(WindowsUpdateTransaction.ReadProcessImagePath(child.Id));
    }

    [Fact]
    public void InvalidProcessIdentityIsNotSilentlyIgnored()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows process inventory"); return; }
        Assert.Throws<Win32Exception>(() => WindowsUpdateTransaction.ReadProcessImagePath(0));
    }
}
