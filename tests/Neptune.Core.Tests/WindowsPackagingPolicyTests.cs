using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class WindowsPackagingPolicyTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void InstallerTransactionKeepsOriginalDirectoryWithoutShellParsing(bool remove)
    {
        var target=Path.Combine(Path.GetTempPath(),"Neptune custom & path","application");
        var package=Path.Combine(Path.GetTempPath(),"retained package.msi");
        var start=WindowsUpdateTransaction.CreateInstallerStartInfo(package,remove,target);
        Assert.False(start.UseShellExecute);
        Assert.Equal(remove?"/x":"/i",start.ArgumentList[0]);
        Assert.Equal(package,start.ArgumentList[1]);
        if(remove)Assert.DoesNotContain(start.ArgumentList,a=>a.StartsWith("INSTALLDIR=",StringComparison.Ordinal));
        else Assert.Equal("INSTALLDIR="+Path.GetFullPath(target),Assert.Single(start.ArgumentList,a=>a.StartsWith("INSTALLDIR=",StringComparison.Ordinal)));
    }
    private static string Application => Path.Combine(Path.GetTempPath(), "neptune-packaging-policy", "installed");

    [Fact]
    public void InstallerManagedDirectoryRejectsPortableUpdateBeforeReplacement()
    {
        var error = Assert.Throws<InvalidOperationException>(() => WindowsPackagingPolicy.RequirePortableDirectory(Application, Application));
        Assert.Contains("Windows Installer", error.Message);
    }

    [Fact]
    public void InstalledIdentityNormalizesTrailingSeparatorAndCase()
    {
        Assert.Throws<InvalidOperationException>(() => WindowsPackagingPolicy.RequirePortableDirectory(
            Application.ToUpperInvariant() + Path.DirectorySeparatorChar, Application));
    }

    [Fact]
    public void PortableDirectoryAndUnregisteredProfileRemainEligible()
    {
        WindowsPackagingPolicy.RequirePortableDirectory(Path.Combine(Path.GetTempPath(), "portable"), Application);
        WindowsPackagingPolicy.RequirePortableDirectory(Application, null);
        WindowsPackagingPolicy.RequirePortableDirectory(Application, "");
    }
}
