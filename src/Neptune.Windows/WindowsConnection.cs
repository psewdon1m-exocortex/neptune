namespace Neptune.Windows;

public enum WindowsKernelCredentialKind
{
    LegacyServiceToken = 0,
    NeptuneOutdoorToken = 1,
}

public sealed record WindowsConnection(
    Uri KernelOrigin,
    string KernelToken,
    string SaturnToken,
    string RemoteFolder,
    WindowsKernelCredentialKind KernelCredentialKind = WindowsKernelCredentialKind.LegacyServiceToken);
