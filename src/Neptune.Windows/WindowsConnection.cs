namespace Neptune.Windows;

public sealed record WindowsConnection(Uri KernelOrigin, string KernelToken, string SaturnToken, string RemoteFolder);
