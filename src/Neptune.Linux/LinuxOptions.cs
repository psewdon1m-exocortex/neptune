namespace Neptune.Linux;

public sealed class LinuxOptions
{
    public string StateDirectory { get; init; } = "/var/lib/neptune";
    public string RegistryPath { get; init; } = "/etc/neptune/projects.json";
    public string SocketPath { get; init; } = "/run/neptune/neptuned.sock";
    public required Uri KernelOrigin { get; init; }
    public string KernelTokenFile { get; init; } = "/etc/neptune/kernel.token";
    public int MaxParallelProjects { get; init; } = 4;
    public int RegisterRefreshSeconds { get; init; } = 60;
}

