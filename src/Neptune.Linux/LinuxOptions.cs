namespace Neptune.Linux;

public sealed class LinuxOptions
{
    public string StateDirectory { get; init; } = "/var/lib/neptune";
    public string RegistryPath { get; init; } = "/var/lib/neptune/projects.json";
    public string SocketPath { get; init; } = "/run/neptune/neptuned.sock";
    public required Uri KernelOrigin { get; init; }
    public string KernelTokenFile { get; init; } = "/etc/neptune/kernel.token";
    public int MaxParallelProjects { get; init; } = 4;
    public int RegisterRefreshSeconds { get; init; } = 60;
    public int RemoteControlPollSeconds { get; init; } = 15;
    public string UpdaterSocketPath { get; init; } = "/run/exocortex/updater.sock";
    public string UpdaterAgentTokenFile { get; init; } = "/etc/neptune/updater-agent.token";
}
