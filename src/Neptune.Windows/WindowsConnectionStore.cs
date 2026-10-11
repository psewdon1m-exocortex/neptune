using System.Security.Cryptography;
using System.Runtime.Versioning;
using System.Text;
using System.Text.Json;

namespace Neptune.Windows;

internal sealed record StoredWindowsConnection(string KernelOrigin, string KernelToken, string SaturnToken, string? RemoteFolder, string? KernelCredentialKind = null);

[SupportedOSPlatform("windows")]
public sealed class WindowsConnectionStore(string stateDirectory)
{
    private readonly string _path = Path.Combine(stateDirectory, "connection.json");

    public WindowsConnection? Read()
    {
        if (!File.Exists(_path)) return null;
        var stored = JsonSerializer.Deserialize<StoredWindowsConnection>(File.ReadAllText(_path))
            ?? throw new InvalidDataException("Neptune connection configuration is invalid.");
        var kind = stored.KernelCredentialKind switch
        {
            null or "legacy-service" => WindowsKernelCredentialKind.LegacyServiceToken,
            "neptune-outdoor-v1" => WindowsKernelCredentialKind.NeptuneOutdoorToken,
            _ => throw new InvalidDataException("Neptune Kernel credential type is invalid."),
        };
        return new WindowsConnection(new Uri(stored.KernelOrigin), Unprotect(stored.KernelToken), Unprotect(stored.SaturnToken),
            string.IsNullOrWhiteSpace(stored.RemoteFolder) ? Environment.MachineName : stored.RemoteFolder, kind);
    }

    public void Write(WindowsConnection connection)
    {
        Directory.CreateDirectory(stateDirectory);
        var kind = connection.KernelCredentialKind switch
        {
            WindowsKernelCredentialKind.LegacyServiceToken => "legacy-service",
            WindowsKernelCredentialKind.NeptuneOutdoorToken => "neptune-outdoor-v1",
            _ => throw new InvalidDataException("Neptune Kernel credential type is invalid."),
        };
        var stored = new StoredWindowsConnection(connection.KernelOrigin.AbsoluteUri, Protect(connection.KernelToken), Protect(connection.SaturnToken), connection.RemoteFolder.Trim(), kind);
        var temporary = _path + ".tmp";
        File.WriteAllText(temporary, JsonSerializer.Serialize(stored, new JsonSerializerOptions { WriteIndented = true }));
        File.Move(temporary, _path, overwrite: true);
    }

    private static string Protect(string value) => Convert.ToBase64String(ProtectedData.Protect(Encoding.UTF8.GetBytes(value.Trim()), null, DataProtectionScope.CurrentUser));
    private static string Unprotect(string value) => Encoding.UTF8.GetString(ProtectedData.Unprotect(Convert.FromBase64String(value), null, DataProtectionScope.CurrentUser));
}
