using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Neptune.Windows;

public sealed record WindowsConnection(Uri KernelOrigin, string KernelToken, string SaturnToken);
internal sealed record StoredWindowsConnection(string KernelOrigin, string KernelToken, string SaturnToken);

public sealed class WindowsConnectionStore(string stateDirectory)
{
    private readonly string _path = Path.Combine(stateDirectory, "connection.json");

    public WindowsConnection? Read()
    {
        if (!File.Exists(_path)) return null;
        var stored = JsonSerializer.Deserialize<StoredWindowsConnection>(File.ReadAllText(_path))
            ?? throw new InvalidDataException("Neptune connection configuration is invalid.");
        return new WindowsConnection(new Uri(stored.KernelOrigin), Unprotect(stored.KernelToken), Unprotect(stored.SaturnToken));
    }

    public void Write(WindowsConnection connection)
    {
        Directory.CreateDirectory(stateDirectory);
        var stored = new StoredWindowsConnection(connection.KernelOrigin.AbsoluteUri, Protect(connection.KernelToken), Protect(connection.SaturnToken));
        var temporary = _path + ".tmp";
        File.WriteAllText(temporary, JsonSerializer.Serialize(stored, new JsonSerializerOptions { WriteIndented = true }));
        File.Move(temporary, _path, overwrite: true);
    }

    private static string Protect(string value) => Convert.ToBase64String(ProtectedData.Protect(Encoding.UTF8.GetBytes(value.Trim()), null, DataProtectionScope.CurrentUser));
    private static string Unprotect(string value) => Encoding.UTF8.GetString(ProtectedData.Unprotect(Convert.FromBase64String(value), null, DataProtectionScope.CurrentUser));
}

