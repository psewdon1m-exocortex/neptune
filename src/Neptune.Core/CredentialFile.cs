using System.Text;

namespace Neptune.Core;

public static class CredentialFile
{
    public const int MaximumBytes = 128 * 1024;

    public static async Task<string> ReadAsync(string path, CancellationToken cancellationToken = default)
    {
        var info = new FileInfo(path);
        if (!info.Exists || info.LinkTarget is not null || info.Length > MaximumBytes)
            throw new InvalidDataException("The service credential is unavailable or exceeds its bound.");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(5));
        await using var input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read,
            4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
        var bytes = new byte[MaximumBytes + 1];
        var length = await input.ReadAtLeastAsync(bytes, bytes.Length, throwOnEndOfStream: false, deadline.Token);
        if (length > MaximumBytes) throw new InvalidDataException("The service credential exceeds its bound.");
        // Existing machine-token files allow surrounding whitespace. Access Keys
        // never pass through this helper and retain their separate exact-value contract.
        var value = new UTF8Encoding(false, true).GetString(bytes, 0, length).Trim();
        if (value.Length == 0 || value.Any(char.IsControl)) throw new InvalidDataException("The service credential is invalid.");
        return value;
    }
}
