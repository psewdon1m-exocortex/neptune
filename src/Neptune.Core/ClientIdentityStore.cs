using System.Text.RegularExpressions;

namespace Neptune.Core;

public sealed partial class ClientIdentityStore(string stateDirectory)
{
    private readonly string _path = Path.Combine(stateDirectory, "client-instance-id");

    public async Task<string> GetOrCreateAsync(CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(stateDirectory);
        await using var gate = await AcquireGateAsync(cancellationToken);
        if (File.Exists(_path))
        {
            var existing = await ReadExistingAsync(cancellationToken);
            if (!ClientIdPattern().IsMatch(existing))
                throw new InvalidDataException($"Invalid client instance id in '{_path}'.");
            return existing;
        }

        var created = $"client-{Guid.NewGuid():N}";
        var temporary = $"{_path}.{Guid.NewGuid():N}.tmp";
        await File.WriteAllTextAsync(temporary, created + Environment.NewLine, cancellationToken);
        try
        {
            File.Move(temporary, _path, overwrite: false);
            return created;
        }
        catch (IOException) when (File.Exists(_path))
        {
            File.Delete(temporary);
            return await ReadExistingAsync(cancellationToken);
        }
    }

    private async Task<FileStream> AcquireGateAsync(CancellationToken cancellationToken)
    {
        var lockPath = _path + ".lock";
        for (var attempt = 0; ; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try { return new FileStream(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None); }
            catch (IOException) when (attempt < 1_000)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken);
            }
        }
    }

    private async Task<string> ReadExistingAsync(CancellationToken cancellationToken)
    {
        for (var attempt = 0; ; attempt++)
        {
            try { return (await File.ReadAllTextAsync(_path, cancellationToken)).Trim(); }
            catch (IOException) when (attempt < 20)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken);
            }
        }
    }

    [GeneratedRegex("^client-[a-f0-9]{32}$", RegexOptions.CultureInvariant)]
    private static partial Regex ClientIdPattern();
}
