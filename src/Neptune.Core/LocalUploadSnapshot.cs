using System.Security.Cryptography;

namespace Neptune.Core;

public sealed class LocalSourceChangedException : IOException
{
    public LocalSourceChangedException() : base("The local source changed while preparing its upload snapshot.") { }
}

// Legacy DAV uploads must not hold an editor's file open during network I/O.
// Read cooperatively, then send only the private, checksum-verified copy.
internal sealed class LocalUploadSnapshot(FileStream content, string sha256) : IAsyncDisposable
{
    internal FileStream Content { get; } = content;
    internal string Sha256 { get; } = sha256;

    internal static FileStream OpenSource(string path) => new(path, FileMode.Open, FileAccess.Read,
        FileShare.ReadWrite | FileShare.Delete, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);

    internal static async Task<LocalUploadSnapshot> CaptureAsync(string path, string? expectedSha256, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var before = new FileInfo(path);
        var size = before.Length;
        var modified = before.LastWriteTimeUtc;
        var created = before.CreationTimeUtc;
        var spoolPath = Path.Combine(Path.GetTempPath(), "neptune-upload-" + Guid.NewGuid().ToString("N") + ".tmp");
        var spool = new FileStream(spoolPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read,
            128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan | FileOptions.DeleteOnClose);
        try
        {
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            await using (var source = OpenSource(path))
            {
                if (source.Length != size) throw new LocalSourceChangedException();
                var buffer = new byte[128 * 1024];
                long copied = 0;
                while (true)
                {
                    var count = await source.ReadAsync(buffer, ct);
                    if (count == 0) break;
                    copied = checked(copied + count);
                    // A growing source must not consume unbounded temporary space.
                    if (copied > size) throw new LocalSourceChangedException();
                    hash.AppendData(buffer, 0, count);
                    await spool.WriteAsync(buffer.AsMemory(0, count), ct);
                }
                var after = new FileInfo(path);
                if (copied != size || source.Length != size || after.Length != size
                    || after.LastWriteTimeUtc != modified || after.CreationTimeUtc != created)
                    throw new LocalSourceChangedException();
            }
            var digest = Convert.ToHexStringLower(hash.GetHashAndReset());
            if (expectedSha256 is not null && !string.Equals(expectedSha256, digest, StringComparison.OrdinalIgnoreCase))
                throw new LocalSourceChangedException();
            await spool.FlushAsync(ct);
            spool.Position = 0;
            return new LocalUploadSnapshot(spool, digest);
        }
        catch
        {
            await spool.DisposeAsync();
            throw;
        }
    }

    public ValueTask DisposeAsync() => Content.DisposeAsync();
}
