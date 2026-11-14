using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Neptune.Core;

// Windows legacy fallback retains the exact bytes and CAS precondition through
// disconnect/restart. This is a bounded spool, not a replacement guarded engine.
internal sealed class RetainedLegacyUpload : IAsyncDisposable
{
    private const long Quota = 8L * 1024 * 1024 * 1024;
    private const int MaximumEntries = 512;
    internal sealed record Record(string Schema, string Scope, string Target, string Hash, long Size, bool HasPrecondition, string? Etag);
    private Record _record;
    private readonly string _manifest, _bytes;
    private readonly FileStream _owner;
    internal FileStream Content { get; }
    internal string Sha256 => _record.Hash;
    internal bool HasPrecondition => _record.HasPrecondition;
    internal string? Etag => _record.Etag;
    private RetainedLegacyUpload(Record record, string manifest, string bytes, FileStream content, FileStream owner)
    { _record = record; _manifest = manifest; _bytes = bytes; Content = content; _owner = owner; }

    internal static async Task<RetainedLegacyUpload> OpenAsync(string directory, string scope, Uri target, string source, string? expectedHash, CancellationToken ct)
    {
        Plain(directory); Directory.CreateDirectory(directory); Plain(directory);
        var owner = new FileStream(Path.Combine(directory, "owner.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        FileStream? input = null;
        try
        {
            var key = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(scope + "\n" + target.AbsoluteUri)));
            var manifest = Path.Combine(directory, key + ".json"); var bytes = Path.Combine(directory, key + ".bin");
            Plain(manifest); Plain(bytes);
            Record record;
            if (File.Exists(manifest)) record = Read(manifest);
            else
            {
                if (File.Exists(bytes)) throw new InvalidDataException("Unresolved legacy spool requires review; original bytes retained.");
                var entries = Directory.EnumerateFiles(directory, "*.bin").Take(MaximumEntries + 1).ToArray();
                var size = new FileInfo(source).Length;
                if (entries.Length >= MaximumEntries || size > Quota || entries.Sum(path => new FileInfo(path).Length) > Quota - size)
                    throw new IOException("Retained legacy spool quota reached; queued originals retained.");
                await using var snapshot = await LocalUploadSnapshot.CaptureAsync(source, expectedHash, ct);
                // Publish the manifest only after all sealed bytes reach disk.
                await using (var output = new FileStream(bytes, FileMode.CreateNew, FileAccess.Write, FileShare.None, 128 * 1024, FileOptions.Asynchronous))
                { await snapshot.Content.CopyToAsync(output, ct); output.Flush(true); }
                record = new("neptune.legacy-original.v1", scope, target.AbsoluteUri, snapshot.Sha256, snapshot.Content.Length, false, null);
                await SaveAsync(manifest, record, ct);
            }
            if (record.Scope != scope || record.Target != target.AbsoluteUri) throw new InvalidDataException("Retained legacy origin binding changed.");
            input = new FileStream(bytes, FileMode.Open, FileAccess.Read, FileShare.Read, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
            if (input.Length != record.Size || Convert.ToHexStringLower(await SHA256.HashDataAsync(input, ct)) != record.Hash)
                throw new InvalidDataException("Retained legacy source checksum changed; writes refused.");
            input.Position = 0;
            return new(record, manifest, bytes, input, owner);
        }
        catch { if (input is not null) await input.DisposeAsync(); owner.Dispose(); throw; }
    }
    internal async Task BindAsync(string? etag, CancellationToken ct)
    {
        if (_record.HasPrecondition) return;
        var next = _record with { HasPrecondition = true, Etag = etag };
        await SaveAsync(_manifest, next, ct); _record = next;
    }
    internal async Task ConfirmAsync()
    {
        await Content.DisposeAsync();
        File.Delete(_manifest); File.Delete(_bytes);
    }
    internal static IEnumerable<Uri> Pending(string directory, string scope, Uri root)
    {
        if (!Directory.Exists(directory)) yield break;
        Plain(directory);
        var paths = Directory.EnumerateFiles(directory, "*.json").Take(MaximumEntries + 1).ToArray();
        if (paths.Length > MaximumEntries) throw new InvalidDataException("Legacy spool inventory exceeds its bound.");
        foreach (var path in paths.Order(StringComparer.Ordinal))
        {
            var record = Read(path); var target = new Uri(record.Target);
            if (record.Scope == scope && root.IsBaseOf(target)) yield return target;
        }
    }
    private static Record Read(string path)
    {
        Plain(path);
        if (new FileInfo(path).Length > 16 * 1024) throw new InvalidDataException("Legacy spool manifest limit exceeded.");
        var record = JsonSerializer.Deserialize<Record>(File.ReadAllBytes(path)) ?? throw new InvalidDataException("Invalid legacy spool manifest.");
        if (record.Schema != "neptune.legacy-original.v1" || record.Size < 0 || record.Size > Quota
            || record.Hash.Length != 64 || record.Hash.Any(c => !char.IsAsciiHexDigitLower(c)))
            throw new InvalidDataException("Invalid legacy spool identity.");
        return record;
    }
    private static async Task SaveAsync(string path, Record record, CancellationToken ct)
    {
        var temporary = path + ".partial";
        Plain(temporary);
        await using (var output = new FileStream(temporary, FileMode.Create, FileAccess.Write, FileShare.None))
        { await output.WriteAsync(JsonSerializer.SerializeToUtf8Bytes(record), ct); output.Flush(true); }
        File.Move(temporary, path, overwrite: true);
    }
    private static void Plain(string path)
    {
        for (var part = Path.GetFullPath(path); !string.IsNullOrEmpty(part); part = Path.GetDirectoryName(part))
            if ((File.Exists(part) || Directory.Exists(part)) && (File.GetAttributes(part) & FileAttributes.ReparsePoint) != 0)
                throw new InvalidDataException("Legacy spool cannot traverse a reparse point.");
    }
    public async ValueTask DisposeAsync() { await Content.DisposeAsync(); _owner.Dispose(); }
}
