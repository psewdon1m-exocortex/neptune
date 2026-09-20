using System.IO.Compression;
using System.Text;

namespace Neptune.Core;

public static class MirrorArchive
{
    public static void Extract(string archivePath, string destination, CancellationToken cancellationToken = default)
    {
        using var archive = ZipFile.OpenRead(archivePath);
        if (archive.Entries.Count > 100_000) throw new InvalidDataException("Mirror archive contains too many entries.");
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var files = new HashSet<string>(StringComparer.Ordinal);
        var directories = new HashSet<string>(StringComparer.Ordinal);
        var spelling = new Dictionary<string, string>(StringComparer.Ordinal);
        var entries = new List<(ZipArchiveEntry Entry, string Relative, bool Directory)>();
        long total = 0;
        foreach (var entry in archive.Entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var isDirectory = entry.FullName.EndsWith('/');
            var relative = isDirectory ? entry.FullName[..^1] : entry.FullName;
            var parts = relative.Split('/');
            var mode = (entry.ExternalAttributes >> 16) & 0xF000;
            if (relative.Length == 0
                || Encoding.UTF8.GetByteCount(relative) > 4096 || parts.Length > 64
                || parts.Any(part => part is "" or "." or ".." || part.Contains('\\') || part.Contains(':') || part.Any(char.IsControl))
                || mode != 0 && mode != (isDirectory ? 0x4000 : 0x8000) || (entry.ExternalAttributes & 0x400) != 0
                || !seen.Add(UnicodeNames.Key(relative)) || isDirectory && entry.Length != 0)
                throw new InvalidDataException("Mirror archive contains an unsafe, duplicate or special entry.");
            total = checked(total + entry.Length);
            if (total > 32L * 1024 * 1024 * 1024 || entry.Length > 8L * 1024 * 1024 * 1024
                || entry.Length > Math.Max(1, entry.CompressedLength) * 1000L)
                throw new InvalidDataException("Mirror archive expands beyond its limit.");
            (isDirectory ? directories : files).Add(UnicodeNames.Key(relative));
            for (var index = 1; index <= parts.Length; index++)
            {
                var original = string.Join('/', parts[..index]);
                var key = UnicodeNames.Key(original);
                if (spelling.TryGetValue(key, out var previous) && previous != original)
                    throw new InvalidDataException("Mirror archive contains colliding directory spellings.");
                spelling[key] = original;
                if (index < parts.Length) directories.Add(key);
            }
            entries.Add((entry, relative, isDirectory));
        }
        if (files.Overlaps(directories)) throw new InvalidDataException("Mirror archive has conflicting file and directory paths.");
        var root = Path.GetFullPath(destination) + Path.DirectorySeparatorChar;
        var buffer = new byte[256 * 1024];
        foreach (var (entry, relative, isDirectory) in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var target = Path.GetFullPath(Path.Combine(destination, relative.Replace('/', Path.DirectorySeparatorChar)));
            if (!target.StartsWith(root, StringComparison.Ordinal)) throw new InvalidDataException("Unsafe mirror archive path.");
            if (isDirectory) { Directory.CreateDirectory(target); continue; }
            Directory.CreateDirectory(Path.GetDirectoryName(target)!);
            using var input = entry.Open();
            using var output = new FileStream(target, FileMode.CreateNew, FileAccess.Write, FileShare.None);
            long written = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var count = input.Read(buffer);
                if (count == 0) break;
                written += count;
                if (written > entry.Length) throw new InvalidDataException("Mirror entry exceeds its declared size.");
                output.Write(buffer, 0, count);
            }
            if (written != entry.Length) throw new InvalidDataException("Mirror entry is truncated.");
        }
    }
}
