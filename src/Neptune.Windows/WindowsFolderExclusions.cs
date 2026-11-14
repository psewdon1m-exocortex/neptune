using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;

namespace Neptune.Windows;

// Desired exclusions live with the profile, independently of a server binding or
// journal replacement. A missing file preserves an older guarded policy; damaged
// settings stop synchronization instead of silently including private folders.
internal sealed class WindowsFolderExclusions(string stateDirectory)
{
    private sealed record Saved(int Version, string MappingId, string ClientId, string Root, string[] Paths);
    internal static string[] Normalize(IEnumerable<string> paths)
    {
        var result = paths.Select(p => p.Replace('\\', '/').TrimEnd('/'))
            .Distinct(StringComparer.OrdinalIgnoreCase).Order(StringComparer.Ordinal).ToArray();
        new MappingSyncPolicy("legacy", result).Validate();
        return result;
    }

    internal static string SelectedFolder(string root, string folder)
    {
        root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(root));
        folder = Path.TrimEndingDirectorySeparator(Path.GetFullPath(folder));
        if (folder.Equals(root, StringComparison.OrdinalIgnoreCase)
            || !folder.StartsWith(root.TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Choose a subfolder inside this synchronized folder. To stop the whole folder, turn Mirror off.");
        WindowsGuardedCapabilities.RequirePlainAncestors(folder);
        if (!Directory.Exists(folder)) throw new DirectoryNotFoundException("The selected folder no longer exists.");
        return Normalize([Path.GetRelativePath(root, folder)])[0];
    }

    internal string FileName(SyncMapping mapping) => Path.Combine(stateDirectory, "folder-exclusions", Key(mapping.MappingId) + ".json");
    private static string Key(string value) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
    private static string Root(SyncMapping mapping) => Path.TrimEndingDirectorySeparator(Path.GetFullPath(mapping.LocalPath));

    internal string[]? Read(SyncMapping mapping)
    {
        var file = FileName(mapping);
        WindowsGuardedCapabilities.RequirePlainAncestors(file);
        if (!File.Exists(file)) return null;
        if (new FileInfo(file).Length > 2 * 1024 * 1024) throw new InvalidDataException("Folder exclusions exceed their bound.");
        var raw = File.ReadAllText(file);
        using var document = JsonDocument.Parse(raw);
        if (document.RootElement.ValueKind != JsonValueKind.Object
            || document.RootElement.EnumerateObject().GroupBy(p => p.Name, StringComparer.Ordinal).Any(g => g.Count() != 1))
            throw new InvalidDataException("Invalid folder exclusions document.");
        var saved = JsonSerializer.Deserialize<Saved>(raw) ?? throw new InvalidDataException("Folder exclusions are missing.");
        if (saved.Version != 1 || saved.MappingId != mapping.MappingId || saved.ClientId != mapping.ClientInstanceId
            || !string.Equals(saved.Root, Root(mapping), StringComparison.OrdinalIgnoreCase) || saved.Paths is null)
            throw new InvalidDataException("Folder exclusions belong to a different mapping or unsupported format.");
        new MappingSyncPolicy("legacy", saved.Paths).Validate();
        return Normalize(saved.Paths);
    }

    internal void Write(SyncMapping mapping, IEnumerable<string> paths)
    {
        var saved = new Saved(1, mapping.MappingId, mapping.ClientInstanceId, Root(mapping), Normalize(paths));
        var file = FileName(mapping);
        WindowsGuardedCapabilities.RequirePlainAncestors(file);
        Directory.CreateDirectory(Path.GetDirectoryName(file)!);
        var temporary = file + "." + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            using (var stream = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            { JsonSerializer.Serialize(stream, saved, new JsonSerializerOptions { WriteIndented = true }); stream.Flush(true); }
            File.Move(temporary, file, overwrite: true);
        }
        finally { if (File.Exists(temporary)) File.Delete(temporary); }
    }

    // Upgrade path for exclusions previously entered in Review. Read only the
    // current scope in each retained namespace, never backups or retired scopes.
    internal async Task<string[]> ReadRetainedAsync(SyncMapping mapping, CancellationToken ct)
    {
        var directory = Path.Combine(stateDirectory, "guarded-sync");
        WindowsGuardedCapabilities.RequirePlainAncestors(directory);
        if (!Directory.Exists(directory)) return [];
        string[]? found = null;
        var namespaces = Directory.GetDirectories(directory);
        if (namespaces.Length > 256) throw new InvalidDataException("Too many retained sync namespaces.");
        foreach (var ns in namespaces)
        {
            var database = Path.Combine(ns, Key(mapping.MappingId), "cycle.db");
            WindowsGuardedCapabilities.RequirePlainAncestors(database);
            if (!File.Exists(database)) continue;
            await using var connection = new SqliteConnection(new SqliteConnectionStringBuilder
                { DataSource = database, Mode = SqliteOpenMode.ReadOnly, Pooling = false }.ToString());
            await connection.OpenAsync(ct);
            using var transaction = connection.BeginTransaction(deferred: true);
            await using var q = connection.CreateCommand(); q.Transaction = transaction;
            q.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE name IN ('local_cycle_active','client_mapping_policies')";
            var tables = Convert.ToInt64(await q.ExecuteScalarAsync(ct));
            if (tables == 0) continue;
            if (tables != 2) throw new InvalidDataException("Incomplete retained exclusion policy schema.");
            q.CommandText = "SELECT p.policy_json,p.policy_hash FROM client_mapping_policies p JOIN local_cycle_active a ON a.scope_id=p.scope_id";
            await using var reader = await q.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var policy = JsonSerializer.Deserialize<MappingSyncPolicy>(reader.GetString(0), new JsonSerializerOptions(JsonSerializerDefaults.Web))
                    ?? throw new InvalidDataException("Retained exclusions missing.");
                policy.Validate();
                if (policy.Hash != reader.GetString(1)) throw new InvalidDataException("Retained exclusions changed.");
                var paths = Normalize(policy.ExcludedPrefixes);
                if (found is not null && !found.SequenceEqual(paths))
                    throw new InvalidOperationException("Retained servers have different exclusions. Connect this mapping before editing its exclusions.");
                found = paths;
            }
        }
        return found ?? [];
    }
}
