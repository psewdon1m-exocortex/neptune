using System.Text.Json;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private string BootstrapWorkBackupManifestPath => databasePath + ".bootstrap-work-backup.json";
    private string BootstrapReceiptBackupManifestPath => databasePath + ".bootstrap-work-v2-backup.json";
    private string BootstrapNativeBackupManifestPath => databasePath + ".bootstrap-work-v3-backup.json";
    private sealed record BootstrapWorkBackup(string SourcePath, string BackupPath, string Sha256, int TargetFormat, DateTimeOffset CreatedAt);

    private async Task BackupBeforeBootstrapWorkAsync(CancellationToken ct, int targetFormat = 1)
    {
        if (targetFormat is not (1 or 2 or 3)) throw new ArgumentOutOfRangeException(nameof(targetFormat));
        var temporary = databasePath + ".bootstrap-work-" + Guid.NewGuid().ToString("N") + ".partial";
        try
        {
            // Backup API, rather than copying .db, includes committed WAL and
            // validates a consistent source snapshot before additive DDL.
            await CreateConsistentSyncDatabaseCopyAsync(databasePath, temporary, ct);
            var digest = await HashSyncBackupAsync(temporary, ct);
            var target = databasePath + ".pre-bootstrap-work-v" + targetFormat + "-" + digest + ".db";
            if (File.Exists(target))
            { if (await HashSyncBackupAsync(target, ct) != digest) throw new InvalidDataException("Execution backup was modified."); File.Delete(temporary); }
            else File.Move(temporary, target);
            var manifest = new BootstrapWorkBackup(Path.GetFullPath(databasePath), Path.GetFullPath(target), digest, targetFormat, DateTimeOffset.UtcNow);
            var manifestPath = targetFormat == 1 ? BootstrapWorkBackupManifestPath : targetFormat == 2 ? BootstrapReceiptBackupManifestPath : BootstrapNativeBackupManifestPath;
            var pending = manifestPath + "." + Guid.NewGuid().ToString("N") + ".partial";
            try
            {
                await File.WriteAllTextAsync(pending, JsonSerializer.Serialize(manifest), ct);
                using (var stream = new FileStream(pending, FileMode.Open, FileAccess.Write, FileShare.None)) stream.Flush(true);
                File.Move(pending, manifestPath, overwrite: true);
            }
            finally { if (File.Exists(pending)) File.Delete(pending); }
        }
        finally { if (File.Exists(temporary)) File.Delete(temporary); }
    }

    // Offline recovery creates a separate pre-execution-module profile. It
    // preserves the active DB/WAL and never discards acknowledged outcomes.
    public async Task<string> CreatePreBootstrapWorkRollbackCopyAsync(string destinationPath, CancellationToken cancellationToken = default)
        => await CreateBootstrapWorkRollbackCopyAsync(destinationPath,File.Exists(BootstrapWorkBackupManifestPath) ? 1 : 2,cancellationToken);

    public async Task<string> CreatePreBootstrapReceiptRollbackCopyAsync(string destinationPath, CancellationToken cancellationToken = default)
        => await CreateBootstrapWorkRollbackCopyAsync(destinationPath,2,cancellationToken);

    public async Task<string> CreatePreNativeBootstrapCaptureRollbackCopyAsync(string destinationPath, CancellationToken cancellationToken = default)
        => await CreateBootstrapWorkRollbackCopyAsync(destinationPath,3,cancellationToken);

    private async Task<string> CreateBootstrapWorkRollbackCopyAsync(string destinationPath,int targetFormat,CancellationToken cancellationToken)
    {
        var target = Path.GetFullPath(destinationPath);
        if (string.Equals(target, Path.GetFullPath(databasePath), OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal)
            || File.Exists(target) || File.Exists(target + "-wal") || File.Exists(target + "-shm"))
            throw new InvalidOperationException("Rollback requires a separate absent database.");
        var manifestPath = targetFormat == 1 ? BootstrapWorkBackupManifestPath : targetFormat == 2 ? BootstrapReceiptBackupManifestPath : BootstrapNativeBackupManifestPath;
        if (targetFormat == 3)
        {
            var info = new FileInfo(manifestPath);
            if (info.Length is < 1 or > 16384) throw new InvalidDataException("Native capture backup manifest exceeds its fixed limit.");
            try
            {
                using var json = JsonDocument.Parse(await File.ReadAllTextAsync(manifestPath,cancellationToken),new JsonDocumentOptions {MaxDepth=4});
                if(json.RootElement.ValueKind != JsonValueKind.Object)throw new InvalidDataException("Native capture backup manifest is not an object.");
                var names = new HashSet<string>(StringComparer.Ordinal);
                foreach(var property in json.RootElement.EnumerateObject())
                    if(!names.Add(property.Name))throw new InvalidDataException("Native capture backup manifest has duplicate fields.");
                if(!names.SetEquals(["SourcePath","BackupPath","Sha256","TargetFormat","CreatedAt"]))throw new InvalidDataException("Native capture backup manifest is incompatible.");
            }
            catch(JsonException error){throw new InvalidDataException("Native capture backup manifest is malformed.",error);}
        }
        var manifest = JsonSerializer.Deserialize<BootstrapWorkBackup>(await File.ReadAllTextAsync(manifestPath, cancellationToken))
            ?? throw new InvalidDataException("Execution backup manifest is invalid.");
        if (manifest.SourcePath != Path.GetFullPath(databasePath) || manifest.TargetFormat != targetFormat
            || !Path.GetFullPath(manifest.BackupPath).StartsWith(Path.GetFullPath(databasePath) + ".pre-bootstrap-work-v" + targetFormat + "-", StringComparison.Ordinal)
            || await HashSyncBackupAsync(manifest.BackupPath, cancellationToken) != manifest.Sha256)
            throw new InvalidDataException("Execution backup identity/integrity was not verified.");
        Directory.CreateDirectory(Path.GetDirectoryName(target)!);
        // SQLite's Windows VFS must receive the extended form when a hashed
        // recovery filename exceeds MAX_PATH. The public manifest continues to
        // bind its ordinary absolute path; no shared migration helper changes.
        static string NativeRecoveryPath(string path) => OperatingSystem.IsWindows() && path.Length >= 260
            && !path.StartsWith("\\\\?\\", StringComparison.Ordinal) ? "\\\\?\\" + path : path;
        await CreateConsistentSyncDatabaseCopyAsync(targetFormat == 3 ? NativeRecoveryPath(manifest.BackupPath) : manifest.BackupPath,
            targetFormat == 3 ? NativeRecoveryPath(target) : target, cancellationToken);
        return target;
    }
}
