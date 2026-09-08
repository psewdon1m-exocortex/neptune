using System.Text.Json;
using System.Net.Http;
using System.Security.Cryptography;
using Neptune.Core;

namespace Neptune.Windows;

public sealed class WindowsSyncService(WindowsProfileContext profile)
{
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromHours(2) };

    public async Task<int> SyncAsync(
        string clientInstanceId,
        IReadOnlyList<SyncMapping> mappings,
        WindowsConnection connection,
        IProgress<string>? progress = null,
        CancellationToken cancellationToken = default)
    {
        var register = new KernelRegisterClient(_http, Path.Combine(profile.StateDirectory, "register-lkg.json"));
        JsonDocument snapshot;
        try { snapshot = await register.GetSnapshotAsync(connection.KernelOrigin, connection.KernelToken, cancellationToken); }
        catch when (File.Exists(Path.Combine(profile.StateDirectory, "register-lkg.json"))) { snapshot = register.GetLastKnownGood(); }
        using (snapshot)
        {
            var state = new NeptuneStateStore(Path.Combine(profile.StateDirectory, "neptune.db"));
            await state.InitializeAsync(cancellationToken);
            var values = snapshot.RootElement.GetProperty("values");
            var saturnOrigin = RegisterValues.HttpsOrigin(values, "saturn");
            var syncPath = RegisterValues.RequiredString(values, "services.saturn.paths.sync");
            var syncRoot = new Uri(saturnOrigin, syncPath.TrimEnd('/') + "/");
            var client = new WebDavSyncClient(_http);
            var uploaded = 0;
            var failures = 0;
            foreach (var mapping in mappings.Where(item => item.Enabled))
            {
                foreach (var file in SafeFiles(mapping.LocalPath))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var relative = Path.GetRelativePath(mapping.LocalPath, file);
                    var info = new FileInfo(file);
                    var modifiedAt = new DateTimeOffset(info.LastWriteTimeUtc, TimeSpan.Zero);
                    var previous = await state.GetSyncFileAsync(mapping.MappingId, relative, cancellationToken);
                    if (previous is not null && previous.State == "synced" && previous.LocalSize == info.Length && previous.LocalModifiedAt == modifiedAt)
                    {
                        var remoteEtag = await client.ReadEtagAsync(WebDavSyncClient.TargetUri(syncRoot, clientInstanceId, mapping.MappingId, relative), connection.SaturnToken, cancellationToken);
                        if (remoteEtag is not null && remoteEtag == previous.RemoteEtag) continue;
                    }
                    progress?.Report($"{Path.GetFileName(mapping.LocalPath)} · {relative}");
                    string hash;
                    await using (var source = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read, 128 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan))
                        hash = Convert.ToHexStringLower(await SHA256.HashDataAsync(source, cancellationToken));
                    var afterHash = new FileInfo(file);
                    if (afterHash.Length != info.Length || afterHash.LastWriteTimeUtc != info.LastWriteTimeUtc)
                    {
                        failures++;
                        await state.UpsertSyncFileAsync(new SyncFileRecord(mapping.MappingId, relative, info.Length, modifiedAt, hash, previous?.RemoteEtag, "retry-wait", DateTimeOffset.UtcNow, "File changed while being read."), cancellationToken);
                        continue;
                    }
                    try
                    {
                        var etag = await client.UploadAsync(syncRoot, connection.SaturnToken, clientInstanceId, mapping.MappingId, relative, file, cancellationToken);
                        var afterUpload = new FileInfo(file);
                        if (afterUpload.Length != info.Length || afterUpload.LastWriteTimeUtc != info.LastWriteTimeUtc)
                        {
                            failures++;
                            await state.UpsertSyncFileAsync(new SyncFileRecord(mapping.MappingId, relative, afterUpload.Length, new DateTimeOffset(afterUpload.LastWriteTimeUtc, TimeSpan.Zero), hash, etag, "retry-wait", DateTimeOffset.UtcNow, "File changed while being uploaded."), cancellationToken);
                            continue;
                        }
                        await state.UpsertSyncFileAsync(new SyncFileRecord(mapping.MappingId, relative, info.Length, modifiedAt, hash, etag, "synced", DateTimeOffset.UtcNow, null), cancellationToken);
                        uploaded++;
                    }
                    catch (Exception error) when (error is not OperationCanceledException)
                    {
                        failures++;
                        await state.UpsertSyncFileAsync(new SyncFileRecord(mapping.MappingId, relative, info.Length, modifiedAt, hash, previous?.RemoteEtag, "retry-wait", DateTimeOffset.UtcNow, error.Message), cancellationToken);
                    }
                }
            }
            if (failures > 0) throw new IOException($"{failures} file(s) could not be synchronized and remain queued for retry.");
            return uploaded;
        }
    }

    private static IEnumerable<string> SafeFiles(string root)
    {
        var pending = new Stack<string>();
        pending.Push(root);
        while (pending.Count > 0)
        {
            var directory = pending.Pop();
            IEnumerable<string> entries;
            try { entries = Directory.EnumerateFileSystemEntries(directory).ToArray(); }
            catch (UnauthorizedAccessException) { continue; }
            catch (DirectoryNotFoundException) { continue; }
            foreach (var entry in entries)
            {
                FileAttributes attributes;
                try { attributes = File.GetAttributes(entry); }
                catch (IOException) { continue; }
                catch (UnauthorizedAccessException) { continue; }
                if ((attributes & FileAttributes.ReparsePoint) != 0) continue;
                if ((attributes & FileAttributes.Directory) != 0) pending.Push(entry);
                else yield return entry;
            }
        }
    }
}
