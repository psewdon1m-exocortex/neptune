using System.Text.Json;
using System.Net.Http;
using System.Security.Cryptography;
using Neptune.Core;

namespace Neptune.Windows;

public sealed record WindowsSyncResult(int UploadedFiles, string AccentColor);

public sealed class WindowsSyncService(WindowsProfileContext profile)
{
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromHours(2) };

    public async Task<string> ConnectAsync(string clientInstanceId, WindowsConnection connection, CancellationToken cancellationToken = default)
    {
        var prepared = await PrepareAsync(clientInstanceId, connection, cancellationToken);
        return prepared.AccentColor;
    }

    public async Task<WindowsSyncResult> SyncAsync(
        string clientInstanceId,
        IReadOnlyList<SyncMapping> mappings,
        WindowsConnection connection,
        IProgress<string>? progress = null,
        CancellationToken cancellationToken = default)
    {
        var prepared = await PrepareAsync(clientInstanceId, connection, cancellationToken);
        {
            var state = new NeptuneStateStore(Path.Combine(profile.StateDirectory, "neptune.db"));
            await state.InitializeAsync(cancellationToken);
            var client = new WebDavSyncClient(_http);
            var uploaded = 0;
            var failures = 0;
            foreach (var mapping in mappings.Where(item => item.Enabled))
            {
                var mappingName = Path.GetFileName(mapping.LocalPath.TrimEnd(Path.DirectorySeparatorChar));
                if (string.IsNullOrWhiteSpace(mappingName)) mappingName = "root";
                if (mappings.Any(other => other.MappingId != mapping.MappingId && string.Equals(Path.GetFileName(other.LocalPath.TrimEnd(Path.DirectorySeparatorChar)), mappingName, StringComparison.OrdinalIgnoreCase)))
                    throw new InvalidOperationException($"Two selected directories cannot use the same Saturn folder name '{mappingName}'.");
                await client.EnsureDirectoryAsync(prepared.NamespaceRoot, connection.SaturnToken, mappingName, cancellationToken);
                var mappingRoot = new Uri(prepared.NamespaceRoot, Uri.EscapeDataString(mappingName) + "/");
                var entries = SafeEntries(mapping.LocalPath).ToArray();
                var localFiles = entries.Where(value => !value.IsDirectory).Select(value => value.RelativePath).ToHashSet(StringComparer.Ordinal);
                var localDirectories = entries.Where(value => value.IsDirectory).Select(value => value.RelativePath).ToHashSet(StringComparer.Ordinal);
                foreach (var directory in localDirectories.OrderBy(value => value.Count(character => character == '/')))
                    await client.EnsureDirectoryAsync(mappingRoot, connection.SaturnToken, directory, cancellationToken);
                foreach (var entry in entries.Where(value => !value.IsDirectory))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var file = entry.FullPath;
                    var relative = entry.RelativePath;
                    var info = new FileInfo(file);
                    var modifiedAt = new DateTimeOffset(info.LastWriteTimeUtc, TimeSpan.Zero);
                    var previous = await state.GetSyncFileAsync(mapping.MappingId, relative, cancellationToken);
                    if (previous is not null && previous.State == "synced" && previous.LocalSize == info.Length && previous.LocalModifiedAt == modifiedAt)
                    {
                        var remoteEtag = await client.ReadEtagAsync(WebDavSyncClient.RelativeTargetUri(mappingRoot, relative), connection.SaturnToken, cancellationToken);
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
                        var etag = await client.UploadAsync(mappingRoot, connection.SaturnToken, relative, file, cancellationToken);
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
                try { await client.MirrorAsync(mappingRoot, connection.SaturnToken, localFiles, localDirectories, protectMassDeletion: false, cancellationToken: cancellationToken); }
                catch (Exception error) when (error is not OperationCanceledException)
                {
                    failures++;
                    progress?.Report($"{mappingName} · remote cleanup failed: {error.Message}");
                }
            }
            if (failures > 0) throw new IOException($"{failures} file(s) could not be synchronized and remain queued for retry.");
            return new WindowsSyncResult(uploaded, prepared.AccentColor);
        }
    }

    private async Task<PreparedConnection> PrepareAsync(string clientInstanceId, WindowsConnection connection, CancellationToken cancellationToken)
    {
        var register = new KernelRegisterClient(_http, Path.Combine(profile.StateDirectory, "register-lkg.json"));
        var snapshot = await register.GetSnapshotAsync(connection.KernelOrigin, connection.KernelToken, cancellationToken);
        using (snapshot)
        {
            var values = snapshot.RootElement.GetProperty("values");
            var saturnOrigin = RegisterValues.HttpsOrigin(values, "saturn");
            var syncPath = RegisterValues.RequiredString(values, "services.saturn.paths.sync");
            string preferencesPath;
            try { preferencesPath = RegisterValues.RequiredString(values, "services.saturn.paths.sync_preferences"); }
            catch (InvalidDataException) { preferencesPath = "/api/v1/sync/preferences"; }
            var client = new WebDavSyncClient(_http);
            var syncRoot = new Uri(saturnOrigin, syncPath.TrimEnd('/') + "/");
            var namespaceRoot = await client.ClaimNamespaceAsync(syncRoot, connection.SaturnToken, connection.RemoteFolder, clientInstanceId, cancellationToken);
            var accent = "#00A8FF";
            try { accent = await client.ReadAccentAsync(new Uri(saturnOrigin, preferencesPath), connection.SaturnToken, cancellationToken); }
            catch (HttpRequestException error) when (error.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                // Older Saturn deployments have no preferences endpoint. The next
                // successful connection after Saturn is upgraded replaces this fallback.
            }
            return new PreparedConnection(namespaceRoot, accent);
        }
    }

    private static IEnumerable<LocalEntry> SafeEntries(string root)
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
                var relative = Path.GetRelativePath(root, entry).Replace('\\', '/');
                if ((attributes & FileAttributes.Directory) != 0)
                {
                    yield return new LocalEntry(entry, relative, true);
                    pending.Push(entry);
                }
                else yield return new LocalEntry(entry, relative, false);
            }
        }
    }

    private sealed record PreparedConnection(Uri NamespaceRoot, string AccentColor);
    private sealed record LocalEntry(string FullPath, string RelativePath, bool IsDirectory);
}
