namespace Neptune.Core;

// A bounded metadata producer. Data execution is intentionally a separate gate:
// eligibility means sibling names were validated, not server parents prepared,
// publication acknowledged, or an authoritative local capture cut established.
public sealed class NeptuneBootstrapScanner(NeptuneStateStore state, IBootstrapScanSource? source = null)
{
    private readonly IBootstrapScanSource _source = source ?? new FileSystemBootstrapScanSource();

    public Task<BootstrapScanState> ScanAsync(string mappingId, string clientInstanceId, string sourceRoot,
        Func<BootstrapDirectory, CancellationToken, Task>? branchEligible = null, CancellationToken cancellationToken = default)
        => Task.Run(() => ScanCoreAsync(mappingId, clientInstanceId, sourceRoot, branchEligible, cancellationToken), CancellationToken.None);

    private async Task<BootstrapScanState> ScanCoreAsync(string mappingId, string clientInstanceId, string sourceRoot,
        Func<BootstrapDirectory, CancellationToken, Task>? branchEligible, CancellationToken cancellationToken)
    {
        await using var ownership = await state.AcquireBootstrapOwnerAsync(mappingId, cancellationToken);
        await state.RecoverBootstrapOwnerAsync(mappingId, cancellationToken);
        var scan = await state.BeginBootstrapScanAsync(Guid.NewGuid().ToString("N"), mappingId, clientInstanceId, sourceRoot,
            Guid.NewGuid().ToString("N"), cancellationToken);
        try
        {
            // One enumerator and at most 256 metadata objects; the directory
            // frontier and eligible files live in SQLite, not Stack/ToArray.
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                // Refill transactions are independently bounded, including
                // empty-parent completion. No tree-wide INSERT SELECT barrier.
                while (await state.RefillBootstrapFrontierAsync(scan.ScanId, scan.OwnerToken, cancellationToken) is { AddedDirectories: 0 })
                    cancellationToken.ThrowIfCancellationRequested();
                var directory = await state.ClaimBootstrapDirectoryAsync(scan.ScanId, scan.OwnerToken, cancellationToken);
                if (directory is null) break;
                var fullPath = DirectoryPath(scan.SourceRoot, directory.RelativePath);
                var before = _source.ReadDirectoryStamp(fullPath);
                var batch = new List<BootstrapSourceEntry>(NeptuneStateStore.BootstrapMetadataBatchLimit);
                foreach (var entry in _source.EnumerateDirectory(fullPath))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    batch.Add(entry);
                    if (batch.Count == NeptuneStateStore.BootstrapMetadataBatchLimit)
                    {
                        await state.StageBootstrapEntriesAsync(directory, batch, cancellationToken);
                        await state.RetireBootstrapBatchAsync(mappingId, cancellationToken);
                        batch.Clear();
                    }
                }
                if (batch.Count != 0)
                {
                    await state.StageBootstrapEntriesAsync(directory, batch, cancellationToken);
                    await state.RetireBootstrapBatchAsync(mappingId, cancellationToken);
                }
                if (_source.ReadDirectoryStamp(fullPath) != before)
                    throw new IOException("The directory changed during enumeration; a fresh generation is required.");
                await state.CompleteBootstrapDirectoryAsync(directory, cancellationToken);
                if (branchEligible is not null) await branchEligible(directory, cancellationToken);
            }
            await state.FinishBootstrapScanAsync(scan.ScanId, scan.OwnerToken, BootstrapScanStatus.EnumerationComplete, cancellationToken: cancellationToken);
        }
        catch (OperationCanceledException)
        {
            await state.FinishBootstrapScanAsync(scan.ScanId, scan.OwnerToken, BootstrapScanStatus.Interrupted, "Scanner cancelled; enumeration is incomplete.", CancellationToken.None);
            throw;
        }
        catch (Exception error)
        {
            var message = error.Message.Length > 2048 ? error.Message[..2048] : error.Message;
            await state.FinishBootstrapScanAsync(scan.ScanId, scan.OwnerToken, BootstrapScanStatus.Incomplete, message, CancellationToken.None);
            throw;
        }
        return (await state.GetBootstrapScanAsync(scan.ScanId, cancellationToken))!;
    }

    private static string DirectoryPath(string root, string relative)
    {
        var resolved = Path.GetFullPath(Path.Combine(root, relative.Replace('/', Path.DirectorySeparatorChar)));
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        var prefix = Path.EndsInDirectorySeparator(root) ? root : root + Path.DirectorySeparatorChar;
        if (!string.Equals(resolved, root, comparison) && !resolved.StartsWith(prefix, comparison))
            throw new InvalidDataException("A directory queue entry escaped its source root.");
        return resolved;
    }
}

public sealed class FileSystemBootstrapScanSource : IBootstrapScanSource
{
    public BootstrapDirectoryStamp ReadDirectoryStamp(string fullPath)
    {
        var info = new DirectoryInfo(fullPath);
        if (!info.Exists || (info.Attributes & FileAttributes.ReparsePoint) != 0) throw new IOException("The scan directory is missing or became a reparse point.");
        return new(new DateTimeOffset(info.CreationTimeUtc, TimeSpan.Zero), new DateTimeOffset(info.LastWriteTimeUtc, TimeSpan.Zero));
    }

    public IEnumerable<BootstrapSourceEntry> EnumerateDirectory(string fullPath)
    {
        // Preserve the existing no-reparse-point policy. Access errors are not
        // skipped: they make the generation incomplete and block all cleanup.
        foreach (var path in Directory.EnumerateFileSystemEntries(fullPath, "*", new EnumerationOptions
        {
            RecurseSubdirectories = false, IgnoreInaccessible = false, AttributesToSkip = 0, ReturnSpecialDirectories = false
        }))
        {
            var attributes = File.GetAttributes(path);
            if ((attributes & FileAttributes.ReparsePoint) != 0) continue;
            var directory = (attributes & FileAttributes.Directory) != 0;
            var info = new FileInfo(path);
            yield return new(Path.GetFileName(path), directory, directory ? 0 : info.Length, new DateTimeOffset(info.LastWriteTimeUtc, TimeSpan.Zero));
        }
    }
}
