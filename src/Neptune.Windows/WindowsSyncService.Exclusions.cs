using Neptune.Core;

namespace Neptune.Windows;

public sealed record WindowsFolderExclusionResult(bool Applied, string? PendingReason);

public sealed partial class WindowsSyncService
{
    private WindowsFolderExclusions FolderExclusions => new(profile.StateDirectory);

    public async Task<string[]> ReadExcludedFoldersAsync(SyncMapping mapping, CancellationToken ct = default)
    {
        await _syncGate.WaitAsync(ct);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return FolderExclusions.Read(mapping) ?? (_guardedSessions.TryGetValue(mapping.MappingId, out var session)
                ? (await session.ReadPolicyAsync(ct)).ExcludedPrefixes.ToArray()
                : await FolderExclusions.ReadRetainedAsync(mapping, ct));
        }
        finally { _syncGate.Release(); }
    }

    public async Task<WindowsFolderExclusionResult> SetExcludedFoldersAsync(SyncMapping mapping,
        IEnumerable<string> paths, IReadOnlyList<string> expected, CancellationToken ct = default)
    {
        var desired = WindowsFolderExclusions.Normalize(paths);
        await _syncGate.WaitAsync(ct);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var previous = FolderExclusions.Read(mapping) ?? (_guardedSessions.TryGetValue(mapping.MappingId, out var current)
                ? (await current.ReadPolicyAsync(ct)).ExcludedPrefixes.ToArray()
                : await FolderExclusions.ReadRetainedAsync(mapping, ct));
            if (!WindowsFolderExclusions.Normalize(previous).SequenceEqual(WindowsFolderExclusions.Normalize(expected)))
                throw new InvalidOperationException("The exclusions changed while this window was open. Reopen the list before saving.");
            // Durable desired policy first: a crash or retained original can delay
            // application, but must never start uploads under the previous list.
            FolderExclusions.Write(mapping, desired);
            if (_guardedSessions.TryGetValue(mapping.MappingId, out var session))
            {
                try { await ApplyFolderExclusionsAsync(mapping, session, ct); }
                catch (Exception error) when (error is InvalidOperationException or InvalidDataException)
                { return new(false, error.Message); }
            }
            MappingWorkAvailable?.Invoke(mapping.MappingId);
            return new(true, null);
        }
        finally { _syncGate.Release(); }
    }

    private async Task ApplyFolderExclusionsAsync(SyncMapping mapping, WindowsGuardedMappingSession session, CancellationToken ct)
    {
        var current = await session.ReadPolicyAsync(ct);
        var desired = FolderExclusions.Read(mapping);
        if (desired is null) { FolderExclusions.Write(mapping, current.ExcludedPrefixes); return; }
        var proposed = current with { ExcludedPrefixes = desired };
        if (proposed.Hash == current.Hash) return;
        var preview = await session.PreviewPolicyAsync(proposed, ct);
        await session.ApplyPolicyAsync(proposed, preview.Digest, ct);
    }
}
