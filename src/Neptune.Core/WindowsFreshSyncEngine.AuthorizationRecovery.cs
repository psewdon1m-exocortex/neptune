using System.Text.Json;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

public sealed partial class WindowsFreshSyncEngine
{
    // Called only on the qualified, exclusively owned scope. Heartbeat alone
    // grants no authority to reset its durable waits. Every original is probed
    // before its scheduler row is released; attempts and originals are retained.
    public async Task ResumeAuthorizedOperationsAsync(CancellationToken ct = default)
    {
        await _serial.WaitAsync(ct);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_remote.Binding != _configuration.Binding) throw new InvalidDataException("Authorization probe changed binding.");
            while (true)
            {
                var waits = await _session.ReadAuthorizationWaitsAsync(ScopeId, ct);
                if (waits.Count == 0) break;
                foreach (var wait in waits)
                {
                    if (wait.Kind == "cycle") await SyncRemote(ct);
                    else
                    {
                        var mutation = await _session.ReadContentMutationAsync(ScopeId, wait.Operation, ct);
                        if (mutation is not null)
                        {
                            if (_remote is not IWindowsCycleExtensions extensions) throw new InvalidDataException("Original mutation transport is unavailable.");
                            try { _ = await Admission(() => extensions.MutateAsync(mutation.Request, true, ct), ct); }
                            catch (GuardedCycleRemoteException e) when (e.StructuredWireCode && e.Status == System.Net.HttpStatusCode.Conflict && e.Code == "operation_reused")
                            {
                                // Status also uses this code when admission was blocked.
                                // Execute resolves only this immutable original; a real
                                // identity mismatch still fails closed on the server.
                                _ = await Admission(() => extensions.MutateAsync(mutation.Request, false, ct), ct);
                            }
                        }
                        else
                        {
                            var work = await _session.ReadCycleWorkAsync(ScopeId, wait.Operation, ct);
                            var stream = await _session.ReadStreamAsync(ScopeId, wait.Operation, ct);
                            if (stream is not null)
                            {
                                if (_remote is not IWindowsStreamRemote streaming) throw new InvalidDataException("Original stream transport is unavailable.");
                                try { _ = await Admission(() => streaming.StreamCommandAsync("status", stream.Input, null, ct), ct); }
                                catch (GuardedCycleRemoteException e) when (e.StructuredWireCode && e.Status == System.Net.HttpStatusCode.Conflict && e.Code == "operation_reused")
                                { _ = await Admission(() => streaming.StreamCommandAsync("prepare", stream.Input, null, ct), ct); }
                            }
                            else if (work.InputJson is not null && !work.Entry.Directory)
                            {
                                var input = JsonSerializer.Deserialize<CycleUploadInput>(work.InputJson, LocalCycleWire.Json)!;
                                // Without a returned upload id, the protocol's idempotent
                                // prepare resolves the SAME retained key, never a new intent.
                                _ = await Admission(() => _remote.UploadAsync(work.UploadId is null ? "prepare" : "inspect", input, work.UploadId, work.IntentHash!, ct), ct);
                            }
                            else if (work.InputJson is not null)
                            {
                                var input = JsonSerializer.Deserialize<CycleFolderInput>(work.InputJson, LocalCycleWire.Json)!;
                                _ = await Admission(() => _remote.EnsureDirectoryAsync(input, ct), ct);
                            }
                            else await SyncRemote(ct); // No attempted immutable operation exists yet.
                        }
                    }
                    ct.ThrowIfCancellationRequested();
                    await _session.ResumeAuthorizedOperationAsync(ScopeId, wait, ct);
                }
                if (waits.Count < 120) break;
            }
            _schedule = await _session.ReadOperationScheduleAsync(ScopeId, ct);
            _watcher.ScheduleRetry(_schedule.NextAttemptUnixMilliseconds);
        }
        finally { _serial.Release(); }
    }
}
