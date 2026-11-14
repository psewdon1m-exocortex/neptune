using System.Net;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class NetworkAcceptanceTests
{
    [Fact]
    public async Task N05_AuthorizationBeforeStreamAdmissionRecoversSameOriginal()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Requires native Windows fixture");
        var ct = TestContext.Current.CancellationToken; var clock = new Stage3Clock();
        await using var f = await WindowsFreshCycleTests.Fixture.Create(clock);
        f.Remote.StreamEnabled = true;
        var bytes = new byte[1024 * 1024 + 17]; new Random(5).NextBytes(bytes);
        await File.WriteAllBytesAsync(Path.Combine(f.Source, "stream"), bytes, ct);
        var remote = new AuthorizationRemote(f.Remote) { Status = 407 };
        await using var engine = await WindowsFreshSyncEngine.ResumeOrOpenAsync(new WindowsCycleNativeOptions(LocalJournalReadMode.IdentityOnly, true), f.Store, f.Source, Path.Combine(f.Directory, "spool"),
            new("mapping", "client", "audit", "rules-v1", new Uri("https://test.invalid/"), f.Remote.Device, "audit-token", "local-test", f.Remote.Binding), remote,
            new(LargeFileThreshold: 1024, MinimumFreeDiskBytes: 0), cancellationToken: ct);
        await engine.InitialSyncAsync(ct);
        Assert.Equal(1L, await f.Scalar("SELECT COUNT(*) FROM client_operation_schedule WHERE state='AUTH_REQUIRED'"));
        Assert.Empty(f.Remote.StreamSent);
        var original = await f.Scalar("SELECT input_json FROM client_stream_operations");
        var attempts = await f.Scalar("SELECT SUM(attempts) FROM client_operation_schedule");
        remote.Status = 0;
        await engine.ResumeAuthorizedOperationsAsync(ct);
        Assert.Equal(attempts, await f.Scalar("SELECT SUM(attempts) FROM client_operation_schedule"));
        await Stage3ChatBTests.AdvancePastDue(f, clock);
        var result = await engine.FreshSyncAsync(ct);
        Assert.Null(result.Error);
        Assert.Equal(1, f.Remote.Completes);
        Assert.Single(f.Remote.StreamSent);
        Assert.Equal(original, await f.Scalar("SELECT input_json FROM client_stream_operations"));
        Assert.Equal(Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(bytes)), await f.Scalar("SELECT sha256 FROM local_cycle_work WHERE phase=5"));
        Assert.Equal(new[] { "prepare", "status", "prepare", "status", "seal", "complete" }, remote.StreamCommands);
    }
    [Fact]
    public async Task N05_AuthorizationBeforeMutationAdmissionRecoversSameOriginal()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Requires native Windows fixture");
        var ct = TestContext.Current.CancellationToken; var clock = new Stage3Clock();
        await using var f = await WindowsFreshCycleTests.Fixture.Create(clock);
        f.Remote.ExtensionsEnabled = true;
        File.WriteAllText(Path.Combine(f.Source, "empty"), "");
        var remote = new AuthorizationRemote(f.Remote) { Status = 407 };
        await using var engine = await WindowsFreshSyncEngine.ResumeOrOpenAsync(new WindowsCycleNativeOptions(LocalJournalReadMode.IdentityOnly, true), f.Store, f.Source, Path.Combine(f.Directory, "spool"),
            new("mapping", "client", "audit", "rules-v1", new Uri("https://test.invalid/"), f.Remote.Device, "audit-token", "local-test", f.Remote.Binding), remote,
            new(MinimumFreeDiskBytes: 0), cancellationToken: ct);
        await engine.InitialSyncAsync(ct);
        Assert.Empty(f.Remote.Mutations);
        Assert.Equal(1L, await f.Scalar("SELECT COUNT(*) FROM client_operation_schedule WHERE state='AUTH_REQUIRED'"));
        var original = System.Text.Json.JsonSerializer.Serialize(remote.LastMutation);
        Assert.NotNull(remote.LastMutation);
        remote.Status = 0;
        await engine.ResumeAuthorizedOperationsAsync(ct);
        await Stage3ChatBTests.AdvancePastDue(f, clock);
        var result = await engine.FreshSyncAsync(ct);
        Assert.Null(result.Error);
        Assert.Single(f.Remote.Mutations);
        Assert.Equal(original, System.Text.Json.JsonSerializer.Serialize(f.Remote.Mutations[0]));
    }
    [Theory]
    [InlineData(407)]
    [InlineData(401)]
    [InlineData(-1)]
    public async Task N05_RestartReleasesOnlyQualifiedOriginalWithoutResettingAttempts(int status)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Requires native Windows fixture");
        var ct = TestContext.Current.CancellationToken;
        var clock = new Stage3Clock();
        await using var a = await WindowsFreshCycleTests.Fixture.Create(clock);
        await using var b = await WindowsFreshCycleTests.Fixture.Create();
        File.WriteAllText(Path.Combine(a.Source, "file"), "original A");
        File.WriteAllText(Path.Combine(b.Source, "file"), "original B");
        var ra = new AuthorizationRemote(a.Remote) { Status = status };
        var rb = new AuthorizationRemote(b.Remote) { Status = 403 };
        async Task<WindowsFreshSyncEngine> Open(WindowsFreshCycleTests.Fixture f, AuthorizationRemote remote)
            => await WindowsFreshSyncEngine.ResumeOrOpenAsync(new WindowsCycleNativeOptions(LocalJournalReadMode.IdentityOnly, true), f.Store, f.Source, Path.Combine(f.Directory, "spool"),
                new("mapping", "client", "audit", "rules-v1", new Uri("https://test.invalid/"), f.Remote.Device, "audit-token", "local-test", f.Remote.Binding), remote,
                new(MinimumFreeDiskBytes: 0), cancellationToken: ct);
        await using (var first = await Open(a, ra)) { await first.InitialSyncAsync(ct); }
        await using (var first = await Open(b, rb)) { await first.InitialSyncAsync(ct); }
        Assert.Equal(1L, await a.Scalar("SELECT COUNT(*) FROM client_operation_schedule WHERE state='AUTH_REQUIRED'"));
        var original = a.Remote.LastInput;
        var attempts = await a.Scalar("SELECT SUM(attempts) FROM client_operation_schedule");
        ra.Status = 0;
        await using var recovered = await Open(a, ra);
        await using var refused = await Open(b, rb);
        await recovered.ResumeAuthorizedOperationsAsync(ct);
        await Assert.ThrowsAsync<GuardedCycleRemoteException>(() => refused.ResumeAuthorizedOperationsAsync(ct));
        Assert.Equal(0L, await a.Scalar("SELECT COUNT(*) FROM client_operation_schedule WHERE state='AUTH_REQUIRED'"));
        Assert.Equal(attempts, await a.Scalar("SELECT SUM(attempts) FROM client_operation_schedule"));
        Assert.Equal(1L, await b.Scalar("SELECT COUNT(*) FROM client_operation_schedule WHERE state='AUTH_REQUIRED' AND not_before=9223372036854775807"));
        Assert.Equal(System.Text.Json.JsonSerializer.Serialize(original), System.Text.Json.JsonSerializer.Serialize(a.Remote.LastInput));
        await Stage3ChatBTests.AdvancePastDue(a, clock);
        var result = recovered.HasRetainedAnchor ? await recovered.FreshSyncAsync(ct) : await recovered.InitialSyncAsync(ct);
        Assert.Null(result.Error);
        Assert.Equal(1, a.Remote.Prepares);
        Assert.Equal(1, a.Remote.Completes);
    }

    private sealed class AuthorizationRemote(WindowsFreshCycleTests.FakeRemote inner) : IWindowsCycleRemote, IWindowsCycleExtensions, IWindowsStreamRemote
    {
        public int Status;
        public SyncMutationRequest? LastMutation;
        private bool _streamAdmitted;
        public readonly List<string> StreamCommands = [];
        public Task<SyncStreamStatus> StreamCommandAsync(string command, SyncStreamInput original, string? hash, CancellationToken ct)
        {
            StreamCommands.Add(command);
            if (Status != 0) throw new GuardedCycleRemoteException((HttpStatusCode)Status, "fixture_access");
            if (command == "status" && !_streamAdmitted) throw new GuardedCycleRemoteException(HttpStatusCode.Conflict, "operation_reused", true);
            if (command == "prepare") _streamAdmitted = true;
            return inner.StreamCommandAsync(command, original, hash, ct);
        }
        public Task<SyncStreamStatus> StreamChunkAsync(SyncStreamInput original, SyncStreamChunk chunk, ReadOnlyMemory<byte> bytes, CancellationToken ct)
            => inner.StreamChunkAsync(original, chunk, bytes, ct);
        public int NamespaceRevision => inner.NamespaceRevision;
        public SyncNamespacePolicy? NamespacePolicy => inner.NamespacePolicy;
        public bool Supports(string feature) => inner.Supports(feature);
        public Task<string> ApproveDeletePlanAsync(SyncDeletePlan original, CancellationToken ct) => inner.ApproveDeletePlanAsync(original, ct);
        public Task<SyncContentReference?> LookupContentAsync(long size, string hash, CancellationToken ct) => inner.LookupContentAsync(size, hash, ct);
        public Task<SyncFeedPage> WaitForChangesAsync(string cursor, long after, CancellationToken ct) => inner.WaitForChangesAsync(cursor, after, ct);
        public Task<SyncMutationReceipt> MutateAsync(SyncMutationRequest original, bool inspect, CancellationToken ct)
        {
            LastMutation = original;
            if (Status != 0) throw new GuardedCycleRemoteException((HttpStatusCode)Status, "fixture_access");
            return inner.MutateAsync(original, inspect, ct);
        }
        public SyncRootBinding Binding => inner.Binding;
        public Task<byte[]> StartSnapshotAsync(string id, CancellationToken ct) => inner.StartSnapshotAsync(id, ct);
        public Task<byte[]> StepSnapshotAsync(string id, CancellationToken ct) => inner.StepSnapshotAsync(id, ct);
        public Task<SyncSnapshotReady> ReadySnapshotAsync(string id, CancellationToken ct) => inner.ReadySnapshotAsync(id, ct);
        public Task<SyncSnapshotEntriesPage> ReadEntriesAsync(SyncSnapshotReady ready, string cursor, string? after, CancellationToken ct) => inner.ReadEntriesAsync(ready, cursor, after, ct);
        public Task<SyncFeedPage> ReadChangesAsync(string cursor, long after, CancellationToken ct) => inner.ReadChangesAsync(cursor, after, ct);
        public Task<CycleFolderOutcome> EnsureDirectoryAsync(CycleFolderInput input, CancellationToken ct) => inner.EnsureDirectoryAsync(input, ct);
        public async Task<CycleUploadStatus> UploadAsync(string command, CycleUploadInput input, string? upload, string hash, CancellationToken ct)
        {
            var result = await inner.UploadAsync(command, input, upload, hash, ct);
            if (Status == -1) throw new HttpRequestException(HttpRequestError.SecureConnectionError, "fixture TLS verification failure");
            if (Status != 0) throw new GuardedCycleRemoteException((HttpStatusCode)Status, "fixture_access");
            return result;
        }
        public Task<CycleUploadStatus> AppendAsync(CycleUploadInput input, string upload, string hash, long offset, ReadOnlyMemory<byte> bytes, CancellationToken ct)
            => inner.AppendAsync(input, upload, hash, offset, bytes, ct);
    }
}
