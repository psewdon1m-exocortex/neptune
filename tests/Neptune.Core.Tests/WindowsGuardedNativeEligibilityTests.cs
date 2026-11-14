using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Globalization;
using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("Native fresh cycle")]
public sealed partial class WindowsGuardedNativeEligibilityTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static Task<T> Medium<T>(Func<Task<T>> action) => (Task<T>)typeof(IdentityOnlyFreshCycleTests)
        .GetMethod("ReducedMedium", BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(typeof(T)).Invoke(null, [action])!;
    private static WindowsCycleConfiguration Config(WindowsFreshCycleTests.Fixture f) => new("mapping", "client", "audit",
        "windows-guarded-append-only-v1", new Uri("https://test.invalid/"), f.Remote.Device, "windows-device:" + f.Remote.Device,
        "local-test", f.Remote.Binding, AllowVerifiedEqualAdoption: true);

    [Fact]
    public async Task ActualReducedMediumEligibilityDoesNotCreateProfileOrEnumerateSource()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create();
        await Medium(async () =>
        {
            var unopened = Path.Combine(f.Directory, "unopened", "cycle.db");
            await using var eligibility = await WindowsGuardedSourceEligibility.OpenAsync(f.Source, unopened, Config(f), Ct);
            await eligibility.RequireCurrentAsync(Ct);
            Assert.Equal(1, eligibility.SubscriptionGeneration);
            Assert.False(Directory.Exists(Path.GetDirectoryName(unopened)));
            return true;
        });
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 1)]
    [InlineData(1, 2)]
    public async Task ActualReducedMediumRetainedContinuityIsReadonlyAndPreservesGeneration(int mode, int generation)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create();
        await Medium(async () =>
        {
            await Source(f, (LocalJournalReadMode)mode, generation);
            var before = await Hashes(f.Database);
            await using (var eligibility = await WindowsGuardedSourceEligibility.OpenAsync(f.Source, f.Database, Config(f), Ct))
            {
                await eligibility.RequireCurrentAsync(Ct);
                Assert.Equal(generation, eligibility.SubscriptionGeneration);
            }
            Assert.Equal(before, await Hashes(f.Database));
            return true;
        });
    }

    [Theory]
    [InlineData("durable-gap")]
    [InlineData("expired-cursor")]
    [InlineData("future-cursor")]
    [InlineData("journal")]
    [InlineData("profile")]
    [InlineData("client")]
    [InlineData("incarnation")]
    [InlineData("legacy-write")]
    [InlineData("unknown-format")]
    public async Task RetainedGapOrRebindRefusesWithoutChangingOriginalEvidence(string change)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create();
        await Source(f, LocalJournalReadMode.IdentityOnly, 1);
        var sql = change switch
        {
            "durable-gap" => "UPDATE local_source_roots SET valid=0,gap_epoch=gap_epoch+1,gap_code='JournalGap'",
            "expired-cursor" => "UPDATE local_source_roots SET cursor=0",
            "future-cursor" => "UPDATE local_source_roots SET cursor=9223372036854775807",
            "journal" => "UPDATE local_source_roots SET journal_id='0000000000000000'",
            "profile" => "UPDATE local_source_roots SET binding_json=json_set(binding_json,'$.profileId','another')",
            "client" => "UPDATE local_source_roots SET binding_json=json_set(binding_json,'$.clientId','another')",
            "incarnation" => "UPDATE local_source_roots SET binding_json=json_set(binding_json,'$.rootIncarnation',1)",
            "legacy-write" => "UPDATE local_source_revision SET revision=revision+1",
            "unknown-format" => "UPDATE local_source_format SET version=999",
            _ => throw new InvalidOperationException()
        };
        await using (var db = new SqliteConnection("Data Source=" + f.Database + ";Pooling=False"))
        {
            await db.OpenAsync(Ct); await using var q = db.CreateCommand(); q.CommandText = sql; await q.ExecuteNonQueryAsync(Ct);
        }
        var before = await Hashes(f.Database);
        if (change == "unknown-format")
            await Assert.ThrowsAsync<InvalidDataException>(() => WindowsGuardedSourceEligibility.OpenAsync(f.Source, f.Database, Config(f), Ct));
        else
            await Assert.ThrowsAsync<LocalSourceGapException>(() => WindowsGuardedSourceEligibility.OpenAsync(f.Source, f.Database, Config(f), Ct));
        Assert.Equal(before, await Hashes(f.Database));
    }

    private static async Task Source(WindowsFreshCycleTests.Fixture f, LocalJournalReadMode mode, int generation)
    {
        await using var native = await WindowsNativeCycleCapture.OpenAsync(f.Source, Ct);
        var root = native.RootProof;
        await using var reader = new WindowsUsnJournalReader(root.VolumeGuid, LocalJournalReadMode.IdentityOnly);
        var bounds = await reader.QueryAsync(Ct);
        var config = Config(f);
        var binding = new LocalSourceBinding(config.ProfileId, config.ClientId, config.MappingId, root.RootPath,
            root.VolumeGuid, root.Handle.LegacyVolumeSerial.ToString("x8", CultureInfo.InvariantCulture), "NTFS",
            root.Handle.FileId128, root.Handle.CreationTime, generation, config.RulesVersion, "windows-raw-nfc-v1");
        await using var session = await f.Store.OpenLocalJournalSessionAsync(binding, bounds, bounds.NextUsn,
            new(MinimumFreeDiskBytes: 10L << 30), mode, Ct);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PendingOrNeedsAttentionReplacementCannotEnterOrdinaryService(bool needsAttention)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create();
        var options = new WindowsCycleNativeOptions(LocalJournalReadMode.IdentityOnly, true);
        var budget = new WindowsCycleBudget(MinimumFreeDiskBytes: 10L << 30);
        var spool = Path.Combine(f.Directory, "spool");
        string original;
        await using (var engine = await WindowsFreshSyncEngine.OpenAsync(options, f.Store, f.Source, spool, Config(f), f.Remote, budget, cancellationToken: Ct))
        {
            Assert.Null((await engine.InitialSyncAsync(Ct)).Error); original = engine.ScopeId;
            var session = (LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(engine)!;
            await session.MarkGapAsync("JournalGap", Ct);
        }
        await using (var replacement = await WindowsFreshSyncEngine.OpenJournalReplacementAsync(options,
            new(Guid.NewGuid().ToString("N"), original), f.Store, f.Source, spool, Config(f), f.Remote, budget, cancellationToken: Ct))
        { Assert.Equal(LocalJournalReplacementStatus.SourceOpened, replacement.JournalReplacement!.Status); }
        if (needsAttention)
        {
            await using var db = new SqliteConnection("Data Source=" + f.Database + ";Pooling=False");
            await db.OpenAsync(Ct); await using var q = db.CreateCommand();
            q.CommandText = "UPDATE local_cycle_replacement_requests SET status=4,error='injected bounded attempts exhausted'";
            await q.ExecuteNonQueryAsync(Ct);
        }
        var before = await Hashes(f.Database);
        var error = await Assert.ThrowsAsync<LocalSourceGapException>(() => WindowsGuardedSourceEligibility.OpenAsync(f.Source, f.Database, Config(f), Ct));
        Assert.Equal("JournalReplacementPending", error.Code);
        Assert.Equal(before, await Hashes(f.Database));
    }

    private static async Task<string[]> Hashes(string database)
    {
        var result = new List<string>();
        foreach (var path in new[] { database, database + "-wal" })
        {
            // SQLite may create an empty WAL/shm for a read-only connection.
            // An empty WAL contains no committed evidence; compare every byte
            // whenever frames exist, including under a pinned reader below.
            if (!File.Exists(path) || path.EndsWith("-wal", StringComparison.Ordinal) && new FileInfo(path).Length == 0)
            { result.Add("no-wal-frames"); continue; }
            await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            result.Add(Convert.ToHexStringLower(await SHA256.HashDataAsync(stream, Ct)));
        }
        return result.ToArray();
    }

    [Fact]
    public async Task ReadonlyEligibilityPreservesActualHeldWalFrames()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create();
        await Source(f, LocalJournalReadMode.IdentityOnly, 1);
        await using var pin = new SqliteConnection("Data Source=" + f.Database + ";Pooling=False");
        await pin.OpenAsync(Ct); using var transaction = pin.BeginTransaction(deferred: true);
        await using (var read = pin.CreateCommand())
        {
            read.Transaction = transaction; read.CommandText = "SELECT cursor FROM local_source_roots LIMIT 1";
            Assert.NotNull(await read.ExecuteScalarAsync(Ct));
        }
        await using (var writer = new SqliteConnection("Data Source=" + f.Database + ";Pooling=False"))
        {
            await writer.OpenAsync(Ct); await using var update = writer.CreateCommand();
            update.CommandText = "UPDATE local_source_roots SET owner_epoch=owner_epoch+1"; await update.ExecuteNonQueryAsync(Ct);
        }
        Assert.True(new FileInfo(f.Database + "-wal").Length > 32);
        var before = await Hashes(f.Database);
        await using (var eligibility = await WindowsGuardedSourceEligibility.OpenAsync(f.Source, f.Database, Config(f), Ct))
            await eligibility.RequireCurrentAsync(Ct);
        Assert.Equal(before, await Hashes(f.Database));
    }

    [Fact]
    public async Task MissingSourceRefusesBeforeAnchorHttpAndProfileEffects()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await WindowsFreshCycleTests.Fixture.Create();
        var mapping = (await f.Store.ListMappingsAsync(Ct)).Single() with { LocalPath = Path.Combine(f.Directory, "missing-source") };
        var profile = new WindowsProfileContext("audit", Path.Combine(f.Directory, "unopened-profile"), false);
        var capability = new WindowsGuardedCapability(f.Remote.Binding, f.Remote.Device, "local-test", new string('a', 64), new(MinimumFreeDiskBytes: 10L << 30), 1L << 30);
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var requests = 0;
        var server = Task.Run(async () =>
        {
            try
            {
                while (!stop.IsCancellationRequested)
                {
                    using var client = await listener.AcceptTcpClientAsync(stop.Token);
                    Interlocked.Increment(ref requests);
                    await client.GetStream().WriteAsync(Encoding.ASCII.GetBytes("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"), stop.Token);
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        }, Ct);
        try
        {
            var origin = new Uri("http://127.0.0.1:" + ((IPEndPoint)listener.LocalEndpoint).Port);
            await Assert.ThrowsAnyAsync<Exception>(() => WindowsGuardedMappingSession.OpenAsync(profile, "client", mapping,
                origin, capability, new string('b', 43), "root", null, Ct));
            Assert.False(Directory.Exists(profile.StateDirectory));
            Assert.Equal(0, Volatile.Read(ref requests));
        }
        finally { stop.Cancel(); await server; }
    }
}
