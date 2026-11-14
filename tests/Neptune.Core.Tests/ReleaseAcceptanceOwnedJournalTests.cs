using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class LocalCoherenceTests
{
    [Fact]
    [Trait("Lane", "Live")]
    public async Task T10_ActualOwnedJournalResetFencesBothRetainedSubscriptionsWithoutChangingFiles()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Requires owned Windows NTFS volume");
        var proofFile = Environment.GetEnvironmentVariable("NEPTUNE_ACCEPTANCE_VOLUME_PROOF");
        Assert.SkipUnless(proofFile is not null, "Requires explicit isolated-volume reset admission");
        using var proof = JsonDocument.Parse(await File.ReadAllBytesAsync(proofFile!, Ct));
        var owner = proof.RootElement;
        Assert.Equal("ra20261020-rc01-b-owned-volume/v1", owner.GetProperty("schema").GetString());
        Assert.Equal("ra20261020-rc01-b", owner.GetProperty("namespace").GetString());
        var drive = owner.GetProperty("drive").GetString()!;
        Assert.Equal("S", drive);
        var nativeVolume = owner.GetProperty("volumeId").GetString()!.Substring(11, 36);
        await using var first = await Fixture.Create();
        await using var second = await Fixture.Create();
        foreach (var fixture in new[] { first, second })
            Assert.Equal(drive + ":\\", Path.GetPathRoot(fixture.Source));
        var expected = new Dictionary<string, string>(StringComparer.Ordinal);
        var expectedSizes = new Dictionary<string, long>(StringComparer.Ordinal);
        var expectedDirectories = new HashSet<string>(StringComparer.Ordinal);
        foreach (var fixture in new[] { first, second })
        {
            expected[fixture.File] = Hash(Encoding.UTF8.GetBytes("original"));
            expectedSizes[fixture.File] = Encoding.UTF8.GetByteCount("original");
            for (var directory = 0; directory < 512; directory++)
            {
                var generatedDirectory = Path.Combine(fixture.Source, $"directory-{directory:D4}");
                Directory.CreateDirectory(generatedDirectory);
                expectedDirectories.Add(generatedDirectory);
            }
            for (var index = 1; index < 5000; index++)
            {
                var file = Path.Combine(fixture.Source, $"directory-{index % 512:D4}", $"file-{index:D5}.bin");
                var bytes = Encoding.UTF8.GetBytes($"independent-generation-271828-{index:D5}");
                await File.WriteAllBytesAsync(file, bytes, Ct);
                expected[file] = Hash(bytes);
                expectedSizes[file] = bytes.Length;
            }
        }
        await first.Evidence("T10-input-manifest", new
        {
            generator = "independent-generation-271828", roots = new[] { first.Source, second.Source },
            files = expected.OrderBy(item => item.Key, StringComparer.Ordinal).Select(item => new
                { path = item.Key, kind = "file", size = expectedSizes[item.Key], sha256 = item.Value }),
            directories = expectedDirectories.Order(StringComparer.Ordinal)
        });
        var cursors = new Dictionary<string, long>();
        var anchors = new Dictionary<string, string>();
        var scopes = new Dictionary<string, string>();
        var remote = new WindowsFreshCycleTests.FakeRemote();
        var configuration = new WindowsCycleConfiguration("mapping", "client", "audit", "rules-v1",
            new Uri("https://test.invalid/"), remote.Device, "audit-token", "local-test", remote.Binding);
        var cycleBudget = new WindowsCycleBudget(MinimumFreeDiskBytes: 0);
        var checkpoints = new List<object>();
        for (var restart = 0; restart < 3; restart++)
        {
            foreach (var fixture in new[] { first, second })
            {
                await using var coordinator = await WindowsLocalCoherenceCoordinator.OpenWithModeAsync(
                    fixture.Source, LocalJournalReadMode.IdentityOnly, Ct);
                Assert.Equal(nativeVolume, coordinator.RootProof.VolumeGuid, ignoreCase: true);
                var bounds = await coordinator.QueryJournalAsync(Ct);
                await using var session = await fixture.Store.OpenLocalJournalSessionAsync(fixture.Binding(coordinator),
                    bounds, bounds.NextUsn, new(MinimumFreeDiskBytes: 0), LocalJournalReadMode.IdentityOnly, Ct);
                if (restart == 0) scopes[fixture.Source] = await session.BeginCycleAsync(configuration, cycleBudget, Ct);
                else
                {
                    var retained = await session.ResumeParentCycleAsync(configuration, cycleBudget, Path.Combine(fixture.Directory, "spool"), Ct);
                    Assert.NotNull(retained);
                    Assert.Equal(scopes[fixture.Source], retained.ScopeId);
                    Assert.Equal(anchors[fixture.Source], retained.CoherenceId);
                }
                var anchor = restart == 0
                    ? await coordinator.BuildAnchorAsync(session, cancellationToken: Ct)
                    : await coordinator.ObserveAndRetireChangesAsync(session, anchors[fixture.Source], Ct);
                Assert.Equal(LocalCoherenceStatus.Anchored, anchor.Status);
                if (restart == 0)
                {
                    anchors[fixture.Source] = anchor.CoherenceId;
                    await session.BindCycleCoherenceAsync(scopes[fixture.Source], anchor.CoherenceId, Ct);
                    while (await session.SeedCycleEntriesAsync(scopes[fixture.Source], Ct) > 0) Ct.ThrowIfCancellationRequested();
                }
                else Assert.Equal(anchors[fixture.Source], anchor.CoherenceId);
                var cursor = (await session.ReadStateAsync(Ct)).Cursor;
                if (cursors.TryGetValue(fixture.Source, out var prior)) Assert.True(cursor.NextReadUsn >= prior);
                cursors[fixture.Source] = cursor.NextReadUsn;
                checkpoints.Add(new { restart, fixture.Source, cursor, anchor.CoherenceId });
            }
        }
        await using var firstCoordinator = await WindowsLocalCoherenceCoordinator.OpenWithModeAsync(first.Source, LocalJournalReadMode.IdentityOnly, Ct);
        await using var secondCoordinator = await WindowsLocalCoherenceCoordinator.OpenWithModeAsync(second.Source, LocalJournalReadMode.IdentityOnly, Ct);
        var before = await firstCoordinator.QueryJournalAsync(Ct);
        await using var firstSession = await first.Store.OpenLocalJournalSessionAsync(first.Binding(firstCoordinator), before, before.NextUsn, new(MinimumFreeDiskBytes: 0), LocalJournalReadMode.IdentityOnly, Ct);
        await using var secondSession = await second.Store.OpenLocalJournalSessionAsync(second.Binding(secondCoordinator), before, before.NextUsn, new(MinimumFreeDiskBytes: 0), LocalJournalReadMode.IdentityOnly, Ct);
        Assert.NotNull(await firstSession.ResumeParentCycleAsync(configuration, cycleBudget, Path.Combine(first.Directory, "spool"), Ct));
        Assert.NotNull(await secondSession.ResumeParentCycleAsync(configuration, cycleBudget, Path.Combine(second.Directory, "spool"), Ct));
        var firstCursor = (await firstSession.ReadStateAsync(Ct)).Cursor;
        var secondCursor = (await secondSession.ReadStateAsync(Ct)).Cursor;
        var commands = new List<object>();
        await OwnedFsutil("deletejournal", "/D", "/N");
        await OwnedFsutil("createjournal", "m=8388608", "a=1048576");
        var after = await firstCoordinator.QueryJournalAsync(Ct);
        Assert.NotEqual(before.JournalIdHex, after.JournalIdHex);
        var firstGap = await Assert.ThrowsAsync<LocalSourceGapException>(() => firstCoordinator.ObserveAndRetireChangesAsync(firstSession, anchors[first.Source], Ct));
        var stillIndependent = await secondSession.ReadStateAsync(Ct);
        Assert.True(stillIndependent.Cursor.Valid);
        Assert.Equal(secondCursor.NextReadUsn, stillIndependent.Cursor.NextReadUsn);
        var secondGap = await Assert.ThrowsAsync<LocalSourceGapException>(() => secondCoordinator.ObserveAndRetireChangesAsync(secondSession, anchors[second.Source], Ct));
        var firstState = await firstSession.ReadStateAsync(Ct);
        var secondState = await secondSession.ReadStateAsync(Ct);
        Assert.False(firstState.Cursor.Valid);
        Assert.False(secondState.Cursor.Valid);
        Assert.Equal(firstCursor.NextReadUsn, firstState.Cursor.NextReadUsn);
        Assert.Equal(secondCursor.NextReadUsn, secondState.Cursor.NextReadUsn);
        var actual = Directory.EnumerateFiles(first.Source, "*", SearchOption.AllDirectories)
            .Concat(Directory.EnumerateFiles(second.Source, "*", SearchOption.AllDirectories)).Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(expected.Keys.Order(StringComparer.Ordinal), actual);
        Assert.Equal(expectedDirectories.Order(StringComparer.Ordinal), Directory.EnumerateDirectories(first.Source, "*", SearchOption.AllDirectories)
            .Concat(Directory.EnumerateDirectories(second.Source, "*", SearchOption.AllDirectories)).Order(StringComparer.Ordinal));
        var readback = new List<object>();
        foreach (var file in actual)
        {
            var bytes = await File.ReadAllBytesAsync(file, Ct);
            var hash = Hash(bytes);
            Assert.Equal(expected[file], hash);
            Assert.Equal(expectedSizes[file], bytes.LongLength);
            readback.Add(new { path = file, size = bytes.LongLength, sha256 = hash });
        }
        await first.Evidence("T10-source-readback", new { files = readback });
        await first.Evidence("T10-actual-reset", new { before, after, checkpoints, commands,
            files = actual.Length, directories = 1024, firstGap.Code,
            secondGap = secondGap.Code, firstState.Cursor, secondCursor = secondState.Cursor,
            fullSourceReadback = true, serverRecoveryQualified = false });

        async Task OwnedFsutil(string action, params string[] parameters)
        {
            // Recheck actual native identity immediately before each mutation.
            Assert.Equal(nativeVolume, firstCoordinator.RootProof.VolumeGuid, ignoreCase: true);
            Assert.Equal("S:\\", Path.GetPathRoot(first.Source));
            using var currentProof = JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine("S:\\", ".ra20261020-rc01-b-owner.json"), Ct));
            Assert.Equal(owner.GetProperty("volumeId").GetString(), currentProof.RootElement.GetProperty("volumeId").GetString());
            var start = new ProcessStartInfo(Path.Combine(Environment.SystemDirectory, "fsutil.exe"))
                { UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true };
            start.ArgumentList.Add("usn"); start.ArgumentList.Add(action);
            foreach (var parameter in parameters) start.ArgumentList.Add(parameter);
            start.ArgumentList.Add(drive + ":");
            using var process = Process.Start(start) ?? throw new IOException("Owned journal tool did not start");
            var output = process.StandardOutput.ReadToEndAsync(Ct);
            var error = process.StandardError.ReadToEndAsync(Ct);
            await process.WaitForExitAsync(Ct).WaitAsync(TimeSpan.FromSeconds(30), Ct);
            commands.Add(new { action, drive, process.ExitCode, output = await output, error = await error });
            await first.Evidence("T10-journal-command-" + action, new { commands });
            Assert.True(process.ExitCode == 0, $"Owned {action} failed ({process.ExitCode}): {await output} {await error}");
        }
        static string Hash(byte[] bytes) => Convert.ToHexStringLower(SHA256.HashData(bytes));
    }
}
