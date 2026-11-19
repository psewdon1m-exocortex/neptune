using System.Collections.Concurrent;
using System.ComponentModel;
using System.IO.MemoryMappedFiles;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class NativeBootstrapCaptureTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ActualNtfsSealBindsExactBytesAndWorkWithoutClaimingCut()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync();
        await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct);
        var seal = await native.CaptureWorkAsync(f.Work(), f.SealPath, 3, Ct);
        var work = f.Work() with { SealedSize = seal.Source.Size, Sha256 = seal.Source.Sha256,
            SourceIdentity = seal.Source.SourceIdentity, NativeCaptureProofJson = seal.ProofJson };
        var proof = NativeBootstrapCaptureProof.ValidatePersisted(work)!;
        Assert.Equal(Encoding.UTF8.GetBytes("abc"), await File.ReadAllBytesAsync(f.SealPath, Ct));
        Assert.Equal(Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes("abc"))), proof.Sha256);
        Assert.Equal(native.RootProof.Handle.FileId128, proof.RootFileId128);
        Assert.Equal(native.RootProof.VolumeGuid, proof.VolumeGuid);
        Assert.Equal("Unknown", proof.SourceCycle); Assert.Equal("Unknown", proof.LocalCompleteness);
        Assert.True(proof.HistoricalOnly); Assert.Equal(30, JsonNode.Parse(seal.ProofJson)!.AsObject().Count);
        await f.EvidenceAsync("native-seal", new { proof.Schema, proof.FileId128, proof.RootFileId128,
            proof.SealedSize, proof.Sha256, proof.LastFileUsn, proof.HistoricalOnly, proof.SourceCycle });
    }

    [Theory]
    [InlineData(false)] [InlineData(true)]
    public async Task OpenWriterAndSameReasonWritesCannotProduceASeal(bool repeatedWrites)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync();
        await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct);
        await using (var writer = new FileStream(f.FilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete))
        {
            if (repeatedWrites)
            { writer.Position = 0; writer.WriteByte((byte)'x'); writer.Flush(true); writer.Position = 0; writer.WriteByte((byte)'y'); writer.Flush(true); }
            await Assert.ThrowsAsync<BootstrapCaptureDeferredException>(() => native.CaptureWorkAsync(f.Work(), f.SealPath, 3, Ct));
            Assert.False(File.Exists(f.SealPath));
        }
        var sealedAfterClose = await native.CaptureWorkAsync(f.Work(), f.SealPath, 3, Ct);
        Assert.Equal(repeatedWrites ? "ybc" : "abc", await File.ReadAllTextAsync(f.SealPath, Ct));
        Assert.NotNull(sealedAfterClose.ProofJson);
    }

    [Fact]
    public async Task WritableMappedViewStillDefersAfterOriginalFileHandleCloses()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync();
        await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct);
        var input = new FileStream(f.FilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
        using (var mapping = MemoryMappedFile.CreateFromFile(input, null, 3, MemoryMappedFileAccess.ReadWrite,
            HandleInheritability.None, leaveOpen: true))
        using (var view = mapping.CreateViewAccessor(0, 3, MemoryMappedFileAccess.ReadWrite))
        {
            input.Dispose(); view.Write(0, (byte)'m'); view.Flush();
            await Assert.ThrowsAsync<BootstrapCaptureDeferredException>(() => native.CaptureWorkAsync(f.Work(), f.SealPath, 3, Ct));
            view.Write(0, (byte)'n'); view.Flush(); Assert.False(File.Exists(f.SealPath));
        }
        await native.CaptureWorkAsync(f.Work(), f.SealPath, 3, Ct);
        Assert.Equal("nbc", await File.ReadAllTextAsync(f.SealPath, Ct));
        await f.EvidenceAsync("mmap-exclusion", new { originalHandleClosed = true, mappedViewBlockedSeal = true, finalBytes = "nbc" });
    }

    [Fact]
    public async Task LiveGuardFencesWriterFileDeleteAndAncestorRenameBeforeAndAfterCopy()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync();
        Directory.CreateDirectory(Path.Combine(f.Source, "parent"));
        var file = Path.Combine(f.Source, "parent", "file.txt"); File.Move(f.FilePath, file);
        var stages = new List<NativeCaptureStage>();
        await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct, (stage, _) =>
        {
            stages.Add(stage);
            Assert.Throws<IOException>(() => { using var writer = new FileStream(file, FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete); });
            Assert.Throws<IOException>(() => File.Delete(file));
            Assert.Throws<IOException>(() => Directory.Move(Path.Combine(f.Source, "parent"), Path.Combine(f.Source, "moved")));
            Assert.Throws<IOException>(() => Directory.Move(f.Source, Path.Combine(f.Root, "moved-root")));
            return Task.CompletedTask;
        });
        await native.CaptureWorkAsync(f.Work("parent/file.txt"), f.SealPath, 3, Ct);
        Assert.Equal(new[] { NativeCaptureStage.GuardOpened, NativeCaptureStage.SpoolFlushed }, stages);
        Assert.Equal("abc", await File.ReadAllTextAsync(f.SealPath, Ct));
    }

    [Theory]
    [InlineData("root-swap")] [InlineData("reparse")] [InlineData("wrong-case")]
    [InlineData("directory")] [InlineData("size-changed")] [InlineData("hardlink")]
    public async Task NativeIdentityPolicyRefusesUnsupportedOrReboundSource(string scenario)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync();
        await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct);
        var work = f.Work();
        switch (scenario)
        {
            case "root-swap":
                // The retained native root handle already denies delete/rename;
                // a replacement cannot be installed while this adapter owns it.
                Assert.Throws<IOException>(() => Directory.Move(f.Source, Path.Combine(f.Root, "original")));
                var sealedOriginal = await native.CaptureWorkAsync(work, f.SealPath, 3, Ct);
                Assert.Equal(native.RootProof.Handle.FileId128, NativeBootstrapCaptureProof.ValidatePersisted(work with {
                    SealedSize = 3, Sha256 = sealedOriginal.Source.Sha256, SourceIdentity = sealedOriginal.Source.SourceIdentity,
                    NativeCaptureProofJson = sealedOriginal.ProofJson })!.RootFileId128);
                return;
            case "reparse":
                Directory.CreateDirectory(Path.Combine(f.Root, "outside")); await File.WriteAllTextAsync(Path.Combine(f.Root, "outside", "file.txt"), "abc", Ct);
                Directory.CreateSymbolicLink(Path.Combine(f.Source, "link"), Path.Combine(f.Root, "outside")); work = f.Work("link/file.txt"); break;
            case "wrong-case": work = f.Work("FILE.txt"); break;
            case "directory": File.Delete(f.FilePath); Directory.CreateDirectory(f.FilePath); break;
            case "size-changed": await File.AppendAllTextAsync(f.FilePath, "x", Ct); break;
            case "hardlink": Assert.True(CreateHardLinkW(Path.Combine(f.Source, "alias.txt"), f.FilePath, nint.Zero), new Win32Exception(Marshal.GetLastWin32Error()).Message); break;
        }
        Assert.NotNull(await Record.ExceptionAsync(() => native.CaptureWorkAsync(work, f.SealPath, 3, Ct)));
        Assert.False(File.Exists(f.SealPath));
    }

    [Theory]
    [InlineData(false)] [InlineData(true)]
    public async Task CancelledCaptureDeletesOnlyNewOwnedSpoolAndNeverAnExistingFile(bool afterFlush)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); using var cancel = new CancellationTokenSource();
        await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct, (stage, _) =>
        { if ((stage == NativeCaptureStage.SpoolFlushed) == afterFlush) cancel.Cancel(); return Task.CompletedTask; });
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => native.CaptureWorkAsync(f.Work(), f.SealPath, 3, cancel.Token));
        Assert.False(File.Exists(f.SealPath));
        await File.WriteAllTextAsync(f.SealPath, "foreign existing file", Ct);
        await using var next = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct);
        await Assert.ThrowsAnyAsync<IOException>(() => next.CaptureWorkAsync(f.Work(), f.SealPath, 3, Ct));
        Assert.Equal("foreign existing file", await File.ReadAllTextAsync(f.SealPath, Ct));
    }

    [Theory]
    [InlineData("schema")] [InlineData("sessionId")] [InlineData("scanId")] [InlineData("scanGeneration")]
    [InlineData("entryId")] [InlineData("operationId")] [InlineData("mappingId")] [InlineData("clientInstanceId")]
    [InlineData("sourceRoot")] [InlineData("relativePath")] [InlineData("targetRoot")] [InlineData("credentialScope")]
    [InlineData("expectedServerRootId")] [InlineData("fileId128")] [InlineData("sealedSize")] [InlineData("sha256")]
    [InlineData("lastFileUsn")] [InlineData("links")] [InlineData("fileAttributes")] [InlineData("volumeGuid")]
    [InlineData("duplicate")] [InlineData("missing")] [InlineData("null")] [InlineData("depth")] [InlineData("oversize")]
    public async Task StoredNativeEvidenceRejectsMalformedOrReboundFields(string field)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct);
        var seal = await native.CaptureWorkAsync(f.Work(), f.SealPath, 3, Ct);
        var obj = JsonNode.Parse(seal.ProofJson)!.AsObject();
        var raw = seal.ProofJson;
        switch (field)
        {
            case "duplicate": raw = raw[..^1] + ",\"schema\":\"" + NativeBootstrapCaptureProof.Schema + "\"}"; break;
            case "missing": obj.Remove("fileId128"); raw = obj.ToJsonString(); break;
            case "null": obj["sha256"] = null; raw = obj.ToJsonString(); break;
            case "depth": raw = "{\"schema\":[[[[[[0]]]]]]}"; break;
            case "oversize": raw = new string(' ', NativeBootstrapCaptureProof.MaximumJsonBytes + 1); break;
            default:
                obj[field] = field switch {
                    "scanGeneration" or "entryId" => JsonValue.Create(0), "links" => JsonValue.Create(2),
                    "fileAttributes" => JsonValue.Create(0x400), "expectedServerRootId" => JsonValue.Create(Guid.NewGuid().ToString()),
                    "sealedSize" => JsonValue.Create("03"), "lastFileUsn" => JsonValue.Create("-1"),
                    "fileId128" => JsonValue.Create(new string('0', 32)), _ => JsonValue.Create("wrong") };
                raw = obj.ToJsonString(); break;
        }
        // Recompute the raw digest to prove independent semantic validation,
        // rather than rejection solely because the JSON bytes changed.
        var work = f.Work() with { SealedSize = 3, Sha256 = seal.Source.Sha256,
            SourceIdentity = ProofIdentity(raw), NativeCaptureProofJson = raw };
        Assert.Throws<InvalidDataException>(() => NativeBootstrapCaptureProof.ValidatePersisted(work));
    }

    [Fact]
    public async Task MissingRecognizedProofAndWorkRebindFailClosed()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct);
        var seal = await native.CaptureWorkAsync(f.Work(), f.SealPath, 3, Ct);
        var original = f.Work() with { SealedSize = 3, Sha256 = seal.Source.Sha256, SourceIdentity = seal.Source.SourceIdentity, NativeCaptureProofJson = seal.ProofJson };
        Assert.Throws<InvalidDataException>(() => NativeBootstrapCaptureProof.ValidatePersisted(original with { NativeCaptureProofJson = null }));
        Assert.Throws<InvalidDataException>(() => NativeBootstrapCaptureProof.ValidatePersisted(original with { EntryId = 2 }));
        Assert.Throws<InvalidDataException>(() => NativeBootstrapCaptureProof.ValidatePersisted(original with { ScanGeneration = 2 }));
        Assert.Null(NativeBootstrapCaptureProof.ValidatePersisted(f.Work() with { SourceIdentity = "old-legacy" }));
        await Assert.ThrowsAsync<InvalidOperationException>(() => native.CaptureAsync(f.Source, "file.txt", f.SealPath, 3, Ct));
    }

    [Fact]
    public async Task ExistingCorePipelineUsesNativeSealAndPublishesBeforeLateScanCompletes()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); Directory.CreateDirectory(Path.Combine(f.Source, "late")); Directory.CreateDirectory(Path.Combine(f.Source, "empty"));
        var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new LateSource(f.Source, barrier.Task);
        await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct);
        var transport = new Transport(); var observedOverlap = false;
        var result = await new NeptuneBootstrapExecutor(f.Store, transport, source, native).ScanAndTransferAsync(f.Request, async (r, _) =>
        {
            if (!r.IsDirectory)
            {
                var committed = (await f.Store.ListBootstrapWorkAsync(transport.Works.Single(w => !w.IsDirectory).SessionId, cancellationToken: Ct)).Single(w => !w.IsDirectory);
                Assert.NotNull(NativeBootstrapCaptureProof.ValidatePersisted(committed));
                Assert.Equal(BootstrapWorkPhase.Published, committed.Phase);
                observedOverlap = !source.Completed; barrier.TrySetResult();
            }
        }, Ct);
        Assert.True(observedOverlap); Assert.True(source.Completed); Assert.Equal(1, result.PublishedFiles); Assert.Equal(2, result.PublishedDirectories);
        Assert.Equal(0, result.Failures); Assert.Null(result.ScanError); Assert.False(result.DestructiveCleanupAllowed);
        Assert.Equal(3L, await f.ScalarAsync("SELECT version FROM bootstrap_work_format"));
        Assert.Equal(1, result.MaximumSourceReaders); Assert.InRange(result.MaximumDataJobs, 1, 4); Assert.InRange(result.MaximumLargeJobs, 0, 2);
        Assert.Equal(Encoding.UTF8.GetBytes("abc"), transport.Published.Single().Value);
        Assert.All(transport.Works.Where(w => w.IsDirectory), w => Assert.Null(w.NativeCaptureProofJson));
        var replay = await new NeptuneBootstrapExecutor(f.Store, transport, captureSource: native).ResumeSessionAsync(result.SessionId, f.Request, cancellationToken: Ct);
        Assert.Equal(1, replay.PublishedFiles); Assert.Equal(3, transport.Calls);
        await f.EvidenceAsync("composed-native-overlap", new { observedOverlap, result, replayEffects = 0 });
    }

    [Fact]
    public async Task RestartReplaysSameSealedBytesAndProofAfterSourceChanges()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct);
        var transport = new Transport { FailOnce = true };
        var first = await new NeptuneBootstrapExecutor(f.Store, transport, captureSource: native).ScanAndTransferAsync(f.Request, cancellationToken: Ct);
        Assert.Equal(1, first.Failures); var before = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        Assert.NotNull(NativeBootstrapCaptureProof.ValidatePersisted(before)); Assert.Equal(3, before.ReceivedBytes);
        await File.WriteAllTextAsync(f.FilePath, "new unrelated local generation", Ct);
        var state = new NeptuneStateStore(f.Database); await state.InitializeAsync(Ct);
        var second = await new NeptuneBootstrapExecutor(state, transport, captureSource: native).ResumeSessionAsync(first.SessionId, f.Request, cancellationToken: Ct);
        Assert.Equal(1, second.PublishedFiles); Assert.Equal(0, second.Failures);
        var after = Assert.Single(await state.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        Assert.Equal(before.NativeCaptureProofJson, after.NativeCaptureProofJson); Assert.Equal(before.OperationId, after.OperationId);
        Assert.Equal(before.UploadId, after.UploadId); Assert.Equal(before.Sha256, after.Sha256);
        Assert.Equal(Encoding.UTF8.GetBytes("abc"), transport.Published.Single().Value);
    }

    [Fact]
    public async Task MalformedPersistedNativeProofStopsBeforeAnyReplayEffect()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct);
        var transport = new Transport { FailOnce = true };
        var first = await new NeptuneBootstrapExecutor(f.Store, transport, captureSource: native).ScanAndTransferAsync(f.Request, cancellationToken: Ct);
        var old = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        await f.ExecuteAsync("UPDATE bootstrap_work_items SET native_capture='{}'");
        var next = await new NeptuneBootstrapExecutor(f.Store, transport, captureSource: native).ResumeSessionAsync(first.SessionId, f.Request, cancellationToken: Ct);
        Assert.Equal(1, next.Failures); Assert.Equal(1, transport.Calls);
        var retained = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        Assert.Equal(old.OperationId, retained.OperationId); Assert.Equal(old.UploadId, retained.UploadId); Assert.Equal(old.ReceivedBytes, retained.ReceivedBytes);
        Assert.True(File.Exists(retained.SpoolPath)); Assert.Equal(old.Attempt, retained.Attempt);
    }

    [Theory]
    [InlineData(false)] [InlineData(true)]
    public async Task OversizedOrBlobNativeColumnCannotMaterializeUnboundedJsonBeforeValidation(bool blob)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct);
        var transport = new Transport { FailOnce = true };
        var first = await new NeptuneBootstrapExecutor(f.Store, transport, captureSource: native).ScanAndTransferAsync(f.Request, cancellationToken: Ct);
        var old = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        await f.ExecuteAsync("UPDATE bootstrap_work_items SET native_capture=" + (blob ? "zeroblob(8388608)" : "CAST(zeroblob(8388608) AS TEXT)"));
        var loaded = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        Assert.Equal("<invalid-native-capture>", loaded.NativeCaptureProofJson);
        Assert.Throws<InvalidDataException>(() => NativeBootstrapCaptureProof.ValidatePersisted(loaded));
        var result = await new NeptuneBootstrapExecutor(f.Store, transport, captureSource: native).ResumeSessionAsync(first.SessionId, f.Request, cancellationToken: Ct);
        Assert.Equal(1, result.Failures); Assert.Equal(1, transport.Calls);
        var retained = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        Assert.Equal(old.Attempt, retained.Attempt); Assert.Equal(old.UploadId, retained.UploadId); Assert.True(File.Exists(retained.SpoolPath));
    }

    [Fact]
    public async Task LegacySealedWorkRemainsUnqualifiedWhenNativeModeIsRequested()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); var transport = new Transport { FailOnce = true };
        var first = await new NeptuneBootstrapExecutor(f.Store, transport).ScanAndTransferAsync(f.Request, cancellationToken: Ct);
        var before = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct)); Assert.Null(before.NativeCaptureProofJson);
        await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct);
        var second = await new NeptuneBootstrapExecutor(f.Store, transport, captureSource: native).ResumeSessionAsync(first.SessionId, f.Request, cancellationToken: Ct);
        Assert.Equal(1, second.Failures); Assert.Equal(1, transport.Calls);
        var old = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        Assert.Null(old.NativeCaptureProofJson); Assert.Equal(before.OperationId, old.OperationId); Assert.Equal(before.UploadId, old.UploadId); Assert.Equal(before.Attempt, old.Attempt);
        Assert.True(File.Exists(old.SpoolPath)); Assert.Equal(before.Phase, old.Phase);
        var legacyRecovery = await new NeptuneBootstrapExecutor(f.Store, transport).ResumeSessionAsync(first.SessionId, f.Request, cancellationToken: Ct);
        Assert.Equal(1, legacyRecovery.PublishedFiles); Assert.Equal(2, transport.Calls);
    }

    [Fact]
    public async Task NativeCaptureUsesBoundedExistingPipelineAcrossWideAndDeepBranches()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync();
        for (var i = 0; i < 32; i++) await File.WriteAllTextAsync(Path.Combine(f.Source, "wide-" + i), "x", Ct);
        var deep = f.Source;
        for (var i = 0; i < 16; i++) { deep = Path.Combine(deep, "d"); Directory.CreateDirectory(deep); }
        await File.WriteAllTextAsync(Path.Combine(deep, "deep.txt"), "deep", Ct);
        await File.WriteAllBytesAsync(Path.Combine(f.Source, "large-a"), new byte[1024 * 1024 + 1], Ct);
        await File.WriteAllBytesAsync(Path.Combine(f.Source, "large-b"), new byte[1024 * 1024 + 1], Ct);
        await File.WriteAllBytesAsync(Path.Combine(f.Source, "over-budget"), new byte[3 * 1024 * 1024], Ct);
        await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct);
        var transport = new Transport();
        var request = f.Request with { Limits = new(LargeFileThreshold: 1024 * 1024, ReadyQueueCapacity: 4, SpoolBudgetBytes: 2 * 1024 * 1024, DiskFreeFloorBytes: 0) };
        var result = await new NeptuneBootstrapExecutor(f.Store, transport, captureSource: native).ScanAndTransferAsync(request, cancellationToken: Ct);
        Assert.Equal(36, result.PublishedFiles); Assert.Equal(16, result.PublishedDirectories); Assert.Equal(1, result.Deferred);
        Assert.Equal(0, result.Failures); Assert.Null(result.ScanError); Assert.Equal(1, result.MaximumSourceReaders);
        Assert.InRange(result.MaximumDataJobs, 1, 4); Assert.InRange(result.MaximumLargeJobs, 1, 2);
        Assert.InRange(result.MaximumReadyQueue, 0, 4); Assert.InRange(result.MaximumSpoolBytes, 0, request.Limits!.SpoolBudgetBytes);
        long cursor = 0; var proofs = 0;
        while (true)
        {
            var page = await f.Store.ListBootstrapWorkAsync(result.SessionId, cursor, 16, Ct); if (page.Count == 0) break;
            foreach (var work in page) if (work.Phase == BootstrapWorkPhase.Published && !work.IsDirectory)
            { Assert.NotNull(NativeBootstrapCaptureProof.ValidatePersisted(work)); proofs++; }
            cursor = page[^1].WorkId;
        }
        Assert.Equal(36, proofs); await f.EvidenceAsync("native-pipeline-budgets", new { result, qualifiedHistoricalByteProofs = proofs, managedSourceBuffersBytes = 3 * 128 * 1024 });
    }

    [Theory]
    [InlineData("cancel")] [InlineData("paused")] [InlineData("rebound")]
    public async Task AcknowledgedNativeOutcomeSurvivesCancellationPauseAndRemap(string action)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct);
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(Ct); var transport = new Transport();
        var first = await new NeptuneBootstrapExecutor(f.Store, transport, captureSource: native).ScanAndTransferAsync(f.Request, async (receipt, _) =>
        {
            if (receipt.IsDirectory) return;
            if (action == "cancel") cancel.Cancel();
            else if (action == "paused") await f.ExecuteAsync("UPDATE sync_mappings SET enabled=0");
            else await f.ExecuteAsync("UPDATE sync_mappings SET local_path=local_path||'-other'");
        }, cancel.Token);
        Assert.Equal(1, first.PublishedFiles); Assert.Equal(0, first.Failures);
        var old = Assert.Single(await f.Store.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        Assert.Equal(BootstrapWorkPhase.Published, old.Phase); Assert.NotNull(old.Receipt); Assert.NotNull(NativeBootstrapCaptureProof.ValidatePersisted(old));
        var reopened = new NeptuneStateStore(f.Database); await reopened.InitializeAsync(Ct);
        var retained = Assert.Single(await reopened.ListBootstrapWorkAsync(first.SessionId, cancellationToken: Ct));
        Assert.Equal(old, retained); Assert.Equal(1, transport.Calls);
    }

    [Fact]
    public async Task IncompleteNativeScanKeepsPublishedReceiptAndNeverAuthorizesCleanup()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }
        await using var f = await Fixture.CreateAsync(); Directory.CreateDirectory(Path.Combine(f.Source, "late"));
        var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = new LateSource(f.Source, barrier.Task, failAfterBarrier: true);
        await using var native = await WindowsQualifiedBootstrapCapture.OpenAsync(f.Source, Ct); var transport = new Transport();
        var result = await new NeptuneBootstrapExecutor(f.Store, transport, source, native).ScanAndTransferAsync(f.Request, (receipt, _) =>
        { if (!receipt.IsDirectory) barrier.TrySetResult(); return Task.CompletedTask; }, Ct);
        Assert.Equal(1, result.PublishedFiles); Assert.NotNull(result.ScanError); Assert.Equal(BootstrapScanStatus.Incomplete, result.Scan!.Status);
        Assert.False(result.DestructiveCleanupAllowed); Assert.All(transport.Works, w => Assert.InRange(w.Attempt, 1, 1));
        var work = (await f.Store.ListBootstrapWorkAsync(result.SessionId, cancellationToken: Ct)).Single(w => !w.IsDirectory);
        Assert.Equal(BootstrapWorkPhase.Published, work.Phase); Assert.NotNull(NativeBootstrapCaptureProof.ValidatePersisted(work));
    }

    [Fact]
    public async Task NativeRollbackRefusesTamperedManifestAndBackupWithActiveProfileUnchanged()
    {
        await using var f = await Fixture.CreateAsync(); await InvokeAsync(f.Store, "EnsureNativeBootstrapWorkSchemaAsync", Ct);
        var manifest = f.Database + ".bootstrap-work-v3-backup.json"; var json = await File.ReadAllTextAsync(manifest, Ct);
        // Establish the physical baseline before pinning a real read snapshot.
        // An open connection alone has not necessarily attached to this WAL;
        // unrelated ClearAllPools may otherwise checkpoint our prior frames.
        await using var wal = new SqliteConnection("Data Source=" + f.Database + ";Pooling=False"); await wal.OpenAsync(Ct);
        await using (var setup = wal.CreateCommand())
        {
            setup.CommandText = "PRAGMA wal_autocheckpoint=0; CREATE TABLE native_rollback_probe(value TEXT)";
            await setup.ExecuteNonQueryAsync(Ct);
        }
        await using (var checkpoint = wal.CreateCommand())
        {
            checkpoint.CommandText = "PRAGMA wal_checkpoint(TRUNCATE)";
            await using var result = await checkpoint.ExecuteReaderAsync(Ct);
            Assert.True(await result.ReadAsync(Ct)); Assert.Equal(0L, result.GetInt64(0));
            Assert.Equal(0L, result.GetInt64(1)); Assert.Equal(0L, result.GetInt64(2));
        }
        await using var snapshot = wal.BeginTransaction(deferred: true);
        await using var pin = wal.CreateCommand(); pin.Transaction = snapshot;
        pin.CommandText = "SELECT COUNT(*) FROM native_rollback_probe";
        Assert.Equal(0L, await pin.ExecuteScalarAsync(Ct));
        // Keep committed user bytes in the WAL while the pre-write reader
        // prevents checkpoint movement of the physical byte-exact baseline.
        await f.ExecuteAsync("INSERT INTO native_rollback_probe VALUES('committed original retained during refusal')");
        Assert.Equal(0L, await pin.ExecuteScalarAsync(Ct));
        var active = await ReadSharedAsync(f.Database);
        var log = File.Exists(f.Database + "-wal") ? await ReadSharedAsync(f.Database + "-wal") : [];
        Assert.True(log.Length > 32, "The refusal must preserve a real committed WAL payload.");
        await File.WriteAllTextAsync(manifest, "[]", Ct);
        await Assert.ThrowsAsync<InvalidDataException>(() => f.Store.CreatePreNativeBootstrapCaptureRollbackCopyAsync(Path.Combine(f.Root, "bad-array.db"), Ct));
        await File.WriteAllTextAsync(manifest, json[..^1] + ",\"TargetFormat\":3}", Ct);
        await Assert.ThrowsAsync<InvalidDataException>(() => f.Store.CreatePreNativeBootstrapCaptureRollbackCopyAsync(Path.Combine(f.Root, "duplicate.db"), Ct));
        await File.WriteAllTextAsync(manifest, json, Ct);
        var backupPath = JsonNode.Parse(json)!["BackupPath"]!.GetValue<string>();
        await File.AppendAllTextAsync(backupPath, "tamper", Ct);
        await Assert.ThrowsAsync<InvalidDataException>(() => f.Store.CreatePreNativeBootstrapCaptureRollbackCopyAsync(Path.Combine(f.Root, "tampered.db"), Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => f.Store.CreatePreNativeBootstrapCaptureRollbackCopyAsync(f.Database, Ct));
        Assert.Equal(active, await ReadSharedAsync(f.Database));
        Assert.Equal(log, File.Exists(f.Database + "-wal") ? await ReadSharedAsync(f.Database + "-wal") : []);
        Assert.Equal(1L, await f.ScalarAsync("SELECT COUNT(*) FROM native_rollback_probe"));
        Assert.False(File.Exists(Path.Combine(f.Root, "bad-array.db")));
        Assert.False(File.Exists(Path.Combine(f.Root, "duplicate.db")));
        Assert.False(File.Exists(Path.Combine(f.Root, "tampered.db")));
    }

    [Fact]
    public async Task AdditiveMigrationIncludesWalPausedMappingsAndOldReceiptsWithoutRetroactiveProof()
    {
        await using var f = await Fixture.CreateAsync(); await InvokeAsync(f.Store, "EnsureBootstrapWorkSchemaAsync", Ct);
        await using var wal = new SqliteConnection("Data Source=" + f.Database + ";Pooling=False"); await wal.OpenAsync(Ct);
        await using var q = wal.CreateCommand(); q.CommandText = """
            PRAGMA wal_autocheckpoint=0; CREATE TABLE native_user_data(value TEXT); INSERT INTO native_user_data VALUES('committed WAL user bytes');
            INSERT INTO bootstrap_work_sessions(session_id,scan_id,generation,mapping_id,client_instance_id,source_root,target_root,credential_scope) VALUES('old','old-scan',1,'mapping','client','source','https://saturn.test/dav/sync/Audit/Mapping/','scope');
            INSERT INTO bootstrap_work_items(work_id,session_id,entry_id,operation_id,relative_path,kind,enumerated_size,modified_at,phase,sealed_size,sha256,source_identity,attempt,upload_id,received_bytes,receipt) VALUES(1,'old',1,'old-key','file',0,3,'2026-10-12T00:00:00.000Z',4,3,'old-hash','old-source',1,'671c88f7-4ef9-4c5e-a2de-8b294135fc4e',3,'original publication');
            UPDATE sync_mappings SET enabled=0;
            """; await q.ExecuteNonQueryAsync(Ct);
        await InvokeAsync(f.Store, "EnsureNativeBootstrapWorkSchemaAsync", Ct);
        Assert.Equal(3L, await f.ScalarAsync("SELECT version FROM bootstrap_work_format"));
        var old = Assert.Single(await f.Store.ListBootstrapWorkAsync("old", cancellationToken: Ct));
        Assert.Null(old.NativeCaptureProofJson); Assert.Null(NativeBootstrapCaptureProof.ValidatePersisted(old));
        Assert.Equal("original publication", old.Receipt); Assert.Equal(3, old.ReceivedBytes); Assert.Equal(0L, await f.ScalarAsync("SELECT enabled FROM sync_mappings"));
        var copy = await f.Store.CreatePreNativeBootstrapCaptureRollbackCopyAsync(Path.Combine(f.Root, "pre-native.db"), Ct);
        await using var backup = new SqliteConnection("Data Source=" + copy + ";Mode=ReadOnly;Pooling=False"); await backup.OpenAsync(Ct);
        await using var verify = backup.CreateCommand(); verify.CommandText = "SELECT value FROM native_user_data";
        Assert.Equal("committed WAL user bytes", await verify.ExecuteScalarAsync(Ct));
        verify.CommandText = "SELECT version FROM bootstrap_work_format"; Assert.Equal(2L, await verify.ExecuteScalarAsync(Ct));
        verify.CommandText = "SELECT receipt FROM bootstrap_work_items"; Assert.Equal("original publication", await verify.ExecuteScalarAsync(Ct));
        await InvokeAsync(f.Store, "EnsureNativeBootstrapWorkSchemaAsync", Ct);
        Assert.Single(Directory.GetFiles(f.Root, "*.pre-bootstrap-work-v3-*.db"));
    }

    [Fact]
    public async Task InterruptedCancelledAndConcurrentNativeMigrationsAreIdempotent()
    {
        await using var f = await Fixture.CreateAsync(); await InvokeAsync(f.Store, "EnsureBootstrapWorkSchemaAsync", Ct);
        await using (var c = new SqliteConnection("Data Source=" + f.Database + ";Pooling=False"))
        {
            await c.OpenAsync(Ct); using var tx = c.BeginTransaction(); await using var q = c.CreateCommand(); q.Transaction = tx;
            q.CommandText = "ALTER TABLE bootstrap_work_items ADD COLUMN native_capture TEXT"; await q.ExecuteNonQueryAsync(Ct); tx.Rollback();
        }
        using var cancelled = new CancellationTokenSource(); cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => InvokeAsync(f.Store, "EnsureNativeBootstrapWorkSchemaAsync", cancelled.Token));
        Assert.Equal(2L, await f.ScalarAsync("SELECT version FROM bootstrap_work_format"));
        await Task.WhenAll(Enumerable.Range(0, 3).Select(_ => InvokeAsync(new NeptuneStateStore(f.Database), "EnsureNativeBootstrapWorkSchemaAsync", Ct)));
        Assert.Equal(3L, await f.ScalarAsync("SELECT version FROM bootstrap_work_format")); Assert.Single(Directory.GetFiles(f.Root, "*.pre-bootstrap-work-v3-*.db"));
    }

    [Theory]
    [InlineData(true)] [InlineData(false)]
    public async Task FutureAndPartialNativeSchemaRejectBeforeAnyWritableInitialization(bool future)
    {
        await using var f = await Fixture.CreateAsync(); await InvokeAsync(f.Store, "EnsureNativeBootstrapWorkSchemaAsync", Ct);
        await using var wal = new SqliteConnection("Data Source=" + f.Database + ";Pooling=False"); await wal.OpenAsync(Ct);
        await using var q = wal.CreateCommand(); q.CommandText = "PRAGMA wal_autocheckpoint=0; " + (future ? "UPDATE bootstrap_work_format SET version=99" : "ALTER TABLE bootstrap_work_items DROP COLUMN native_capture"); await q.ExecuteNonQueryAsync(Ct);
        var db = await ReadSharedAsync(f.Database); var log = await ReadSharedAsync(f.Database + "-wal");
        await Assert.ThrowsAsync<InvalidDataException>(() => new NeptuneStateStore(f.Database).InitializeAsync(Ct));
        Assert.Equal(db, await ReadSharedAsync(f.Database)); Assert.Equal(log, await ReadSharedAsync(f.Database + "-wal"));
    }

    private static string ProofIdentity(string raw) => "native-bootstrap-v1:" + Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(raw)));
    private static async Task InvokeAsync(NeptuneStateStore store, string name, params object?[] args)
    {
        try { await (Task)typeof(NeptuneStateStore).GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(store, args)!; }
        catch (TargetInvocationException error) when (error.InnerException is not null) { System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(error.InnerException).Throw(); }
    }
    private static async Task<byte[]> ReadSharedAsync(string path)
    { await using var s = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete); using var m = new MemoryStream(); await s.CopyToAsync(m, Ct); return m.ToArray(); }
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)] [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CreateHardLinkW(string newName, string existingName, nint security);

    private sealed class Fixture : IAsyncDisposable
    {
        public string Root { get; } = Path.Combine(Environment.GetEnvironmentVariable("NEPTUNE_NATIVE_CAPTURE_TEST_ROOT") ?? Path.Combine(Path.GetTempPath(), "neptune-native-capture-tests"), Guid.NewGuid().ToString("N"));
        public string Source => Path.Combine(Root, "source"); public string FilePath => Path.Combine(Source, "file.txt");
        public string Database => Path.Combine(Root, "profile.db"); public string SealPath => Path.Combine(Root, "sealed.spool");
        public NeptuneStateStore Store = null!;
        public BootstrapExecutionRequest Request => new("mapping", "client", Source, Path.Combine(Root, "spool"), new(DiskFreeFloorBytes: 0));
        public static async Task<Fixture> CreateAsync()
        {
            var f = new Fixture(); Directory.CreateDirectory(f.Source); f.Store = new(f.Database); await f.Store.InitializeAsync(Ct);
            await f.Store.AddMappingAsync(new("mapping", "client", f.Source, true, DateTimeOffset.UtcNow), Ct);
            await File.WriteAllTextAsync(f.FilePath, "abc", Ct); return f;
        }
        public BootstrapWorkRecord Work(string path = "file.txt") => new(1, "session", "scan", 1, 1, new string('a', 64), "mapping", Source,
            "https://saturn.test/dav/sync/Audit/Mapping/", "scope", path, false, 3, DateTimeOffset.UtcNow, BootstrapWorkPhase.Queued, ClientInstanceId: "client");
        public async Task<object?> ScalarAsync(string sql) { await using var c = new SqliteConnection("Data Source=" + Database + ";Pooling=False"); await c.OpenAsync(Ct); await using var q = c.CreateCommand(); q.CommandText = sql; return await q.ExecuteScalarAsync(Ct); }
        public async Task ExecuteAsync(string sql) { await using var c = new SqliteConnection("Data Source=" + Database + ";Pooling=False"); await c.OpenAsync(Ct); await using var q = c.CreateCommand(); q.CommandText = sql; await q.ExecuteNonQueryAsync(Ct); }
        public async Task EvidenceAsync(string name, object value)
        {
            if (Environment.GetEnvironmentVariable("NEPTUNE_NATIVE_CAPTURE_EVIDENCE") is { } path)
            { Directory.CreateDirectory(path); await File.WriteAllTextAsync(Path.Combine(path, name + ".json"), JsonSerializer.Serialize(value, new JsonSerializerOptions { WriteIndented = true }), Ct); }
        }
        public ValueTask DisposeAsync()
        {
            SqliteConnection.ClearAllPools(); var owner = Path.GetFullPath(Environment.GetEnvironmentVariable("NEPTUNE_NATIVE_CAPTURE_TEST_ROOT") ?? Path.Combine(Path.GetTempPath(), "neptune-native-capture-tests"));
            if (!Path.GetFullPath(Root).StartsWith(owner + Path.DirectorySeparatorChar)) throw new InvalidOperationException("Fixture cleanup escaped owned audit source.");
            Directory.Delete(Root, true); return ValueTask.CompletedTask;
        }
    }
    private sealed class LateSource(string root, Task barrier, bool failAfterBarrier = false) : IBootstrapScanSource
    {
        private readonly FileSystemBootstrapScanSource _actual = new(); public bool Completed;
        public BootstrapDirectoryStamp ReadDirectoryStamp(string path) => _actual.ReadDirectoryStamp(path);
        public IEnumerable<BootstrapSourceEntry> EnumerateDirectory(string path)
        {
            if (Path.GetRelativePath(root, path) == "late")
            {
                barrier.Wait(TimeSpan.FromSeconds(10)); if (!barrier.IsCompletedSuccessfully) throw new IOException("Actual publication failed to release late scan.");
                if (failAfterBarrier) throw new UnauthorizedAccessException("Owned late branch is inaccessible"); Completed = true;
            }
            return _actual.EnumerateDirectory(path);
        }
    }
    private sealed class Transport : ICreateOnlyBootstrapTransport
    {
        public Uri TargetRoot => new("https://saturn.test/dav/sync/Audit/Mapping/"); public string CredentialScope => new('a', 64);
        public ConcurrentBag<BootstrapWorkRecord> Works = []; public ConcurrentDictionary<string, byte[]> Published = new();
        public bool FailOnce; public int Calls; private int _failed;
        private readonly ConcurrentDictionary<string, string> _ids = new();
        public async Task<BootstrapPublicationReceipt> TransferAsync(BootstrapWorkRecord work, Func<BootstrapTransferCheckpoint, Task> checkpoint, CancellationToken ct)
        {
            Interlocked.Increment(ref Calls); Works.Add(work);
            if (!work.IsDirectory)
            {
                var bytes = await File.ReadAllBytesAsync(work.SpoolPath!, ct); Assert.Equal(work.SealedSize, bytes.Length);
                Assert.Equal(work.Sha256, Convert.ToHexStringLower(SHA256.HashData(bytes)));
                await checkpoint(new("session", _ids.GetOrAdd(work.OperationId, _ => Guid.NewGuid().ToString()), bytes.Length));
                if (FailOnce && Interlocked.Exchange(ref _failed, 1) == 0) throw new IOException("Lost transport acknowledgement after received bytes");
                Published[work.OperationId] = bytes;
            }
            return new(work.OperationId, WebDavSyncClient.RelativeTargetUri(TargetRoot, work.RelativePath).AbsoluteUri,
                work.RelativePath, work.IsDirectory, work.SealedSize!.Value, work.Sha256, work.IsDirectory ? null : "\"sha256-" + work.Sha256 + "\"", BootstrapPublicationProof.LegacyObservedActive);
        }
    }
}
