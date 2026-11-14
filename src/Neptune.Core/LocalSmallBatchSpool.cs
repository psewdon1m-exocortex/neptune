using System.Buffers.Binary;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Microsoft.Win32.SafeHandles;

namespace Neptune.Core;

// One private aggregate, with immutable spans. It supplements retained original
// spools and uses the same inventory/quota. No source-file path is reopened.
internal static class LocalSmallBatchSpool
{
    internal static async Task<string?> TryBuildAsync(LocalCycleResources resources, LocalSmallBatch batch,
        Func<string, CancellationToken, Task<LocalCycleWork>> readOriginal,
        Func<string, CancellationToken, Task> bindIncarnation, CancellationToken ct)
    {
        if (batch.Phase != LocalSmallBatchPhase.Building || batch.SpoolIdentity is not null) throw new InvalidOperationException("Original aggregate creation is not repeatable.");
        if (!resources.TryReserveAggregate(batch.Plan.BatchId, batch.Plan.DataBytes)) return null;
        try
        {
            var path = resources.PathFor(batch.Plan.BatchId);
            await using var output = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 64 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
            WindowsBootstrapSourceCapture.RequireHandlePath(output.SafeFileHandle, path);
            // Persist identity while this exact new handle excludes rename and
            // writes, before payload bytes. An unrecorded creation is retained.
            var identity = Incarnation(output.SafeFileHandle); await bindIncarnation(identity, ct);
            using var aggregate = IncrementalHash.CreateHash(HashAlgorithmName.SHA256); var buffer = new byte[64 * 1024];
            for (var n = 0; n < batch.Spans.Count; n++)
            {
                var span = batch.Spans[n]; var expected = batch.Plan.Entries[n].Original.Intent; var work = await readOriginal(span.OperationId, ct);
                if (work.ScopeId != batch.ScopeId || work.OperationId != span.OperationId || work.Size != span.Size || work.Sha256 != expected.Sha256
                    || work.CaptureProof is null || Convert.ToHexStringLower(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(work.CaptureProof))) != span.SourceIdentity
                    || output.Position != span.Offset) throw new InvalidDataException("Original aggregate span changed.");
                await using var input = await resources.OpenAsync(work, false, ct); using var item = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
                long remaining = span.Size;
                while (remaining > 0)
                {
                    var count = (int)Math.Min(remaining, buffer.Length); await input.ReadExactlyAsync(buffer.AsMemory(0, count), ct);
                    item.AppendData(buffer, 0, count); aggregate.AppendData(buffer, 0, count); await output.WriteAsync(buffer.AsMemory(0, count), ct); remaining -= count;
                }
                if (input.Position != input.Length || Convert.ToHexStringLower(item.GetHashAndReset()) != expected.Sha256) throw new InvalidDataException("Original private bytes changed during aggregation.");
            }
            if (output.Length != batch.Plan.DataBytes || Incarnation(output.SafeFileHandle) != identity) throw new InvalidDataException("Aggregate identity or size changed.");
            await output.FlushAsync(ct); output.Flush(true); ct.ThrowIfCancellationRequested();
            return Convert.ToHexStringLower(aggregate.GetHashAndReset());
        }
        finally { resources.Release(batch.Plan.BatchId, false); }
    }

    internal static async Task<FileStream> OpenAsync(LocalCycleResources resources, LocalSmallBatch batch, CancellationToken ct)
    {
        if (batch.Phase is not (LocalSmallBatchPhase.Ready or LocalSmallBatchPhase.Attempted) || batch.SpoolHash is null || batch.SpoolIdentity is null)
            throw new InvalidOperationException("Network reads require a durable ready aggregate.");
        var file = Open(resources, batch, false);
        try { await Verify(file, batch, ct); file.Position = 0; return file; }
        catch { await file.DisposeAsync(); throw; }
    }

    internal static async Task RemoveAsync(LocalCycleResources resources, LocalSmallBatch batch, CancellationToken ct)
    {
        if (batch.Phase != LocalSmallBatchPhase.CleanupPending) throw new InvalidOperationException("Aggregate cleanup must be authorized durably first.");
        var path = resources.PathFor(batch.Plan.BatchId);
        // Idempotent recovery after exact handle deletion, before phase4 commit.
        if (!File.Exists(path)) { resources.ConfirmAggregateRemoved(batch.Plan.BatchId); return; }
        if (batch.SpoolIdentity is null) throw new InvalidDataException("Unrecorded aggregate incarnation is retained for explicit recovery.");
        await using (var file = Open(resources, batch, true))
        {
            if (batch.SpoolHash is not null) await Verify(file, batch, ct);
            else if (file.Length > batch.Plan.DataBytes) throw new InvalidDataException("Unsealed aggregate exceeds its reserved size.");
            ct.ThrowIfCancellationRequested(); var disposition = BitConverter.GetBytes(1);
            // Delete this verified handle, without a close/rename/path-delete
            // race. Share-read excludes competing writers and renamers.
            if (!SetFileInformationByHandle(file.SafeFileHandle, 4, disposition, 4)) throw Failure();
        }
        resources.ConfirmAggregateRemoved(batch.Plan.BatchId);
    }

    private static FileStream Open(LocalCycleResources resources, LocalSmallBatch batch, bool deleting)
    {
        if (!LocalCycleWire.Uuid(batch.Plan.BatchId)) throw new InvalidDataException("Invalid aggregate path identity.");
        var path = resources.PathFor(batch.Plan.BatchId);
        var handle = CreateFileW(path, 0x80000000u | (deleting ? 0x00010000u : 0), 1, nint.Zero, 3, 0x00200000u | 0x08000000u, nint.Zero);
        if (handle.IsInvalid) { var error = Failure(); handle.Dispose(); throw error; }
        try
        {
            WindowsBootstrapSourceCapture.RequireHandlePath(handle, path);
            if (Incarnation(handle) != batch.SpoolIdentity) throw new InvalidDataException("Original aggregate incarnation was substituted.");
            return new FileStream(handle, FileAccess.Read, 64 * 1024, false);
        }
        catch { handle.Dispose(); throw; }
    }

    private static async Task Verify(FileStream file, LocalSmallBatch batch, CancellationToken ct)
    {
        if (file.Length != batch.Plan.DataBytes) throw new InvalidDataException("Aggregate length changed.");
        using var aggregate = IncrementalHash.CreateHash(HashAlgorithmName.SHA256); var buffer = new byte[64 * 1024];
        for (var n = 0; n < batch.Spans.Count; n++)
        {
            var span = batch.Spans[n]; if (file.Position != span.Offset) throw new InvalidDataException("Aggregate offsets changed.");
            using var item = IncrementalHash.CreateHash(HashAlgorithmName.SHA256); long remaining = span.Size;
            while (remaining > 0)
            {
                var count = (int)Math.Min(remaining, buffer.Length); await file.ReadExactlyAsync(buffer.AsMemory(0, count), ct);
                item.AppendData(buffer, 0, count); aggregate.AppendData(buffer, 0, count); remaining -= count;
            }
            if (Convert.ToHexStringLower(item.GetHashAndReset()) != batch.Plan.Entries[n].Original.Intent.Sha256) throw new InvalidDataException("Aggregate original span digest changed.");
        }
        if (file.Position != file.Length || Convert.ToHexStringLower(aggregate.GetHashAndReset()) != batch.SpoolHash) throw new InvalidDataException("Aggregate fingerprint changed.");
    }

    private static string Incarnation(SafeFileHandle handle)
    {
        var id = Info(handle, 18, 24); var basic = Info(handle, 0, 40); var standard = Info(handle, 1, 24);
        if ((BinaryPrimitives.ReadUInt32LittleEndian(basic.AsSpan(32)) & (0x400u | 0x10u)) != 0
            || BinaryPrimitives.ReadUInt32LittleEndian(standard.AsSpan(16)) != 1 || standard[21] != 0)
            throw new InvalidDataException("Aggregate must be a regular private file with one link.");
        var identity = new byte[32]; id.CopyTo(identity, 0); basic.AsSpan(0, 8).CopyTo(identity.AsSpan(24)); return Convert.ToHexStringLower(SHA256.HashData(identity));
    }
    private static byte[] Info(SafeFileHandle handle, int kind, uint size)
    { var data = new byte[size]; if (!GetFileInformationByHandleEx(handle, kind, data, size)) throw Failure(); return data; }
    private static Win32Exception Failure() => new(Marshal.GetLastWin32Error(), "Native aggregate ownership operation failed.");
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)] private static extern SafeFileHandle CreateFileW(string path, uint access, uint share, nint security, uint creation, uint flags, nint template);
    [DllImport("kernel32.dll", SetLastError = true)] [return: MarshalAs(UnmanagedType.Bool)] private static extern bool GetFileInformationByHandleEx(SafeFileHandle handle, int information, [Out] byte[] data, uint size);
    [DllImport("kernel32.dll", SetLastError = true)] [return: MarshalAs(UnmanagedType.Bool)] private static extern bool SetFileInformationByHandle(SafeFileHandle handle, int information, byte[] data, uint size);
}
