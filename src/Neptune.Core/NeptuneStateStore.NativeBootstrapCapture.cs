namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    // Format 3 is explicitly opt-in. Ordinary bootstrap/Windows callers retain
    // format 2 and never acquire a native source witness retroactively.
    internal async Task EnsureNativeBootstrapWorkSchemaAsync(CancellationToken cancellationToken)
    {
        await EnsureBootstrapWorkSchemaAsync(cancellationToken);
        if (await PreflightBootstrapWorkAsync(cancellationToken) == 3) return;
        await using var migration = await AcquireSyncMigrationLockAsync(cancellationToken);
        if (await PreflightBootstrapWorkAsync(cancellationToken) == 3) return;
        await BackupBeforeBootstrapWorkAsync(cancellationToken, 3);
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        await ExecuteSyncAsync(connection, transaction, """
            ALTER TABLE bootstrap_work_items ADD COLUMN native_capture TEXT;
            UPDATE bootstrap_work_format SET version=3 WHERE singleton=1;
            """, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    internal async Task SealNativeBootstrapWorkAsync(BootstrapWorkRecord work, NativeBootstrapSeal seal, CancellationToken cancellationToken)
    {
        await EnsureNativeBootstrapWorkSchemaAsync(cancellationToken);
        var source = seal.Source;
        NativeBootstrapCaptureProof.Validate(seal.ProofJson, work, source.Size, source.Sha256, source.SourceIdentity);
        await CheckBootstrapAdmissionAsync(cancellationToken);
        await using var connection = await OpenSyncConnectionAsync(cancellationToken);
        using var transaction = connection.BeginTransaction(deferred: false);
        await RequireBootstrapWorkSessionAsync(connection, transaction, work.SessionId, cancellationToken);
        await using var read = SyncCommand(connection, transaction, $"SELECT {BootstrapWorkSelectColumns(3)} FROM bootstrap_work_items w JOIN bootstrap_work_sessions s ON s.session_id=w.session_id WHERE w.work_id=$id AND w.operation_id=$operation");
        AddWorkIdentity(read, work);
        BootstrapWorkRecord original;
        await using (var reader = await read.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken)) throw new InvalidDataException("The original native capture work was lost.");
            original = ReadBootstrapWork(reader);
        }
        NativeBootstrapCaptureProof.Validate(seal.ProofJson, original, source.Size, source.Sha256, source.SourceIdentity);
        if (original.Phase != BootstrapWorkPhase.Queued || original.Attempt != 0 || original.NativeCaptureProofJson is not null
            || source.Size != original.EnumeratedSize || source.SpoolPath.Length is < 1 or > 4096)
            throw new InvalidOperationException("A native seal cannot replace an existing intent or source generation.");
        await using var update = SyncCommand(connection, transaction, "UPDATE bootstrap_work_items SET phase=1,spool_path=$spool,sealed_size=$size,sha256=$hash,source_identity=$identity,native_capture=$proof WHERE work_id=$id AND operation_id=$operation AND phase=0 AND attempt=0 AND native_capture IS NULL");
        update.Parameters.AddWithValue("$spool", source.SpoolPath); update.Parameters.AddWithValue("$size", source.Size);
        update.Parameters.AddWithValue("$hash", source.Sha256); update.Parameters.AddWithValue("$identity", source.SourceIdentity);
        update.Parameters.AddWithValue("$proof", seal.ProofJson); AddWorkIdentity(update, original);
        if (await update.ExecuteNonQueryAsync(cancellationToken) != 1) throw new InvalidOperationException("The native source intent changed before its seal committed.");
        await transaction.CommitAsync(cancellationToken);
    }
}
