using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private const long LocalWalBudget = 64L * 1024 * 1024;
    private long _localTransactions, _localMaxChanges, _localMaxCommitTicks, _localCheckpoints, _localAdmissionStops;
    private long _localCheckpointDue, _localOutstanding = -1, _localPhysicalWal, _localCheckpointPhysical, _localReservedWrites;
    private long _localPageSize=65536;
    public LocalJournalMetrics GetLocalJournalMetrics() => new(Interlocked.Read(ref _localTransactions),
        Interlocked.Read(ref _localMaxChanges), Interlocked.Read(ref _localMaxCommitTicks), Interlocked.Read(ref _localCheckpoints),
        Interlocked.Read(ref _localAdmissionStops), Interlocked.Read(ref _localPhysicalWal), Interlocked.Read(ref _localOutstanding));

    // A whole-lifetime OS lock protects one profile reader/writer, including
    // across process death. Epochs fence stale session objects; PID is no proof.
    public Task<LocalSourceJournalSession> OpenLocalJournalSessionAsync(LocalSourceBinding binding,
        LocalJournalBounds bounds, long initialStartUsn, LocalJournalAdmission? admission = null,
        CancellationToken cancellationToken = default)
        => OpenLocalJournalSessionAsync(binding,bounds,initialStartUsn,admission,LocalJournalReadMode.Named,cancellationToken);

    public async Task<LocalSourceJournalSession> OpenLocalJournalSessionAsync(LocalSourceBinding binding,
        LocalJournalBounds bounds,long initialStartUsn,LocalJournalAdmission? admission,LocalJournalReadMode readMode,
        CancellationToken cancellationToken=default)
    {
        UsnRecordParser.ValidateMode(readMode);
        ValidateLocalBinding(binding); WindowsUsnJournalReader.ValidateBounds(bounds);
        if (initialStartUsn != bounds.FirstUsn && initialStartUsn != bounds.NextUsn)
            throw new ArgumentException("An initial subscription must use an observed OS FirstUsn or NextUsn boundary.");
        admission ??= new();
        if (admission.MaximumInboxBytes < 1024 || admission.MaximumDatabaseBytes < 1024 * 1024 || admission.MinimumFreeDiskBytes < 0)
            throw new ArgumentOutOfRangeException(nameof(admission));
        await ValidateOptionalLocalNamespaceBeforeInitializeAsync(cancellationToken);
        await ValidateOptionalLocalProjectionBeforeInitializeAsync(cancellationToken);
        await ValidateOptionalLocalCoherenceBeforeInitializeAsync(cancellationToken);
        await ValidateOptionalLocalCycleBeforeInitializeAsync(cancellationToken);
        await PreflightSyncStateAsync(false, cancellationToken);
        await using(var preflight=new SqliteConnection(ReadOnlySyncConnectionString(databasePath)))
        {
            await preflight.OpenAsync(cancellationToken);await using var version=preflight.CreateCommand();version.CommandText="SELECT version FROM sync_state_format WHERE singleton=1";
            if(await version.ExecuteScalarAsync(cancellationToken) is not long main || main!=SyncStateFormatVersion)throw new InvalidDataException("Initialize the recognized profile before opening its optional local subscription.");
        }
        CheckLocalDiskAdmission(admission,0,includeMigrationBackup:await PreflightLocalSourceAsync(cancellationToken)!=2);
        var volumeKey = LocalHash(binding.VolumeGuid + "/" + binding.VolumeSerial);
        var ownerFile = new FileStream(Path.GetFullPath(databasePath) + ".local-source-owner.lock",
            FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        try
        {
            await EnsureLocalSourceAsync(cancellationToken);
            await using var connection = await OpenLocalSourceConnectionAsync(cancellationToken);
            using var transaction = connection.BeginTransaction(deferred: false);
            await using(var pageSize=SyncCommand(connection,transaction,"PRAGMA page_size"))
            {var size=Convert.ToInt64(await pageSize.ExecuteScalarAsync(cancellationToken));ValidateLocalPageSize(size);Interlocked.Exchange(ref _localPageSize,size);}
            await using(var occupancy=SyncCommand(connection,transaction,"SELECT inbox_bytes FROM local_source_format WHERE singleton=1"))
                if(await occupancy.ExecuteScalarAsync(cancellationToken) is not long occupied || occupied>admission.MaximumInboxBytes)
                    throw new LocalSourceGapException("InboxCapacity","Persisted inbox exceeds the configured admission quota.");
            await RequireLocalMappingAsync(connection, transaction, binding, cancellationToken);
            var json = JsonSerializer.Serialize(binding, SyncStateJson); var rootKey = LocalHash(json); var token = Guid.NewGuid().ToString("N");
            var revision = await LocalRevisionAsync(connection, transaction, cancellationToken);
            await using (var active = SyncCommand(connection, transaction, "SELECT r.root_key,r.binding_json FROM local_source_active a JOIN local_source_roots r ON r.root_key=a.root_key WHERE a.mapping_id=$mapping"))
            {
                active.Parameters.AddWithValue("$mapping", binding.MappingId);
                await using var reader = await active.ExecuteReaderAsync(cancellationToken);
                if (await reader.ReadAsync(cancellationToken) && reader.GetString(0) != rootKey)
                {
                    var previous = JsonSerializer.Deserialize<LocalSourceBinding>(reader.GetString(1), SyncStateJson)!;
                    if (binding.SubscriptionGeneration <= previous.SubscriptionGeneration)
                        throw new InvalidOperationException("Rebinding requires a strictly newer explicit local subscription generation.");
                    await reader.DisposeAsync();
                    await using var invalidate = SyncCommand(connection, transaction, "UPDATE local_source_roots SET valid=0,gap_epoch=gap_epoch+1,gap_code='Rebound' WHERE root_key IN (SELECT root_key FROM local_source_active WHERE mapping_id=$mapping)");
                    invalidate.Parameters.AddWithValue("$mapping", binding.MappingId); await invalidate.ExecuteNonQueryAsync(cancellationToken);
                }
            }
            // Fixed root-history budget prevents unbounded subscription churn.
            await using (var count = SyncCommand(connection, transaction, "SELECT COUNT(*) FROM (SELECT 1 FROM local_source_roots LIMIT 65)"))
                if (Convert.ToInt64(await count.ExecuteScalarAsync(cancellationToken)) >= 64)
                {
                    await using var exists = SyncCommand(connection, transaction, "SELECT 1 FROM local_source_roots WHERE root_key=$root");
                    exists.Parameters.AddWithValue("$root", rootKey);
                    if (await exists.ExecuteScalarAsync(cancellationToken) is null) throw new LocalSourceGapException("SubscriptionCapacity", "Retire old subscriptions before admitting another.");
                }
            await using var insert = SyncCommand(connection, transaction, "INSERT INTO local_source_roots(root_key,mapping_id,binding_json,volume_key,journal_id,cursor,owner_token,owner_epoch,valid,legacy_revision,read_mode) VALUES($root,$mapping,$json,$volume,$journal,$cursor,$owner,1,1,$revision,$mode) ON CONFLICT(root_key) DO UPDATE SET owner_token=$owner,owner_epoch=owner_epoch+1");
            insert.Parameters.AddWithValue("$mode",(int)readMode);
            insert.Parameters.AddWithValue("$root", rootKey); insert.Parameters.AddWithValue("$mapping", binding.MappingId);
            insert.Parameters.AddWithValue("$json", json); insert.Parameters.AddWithValue("$volume", volumeKey);
            insert.Parameters.AddWithValue("$journal", bounds.JournalIdHex); insert.Parameters.AddWithValue("$cursor", initialStartUsn);
            insert.Parameters.AddWithValue("$owner", token); insert.Parameters.AddWithValue("$revision", revision);
            await insert.ExecuteNonQueryAsync(cancellationToken);
            await using var setActive = SyncCommand(connection, transaction, "INSERT INTO local_source_active(mapping_id,root_key) VALUES($mapping,$root) ON CONFLICT(mapping_id) DO UPDATE SET root_key=$root");
            setActive.Parameters.AddWithValue("$mapping", binding.MappingId); setActive.Parameters.AddWithValue("$root", rootKey);
            await setActive.ExecuteNonQueryAsync(cancellationToken);
            var state = await ReadLocalStateAsync(connection, transaction, rootKey, token, cancellationToken);
            if (state.LegacyRevision != revision) await LocalGapAsync(connection, transaction, rootKey, "LegacyWrite", cancellationToken);
            else
            {
                try { WindowsUsnJournalReader.ValidateCursor(bounds, state.Cursor); }
                catch (LocalSourceGapException error) { await LocalGapAsync(connection, transaction, rootKey, error.Code, cancellationToken); }
            }
            state=await ReadLocalStateAsync(connection,transaction,rootKey,token,cancellationToken);
            if (state.ReadMode!=readMode)
            {
                // No new subscription/key, skipped range or synthetic name.
                // A named->identity-only switch is explicit at the retained OS
                // cursor; the reverse requires separate reviewed recovery.
                if(state.ReadMode==LocalJournalReadMode.Named&&readMode==LocalJournalReadMode.IdentityOnly&&!state.Cursor.Valid)
                {
                    // Refusing the mode transition must not roll back the
                    // journal/legacy gap just discovered under this owner.
                    // The cursor and original record mode stay unchanged.
                    await transaction.CommitAsync(cancellationToken);
                    throw new LocalSourceGapException("ReadModeTransition","Read mode transition refused; discovered continuity gap retained.");
                }
                if(state.ReadMode!=LocalJournalReadMode.Named||readMode!=LocalJournalReadMode.IdentityOnly)
                    throw new LocalSourceGapException("ReadModeTransition","Read mode cannot be changed without retained valid continuity.");
                await using var mode=SyncCommand(connection,transaction,"UPDATE local_source_roots SET read_mode=$mode WHERE root_key=$root AND owner_token=$owner AND valid=1");
                mode.Parameters.AddWithValue("$mode",(int)readMode);mode.Parameters.AddWithValue("$root",rootKey);mode.Parameters.AddWithValue("$owner",token);
                if(await mode.ExecuteNonQueryAsync(cancellationToken)!=1)throw new InvalidOperationException("Read mode owner changed.");
            }
            await transaction.CommitAsync(cancellationToken);
            return await CreateOwnedJournalSessionAsync(ownerFile, rootKey, token, admission, cancellationToken);
        }
        catch { await ownerFile.DisposeAsync(); throw; }
    }

    private static void ValidateLocalBinding(LocalSourceBinding b)
    {
        foreach (var value in new[] { b.ProfileId, b.ClientId, b.MappingId, b.RulesVersion, b.NameCodec })
            if (string.IsNullOrWhiteSpace(value) || value.Length > 128 || value.Any(char.IsControl)) throw new ArgumentException("Local identity fields are missing or unbounded.");
        if (!Guid.TryParseExact(b.VolumeGuid, "D", out var volume) || volume.ToString("D") != b.VolumeGuid || b.FileSystem != "NTFS"
            || b.VolumeSerial.Length is not (8 or 16) || b.VolumeSerial.Any(c => !char.IsAsciiHexDigit(c)) || b.VolumeSerial != b.VolumeSerial.ToLowerInvariant()
            || b.RootFileId.Length != 32 || b.RootFileId.Any(c => !char.IsAsciiHexDigit(c)) || b.RootFileId != b.RootFileId.ToLowerInvariant()
            || b.RootIncarnation <= 0 || b.SubscriptionGeneration <= 0 || b.SourceRoot.Length > 4096 || !Path.IsPathFullyQualified(b.SourceRoot)
            || b.SourceRoot != Path.GetFullPath(b.SourceRoot)) throw new ArgumentException("Local volume/root binding is not qualified or canonical.");
    }
    private static string LocalHash(string value) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
    private long _validatedLocalSchemaCookie=-1;
    private async Task<SqliteConnection> OpenLocalSourceConnectionAsync(CancellationToken cancellationToken, bool schemaRequired = true)
    {
        var connection = new SqliteConnection(LocalSourceConnectionString(Volatile.Read(ref _ownedLocalPool)!=0));
        try
        {
            await connection.OpenAsync(cancellationToken);
            SyncDiagnostics.AttachLocalSqliteDiagnostics(connection);
            await using(var schema=connection.CreateCommand())
            {
                schema.CommandText="PRAGMA schema_version";
                var cookie=Convert.ToInt64(await schema.ExecuteScalarAsync(cancellationToken));
                if(!schemaRequired||cookie!=Interlocked.Read(ref _validatedLocalSchemaCookie))
                {
                    await ValidateOptionalLocalNamespaceBeforeInitializeAsync(cancellationToken);
                    await ValidateOptionalLocalProjectionBeforeInitializeAsync(cancellationToken);
                    await ValidateOptionalLocalCoherenceBeforeInitializeAsync(cancellationToken);
                    await ValidateOptionalLocalCycleBeforeInitializeAsync(cancellationToken);
                    await ValidateOptionalParentReconcileBeforeInitializeAsync(cancellationToken);
                    if(schemaRequired&&await PreflightLocalSourceAsync(cancellationToken)!=2)throw new InvalidDataException("Local source module requires initialization/migration.");
                    if(Convert.ToInt64(await schema.ExecuteScalarAsync(cancellationToken))!=cookie)throw new InvalidDataException("Local schema changed during validation.");
                    if(schemaRequired)Interlocked.Exchange(ref _validatedLocalSchemaCookie,cookie);
                }
            }
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT version FROM sync_state_format WHERE singleton=1";
            if (await command.ExecuteScalarAsync(cancellationToken) is not long version || version != SyncStateFormatVersion)
                throw new InvalidDataException("Main state format changed.");
            command.CommandText = "PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=1000; PRAGMA foreign_keys=ON";
            await command.ExecuteNonQueryAsync(cancellationToken); return connection;
        }
        catch { await connection.DisposeAsync(); throw; }
    }
    private static async Task RequireLocalMappingAsync(SqliteConnection c, SqliteTransaction t, LocalSourceBinding binding, CancellationToken ct)
    {
        await using var command = SyncCommand(c,t,"SELECT local_path,enabled FROM sync_mappings WHERE mapping_id=$mapping AND client_instance_id=$client");
        command.Parameters.AddWithValue("$mapping",binding.MappingId); command.Parameters.AddWithValue("$client",binding.ClientId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct) || reader.GetInt64(1) != 1 || !string.Equals(Path.GetFullPath(reader.GetString(0)),binding.SourceRoot,
            OperatingSystem.IsWindows()?StringComparison.OrdinalIgnoreCase:StringComparison.Ordinal))
            throw new LocalSourceGapException("MappingChanged", "Mapping is absent, paused, or rebound.");
    }
    private static async Task<long> LocalRevisionAsync(SqliteConnection c,SqliteTransaction t,CancellationToken ct)
    {
        await using var command=SyncCommand(c,t,"SELECT revision FROM local_source_revision WHERE singleton=1");
        return await command.ExecuteScalarAsync(ct) is long revision ? revision : throw new InvalidDataException("Local revision singleton is missing.");
    }
    private static async Task<LocalSourceContinuityState> ReadLocalStateAsync(SqliteConnection c,SqliteTransaction t,string root,string owner,CancellationToken ct)
    {
        await using var command=SyncCommand(c,t,"SELECT binding_json,journal_id,cursor,owner_epoch,valid,gap_epoch,gap_code,inbox_records,inbox_bytes,legacy_revision,retired_through,read_mode FROM local_source_roots WHERE root_key=$root AND owner_token=$owner AND root_key IN (SELECT root_key FROM local_source_active)");
        command.Parameters.AddWithValue("$root",root); command.Parameters.AddWithValue("$owner",owner);
        await using var r=await command.ExecuteReaderAsync(ct);
        if (!await r.ReadAsync(ct)) throw new InvalidOperationException("Local source owner or active subscription changed.");
        var readMode=(LocalJournalReadMode)r.GetInt32(11);UsnRecordParser.ValidateMode(readMode);
        return new(JsonSerializer.Deserialize<LocalSourceBinding>(r.GetString(0),SyncStateJson)!,new(r.GetString(1),r.GetInt64(2),r.GetInt64(3),r.GetInt64(4)!=0),
            r.GetInt64(5),r.IsDBNull(6)?null:r.GetString(6),r.GetInt64(7),r.GetInt64(8),r.GetInt64(9),r.GetInt64(10)) { ReadMode=readMode };
    }
    private static async Task LocalGapAsync(SqliteConnection c,SqliteTransaction t,string root,string code,CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(code) || code.Length>128 || code.Any(char.IsControl)) throw new ArgumentException("Gap code is invalid.");
        await using var command=SyncCommand(c,t,"UPDATE local_source_roots SET valid=0,gap_epoch=gap_epoch+CASE WHEN valid=1 OR gap_code IS NULL THEN 1 ELSE 0 END,gap_code=COALESCE(gap_code,$code) WHERE root_key=$root");
        command.Parameters.AddWithValue("$root",root); command.Parameters.AddWithValue("$code",code); await command.ExecuteNonQueryAsync(ct);
    }
    internal async Task<LocalSourceContinuityState> ReadOwnedLocalStateAsync(string root,string owner,CancellationToken ct)
    {
        await using var c=await OpenLocalSourceConnectionAsync(ct); using var t=c.BeginTransaction(deferred:false);
        var state=await ReadLocalStateAsync(c,t,root,owner,ct);
        if (state.LegacyRevision!=await LocalRevisionAsync(c,t,ct)) await LocalGapAsync(c,t,root,"LegacyWrite",ct);
        try { await RequireLocalMappingAsync(c,t,state.Binding,ct); }
        catch(LocalSourceGapException error) { await LocalGapAsync(c,t,root,error.Code,ct); }
        state=await ReadLocalStateAsync(c,t,root,owner,ct); await t.CommitAsync(ct); return state;
    }
    internal async Task MarkOwnedLocalGapAsync(string root,string owner,string code,CancellationToken ct)
    {
        await using var c=await OpenLocalSourceConnectionAsync(ct); using var t=c.BeginTransaction(deferred:false);
        _=await ReadLocalStateAsync(c,t,root,owner,ct); await LocalGapAsync(c,t,root,code,ct); await t.CommitAsync(ct);
    }
    internal async Task<LocalJournalWriteResult> CaptureOwnedLocalPageAsync(string root,string owner,LocalJournalCursor expected,
        LocalJournalPage page,LocalJournalAdmission admission,CancellationToken ct)
    {
        // Reparse private immutable bytes: no caller-created record list can
        // alter the stored boundary or claim that unsupported data is handled.
        page=UsnRecordParser.ParseMode(page.JournalIdHex,page.StartUsn,page.Payload,page.ReadMode);
        var state=await ReadOwnedLocalStateAsync(root,owner,ct);
        if(page.ReadMode!=state.ReadMode)throw new LocalSourceGapException("ReadModeMismatch","Page mode differs from its durable source owner.");
        if (!state.Cursor.Valid) throw new LocalSourceGapException(state.GapCode??"InvalidCursor","Local ingestion requires explicit recovery/new subscription.");
        if(expected.OwnerEpoch!=state.Cursor.OwnerEpoch || expected.JournalIdHex!=state.Cursor.JournalIdHex || expected.NextReadUsn!=page.StartUsn)
            throw new InvalidOperationException("Expected owner/cursor changed.");
        // Retrying the last already committed page consumes no storage budget.
        // Verify inside the writer transaction below before acknowledging it.
        if(state.Cursor.NextReadUsn==page.StartUsn)
        {
        if(page.Records.Count==0 && page.NextReadUsn==page.StartUsn)return new(true,0,page.NextReadUsn,0);
        // Reserve dirty-page amplification before admitting a known-schema V2/V3
        // batch. Physical WAL growth and conservatively charged writes bound
        // cached PASSIVE results; no assumption that a checkpoint cleared frames.
        var walReserve=checked((8L*page.Records.Count+64)*(Interlocked.Read(ref _localPageSize)+24)+32);
        try { await CheckLocalAdmissionAsync(admission,page.ByteCount+page.Records.Count*(page.ReadMode==LocalJournalReadMode.IdentityOnly?72L:64L),walReserve,ct); }
        // WAL admission rejects this page before its transaction or cursor
        // write. A pinned reader is backpressure, not proof of lost history.
        // CaptureNext still validates fresh journal bounds before any retry.
        catch(LocalSourceGapException error) when(error.Code is not("WalAdmission" or "InboxCapacity")) { await MarkOwnedLocalGapAsync(root,owner,error.Code,CancellationToken.None); throw; }
        }
        await using var c=await OpenLocalSourceConnectionAsync(ct); using var t=c.BeginTransaction(deferred:false);
        var before=await BootstrapTotalChangesAsync(c,t,ct); state=await ReadLocalStateAsync(c,t,root,owner,ct);
        if (state.LegacyRevision!=await LocalRevisionAsync(c,t,ct))
        { await LocalGapAsync(c,t,root,"LegacyWrite",ct); await t.CommitAsync(ct); throw new LocalSourceGapException("LegacyWrite","A legacy writer invalidated local continuity."); }
        try { await RequireLocalMappingAsync(c,t,state.Binding,ct); }
        catch(LocalSourceGapException error) { await LocalGapAsync(c,t,root,error.Code,ct); await t.CommitAsync(ct); throw; }
        if (!state.Cursor.Valid || expected.OwnerEpoch!=state.Cursor.OwnerEpoch || expected.JournalIdHex!=state.Cursor.JournalIdHex
            || expected.NextReadUsn!=page.StartUsn || page.JournalIdHex!=expected.JournalIdHex)
            throw new InvalidOperationException("Expected owner/cursor/page binding changed.");
        using var digest=IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        if(page.ReadMode!=LocalJournalReadMode.Named)digest.AppendData([(byte)page.ReadMode]);
        digest.AppendData(page.Payload);var hash=Convert.ToHexStringLower(digest.GetHashAndReset());
        if(state.Cursor.NextReadUsn!=page.StartUsn)
        {
            await using var duplicate=SyncCommand(c,t,"SELECT 1 FROM local_source_roots WHERE root_key=$root AND last_start=$start AND last_end=$end AND last_hash=$hash AND cursor=$end");
            duplicate.Parameters.AddWithValue("$root",root); duplicate.Parameters.AddWithValue("$start",page.StartUsn); duplicate.Parameters.AddWithValue("$end",page.NextReadUsn); duplicate.Parameters.AddWithValue("$hash",hash);
            if(await duplicate.ExecuteScalarAsync(ct) is not null) return new(true,page.Records.Count,page.NextReadUsn,0);
            throw new InvalidOperationException("Cursor page replay is not the last identical committed page.");
        }
        await using var quota=SyncCommand(c,t,"SELECT inbox_bytes FROM local_source_format WHERE singleton=1");
        var cost=page.ByteCount-8+page.Records.Count*(page.ReadMode==LocalJournalReadMode.IdentityOnly?72L:64L);
        if(Convert.ToInt64(await quota.ExecuteScalarAsync(ct))+cost>admission.MaximumInboxBytes)
        { throw new LocalSourceGapException("InboxCapacity","Durable inbox is full; no page was skipped. Retire qualified applied history before retrying the same cursor."); }
        var offset=8;
        foreach(var record in page.Records)
        {
            ct.ThrowIfCancellationRequested(); var length=System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(page.Payload.AsSpan(offset));
            await using var insert=SyncCommand(c,t,"INSERT INTO local_source_inbox(root_key,journal_id,usn,payload,byte_count) VALUES($root,$journal,$usn,$payload,$bytes)");
            insert.Parameters.AddWithValue("$root",root); insert.Parameters.AddWithValue("$journal",page.JournalIdHex); insert.Parameters.AddWithValue("$usn",record.Usn);
            var stored=UsnRecordParser.StoreRecord(page.Payload.AsSpan(offset,length),page.ReadMode);
            insert.Parameters.AddWithValue("$payload",stored); insert.Parameters.AddWithValue("$bytes",stored.Length+64);
            await insert.ExecuteNonQueryAsync(ct); offset+=length;
        }
        await using var update=SyncCommand(c,t,"UPDATE local_source_roots SET cursor=$end,inbox_records=inbox_records+$count,inbox_bytes=inbox_bytes+$cost,last_start=$start,last_end=$end,last_hash=$hash WHERE root_key=$root; UPDATE local_source_format SET inbox_records=inbox_records+$count,inbox_bytes=inbox_bytes+$cost WHERE singleton=1");
        update.Parameters.AddWithValue("$root",root); update.Parameters.AddWithValue("$end",page.NextReadUsn); update.Parameters.AddWithValue("$start",page.StartUsn);
        update.Parameters.AddWithValue("$count",page.Records.Count); update.Parameters.AddWithValue("$cost",cost); update.Parameters.AddWithValue("$hash",hash);
        await update.ExecuteNonQueryAsync(ct); var changed=await CommitLocalWriteAsync(c,t,before,ct);
        return new(false,page.Records.Count,page.NextReadUsn,changed);
    }
    internal async Task<IReadOnlyList<LocalUsnRecord>> ReadOwnedLocalInboxAsync(string root,string owner,long afterUsn,CancellationToken ct)
    {
        await using var c=await OpenLocalSourceConnectionAsync(ct); using var t=c.BeginTransaction(deferred:true);
        var state=await ReadLocalStateAsync(c,t,root,owner,ct); var records=new List<LocalUsnRecord>(256);
        await using var query=SyncCommand(c,t,"SELECT usn,payload FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn>$after ORDER BY usn LIMIT 256");
        query.Parameters.AddWithValue("$root",root); query.Parameters.AddWithValue("$journal",state.Cursor.JournalIdHex); query.Parameters.AddWithValue("$after",afterUsn);
        await using var reader=await query.ExecuteReaderAsync(ct);
        while(await reader.ReadAsync(ct))
        {
            records.Add(UsnRecordParser.ParseStoredRecord(state.Cursor.JournalIdHex,reader.GetInt64(0),state.Cursor.NextReadUsn,(byte[])reader.GetValue(1)));
        }
        return records.AsReadOnly();
    }
    // LC1 cannot certify applied membership. Retirement is therefore available
    // only for an already-gapped subscription, after explicit recovery decision.
    internal async Task<LocalJournalRetirement> RetireOwnedLocalInboxAsync(string root,string owner,long beforeUsn,LocalJournalAdmission admission,CancellationToken ct)
    {
        await CheckLocalAdmissionAsync(admission,0,checked((8L*256+64)*(Interlocked.Read(ref _localPageSize)+24)+32),ct);
        await using var c=await OpenLocalSourceConnectionAsync(ct); using var t=c.BeginTransaction(deferred:false);
        var effects=await BootstrapTotalChangesAsync(c,t,ct); var state=await ReadLocalStateAsync(c,t,root,owner,ct);
        if(state.Cursor.Valid || beforeUsn<0 || beforeUsn>state.Cursor.NextReadUsn) throw new InvalidOperationException("Only a gapped inbox prefix within its ingested cursor can be retired.");
        await RequireNoCoherenceInboxPin(c,t,root,state.Cursor.JournalIdHex,0,beforeUsn,ct);
        var keys=new List<(long Usn,long Bytes)>(256);
        await using(var query=SyncCommand(c,t,"SELECT usn,byte_count FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn<$before ORDER BY usn LIMIT 256"))
        {
            query.Parameters.AddWithValue("$root",root); query.Parameters.AddWithValue("$journal",state.Cursor.JournalIdHex); query.Parameters.AddWithValue("$before",beforeUsn);
            await using var reader=await query.ExecuteReaderAsync(ct); while(await reader.ReadAsync(ct)) keys.Add((reader.GetInt64(0),reader.GetInt64(1)));
        }
        long bytes=0;
        foreach(var key in keys)
        {
            await using var delete=SyncCommand(c,t,"DELETE FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn=$usn");
            delete.Parameters.AddWithValue("$root",root); delete.Parameters.AddWithValue("$journal",state.Cursor.JournalIdHex); delete.Parameters.AddWithValue("$usn",key.Usn);
            await delete.ExecuteNonQueryAsync(ct); bytes+=key.Bytes;
        }
        await using var counters=SyncCommand(c,t,"UPDATE local_source_roots SET inbox_records=inbox_records-$count,inbox_bytes=inbox_bytes-$bytes,retired_through=MAX(retired_through,$last) WHERE root_key=$root; UPDATE local_source_format SET inbox_records=inbox_records-$count,inbox_bytes=inbox_bytes-$bytes WHERE singleton=1");
        counters.Parameters.AddWithValue("$root",root); counters.Parameters.AddWithValue("$count",keys.Count); counters.Parameters.AddWithValue("$bytes",bytes);
        counters.Parameters.AddWithValue("$last",keys.Count>0?keys[^1].Usn:state.RetiredThroughUsn); await counters.ExecuteNonQueryAsync(ct);
        return new(keys.Count,bytes,await CommitLocalWriteAsync(c,t,effects,ct));
    }
    // Explicit recovery can retire an inactive, already-fenced older generation.
    // Never delete another mapping/volume or an active subscription's prefix.
    internal async Task<LocalJournalHistoryRetirement> RetireSupersededLocalInboxAsync(string root,string owner,
        LocalSourceBinding superseded,LocalJournalAdmission admission,CancellationToken ct)
    {
        ValidateLocalBinding(superseded);
        var current=await ReadOwnedLocalStateAsync(root,owner,ct);
        if(superseded.ProfileId!=current.Binding.ProfileId || superseded.ClientId!=current.Binding.ClientId
            || superseded.MappingId!=current.Binding.MappingId || superseded.VolumeGuid!=current.Binding.VolumeGuid
            || superseded.VolumeSerial!=current.Binding.VolumeSerial || superseded.SubscriptionGeneration>=current.Binding.SubscriptionGeneration)
            throw new InvalidOperationException("Retirement requires an explicit older binding for this mapping and volume.");
        await CheckLocalAdmissionAsync(admission,0,checked((8L*256+64)*(Interlocked.Read(ref _localPageSize)+24)+32),ct);
        var previous=LocalHash(JsonSerializer.Serialize(superseded,SyncStateJson));
        await RefusePinnedLocalProjectionAsync(previous,false,ct);
        await using var c=await OpenLocalSourceConnectionAsync(ct);using var t=c.BeginTransaction(deferred:false);
        _=await ReadLocalStateAsync(c,t,root,owner,ct);var effects=await BootstrapTotalChangesAsync(c,t,ct);
        await RequireNoCoherenceRootPin(c,t,previous,ct);
        await using(var check=SyncCommand(c,t,"SELECT 1 FROM local_source_roots WHERE root_key=$root AND valid=0 AND root_key NOT IN(SELECT root_key FROM local_source_active)"))
        {check.Parameters.AddWithValue("$root",previous);if(await check.ExecuteScalarAsync(ct) is null)throw new InvalidOperationException("Only an existing superseded, fenced subscription may be retired.");}
        var keys=new List<(string Journal,long Usn,long Bytes)>(256);
        await using(var query=SyncCommand(c,t,"SELECT journal_id,usn,byte_count FROM local_source_inbox WHERE root_key=$root ORDER BY journal_id,usn LIMIT 256"))
        {
            query.Parameters.AddWithValue("$root",previous);await using var reader=await query.ExecuteReaderAsync(ct);
            while(await reader.ReadAsync(ct))keys.Add((reader.GetString(0),reader.GetInt64(1),reader.GetInt64(2)));
        }
        long bytes=0;
        foreach(var key in keys)
        {
            await using var delete=SyncCommand(c,t,"DELETE FROM local_source_inbox WHERE root_key=$root AND journal_id=$journal AND usn=$usn");
            delete.Parameters.AddWithValue("$root",previous);delete.Parameters.AddWithValue("$journal",key.Journal);delete.Parameters.AddWithValue("$usn",key.Usn);
            await delete.ExecuteNonQueryAsync(ct);bytes+=key.Bytes;
        }
        await using(var counters=SyncCommand(c,t,"UPDATE local_source_roots SET inbox_records=inbox_records-$count,inbox_bytes=inbox_bytes-$bytes WHERE root_key=$root;UPDATE local_source_format SET inbox_records=inbox_records-$count,inbox_bytes=inbox_bytes-$bytes WHERE singleton=1"))
        {counters.Parameters.AddWithValue("$root",previous);counters.Parameters.AddWithValue("$count",keys.Count);counters.Parameters.AddWithValue("$bytes",bytes);await counters.ExecuteNonQueryAsync(ct);}
        await using var retire=SyncCommand(c,t,"DELETE FROM local_source_roots WHERE root_key=$root AND inbox_records=0 AND NOT EXISTS(SELECT 1 FROM local_source_inbox WHERE root_key=$root)");
        retire.Parameters.AddWithValue("$root",previous);var removed=await retire.ExecuteNonQueryAsync(ct)==1;
        return new(keys.Count,bytes,removed,await CommitLocalWriteAsync(c,t,effects,ct));
    }
    private async Task<long> CommitLocalWriteAsync(SqliteConnection c,SqliteTransaction t,long before,CancellationToken ct)
    {
        var changes=await BootstrapTotalChangesAsync(c,t,ct)-before; var watch=Stopwatch.StartNew(); await t.CommitAsync(ct); watch.Stop();
        Interlocked.Increment(ref _localTransactions); UpdateBootstrapMaximum(ref _localMaxChanges,changes); UpdateBootstrapMaximum(ref _localMaxCommitTicks,watch.Elapsed.Ticks); return changes;
    }
    private async Task CheckLocalAdmissionAsync(LocalJournalAdmission admission,long incoming,long walReserve,CancellationToken ct)
    {
        CheckLocalDiskAdmission(admission,Math.Max(incoming,walReserve));
        await InspectLocalJournalWalAsync(cancellationToken:ct);
        long UpperBound()
        {
            var physical=Interlocked.Read(ref _localPhysicalWal);var measured=Interlocked.Read(ref _localOutstanding);
            if(measured<0)return physical;
            return Math.Min(physical,checked(measured+Math.Max(Interlocked.Read(ref _localReservedWrites),Math.Max(0,physical-Interlocked.Read(ref _localCheckpointPhysical)))));
        }
        if(UpperBound()>LocalWalBudget-walReserve)
        {
            // The last conservative estimate can outlive a released reader or
            // include reservations already checkpointed by SQLite. Wait for
            // the existing throttle, then make ONE fresh PASSIVE observation.
            // Never admit on time alone, spin, truncate, or wait for a reader.
            var remaining=Interlocked.Read(ref _localCheckpointDue)-Environment.TickCount64;
            if(Interlocked.Read(ref _localOutstanding)>=0&&remaining>0)
                await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(remaining,1000)),ct);
            await InspectLocalJournalWalAsync(force:true,cancellationToken:ct);
        }
        if(UpperBound()>LocalWalBudget-walReserve)
        { Interlocked.Increment(ref _localAdmissionStops); throw new LocalSourceGapException("WalAdmission","Outstanding WAL exceeds admission budget; PASSIVE does not truncate or wait for readers."); }
        Interlocked.Add(ref _localReservedWrites,walReserve);
    }
    private void CheckLocalDiskAdmission(LocalJournalAdmission admission,long incoming,bool includeMigrationBackup=false)
    {
        var databaseBytes=File.Exists(databasePath)?new FileInfo(databasePath).Length:0;
        var walBytes=File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0;
        long backupBytes=0;var backupCount=0;
        foreach(var backup in Directory.EnumerateFiles(Path.GetDirectoryName(Path.GetFullPath(databasePath))!,Path.GetFileName(databasePath)+".pre-*.db"))
        {
            if(++backupCount>64)throw new LocalSourceGapException("DiskAdmission","Profile rollback inventory exceeds its bounded admission limit.");
            backupBytes=checked(backupBytes+new FileInfo(backup).Length);
        }
        var free=new DriveInfo(Path.GetPathRoot(Path.GetFullPath(databasePath))!).AvailableFreeSpace;
        var reserve=includeMigrationBackup?Math.Max(incoming,databaseBytes+walBytes):incoming;
        if(databaseBytes>admission.MaximumDatabaseBytes-walBytes-backupBytes-reserve || free<admission.MinimumFreeDiskBytes || free-admission.MinimumFreeDiskBytes<reserve)
        { Interlocked.Increment(ref _localAdmissionStops); throw new LocalSourceGapException("DiskAdmission","Profile disk budget/floor prevents bounded inbox admission."); }
    }
    // -1 outstanding means not measured; a physical file <=64MiB still proves
    // admission within the conservative upper bound, not zero pending frames.
    public async Task<LocalJournalMetrics> InspectLocalJournalWalAsync(bool force=false,CancellationToken cancellationToken=default)
    {
        var ct=cancellationToken;var now=Environment.TickCount64;
        var walBytes=File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0;
        Interlocked.Exchange(ref _localPhysicalWal,walBytes);
        if(walBytes==0)Interlocked.Exchange(ref _localOutstanding,0);
        if(force || walBytes>LocalWalBudget && now>=Interlocked.Read(ref _localCheckpointDue))
        {
            await using var c=await OpenLocalSourceConnectionAsync(ct); await using var command=c.CreateCommand(); command.CommandText="PRAGMA busy_timeout=0";
            await command.ExecuteNonQueryAsync(ct);command.CommandText="PRAGMA page_size";
            var pageSize=Convert.ToInt64(await command.ExecuteScalarAsync(ct));
            ValidateLocalPageSize(pageSize);Interlocked.Exchange(ref _localPageSize,pageSize);
            command.CommandText="PRAGMA wal_checkpoint(PASSIVE)";
            Interlocked.Increment(ref _localCheckpoints); await using var reader=await command.ExecuteReaderAsync(ct); await reader.ReadAsync(ct);
            var log=reader.GetInt64(1); var done=reader.GetInt64(2); var outstanding=log>=0&&done>=0?checked(Math.Max(0,log-done)*(pageSize+24)):walBytes;
            Interlocked.Exchange(ref _localOutstanding,outstanding); Interlocked.Exchange(ref _localCheckpointDue,now+1000);
            Interlocked.Exchange(ref _localCheckpointPhysical,walBytes);Interlocked.Exchange(ref _localReservedWrites,0);
        }
        return GetLocalJournalMetrics();
    }
    private static void ValidateLocalPageSize(long size)
    {if(size is <512 or >65536 || (size&(size-1))!=0)throw new InvalidDataException("SQLite page size cannot establish WAL admission accounting.");}
}

public sealed partial class LocalSourceJournalSession : IAsyncDisposable
{
    private readonly NeptuneStateStore _store; private readonly FileStream _lock; private readonly string _root,_owner;
    internal SyncResourceScheduler ResourceScheduler => _store.ResourceScheduler;
    private readonly SqliteConnection _lifetimeConnection;
    private readonly LocalJournalAdmission _admission; private readonly SemaphoreSlim _serial=new(1,1); private bool _disposed;
    internal LocalSourceJournalSession(NeptuneStateStore store,FileStream ownerLock,SqliteConnection lifetimeConnection,string root,string owner,LocalJournalAdmission admission)
        => (_store,_lock,_lifetimeConnection,_root,_owner,_admission)=(store,ownerLock,lifetimeConnection,root,owner,admission);
    private async Task<T> SerialAsync<T>(Func<Task<T>> action,CancellationToken ct,[System.Runtime.CompilerServices.CallerMemberName] string operation="")
    {
        await _serial.WaitAsync(ct); try { ObjectDisposedException.ThrowIf(_disposed,this); using var measurement=SyncDiagnostics.MeasureLocalHelper(operation); return await action(); } finally { _serial.Release(); }
    }
    public Task<LocalSourceContinuityState> ReadStateAsync(CancellationToken cancellationToken=default)
        => SerialAsync(()=>_store.ReadOwnedLocalStateAsync(_root,_owner,cancellationToken),cancellationToken);
    public Task<LocalJournalWriteResult> CapturePageAsync(LocalJournalCursor expected,LocalJournalPage page,CancellationToken cancellationToken=default)
        => SerialAsync(()=>_store.CaptureOwnedLocalPageAsync(_root,_owner,expected,page,_admission,cancellationToken),cancellationToken);
    public Task MarkGapAsync(string code,CancellationToken cancellationToken=default)
        => SerialAsync(async()=>{ await _store.MarkOwnedLocalGapAsync(_root,_owner,code,cancellationToken); return true; },cancellationToken);
    public Task<IReadOnlyList<LocalUsnRecord>> ReadInboxAsync(long afterUsn=-1,CancellationToken cancellationToken=default)
        => SerialAsync(()=>_store.ReadOwnedLocalInboxAsync(_root,_owner,afterUsn,cancellationToken),cancellationToken);
    public Task<LocalJournalRetirement> RetireGappedInboxBatchAsync(long beforeUsn,CancellationToken cancellationToken=default)
        => SerialAsync(()=>_store.RetireOwnedLocalInboxAsync(_root,_owner,beforeUsn,_admission,cancellationToken),cancellationToken);
    public Task<LocalJournalHistoryRetirement> RetireSupersededInboxBatchAsync(LocalSourceBinding superseded,CancellationToken cancellationToken=default)
        => SerialAsync(()=>_store.RetireSupersededLocalInboxAsync(_root,_owner,superseded,_admission,cancellationToken),cancellationToken);
    public Task<LocalJournalWriteResult> CaptureNextAsync(ILocalJournalReader reader,int maximumBufferBytes=256*1024,CancellationToken cancellationToken=default)
        => SerialAsync(async()=>
        {
            var state=await _store.ReadOwnedLocalStateAsync(_root,_owner,cancellationToken);
            try
            {
                if(!state.Cursor.Valid) throw new LocalSourceGapException(state.GapCode??"InvalidCursor","Explicit recovery/new subscription required.");
                if(reader.Volume!=new LocalJournalVolumeIdentity(state.Binding.VolumeGuid,state.Binding.VolumeSerial,state.Binding.FileSystem))
                    throw new LocalSourceGapException("VolumeBinding","Reader volume identity differs from the durable subscription.");
                if(reader.ReadMode!=state.ReadMode)throw new LocalSourceGapException("ReadModeMismatch","Reader mode differs from the durable source owner.");
                var bounds=await reader.QueryAsync(cancellationToken); WindowsUsnJournalReader.ValidateCursor(bounds,state.Cursor);
                var page=await reader.ReadAsync(state.Cursor,maximumBufferBytes,cancellationToken);
                return await _store.CaptureOwnedLocalPageAsync(_root,_owner,state.Cursor,page,_admission,cancellationToken);
            }
            catch(LocalSourceGapException error) when(error.Code is not("WalAdmission" or "InboxCapacity")) { await _store.MarkOwnedLocalGapAsync(_root,_owner,error.Code,CancellationToken.None); throw; }
        },cancellationToken);
    public async ValueTask DisposeAsync()
    {
        await _serial.WaitAsync();
        try { if(_disposed)return; _disposed=true; try { _store.CloseOwnedLocalPool(); } finally { try { await _lifetimeConnection.DisposeAsync(); } finally { await _lock.DisposeAsync(); } } }
        finally { _serial.Release(); }
    }
}
