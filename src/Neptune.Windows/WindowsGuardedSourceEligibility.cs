using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Windows;

// Read-only admission. Keep the native ancestry alive until the engine has
// acquired its own handles; no enumeration, journal mutation or profile init.
internal sealed class WindowsGuardedSourceEligibility : IAsyncDisposable
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private readonly WindowsNativeCycleCapture _capture;
    private readonly WindowsUsnJournalReader _journal;
    private readonly string _database;
    private readonly WindowsCycleConfiguration _configuration;
    internal long SubscriptionGeneration { get; private set; } = 1;

    private WindowsGuardedSourceEligibility(WindowsNativeCycleCapture capture, WindowsUsnJournalReader journal,
        string database, WindowsCycleConfiguration configuration)
        => (_capture, _journal, _database, _configuration) = (capture, journal, database, configuration);

    internal static async Task<WindowsGuardedSourceEligibility> OpenAsync(string source, string database,
        WindowsCycleConfiguration configuration, CancellationToken ct)
    {
        var capture = await WindowsNativeCycleCapture.OpenAsync(source, ct);
        WindowsUsnJournalReader? journal = null;
        try
        {
            journal = new(capture.RootProof.VolumeGuid, LocalJournalReadMode.IdentityOnly);
            var result = new WindowsGuardedSourceEligibility(capture, journal, database, configuration);
            result.SubscriptionGeneration = await result.ReadContinuityAsync(await journal.QueryAsync(ct), ct);
            return result;
        }
        catch { if (journal is not null) await journal.DisposeAsync(); await capture.DisposeAsync(); throw; }
    }

    internal async Task RequireCurrentAsync(CancellationToken ct)
    {
        await using var current = await WindowsNativeCycleCapture.OpenAsync(_capture.RootProof.RootPath, ct);
        if (current.RootProof != _capture.RootProof)
            throw new LocalSourceGapException("SourceRebound", "The opened source no longer has its admitted native identity.");
        if (await ReadContinuityAsync(await _journal.QueryAsync(ct), ct) != SubscriptionGeneration)
            throw new LocalSourceGapException("SourceRebound", "The retained subscription changed during source admission.");
    }

    private async Task<long> ReadContinuityAsync(LocalJournalBounds bounds, CancellationToken ct)
    {
        WindowsGuardedCapabilities.RequirePlainAncestors(_database);
        if (!File.Exists(_database)) return 1;
        await using var db = new SqliteConnection(new SqliteConnectionStringBuilder
        { DataSource = NeptuneStateStore.NativeDatabasePath(_database), Mode = SqliteOpenMode.ReadOnly, Cache = SqliteCacheMode.Private, Pooling = false }.ToString());
        await db.OpenAsync(ct);
        using var transaction = db.BeginTransaction(deferred: true);
        SqliteCommand Query(string sql)
        {
            var query = db.CreateCommand(); query.Transaction = transaction; query.CommandText = sql; return query;
        }
        await using (var format = Query("SELECT name FROM sqlite_schema WHERE name GLOB 'local_source_*' LIMIT 16"))
        {
            await using var reader = await format.ExecuteReaderAsync(ct);
            var names = new List<string>(); while (await reader.ReadAsync(ct)) names.Add(reader.GetString(0));
            if (names.Count == 0) return 1;
            if (!names.Contains("local_source_format") || names.Count == 16)
                throw new InvalidDataException("Retained source format is incomplete or unrecognized.");
        }
        long version;
        await using (var format = Query("SELECT version FROM local_source_format WHERE singleton=1"))
            version = await format.ExecuteScalarAsync(ct) is long v && v is 1 or 2 ? v
                : throw new InvalidDataException("Retained source format is unsupported.");
        await using var active = Query("SELECT a.mapping_id,a.root_key,CASE WHEN length(r.binding_json)<=4096 THEN r.binding_json END,r.journal_id,r.cursor,r.valid,r.gap_code,r.legacy_revision,v.revision," +
            (version == 1 ? "0" : "r.read_mode") + " FROM local_source_active a LEFT JOIN local_source_roots r ON r.root_key=a.root_key LEFT JOIN local_source_revision v ON v.singleton=1 LIMIT 2");
        await using var row = await active.ExecuteReaderAsync(ct);
        if (!await row.ReadAsync(ct))
            throw new InvalidDataException("Retained source module has no active subscription.");
        if (row.GetString(0) != _configuration.MappingId || row.IsDBNull(2))
            throw new InvalidDataException("Retained source cannot be rebound to another mapping.");
        var raw = row.GetString(2);
        using var parsed = SyncProtocolReader.Parse(Encoding.UTF8.GetBytes(raw));
        var binding = parsed.RootElement.Deserialize<LocalSourceBinding>(Json)
            ?? throw new InvalidDataException("Retained source binding is missing.");
        var proof = _capture.RootProof;
        var expected = new LocalSourceBinding(_configuration.ProfileId, _configuration.ClientId, _configuration.MappingId,
            proof.RootPath, proof.VolumeGuid, proof.Handle.LegacyVolumeSerial.ToString("x8", CultureInfo.InvariantCulture),
            "NTFS", proof.Handle.FileId128, proof.Handle.CreationTime, binding.SubscriptionGeneration,
            _configuration.RulesVersion, "windows-raw-nfc-v1");
        if (binding != expected || binding.SubscriptionGeneration < 1 ||
            row.GetString(1) != Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(expected, Json)))))
            throw new LocalSourceGapException("SourceRebound", "Retained profile, mapping or native source incarnation changed.");
        if (row.GetInt64(5) != 1 || !row.IsDBNull(6) || row.IsDBNull(8) || row.GetInt64(7) != row.GetInt64(8))
            throw new LocalSourceGapException("JournalGap", "Retained source requires explicit recovery before anchor admission.");
        if (row.GetString(3) != bounds.JournalIdHex || row.GetInt64(4) < Math.Max(bounds.FirstUsn, bounds.LowestValidUsn) || row.GetInt64(4) > bounds.NextUsn)
            throw new LocalSourceGapException("JournalGap", "The native journal no longer contains the retained cursor.");
        if (row.GetInt64(9) is not (0 or 1) || await row.ReadAsync(ct))
            throw new InvalidDataException("Retained mapping has unsupported or multiple active subscriptions.");
        await row.DisposeAsync();
        // A replacement is an explicit recovery workflow until installation;
        // ordinary opening must not skip its remaining qualification stages.
        await using (var present = Query("SELECT 1 FROM sqlite_schema WHERE name='local_cycle_replacement_requests' AND type='table'"))
        {
            if (await present.ExecuteScalarAsync(ct) is not null)
            {
                await using var cycleFormat = Query("SELECT version FROM local_cycle_format WHERE singleton=1");
                var cycleVersion = await cycleFormat.ExecuteScalarAsync(ct);
                if (cycleVersion is not long cycle || cycle is not (5 or 6 or 7))
                    throw new InvalidDataException("Retained replacement format is unsupported.");
                // Historical pending requests stop blocking only through a
                // reciprocal, bounded supersession chain ending in installation.
                // A dangling pointer, cycle or cross-binding link stays pending.
                await using var pending = Query(cycle == 5
                    ? "SELECT 1 FROM local_cycle_replacement_requests WHERE mapping_id=$mapping AND status<>3 LIMIT 1"
                    : """
                      WITH RECURSIVE installed(replacement_id,supersedes_id,original_scope,original_root,configuration,fingerprint,replacement_gap_json,depth) AS (
                        SELECT replacement_id,supersedes_id,original_scope,original_root,configuration,fingerprint,replacement_gap_json,0
                        FROM local_cycle_replacement_requests WHERE mapping_id=$mapping AND status=3 AND superseded_by IS NULL
                        UNION ALL
                        SELECT p.replacement_id,p.supersedes_id,p.original_scope,p.original_root,p.configuration,p.fingerprint,p.replacement_gap_json,i.depth+1
                        FROM installed i JOIN local_cycle_replacement_requests p ON p.replacement_id=i.supersedes_id
                        WHERE i.depth<15 AND p.mapping_id=$mapping AND p.superseded_by=i.replacement_id
                          AND p.original_scope=i.original_scope AND p.original_root=i.original_root
                          AND p.configuration=i.configuration AND p.fingerprint=i.fingerprint AND i.replacement_gap_json IS NOT NULL
                      )
                      SELECT 1 FROM local_cycle_replacement_requests p
                      WHERE p.mapping_id=$mapping AND p.status<>3
                        AND NOT EXISTS(SELECT 1 FROM installed i WHERE i.replacement_id=p.replacement_id) LIMIT 1
                      """);
                pending.Parameters.AddWithValue("$mapping", _configuration.MappingId);
                if (await pending.ExecuteScalarAsync(ct) is not null)
                    throw new LocalSourceGapException("JournalReplacementPending", "Explicit journal replacement is not installed yet.");
            }
        }
        return binding.SubscriptionGeneration;
    }

    public async ValueTask DisposeAsync()
    { try { await _journal.DisposeAsync(); } finally { await _capture.DisposeAsync(); } }
}
