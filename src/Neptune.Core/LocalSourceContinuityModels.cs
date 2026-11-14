namespace Neptune.Core;

// IdentityOnly records contain no historical leaf name. A current lookup may
// not fill it in or turn it into named-journal namespace evidence.
public enum LocalJournalReadMode { Named = 0, IdentityOnly = 1 }

// This domain is local only. An inbox cursor is not a closed source cycle,
// namespace cut, common baseline, current remote state, or permission to delete.
public sealed record LocalSourceBinding(string ProfileId, string ClientId, string MappingId,
    string SourceRoot, string VolumeGuid, string VolumeSerial, string FileSystem,
    string RootFileId, long RootIncarnation, long SubscriptionGeneration,
    string RulesVersion, string NameCodec);

public sealed record LocalJournalBounds(string JournalIdHex, long FirstUsn, long LowestValidUsn,
    long NextUsn, ushort MinMajor, ushort MaxMajor, bool RangeTrackingEnabled);
public sealed record LocalJournalCursor(string JournalIdHex, long NextReadUsn, long OwnerEpoch, bool Valid);
public sealed record LocalJournalVolumeIdentity(string VolumeGuid, string VolumeSerial, string FileSystem);
public sealed record LocalUsnRecord(long Usn, string FileIdHex, string ParentIdHex,
    uint Reason, uint SourceInfo, uint FileAttributes, string Name, ushort MajorVersion = 2)
{
    public LocalJournalReadMode ReadMode { get; init; } = LocalJournalReadMode.Named;
}
public sealed record LocalSourceContinuityState(LocalSourceBinding Binding, LocalJournalCursor Cursor,
    long GapEpoch, string? GapCode, long InboxRecords, long InboxBytes, long LegacyRevision,
    long RetiredThroughUsn)
{
    public LocalJournalReadMode ReadMode { get; init; } = LocalJournalReadMode.Named;
    public string LocalCompleteness => "Unknown";
    public string SourceCycle => "Unknown";
}
public sealed record LocalJournalAdmission(long MaximumInboxBytes = 256L * 1024 * 1024,
    long MinimumFreeDiskBytes = 1024L * 1024 * 1024, long MaximumDatabaseBytes = 1024L * 1024 * 1024);
public sealed record LocalJournalWriteResult(bool Duplicate, int Records, long NextReadUsn, long ChangedRows);
public sealed record LocalJournalRetirement(int Records, long Bytes, long ChangedRows);
public sealed record LocalJournalHistoryRetirement(int Records,long Bytes,bool SubscriptionRemoved,long ChangedRows);
public sealed record LocalJournalMetrics(long Transactions, long MaximumChangedRows, long MaximumCommitTicks,
    long CheckpointAttempts, long AdmissionStops, long PhysicalWalBytes, long UncheckpointedWalBytes);

public sealed class LocalSourceGapException(string code, string message, Exception? inner = null)
    : IOException(message, inner)
{
    public string Code { get; } = code;
}

public interface ILocalJournalReader : IAsyncDisposable
{
    LocalJournalReadMode ReadMode => LocalJournalReadMode.Named;
    LocalJournalVolumeIdentity Volume { get; }
    ValueTask<LocalJournalBounds> QueryAsync(CancellationToken cancellationToken = default);
    ValueTask<LocalJournalPage> ReadAsync(LocalJournalCursor cursor, int maximumBufferBytes = 256 * 1024,
        CancellationToken cancellationToken = default);
}

// Construction is restricted to the validating parser. Raw bytes are private,
// immutable, and revalidated at persistence; public host diagnostics need only counts.
public sealed class LocalJournalPage
{
    internal LocalJournalPage(string journal, long start, long next, byte[] payload, LocalUsnRecord[] records,
        LocalJournalReadMode readMode = LocalJournalReadMode.Named)
        => (JournalIdHex, StartUsn, NextReadUsn, Payload, Records, ReadMode) = (journal, start, next, payload, Array.AsReadOnly(records), readMode);
    public LocalJournalReadMode ReadMode { get; }
    public string JournalIdHex { get; }
    public long StartUsn { get; }
    public long NextReadUsn { get; }
    public IReadOnlyList<LocalUsnRecord> Records { get; }
    public int ByteCount => Payload.Length;
    internal byte[] Payload { get; }
}
