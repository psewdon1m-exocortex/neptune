using System.Buffers.Binary;
using System.Text;

namespace Neptune.Core;

public static class UsnRecordParser
{
    public const int MaximumRecords = 256;
    public const int MaximumBufferBytes = 1024 * 1024;
    // Microsoft winioctl.h reasons through DESIRED_STORAGE_CLASS_CHANGE + CLOSE.
    public const uint KnownReasons = 0x81ffff77;
    private static readonly UnicodeEncoding Names = new(false, false, true);

    public static LocalJournalPage Parse(string journalIdHex, long requestedStartUsn, ReadOnlySpan<byte> output)
        => ParseCore(journalIdHex, requestedStartUsn, output, LocalJournalReadMode.Named);

    public static LocalJournalPage ParseIdentityOnly(string journalIdHex, long requestedStartUsn, ReadOnlySpan<byte> output)
        => ParseCore(journalIdHex, requestedStartUsn, output, LocalJournalReadMode.IdentityOnly);

    internal static LocalJournalPage ParseMode(string journalIdHex, long requestedStartUsn, ReadOnlySpan<byte> output, LocalJournalReadMode mode)
        => ParseCore(journalIdHex, requestedStartUsn, output, mode);

    private static LocalJournalPage ParseCore(string journalIdHex, long requestedStartUsn, ReadOnlySpan<byte> output, LocalJournalReadMode mode)
    {
        ValidateMode(mode);
        ValidateJournalId(journalIdHex);
        if (requestedStartUsn < 0 || output.Length < 8 || output.Length > MaximumBufferBytes)
            throw Gap("FrameBounds");
        var osNext = BinaryPrimitives.ReadInt64LittleEndian(output);
        if (osNext < requestedStartUsn) throw Gap("CursorRegression");
        var count = 0; var prefixEnd = output.Length; var next = osNext; var previous = requestedStartUsn - 1;
        // Validate the entire physical page before retaining a bounded prefix.
        // An unknown trailing frame must never be silently skipped by OS cursor.
        for (var offset = 8; offset < output.Length;)
        {
            var record = ValidateRecord(output[offset..], requestedStartUsn, previous, osNext, mode);
            if (count == MaximumRecords) { prefixEnd = Math.Min(prefixEnd, offset); next = Math.Min(next, record.Usn); }
            count++; previous = record.Usn; offset = checked(offset + record.Length);
        }
        if (count > 0 && osNext <= previous) throw Gap("InvalidOsCursor");
        var records = new LocalUsnRecord[Math.Min(count, MaximumRecords)];
        var position = 8;
        for (var i = 0; i < records.Length; i++)
        {
            var span = output[position..]; var length = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(span));
            var major=BinaryPrimitives.ReadUInt16LittleEndian(span[4..]); var shift=major==3?16:0; var idBytes=major==3?16:8;
            var nameLength = BinaryPrimitives.ReadUInt16LittleEndian(span[(56+shift)..]);
            var nameOffset = BinaryPrimitives.ReadUInt16LittleEndian(span[(58+shift)..]);
            // FIDs remain version-tagged opaque wire bytes. Do not truncate
            // FILE_ID_128 or infer a numeric path/identity from its contents.
            records[i] = new(BinaryPrimitives.ReadInt64LittleEndian(span[(24+shift)..]),
                Convert.ToHexStringLower(span.Slice(8,idBytes)), Convert.ToHexStringLower(span.Slice(8+idBytes,idBytes)),
                BinaryPrimitives.ReadUInt32LittleEndian(span[(40+shift)..]), BinaryPrimitives.ReadUInt32LittleEndian(span[(44+shift)..]),
                BinaryPrimitives.ReadUInt32LittleEndian(span[(52+shift)..]), Names.GetString(span.Slice(nameOffset, nameLength)),major) { ReadMode = mode };
            position += length;
        }
        var payload = output[..prefixEnd].ToArray();
        BinaryPrimitives.WriteInt64LittleEndian(payload, next);
        return new(journalIdHex, requestedStartUsn, next, payload, records, mode);
    }

    private static (int Length, long Usn) ValidateRecord(ReadOnlySpan<byte> span, long start, long previous, long next, LocalJournalReadMode mode)
    {
        if (span.Length < 60) throw Gap("TruncatedRecord");
        var length = BinaryPrimitives.ReadUInt32LittleEndian(span);
        if (length < 64 || length > span.Length || length % 8 != 0) throw Gap("RecordLength");
        var major=BinaryPrimitives.ReadUInt16LittleEndian(span[4..]); var shift=major==3?16:0;
        if (major is not (2 or 3) || BinaryPrimitives.ReadUInt16LittleEndian(span[6..]) != 0)
            throw Gap("UnsupportedRecordVersion");
        if(length<64+shift)throw Gap("TruncatedRecord");
        if(length>576+shift)throw Gap("OversizeRecord");
        var usn = BinaryPrimitives.ReadInt64LittleEndian(span[(24+shift)..]);
        if (usn < start || usn <= previous || usn >= next) throw Gap("RecordCursorOrder");
        var reason = BinaryPrimitives.ReadUInt32LittleEndian(span[(40+shift)..]);
        if (reason == 0 || (reason & ~KnownReasons) != 0) throw Gap("UnknownReason");
        // SourceInfo bits affect interpretation; future meanings are a gap.
        if ((BinaryPrimitives.ReadUInt32LittleEndian(span[(44+shift)..]) & ~0x0fu) != 0) throw Gap("UnknownSourceInfo");
        var nameLength = BinaryPrimitives.ReadUInt16LittleEndian(span[(56+shift)..]);
        var nameOffset = BinaryPrimitives.ReadUInt16LittleEndian(span[(58+shift)..]);
        if (mode == LocalJournalReadMode.IdentityOnly)
        {
            // The four alignment bytes are not a filename or reserved-zero
            // field. Native unprivileged reads can leave them nonzero. Retain
            // them in the raw digest; do not decode or normalize them.
            if (nameLength != 0 || nameOffset != 60+shift || length != 64+shift) throw Gap("IdentityOnlyNameBounds");
        }
        else if (nameLength == 0 || nameLength > 510 || nameLength % 2 != 0 || nameOffset < 60+shift || nameOffset % 2 != 0
            || nameOffset + nameLength > length) throw Gap("RecordNameBounds");
        try
        {
            var name = Names.GetString(span.Slice(nameOffset, nameLength));
            if (name.IndexOfAny(['\0', '/', '\\']) >= 0 || name is "." or "..") throw Gap("InvalidRecordName");
        }
        catch (DecoderFallbackException error) { throw new LocalSourceGapException("InvalidUnicode", "USN name encoding is invalid.", error); }
        return (checked((int)length), usn);
    }

    internal static void ValidateMode(LocalJournalReadMode mode)
    { if (mode is not (LocalJournalReadMode.Named or LocalJournalReadMode.IdentityOnly)) throw Gap("UnsupportedReadMode"); }

    // Existing named payloads remain byte-identical. Identity-only payloads
    // carry an explicit durable version/mode envelope wherever raw history is
    // copied (inbox, projection, coherence). They never masquerade as Named.
    internal static byte[] StoreRecord(ReadOnlySpan<byte> raw, LocalJournalReadMode mode)
    {
        ValidateMode(mode);
        if (mode == LocalJournalReadMode.Named) return raw.ToArray();
        var result = new byte[checked(raw.Length+8)];
        "NUSN"u8.CopyTo(result); result[4]=1; result[5]=1; raw.CopyTo(result.AsSpan(8));
        return result;
    }

    internal static LocalUsnRecord ParseStoredRecord(string journal, long usn, long end, ReadOnlySpan<byte> raw)
    {
        if (raw.Length > 592) throw Gap("StoredFrameBounds");
        var mode = LocalJournalReadMode.Named;
        if (raw.StartsWith("NUSN"u8))
        {
            if (raw.Length < 8 || raw[4]!=1 || raw[5]!=1 || raw[6]!=0 || raw[7]!=0) throw Gap("UnsupportedStoredMode");
            mode = LocalJournalReadMode.IdentityOnly; raw=raw[8..];
        }
        var frame = new byte[raw.Length+8]; BinaryPrimitives.WriteInt64LittleEndian(frame,end); raw.CopyTo(frame.AsSpan(8));
        var records = ParseMode(journal,usn,frame,mode).Records;
        if (records.Count!=1 || records[0].Usn!=usn) throw Gap("StoredRecordIdentity");
        return records[0];
    }

    internal static void ValidateJournalId(string value)
    {
        if (value.Length != 16 || value.Any(c => !char.IsAsciiHexDigit(c)) || value != value.ToLowerInvariant())
            throw new ArgumentException("Journal ID must be fixed unsigned 64-bit lowercase hex.");
    }
    private static LocalSourceGapException Gap(string code) => new(code, "USN page cannot establish ingestion continuity: " + code);
}
