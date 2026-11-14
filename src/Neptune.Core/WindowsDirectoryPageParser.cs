using System.Buffers.Binary;
using System.Text;

namespace Neptune.Core;

// FILE_ID_EXTD_DIR_INFO only. Offsets are raw native bytes, not JSON numbers or
// normalized identifiers. FileIndex is deliberately neither read nor persisted.
public static class WindowsDirectoryPageParser
{
    public const int MaximumBufferBytes = 24 * 1024, MaximumRecords = 256;
    private static readonly UnicodeEncoding StrictUnicode = new(false, false, true);
    public static IReadOnlyList<LocalNamespaceEntry> Parse(ReadOnlySpan<byte> buffer)
    {
        if (buffer.Length < 90 || buffer.Length > MaximumBufferBytes)
            throw new InvalidDataException("Directory buffer cannot establish a supported bounded frame.");
        var result = new List<LocalNamespaceEntry>(); var offset = 0; var frames = 0;
        while (true)
        {
            if (++frames > MaximumRecords || buffer.Length - offset < 90) throw new InvalidDataException("Directory frame limit or header is invalid.");
            var frame = buffer[offset..]; var next = BinaryPrimitives.ReadUInt32LittleEndian(frame);
            var length = BinaryPrimitives.ReadUInt32LittleEndian(frame[60..]);
            if (length is < 2 or > 510 || (length & 1) != 0 || length > frame.Length - 88)
                throw new InvalidDataException("Directory name framing is invalid.");
            var end = checked(88 + (int)length);
            if (next != 0 && (next < end || (next & 7) != 0 || next > frame.Length - 90))
                throw new InvalidDataException("Directory next-frame offset is invalid.");
            string name;
            try { name = StrictUnicode.GetString(frame.Slice(88, (int)length)); }
            catch (DecoderFallbackException error) { throw new InvalidDataException("Directory name contains malformed UTF16.", error); }
            var id = Convert.ToHexStringLower(frame.Slice(72, 16));
            var attributes = BinaryPrimitives.ReadUInt32LittleEndian(frame[56..]);
            var tag = (attributes & 0x400) != 0 ? BinaryPrimitives.ReadUInt32LittleEndian(frame[68..]) : 0;
            var size = BinaryPrimitives.ReadInt64LittleEndian(frame[40..]);
            if (id == new string('0', 32) || size < 0 || name.Any(c => c is '\0' or '/' or '\\' || char.IsControl(c))
                || (attributes & (0x40 | 0x10000)) != 0)
                throw new InvalidDataException("Directory identity/type/name is unsupported.");
            // Native . and .. records describe the enumeration protocol; they
            // do not become source entries or make arbitrary invalid names safe.
            if (name is not ("." or ".."))
                result.Add(new(name, id, BinaryPrimitives.ReadInt64LittleEndian(frame[8..]),
                    BinaryPrimitives.ReadInt64LittleEndian(frame[24..]), BinaryPrimitives.ReadInt64LittleEndian(frame[32..]),
                    size, attributes, tag));
            if (next == 0)
            {
                // The API has no bytes-returned field. Caller zeroes its owned
                // buffer; nonzero unexplained tail is an unsupported response.
                if (frame[end..].ContainsAnyExcept((byte)0)) throw new InvalidDataException("Unexplained directory buffer tail.");
                return result.AsReadOnly();
            }
            if (frame.Slice(end, (int)next - end).ContainsAnyExcept((byte)0))
                throw new InvalidDataException("Nonzero directory frame padding.");
            offset += (int)next;
        }
    }
    internal static void ValidateEntry(LocalNamespaceEntry entry)
    {
        if (entry.Name.Length is < 1 or > 255 || entry.Name is "." or ".."
            || entry.Name.Any(c => c is '\0' or '/' or '\\' || char.IsControl(c))
            || entry.FileId128.Length != 32 || entry.FileId128 == new string('0',32)
            || entry.FileId128 != entry.FileId128.ToLowerInvariant() || entry.FileId128.Any(c=>!char.IsAsciiHexDigit(c))
            || entry.Size < 0 || (entry.Attributes & (0x40 | 0x10000)) != 0
            || (entry.Attributes & 0x400) == 0 && entry.ReparseTag != 0)
            throw new InvalidDataException("Namespace entry is not supported raw metadata.");
        try { _ = StrictUnicode.GetBytes(entry.Name); }
        catch (EncoderFallbackException error) { throw new InvalidDataException("Namespace name has malformed UTF16.",error); }
    }
}
