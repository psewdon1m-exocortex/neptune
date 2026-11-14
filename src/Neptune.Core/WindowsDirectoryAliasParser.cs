using System.Buffers.Binary;
using System.Text;

namespace Neptune.Core;

// FILE_ID_BOTH_DIR_INFO, native FileId64 bytes. No relation to FileId128 is
// guessed. Long raw names are joined by the store; ShortName is never an alias.
public static class WindowsDirectoryAliasParser
{
    public const int MaximumBufferBytes=24*1024,MaximumRecords=256;
    private static readonly UnicodeEncoding StrictUnicode=new(false,false,true);
    public static IReadOnlyList<LocalDirectoryAlias> Parse(ReadOnlySpan<byte> buffer)
    {
        if(buffer.Length<106||buffer.Length>MaximumBufferBytes)throw new InvalidDataException("Alias directory buffer is unbounded/truncated.");
        var entries=new List<LocalDirectoryAlias>();var offset=0;var frames=0;
        while(true)
        {
            if(++frames>MaximumRecords||buffer.Length-offset<106)throw new InvalidDataException("Alias directory frame/header exceeds bounds.");
            var frame=buffer[offset..];var next=BinaryPrimitives.ReadUInt32LittleEndian(frame);
            var length=BinaryPrimitives.ReadUInt32LittleEndian(frame[60..]);
            if(length is <2 or >510||(length&1)!=0||length>frame.Length-104)throw new InvalidDataException("Alias long name framing is invalid.");
            var shortLength=frame[68];if(shortLength>24||(shortLength&1)!=0)throw new InvalidDataException("Alias short name length is invalid.");
            var end=checked(104+(int)length);
            if(next!=0&&(next<end||(next&7)!=0||next>frame.Length-106))throw new InvalidDataException("Alias next offset is invalid.");
            string name;
            try {name=StrictUnicode.GetString(frame.Slice(104,(int)length));_=StrictUnicode.GetString(frame.Slice(70,shortLength));}
            catch(DecoderFallbackException e){throw new InvalidDataException("Alias name has malformed UTF16.",e);}
            var id=Convert.ToHexStringLower(frame.Slice(96,8));var attributes=BinaryPrimitives.ReadUInt32LittleEndian(frame[56..]);
            if(id==new string('0',16)||name.Any(c=>c is '\0' or '/' or '\\'||char.IsControl(c))||(attributes&(0x40|0x10000))!=0)
                throw new InvalidDataException("Alias identity/name/type is unsupported.");
            if(name is not("." or ".."))entries.Add(new(name,id,BinaryPrimitives.ReadInt64LittleEndian(frame[8..]),attributes));
            if(next==0){if(frame[end..].ContainsAnyExcept((byte)0))throw new InvalidDataException("Alias response has unexplained tail.");return entries.AsReadOnly();}
            if(frame.Slice(end,(int)next-end).ContainsAnyExcept((byte)0))throw new InvalidDataException("Alias response has nonzero frame padding.");
            offset+=(int)next;
        }
    }
    internal static void Validate(LocalDirectoryAlias entry)
    {
        if(entry.Name.Length is <1 or >255||entry.Name is "." or ".."||entry.Name.Any(c=>c is '\0' or '/' or '\\'||char.IsControl(c))
            ||entry.FileId64.Length!=16||entry.FileId64==new string('0',16)||entry.FileId64!=entry.FileId64.ToLowerInvariant()||entry.FileId64.Any(c=>!char.IsAsciiHexDigit(c)))
            throw new InvalidDataException("Alias entry is not an exact supported native identity.");
        try{_=StrictUnicode.GetBytes(entry.Name);}catch(EncoderFallbackException e){throw new InvalidDataException("Alias raw name UTF16 is invalid.",e);}
    }
}
