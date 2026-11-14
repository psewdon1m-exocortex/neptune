using System.Buffers.Binary;
using System.ComponentModel;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace Neptune.Core;

// Deliberately only QUERY and READ. No elevation, journal creation, truncation,
// resize, close-record forcing, or range-tracking control is implemented here.
public sealed class WindowsUsnJournalReader : ILocalJournalReader
{
    private const uint QueryJournal = 0x000900f4, ReadJournal = 0x000900bb, ReadIdentityOnlyJournal = 0x000903ab;
    private readonly SafeFileHandle _volume;
    public LocalJournalVolumeIdentity Volume { get; }
    public LocalJournalReadMode ReadMode { get; }
    public WindowsUsnJournalReader(string volumeGuid) : this(volumeGuid, LocalJournalReadMode.Named) { }
    public WindowsUsnJournalReader(string volumeGuid, LocalJournalReadMode readMode)
    {
        UsnRecordParser.ValidateMode(readMode); ReadMode=readMode;
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException("NTFS journal reads require Windows.");
        if (!Guid.TryParseExact(volumeGuid, "D", out var guid) || guid.ToString("D") != volumeGuid)
            throw new ArgumentException("An explicit canonical volume GUID is required.");
        var fs=new StringBuilder(64);
        if(!Native.GetVolumeInformationW("\\\\?\\Volume{"+volumeGuid+"}\\",null,0,out var serial,out _,out _,fs,fs.Capacity))
            throw Failure(Marshal.GetLastWin32Error());
        Volume=new(volumeGuid,serial.ToString("x8",CultureInfo.InvariantCulture),fs.ToString());
        if(Volume.FileSystem!="NTFS") throw new LocalSourceGapException("UnsupportedFileSystem","This adapter qualifies NTFS V2/V3 ingestion only.");
        // The unprivileged FSCTL uses a volume root DIRECTORY, not DASD.
        // Named remains the default; this opt-in mode has no historical names.
        _volume = Native.CreateFileW("\\\\?\\Volume{" + volumeGuid + "}" + (readMode==LocalJournalReadMode.IdentityOnly?"\\":""),
            0x80000000, readMode==LocalJournalReadMode.IdentityOnly?7u:3u, IntPtr.Zero, 3,
            readMode==LocalJournalReadMode.IdentityOnly?0x02000000u:0u, IntPtr.Zero);
        if (_volume.IsInvalid) { var error = Marshal.GetLastWin32Error(); _volume.Dispose(); throw Failure(error); }
    }
    public ValueTask<LocalJournalBounds> QueryAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested(); var output = new byte[80];
        if (!Native.DeviceIoControl(_volume, QueryJournal, null, 0, output, output.Length, out var returned, IntPtr.Zero))
            throw Failure(Marshal.GetLastWin32Error());
        // V0/V1 do not prove range tracking disabled. Require the complete V2
        // response on this opt-in adapter; unavailable rights/versions degrade.
        if (returned != 80) throw new LocalSourceGapException("QueryVersion", "Journal range-tracking capability is unknown.");
        var flags = BinaryPrimitives.ReadUInt32LittleEndian(output.AsSpan(60));
        if ((flags & ~1u) != 0) throw new LocalSourceGapException("QueryFlags", "Unknown journal tracking flags.");
        var bounds = new LocalJournalBounds(BinaryPrimitives.ReadUInt64LittleEndian(output).ToString("x16", CultureInfo.InvariantCulture),
            BinaryPrimitives.ReadInt64LittleEndian(output.AsSpan(8)), BinaryPrimitives.ReadInt64LittleEndian(output.AsSpan(24)),
            BinaryPrimitives.ReadInt64LittleEndian(output.AsSpan(16)), BinaryPrimitives.ReadUInt16LittleEndian(output.AsSpan(56)),
            BinaryPrimitives.ReadUInt16LittleEndian(output.AsSpan(58)), (flags & 1) != 0);
        ValidateBounds(bounds); cancellationToken.ThrowIfCancellationRequested(); return ValueTask.FromResult(bounds);
    }
    public async ValueTask<LocalJournalPage> ReadAsync(LocalJournalCursor cursor, int maximumBufferBytes = 256 * 1024,
        CancellationToken cancellationToken = default)
    {
        if (maximumBufferBytes is < 1024 or > UsnRecordParser.MaximumBufferBytes) throw new ArgumentOutOfRangeException(nameof(maximumBufferBytes));
        if (!cursor.Valid) throw new LocalSourceGapException("InvalidCursor", "Journal cursor is invalid.");
        var before = await QueryAsync(cancellationToken); ValidateCursor(before, cursor);
        var input = new byte[48]; BinaryPrimitives.WriteInt64LittleEndian(input, cursor.NextReadUsn);
        BinaryPrimitives.WriteUInt32LittleEndian(input.AsSpan(8), uint.MaxValue); // no reason filtering
        BinaryPrimitives.WriteUInt64LittleEndian(input.AsSpan(32), ulong.Parse(cursor.JournalIdHex, NumberStyles.HexNumber, CultureInfo.InvariantCulture));
        BinaryPrimitives.WriteUInt16LittleEndian(input.AsSpan(40), before.MinMajor);
        BinaryPrimitives.WriteUInt16LittleEndian(input.AsSpan(42), before.MaxMajor);
        // ReturnOnlyOnClose, Timeout, BytesToWaitFor are zero: all records and a
        // bounded nonblocking read. Coalesced open cycles remain UNKNOWN.
        var output = new byte[maximumBufferBytes];
        if (!Native.DeviceIoControl(_volume, ReadMode==LocalJournalReadMode.IdentityOnly?ReadIdentityOnlyJournal:ReadJournal,
            input, input.Length, output, output.Length, out var returned, IntPtr.Zero))
            throw Failure(Marshal.GetLastWin32Error());
        var after = await QueryAsync(cancellationToken); ValidateCursor(after, cursor);
        var page = UsnRecordParser.ParseMode(cursor.JournalIdHex, cursor.NextReadUsn, output.AsSpan(0, returned), ReadMode);
        if (page.NextReadUsn > after.NextUsn) throw new LocalSourceGapException("FutureCursor", "Read cursor exceeds observed journal bounds.");
        return page;
    }
    internal static void ValidateBounds(LocalJournalBounds bounds)
    {
        UsnRecordParser.ValidateJournalId(bounds.JournalIdHex);
        if (bounds.FirstUsn < 0 || bounds.LowestValidUsn < 0 || bounds.NextUsn < Math.Max(bounds.FirstUsn, bounds.LowestValidUsn)
            || bounds.MinMajor is not (2 or 3) || bounds.MaxMajor<bounds.MinMajor || bounds.MaxMajor>4 || bounds.RangeTrackingEnabled)
            throw new LocalSourceGapException("UnsupportedBounds", "Journal bounds or tracking mode cannot establish V2/V3 ingestion continuity.");
    }
    internal static void ValidateCursor(LocalJournalBounds bounds, LocalJournalCursor cursor)
    {
        ValidateBounds(bounds);
        if (cursor.JournalIdHex != bounds.JournalIdHex || cursor.NextReadUsn < Math.Max(bounds.FirstUsn, bounds.LowestValidUsn)
            || cursor.NextReadUsn > bounds.NextUsn)
            throw new LocalSourceGapException("JournalGap", "Journal identity or retained range no longer contains the cursor.");
    }
    private static LocalSourceGapException Failure(int error) => new("Win32-" + error,
        "Read-only journal operation is unavailable.", new Win32Exception(error));
    public ValueTask DisposeAsync() { _volume.Dispose(); return ValueTask.CompletedTask; }
    private static class Native
    {
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        internal static extern SafeFileHandle CreateFileW(string name, uint access, uint share, IntPtr security, uint creation, uint flags, IntPtr template);
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]
        [return:MarshalAs(UnmanagedType.Bool)]
        internal static extern bool GetVolumeInformationW(string root,StringBuilder? volumeName,int volumeNameLength,
            out uint serial,out uint maximumComponentLength,out uint flags,StringBuilder fileSystem,int fileSystemLength);
        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static extern bool DeviceIoControl(SafeFileHandle handle, uint control, byte[]? input, int inputSize,
            [Out] byte[] output, int outputSize, out int bytesReturned, IntPtr overlapped);
    }
}
