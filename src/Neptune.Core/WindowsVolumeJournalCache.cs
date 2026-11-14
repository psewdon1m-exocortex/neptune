using System.IO.MemoryMappedFiles;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;

namespace Neptune.Core;

// User/session-wide read coordination, also shared by separate profile
// processes. It contains only a disposable page cache, never durable cursors.
internal sealed class WindowsVolumeJournalCache : IDisposable
{
    private const long Magic=0x4e50544a50414731;
    private const int Slots=8,Header=96,Page=UsnRecordParser.MaximumBufferBytes;
    private readonly Mutex _mutex;
    private readonly MemoryMappedFile _memory;
    private readonly MemoryMappedViewAccessor _view;
    internal WindowsVolumeJournalCache(string volume,LocalJournalReadMode mode)
    {
        if(!OperatingSystem.IsWindows())throw new PlatformNotSupportedException();
        using var identity=WindowsIdentity.GetCurrent();
        var key=Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(identity.User!.Value+"\n"+volume+"\n"+(int)mode)));
        var name="Local\\Neptune.JournalPages.v1."+key;
        _mutex=new(false,name+".owner");
        MemoryMappedFile? memory=null;
        try{memory=MemoryMappedFile.CreateOrOpen(name,16L+Slots*(Header+Page),MemoryMappedFileAccess.ReadWrite);_view=memory.CreateViewAccessor();_memory=memory;}
        catch{memory?.Dispose();_mutex.Dispose();throw;}
    }
    internal LocalJournalPage Read(WindowsUsnJournalReader reader,LocalJournalCursor cursor,int maximum,CancellationToken ct)
    {
        var acquired=false;var abandoned=false;
        try
        {
            // Mutex ownership stays on this worker thread through all native
            // calls. No await can transfer ReleaseMutex to another thread.
            try{var result=WaitHandle.WaitAny([_mutex,ct.WaitHandle]);if(result==1)throw new OperationCanceledException(ct);acquired=true;}
            catch(AbandonedMutexException){acquired=true;abandoned=true;}
            ct.ThrowIfCancellationRequested();
            if(abandoned||_view.ReadInt64(0)!=Magic)
            {for(var slot=0;slot<Slots;slot++)_view.Write(Offset(slot),0);_view.Write(8,0);_view.Write(0,Magic);}
            if(!cursor.Valid||maximum is <1024 or >Page)throw new InvalidDataException("Invalid coordinated journal request.");
            var bounds=reader.QueryAsync(ct).AsTask().GetAwaiter().GetResult();WindowsUsnJournalReader.ValidateCursor(bounds,cursor);
            for(var slot=0;slot<Slots;slot++)
            {
                var at=Offset(slot);if(_view.ReadInt32(at)!=1||_view.ReadInt32(at+4)!=maximum||_view.ReadInt64(at+8)!=cursor.NextReadUsn)continue;
                var journal=new byte[16];_view.ReadArray(at+24,journal,0,16);if(Encoding.ASCII.GetString(journal)!=cursor.JournalIdHex)continue;
                var length=_view.ReadInt32(at+72);if(length is <8 or >Page||length>maximum){_view.Write(at,0);continue;}
                var bytes=new byte[length];var expected=new byte[32];_view.ReadArray(at+Header,bytes,0,length);_view.ReadArray(at+40,expected,0,32);
                if(!CryptographicOperations.FixedTimeEquals(SHA256.HashData(bytes),expected)){_view.Write(at,0);continue;}
                var cached=UsnRecordParser.ParseMode(cursor.JournalIdHex,cursor.NextReadUsn,bytes,reader.ReadMode);
                var fresh=reader.QueryAsync(ct).AsTask().GetAwaiter().GetResult();WindowsUsnJournalReader.ValidateCursor(fresh,cursor);
                if(cached.NextReadUsn==_view.ReadInt64(at+16)&&cached.NextReadUsn<=fresh.NextUsn)return cached;
                _view.Write(at,0);
            }
            var page=reader.ReadAsync(cursor,maximum,ct).AsTask().GetAwaiter().GetResult();
            if(page.NextReadUsn>page.StartUsn)
            {
                var slot=(int)((uint)_view.ReadInt32(8)%Slots);var at=Offset(slot);_view.Write(at,0);
                _view.Write(at+4,maximum);_view.Write(at+8,page.StartUsn);_view.Write(at+16,page.NextReadUsn);
                _view.WriteArray(at+24,Encoding.ASCII.GetBytes(page.JournalIdHex),0,16);_view.WriteArray(at+40,SHA256.HashData(page.Payload),0,32);
                _view.Write(at+72,page.ByteCount);_view.WriteArray(at+Header,page.Payload,0,page.ByteCount);_view.Write(at,1);_view.Write(8,(slot+1)%Slots);
            }
            return page;
        }
        finally{if(acquired)_mutex.ReleaseMutex();}
    }
    private static long Offset(int slot)=>16L+slot*(Header+Page);
    public void Dispose(){_view.Dispose();_memory.Dispose();_mutex.Dispose();}
}
