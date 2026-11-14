using System.Buffers.Binary;
using System.ComponentModel;
using System.IO.MemoryMappedFiles;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text.Json;
using Microsoft.Win32.SafeHandles;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("LC1 resource qualification")]
public sealed class IdentityOnlyNativeEligibilityTests
{
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    [Fact]
    public async Task ActualReducedMediumReadsNamelessJournalAliasesAndSealedSameHandleBytes()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await IdentityOnlyJournalTests.Fixture.Create();
        var result=await ReducedMedium(async()=>
        {
            if(!OperatingSystem.IsWindows())throw new PlatformNotSupportedException();
            using var current=WindowsIdentity.GetCurrent();var rights=Rights(current.AccessToken);Assert.Equal(8192,rights.Integrity);Assert.Equal(0,rights.Elevation);Assert.False(rights.AdminEnabled);Assert.True(rights.AdminDenyOnly);
            await using var source=await WindowsLocalNamespaceSource.OpenAsync(f.Source,Ct);
            await using var reader=new WindowsUsnJournalReader(source.RootProof.VolumeGuid,LocalJournalReadMode.IdentityOnly);var begin=await reader.QueryAsync(Ct);
            var file=Path.Combine(f.Source,"owned-before");await File.WriteAllTextAsync(file,"own bounded bytes",Ct);File.Move(file,Path.Combine(f.Source,"owned-after"));Directory.CreateDirectory(Path.Combine(f.Source,"owned-empty"));
            var directory=new LocalNamespaceDirectory("audit",source.RootProof.Handle.FileId128,"",source.RootProof.Handle.CreationTime,"audit");
            await using var parent=await source.OpenDirectoryAsync(directory,Ct);var entries=new List<LocalNamespaceEntry>();
            for(var pages=0;;pages++){Assert.InRange(pages,0,8);var page=await parent.ReadPageAsync(Ct);entries.AddRange(page.Entries);if(page.End)break;}
            await using var aliases=new WindowsLocalDirectoryAliasSource(source.RootProof);await using var aliasReader=await aliases.OpenAsync(directory,parent.Proof,Ct);var aliasCount=0;
            for(var pages=0;;pages++){Assert.InRange(pages,0,8);var page=await aliasReader.ReadPageAsync(Ct);aliasCount+=page.Entries.Count;if(page.End)break;}
            await aliasReader.ValidateUnchangedAsync(Ct);await parent.ValidateUnchangedAsync(Ct);Assert.Equal(2,entries.Count);Assert.Equal(2,aliasCount);
            var own=entries.Select(e=>e.FileId128).ToHashSet(StringComparer.Ordinal);var boundary=await reader.QueryAsync(Ct);var cursor=new LocalJournalCursor(begin.JournalIdHex,begin.NextUsn,1,true);var ownRecords=new List<LocalUsnRecord>();var recordCount=0;var pageCount=0;
            while(cursor.NextReadUsn<boundary.NextUsn){Assert.InRange(pageCount++,0,31);LocalJournalPage page;try{page=await reader.ReadAsync(cursor,64*1024,Ct);}catch(LocalSourceGapException){await f.Evidence("native-mode-layout-primary",Diagnostic(source.RootProof.VolumeGuid,cursor,own));throw;}Assert.Equal(LocalJournalReadMode.IdentityOnly,page.ReadMode);Assert.True(page.NextReadUsn>cursor.NextReadUsn);recordCount+=page.Records.Count;ownRecords.AddRange(page.Records.Where(r=>own.Contains(r.FileIdHex)));cursor=cursor with{NextReadUsn=page.NextReadUsn};}
            Assert.True(ownRecords.Count>=8);Assert.All(ownRecords,r=>{Assert.Empty(r.Name);Assert.Equal(LocalJournalReadMode.IdentityOnly,r.ReadMode);});
            Assert.Contains(ownRecords,r=>(r.Reason&0x1000)!=0);Assert.Contains(ownRecords,r=>(r.Reason&0x2000)!=0);Assert.Contains(ownRecords,r=>(r.Reason&0x80000000)!=0);
            // Reopen the reader and allocate fresh buffers for the SAME
            // observed owned interval. Records after D may legitimately appear
            // on a busy volume; their bytes/names are never published here.
            var repeated=new List<object>();string? meaningful=null,rawOwned=null;
            for(var repeat=0;repeat<5;repeat++)
            {
                await using var fresh=new WindowsUsnJournalReader(source.RootProof.VolumeGuid,LocalJournalReadMode.IdentityOnly);var rereadCursor=new LocalJournalCursor(begin.JournalIdHex,begin.NextUsn,1,true);var ownRows=new List<LocalUsnRecord>();var ownedRaw=new List<byte[]>();var pageDigests=new List<string>();var nativeNext=new List<long>();
                for(var pages=0;rereadCursor.NextReadUsn<boundary.NextUsn;pages++)
                {
                    Assert.InRange(pages,0,31);var page=await fresh.ReadAsync(rereadCursor,64*1024,Ct);var bytes=(byte[])typeof(LocalJournalPage).GetProperty("Payload",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(page)!;
                    pageDigests.Add(Convert.ToHexStringLower(SHA256.HashData(bytes)));nativeNext.Add(page.NextReadUsn);var offset=8;
                    foreach(var record in page.Records){var length=BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset));if(record.Usn<boundary.NextUsn&&own.Contains(record.FileIdHex)){ownRows.Add(record);ownedRaw.Add(bytes.AsSpan(offset,length).ToArray());}offset+=length;}
                    rereadCursor=rereadCursor with{NextReadUsn=page.NextReadUsn};
                }
                var semantic=Convert.ToHexStringLower(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(ownRows)));using var digest=IncrementalHash.CreateHash(HashAlgorithmName.SHA256);foreach(var raw in ownedRaw)digest.AppendData(raw);var rawHash=Convert.ToHexStringLower(digest.GetHashAndReset());
                meaningful??=semantic;rawOwned??=rawHash;Assert.Equal(meaningful,semantic);Assert.Equal(rawOwned,rawHash);Assert.Equal(ownRecords.Count,ownRows.Count);
                repeated.Add(new{repeat,ownRecords=ownRows.Count,meaningfulSha256=semantic,rawOwnedSha256=rawHash,pageDigests,nativeNext});
            }
            await f.Evidence("native-medium-repeat-read",new{start=begin.NextUsn,cut=boundary.NextUsn,reads=repeated,ownRawPaddingStable=true,qualification="This host/synthetic medium only; future differing raw page fails exact duplicate rather than silently normalizing."});
            using var hint=Native.CreateFileW(f.Source,0x80,7,nint.Zero,3,0x02000000,nint.Zero);Check(!hint.IsInvalid);var leaf=entries.Single(e=>!e.IsDirectory);var id=Convert.FromHexString(leaf.FileId128);var descriptor=new IdDescriptor{Size=24,Type=2,Low=BinaryPrimitives.ReadUInt64LittleEndian(id),High=BinaryPrimitives.ReadUInt64LittleEndian(id.AsSpan(8))};
            using var byId=Native.OpenFileById(hint,ref descriptor,0x80,7,nint.Zero,0x02000000|0x00200000);var idError=byId.IsInvalid?Marshal.GetLastWin32Error():0;
            if(!byId.IsInvalid){var nativeId=new byte[24];Check(Native.GetFileInformationByHandleEx(byId,18,nativeId,24));Assert.Equal(leaf.FileId128,Convert.ToHexStringLower(nativeId.AsSpan(8,16)));}
            await using var capture=await WindowsNativeCycleCapture.OpenAsync(f.Source,Ct);Assert.Equal(source.RootProof,capture.RootProof);var work=Work(source.RootProof,leaf);var spool=Path.Combine(f.Directory,"owned-seal");var seal=await Capture(capture,work,spool);
            Assert.Equal(await File.ReadAllBytesAsync(Path.Combine(f.Source,leaf.Name),Ct),await File.ReadAllBytesAsync(spool,Ct));using var proof=JsonDocument.Parse(seal.ProofJson);Assert.True(long.Parse(proof.RootElement.GetProperty("lastFileUsn").GetString()!)>=0);
            return new{rights,source.RootProof.VolumeGuid,source.RootProof.Handle.FileId128,ownAliases=aliasCount,ownRecords=ownRecords.Count,recordCount,pageCount,namesPublished=0,byIdAvailable=!byId.IsInvalid,byIdError=idError,sealedBytes=seal.Size,seal.Sha256,lastFileUsn=proof.RootElement.GetProperty("lastFileUsn").GetString(),journalMutations=0,ordinaryLinkedUser="UNVERIFIED",scope="Synthetic reduced-medium native API eligibility; no engine/B/source-cut activation."};
        });
        await f.Evidence("native-medium-eligibility",result);
    }
    [Theory][InlineData(false)][InlineData(true)]
    public async Task ActualReducedMediumWriterOrMappedBytesStillPreventSeal(bool mapped)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await IdentityOnlyJournalTests.Fixture.Create();await File.WriteAllTextAsync(Path.Combine(f.Source,"owned-file"),"owned bytes",Ct);
        await ReducedMedium(async()=>
        {
            await using var source=await WindowsLocalNamespaceSource.OpenAsync(f.Source,Ct);await using var parent=await source.OpenDirectoryAsync(new("audit",source.RootProof.Handle.FileId128,"",source.RootProof.Handle.CreationTime,"audit"),Ct);var page=await parent.ReadPageAsync(Ct);var leaf=page.Entries.Single();await parent.DisposeAsync();
            await using var capture=await WindowsNativeCycleCapture.OpenAsync(f.Source,Ct);var work=Work(source.RootProof,leaf);var spool=Path.Combine(f.Directory,"refused-seal");
            if(mapped){using var memory=MemoryMappedFile.CreateFromFile(Path.Combine(f.Source,leaf.Name),FileMode.Open,null,0,MemoryMappedFileAccess.ReadWrite);using var view=memory.CreateViewAccessor();view.Write(0,(byte)42);view.Flush();await Assert.ThrowsAsync<BootstrapCaptureDeferredException>(()=>Capture(capture,work,spool));}
            else{await using var writer=new FileStream(Path.Combine(f.Source,leaf.Name),FileMode.Open,FileAccess.Write,FileShare.ReadWrite|FileShare.Delete);await Assert.ThrowsAsync<BootstrapCaptureDeferredException>(()=>Capture(capture,work,spool));}
            Assert.False(File.Exists(spool));return true;
        });
    }
    [Fact]
    public async Task ActualReducedMediumRootRenameOrSwapNeverKeepsOldPathProof()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await IdentityOnlyJournalTests.Fixture.Create();await File.WriteAllTextAsync(Path.Combine(f.Source,"owned-file"),"owned",Ct);
        var result=await ReducedMedium(async()=>
        {
            await using var source=await WindowsLocalNamespaceSource.OpenAsync(f.Source,Ct);await using var parent=await source.OpenDirectoryAsync(new("audit",source.RootProof.Handle.FileId128,"",source.RootProof.Handle.CreationTime,"audit"),Ct);await parent.ReadPageAsync(Ct);var moved=false;
            try{Directory.Move(f.Source,Path.Combine(f.Directory,"moved-owned"));moved=true;Directory.CreateDirectory(f.Source);}catch(IOException){ }
            if(moved)await Assert.ThrowsAnyAsync<IOException>(async()=>await parent.ValidateUnchangedAsync(Ct));else await parent.ValidateUnchangedAsync(Ct);
            return new{renameCompleted=moved,oldPathProofAcceptedAfterRename=false,noClaimThatShareDeleteAlwaysFencesRename=true};
        });await f.Evidence("native-medium-root-swap",result);
    }
    private static LocalCycleWork Work(LocalNamespaceRootProof root,LocalNamespaceEntry leaf)=>new(Guid.NewGuid().ToString("D"),Guid.NewGuid().ToString("N"),new(leaf.FileId128,root.Handle.FileId128,leaf.Name,leaf.Name,leaf.CreationTime,false,leaf.Size,new(1,1),null,null),LocalCycleWorkPhase.Queued,null,null,null,null,null,null,null,0,null,null);
    private static Task<LocalCycleSeal> Capture(WindowsNativeCycleCapture capture,LocalCycleWork work,string spool)
        =>(Task<LocalCycleSeal>)typeof(WindowsNativeCycleCapture).GetMethod("CaptureAsync",BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(capture,[work,spool,work.Entry.EnumeratedSize,Ct])!;
    private static object Diagnostic(string volume,LocalJournalCursor cursor,HashSet<string> own)
    {
        using var h=Native.CreateFileW("\\\\?\\Volume{"+volume+"}\\",0x80000000,7,nint.Zero,3,0x02000000,nint.Zero);Check(!h.IsInvalid);var input=new byte[48];BinaryPrimitives.WriteInt64LittleEndian(input,cursor.NextReadUsn);BinaryPrimitives.WriteUInt32LittleEndian(input.AsSpan(8),uint.MaxValue);BinaryPrimitives.WriteUInt64LittleEndian(input.AsSpan(32),ulong.Parse(cursor.JournalIdHex,System.Globalization.NumberStyles.HexNumber));BinaryPrimitives.WriteUInt16LittleEndian(input.AsSpan(40),2);BinaryPrimitives.WriteUInt16LittleEndian(input.AsSpan(42),3);var output=new byte[64*1024];Check(Native.DeviceIoControl(h,0x903ab,input,input.Length,output,output.Length,out var bytes,nint.Zero));var rows=new List<object>();var count=0;
        for(var offset=8;offset<bytes;){var r=output.AsSpan(offset);var length=BinaryPrimitives.ReadInt32LittleEndian(r);if(length<64||length>592||offset+length>bytes)throw new InvalidDataException();var major=BinaryPrimitives.ReadUInt16LittleEndian(r[4..]);if(major is not(2 or 3))throw new InvalidDataException();count++;var shift=major==3?16:0;var id=Convert.ToHexStringLower(r.Slice(8,major==3?16:8));if(own.Contains(id))rows.Add(new{major,length,id,nameLength=BinaryPrimitives.ReadUInt16LittleEndian(r[(56+shift)..]),nameOffset=BinaryPrimitives.ReadUInt16LittleEndian(r[(58+shift)..]),nonzeroTail=r.Slice(60+shift,length-60-shift).ContainsAnyExcept((byte)0),ownedNativeRawHex=Convert.ToHexStringLower(r[..length])});offset+=length;}
        return new{cursor.JournalIdHex,cursor.NextReadUsn,returnedBytes=bytes,physicalRecords=count,own=rows,outsideRawPublished=0};
    }
    [SupportedOSPlatform("windows")]
    private static async Task<T> ReducedMedium<T>(Func<Task<T>> action)
    {
        using var original=WindowsIdentity.GetCurrent();var before=Rights(original.AccessToken);Check(Native.CreateRestrictedToken(original.AccessToken,5,0,nint.Zero,0,nint.Zero,0,nint.Zero,out var token));using(token)
        {
            Check(Native.ConvertStringSidToSidW("S-1-16-8192",out var sid));try{var label=new Label{Sid=sid,Attributes=0x20};Check(Native.SetTokenInformation(token,25,ref label,Marshal.SizeOf<Label>()+Native.GetLengthSid(sid)));}finally{Native.LocalFree(sid);}
            var value=await WindowsIdentity.RunImpersonatedAsync(token,action);using var after=WindowsIdentity.GetCurrent();Assert.Equal(before,Rights(after.AccessToken));return value;
        }
    }
    [SupportedOSPlatform("windows")]
    private static TokenRights Rights(SafeAccessTokenHandle token)
    {
        int Value(int kind){var p=Info(token,kind);try{return Marshal.ReadInt32(p);}finally{Marshal.FreeHGlobal(p);}}
        var integrity=Info(token,25);int rid;try{var sid=Marshal.ReadIntPtr(integrity);var count=Marshal.ReadByte(Native.GetSidSubAuthorityCount(sid));rid=Marshal.ReadInt32(Native.GetSidSubAuthority(sid,(uint)(count-1)));}finally{Marshal.FreeHGlobal(integrity);}
        var groups=Info(token,2);bool enabled=false,denied=false;try{var count=Marshal.ReadInt32(groups);if(count<0||count>512)throw new InvalidDataException();for(var i=0;i<count;i++){var p=nint.Add(groups,8+i*16);if(new SecurityIdentifier(Marshal.ReadIntPtr(p)).Value=="S-1-5-32-544"){var bits=(uint)Marshal.ReadInt32(p,8);enabled=(bits&4)!=0;denied=(bits&16)!=0;}}}finally{Marshal.FreeHGlobal(groups);}return new(Value(20),Value(18),rid,enabled,denied);
    }
    private static nint Info(SafeAccessTokenHandle token,int kind){Native.GetTokenInformation(token,kind,nint.Zero,0,out var size);if(size<1||size>65536)throw new Win32Exception(Marshal.GetLastWin32Error());var p=Marshal.AllocHGlobal(size);if(!Native.GetTokenInformation(token,kind,p,size,out var actual)||actual>size){var e=Marshal.GetLastWin32Error();Marshal.FreeHGlobal(p);throw new Win32Exception(e);}return p;}
    private static void Check(bool value){if(!value)throw new Win32Exception(Marshal.GetLastWin32Error());}
    private sealed record TokenRights(int Elevation,int ElevationType,int Integrity,bool AdminEnabled,bool AdminDenyOnly);
    [StructLayout(LayoutKind.Sequential)]private struct Label{public nint Sid;public uint Attributes;}
    [StructLayout(LayoutKind.Explicit,Size=24)]private struct IdDescriptor{[FieldOffset(0)]public uint Size;[FieldOffset(4)]public int Type;[FieldOffset(8)]public ulong Low;[FieldOffset(16)]public ulong High;}
    private static class Native
    {
        [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]internal static extern SafeFileHandle CreateFileW(string path,uint access,uint share,nint security,uint creation,uint flags,nint template);
        [DllImport("kernel32.dll",SetLastError=true)]internal static extern SafeFileHandle OpenFileById(SafeFileHandle hint,ref IdDescriptor id,uint access,uint share,nint security,uint flags);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetFileInformationByHandleEx(SafeFileHandle handle,int type,byte[] output,uint size);
        [DllImport("kernel32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool DeviceIoControl(SafeFileHandle handle,uint control,byte[] input,int inputSize,byte[] output,int outputSize,out int returned,nint overlapped);
        [DllImport("advapi32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool CreateRestrictedToken(SafeAccessTokenHandle original,uint flags,uint sidCount,nint sids,uint privilegeCount,nint privileges,uint restrictedCount,nint restricted,out SafeAccessTokenHandle result);
        [DllImport("advapi32.dll",CharSet=CharSet.Unicode,SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool ConvertStringSidToSidW(string value,out nint sid);
        [DllImport("advapi32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool SetTokenInformation(SafeAccessTokenHandle token,int kind,ref Label label,int size);
        [DllImport("advapi32.dll",SetLastError=true)][return:MarshalAs(UnmanagedType.Bool)]internal static extern bool GetTokenInformation(SafeAccessTokenHandle token,int kind,nint output,int size,out int needed);
        [DllImport("advapi32.dll")]internal static extern int GetLengthSid(nint sid);
        [DllImport("advapi32.dll")]internal static extern nint GetSidSubAuthorityCount(nint sid);
        [DllImport("advapi32.dll")]internal static extern nint GetSidSubAuthority(nint sid,uint index);
        [DllImport("kernel32.dll")]internal static extern nint LocalFree(nint memory);
    }
}
