using System.Diagnostics;
using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

[Trait("Lane", "Live")]
public sealed class ReleaseAcceptanceDiskFullTests
{
    [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
    private static extern bool GetVolumeNameForVolumeMountPoint(string mount, StringBuilder name, int length);

    [Fact]
    public async Task X10_RealOwnedNtfsDiskFullPreservesAcceptedOriginalAndResumesItsCommittedPrefix()
    {
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException("Owned Windows NTFS pressure fixture required.");
        var ct=TestContext.Current.CancellationToken;
        var proofPath=Environment.GetEnvironmentVariable("NEPTUNE_PRESSURE_VOLUME_PROOF")??throw new InvalidOperationException("Explicit new pressure volume proof required.");
        using var proof=JsonDocument.Parse(await File.ReadAllBytesAsync(proofPath,ct));
        var owner=proof.RootElement;
        Assert.Equal("ra20261020-rc01-b-pressure-volume/v1",owner.GetProperty("schema").GetString());
        Assert.Equal("OWNED_NTFS_READY",owner.GetProperty("stage").GetString());
        var drive=owner.GetProperty("drive").GetString()!;
        Assert.DoesNotContain(drive,new[]{"C","E","R","S"});
        var root=drive+":\\";
        var volumeId=owner.GetProperty("volumeId").GetString()!;
        ValidateVolume();
        Assert.InRange(new DriveInfo(root).TotalSize,64L*1024*1024,512L*1024*1024);
        var directory=Path.Combine(root,"ra20261020-rc01-b-x10-"+Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var database=Path.Combine(directory,"state.db");
        var spool=Path.Combine(directory,"original.zip");
        var filler=Path.Combine(directory,"owned-pressure.fill");
        var evidenceRoot=Environment.GetEnvironmentVariable("NEPTUNE_FRESH_CYCLE_EVIDENCE_ROOT")??throw new InvalidOperationException("Owned evidence root required.");
        Directory.CreateDirectory(evidenceRoot);
        var bytes=new byte[4*1024*1024+13];
        RandomNumberGenerator.Fill(bytes);
        await File.WriteAllBytesAsync(spool,bytes,ct);
        var digest=Convert.ToHexStringLower(SHA256.HashData(bytes));
        var state=new NeptuneStateStore(database);
        await state.InitializeAsync(ct);
        var now=DateTimeOffset.UtcNow;
        var original=new BackupRun("ownedx10original","fixture","ra20261020-rc01-b","spooled",spool,bytes.Length,digest,"owned-receiver-run",0,1,now,now,null);
        await state.AddRunAsync(original,ct);
        var receiver=new Receiver(bytes,digest);
        using var http=new HttpClient(receiver);
        var client=new SaturnBackupClient(http,128*1024);
        var coordinator=new BackupCoordinator(state,original.ClientInstanceId,directory);
        var key=coordinator.CreateIdempotencyKey(original);
        await Assert.ThrowsAsync<HttpRequestException>(()=>client.UploadAsync(new Uri("https://owned.invalid/backups/"),"fixture","fixture-token",original,key,"x10",ct));
        Assert.Equal(128*1024,receiver.Accepted.Length);
        Assert.Equal(0,receiver.Completions);
        original=original with{State="retry-wait",Error="Accepted first chunk; reply lost"};
        await state.UpdateRunAsync(original,ct);
        SqliteConnection.ClearAllPools();
        Assert.False(File.Exists(database+"-wal"));
        var beforeDb=await Hash(database);
        await Evidence("before",new{directory,database,spool,original,key,digest,acceptedBytes=receiver.Accepted.Length,databaseSha256=beforeDb,volumeId,free=new DriveInfo(root).AvailableFreeSpace,systemFree=new DriveInfo("C:\\").AvailableFreeSpace,pid=Environment.ProcessId});
        var fullErrors=new List<object>();
        try
        {
            ValidateVolume();
            var buffer=new byte[1024*1024];RandomNumberGenerator.Fill(buffer);
            using(var fill=new FileStream(filler,FileMode.CreateNew,FileAccess.Write,FileShare.Read,1,FileOptions.WriteThrough))
            {
                foreach(var chunk in new[]{1024*1024,64*1024,4096,512})
                {
                    while(true)
                    {
                        ct.ThrowIfCancellationRequested();
                        Assert.True(fill.Length<=512L*1024*1024,"Filler may not exceed admitted virtual capacity.");
                        try{fill.Write(buffer,0,chunk);}
                        catch(IOException error)when((error.HResult&0xffff)is 112 or 39)
                        {fullErrors.Add(new{chunk,error.HResult,error.Message,free=new DriveInfo(root).AvailableFreeSpace});break;}
                    }
                }
            }
            var freeAtFailure=new DriveInfo(root).AvailableFreeSpace;
            Assert.NotEmpty(fullErrors);
            Assert.InRange(freeAtFailure,0,511);
            var failure=await Assert.ThrowsAsync<SqliteException>(()=>state.UpdateRunAsync(original with{State="uploading",Attempt=2,Error=null},ct));
            Assert.Contains(failure.SqliteErrorCode,new[]{10,13,14});
            await Evidence("full-error",new{fullErrors,freeAtFailure,sqliteCode=failure.SqliteErrorCode,sqliteExtendedCode=failure.SqliteExtendedErrorCode,failure.Message});
            Assert.Equal(beforeDb,await Hash(database));
            Assert.Equal(digest,await Hash(spool));
            Assert.Equal(128*1024,receiver.Accepted.Length);
            Assert.Equal(0,receiver.Completions);
            await Evidence("disk-full",new{fullErrors,freeAtFailure,sqliteCode=failure.SqliteErrorCode,sqliteExtendedCode=failure.SqliteExtendedErrorCode,failure.Message,databaseSha256=await Hash(database),payloadSha256=await Hash(spool),acceptedBytes=receiver.Accepted.Length,receiver.Completions});
            RemoveFiller();
            SqliteConnection.ClearAllPools();
            state=new NeptuneStateStore(database);
            await state.InitializeAsync(ct);
            var retained=await state.GetRunAsync(original.ClientInstanceId,original.RunId,ct);
            Assert.Equal(original,retained);
            Assert.Equal(key,new BackupCoordinator(state,original.ClientInstanceId,directory).CreateIdempotencyKey(retained!));
            await state.UpdateRunAsync(retained! with{State="uploading",Attempt=2,Error=null},ct);
            var receipt=await client.UploadAsync(new Uri("https://owned.invalid/backups/"),"fixture","fixture-token",retained!,key,"x10",ct);
            Assert.Equal(digest,receipt.Sha256);
            Assert.Equal(bytes.LongLength,receipt.SizeBytes);
            Assert.Equal(bytes,receiver.Accepted.ToArray());
            Assert.Equal(1,receiver.Completions);
            Assert.Equal(2,receiver.Creates);
            Assert.Single(receiver.Keys);
            await state.UpdateRunAsync(retained! with{State="complete",UploadedBytes=bytes.Length,Attempt=2,Error=null},ct);
            var completed=await state.GetRunAsync(original.ClientInstanceId,original.RunId,ct);
            Assert.Equal(original.RunId,completed!.RunId);
            Assert.Equal("complete",completed.State);
            Assert.Equal(bytes.LongLength,completed.UploadedBytes);
            await using(var read=new SqliteConnection("Data Source="+database+";Mode=ReadOnly;Pooling=False"))
            {await read.OpenAsync(ct);await using var query=read.CreateCommand();query.CommandText="PRAGMA integrity_check";Assert.Equal("ok",await query.ExecuteScalarAsync(ct));}
            var process=Process.GetCurrentProcess();process.Refresh();
            Assert.InRange(process.PeakWorkingSet64,1,512L*1024*1024);
            await Evidence("result",new{original,completed,key,digest,readbackSha256=Convert.ToHexStringLower(SHA256.HashData(receiver.Accepted.ToArray())),receiver.Creates,receiver.Completions,receiver.ResumeOffsets,uniqueKeys=receiver.Keys.Count,processPeakWorkingSet=process.PeakWorkingSet64,freeAfter=new DriveInfo(root).AvailableFreeSpace,directory,qualification="real NTFS disk-full and production store/uploader with in-memory test transport; store-level partial, not API E2E"});
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if(File.Exists(filler))RemoveFiller();
            await Evidence("retained",new{directory,database,spool,fillerRemoved=!File.Exists(filler),free=new DriveInfo(root).AvailableFreeSpace,volumeId});
        }

        void ValidateVolume()
        {
            var native=new StringBuilder(128);
            Assert.True(GetVolumeNameForVolumeMountPoint(root,native,native.Capacity));
            Assert.Equal(volumeId,native.ToString(),ignoreCase:true);
            Assert.Equal("NTFS",new DriveInfo(root).DriveFormat);
            using var marker=JsonDocument.Parse(File.ReadAllBytes(Path.Combine(root,".ra20261020-rc01-b-x10-owner.json")));
            Assert.Equal(owner.GetProperty("diskUniqueId").GetString(),marker.RootElement.GetProperty("diskUniqueId").GetString());
        }
        void RemoveFiller()
        {
            ValidateVolume();
            Assert.StartsWith(Path.GetFullPath(directory)+Path.DirectorySeparatorChar,Path.GetFullPath(filler),StringComparison.OrdinalIgnoreCase);
            Assert.False((File.GetAttributes(filler)&FileAttributes.ReparsePoint)!=0);
            File.Delete(filler);
        }
        async Task<string> Hash(string path){await using var stream=new FileStream(path,FileMode.Open,FileAccess.Read,FileShare.ReadWrite|FileShare.Delete);return Convert.ToHexStringLower(await SHA256.HashDataAsync(stream,ct));}
        Task Evidence(string label,object data)=>File.WriteAllTextAsync(Path.Combine(evidenceRoot,"X10-"+label+".json"),JsonSerializer.Serialize(data,new JsonSerializerOptions{WriteIndented=true}),ct);
    }

    private sealed class Receiver(byte[] expected,string sha):HttpMessageHandler
    {
        internal readonly MemoryStream Accepted=new();internal readonly HashSet<string> Keys=new(StringComparer.Ordinal);internal readonly List<long> ResumeOffsets=[];internal int Creates,Completions;private bool lose=true;
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken ct)
        {
            var path=request.RequestUri!.AbsolutePath;
            if(path.EndsWith("/capabilities",StringComparison.Ordinal))return Json(new{schema="saturn.backup-ingest.capabilities.v1",protocolVersion=1,resumable=true,checksum="sha256",maxChunkBytes=128*1024});
            if(request.Method==HttpMethod.Post&&path.EndsWith("/runs",StringComparison.Ordinal)){Creates++;Keys.Add(request.Headers.GetValues("Idempotency-Key").Single());return Json(new{id="owned-receiver-run"});}
            if(request.Method==HttpMethod.Head){ResumeOffsets.Add(Accepted.Length);var response=new HttpResponseMessage(HttpStatusCode.OK);response.Headers.Add("Upload-Offset",Accepted.Length.ToString(System.Globalization.CultureInfo.InvariantCulture));return response;}
            if(request.Method==HttpMethod.Patch)
            {
                Assert.Equal(Accepted.Length.ToString(System.Globalization.CultureInfo.InvariantCulture),request.Headers.GetValues("Upload-Offset").Single());
                var payload=await request.Content!.ReadAsByteArrayAsync(ct);Accepted.Write(payload);
                if(lose){lose=false;throw new HttpRequestException("owned test transport lost first committed chunk reply");}
                var response=new HttpResponseMessage(HttpStatusCode.OK);response.Headers.Add("Upload-Offset",Accepted.Length.ToString(System.Globalization.CultureInfo.InvariantCulture));return response;
            }
            Assert.EndsWith("/complete",path);Assert.Equal(expected,Accepted.ToArray());Completions++;
            return Json(new{receipt=new BackupReceipt("saturn.backup.receipt.v1","owned-receiver-run","fixture","fixture","fixture/original.zip",expected.LongLength,sha,DateTimeOffset.UtcNow)});
        }
        private static HttpResponseMessage Json(object value)=>new(HttpStatusCode.OK){Content=new StringContent(JsonSerializer.Serialize(value),Encoding.UTF8,"application/json")};
    }
}
