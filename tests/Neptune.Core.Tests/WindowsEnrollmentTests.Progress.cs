using System.Net;
using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsEnrollmentTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ProgressReportsEachMirrorAndNeverCompletesDeferredAtomicReplacement(bool disappear)
    {
        var ct=TestContext.Current.CancellationToken;var root=Path.Combine(Path.GetTempPath(),"neptune-progress-"+Guid.NewGuid().ToString("N"));
        var first=Path.Combine(root,"notes");var second=Path.Combine(root,"documents");Directory.CreateDirectory(first);Directory.CreateDirectory(second);
        var file=Path.Combine(first,"заметка.md");await File.WriteAllTextAsync(file,"first",ct);await File.WriteAllTextAsync(Path.Combine(second,"file.md"),"second",ct);
        var remote=new Dictionary<string,string>();var puts=0;var removed=false;
        using var handler=new Handler("Assigned");
        handler.DavOverride=request=>
        {
            var route=request.RequestUri!.AbsolutePath;
            if(disappear&&!removed&&request.Method.Method=="MKCOL"&&route.EndsWith("/notes/",StringComparison.Ordinal))
            {File.Delete(file);removed=true;}
            if(request.Method==HttpMethod.Put)
            {remote[route]=Convert.ToHexStringLower(SHA256.HashData(request.Content!.ReadAsByteArrayAsync(ct).GetAwaiter().GetResult()));puts++;}
            if(request.Method==HttpMethod.Put||request.Method==HttpMethod.Head)
            {
                var response=new HttpResponseMessage(remote.ContainsKey(route)?HttpStatusCode.OK:HttpStatusCode.NotFound);
                if(remote.TryGetValue(route,out var hash))response.Headers.ETag=new($"\"sha256-{hash}\"");return response;
            }
            return null;
        };
        using var http=new HttpClient(handler);var service=new WindowsSyncService(new("progress-test",root,false),http);
        var connection=new WindowsConnection(new Uri("https://kernel.test"),"fixture",new string('b',43),"Assigned");
        SyncMapping[] mappings=[new("first","client",first,true,DateTimeOffset.UtcNow),new("second","client",second,true,DateTimeOffset.UtcNow)];
        var events=new List<WindowsSyncProgress>();var sink=new SyncProgressCallback<WindowsSyncProgress>(events.Add);
        try
        {
            var state=new NeptuneStateStore(Path.Combine(root,"neptune.db"));await state.InitializeAsync(ct);
            foreach(var mapping in mappings)await state.AddMappingAsync(mapping,ct);
            var result=await service.SyncAsync("client",mappings,connection,cancellationToken:ct,detailedProgress:sink);
            Assert.Equal(disappear?1:2,result.UploadedFiles);Assert.Equal(disappear?1:0,result.DeferredFiles);
            Assert.Contains(events,p=>p.MappingName=="notes"&&p.MappingIndex==1&&p.MappingCount==2&&p.Work.Stage==SyncProgressStage.Scanning);
            var notes=events.Last(p=>p.MappingName=="notes");Assert.Equal(disappear?SyncProgressStage.Waiting:SyncProgressStage.Completed,notes.Work.Stage);
            Assert.Equal(disappear?0:1,notes.Work.CompletedItems);
            var docs=events.Last(p=>p.MappingName=="documents");Assert.Equal(SyncProgressStage.Completed,docs.Work.Stage);Assert.Equal(2,docs.MappingIndex);Assert.Equal(1,docs.Work.CompletedItems);
            if(!disappear)
            {
                events.Clear();result=await service.SyncAsync("client",mappings,connection,cancellationToken:ct,detailedProgress:sink);
                Assert.Equal(0,result.UploadedFiles);Assert.Equal(2,puts);
                Assert.All(events.Where(p=>p.Work.Stage==SyncProgressStage.Completed),p=>{Assert.Equal(1,p.Work.CompletedItems);Assert.Equal(0,p.Work.TransferredBytes);});
                Assert.Equal(2,Directory.GetFiles(root,"*.md",SearchOption.AllDirectories).Length);
            }
        }
        finally{await service.DisposeAsync();SqliteConnection.ClearAllPools();Directory.Delete(root,true);}
    }
}
