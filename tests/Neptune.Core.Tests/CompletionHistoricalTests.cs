using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("Native fresh cycle")]
[Trait("Lane", "Live")]
public sealed class CompletionHistoricalTests
{
    [Fact]
    public async Task HistoricalNativePrefixAvailabilityIsQualified()
    {
        var ct=TestContext.Current.CancellationToken;var output=Environment.GetEnvironmentVariable("COMPLETION_EVIDENCE")!;
        await using var db=new SqliteConnection(new SqliteConnectionStringBuilder{DataSource=Path.Combine(output,"historical","before.db"),Mode=SqliteOpenMode.ReadOnly,Pooling=false}.ToString());await db.OpenAsync(ct);
        await using var q=db.CreateCommand();q.CommandText="SELECT binding_json,journal_id,cursor,owner_epoch FROM local_source_roots LIMIT 1";
        await using var row=await q.ExecuteReaderAsync(ct);Assert.True(await row.ReadAsync(ct));var binding=JsonSerializer.Deserialize<LocalSourceBinding>(row.GetString(0),new JsonSerializerOptions(JsonSerializerDefaults.Web))!;
        var cursor=new LocalJournalCursor(row.GetString(1),row.GetInt64(2),row.GetInt64(3),true);
        await using var native=new WindowsUsnJournalReader(binding.VolumeGuid,LocalJournalReadMode.IdentityOnly);var bounds=await native.QueryAsync(ct);
        Exception? blocked=Record.Exception(()=>WindowsUsnJournalReader.ValidateCursor(bounds,cursor));
        await File.WriteAllTextAsync(Path.Combine(output,"historical","native-prefix.json"),JsonSerializer.Serialize(new{cursor,bounds,blocker=blocked?.ToString()}),ct);
        Assert.Null(blocked);
    }
    [Fact]
    public async Task RetainedHistoricalPlanContinuesWithPhaseTrace()
    {
        var ct=TestContext.Current.CancellationToken;
        var output=Environment.GetEnvironmentVariable("COMPLETION_EVIDENCE")??throw new InvalidOperationException("Evidence required");
        var copy=Path.Combine(output,"historical","continuation-"+DateTime.UtcNow.ToString("HHmmss")+".db");
        File.Copy(Path.Combine(output,"historical","after.db"),copy);
        await using var db=new SqliteConnection("Data Source="+copy+";Pooling=False");await db.OpenAsync(ct);
        string root,owner,scope,coherence,journal,plan;long cursor;
        LocalSourceBinding binding;
        await using(var q=db.CreateCommand())
        {
            q.CommandText="SELECT r.root_key,r.owner_token,p.scope_id,p.coherence_id,r.journal_id,r.cursor,r.binding_json,p.plan_id FROM local_source_roots r JOIN local_parent_reconcile_plans p ON p.root_key=r.root_key WHERE p.status<3 LIMIT 1";
            await using var row=await q.ExecuteReaderAsync(ct);Assert.True(await row.ReadAsync(ct));root=row.GetString(0);owner=row.GetString(1);scope=row.GetString(2);coherence=row.GetString(3);journal=row.GetString(4);cursor=row.GetInt64(5);binding=JsonSerializer.Deserialize<LocalSourceBinding>(row.GetString(6),new JsonSerializerOptions(JsonSerializerDefaults.Web))!;plan=row.GetString(7);
        }
        var store=new NeptuneStateStore(copy);await store.InspectLocalJournalWalAsync(true,ct);
        await using var source=await WindowsLocalNamespaceSource.OpenAsync(binding.SourceRoot,ct);
        Assert.Equal(binding.RootFileId,source.RootProof.Handle.FileId128);
        var reader=new FrozenJournal(binding,journal,cursor);
        await using var coordinator=(WindowsLocalCoherenceCoordinator)typeof(WindowsLocalCoherenceCoordinator).GetConstructors(BindingFlags.Instance|BindingFlags.NonPublic).Single().Invoke([source,reader,null]);
        var ownerFile=new FileStream(copy+".local-source-owner.lock",FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None);
        var factory=typeof(NeptuneStateStore).GetMethod("CreateOwnedJournalSessionAsync",BindingFlags.Instance|BindingFlags.NonPublic)!;
        await using var session=await (Task<LocalSourceJournalSession>)factory.Invoke(store,[ownerFile,root,owner,new LocalJournalAdmission(MinimumFreeDiskBytes:0),ct])!;
        var timer=Stopwatch.StartNew();Exception? error=null;LocalCoherenceState? result=null;
        try{result=await coordinator.ReconcileParentsAsync(session,scope,coherence,ct);}
        catch(Exception e){error=e;}
        var diagnostics=coordinator.Diagnostics.Snapshot();
        await File.WriteAllTextAsync(copy+".result.json",JsonSerializer.Serialize(new{copy,plan,elapsedMs=timer.ElapsedMilliseconds,result,error=error?.ToString(),diagnostics},new JsonSerializerOptions{WriteIndented=true}),ct);
        Assert.Null(error);
        await using(var q=db.CreateCommand()){q.CommandText="SELECT status FROM local_parent_reconcile_plans WHERE plan_id=$id";q.Parameters.AddWithValue("$id",plan);Assert.Equal(3L,await q.ExecuteScalarAsync(ct));}
    }
    private sealed class FrozenJournal(LocalSourceBinding binding,string journal,long cursor):ILocalJournalReader
    {
        public LocalJournalReadMode ReadMode=>LocalJournalReadMode.IdentityOnly;
        public LocalJournalVolumeIdentity Volume=>new(binding.VolumeGuid,binding.VolumeSerial,binding.FileSystem);
        public ValueTask<LocalJournalBounds> QueryAsync(CancellationToken ct=default)=>ValueTask.FromResult(new LocalJournalBounds(journal,0,0,cursor,2,3,false));
        public ValueTask<LocalJournalPage> ReadAsync(LocalJournalCursor start,int maximumBufferBytes=262144,CancellationToken ct=default)=>throw new InvalidOperationException("Frozen retained cut must not request newer host journal bytes");
        public ValueTask DisposeAsync()=>ValueTask.CompletedTask;
    }
}
