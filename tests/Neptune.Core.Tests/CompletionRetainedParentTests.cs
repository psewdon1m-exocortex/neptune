using System.Reflection;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("Native fresh cycle")]
[Trait("Lane", "Live")]
public sealed class CompletionRetainedParentTests
{
    [Fact]
    public async Task OriginalEmptyParentPlanContinuesAtRealNativeCut()
    {
        var ct=TestContext.Current.CancellationToken;
        var original=Environment.GetEnvironmentVariable("ENDURANCE_RETAINED_PARENT_DB")??throw new InvalidOperationException("Retained evidence required");
        var output=Environment.GetEnvironmentVariable("ENDURANCE_RETAINED_PARENT_EVIDENCE")??throw new InvalidOperationException("Evidence output required");
        Directory.CreateDirectory(output);var copy=Path.Combine(output,"continuation.db");Assert.False(File.Exists(copy));
        foreach(var suffix in new[]{"","-wal","-shm"})if(File.Exists(original+suffix))File.Copy(original+suffix,copy+suffix);
        await using var db=new SqliteConnection("Data Source="+copy+";Pooling=False");await db.OpenAsync(ct);
        string root,owner,scope,coherence,plan;LocalSourceBinding binding;long beforeCursor;
        await using(var q=db.CreateCommand())
        {
            q.CommandText="SELECT r.root_key,r.owner_token,p.scope_id,p.coherence_id,r.binding_json,p.plan_id,r.cursor FROM local_source_roots r JOIN local_parent_reconcile_plans p ON p.root_key=r.root_key WHERE p.status=0 AND p.parents=0 LIMIT 1";
            await using var row=await q.ExecuteReaderAsync(ct);Assert.True(await row.ReadAsync(ct));root=row.GetString(0);owner=row.GetString(1);scope=row.GetString(2);coherence=row.GetString(3);binding=JsonSerializer.Deserialize<LocalSourceBinding>(row.GetString(4),new JsonSerializerOptions(JsonSerializerDefaults.Web))!;plan=row.GetString(5);beforeCursor=row.GetInt64(6);
        }
        var store=new NeptuneStateStore(copy);await store.InspectLocalJournalWalAsync(true,ct);
        await using var source=await WindowsLocalNamespaceSource.OpenAsync(binding.SourceRoot,ct);Assert.Equal(binding.RootFileId,source.RootProof.Handle.FileId128);
        var reader=new WindowsUsnJournalReader(binding.VolumeGuid,LocalJournalReadMode.IdentityOnly);
        await using var coordinator=(WindowsLocalCoherenceCoordinator)typeof(WindowsLocalCoherenceCoordinator).GetConstructors(BindingFlags.Instance|BindingFlags.NonPublic).Single().Invoke([source,reader,null]);
        var ownerFile=new FileStream(copy+".local-source-owner.lock",FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None);
        var factory=typeof(NeptuneStateStore).GetMethod("CreateOwnedJournalSessionAsync",BindingFlags.Instance|BindingFlags.NonPublic)!;
        await using var session=await (Task<LocalSourceJournalSession>)factory.Invoke(store,[ownerFile,root,owner,new LocalJournalAdmission(MinimumFreeDiskBytes:0),ct])!;
        Exception? error=null;LocalCoherenceState? result=null;
        try{await coordinator.ObserveAndRetireChangesAsync(session,coherence,ct);result=await coordinator.ReconcileParentsAsync(session,scope,coherence,ct);}catch(Exception e){error=e;}
        var state=await session.ReadStateAsync(ct);
        await File.WriteAllTextAsync(Path.Combine(output,"result.json"),JsonSerializer.Serialize(new{original,copy,plan,beforeCursor,afterCursor=state.Cursor.NextReadUsn,result,error=error?.ToString(),diagnostics=coordinator.Diagnostics.Snapshot()},new JsonSerializerOptions{WriteIndented=true}),ct);
        Assert.Null(error);Assert.NotNull(result);Assert.Equal(LocalCoherenceStatus.Anchored,result.Status);Assert.True(state.Cursor.NextReadUsn>=beforeCursor);
        await using var final=db.CreateCommand();final.CommandText="SELECT status=3 AND full_membership=1 AND hardlink_verified=1 FROM local_parent_reconcile_plans WHERE plan_id=$id";final.Parameters.AddWithValue("$id",plan);Assert.Equal(1L,await final.ExecuteScalarAsync(ct));
    }
}
