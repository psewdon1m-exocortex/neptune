using System.Reflection;
using System.Runtime.Loader;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class RetainedParentRenameRecoveryTests
{
    [Fact]
    public async Task RetainedOrphanCloseSeedsFullProofWithoutChangingOriginals()
    {
        var source=Environment.GetEnvironmentVariable("NEPTUNE_PARENT_RENAME_REPRO");
        if(string.IsNullOrWhiteSpace(source)){Assert.Skip("Requires an offline retained parent-rename failure copy");return;}
        var ct=TestContext.Current.CancellationToken;
        var directory=Path.Combine(Path.GetTempPath(),"neptune-parent-rename-"+Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);var path=Path.Combine(directory,"cycle.db");
        try
        {
            File.Copy(source,path);
            await using var c=new SqliteConnection("Data Source="+path+";Pooling=False");await c.OpenAsync(ct);
            async Task<string> Originals(SqliteConnection? connection=null)
            {
                using var hash=IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
                await using var q=(connection??c).CreateCommand();q.CommandText="SELECT operation_id,input_json,result_json FROM local_cycle_work WHERE input_json IS NOT NULL ORDER BY operation_id";
                await using var r=await q.ExecuteReaderAsync(ct);
                while(await r.ReadAsync(ct))hash.AppendData(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new[]{r.GetString(0),r.GetString(1),r.IsDBNull(2)?null:r.GetString(2)})+"\n"));
                return Convert.ToHexStringLower(hash.GetHashAndReset());
            }
            var before=await Originals();string root,owner,scope;
            await using(var q=c.CreateCommand())
            {
                q.CommandText="SELECT p.root_key,r.owner_token,p.scope_id FROM local_parent_reconcile_plans p JOIN local_source_roots r ON r.root_key=p.root_key WHERE p.status<3 ORDER BY p.rowid DESC LIMIT 1";
                await using var r=await q.ExecuteReaderAsync(ct);Assert.True(await r.ReadAsync(ct));root=r.GetString(0);owner=r.GetString(1);scope=r.GetString(2);
            }
            var store=new NeptuneStateStore(path);await store.InspectLocalJournalWalAsync(true,ct);var admission=new LocalJournalAdmission(MaximumDatabaseBytes:16L<<30);
            var begin=typeof(NeptuneStateStore).GetMethod("BeginParentPlan",BindingFlags.Instance|BindingFlags.NonPublic)!;
            var plan=await (Task<LocalParentReconciliation>)begin.Invoke(store,[root,owner,scope,admission,ct])!;
            await using(var q=c.CreateCommand()){q.CommandText="SELECT version FROM local_parent_reconcile_format";Assert.Equal(4L,await q.ExecuteScalarAsync(ct));}
            // Exercise the actual frozen previous reader against the upgraded
            // copy, before it can initialize or mutate the optional module.
            var oldAssembly=Environment.GetEnvironmentVariable("NEPTUNE_PARENT_RENAME_OLD_READER");
            if(!string.IsNullOrWhiteSpace(oldAssembly))
            {
                var context=new AssemblyLoadContext("old-parent-reader-"+Guid.NewGuid().ToString("N"),isCollectible:true);
                try
                {
                    var type=context.LoadFromAssemblyPath(Path.GetFullPath(oldAssembly)).GetType("Neptune.Core.NeptuneStateStore")!;
                    var oldStore=Activator.CreateInstance(type,[path,null])!;
                    var preflight=type.GetMethod("PreflightParentReconcileAsync",BindingFlags.Instance|BindingFlags.NonPublic)!;
                    var error=await Assert.ThrowsAsync<InvalidDataException>(async()=>await (Task)preflight.Invoke(oldStore,[ct])!);
                    Assert.Contains("Unsupported optional namespace format",error.Message);
                    Assert.Equal(before,await Originals());
                }
                finally{context.Unload();}
            }
            var rollback=await store.CreateParentOrphanCloseRollbackCopyAsync(Path.Combine(directory,"rollback-v3.db"),ct);
            await using(var old=new SqliteConnection("Data Source="+rollback+";Mode=ReadOnly;Pooling=False"))
            {
                await old.OpenAsync(ct);await using var q=old.CreateCommand();q.CommandText="SELECT version FROM local_parent_reconcile_format";Assert.Equal(3L,await q.ExecuteScalarAsync(ct));
                q.CommandText="SELECT count(*) FROM sqlite_master WHERE name='local_parent_reconcile_orphan_closes'";Assert.Equal(0L,await q.ExecuteScalarAsync(ct));
                Assert.Equal(before,await Originals(old));
            }
            var seed=typeof(NeptuneStateStore).GetMethod("SeedParentFrontier",BindingFlags.Instance|BindingFlags.NonPublic)!;
            var batches=0;while(await (Task<int>)seed.Invoke(store,[root,owner,scope,plan.PlanId,admission,ct])!>0)Assert.True(++batches<2000);
            Assert.Equal(before,await Originals());
            await using(var q=c.CreateCommand())
            {
                q.CommandText="SELECT full_membership,hardlink_verified,status FROM local_parent_reconcile_plans WHERE plan_id=$id";q.Parameters.AddWithValue("$id",plan.PlanId);
                await using var r=await q.ExecuteReaderAsync(ct);Assert.True(await r.ReadAsync(ct));Assert.Equal(1,r.GetInt32(0));Assert.Equal(0,r.GetInt32(1));Assert.Equal(0,r.GetInt32(2));
            }
            await using(var q=c.CreateCommand()){q.CommandText="SELECT count(*) FROM local_parent_reconcile_orphan_closes WHERE plan_id=$id AND verified=0";q.Parameters.AddWithValue("$id",plan.PlanId);Assert.True((long)(await q.ExecuteScalarAsync(ct))!>0);}
        }
        finally
        {
            SqliteConnection.ClearAllPools();Directory.Delete(directory,true);
        }
    }
}
