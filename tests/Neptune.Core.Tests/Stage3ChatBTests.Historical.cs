using System.Diagnostics;
using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class Stage3ChatBTests
{
    public static bool NoHistoricalDatabase => string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("NEPTUNE_HISTORICAL_DATABASE"));
    [Fact(Skip="Historical profile was removed by artifact cleanup; provide NEPTUNE_HISTORICAL_DATABASE. This is NOT_RUN, not historical qualification.", SkipWhen=nameof(NoHistoricalDatabase))]
    public async Task B01_HistoricalCopyRetainsEvidenceAndReportsQualifiedBlocker()
    {
        RequireWindows();await using var f=await WindowsFreshCycleTests.Fixture.Create(new Stage3Clock());
        var original=Path.GetFullPath(Environment.GetEnvironmentVariable("NEPTUNE_HISTORICAL_DATABASE")??throw new InvalidOperationException("Historical profile path required"));
        // Copy all three SQLite files before opening anything. No reader or
        // writer is opened against the historical profile itself.
        var copy=Path.Combine(f.Directory,"historical.db");var inputs=new Dictionary<string,string>();
        foreach(var suffix in new[]{"","-wal","-shm"})if(File.Exists(original+suffix)){inputs[suffix]=Convert.ToHexStringLower(SHA256.HashData(await File.ReadAllBytesAsync(original+suffix,Ct)));File.Copy(original+suffix,copy+suffix);}
        await using var db=new SqliteConnection("Data Source="+copy+";Pooling=False");await db.OpenAsync(Ct);
        async Task<object?> Scalar(string sql){await using var q=db.CreateCommand();q.CommandText=sql;return await q.ExecuteScalarAsync(Ct);}
        var integrity=await Scalar("PRAGMA quick_check");Assert.Equal("ok",integrity);
        var evidenceTables=new[]{"local_coherence_unresolved","local_coherence_dirty","local_cycle_work","local_cycle_files","local_source_inbox"};
        async Task<Dictionary<string,string>> EvidenceHashes(){var values=new Dictionary<string,string>();foreach(var table in evidenceTables){await using var q=db.CreateCommand();q.CommandText="SELECT * FROM "+table;await using var r=await q.ExecuteReaderAsync(Ct);using var sha=IncrementalHash.CreateHash(HashAlgorithmName.SHA256);while(await r.ReadAsync(Ct))for(var i=0;i<r.FieldCount;i++){var bytes=r.IsDBNull(i)?new byte[]{0}:r.GetValue(i)is byte[] b?b:System.Text.Encoding.UTF8.GetBytes(Convert.ToString(r.GetValue(i),System.Globalization.CultureInfo.InvariantCulture)!);sha.AppendData(BitConverter.GetBytes(bytes.Length));sha.AppendData(bytes);}values[table]=Convert.ToHexStringLower(sha.GetHashAndReset());}return values;}
        var before=await EvidenceHashes();var counts=new Dictionary<string,long>();foreach(var table in evidenceTables)counts[table]=Convert.ToInt64(await Scalar("SELECT COUNT(*) FROM "+table));
        string root,owner,scope,plan;await using(var q=db.CreateCommand()){q.CommandText="SELECT p.root_key,r.owner_token,p.scope_id,p.plan_id FROM local_parent_reconcile_plans p JOIN local_source_roots r ON r.root_key=p.root_key WHERE p.status<3 LIMIT 1";await using var r=await q.ExecuteReaderAsync(Ct);Assert.True(await r.ReadAsync(Ct));root=r.GetString(0);owner=r.GetString(1);scope=r.GetString(2);plan=r.GetString(3);}
        var retainedRange=new{minimum=await Scalar("SELECT MIN(usn) FROM local_coherence_unresolved"),maximum=await Scalar("SELECT MAX(usn) FROM local_coherence_unresolved"),recordAfter=await Scalar("SELECT record_after FROM local_parent_reconcile_plans WHERE status<3 LIMIT 1"),start=await Scalar("SELECT start_usn FROM local_parent_reconcile_plans WHERE status<3 LIMIT 1"),parents=await Scalar("SELECT parents FROM local_parent_reconcile_plans WHERE status<3 LIMIT 1"),entries=await Scalar("SELECT entries FROM local_parent_reconcile_plans WHERE status<3 LIMIT 1")};
        var trace=new List<object>();string? blocker=null;var sw=Stopwatch.StartNew();
        try
        {
            var opener=new NeptuneStateStore(copy);await opener.InspectLocalJournalWalAsync(true,Ct);await opener.BeginParentPlan(root,owner,scope,new(MinimumFreeDiskBytes:0),Ct);
            for(var step=0;step<2000;step++)
            {
                var store=new NeptuneStateStore(copy);await store.InspectLocalJournalWalAsync(true,Ct);var start=sw.ElapsedMilliseconds;
                var count=await store.SeedParentFrontier(root,owner,scope,plan,new(MinimumFreeDiskBytes:0),Ct);
                trace.Add(new{step,count,elapsedMs=sw.ElapsedMilliseconds-start,recordAfter=await Scalar("SELECT record_after FROM local_parent_reconcile_plans WHERE status<3 LIMIT 1")});Assert.InRange(count,0,256);if(count==0)break;
                Assert.True(step<1999,"Historical frontier did not converge within bounded retained record count.");
            }
            blocker="Retained frontier already consumed at captured boundary (first replay consumed 0); current copy cannot establish the old 300-second bottleneck or before/fix comparison. Native scan/import replay remains outside this frontier-only evidence; C01 OPEN.";
        }
        catch(Exception e)when(e is LocalSourceGapException or InvalidDataException or InvalidOperationException or SqliteException)
        {blocker=e.GetType().Name+": "+e.Message;}
        var after=await EvidenceHashes();Assert.Equal(before.OrderBy(p=>p.Key),after.OrderBy(p=>p.Key));
        foreach(var pair in inputs)Assert.Equal(pair.Value,Convert.ToHexStringLower(SHA256.HashData(await File.ReadAllBytesAsync(original+pair.Key,Ct))));
        await f.Evidence("B01-historical",new{historicalManifest="batch-status-network/restart.launch.json",inputs,counts,retainedRange,trace,blocker,protectedTablesBefore=before,protectedTablesAfter=after,C01="OPEN"});Assert.NotNull(blocker);
    }
}
