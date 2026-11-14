using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class Stage3ChatBTests
{
    private static async Task Sql(WindowsFreshCycleTests.Fixture f,string sql,params (string Name,object Value)[] parameters)
    {
        await using var db=new SqliteConnection("Data Source="+f.Database+";Pooling=False;Cache=Private");await db.OpenAsync(Ct);
        await using var q=db.CreateCommand();q.CommandText=sql;foreach(var p in parameters)q.Parameters.AddWithValue(p.Name,p.Value);await q.ExecuteNonQueryAsync(Ct);
    }
    private static byte[] UsnFixtureRecord(long usn,uint reason)
    {
        var bytes=new byte[80];BinaryPrimitives.WriteUInt32LittleEndian(bytes,80);BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(4),3);
        bytes.AsSpan(8,16).Fill(0xff);bytes.AsSpan(24,16).Fill(0xee);BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(40),usn);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(56),reason);BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(72),2);BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(74),76);bytes[76]=(byte)'x';return bytes;
    }
    [Theory]
    [InlineData("data256",256)][InlineData("relevant41",40)][InlineData("mixed",240)][InlineData("boundary257",256)]
    public async Task B01_BoundedFrontierCommitsResumeWithoutLosingFortyFirstRelevant(string shape,int firstConsumed)
    {
        RequireWindows();await using var f=await WindowsFreshCycleTests.Fixture.Create();await using var engine=await Open(f);Assert.Null((await engine.InitialSyncAsync(Ct)).Error);
        var session=Session(engine);var plan=await session.BeginParentReconciliationAsync(engine.ScopeId,Ct);
        var reasons=shape switch{"relevant41"=>Enumerable.Repeat(0x100u,41).ToArray(),"mixed"=>Enumerable.Repeat(0x100u,39).Concat(Enumerable.Repeat(1u,200)).Concat([0x100u,0x100u]).ToArray(),"boundary257"=>Enumerable.Repeat(1u,257).ToArray(),_=>Enumerable.Repeat(1u,256).ToArray()};
        var start=plan.StartUsn-10000;await Sql(f,"UPDATE local_parent_reconcile_plans SET record_after=$after WHERE plan_id=$id",("$after",start-1),("$id",plan.PlanId));
        for(var i=0;i<reasons.Length;i++){var raw=UsnFixtureRecord(start+i,reasons[i]);await Sql(f,"INSERT INTO local_coherence_unresolved VALUES($id,$usn,$raw,$hash,'fixture-retained',336)",("$id",plan.CoherenceId),("$usn",start+i),("$raw",raw),("$hash",Convert.ToHexStringLower(SHA256.HashData(raw))));}
        var owner=typeof(LocalSourceJournalSession).GetField("_owner",System.Reflection.BindingFlags.Instance|System.Reflection.BindingFlags.NonPublic)!.GetValue(session)!.ToString()!;
        var root=typeof(LocalSourceJournalSession).GetField("_root",System.Reflection.BindingFlags.Instance|System.Reflection.BindingFlags.NonPublic)!.GetValue(session)!.ToString()!;
        var offset=0;var commits=new List<object>();
        while(offset<reasons.Length)
        {
            // A new state instance reads the committed cursor on every step.
            var store=new NeptuneStateStore(f.Database);await store.InspectLocalJournalWalAsync(force:true,cancellationToken:Ct);var consumed=await store.SeedParentFrontier(root,owner,engine.ScopeId,plan.PlanId,new(MinimumFreeDiskBytes:0),Ct);
            Assert.InRange(consumed,1,256);if(offset==0)Assert.Equal(firstConsumed,consumed);
            Assert.InRange(reasons.Skip(offset).Take(consumed).Count(reason=>(reason&0x13300)!=0),0,40);offset+=consumed;
            var after=await f.Scalar("SELECT record_after FROM local_parent_reconcile_plans WHERE plan_id='"+plan.PlanId+"'");Assert.Equal(start+offset-1,after);commits.Add(new{consumed,after,metrics=store.GetParentReconciliationMetrics()});
        }
        Assert.Equal((long)reasons.Length,await f.Scalar("SELECT COUNT(*) FROM local_coherence_unresolved WHERE code='fixture-retained'"));
        await f.Evidence("B01-"+shape,new{scope=engine.ScopeId,plan,commits,retained=reasons.Length});
    }

    [Fact]
    public async Task B06_NoHiddenExcludesAndPagedReincludeSurvivesRestart()
    {
        RequireWindows();await using var f=await WindowsFreshCycleTests.Fixture.Create();
        foreach(var dir in new[]{".git","node_modules",".audit"}){Directory.CreateDirectory(Path.Combine(f.Source,dir));File.WriteAllText(Path.Combine(f.Source,dir,"file"),"visible");}
        for(var i=0;i<251;i++)File.WriteAllText(Path.Combine(f.Source,"file-"+i),"x");
        string scope;var policy=new MappingSyncPolicy("legacy",[".git","node_modules",".audit"]);long remoteCount;
        await using(var engine=await Open(f))
        {
            var first=await engine.InitialSyncAsync(Ct);Assert.Null(first.Error);Assert.Equal(254,first.PublishedFiles);scope=engine.ScopeId;remoteCount=f.Remote.Count;
            var s=Session(engine);var preview=await s.PreviewPolicyAsync(scope,policy,Ct);Assert.Equal(6,preview.Affected);Assert.Equal(0,preview.Collisions);
            await Assert.ThrowsAsync<InvalidOperationException>(()=>s.ApplyPolicyAsync(scope,policy,new string('0',64),Ct));await s.ApplyPolicyAsync(scope,policy,preview.Digest,Ct);
            Assert.Equal(250,await s.ContinuePolicyAsync(scope,Ct));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM client_policy_changes"));
        }
        f.Store=new(f.Database);await using var restored=await Open(f);Assert.Equal(scope,restored.ScopeId);var session=Session(restored);
        Assert.Equal(7,await session.ContinuePolicyAsync(scope,Ct));Assert.Equal(0,await session.ContinuePolicyAsync(scope,Ct));Assert.Equal(remoteCount,f.Remote.Count);
        var generationBefore=Convert.ToInt64(await f.Scalar("SELECT SUM(watcher_generation) FROM local_cycle_files"));
        var included=await session.PreviewPolicyAsync(scope,MappingSyncPolicy.Default,Ct);await session.ApplyPolicyAsync(scope,MappingSyncPolicy.Default,included.Digest,Ct);
        while(await session.ContinuePolicyAsync(scope,Ct)>0){}Assert.Equal(6L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE phase=0"));Assert.Equal(generationBefore+6,await f.Scalar("SELECT SUM(watcher_generation) FROM local_cycle_files"));
        Assert.Equal(remoteCount,f.Remote.Count);await f.Evidence("B06-policy",new{scope,excludedHash=policy.Hash,included,remoteCount});
    }

    [Fact]
    public async Task B06_PendingOriginalPreventsPolicyRebinding()
    {
        RequireWindows();await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"file"),"original");f.Remote.LosePrepare=true;
        await using var engine=await Open(f);await engine.InitialSyncAsync(Ct);var before=await f.Scalar("SELECT input_json FROM local_cycle_work WHERE active=1");var s=Session(engine);
        var policy=new MappingSyncPolicy("legacy",["file"]);var preview=await s.PreviewPolicyAsync(engine.ScopeId,policy,Ct);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>s.ApplyPolicyAsync(engine.ScopeId,policy,preview.Digest,Ct));Assert.Equal(before,await f.Scalar("SELECT input_json FROM local_cycle_work WHERE active=1"));
    }

    [Fact]
    public async Task B12_WalBackupOwnershipLifecycleAndOriginalBytes()
    {
        RequireWindows();await using var f=await WindowsFreshCycleTests.Fixture.Create();var profileRoot=Path.Combine(f.Directory,"profile");Directory.CreateDirectory(profileRoot);
        var profile=new WindowsProfileContext("fixture",profileRoot,false);var db=Path.Combine(profileRoot,"neptune.db");var store=new NeptuneStateStore(db);await store.InitializeAsync(Ct);
        await store.SaveSyncUploadCheckpointAsync(new("fixture","https://fixture.invalid/file","original-key","original-hash","original-upload"),Ct);
        await File.WriteAllTextAsync(Path.Combine(profileRoot,"enrollment.fixture"),"encrypted-credential-canary",Ct);
        await using var held=new SqliteConnection("Data Source="+db+";Pooling=False");await held.OpenAsync(Ct);
        await using(var q=held.CreateCommand()){q.CommandText="PRAGMA journal_mode=WAL;CREATE TABLE fixture_wal(value TEXT);INSERT INTO fixture_wal VALUES('committed WAL')";await q.ExecuteNonQueryAsync(Ct);}
        var before=await WindowsProfileCommands.InspectAsync(profile,Ct);Assert.Single(before);
        using(var owner=new FileStream(db+".local-source-owner.lock",FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None))
            await Assert.ThrowsAsync<IOException>(()=>WindowsProfileCommands.CheckpointAsync(profile,Ct));
        var output=Path.Combine(f.Directory,"control.json");await WindowsProfileCommands.ExecuteAsync(profile,["--profile-command","pause","--output",output]);Assert.False(new WindowsPreferencesStore(profileRoot).Read().SynchronizationEnabled);
        var backup=Path.Combine(f.Directory,"backup");await WindowsProfileCommands.BackupAsync(profile,backup,Ct);
        await using(var copy=new SqliteConnection("Data Source="+Path.Combine(backup,"neptune.db")+";Mode=ReadOnly;Pooling=False"))
        {await copy.OpenAsync(Ct);await using var q=copy.CreateCommand();q.CommandText="SELECT value FROM fixture_wal";Assert.Equal("committed WAL",await q.ExecuteScalarAsync(Ct));q.CommandText="SELECT upload_id FROM sync_upload_checkpoints WHERE scope='fixture'";Assert.Equal("original-upload",await q.ExecuteScalarAsync(Ct));}
        Assert.Equal(await File.ReadAllBytesAsync(Path.Combine(profileRoot,"enrollment.fixture"),Ct),await File.ReadAllBytesAsync(Path.Combine(backup,"enrollment.fixture"),Ct));
        await WindowsProfileCommands.CheckpointAsync(profile,Ct);var plan=Path.Combine(f.Directory,"plan.json");await WindowsProfileCommands.ExecuteAsync(profile,["--profile-command","migrate-plan","--output",plan]);
        Assert.Equal(before[0].EvidenceSha256,(await WindowsProfileCommands.InspectAsync(profile,Ct))[0].EvidenceSha256);
        await WindowsProfileCommands.ExecuteAsync(profile,["--profile-command","adopt","--plan",plan,"--destination",Path.Combine(f.Directory,"adoption-backup"),"--output",output]);
        Assert.Equal("original-upload",Assert.Single(await store.ListSyncUploadCheckpointsAsync("fixture",Ct)).UploadId);
    }
}
