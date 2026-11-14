using System.Buffers.Binary;
using System.Reflection;
using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
public sealed class ParentOrphanCloseTests
{
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    private static readonly LocalParentReconciliation Plan=new("plan","scope","coherence",LocalParentReconciliationStatus.Scanning,1000,0,1,0,null);
    private static MethodInfo Method(string name)=>typeof(NeptuneStateStore).GetMethod(name,BindingFlags.Static|BindingFlags.NonPublic)!;
    private static Task<bool> Defer(SqliteConnection c,SqliteTransaction t,LocalUsnRecord r)=>(Task<bool>)Method("DeferUnknownParentRenameClose").Invoke(null,[c,t,Plan,r,Ct])!;
    private static Task<int> Verify(SqliteConnection c,SqliteTransaction t)=>(Task<int>)Method("VerifyOrphanParentCloses").Invoke(null,[c,t,Plan,Ct])!;
    private static LocalUsnRecord Record(long usn=10,ushort major=3,uint reason=0x80002000)=>new(usn,new('1',major==3?32:16),new('2',major==3?32:16),reason,0,0x20,"",major){ReadMode=LocalJournalReadMode.IdentityOnly};
    private static async Task Execute(SqliteConnection c,SqliteTransaction? t,string sql)
    {await using var q=c.CreateCommand();q.Transaction=t;q.CommandText=sql;await q.ExecuteNonQueryAsync(Ct);}
    private static async Task<long> Count(SqliteConnection c,SqliteTransaction t,string sql)
    {await using var q=c.CreateCommand();q.Transaction=t;q.CommandText=sql;return (long)(await q.ExecuteScalarAsync(Ct))!;}
    private static async Task<SqliteConnection> Database()
    {
        var c=new SqliteConnection("Data Source=:memory:");await c.OpenAsync(Ct);
        await Execute(c,null,"CREATE TABLE local_cycle_files(scope_id TEXT,node_id TEXT,PRIMARY KEY(scope_id,node_id));CREATE TABLE local_cycle_scopes(scope_id TEXT,coherence_id TEXT);INSERT INTO local_cycle_scopes VALUES('scope','coherence');CREATE TABLE local_coherence_nodes(coherence_id TEXT,node_id TEXT,wire64 TEXT,created INTEGER,kind INTEGER);CREATE TABLE local_coherence_unresolved(coherence_id TEXT,usn INTEGER,payload BLOB,sha256 TEXT,code TEXT,PRIMARY KEY(coherence_id,usn))");
        var schema=(Dictionary<string,string>)typeof(NeptuneStateStore).GetProperty("ParentOrphanCloseSchema",BindingFlags.Static|BindingFlags.NonPublic)!.GetValue(null)!;
        foreach(var sql in schema.Values)await Execute(c,null,sql);
        await Execute(c,null,"INSERT INTO local_parent_reconcile_format VALUES(1,4,1024);INSERT INTO local_parent_reconcile_plans(plan_id,root_key,scope_id,coherence_id,owner_epoch,revision,gap,journal,start_usn,status,parents,full_membership,hardlink_verified) VALUES('plan','root','scope','coherence',1,0,0,'0123456789abcdef',1000,0,1,1,1);INSERT INTO local_parent_reconcile_parents(plan_id,node_id,path,created,claim,state,refilled) VALUES('plan','root','',1,'claim',2,1)");return c;
    }
    private static async Task History(SqliteConnection c,SqliteTransaction t,LocalUsnRecord r)
    {
        var shift=r.MajorVersion==3?16:0;var n=r.MajorVersion==3?16:8;var raw=new byte[72+shift];"NUSN"u8.CopyTo(raw);raw[4]=1;raw[5]=1;var native=raw.AsSpan(8);
        BinaryPrimitives.WriteUInt32LittleEndian(native,(uint)(64+shift));BinaryPrimitives.WriteUInt16LittleEndian(native[4..],r.MajorVersion);
        Convert.FromHexString(r.FileIdHex).CopyTo(native[8..]);Convert.FromHexString(r.ParentIdHex).CopyTo(native[(8+n)..]);BinaryPrimitives.WriteInt64LittleEndian(native[(24+shift)..],r.Usn);BinaryPrimitives.WriteUInt32LittleEndian(native[(40+shift)..],r.Reason);BinaryPrimitives.WriteUInt32LittleEndian(native[(52+shift)..],r.FileAttributes);BinaryPrimitives.WriteUInt16LittleEndian(native[(58+shift)..],(ushort)(60+shift));
        await using var q=c.CreateCommand();q.Transaction=t;q.CommandText="INSERT INTO local_coherence_unresolved VALUES('coherence',$usn,$raw,$hash,'PriorMembershipUncertainty')";q.Parameters.AddWithValue("$usn",r.Usn);q.Parameters.AddWithValue("$raw",raw);q.Parameters.AddWithValue("$hash",Convert.ToHexStringLower(SHA256.HashData(raw)));await q.ExecuteNonQueryAsync(Ct);
    }
    [Theory][InlineData(2)][InlineData(3)]
    public async Task UnknownLateCloseRequiresDurableAbsenceWithoutInventingOld(int major)
    {
        await using var c=await Database();var r=Record(major:(ushort)major);
        using(var t=c.BeginTransaction()){await History(c,t,r);Assert.True(await Defer(c,t,r));Assert.Equal(0,await Count(c,t,"SELECT count(*) FROM local_parent_reconcile_rename"));t.Commit();}
        using(var t=c.BeginTransaction()){Assert.Equal(1,await Verify(c,t));Assert.Equal(0,await Verify(c,t));Assert.Equal(1,await Count(c,t,"SELECT count(*) FROM local_coherence_unresolved WHERE code='PriorMembershipUncertainty'"));t.Commit();}
    }
    [Theory][InlineData("bare-new")][InlineData("known-node")][InlineData("known-parent")][InlineData("logical-baseline")][InlineData("missing-history")][InlineData("existing-pair")][InlineData("after-start")]
    public async Task UnsupportedOrManagedCloseKeepsStrictPairRequirement(string kind)
    {
        await using var c=await Database();using var t=c.BeginTransaction();var r=Record(usn:kind=="after-start"?1000:10,reason:kind=="bare-new"?0x2000u:0x80002000u);
        if(kind!="missing-history")await History(c,t,r);
        if(kind is "known-node" or "known-parent")await Execute(c,t,$"INSERT INTO local_coherence_nodes VALUES('coherence','{(kind=="known-node"?r.FileIdHex:r.ParentIdHex)}',NULL,1,0)");
        if(kind=="logical-baseline")await Execute(c,t,$"INSERT INTO local_cycle_files VALUES('scope','{r.FileIdHex}')");
        if(kind=="existing-pair")await Execute(c,t,$"INSERT INTO local_parent_reconcile_rename VALUES('plan','{r.FileIdHex}',3,1,'old',NULL,9)");
        Assert.False(await Defer(c,t,r));Assert.Equal(0,await Count(c,t,"SELECT count(*) FROM local_parent_reconcile_orphan_closes"));
    }
    [Theory][InlineData("partial-parent")][InlineData("unfilled")][InlineData("partial-membership")][InlineData("unverified-hardlink")][InlineData("inside-node")][InlineData("inside-parent")][InlineData("newly-known")][InlineData("lost-history")][InlineData("changed-history")]
    public async Task AbsenceVerificationRefusesIncompleteOrInsideEvidence(string kind)
    {
        await using var c=await Database();using var t=c.BeginTransaction();var r=Record();await History(c,t,r);Assert.True(await Defer(c,t,r));
        var sql=kind switch{
            "partial-parent"=>"UPDATE local_parent_reconcile_parents SET state=0",
            "unfilled"=>"UPDATE local_parent_reconcile_parents SET refilled=0",
            "partial-membership"=>"UPDATE local_parent_reconcile_plans SET full_membership=0",
            "unverified-hardlink"=>"UPDATE local_parent_reconcile_plans SET hardlink_verified=0",
            "inside-node" or "inside-parent"=>$"INSERT INTO local_parent_reconcile_entries(plan_id,parent_id,node_id,name,canonical,created,kind,size,path) VALUES('plan','root','{(kind=="inside-node"?r.FileIdHex:r.ParentIdHex)}','inside','inside',1,0,0,'inside')",
            "newly-known"=>$"INSERT INTO local_cycle_files VALUES('scope','{r.FileIdHex}')",
            "lost-history"=>"DELETE FROM local_coherence_unresolved",
            _=>"UPDATE local_coherence_unresolved SET payload=zeroblob(length(payload))"};
        await Execute(c,t,sql);
        if(kind is "lost-history" or "changed-history")await Assert.ThrowsAsync<InvalidDataException>(()=>Verify(c,t));
        else await Assert.ThrowsAsync<LocalSourceGapException>(()=>Verify(c,t));
        Assert.Equal(0,await Count(c,t,"SELECT count(*) FROM local_parent_reconcile_orphan_closes WHERE verified=1"));
    }
    [Fact]
    public async Task VerificationCommitsBoundedPagesAndResumes()
    {
        await using var c=await Database();using(var t=c.BeginTransaction()){for(var n=1;n<=257;n++){var r=Record(n);await History(c,t,r);Assert.True(await Defer(c,t,r));}t.Commit();}
        foreach(var expected in new[]{128,128,1,0}){using var t=c.BeginTransaction();Assert.Equal(expected,await Verify(c,t));t.Commit();}
    }
}
