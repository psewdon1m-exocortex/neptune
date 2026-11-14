using System.Buffers.Binary;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class ParentRenameCutTests
{
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    private static readonly MethodInfo Method=typeof(NeptuneStateStore).GetMethod("RejectRetainedCrossCutRename",BindingFlags.Static|BindingFlags.NonPublic)!;
    private static string Wire(int major,char value='1')=>new(value,major==3?32:16);
    private static Task<bool> Reject(SqliteConnection c,SqliteTransaction t,long end=1000,string id="plan")=>(Task<bool>)Method.Invoke(null,[c,t,id,end,Ct])!;
    private static async Task<SqliteConnection> Database(int major)
    {
        var c=new SqliteConnection("Data Source=:memory:");await c.OpenAsync(Ct);
        await using var q=c.CreateCommand();q.CommandText="""
            CREATE TABLE local_parent_reconcile_plans(plan_id TEXT PRIMARY KEY,root_key TEXT,journal TEXT,start_usn INTEGER,scan_start_usn INTEGER,status INTEGER,error TEXT);
            INSERT INTO local_parent_reconcile_plans VALUES('plan','root','0000000000000001',20,0,0,NULL);
            CREATE TABLE local_parent_reconcile_rename(plan_id TEXT,wire TEXT,major INTEGER,state INTEGER,old_parent TEXT,new_parent TEXT,last_usn INTEGER);
            CREATE TABLE local_source_inbox(root_key TEXT,journal_id TEXT,usn INTEGER,payload BLOB);
            CREATE INDEX inbox ON local_source_inbox(root_key,journal_id,usn);
            """;await q.ExecuteNonQueryAsync(Ct);
        q.CommandText="INSERT INTO local_parent_reconcile_rename VALUES('plan',$wire,$major,1,$parent,NULL,10)";q.Parameters.AddWithValue("$wire",Wire(major));q.Parameters.AddWithValue("$major",major);q.Parameters.AddWithValue("$parent",Wire(major,'2'));await q.ExecuteNonQueryAsync(Ct);return c;
    }
    private static async Task History(SqliteConnection c,SqliteTransaction t,int major,long usn,uint reason,char node='1',char parent='2',string root="root",string journal="0000000000000001")
    {
        var shift=major==3?16:0;var width=major==3?16:8;var raw=new byte[72+shift];"NUSN"u8.CopyTo(raw);raw[4]=1;raw[5]=1;var native=raw.AsSpan(8);
        BinaryPrimitives.WriteUInt32LittleEndian(native,(uint)(64+shift));BinaryPrimitives.WriteUInt16LittleEndian(native[4..],(ushort)major);Convert.FromHexString(Wire(major,node)).CopyTo(native[8..]);Convert.FromHexString(Wire(major,parent)).CopyTo(native[(8+width)..]);BinaryPrimitives.WriteInt64LittleEndian(native[(24+shift)..],usn);BinaryPrimitives.WriteUInt32LittleEndian(native[(40+shift)..],reason);BinaryPrimitives.WriteUInt32LittleEndian(native[(52+shift)..],0x20);BinaryPrimitives.WriteUInt16LittleEndian(native[(58+shift)..],(ushort)(60+shift));
        await using var q=c.CreateCommand();q.Transaction=t;q.CommandText="INSERT INTO local_source_inbox VALUES($root,$journal,$usn,$raw)";q.Parameters.AddWithValue("$root",root);q.Parameters.AddWithValue("$journal",journal);q.Parameters.AddWithValue("$usn",usn);q.Parameters.AddWithValue("$raw",raw);await q.ExecuteNonQueryAsync(Ct);
    }
    private static async Task<long> Value(SqliteConnection c,SqliteTransaction t,string sql)
    {await using var q=c.CreateCommand();q.Transaction=t;q.CommandText=sql;return (long)(await q.ExecuteScalarAsync(Ct))!;}
    [Theory][InlineData(2,20)][InlineData(3,20)][InlineData(3,21)]
    public async Task OrderedActualNewAtOrAfterCutRejectsOnlyCandidate(int major,long next)
    {
        await using var c=await Database(major);using var t=c.BeginTransaction();await History(c,t,major,10,0x1000);await History(c,t,major,15,0x80001000);await History(c,t,major,next,0x2000,parent:'3');
        Assert.True(await Reject(c,t));Assert.False(await Reject(c,t));Assert.Equal(4,await Value(c,t,"SELECT status FROM local_parent_reconcile_plans"));Assert.Equal(1,await Value(c,t,"SELECT state FROM local_parent_reconcile_rename"));Assert.Equal(3,await Value(c,t,"SELECT count(*) FROM local_source_inbox"));
    }
    [Theory][InlineData("missing")][InlineData("before-cut")][InlineData("both")][InlineData("second-old")][InlineData("wrong-root")][InlineData("wrong-journal")][InlineData("wrong-wire")][InlineData("old-absent")][InlineData("incarnation")][InlineData("not-ingested")][InlineData("installed")][InlineData("scan-cut")]
    public async Task IncompleteAmbiguousOrForeignEvidenceCannotReject(string kind)
    {
        await using var c=await Database(3);using var t=c.BeginTransaction();if(kind!="old-absent")await History(c,t,3,10,0x1000);
        if(kind=="second-old")await History(c,t,3,15,0x1000);if(kind=="incarnation")await History(c,t,3,15,0x200);
        if(kind!="missing")await History(c,t,3,kind=="before-cut"?19:20,kind=="both"?0x3000u:0x2000u,node:kind=="wrong-wire"?'4':'1',parent:'3',root:kind=="wrong-root"?"foreign":"root",journal:kind=="wrong-journal"?"0000000000000002":"0000000000000001");
        if(kind is "installed" or "scan-cut"){await using var q=c.CreateCommand();q.Transaction=t;q.CommandText=kind=="installed"?"UPDATE local_parent_reconcile_plans SET status=3":"UPDATE local_parent_reconcile_plans SET scan_start_usn=30";await q.ExecuteNonQueryAsync(Ct);}
        Assert.False(await Reject(c,t,kind=="not-ingested"?20:1000));Assert.Equal(kind=="installed"?3:0,await Value(c,t,"SELECT status FROM local_parent_reconcile_plans"));Assert.Equal(1,await Value(c,t,"SELECT state FROM local_parent_reconcile_rename"));
    }
    [Fact]
    public async Task ExcessiveHistoryRefusesWithoutChangingCandidate()
    {
        await using var c=await Database(3);using var t=c.BeginTransaction();await History(c,t,3,10,0x1000);for(var i=11;i<140;i++)await History(c,t,3,i,1);await History(c,t,3,140,0x2000,parent:'3');Assert.False(await Reject(c,t));Assert.Equal(0,await Value(c,t,"SELECT status FROM local_parent_reconcile_plans"));
    }
    [Fact]
    public async Task EveryUnpairedIdentityMustHaveItsOwnOrderedNew()
    {
        await using var c=await Database(3);using var t=c.BeginTransaction();await History(c,t,3,10,0x1000);await History(c,t,3,20,0x2000,parent:'3');
        await using(var q=c.CreateCommand()){q.Transaction=t;q.CommandText="INSERT INTO local_parent_reconcile_rename VALUES('plan',$wire,3,1,$parent,NULL,11)";q.Parameters.AddWithValue("$wire",Wire(3,'4'));q.Parameters.AddWithValue("$parent",Wire(3,'2'));await q.ExecuteNonQueryAsync(Ct);}
        await History(c,t,3,11,0x1000,node:'4');Assert.False(await Reject(c,t));Assert.Equal(0,await Value(c,t,"SELECT status FROM local_parent_reconcile_plans"));
        await History(c,t,3,21,0x2000,node:'4',parent:'3');Assert.True(await Reject(c,t));Assert.Equal(2,await Value(c,t,"SELECT count(*) FROM local_parent_reconcile_rename WHERE state=1"));
    }
    [Fact]
    public async Task ExcessiveUnpairedIdentitiesCannotTriggerUnboundedReproof()
    {
        await using var c=await Database(3);using var t=c.BeginTransaction();
        await using(var q=c.CreateCommand()){q.Transaction=t;q.CommandText="WITH RECURSIVE n(x) AS(SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<128) INSERT INTO local_parent_reconcile_rename SELECT 'plan',printf('%032x',x),3,1,$parent,NULL,10 FROM n";q.Parameters.AddWithValue("$parent",Wire(3,'2'));await q.ExecuteNonQueryAsync(Ct);}
        Assert.False(await Reject(c,t));Assert.Equal(0,await Value(c,t,"SELECT status FROM local_parent_reconcile_plans"));
    }
    [Fact]
    public async Task RetainedOfflineCopyRejectsCutWithoutChangingOriginalsOrHistory()
    {
        var source=Environment.GetEnvironmentVariable("NEPTUNE_RETAINED_RENAME_CUT_DB");if(string.IsNullOrWhiteSpace(source)){Assert.Skip("Requires a retained offline cross-cut database");return;}
        var dir=Path.Combine(Path.GetTempPath(),"neptune-rename-cut-"+Guid.NewGuid().ToString("N"));Directory.CreateDirectory(dir);var path=Path.Combine(dir,"cycle.db");
        try
        {
            await using var c=new SqliteConnection("Data Source="+path+";Pooling=False");await c.OpenAsync(Ct);
            await using(var original=new SqliteConnection("Data Source="+source+";Mode=ReadOnly;Pooling=False")){await original.OpenAsync(Ct);original.BackupDatabase(c);}
            async Task<string> Digest(SqliteTransaction t)
            {
                using var h=IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
                foreach(var sql in new[]{"SELECT operation_id,input_json,result_json FROM local_cycle_work ORDER BY operation_id","SELECT root_key,journal_id,usn,hex(payload) FROM local_source_inbox ORDER BY root_key,journal_id,usn","SELECT * FROM local_parent_reconcile_rename ORDER BY plan_id,major,wire"})
                {await using var q=c.CreateCommand();q.Transaction=t;q.CommandText=sql;await using var r=await q.ExecuteReaderAsync(Ct);while(await r.ReadAsync(Ct))for(var i=0;i<r.FieldCount;i++){var bytes=Encoding.UTF8.GetBytes(r.IsDBNull(i)?"<NULL>":Convert.ToString(r.GetValue(i),System.Globalization.CultureInfo.InvariantCulture)!);h.AppendData(BitConverter.GetBytes(bytes.Length));h.AppendData(bytes);}}
                return Convert.ToHexStringLower(h.GetHashAndReset());
            }
            using var tx=c.BeginTransaction();string id;long end;
            await using(var q=c.CreateCommand()){q.Transaction=tx;q.CommandText="SELECT p.plan_id,r.cursor FROM local_parent_reconcile_plans p JOIN local_source_roots r ON r.root_key=p.root_key WHERE p.status=0 AND EXISTS(SELECT 1 FROM local_parent_reconcile_rename n WHERE n.plan_id=p.plan_id AND n.state=1) LIMIT 1";await using var r=await q.ExecuteReaderAsync(Ct);Assert.True(await r.ReadAsync(Ct));id=r.GetString(0);end=r.GetInt64(1);}
            var before=await Digest(tx);Assert.True(await Reject(c,tx,end,id));Assert.Equal(before,await Digest(tx));tx.Commit();
        }
        finally{SqliteConnection.ClearAllPools();Directory.Delete(dir,true);}
    }
}
