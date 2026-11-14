using System.Reflection;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;
namespace Neptune.Core.Tests;
public sealed class ParentRenameStateTests
{
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    private static readonly MethodInfo Method=typeof(NeptuneStateStore).GetMethod("RetainParentRename",BindingFlags.Static|BindingFlags.NonPublic)!;
    private static readonly string Node=new('1',32),A=new('2',32),B=new('3',32);
    private static async Task<SqliteConnection> Database()
    {var c=new SqliteConnection("Data Source=:memory:");await c.OpenAsync(Ct);var schemas=(Dictionary<string,string>)typeof(NeptuneStateStore).GetField("ParentReconcileSchema",BindingFlags.Static|BindingFlags.NonPublic)!.GetValue(null)!;await using var q=c.CreateCommand();q.CommandText=schemas["local_parent_reconcile_rename"];await q.ExecuteNonQueryAsync(Ct);return c;}
    private static Task Record(SqliteConnection c,SqliteTransaction t,long usn,uint reason,string parent)=>
        (Task)Method.Invoke(null,[c,t,"plan",new LocalUsnRecord(usn,Node,parent,reason,0,0x20,"",3){ReadMode=LocalJournalReadMode.IdentityOnly},Ct])!;
    [Fact]
    public async Task OrderedBothParentsAndCumulativeCloseRetainOneCompletedTransition()
    {await using var c=await Database();using var t=c.BeginTransaction();await Record(c,t,10,0x1000,A);await Record(c,t,20,0x2000,B);await Record(c,t,30,0x80002000,B);await using var q=c.CreateCommand();q.Transaction=t;q.CommandText="SELECT state||':'||old_parent||':'||new_parent FROM local_parent_reconcile_rename";Assert.Equal("2:"+A+":"+B,await q.ExecuteScalarAsync(Ct));}
    [Theory][InlineData("new-first")][InlineData("both-bits")][InlineData("wrong-close-parent")][InlineData("second-old")][InlineData("missing-old-next")][InlineData("stale-usn")]
    public async Task MissingHalfOrWrongParentOrUnsupportedOrderRefuses(string kind)
    {
        await using var c=await Database();using var t=c.BeginTransaction();
        if(kind is not("new-first" or "both-bits")){await Record(c,t,10,0x1000,A);if(kind is not "second-old")await Record(c,t,20,0x2000,B);}
        await Assert.ThrowsAsync<LocalSourceGapException>(()=>kind switch{
            "new-first"=>Record(c,t,10,0x2000,B),"both-bits"=>Record(c,t,10,0x3000,A),"wrong-close-parent"=>Record(c,t,30,0x80002000,A),"second-old"=>Record(c,t,20,0x1000,A),"missing-old-next"=>Record(c,t,30,0x2000,B),_=>Record(c,t,15,0x80002000,B)});
    }
}
