using Neptune.Core;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class JournalGapReplacementTests
{
    [Theory]
    [InlineData("principal")]
    [InlineData("native-binding")]
    [InlineData("cycle")]
    [InlineData("receipt")]
    [InlineData("uninstalled")]
    public async Task InstalledPublicationChainRefusesChangedHistoricalAuthority(string changed)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"retained chain bytes");
        await Medium(async()=>
        {
            string first,middle;await using(var e=await Open(f)){first=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}
            var request=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),first);
            await using(var e=await Replace(f,request)){middle=e.ScopeId;await e.QualifyJournalReplacementAsync(Ct);Assert.Equal("NoWorkAtAppliedEventBoundary",(await e.FreshSyncAsync(Ct)).Decision);await Session(e).MarkGapAsync("JournalGap",Ct);}
            await using var current=await Replace(f,new(Guid.NewGuid().ToString("N"),middle));await current.QualifyJournalReplacementAsync(Ct);
            await using(var c=new SqliteConnection("Data Source="+f.Database+";Pooling=False"))
            {
                await c.OpenAsync(Ct);await using var q=c.CreateCommand();q.Parameters.AddWithValue("$first",first);q.Parameters.AddWithValue("$request",request.ReplacementId);q.Parameters.AddWithValue("$current",current.ScopeId);
                if(changed=="principal")
                {
                    var raw=JsonSerializer.Serialize(Config(f) with{CredentialScope="another-principal"},new JsonSerializerOptions(JsonSerializerDefaults.Web));
                    q.Parameters.AddWithValue("$raw",raw);q.Parameters.AddWithValue("$hash",Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(raw))));
                    q.CommandText="UPDATE local_cycle_scopes SET configuration=$raw,fingerprint=$hash WHERE scope_id=$first";
                }
                else q.CommandText=changed switch
                {
                    "native-binding"=>"UPDATE local_source_roots SET binding_json=json_set(binding_json,'$.rootIncarnation',123) WHERE root_key=(SELECT root_key FROM local_cycle_scopes WHERE scope_id=$first)",
                    "cycle"=>"UPDATE local_cycle_replacement_requests SET original_scope=$current WHERE replacement_id=$request",
                    "receipt"=>"UPDATE local_cycle_work SET sha256='"+new string('a',64)+"' WHERE scope_id=$first AND input_json IS NOT NULL",
                    "uninstalled"=>"UPDATE local_cycle_replacement_requests SET status=2 WHERE replacement_id=$request",
                    _=>throw new InvalidOperationException()
                };
                Assert.Equal(1,await q.ExecuteNonQueryAsync(Ct));
            }
            var before=await Snapshot(f,first);var prepares=f.Remote.Prepares;var outcome=await current.FreshSyncAsync(Ct);
            Assert.NotEqual("NoWorkAtAppliedEventBoundary",outcome.Decision);Assert.Equal(0,outcome.PublishedFiles);Assert.Equal(prepares,f.Remote.Prepares);
            Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE scope_id='"+current.ScopeId+"' AND baseline IS NOT NULL"));
            Assert.Equal(before,await Snapshot(f,first));return true;
        });
    }

    [Fact]
    public async Task RepeatedInstalledReplacementPreservesTransitivePublicationOwnership()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.Create();File.WriteAllText(Path.Combine(f.Source,"original"),"same immutable bytes");Directory.CreateDirectory(Path.Combine(f.Source,"empty"));
        await Medium(async()=>
        {
            string original;await using(var e=await Open(f)){original=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}
            var before=await Snapshot(f,original);var prepares=f.Remote.Prepares;
            var first=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),original);string middle;
            await using(var e=await Replace(f,first))
            {
                middle=e.ScopeId;await e.QualifyJournalReplacementAsync(Ct);var current=await e.FreshSyncAsync(Ct);Assert.Null(current.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",current.Decision);Assert.Equal(0,current.PublishedFiles);
                Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE scope_id='"+middle+"' AND input_json IS NOT NULL"));await Session(e).MarkGapAsync("JournalGap",Ct);
            }
            var intermediate=await Snapshot(f,middle);var next=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),middle);
            await using(var e=await Replace(f,next))
            {
                await e.QualifyJournalReplacementAsync(Ct);var current=await e.FreshSyncAsync(Ct);await f.Evidence("installed-replacement-chain",new{first,next,current,preparesBefore=prepares,preparesAfter=f.Remote.Prepares});
                Assert.Null(current.Error);Assert.Equal("NoWorkAtAppliedEventBoundary",current.Decision);Assert.Equal(0,current.Conflicts);Assert.Equal(0,current.PublishedFiles);Assert.Equal(prepares,f.Remote.Prepares);
                Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE scope_id='"+e.ScopeId+"' AND input_json IS NOT NULL"));
                var repeat=await e.FreshSyncAsync(Ct);Assert.Null(repeat.Error);Assert.Equal(0,repeat.CapturedFiles);
            }
            Assert.Equal(before,await Snapshot(f,original));Assert.Equal(intermediate,await Snapshot(f,middle));return true;
        });
    }
}
