using System.Reflection;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class WindowsGuardedNativeEligibilityTests
{
    [Theory]
    [InlineData("installed")]
    [InlineData("installed-twice")]
    [InlineData("pending")]
    [InlineData("broken-link")]
    public async Task InstalledSupersessionHistoryAllowsOrdinaryNativeAdmission(string gate)
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }await using var f=await WindowsFreshCycleTests.Fixture.CreateOnHostVolume();File.WriteAllText(Path.Combine(f.Source,"file"),"original bytes");
        await Medium(async()=>
        {
            var options=new WindowsCycleNativeOptions(LocalJournalReadMode.IdentityOnly,true);var budget=new WindowsCycleBudget(MinimumFreeDiskBytes:10L<<30);var spool=Path.Combine(f.Directory,"spool");
            LocalSourceJournalSession Session(WindowsFreshSyncEngine e)=>(LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(e)!;
            string original;await using(var e=await WindowsFreshSyncEngine.OpenAsync(options,f.Store,f.Source,spool,Config(f),f.Remote,budget,cancellationToken:Ct)){original=e.ScopeId;Assert.Null((await e.InitialSyncAsync(Ct)).Error);await Session(e).MarkGapAsync("JournalGap",Ct);}
            var first=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),original);string failed;
            await using(var e=await WindowsFreshSyncEngine.OpenJournalReplacementAsync(options,first,f.Store,f.Source,spool,Config(f),f.Remote,budget,cancellationToken:Ct)){failed=e.ScopeId;await Session(e).MarkGapAsync("JournalGap",Ct);}
            // Owned fixture injects a demonstrably expired cursor; no claim of
            // physically truncating the OS journal is made by this regression.
            await f.Scalar("UPDATE local_source_roots SET cursor=0 WHERE root_key=(SELECT root_key FROM local_cycle_scopes WHERE scope_id='"+failed+"')");
            var next=new JournalGapReplacementRequest(Guid.NewGuid().ToString("N"),original,first.ReplacementId);
            await using(var e=await WindowsFreshSyncEngine.OpenJournalReplacementAsync(options,next,f.Store,f.Source,spool,Config(f),f.Remote,budget,cancellationToken:Ct))
            {
                if(gate=="installed-twice"){failed=e.ScopeId;await Session(e).MarkGapAsync("JournalGap",Ct);}
                else if(gate!="pending"){await e.QualifyJournalReplacementAsync(Ct);Assert.Equal("NoWorkAtAppliedEventBoundary",(await e.FreshSyncAsync(Ct)).Decision);}
            }
            if(gate=="installed-twice")
            {
                await f.Scalar("UPDATE local_source_roots SET cursor=0 WHERE root_key=(SELECT root_key FROM local_cycle_scopes WHERE scope_id='"+failed+"')");
                next=new(Guid.NewGuid().ToString("N"),original,next.ReplacementId);
                await using var e=await WindowsFreshSyncEngine.OpenJournalReplacementAsync(options,next,f.Store,f.Source,spool,Config(f),f.Remote,budget,cancellationToken:Ct);await e.QualifyJournalReplacementAsync(Ct);Assert.Equal("NoWorkAtAppliedEventBoundary",(await e.FreshSyncAsync(Ct)).Decision);
            }
            if(gate=="broken-link")await f.Scalar("UPDATE local_cycle_replacement_requests SET superseded_by='"+Guid.NewGuid().ToString("N")+"' WHERE replacement_id='"+first.ReplacementId+"'");
            var before=await Hashes(f.Database);
            if(gate.StartsWith("installed",StringComparison.Ordinal))
            {await using var eligibility=await WindowsGuardedSourceEligibility.OpenAsync(f.Source,f.Database,Config(f),Ct);Assert.Equal(gate=="installed-twice"?4:3,eligibility.SubscriptionGeneration);await eligibility.RequireCurrentAsync(Ct);}
            else{var error=await Assert.ThrowsAsync<LocalSourceGapException>(()=>WindowsGuardedSourceEligibility.OpenAsync(f.Source,f.Database,Config(f),Ct));Assert.Equal("JournalReplacementPending",error.Code);}
            Assert.Equal(before,await Hashes(f.Database));return true;
        });
    }
}
