using System.Reflection;
using System.Security.Cryptography;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("Native fresh cycle")]
public sealed class WindowsJournalRecoveryBoundaryTests
{
    [Theory]
    [InlineData("JournalGap",true)]
    [InlineData("SourceRebound",false)]
    [InlineData("LegacyWrite",false)]
    public async Task RecoveryResumeAcceptsOnlyTheExactRecordedNativeJournalGap(string cause,bool expected)
    {
        if(!OperatingSystem.IsWindows()){Assert.Skip("Windows native source required");return;}
        var ct=TestContext.Current.CancellationToken;
        await using var f=await WindowsFreshCycleTests.Fixture.Create();string scope;
        await using(var engine=await f.Open())
        {
            scope=engine.ScopeId;
            Assert.False(await WindowsProfileCommands.HasRecordedJournalGapAsync(f.Database,scope,ct));
            var session=(LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(engine)!;
            await session.MarkGapAsync(cause,ct);
        }
        Assert.Equal(expected,await WindowsProfileCommands.HasRecordedJournalGapAsync(f.Database,scope,ct));
        Assert.False(await WindowsProfileCommands.HasRecordedJournalGapAsync(f.Database,Guid.NewGuid().ToString("N"),ct));
    }
    [Fact]
    public async Task PublicJournalPlanIsReadOnlyAndPinsOriginalIdentityAndInventory()
    {
        if(!OperatingSystem.IsWindows()){Assert.Skip("Windows native source required");return;}
        var ct=TestContext.Current.CancellationToken;
        await using var f=await WindowsFreshCycleTests.Fixture.Create();
        File.WriteAllText(Path.Combine(f.Source,"original"),"retained bytes");string scope;
        await using(var engine=await f.Open())
        {
            scope=engine.ScopeId;Assert.Null((await engine.InitialSyncAsync(ct)).Error);
            var session=(LocalSourceJournalSession)typeof(WindowsFreshSyncEngine).GetField("_session",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(engine)!;
            await session.MarkGapAsync("JournalGap",ct);
        }
        var state=Path.Combine(f.Directory,"profile");var file=Path.Combine(state,"guarded-sync","namespace","mapping","cycle.db");Directory.CreateDirectory(Path.GetDirectoryName(file)!);
        await using(var from=new SqliteConnection(new SqliteConnectionStringBuilder{DataSource=f.Database,Pooling=false}.ToString()))
        await using(var to=new SqliteConnection(new SqliteConnectionStringBuilder{DataSource=file,Pooling=false}.ToString()))
        {await from.OpenAsync(ct);await to.OpenAsync(ct);from.BackupDatabase(to);}
        var profile=new WindowsProfileContext("audit",state,false);
        var preferences=new WindowsPreferencesStore(state);preferences.Write(WindowsPreferences.Default with{SynchronizationEnabled=false});
        var hash=Convert.ToHexStringLower(SHA256.HashData(await File.ReadAllBytesAsync(file,ct)));
        var plan=await WindowsProfileCommands.PreviewJournalRecoveryAsync(profile,Path.GetRelativePath(state,file),scope,ct);
        Assert.Equal(scope,plan.Target.ScopeId);Assert.Equal(1,plan.SubscriptionGeneration);
        Assert.Equal(hash,Convert.ToHexStringLower(SHA256.HashData(await File.ReadAllBytesAsync(file,ct))));
        WindowsProfileCommands.ValidateJournalPlan(profile,plan,plan.Sid);
        Assert.Throws<InvalidDataException>(()=>WindowsProfileCommands.ValidateJournalPlan(profile,plan with{Profile="another"},plan.Sid));
        Assert.Throws<InvalidDataException>(()=>WindowsProfileCommands.ValidateJournalPlan(profile,plan,"another-sid"));
        Assert.Throws<InvalidDataException>(()=>WindowsProfileCommands.ValidateJournalPlan(profile,plan with{StateDirectory=f.Source},plan.Sid));
        Assert.Throws<InvalidDataException>(()=>WindowsProfileCommands.ValidateJournalPlan(profile,plan with{Inventory=plan.Inventory with{Digest=new string('0',64)}},plan.Sid));
        Assert.Throws<InvalidDataException>(()=>WindowsProfileCommands.ValidateJournalPlan(profile,plan with{RequestId="changed"},plan.Sid));
        preferences.Write(preferences.Read() with{SynchronizationEnabled=true});
        await Assert.ThrowsAsync<InvalidOperationException>(()=>WindowsProfileCommands.PreviewJournalRecoveryAsync(profile,Path.GetRelativePath(state,file),scope,ct));
        Assert.Equal(hash,Convert.ToHexStringLower(SHA256.HashData(await File.ReadAllBytesAsync(file,ct))));
    }
}
