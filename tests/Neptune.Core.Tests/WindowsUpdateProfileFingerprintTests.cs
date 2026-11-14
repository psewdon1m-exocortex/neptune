using System.Reflection;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class WindowsUpdateProfileFingerprintTests
{
    private static string Fingerprint(string root)
    {
        var method = typeof(UpdateApplyMode).Assembly.GetType("Neptune.Windows.WindowsUpdateTransaction", true)!
            .GetMethod("FingerprintTree", BindingFlags.NonPublic | BindingFlags.Static, null, [typeof(string), typeof(bool)], null)!;
        return (string)method.Invoke(null, [root, true])!;
    }

    [Fact]
    public async Task ReadOnlyHealthCreatingAnEmptyWalDoesNotInvalidateRollbackBaseline()
    {
        var root = Path.Combine(Path.GetTempPath(), "neptune-update-health-" + Guid.NewGuid().ToString("N"));
        var profile = Path.Combine(root, "test-profile");
        Directory.CreateDirectory(profile);
        var database = Path.Combine(profile, "neptune.db");
        try
        {
            await new NeptuneStateStore(database).InitializeAsync(TestContext.Current.CancellationToken);
            SqliteConnection.ClearAllPools();
            Assert.False(File.Exists(database + "-wal"));
            var before = Fingerprint(root);
            await new NeptuneStateStore(database).ValidateProfileCompatibilityAsync(TestContext.Current.CancellationToken);
            SqliteConnection.ClearAllPools();
            Assert.True(File.Exists(database + "-wal"));
            Assert.Equal(0L, new FileInfo(database + "-wal").Length);
            Assert.Equal(before, Fingerprint(root));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            Directory.Delete(root, true);
        }
    }

    [Fact]
    public void NonemptyWalAlwaysChangesTheRollbackBaseline()
    {
        using var fixture=new FingerprintFixture();
        var before=Fingerprint(fixture.Root);
        File.WriteAllBytes(fixture.Database+"-wal",[1]);
        Assert.NotEqual(before,Fingerprint(fixture.Root));
    }

    [Fact]
    public void OtherEmptyProfileFilesAreStillIncluded()
    {
        using var fixture=new FingerprintFixture();
        var before=Fingerprint(fixture.Root);
        File.WriteAllBytes(Path.Combine(Path.GetDirectoryName(fixture.Database)!,"receipt"),[]);
        Assert.NotEqual(before,Fingerprint(fixture.Root));
    }

    [Fact]
    public void WalNamedFilesOutsideQualifiedDatabasePathsAreStillIncluded()
    {
        using var fixture=new FingerprintFixture();
        var directory=Path.Combine(Path.GetDirectoryName(fixture.Database)!,"payload");Directory.CreateDirectory(directory);
        File.Copy(fixture.Database,Path.Combine(directory,"neptune.db"));
        var before=Fingerprint(fixture.Root);
        File.WriteAllBytes(Path.Combine(directory,"neptune.db-wal"),[]);
        Assert.NotEqual(before,Fingerprint(fixture.Root));
    }

    [Fact]
    public void EmptyWalForAnInvalidDatabaseIsStillIncluded()
    {
        using var fixture=new FingerprintFixture();
        File.WriteAllText(fixture.Database,"not a SQLite database");
        var before=Fingerprint(fixture.Root);
        File.WriteAllBytes(fixture.Database+"-wal",[]);
        Assert.NotEqual(before,Fingerprint(fixture.Root));
    }

    [Fact]
    public void QualifiedGuardedCycleEmptyWalMatchesItsAbsence()
    {
        using var fixture=new FingerprintFixture();
        var directory=Path.Combine(Path.GetDirectoryName(fixture.Database)!,"guarded-sync","namespace","mapping");
        Directory.CreateDirectory(directory);
        var cycle=Path.Combine(directory,"cycle.db");
        File.Copy(fixture.Database,cycle);
        var before=Fingerprint(fixture.Root);
        File.WriteAllBytes(cycle+"-wal",[]);
        Assert.Equal(before,Fingerprint(fixture.Root));
        File.WriteAllBytes(cycle+"-wal",[1]);
        Assert.NotEqual(before,Fingerprint(fixture.Root));
    }

    [Fact]
    public void LegacyJournalRetainsItsOriginalByteLevelFingerprint()
    {
        using var fixture=new FingerprintFixture();
        var method=typeof(UpdateApplyMode).Assembly.GetType("Neptune.Windows.WindowsUpdateTransaction",true)!
            .GetMethod("FingerprintTree",BindingFlags.NonPublic|BindingFlags.Static,null,[typeof(string),typeof(bool),typeof(bool)],null)!;
        string Legacy()=>(string)method.Invoke(null,[fixture.Root,true,false])!;
        var before=Legacy();var currentBefore=Fingerprint(fixture.Root);
        File.WriteAllBytes(fixture.Database+"-wal",[]);
        Assert.NotEqual(before,Legacy());
        Assert.Equal(currentBefore,Fingerprint(fixture.Root));
    }

    [Fact]
    public void JournalWithoutFingerprintVersionDefaultsToLegacy()
    {
        var journal=new WindowsUpdateJournal("neptune.windows-update.v1","target","previous","candidate","profile",Guid.NewGuid().ToString("N"),"prepared","portable-zip");
        var node=JsonSerializer.SerializeToNode(journal)!.AsObject();
        node.Remove("ProfileFingerprintVersion");
        Assert.Equal(1,node.Deserialize<WindowsUpdateJournal>()!.ProfileFingerprintVersion);
    }

    private sealed class FingerprintFixture : IDisposable
    {
        internal string Root {get;}=Path.Combine(Path.GetTempPath(),"neptune-update-fingerprint-"+Guid.NewGuid().ToString("N"));
        internal string Database {get;}
        internal FingerprintFixture()
        {
            var profile=Path.Combine(Root,"test-profile");Directory.CreateDirectory(profile);Database=Path.Combine(profile,"neptune.db");
            using var db=new SqliteConnection(new SqliteConnectionStringBuilder{DataSource=Database,Pooling=false}.ToString());
            db.Open();using var command=db.CreateCommand();command.CommandText="CREATE TABLE kept(value TEXT); INSERT INTO kept VALUES('preserved');";command.ExecuteNonQuery();
        }
        public void Dispose()
        {
            var target=Path.GetFullPath(Root);
            if(!target.StartsWith(Path.GetFullPath(Path.GetTempPath()).TrimEnd(Path.DirectorySeparatorChar)+Path.DirectorySeparatorChar,StringComparison.OrdinalIgnoreCase)
                ||!Path.GetFileName(target).StartsWith("neptune-update-fingerprint-",StringComparison.Ordinal))throw new InvalidOperationException("Unexpected fixture cleanup path.");
            Directory.Delete(target,true);
        }
    }
}
