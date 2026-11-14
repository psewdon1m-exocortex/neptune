using System.Reflection;
using Microsoft.Data.Sqlite;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class BootstrapSnapshotConcurrencyTests
{
    [Fact]
    public async Task HeldConsumerSnapshotDoesNotBlockScannerPromotionAndKeepsItsOriginalCut()
    {
        var root = Path.Combine(Path.GetTempPath(), "neptune-snapshot-concurrency", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var state = new NeptuneStateStore(Path.Combine(root,"profile.db"));
        Task? writer = null; bool completedWhileHeld = false; object? first = null; object? during = null;
        var connectionString = (string)typeof(NeptuneStateStore).GetProperty("ConnectionString", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(state)!;
        var options = new SqliteConnectionStringBuilder(connectionString) { Pooling = false, DefaultTimeout = 1 };
        try
        {
            await state.InitializeAsync(TestContext.Current.CancellationToken);
            await state.AddMappingAsync(new("mapping","client",root,true,DateTimeOffset.UtcNow),TestContext.Current.CancellationToken);
            var scan=await state.BeginBootstrapScanAsync("scan","mapping","client",root,"owner",TestContext.Current.CancellationToken);
            var directory=await state.ClaimBootstrapDirectoryAsync(scan.ScanId,"owner",TestContext.Current.CancellationToken);
            Assert.NotNull(directory);
            await state.StageBootstrapEntriesAsync(directory,[new("zero",false,0,DateTimeOffset.UtcNow)],TestContext.Current.CancellationToken);
            await using(var reader=new SqliteConnection(options.ToString()))
            {
                await reader.OpenAsync(TestContext.Current.CancellationToken);
                using var transaction=reader.BeginTransaction(deferred:true);
                await using var read=reader.CreateCommand();read.Transaction=transaction;read.CommandText="SELECT files FROM bootstrap_scan_runs WHERE scan_id='scan'";
                first=await read.ExecuteScalarAsync(TestContext.Current.CancellationToken);
                // A real consumer snapshot is retained before a real scanner
                // calls its public transactional promotion method.
                writer=Task.Run(()=>state.CompleteBootstrapDirectoryAsync(directory,TestContext.Current.CancellationToken),TestContext.Current.CancellationToken);
                completedWhileHeld=await Task.WhenAny(writer,Task.Delay(TimeSpan.FromSeconds(3),TestContext.Current.CancellationToken))==writer;
                if(completedWhileHeld){await writer;during=await read.ExecuteScalarAsync(TestContext.Current.CancellationToken);}
                await transaction.RollbackAsync(TestContext.Current.CancellationToken);
            }
            await writer;
            var after=await state.GetBootstrapScanAsync("scan",TestContext.Current.CancellationToken);
            var eligible=await state.ListBootstrapEligibleEntriesAsync("scan",cancellationToken:TestContext.Current.CancellationToken);
            Assert.True(completedWhileHeld,"Actual scanner promotion was blocked by a consumer read snapshot; WAL must permit its commit without releasing the consumer.");
            Assert.Equal(0L,first);Assert.Equal(0L,during);Assert.Equal(1,after!.Files);Assert.Single(eligible);
        }
        finally
        {
            // The reader is disposed before this point; join pending writer
            // before removing only this independently owned fixture.
            if(writer is not null)await writer;
            SqliteConnection.ClearAllPools();
            var parent=Path.GetFullPath(Path.Combine(Path.GetTempPath(),"neptune-snapshot-concurrency"))+Path.DirectorySeparatorChar;
            if(!Path.GetFullPath(root).StartsWith(parent,StringComparison.OrdinalIgnoreCase))throw new InvalidOperationException("Cleanup escaped owned fixture.");
            Directory.Delete(root,true);
        }
    }
}
