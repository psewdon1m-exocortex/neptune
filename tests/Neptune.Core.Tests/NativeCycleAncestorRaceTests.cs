using System.Reflection;
using System.Text.Json;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

[Collection("Native fresh cycle")]
public sealed class NativeCycleAncestorRaceTests
{
    [Fact]
    public async Task ActualDirectoryMoveAndOwnedJunctionSwapCannotSealOutsideOriginalParent()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Requires Windows native filesystem fixture"); return; }var ct=TestContext.Current.CancellationToken;
        await using var f=await WindowsFreshCycleTests.Fixture.Create();var parent=Path.Combine(f.Source,"parent");Directory.CreateDirectory(parent);File.WriteAllText(Path.Combine(parent,"file"),"original");
        await using var source=await WindowsLocalNamespaceSource.OpenAsync(f.Source,ct);var root=source.RootProof;
        await using var top=await source.OpenDirectoryAsync(new("audit",root.Handle.FileId128,"",root.Handle.CreationTime,"audit"),ct);
        var parentEntry=(await top.ReadPageAsync(ct)).Entries.Single();
        await using var child=await source.OpenDirectoryAsync(new("audit",parentEntry.FileId128,"parent",parentEntry.CreationTime,"audit"),ct);
        var leaf=(await child.ReadPageAsync(ct)).Entries.Single();await child.DisposeAsync();await top.DisposeAsync();await source.DisposeAsync();
        var moved=Path.Combine(f.Directory,"moved-parent");var renameSucceeded=false;var renameRejected=false;var linkCreated=false;
        await using var capture=await WindowsNativeCycleCapture.OpenAsync(f.Source,ct,(stage,_)=>
        {
            if(stage==NativeCaptureStage.GuardOpened)
            {
                try{Directory.Move(parent,moved);renameSucceeded=true;}
                catch(IOException){renameRejected=true;}
                if(renameSucceeded){Directory.CreateSymbolicLink(parent,moved);linkCreated=true;}
            }
            return Task.CompletedTask;
        });
        var operation=Guid.NewGuid().ToString("D");var work=new LocalCycleWork(operation,Guid.NewGuid().ToString("N"),new(leaf.FileId128,parentEntry.FileId128,"parent/file","file",leaf.CreationTime,false,leaf.Size,new(0,0),null,null),LocalCycleWorkPhase.Queued,null,null,null,null,null,null,null,0,null,null);
        var output=Path.Combine(f.Directory,"captured.spool");Exception? failure=null;LocalCycleSeal? seal=null;
        try{seal=await (Task<LocalCycleSeal>)typeof(WindowsNativeCycleCapture).GetMethod("CaptureAsync",BindingFlags.Instance|BindingFlags.NonPublic)!.Invoke(capture,[work,output,8L,ct])!;}
        catch(Exception e){failure=e;}
        await f.Evidence("native-parent-move-junction",new{renameSucceeded,renameRejected,linkCreated,failure=failure?.ToString(),sealedBytes=seal?.Size});
        if(renameSucceeded){Assert.NotNull(failure);Assert.Null(seal);Assert.False(File.Exists(output));}
        else{Assert.True(renameRejected);Assert.Null(failure);Assert.NotNull(seal);Assert.Equal("original",await File.ReadAllTextAsync(output,ct));}
        // No assertion that share flags universally fence POSIX ancestor moves.
        // Success requires final native identity/path validation to reject a swap.
        if(linkCreated)Directory.Delete(parent);
    }
}
