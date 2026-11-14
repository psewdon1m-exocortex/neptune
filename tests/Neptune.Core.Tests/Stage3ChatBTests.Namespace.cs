using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class WindowsFreshCycleTests
{
    internal sealed partial class FakeRemote:IWindowsCycleExtensions
    {
        internal bool ExtensionsEnabled,CloneEnabled,LoseMutationReply,LoseApprovalReply,WaitEnabled;
        internal Func<string,long,CancellationToken,Task<SyncFeedPage>>? WaitHandler;
        internal readonly List<SyncMutationRequest> Mutations=[];
        internal readonly List<SyncDeletePlan> Approvals=[];
        private readonly Dictionary<string,SyncMutationReceipt> _mutationReceipts=[];
        internal int MutationStatusReads;
        internal Func<SyncMutationRequest,Task>? BeforeMutation;
        internal SyncDescriptor CurrentFixtureNode(string name)=>_nodes.Values.Single(n=>n.Name==name);
        public int NamespaceRevision=>2;
        public SyncNamespacePolicy? NamespacePolicy=>ExtensionsEnabled?new("legacy",new string('a',64)):null;
        public bool Supports(string feature)=>feature=="changes-wait-v1"&&WaitEnabled||feature=="stream-upload-v1"&&StreamEnabled||feature=="scale-transfer-v2"&&ParallelStreamEnabled||ExtensionsEnabled&&(feature=="namespace-mutations-v1"||feature=="clone-content-v1"&&CloneEnabled);
        public Task<SyncFeedPage> WaitForChangesAsync(string cursor,long after,CancellationToken ct)=>WaitHandler?.Invoke(cursor,after,ct)??ReadChangesAsync(cursor,after,ct);
        public Task<SyncContentReference?> LookupContentAsync(long size,string hash,CancellationToken ct)
        {lock(_gate){var node=_nodes.Values.FirstOrDefault(n=>n.Type=="file"&&n.SizeBytes==size.ToString()&&n.ContentSha256==hash);return Task.FromResult(node is null?null:new SyncContentReference(node.NodeId,node.ContentVersionId!,node.Version,node.SizeBytes,hash));}}
        public Task<string> ApproveDeletePlanAsync(SyncDeletePlan original,CancellationToken ct)
        {
            lock(_gate){Approvals.Add(original);if(LoseApprovalReply){LoseApprovalReply=false;throw new HttpRequestException("fixture approval response lost");}
                return Task.FromResult(JsonSerializer.Serialize(new{schema="saturn.delete-plan-approved.v1",planId=original.PlanId,planDigest=original.PlanDigest,itemCount=original.Items.Count}));}
        }
        public async Task<SyncMutationReceipt> MutateAsync(SyncMutationRequest original,bool inspect,CancellationToken ct)
        {
            if(!inspect&&BeforeMutation is {} before)await before(original);
            lock(_gate)
            {
                if(inspect)MutationStatusReads++;
                if(_mutationReceipts.TryGetValue(original.OperationId,out var replay))return replay;
                if(inspect)throw new GuardedCycleRemoteException(HttpStatusCode.Conflict,"operation_reused",true);
                var input=original.Input;var expected=input.GetProperty("expected");SyncDescriptor? current=null;
                if(expected.GetProperty("kind").GetString()=="version")
                {if(!_nodes.TryGetValue(expected.GetProperty("node_id").GetString()!,out current)||current.Version!=expected.GetProperty("version").GetString())throw new GuardedCycleRemoteException(HttpStatusCode.PreconditionFailed,"precondition_failed",true);}
                var parent=input.TryGetProperty("parentId",out var p)?p.GetString()!:current!.ParentId!;var name=input.TryGetProperty("name",out var n)?n.GetString()!:current!.Name;
                if(original.Kind is "move" or "create-empty" or "clone-content"&&_nodes.Values.Any(d=>d.ParentId==parent&&d.Name==name&&d.NodeId!=current?.NodeId))throw new GuardedCycleRemoteException(HttpStatusCode.PreconditionFailed,"precondition_failed",true);
                SyncDescriptor result;
                if(original.Kind is "move" or "metadata-patch" or "quarantine-delete")result=current! with{ParentId=parent,Name=name,Version=Guid.NewGuid().ToString("D")};
                else
                {
                    var source=original.Kind=="clone-content"?_nodes[input.GetProperty("source").GetProperty("nodeId").GetString()!]:null;
                    result=new(Guid.NewGuid().ToString("D"),parent,name,"file",source?.SizeBytes??"0",source?.ContentSha256??Convert.ToHexStringLower(SHA256.HashData([])),Guid.NewGuid().ToString("D"),Guid.NewGuid().ToString("D"),"AVAILABLE");
                }
                var deleted=original.Kind=="quarantine-delete";
                if(deleted){if(_nodes.Values.Any(d=>d.ParentId==result.NodeId))throw new GuardedCycleRemoteException(HttpStatusCode.Conflict,"directory_not_empty",true);_nodes.Remove(result.NodeId);_events.Add(new(result,(_events.Count+1).ToString(),"tombstone",original.OperationId,current!.Version));}
                else{_nodes[result.NodeId]=result;Event(result,original.OperationId);}
                var raw=JsonSerializer.SerializeToUtf8Bytes(new{schema="saturn.namespace-mutation-status.v1",operationId=original.OperationId,intentHash=new string('b',64),state="PUBLISHED",result=new{description=Wire(result),firstSeq=_events.Count.ToString(),lastSeq=_events.Count.ToString(),tombstone=deleted}});
                var receipt=SyncMutationWire.Read(raw,original);_mutationReceipts.Add(original.OperationId,receipt);Mutations.Add(original);
                if(LoseMutationReply){LoseMutationReply=false;throw new HttpRequestException("fixture commit response lost");}return receipt;
            }
        }
    }
}

public sealed partial class Stage3ChatBTests
{
    private static async Task<object> NamespaceState(WindowsFreshCycleTests.Fixture f)
    {
        var tables=new[]{"local_cycle_files","local_cycle_work","local_parent_reconcile_plans","local_parent_reconcile_entries","local_parent_reconcile_conflicts","client_namespace_operations"};
        var result=new Dictionary<string,object>();await using var c=new Microsoft.Data.Sqlite.SqliteConnection("Data Source="+f.Database+";Pooling=False");await c.OpenAsync(Ct);
        foreach(var table in tables){await using var q=c.CreateCommand();q.CommandText="SELECT 1 FROM sqlite_master WHERE name=$name";q.Parameters.AddWithValue("$name",table);if(await q.ExecuteScalarAsync(Ct)is null)continue;q.Parameters.Clear();q.CommandText="SELECT * FROM "+table+" LIMIT 20";await using var r=await q.ExecuteReaderAsync(Ct);var rows=new List<object>();while(await r.ReadAsync(Ct)){var row=new Dictionary<string,object?>();for(var i=0;i<r.FieldCount;i++)row[r.GetName(i)]=r.IsDBNull(i)?null:r.GetValue(i);rows.Add(row);}result[table]=rows;}return result;
    }
    private static Task<WindowsFreshSyncEngine> NamespaceEngine(WindowsFreshCycleTests.Fixture f)=>WindowsFreshSyncEngine.ResumeOrOpenAsync(new WindowsCycleNativeOptions(LocalJournalReadMode.Named,true),f.Store,f.Source,Path.Combine(f.Directory,"spool"),Configuration(f),f.Remote,new(MinimumFreeDiskBytes:0),cancellationToken:Ct);
    [Fact]
    public async Task B04_ConfirmedDirectoryNewHintRetainsManagedAuthority()
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);Directory.CreateDirectory(Path.Combine(f.Source,"dir"));await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,1);
        Directory.SetLastWriteTimeUtc(Path.Combine(f.Source,"dir"),DateTime.UtcNow.AddMinutes(1));await AwaitHint(f);clock.Advance(TimeSpan.FromSeconds(1));var result=await engine.FreshSyncAsync(Ct);await f.Evidence("B04-directory-hint",new{result,state=await NamespaceState(f)});Assert.Null(result.Error);Assert.Equal(0,result.Conflicts);Assert.Equal(0,result.PendingEntries);Assert.Equal(2,f.Remote.Count);
    }
    private static async Task ManagedBaseline(WindowsFreshSyncEngine engine,WindowsFreshCycleTests.Fixture f,Stage3Clock clock,int entries)
    {
        var initial=await engine.InitialSyncAsync(Ct);var passes=new List<WindowsCycleResult>{initial};
        for(var i=0;i<4&&initial.PendingEntries>0;i++){Assert.Null(initial.Error);await AdvancePastDue(f,clock);initial=await engine.FreshSyncAsync(Ct);passes.Add(initial);}
        await f.Evidence("managed-baseline",new{passes,state=initial.PendingEntries>0?await NamespaceState(f):null});Assert.Null(initial.Error);Assert.Equal(0,initial.PendingEntries);Assert.Equal((long)entries,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE baseline IS NOT NULL"));
    }
    [Theory]
    [InlineData("rename")][InlineData("case")][InlineData("directory")][InlineData("lost-reply")]
    public async Task B04_NativeRenamePreservesResourceAndCleanDescendantBytes(string scenario)
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);f.Remote.ExtensionsEnabled=true;
        var directory=scenario=="directory";if(directory)Directory.CreateDirectory(Path.Combine(f.Source,"before"));var path=Path.Combine(f.Source,directory?"before/leaf":"before");File.WriteAllText(path,"sealed bytes");
        await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,directory?2:1);var resource=await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE kind=0");var prepares=f.Remote.Prepares;
        if(directory)Directory.Move(Path.Combine(f.Source,"before"),Path.Combine(f.Source,"after"));else File.Move(path,Path.Combine(f.Source,scenario=="case"?"BEFORE":"after"));
        await AwaitHint(f);clock.Advance(TimeSpan.FromSeconds(1));f.Remote.LoseMutationReply=scenario=="lost-reply";var changed=await engine.FreshSyncAsync(Ct);
        if(scenario=="lost-reply"){await AdvancePastDue(f,clock);changed=await engine.FreshSyncAsync(Ct);Assert.True(f.Remote.MutationStatusReads>0);}
        await f.Evidence("B04-"+scenario,new{changed,f.Remote.Mutations,state=await NamespaceState(f)});Assert.Null(changed.Error);Assert.Equal(0,changed.CapturedBytes);Assert.Equal(prepares,f.Remote.Prepares);
        Assert.Equal(resource,await f.Scalar("SELECT remote_node FROM local_cycle_files WHERE kind=0"));Assert.Single(f.Remote.Mutations,x=>x.Kind=="move");
        Assert.Equal(directory?"after/leaf":scenario=="case"?"BEFORE":"after",await f.Scalar("SELECT path FROM local_cycle_files WHERE kind=0"));
    }
    [Fact]
    public async Task B05_DeleteApprovalLostReplyRestartsSamePlanBeforePostorderEffects()
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);f.Remote.ExtensionsEnabled=true;
        Directory.CreateDirectory(Path.Combine(f.Source,"parent","child"));File.WriteAllText(Path.Combine(f.Source,"parent","child","grandchild"),"managed");
        await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,3);Directory.Delete(Path.Combine(f.Source,"parent"),true);await AwaitHint(f);clock.Advance(TimeSpan.FromSeconds(1));var observed=await engine.FreshSyncAsync(Ct);await f.Evidence("B05-observed",new{observed,state=await NamespaceState(f)});
        var preview=await engine.PreviewDeletesAsync(Ct);Assert.Equal(3,preview.Operations.Count);Assert.Empty(f.Remote.Mutations);
        await Assert.ThrowsAsync<InvalidOperationException>(()=>engine.ApproveDeletesAsync(new string('0',64),Ct));await engine.ApproveDeletesAsync(preview.Digest,Ct);
        f.Remote.LoseApprovalReply=true;f.Remote.BeforeMutation=async request=>{if(request.Kind=="quarantine-delete")Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM client_delete_plans WHERE receipt_json IS NOT NULL"));};
        await engine.FreshSyncAsync(Ct);Assert.Empty(f.Remote.Mutations);await engine.DisposeAsync();f.Store=new(f.Database,clock);await AdvancePastDue(f,clock);await using var resumed=await NamespaceEngine(f);var recovered=await resumed.FreshSyncAsync(Ct);
        await f.Evidence("B05-delete",new{preview,recovered,f.Remote.Approvals,f.Remote.Mutations});Assert.Null(recovered.Error);
        Assert.Equal(2,f.Remote.Approvals.Count);Assert.Equal(JsonSerializer.Serialize(f.Remote.Approvals[0]),JsonSerializer.Serialize(f.Remote.Approvals[1]));
        Assert.Equal(new[]{"grandchild","child","parent"},f.Remote.Mutations.Select(m=>preview.Operations.Single(p=>p.OperationId==m.OperationId).Path.Split('/').Last()));
        Assert.Equal(1,f.Remote.Count);Assert.Equal(0L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE baseline IS NOT NULL"));
    }
    [Theory]
    [InlineData("mtime")][InlineData("changed-same-mtime")][InlineData("ambiguous")]
    public async Task B09_MetadataRequiresNativeContentProofAndKeepsOriginalOnAmbiguity(string scenario)
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);f.Remote.ExtensionsEnabled=true;var path=Path.Combine(f.Source,"file");File.WriteAllText(path,"original");
        await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,1);var baseline=(string)(await f.Scalar("SELECT baseline FROM local_cycle_files"))!;var before=JsonSerializer.Deserialize<LocalCycleBaseline>(baseline,LocalCycleWire.Json)!;
        var mtime=File.GetLastWriteTimeUtc(path);if(scenario=="changed-same-mtime"){File.WriteAllText(path,"modified");File.SetLastWriteTimeUtc(path,mtime);}else File.SetLastWriteTimeUtc(path,mtime.AddMinutes(1));
        await AwaitHint(f);clock.Advance(TimeSpan.FromSeconds(1));f.Remote.LoseMutationReply=scenario=="ambiguous";var result=await engine.FreshSyncAsync(Ct);
        if(scenario=="ambiguous"){await engine.DisposeAsync();await AdvancePastDue(f,clock);await using var resumed=await NamespaceEngine(f);result=await resumed.FreshSyncAsync(Ct);}
        await f.Evidence("B09-metadata-"+scenario,new{result,f.Remote.Mutations});Assert.Null(result.Error);var after=JsonSerializer.Deserialize<LocalCycleBaseline>((string)(await f.Scalar("SELECT baseline FROM local_cycle_files"))!,LocalCycleWire.Json)!;
        Assert.Equal(before.Description.NodeId,after.Description.NodeId);
        if(scenario=="changed-same-mtime"){Assert.Equal(8,result.CapturedBytes);Assert.NotEqual(before.Description.ContentVersionId,after.Description.ContentVersionId);Assert.Empty(f.Remote.Mutations);}
        else{Assert.Equal(0,result.CapturedBytes);Assert.Equal(before.Description.ContentVersionId,after.Description.ContentVersionId);Assert.Single(f.Remote.Mutations,m=>m.Kind=="metadata-patch");}
    }
    [Theory]
    [InlineData("destination-cas")][InlineData("late-generation")]
    public async Task B04_NativeMoveCasAndLateWriteNeverOverwriteForeignOrClearNewBytes(string scenario)
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);f.Remote.ExtensionsEnabled=true;var path=Path.Combine(f.Source,"old");File.WriteAllText(path,"old bytes");
        await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,1);var id=await f.Scalar("SELECT remote_node FROM local_cycle_files");File.Move(path,Path.Combine(f.Source,"new"));await AwaitHint(f);clock.Advance(TimeSpan.FromSeconds(1));
        f.Remote.BeforeMutation=async _=>{f.Remote.BeforeMutation=null;if(scenario=="destination-cas")f.Remote.AddExisting("new","foreign");else{File.WriteAllText(Path.Combine(f.Source,"new"),"later data");await Session(engine).SaveEventHintsAsync(["new"],false,Ct);}};
        var first=await engine.FreshSyncAsync(Ct);await f.Evidence("B04-"+scenario,new{first,f.Remote.Mutations,state=await NamespaceState(f)});Assert.Equal(id,await f.Scalar("SELECT remote_node FROM local_cycle_files"));
        if(scenario=="destination-cas"){Assert.Empty(f.Remote.Mutations);Assert.True(first.PendingEntries>0);Assert.Equal(2+1,f.Remote.Count);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM client_namespace_operations WHERE state='NOT_PUBLISHED' AND error='precondition_failed'"));}
        else{Assert.Single(f.Remote.Mutations);Assert.True(first.PendingEntries>0);clock.Advance(TimeSpan.FromSeconds(1));var next=await engine.FreshSyncAsync(Ct);Assert.Null(next.Error);Assert.Equal(10,next.CapturedBytes);Assert.Equal(0,next.PendingEntries);}
    }
    [Theory]
    [InlineData("exclude")][InlineData("remote-edit")][InlineData("replacement")]
    public async Task B05_UnsafeAbsenceCannotAuthorizeDelete(string scenario)
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);f.Remote.ExtensionsEnabled=true;var path=Path.Combine(f.Source,"file");File.WriteAllText(path,"managed");
        await using var engine=await NamespaceEngine(f);await ManagedBaseline(engine,f,clock,1);var id=(string)(await f.Scalar("SELECT remote_node FROM local_cycle_files"))!;
        if(scenario=="exclude"){var policy=(await engine.ReadMappingPolicyAsync(Ct)) with{ExcludedPrefixes=["file"]};var p=await engine.PreviewPolicyAsync(policy,Ct);await engine.ApplyPolicyAsync(policy,p.Digest,Ct);}
        File.Move(path,Path.Combine(f.Directory,"retained-original"));if(scenario=="replacement")File.WriteAllText(path,"unmanaged replacement");if(scenario=="remote-edit")f.Remote.RenameFolder(id,"remote-name");
        await AwaitHint(f);clock.Advance(TimeSpan.FromSeconds(1));var result=await engine.FreshSyncAsync(Ct);var preview=await engine.PreviewDeletesAsync(Ct);await f.Evidence("B05-negative-"+scenario,new{result,preview});Assert.Empty(preview.Operations);Assert.Empty(f.Remote.Mutations);Assert.Equal(2,f.Remote.Count);Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_files WHERE baseline IS NOT NULL"));
    }
    [Theory]
    [InlineData("empty")][InlineData("clone")][InlineData("off")]
    public async Task B09_EmptyAndCloneAvoidFakeUploadsAndCapabilityOffFallsBack(string scenario)
    {
        RequireWindows();await using var f=await WindowsFreshCycleTests.Fixture.Create();f.Remote.ExtensionsEnabled=scenario!="off";f.Remote.CloneEnabled=scenario=="clone";
        if(scenario=="clone")f.Remote.AddExisting("known","clone bytes");File.WriteAllText(Path.Combine(f.Source,"target"),scenario=="clone"?"clone bytes":"");
        await using var engine=await NamespaceEngine(f);var result=await engine.InitialSyncAsync(Ct);await f.Evidence("B09-"+scenario,new{result,f.Remote.Prepares,f.Remote.Mutations});Assert.Null(result.Error);Assert.Equal(1,result.PublishedFiles);
        Assert.Equal(scenario=="off"?1:0,f.Remote.Prepares);Assert.Equal(scenario=="off"?0:1,f.Remote.Mutations.Count);
        if(scenario!="off")Assert.Equal(scenario=="empty"?"create-empty":"clone-content",f.Remote.Mutations[0].Kind);
    }
    [Theory][InlineData("no-match")][InlineData("expected-mismatch")][InlineData("ambiguous")]
    public async Task B09_CloneFallbackAndLostAckKeepIndependentTarget(string scenario)
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);f.Remote.ExtensionsEnabled=true;f.Remote.CloneEnabled=true;f.Remote.AddExisting("known","clone bytes");var source=f.Remote.CurrentFixtureNode("known");File.WriteAllText(Path.Combine(f.Source,"target"),scenario=="no-match"?"different bytes":"clone bytes");
        if(scenario=="expected-mismatch")f.Remote.BeforeMutation=_=>throw new GuardedCycleRemoteException(System.Net.HttpStatusCode.PreconditionFailed,"precondition_failed",true);f.Remote.LoseMutationReply=scenario=="ambiguous";
        await using var engine=await NamespaceEngine(f);var first=await engine.InitialSyncAsync(Ct);WindowsCycleResult result=first;
        if(scenario=="ambiguous"){Assert.True(first.PendingEntries>0);await engine.DisposeAsync();await AdvancePastDue(f,clock);await using var resumed=await NamespaceEngine(f);result=await resumed.FreshSyncAsync(Ct);Assert.True(f.Remote.MutationStatusReads>0);Assert.Equal(0,result.CapturedBytes);}
        await f.Evidence("B09-clone-"+scenario,new{first,result,f.Remote.Prepares,f.Remote.Mutations});Assert.Null(result.Error);Assert.Equal(0,result.PendingEntries);var target=f.Remote.CurrentFixtureNode("target");Assert.NotEqual(source.NodeId,target.NodeId);Assert.NotEqual(source.ContentVersionId,target.ContentVersionId);Assert.Equal(source,f.Remote.CurrentFixtureNode("known"));Assert.Equal(scenario=="ambiguous"?0:1,f.Remote.Prepares);Assert.Equal(scenario=="ambiguous"?1:0,f.Remote.Mutations.Count);
    }
}
