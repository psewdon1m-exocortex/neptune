using System.Reflection;
using System.Text.Json;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;
public sealed partial class WindowsFreshCycleTests
{
    internal sealed partial class FakeRemote:ISmallFileBatchRemote
    {
        public Task<SmallFileBatchOutcome> ExecuteSmallJobAsync(SmallFileBatchPlan plan,Stream bytes,CancellationToken ct)
            => throw new NotSupportedException("This fixture advertises only the original batch protocol.");
        internal bool Stage3BatchEnabled;
        internal readonly List<SmallFileBatchPlan> Stage3BatchPlans=[];
        internal readonly List<long> Stage3IndividualOffsets=[];
        public Task<SmallFileBatchCapability?> ReadSmallBatchCapabilitiesAsync(CancellationToken ct)=>Task.FromResult<SmallFileBatchCapability?>(Stage3BatchEnabled?new(Binding,Device,"local-test",new string('a',64),500,262144,33554432,4194304):null);
        private SmallFileBatchOutcome BatchOutcome(SmallFileBatchPlan plan)=>new(plan.BatchId,plan.Entries.Select(e=>{var status=_keys.TryGetValue(e.Original.Intent.IdempotencyKey,out var id)?_uploads[id].Status:null;return new SmallFileBatchItemOutcome(e.Original.Intent.IdempotencyKey,e.SourceGeneration,e.SourceIdentity,status is null?"MISSING":status.Phase=="published"?"PUBLISHED":status.Phase=="not_published"?"NOT_PUBLISHED":"PENDING",status,null);}).ToArray());
        public Task<SmallFileBatchOutcome> ReadSmallBatchAsync(SmallFileBatchPlan plan,CancellationToken ct)=>Task.FromResult(BatchOutcome(plan));
        public Task<SmallFileBatchOutcome> PrepareSmallBatchAsync(SmallFileBatchPlan plan,CancellationToken ct){Stage3BatchPlans.Add(plan);return Task.FromResult(BatchOutcome(plan));}
        public async Task<SmallFileBatchOutcome> SendSmallBatchAsync(SmallFileBatchPlan plan,Stream bytes,CancellationToken ct)
        {
            foreach(var e in plan.Entries){var input=e.Original;var data=new byte[int.Parse(input.Intent.SizeBytes)];await bytes.ReadExactlyAsync(data,ct);var hash=LocalCycleWire.IntentHash(input);var status=await UploadAsync("prepare",input,null,hash,ct);await AppendAsync(input,status.UploadId,hash,0,data,ct);await UploadAsync("complete",input,status.UploadId,hash,ct);}return BatchOutcome(plan);
        }
    }
}
public sealed partial class Stage3ChatBTests
{
    [Fact]
    public async Task B08_SixMixedOriginalsRestartOnlyTwoMissingUseNewAggregate()
    {
        RequireWindows();var clock=new Stage3Clock();await using var f=await WindowsFreshCycleTests.Fixture.Create(clock);for(var i=0;i<6;i++)File.WriteAllBytes(Path.Combine(f.Source,"file-"+i),Enumerable.Repeat((byte)i,128).ToArray());
        await using var engine=await f.Open((o,_)=>o.Stage==WindowsCycleStage.FileCaptured?Task.FromException(new BootstrapCaptureDeferredException("fixture stop after native seal before intent")):Task.CompletedTask);
        var first=await engine.InitialSyncAsync(Ct);await f.Evidence("B08-six-capture",new{first,state=await NamespaceState(f)});Assert.Equal(6,first.CapturedFiles);Assert.Equal(0,first.PublishedFiles);Assert.Equal(0,f.Remote.Prepares);Assert.Empty(f.Remote.Stage3BatchPlans);
        var s=Session(engine);await AdvancePastDue(f,clock);await s.RetryCycleDeferredAsync(engine.ScopeId,Ct);await s.EnableSmallBatchesAsync(engine.ScopeId,Ct);
        var works=await s.ReadCycleActiveWorkAsync(engine.ScopeId,false,Ct);Assert.Equal(6,works.Count);var inputs=new List<CycleUploadInput>();
        foreach(var work in works)
        {
            var target=await s.ReadCycleTargetAsync(engine.ScopeId,work.Entry,Ct);var input=new CycleUploadInput(f.Remote.Binding.ServerEpoch,f.Remote.Binding.BindingVersion,"local-test",new("saturn.upload-intent.v2","windows_sync",f.Remote.Device,f.Remote.Binding.RootId,"/"+work.Entry.RelativePath,"neptune-cycle-"+work.OperationId,target.Parent,work.Entry.RawName,null,work.Size!.Value.ToString(),work.Sha256!,null,"*",true,null,new("absent"),target.Topology));
            await s.SaveCycleInputAsync(engine.ScopeId,work.OperationId,input,Ct);inputs.Add(input);
        }
        var aggregates=new List<string>();
        var resources=(LocalCycleResources)typeof(WindowsFreshSyncEngine).GetField("_resources",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(engine)!;
        foreach(var group in works.Chunk(NeptuneStateStore.LocalSmallBatchItems))
        {
            var aggregate=Guid.NewGuid().ToString("D");aggregates.Add(aggregate);var row=await s.CreateSmallBatchAsync(engine.ScopeId,aggregate,group.Select(w=>w.OperationId).ToArray(),Ct);
            var hash=await LocalSmallBatchSpool.TryBuildAsync(resources,row,(op,ct)=>s.ReadCycleWorkAsync(engine.ScopeId,op,ct),(identity,ct)=>s.BindSmallBatchSpoolAsync(engine.ScopeId,aggregate,identity,ct),Ct);Assert.NotNull(hash);
            await s.ChangeSmallBatchAsync(engine.ScopeId,aggregate,LocalSmallBatchPhase.Ready,hash,Ct);await s.ChangeSmallBatchAsync(engine.ScopeId,aggregate,LocalSmallBatchPhase.Attempted,null,Ct);
        }
        // Server committed two publications, accepted a partial fifth, refused
        // the sixth; its aggregate response never reached the local state.
        for(var i=0;i<6;i++)
        {
            if(i is 2 or 3)continue;var input=inputs[i];var intent=LocalCycleWire.IntentHash(input);var status=await f.Remote.UploadAsync("prepare",input,null,intent,Ct);
            if(i<2||i==4){var bytes=await File.ReadAllBytesAsync(works[i].SpoolPath!,Ct);await f.Remote.AppendAsync(input,status.UploadId,intent,0,bytes.AsMemory(0,i==4?32:128),Ct);}
            if(i!=4){f.Remote.CancelComplete=i==5;await f.Remote.UploadAsync("complete",input,status.UploadId,intent,Ct);}
        }
        var originals=await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)");await engine.DisposeAsync();await VerifyAcceptedProfileBackup(f,"batch");f.Store=new(f.Database,clock);f.Remote.Stage3BatchEnabled=true;
        await using var resumed=await Open(f);Assert.True(await resumed.EnableSmallFileBatchQualificationAsync(Ct));var recovered=await f.FreshWhenDueAsync(resumed,Ct);await f.Evidence("B08-six-mixed",new{recovered,oldAggregates=aggregates,subsets=f.Remote.Stage3BatchPlans.Select(p=>new{p.BatchId,ids=p.Entries.Select(e=>e.Original.Intent.IdempotencyKey)}),f.Remote.Stage3IndividualOffsets});
        Assert.Null(recovered.Error);Assert.Equal(0,recovered.CapturedBytes);var subset=Assert.Single(f.Remote.Stage3BatchPlans);Assert.DoesNotContain(subset.BatchId,aggregates);Assert.Equal(inputs.Skip(2).Take(2).Select(i=>i.Intent.IdempotencyKey).Order(),subset.Entries.Select(e=>e.Original.Intent.IdempotencyKey).Order());
        Assert.Equal(originals,await f.Scalar("SELECT json_group_array(input_json) FROM(SELECT input_json FROM local_cycle_work ORDER BY work_no)"));Assert.Equal(5L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE phase=5"));Assert.Equal(1L,await f.Scalar("SELECT COUNT(*) FROM local_cycle_work WHERE phase=7 AND result_json IS NOT NULL"));Assert.Contains(32L,f.Remote.Stage3IndividualOffsets);Assert.Equal(6,f.Remote.Prepares);Assert.Equal(6,f.Remote.Count);
    }
}
