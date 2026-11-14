using System.Net;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Neptune.Core;
using Neptune.Core.Sync.Protocol;
using Xunit;

namespace Neptune.Core.Tests;

// In-memory HttpMessageHandler only. No TCP listener, profile, SQLite or payload.
public sealed partial class TransportDisposalTests
{
    private static CancellationToken Ct=>TestContext.Current.CancellationToken;
    private static readonly TimeSpan Bound=TimeSpan.FromSeconds(5);
    private static WindowsCycleConfiguration Config()
    {
        var id=()=>Guid.NewGuid().ToString("D");
        return new("mapping","client","audit","rules-v1",new Uri("http://localhost/"),id(),"fixture-only","local-test",
            new("neptune-sync/1","saturn.current-namespace.v1",id(),id(),id(),"test-policy",id()));
    }
    private static SaturnGuardedCycleTransport Transport(HttpClient http,WindowsCycleConfiguration c)
        =>(SaturnGuardedCycleTransport)typeof(SaturnGuardedCycleTransport).GetConstructor(BindingFlags.Instance|BindingFlags.NonPublic,null,
            [typeof(HttpClient),typeof(WindowsCycleConfiguration),typeof(Func<CancellationToken,Task<string>>)],null)!.Invoke(
                [http,c,(Func<CancellationToken,Task<string>>)(_=>Task.FromResult("Bearer fixture-only"))]);
    private static void Configure(SaturnGuardedCycleTransport t)
        =>t.ConfigureExtensions(new HashSet<string>{"folder-batch-v1","namespace-mutations-v1"},new("legacy",new string('a',64)),2);
    private static CycleFolderInput Folder(WindowsCycleConfiguration c)
    {
        var op=Guid.NewGuid().ToString("D");return new(op,"neptune-cycle-"+op,c.Binding.RootId,c.Binding.ServerEpoch,c.Binding.BindingVersion,
            c.Binding.RootId,"pending-folder","ensure",[new(c.Binding.RootId,null,"root")]);
    }
    private static Task Start(SaturnGuardedCycleTransport t,WindowsCycleConfiguration c,bool metadata,CancellationToken ct)
        =>metadata?t.MutateAsync(new(Guid.NewGuid().ToString("D"),"metadata-patch",JsonSerializer.SerializeToElement(new{rootId=c.Binding.RootId}),t.NamespacePolicy!),false,ct):t.EnsureDirectoryAsync(Folder(c),ct);
    private static Task Pump(SaturnGuardedCycleTransport t,bool metadata)
        =>(Task)typeof(SaturnGuardedCycleTransport).GetField(metadata?"_mutationPump":"_folderPump",BindingFlags.Instance|BindingFlags.NonPublic)!.GetValue(t)!;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SynchronousDisposeJoinsActualBatchHandlerAfterCallerCancellation(bool metadata)
    {
        using var handler=new HeldCancellationHandler();using var http=new HttpClient(handler);
        var config=Config();var transport=Transport(http,config);Configure(transport);using var caller=new CancellationTokenSource();
        var request=Start(transport,config,metadata,caller.Token);var gate=await handler.Next(Ct);Task? disposal=null;
        try
        {
            caller.Cancel();await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>request);
            Assert.False(gate.Cancelled.Task.IsCompleted); // Caller returned while the real batch is still alive.
            // Disposal must join cleanup even when the test's caller token is cancelled.
            disposal=Task.Run(transport.Dispose,CancellationToken.None);
            await gate.Cancelled.Task.WaitAsync(Bound,Ct);
            // The handler has observed shutdown but deliberately has not finished cleanup.
            // Old Dispose returns here and fails this assertion; no source-field check substitutes.
            await Assert.ThrowsAsync<TimeoutException>(()=>disposal.WaitAsync(TimeSpan.FromMilliseconds(200),Ct));
            Assert.False(gate.Exited.Task.IsCompleted);
            gate.Release.TrySetResult();await disposal.WaitAsync(Bound,Ct);
            Assert.True(gate.Exited.Task.IsCompleted);Assert.True(Pump(transport,metadata).IsCompleted);
        }
        finally
        {
            gate.Release.TrySetResult();if(disposal is null)transport.Dispose();else await disposal.WaitAsync(Bound,Ct);
        }
    }

    private sealed class Gate
    {
        public TaskCompletionSource Cancelled{get;}=new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release{get;}=new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Exited{get;}=new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
    private sealed class HeldCancellationHandler:HttpMessageHandler
    {
        private readonly Channel<Gate> _entries=Channel.CreateUnbounded<Gate>();private int _entered;
        public int EnteredCount=>Volatile.Read(ref _entered);
        public Task<Gate> Next(CancellationToken ct)=>_entries.Reader.ReadAsync(ct).AsTask().WaitAsync(Bound,ct);
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken ct)
        {
            var gate=new Gate();Interlocked.Increment(ref _entered);await _entries.Writer.WriteAsync(gate,ct);
            try{await Task.Delay(Timeout.Infinite,ct);throw new InvalidOperationException("No response before cancellation.");}
            catch(OperationCanceledException){gate.Cancelled.TrySetResult();await gate.Release.Task.ConfigureAwait(false);throw;}
            finally{gate.Exited.TrySetResult();}
        }
    }
    private sealed class ReplyHandler(Func<HttpRequestMessage,Task<byte[]>> reply):HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken ct)
            =>new(HttpStatusCode.OK){Content=new ByteArrayContent(await reply(request)),RequestMessage=request};
    }
}
