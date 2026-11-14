using System.Net;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class NetworkRecoveryTests
{
    [Fact]
    public async Task RouteStormCancelsOldGenerationAndUsesOneProbeBeforeResuming()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        await using var network = new NetworkRecoveryCoordinator(async ct =>
        { Interlocked.Increment(ref calls); started.TrySetResult(); await release.Task.WaitAsync(ct); }, TimeSpan.FromMilliseconds(30));
        network.WorkAvailable += () => resumed.TrySetResult();
        var old = network.Admit();
        for (var i = 0; i < 100; i++) network.Invalidate();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        Assert.True(old.IsCancellationRequested);
        Assert.Throws<NetworkUnavailableException>(() => network.Admit());
        Assert.Equal(1, calls);
        release.SetResult();
        await resumed.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        Assert.False(network.Admit().IsCancellationRequested);
        Assert.Equal("Recovering operations", network.State);
        network.Recovered(); Assert.Equal("Online", network.State);
    }

    [Fact]
    public async Task FailedProbeRetriesWithoutFileEventAndHonorsServerDelay()
    {
        var resumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        await using var network = new NetworkRecoveryCoordinator(_ =>
        { if (Interlocked.Increment(ref calls) == 1) throw new HttpRequestException("offline"); return Task.CompletedTask; }, TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(20));
        network.WorkAvailable += () => resumed.TrySetResult();
        network.Invalidate(retryAfter: TimeSpan.FromMilliseconds(150));
        await Task.Delay(60, TestContext.Current.CancellationToken); Assert.Equal(0, calls);
        await resumed.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        Assert.Equal(2, calls);
    }

    [Theory]
    [InlineData(407, "Proxy authorization required")]
    [InlineData(401, "Server authorization required")]
    [InlineData(503, "Waiting for network")]
    public void AuthorizationAndNetworkStatesAreDistinct(int code, string state)
        => Assert.Equal(state, NetworkFailure.State(new GuardedCycleRemoteException((HttpStatusCode)code, "test")));
}
