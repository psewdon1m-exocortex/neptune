using System.Net;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class NetworkAcceptanceTests
{
    [Fact]
    public async Task N06_FailedProbeUpdatesAccessStateAndPreservesRetryAfterAcrossHints()
    {
        var failed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        await using var network = new NetworkRecoveryCoordinator(_ =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                failed.SetResult();
                throw new GuardedCycleRemoteException(HttpStatusCode.ProxyAuthenticationRequired, "proxy")
                    { RetryAfter = TimeSpan.FromMilliseconds(500) };
            }
            return Task.CompletedTask;
        }, TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(10));
        network.WorkAvailable += () => resumed.TrySetResult();
        network.Invalidate("Waiting for network (DNS)");
        await failed.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.Equal("Proxy authorization required", network.State);
        for (var i = 0; i < 100; i++) network.Invalidate();
        await Task.Delay(150, TestContext.Current.CancellationToken);
        Assert.Equal(1, calls);
        await resumed.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        Assert.Equal(2, calls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task N03_StaleProbeCannotPublishSuccessOrFailure(bool fail)
    {
        var first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSecond = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        var wakes = 0;
        await using var network = new NetworkRecoveryCoordinator(async ct =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                // Deliberately deliver a late completion from an adapter that
                // has not observed route cancellation yet.
                first.SetResult(); await release.Task.WaitAsync(TestContext.Current.CancellationToken);
                if (fail) throw new GuardedCycleRemoteException(HttpStatusCode.Forbidden, "old") { RetryAfter = TimeSpan.FromHours(1) };
            }
            else { second.SetResult(); await releaseSecond.Task.WaitAsync(ct); }
        }, TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(10));
        network.WorkAvailable += () => Interlocked.Increment(ref wakes);
        network.Invalidate();
        await first.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        network.Invalidate("Waiting for network (DNS)");
        release.SetResult();
        await second.Task.WaitAsync(TimeSpan.FromSeconds(3), TestContext.Current.CancellationToken);
        Assert.Equal(0, wakes);
        Assert.Equal("Waiting for network (DNS)", network.State);
        Assert.Throws<NetworkUnavailableException>(() => network.Admit());
        releaseSecond.SetResult();
    }
}
