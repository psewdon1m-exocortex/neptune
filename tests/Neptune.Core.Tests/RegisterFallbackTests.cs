using System.Net;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class RegisterFallbackTests
{
    [Theory]
    [InlineData(HttpStatusCode.Unauthorized)]
    [InlineData(HttpStatusCode.Forbidden)]
    [InlineData(HttpStatusCode.BadRequest)]
    public void DoesNotUsePreviousTargetAfterAuthorizationOrContractFailure(HttpStatusCode status)
        => Assert.False(RegisterFallback.CanUse(new HttpRequestException("rejected", null, status)));

    [Fact]
    public void AllowsPreviousTargetDuringNetworkOrServerFailure()
    {
        Assert.True(RegisterFallback.CanUse(new HttpRequestException("network")));
        Assert.True(RegisterFallback.CanUse(new HttpRequestException("unavailable", null, HttpStatusCode.ServiceUnavailable)));
        Assert.False(RegisterFallback.CanUse(new System.Text.Json.JsonException("malformed")));
    }
}
