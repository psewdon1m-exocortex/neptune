using System.Net;
using System.Text;
using Neptune.Windows;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class PublicationCapabilityWaitTests
{
    private sealed class Handler(Func<HttpRequestMessage,CancellationToken,Task<HttpResponseMessage>> run):HttpMessageHandler
    { protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken ct)=>run(request,ct); }
    [Fact]
    public async Task OnlyExplicitOwnedPublicationWaitsAndThenRechecksFreshCapability()
    {
        var calls=0;var uri=new Uri("https://saturn.test/capabilities");
        using var http=new HttpClient(new Handler((request,_)=>Task.FromResult(new HttpResponseMessage(++calls==1?HttpStatusCode.ServiceUnavailable:HttpStatusCode.OK) {
            RequestMessage=request, Content=new StringContent(calls==1?"{\"code\":\"publication_pending\",\"retry_after_ms\":2000}":"{}",Encoding.UTF8,"application/json")
        })));
        using var result=await WindowsGuardedCapabilities.ReadWhilePublishingAsync(http,uri,"fixture",TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.OK,result.StatusCode);Assert.Equal(2,calls);
    }
    [Theory]
    [InlineData(409,"{\"code\":\"reconciliation_required\"}")]
    [InlineData(401,"{\"code\":\"unauthorized\"}")]
    [InlineData(503,"{\"code\":\"backend_draining\"}")]
    [InlineData(503,"{\"code\":\"publication_pending\",\"code\":\"publication_pending\"}")]
    public async Task UnrelatedOrAmbiguousFailuresNeverAuthorizeRetry(int status,string body)
    {
        var calls=0;using var http=new HttpClient(new Handler((request,_)=>{calls++;return Task.FromResult(new HttpResponseMessage((HttpStatusCode)status){RequestMessage=request,Content=new StringContent(body)});}));
        using var result=await WindowsGuardedCapabilities.ReadWhilePublishingAsync(http,new Uri("https://saturn.test/capabilities"),"fixture",TestContext.Current.CancellationToken);
        Assert.Equal(status,(int)result.StatusCode);Assert.Equal(1,calls);
    }
    [Fact]
    public async Task PendingWaitHonorsCallerCancellation()
    {
        using var cancel=CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);var calls=0;
        using var http=new HttpClient(new Handler((request,_)=>{calls++;cancel.CancelAfter(50);return Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable){RequestMessage=request,Content=new StringContent("{\"code\":\"publication_pending\"}")});}));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(()=>WindowsGuardedCapabilities.ReadWhilePublishingAsync(http,new Uri("https://saturn.test/capabilities"),"fixture",cancel.Token));Assert.Equal(1,calls);
    }
}
