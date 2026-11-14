using System.Net;
using System.Net.Sockets;
using System.Text;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed class GuardedCycleCursorWireTests
{
    [Theory]
    [InlineData(410,"{\"code\":\"cursor_expired\"}",true,"cursor_expired")]
    [InlineData(409,"{\"code\":\"journal_gap\"}",true,"journal_gap")]
    [InlineData(410,"{\"code\":\"invalid_cursor\"}",true,"invalid_cursor")]
    [InlineData(401,"{\"code\":\"unauthorized\"}",true,"unauthorized")]
    [InlineData(410,"{\"code\":\"cursor_expired\",\"extra\":1}",false,"http_410")]
    [InlineData(410,"{\"code\":\"cursor_expired\",\"code\":\"cursor_expired\"}",false,"http_410")]
    [InlineData(410,"[\"cursor_expired\"]",false,"http_410")]
    [InlineData(410,"{\"code\":7}",false,"http_410")]
    [InlineData(410,"broken JSON",false,"http_410")]
    public async Task ActualLoopbackTransportRecognizesOnlyExactBoundedErrorEnvelope(int status,string json,bool structured,string code)
    {
        using var listener=new TcpListener(IPAddress.Loopback,0);listener.Start();var origin=new Uri("http://127.0.0.1:"+((IPEndPoint)listener.LocalEndpoint).Port+"/");using var timeout=new CancellationTokenSource(TimeSpan.FromSeconds(10));var ct=timeout.Token;
        var server=Task.Run(async()=>
        {
            using var client=await listener.AcceptTcpClientAsync(ct);await using var stream=client.GetStream();using var request=new MemoryStream();var buffer=new byte[4096];var split=-1;long size=0;
            while(split<0||request.Length<split+4+size){var n=await stream.ReadAsync(buffer,ct);if(n==0||request.Length+n>64*1024)throw new IOException("Bounded loopback request incomplete.");request.Write(buffer,0,n);var text=Encoding.ASCII.GetString(request.GetBuffer(),0,(int)request.Length);split=text.IndexOf("\r\n\r\n",StringComparison.Ordinal);if(split>=0)size=long.Parse(text[..split].Split("\r\n").Single(h=>h.StartsWith("Content-Length:",StringComparison.OrdinalIgnoreCase))[15..].Trim());}
            Assert.Contains("/changes ",Encoding.ASCII.GetString(request.GetBuffer(),0,split),StringComparison.Ordinal);var body=Encoding.UTF8.GetBytes(json);await stream.WriteAsync(Encoding.ASCII.GetBytes($"HTTP/1.1 {status} Refused\r\nContent-Type: application/json\r\nContent-Length: {body.Length}\r\nConnection: close\r\n\r\n"),ct);await stream.WriteAsync(body,ct);await stream.FlushAsync(ct);
        },ct);
        var fixture=new WindowsFreshCycleTests.FakeRemote();var config=new WindowsCycleConfiguration("mapping","client","audit","rules-v1",origin,fixture.Device,"test","local-test",fixture.Binding);using var transport=new SaturnGuardedCycleTransport(config,_=>Task.FromResult("Bearer fixture"));var error=await Assert.ThrowsAsync<GuardedCycleRemoteException>(()=>transport.ReadChangesAsync("opaque-original-cursor",0,ct));Assert.Equal((HttpStatusCode)status,error.Status);Assert.Equal(code,error.Code);Assert.Equal(structured,error.StructuredWireCode);await server;Assert.Equal(1,transport.RequestCounts.Values.Sum());Assert.False(listener.Pending());
    }
}
