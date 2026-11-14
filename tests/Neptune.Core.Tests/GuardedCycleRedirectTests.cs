using System.Net;
using System.Net.Sockets;
using System.Text;
using Neptune.Core;
using Xunit;

namespace Neptune.Core.Tests;

public sealed partial class GuardedCycleRedirectTests
{
    [Theory]
    [InlineData(307,"prepare",true)][InlineData(308,"append",true)]
    [InlineData(307,"append",false)][InlineData(308,"prepare",false)]
    public async Task PublicTransportNeverForwardsMutationAuthorizationOrBytesOnRedirect(int status,string command,bool otherAuthority)
    {
        using var source=new TcpListener(IPAddress.Loopback,0);using var destination=new TcpListener(IPAddress.Loopback,0);source.Start();destination.Start();var origin=new Uri("http://127.0.0.1:"+((IPEndPoint)source.LocalEndpoint).Port+"/");var target=otherAuthority?new Uri("http://127.0.0.1:"+((IPEndPoint)destination.LocalEndpoint).Port+"/stolen-route"):new Uri(origin,"another-route");using var deadline=new CancellationTokenSource(TimeSpan.FromSeconds(10));var ct=deadline.Token;
        var accepted=Task.Run(async()=>
        {
            using var client=await source.AcceptTcpClientAsync(ct);await using var stream=client.GetStream();using var request=new MemoryStream();var buffer=new byte[4096];int split=-1;long length=0;
            while(split<0||request.Length<split+4+length)
            {
                var n=await stream.ReadAsync(buffer,ct);if(n==0)throw new IOException("Unexpected request end.");request.Write(buffer,0,n);if(request.Length>64*1024)throw new IOException("Loopback request exceeded bounded fixture.");var text=Encoding.ASCII.GetString(request.GetBuffer(),0,(int)request.Length);split=text.IndexOf("\r\n\r\n",StringComparison.Ordinal);
                if(split>=0){var headers=text[..split].Split("\r\n");length=long.Parse(headers.Single(h=>h.StartsWith("Content-Length:",StringComparison.OrdinalIgnoreCase))[15..].Trim());}
            }
            var raw=request.ToArray();var head=Encoding.ASCII.GetString(raw,0,split);Assert.Contains("Authorization: Bearer local-test-secret",head,StringComparison.Ordinal);Assert.True(length>0);
            if(command=="append"){Assert.Equal(new byte[]{7,8,9},raw.AsSpan(raw.Length-3).ToArray());Assert.Contains("application/vnd.saturn.sync-upload.v1",head,StringComparison.Ordinal);}
            var response=Encoding.ASCII.GetBytes($"HTTP/1.1 {status} Redirect\r\nLocation: {target.AbsoluteUri}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");await stream.WriteAsync(response,ct);await stream.FlushAsync(ct);return raw.Length;
        },ct);
        var fixture=new WindowsFreshCycleTests.FakeRemote();var config=new WindowsCycleConfiguration("mapping","client","audit","rules-v1",origin,fixture.Device,"test","local-test",fixture.Binding);
        var input=new CycleUploadInput(fixture.Binding.ServerEpoch,fixture.Binding.BindingVersion,"local-test",new("saturn.upload-intent.v2","windows_sync",fixture.Device,fixture.Binding.RootId,"/file","neptune-cycle-"+Guid.NewGuid().ToString("D"),fixture.Binding.RootId,"file",null,"3",new string('a',64),null,"*",true,null,new("absent"),[new(fixture.Binding.RootId,null,"root")]));
        using var transport=new SaturnGuardedCycleTransport(config,_=>Task.FromResult("Bearer local-test-secret"));
        var failure=await Assert.ThrowsAsync<GuardedCycleRemoteException>(()=>command=="prepare"?transport.UploadAsync("prepare",input,null,new string('b',64),ct):(Task)transport.AppendAsync(input,Guid.NewGuid().ToString("D"),new string('b',64),0,new byte[]{7,8,9},ct));
        Assert.Equal((HttpStatusCode)status,failure.Status);Assert.Equal("guarded_redirect_refused",failure.Code);Assert.True(await accepted>0);Assert.False(source.Pending());Assert.False(destination.Pending());Assert.Equal(1,transport.RequestCounts.Values.Sum());
    }
}
