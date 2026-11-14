using System.Net;

namespace Neptune.Core;

// Only explicitly non-admitted requests can be replayed here. Connection loss,
// timeout, UNKNOWN and preconditions stay with the durable original recovery.
internal sealed class SyncRetryController
{
    private long _notBefore;
    private int _failures;
    internal async Task<T> ExecuteAsync<T>(Func<Task<T>> request,SyncDiagnostics diagnostics,CancellationToken ct)
    {
        for(var attempt=0;;attempt++)
        {
            while(Interlocked.Read(ref _notBefore)-Environment.TickCount64 is var remaining&&remaining>0)await Task.Delay(TimeSpan.FromMilliseconds(remaining),ct);
            ct.ThrowIfCancellationRequested();
            try{var result=await request();Interlocked.Exchange(ref _failures,0);diagnostics.ClearWait();return result;}
            catch(GuardedCycleRemoteException e)when(IsNotAdmitted(e)&&attempt<7)
            {
                var failures=Math.Min(10,Interlocked.Increment(ref _failures));
                var delay=Math.Max(e.RetryAfter?.TotalMilliseconds??0,Random.Shared.NextDouble()*Math.Min(60_000,1000*(1<<Math.Min(failures-1,6))));
                if(delay>120_000)throw; // Long server deferrals return to durable scheduling.
                var until=checked(Environment.TickCount64+(long)delay);
                long before;do{before=Interlocked.Read(ref _notBefore);if(before>=until)break;}while(Interlocked.CompareExchange(ref _notBefore,until,before)!=before);
                diagnostics.Retry(e.Code);
            }
        }
    }
    private static bool IsNotAdmitted(GuardedCycleRemoteException e)=>e.StructuredWireCode&&
        (e.Status==HttpStatusCode.TooManyRequests&&e.Code is "admission_full" or "capacity"
        ||e.Status==HttpStatusCode.ServiceUnavailable&&e.Code is "backend_busy" or "backend_draining");
    internal static SyncRetryReason? Classify(Exception error)
    {
        if(error is HttpIOException{HttpRequestError:HttpRequestError.ResponseEnded or HttpRequestError.ConnectionError}
            ||error is IOException{InnerException:System.Net.Sockets.SocketException})return new("WAIT_CONNECTIVITY","response_body_interrupted");
        if(error is GuardedCycleRemoteException remote)
        {
            if(remote.Status==HttpStatusCode.ProxyAuthenticationRequired)return new("AUTH_REQUIRED","proxy_authorization");
            if(remote.Status is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)return new("AUTH_REQUIRED","server_authorization");
            if(IsNotAdmitted(remote))return new("RETRY_WAIT","admission",remote.RetryAfter);
            if(remote.Status==HttpStatusCode.RequestTimeout||(int)remote.Status>=500)return new("WAIT_CONNECTIVITY","remote_unavailable",remote.RetryAfter);
            if(remote.Status==HttpStatusCode.TooManyRequests)return new("RETRY_WAIT","rate_limit",remote.RetryAfter);
            return null;
        }
        if(error is System.Net.Http.HttpRequestException http)
        {
            if(http.StatusCode==HttpStatusCode.ProxyAuthenticationRequired)return new("AUTH_REQUIRED","proxy_authorization");
            if(http.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)return new("AUTH_REQUIRED","server_authorization");
            if(http.StatusCode is {} status&&status!=HttpStatusCode.RequestTimeout&&status!=HttpStatusCode.TooManyRequests&&(int)status<500)return null;
            if(http.HttpRequestError==System.Net.Http.HttpRequestError.SecureConnectionError)return new("AUTH_REQUIRED","tls");
            if(http.HttpRequestError is System.Net.Http.HttpRequestError.InvalidResponse or System.Net.Http.HttpRequestError.HttpProtocolError)return null;
            return new("WAIT_CONNECTIVITY",http.HttpRequestError.ToString());
        }
        if(error is TimeoutException or TaskCanceledException or NetworkUnavailableException)return new("WAIT_CONNECTIVITY","transport");
        if(error is RootMutationDeferredException)return new("RETRY_WAIT","root_original_unresolved");
        if(error is BootstrapCaptureDeferredException)return new("SOURCE_BUSY","source_or_original_unresolved");
        if(error is CycleSpoolAdmissionException)return new("RETRY_WAIT","resource_pressure");
        if(error is IOException&&((error.HResult&0xffff)is 32 or 33))return new("SOURCE_BUSY","sharing_violation");
        if(error is System.ComponentModel.Win32Exception native&&native.NativeErrorCode is 32 or 33)return new("SOURCE_BUSY","sharing_violation");
        return null;
    }
}
