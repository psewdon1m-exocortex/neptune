using System.Net;
using System.Net.Http;

namespace Neptune.Core;

public static class NetworkFailure
{
    public static TimeSpan? RetryAfter(HttpResponseMessage response)
    {
        var delay = response.Headers.RetryAfter?.Delta;
        if (delay is null && response.Headers.RetryAfter?.Date is { } date) delay = date - DateTimeOffset.UtcNow;
        return delay < TimeSpan.Zero ? TimeSpan.Zero : delay;
    }
    public static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken ct)
    {
        if (response.IsSuccessStatusCode) return;
        var failure = new GuardedCycleRemoteException(response.StatusCode, "http_" + (int)response.StatusCode) { RetryAfter = RetryAfter(response) };
        // Consume only a bounded diagnostic body. A broken error body cannot
        // erase the status/Retry-After already received from the route.
        try
        {
            await using var body = await response.Content.ReadAsStreamAsync(ct);
            var buffer = new byte[4096]; var remaining = buffer.Length;
            while (remaining > 0) { var n = await body.ReadAsync(buffer.AsMemory(0, remaining), ct); if (n == 0) break; remaining -= n; }
        }
        catch (Exception e) when (!ct.IsCancellationRequested && e is IOException or HttpRequestException or TimeoutException or OperationCanceledException) { }
        ct.ThrowIfCancellationRequested();
        throw failure;
    }
    public static Task EnsureLegacySuccessAsync(HttpResponseMessage response, CancellationToken ct)
    {
        if (response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden or HttpStatusCode.ProxyAuthenticationRequired
            or HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests || (int)response.StatusCode >= 500)
            return EnsureSuccessAsync(response, ct);
        response.EnsureSuccessStatusCode();
        return Task.CompletedTask;
    }
    public static string? State(Exception error)
    {
        if (error is HttpIOException { HttpRequestError: HttpRequestError.ResponseEnded or HttpRequestError.ConnectionError }
            || error is IOException { InnerException: System.Net.Sockets.SocketException }) return "Waiting for network";
        var status = error is GuardedCycleRemoteException guarded ? guarded.Status : (error as HttpRequestException)?.StatusCode;
        if (status == HttpStatusCode.ProxyAuthenticationRequired) return "Proxy authorization required";
        if (status is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden) return "Server authorization required";
        if (status is { } code && code != HttpStatusCode.RequestTimeout && code != HttpStatusCode.TooManyRequests && (int)code < 500) return null;
        if (error is HttpRequestException http)
            return http.HttpRequestError switch
            {
                HttpRequestError.SecureConnectionError => "TLS connection requires attention",
                HttpRequestError.ProxyTunnelError => "Proxy connection requires attention",
                HttpRequestError.NameResolutionError => "Waiting for network (DNS)",
                HttpRequestError.InvalidResponse or HttpRequestError.HttpProtocolError => null,
                _ => "Waiting for network"
            };
        if (error is TimeoutException or OperationCanceledException or NetworkUnavailableException
            || status == HttpStatusCode.RequestTimeout || status == HttpStatusCode.TooManyRequests || (int?)status >= 500)
            return "Waiting for network";
        return null;
    }
}
