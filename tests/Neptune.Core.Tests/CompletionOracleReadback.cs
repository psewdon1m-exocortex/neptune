using System.Net.Http.Json;
using System.Text.Json;
using System.Globalization;

namespace Neptune.Core.Tests;

internal static class CompletionOracleReadback
{
    internal static async Task<JsonElement> ReadMetadataAsync(HttpClient http, Uri origin,
        string kind, string folder, CancellationToken cancellationToken,
        TimeSpan? attemptTimeout = null, int attempts = 3,
        Func<int, string, Task>? onRetry = null)
        => await ReadAsync(http, origin, "__completion/readback", new { kind, folder, metadataOnly = true },
            cancellationToken, attemptTimeout ?? TimeSpan.FromMinutes(2), attempts, onRetry, "metadata", null);

    internal sealed record Progress(int Files, long Bytes, int Folders, bool Found);

    internal static Task<JsonElement> ReadPhysicalAsync(HttpClient http, Uri origin, string kind, string folder,
        CancellationToken cancellationToken, TimeSpan timeout) =>
        ReadAsync(http, origin, "__completion/readback", new { kind, folder, metadataOnly = false }, cancellationToken,
            timeout, 1, null, "physical", null);

    internal static async Task<Progress> ReadProgressAsync(HttpClient http, Uri origin, string kind, string folder,
        string rootId, CancellationToken cancellationToken, TimeSpan? attemptTimeout = null, int attempts = 3,
        Func<int, string, Task>? onRetry = null)
    {
        var snapshot = await ReadAsync(http, origin, "__completion/progress", new { kind, folder }, cancellationToken,
            attemptTimeout ?? TimeSpan.FromSeconds(15), attempts, onRetry, "aggregate", 4096);
        if (snapshot.GetProperty("schema").GetString() != "saturn.completion-progress.v1"
            || snapshot.GetProperty("rootId").GetString() != rootId || snapshot.GetProperty("folder").GetString() != folder)
            throw new InvalidDataException("Aggregate oracle scope/schema mismatch");
        var files = int.Parse(snapshot.GetProperty("files").GetString()!, NumberStyles.None, CultureInfo.InvariantCulture);
        var folders = int.Parse(snapshot.GetProperty("folders").GetString()!, NumberStyles.None, CultureInfo.InvariantCulture);
        var bytes = long.Parse(snapshot.GetProperty("bytes").GetString()!, NumberStyles.None, CultureInfo.InvariantCulture);
        var found = snapshot.GetProperty("found").GetBoolean();
        if ((!found && (files != 0 || folders != 0 || bytes != 0)) || (found && folders == 0))
            throw new InvalidDataException("Aggregate oracle inconsistent counts");
        return new Progress(files, bytes, folders, found);
    }

    private static async Task<JsonElement> ReadAsync(HttpClient http, Uri origin, string endpoint, object body,
        CancellationToken cancellationToken, TimeSpan timeout, int attempts, Func<int, string, Task>? onRetry,
        string phase, int? maximumBytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(attempts, 1);
        if (timeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeout));
        for (var attempt = 1; ; attempt++)
        {
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            deadline.CancelAfter(timeout);
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, new Uri(origin, endpoint)) { Content = JsonContent.Create(body) };
                using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, deadline.Token);
                response.EnsureSuccessStatusCode();
                using var stream = await response.Content.ReadAsStreamAsync(deadline.Token);
                if (maximumBytes is { } limit)
                {
                    var bytes = new byte[limit + 1];
                    var count = 0;
                    while (count <= limit)
                    {
                        var read = await stream.ReadAsync(bytes.AsMemory(count), deadline.Token);
                        if (read == 0) break;
                        count += read;
                    }
                    if (count > limit) throw new InvalidDataException("Aggregate oracle response too large");
                    using var bounded = JsonDocument.Parse(bytes.AsMemory(0, count));
                    return bounded.RootElement.Clone();
                }
                using var result = await JsonDocument.ParseAsync(
                    stream, cancellationToken: deadline.Token);
                return result.RootElement.Clone();
            }
            catch (OperationCanceledException error) when (!cancellationToken.IsCancellationRequested && deadline.IsCancellationRequested)
            {
                if (attempt == attempts) throw new TimeoutException($"{phase} oracle did not respond in {attempts} bounded attempts.", error);
                if (onRetry is not null) await onRetry(attempt, phase + "_oracle_timeout");
            }
            catch (HttpRequestException error) when (!cancellationToken.IsCancellationRequested && error.StatusCode is null && attempt < attempts)
            {
                if (onRetry is not null) await onRetry(attempt, phase + "_oracle_transport_failure");
            }
            catch (IOException) when (!cancellationToken.IsCancellationRequested && attempt < attempts)
            {
                // With ResponseHeadersRead an interrupted body surfaces from the
                // stream (HttpIOException), after SendAsync has already completed.
                if (onRetry is not null) await onRetry(attempt, phase + "_oracle_transport_failure");
            }
            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }
    }
}
