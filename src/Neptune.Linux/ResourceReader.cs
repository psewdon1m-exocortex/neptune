using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Neptune.Core;

namespace Neptune.Linux;

public sealed class ResourceReader(LinuxOptions options, IHttpClientFactory clients)
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _streams = new(StringComparer.Ordinal);

    public async Task TransferAsync(HttpContext context, ProjectRegistration project, string operation)
    {
        var reader = project.Reader ?? throw new BadHttpRequestException("Reader is not configured.", 403);
        string path;
        int limit;
        try
        {
            ResourceReaderContract.Purpose(context.Request.Headers["X-Neptune-Purpose"].ToString());
            if (context.Request.Query["path"].Count != 1 || context.Request.Query["limit"].Count > 1
                || context.Request.Query["cursor"].Count > 1) throw new ArgumentException("Repeated reader parameter.");
            path = ResourceReaderContract.Path(context.Request.Query["path"].ToString(), reader.Root);
            var parsed = 100;
            if (context.Request.Query.ContainsKey("limit") && !int.TryParse(context.Request.Query["limit"], out parsed))
                throw new ArgumentException("Invalid page size.");
            limit = ResourceReaderContract.Page(parsed);
            if (operation == "content") ResourceReaderContract.Range(context.Request.Headers.Range.FirstOrDefault());
        }
        catch (UnauthorizedAccessException) { throw new BadHttpRequestException("Reader scope is not permitted.", 403); }
        catch (ArgumentException)
        {
            throw new BadHttpRequestException("Reader request is invalid.", operation == "content" && context.Request.Headers.ContainsKey("Range") ? 416 : 422);
        }
        var cursor = context.Request.Query["cursor"].ToString();
        if (cursor.Length > 2048 || cursor.Any(char.IsControl)) throw new BadHttpRequestException("Reader cursor is invalid.", 422);
        var slots = _streams.GetOrAdd(project.ProjectId, _ => new SemaphoreSlim(4));
        if (!await slots.WaitAsync(0, context.RequestAborted)) throw new BadHttpRequestException("Reader capacity is busy.", 429);
        try
        {
            using var headerDeadline = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);
            headerDeadline.CancelAfter(TimeSpan.FromSeconds(15));
            var client = clients.CreateClient("resource-reader");
            var token = await CredentialFile.ReadAsync(options.KernelTokenFile, headerDeadline.Token);
            var register = new KernelRegisterClient(client, Path.Combine(options.StateDirectory, "reader-register.json"));
            using var snapshot = await register.GetSnapshotAsync(options.KernelOrigin, token.Trim(), headerDeadline.Token,
                ["services.saturn.sni", "services.saturn.port"]);
            var origin = RegisterValues.HttpsOrigin(snapshot.RootElement.GetProperty("values"), "saturn");
            var route = operation == "list" ? "resources" : "resource-" + operation;
            var relative = "/api/v1/neptune-reader/" + route + "?path=" + Uri.EscapeDataString(path)
                + "&limit=" + limit + (cursor.Length == 0 ? "" : "&cursor=" + Uri.EscapeDataString(cursor));
            using var request = new HttpRequestMessage(HttpMethod.Get, new Uri(origin, relative));
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer",
                await CredentialFile.ReadAsync(reader.SaturnTokenFile, headerDeadline.Token));
            if (operation == "content")
            {
                if (context.Request.Headers.Range.Count > 0) request.Headers.TryAddWithoutValidation("Range", context.Request.Headers.Range.ToString());
                var condition = context.Request.Headers.IfRange.ToString();
                if (condition.Length > 512 || condition.Any(char.IsControl)) throw new BadHttpRequestException("If-Range is invalid.", 422);
                if (condition.Length > 0) request.Headers.TryAddWithoutValidation("If-Range", condition);
            }
            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, headerDeadline.Token);
            var status = (int)response.StatusCode;
            if (status is not (200 or 206))
            {
                context.Response.StatusCode = status is 403 or 404 or 409 or 412 or 416 or 422 or 429 ? status : 503;
                if (response.Content.Headers.ContentRange is not null)
                    context.Response.Headers.ContentRange = response.Content.Headers.ContentRange.ToString();
                return;
            }
            if (operation != "content")
            {
                if (response.Content.Headers.ContentLength > ResourceReaderContract.MaximumMetadataBytes)
                    throw new InvalidDataException("Reader metadata is too large.");
                await using var stream = await response.Content.ReadAsStreamAsync(headerDeadline.Token);
                using var output = new MemoryStream();
                var chunk = new byte[64 * 1024];
                while (true)
                {
                    var count = await stream.ReadAsync(chunk, headerDeadline.Token);
                    if (count == 0) break;
                    if (output.Length + count > ResourceReaderContract.MaximumMetadataBytes)
                        throw new InvalidDataException("Reader metadata is too large.");
                    output.Write(chunk, 0, count);
                }
                using var data = JsonDocument.Parse(output.ToArray());
                try { ValidateMetadata(data.RootElement, path, reader.Root, operation == "list", limit); }
                catch (Exception error) when (error is ArgumentException or UnauthorizedAccessException or KeyNotFoundException or InvalidOperationException or FormatException or OverflowException)
                { throw new InvalidDataException("Reader metadata does not match its contract."); }
                context.Response.ContentType = "application/json";
                context.Response.ContentLength = output.Length;
                output.Position = 0;
                await output.CopyToAsync(context.Response.Body, context.RequestAborted);
                return;
            }
            if (response.Content.Headers.ContentLength is not long length || length < 0
                || response.Headers.ETag is null || status == 206 && response.Content.Headers.ContentRange is null)
                throw new InvalidDataException("Reader content headers are incomplete.");
            context.Response.StatusCode = status;
            headerDeadline.CancelAfter(Timeout.InfiniteTimeSpan);
            context.Response.ContentType = response.Content.Headers.ContentType?.ToString() ?? "application/octet-stream";
            context.Response.ContentLength = length;
            context.Response.Headers.ETag = response.Headers.ETag.ToString();
            context.Response.Headers.AcceptRanges = "bytes";
            context.Response.Headers.ContentDisposition = "attachment";
            context.Response.Headers["X-Content-Type-Options"] = "nosniff";
            if (response.Content.Headers.ContentRange is not null)
                context.Response.Headers.ContentRange = response.Content.Headers.ContentRange.ToString();
            await using var input = await response.Content.ReadAsStreamAsync(context.RequestAborted);
            var buffer = ArrayPool<byte>.Shared.Rent(ResourceReaderContract.StreamBufferBytes);
            try
            {
                long transferred = 0;
                using var transferDeadline = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);
                transferDeadline.CancelAfter(TimeSpan.FromHours(1));
                using var progress = CancellationTokenSource.CreateLinkedTokenSource(transferDeadline.Token);
                while (true)
                {
                    progress.CancelAfter(TimeSpan.FromSeconds(60));
                    var count = await input.ReadAsync(buffer.AsMemory(0, ResourceReaderContract.StreamBufferBytes), progress.Token);
                    if (count == 0) break;
                    transferred += count;
                    if (transferred > length) throw new InvalidDataException("Resource length changed during transfer.");
                    progress.CancelAfter(TimeSpan.FromSeconds(60));
                    await context.Response.Body.WriteAsync(buffer.AsMemory(0, count), progress.Token);
                }
                if (transferred != length) throw new InvalidDataException("Resource transfer ended early.");
            }
            finally { ArrayPool<byte>.Shared.Return(buffer, clearArray: true); }
        }
        catch (Exception error) when (error is HttpRequestException or IOException or JsonException or OperationCanceledException)
        {
            if (context.Response.HasStarted || context.RequestAborted.IsCancellationRequested) context.Abort();
            else context.Response.StatusCode = 503;
        }
        finally { slots.Release(); }
    }

    private static void ValidateMetadata(JsonElement data, string requested, string root, bool listing, int limit)
    {
        void Entry(JsonElement item, bool child)
        {
            var path = item.GetProperty("path").GetString() ?? "";
            ResourceReaderContract.Path(path, root);
            if (child ? !path.StartsWith(requested + "/", StringComparison.Ordinal)
                || path[(requested.Length + 1)..].Contains('/') : path != requested)
                throw new InvalidDataException("Reader metadata escaped the requested directory.");
            if (item.GetProperty("type").GetString() is not ("file" or "folder")
                || item.GetProperty("size_bytes").GetInt64() < 0
                || (item.GetProperty("etag").GetString()?.Length ?? 0) is 0 or > 512)
                throw new InvalidDataException("Reader metadata is invalid.");
        }
        if (!listing) { Entry(data, false); return; }
        var entries = data.GetProperty("entries");
        if (entries.ValueKind != JsonValueKind.Array || entries.GetArrayLength() > limit)
            throw new InvalidDataException("Reader listing exceeds its page limit.");
        foreach (var entry in entries.EnumerateArray()) Entry(entry, true);
        if (data.TryGetProperty("next_cursor", out var cursor) && cursor.ValueKind != JsonValueKind.Null
            && (cursor.ValueKind != JsonValueKind.String || cursor.GetString() is not string next
                || next.Length > 2048 || next.Any(char.IsControl)))
            throw new InvalidDataException("Reader cursor is invalid.");
    }
}
