using System.Text.Json;

namespace Neptune.Core;

public static class BoundedJson
{
    public static async Task<JsonDocument> ReadAsync(HttpContent content, int maximumBytes, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(15));
        cancellationToken = deadline.Token;
        if (content.Headers.ContentLength is > 0 && content.Headers.ContentLength > maximumBytes || content.Headers.ContentEncoding.Count != 0)
            throw new InvalidDataException("Dependency metadata exceeds its limit or uses an unsupported encoding.");
        await using var input = await content.ReadAsStreamAsync(cancellationToken);
        using var output = new MemoryStream();
        var buffer = new byte[16 * 1024];
        while (true)
        {
            var count = await input.ReadAsync(buffer, cancellationToken);
            if (count == 0) break;
            if (output.Length + count > maximumBytes) throw new InvalidDataException("Dependency metadata exceeds its limit.");
            output.Write(buffer, 0, count);
        }
        return JsonDocument.Parse(output.ToArray(), new JsonDocumentOptions { MaxDepth = 64 });
    }
}
