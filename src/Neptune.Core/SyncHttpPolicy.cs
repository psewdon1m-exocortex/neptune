using System.Net;

namespace Neptune.Core;

// A response owns its deadline until its body is consumed/disposed. No request
// is retried here: a timed out write may already have committed at Saturn.
public static class SyncHttpPolicy
{
    public static readonly TimeSpan ControlTimeout = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan LongPollTimeout = TimeSpan.FromSeconds(45);
    public static HttpClient CreateClient() => new(new SocketsHttpHandler
    {
        AllowAutoRedirect = false, ConnectTimeout = TimeSpan.FromSeconds(10),
        PooledConnectionLifetime = TimeSpan.FromMinutes(2), PooledConnectionIdleTimeout = TimeSpan.FromSeconds(30),
        MaxConnectionsPerServer = 8, AutomaticDecompression = DecompressionMethods.None
    }) { Timeout = Timeout.InfiniteTimeSpan };

    public static async Task<HttpResponseMessage> SendAsync(HttpClient http, HttpRequestMessage request,
        HttpCompletionOption completion, CancellationToken ct, TimeSpan? controlTimeout = null,
        TimeSpan? idleTimeout = null, bool longPoll = false)
    {
        var media = request.Content?.Headers.ContentType?.MediaType;
        var transfer = request.Content is not null && (request.Method == HttpMethod.Put || request.Method == HttpMethod.Patch
            || media is "multipart/related" or "application/octet-stream" or "application/vnd.saturn.sync-stream.v1" or "application/vnd.saturn.sync-upload.v1");
        var idle = idleTimeout ?? (longPoll ? LongPollTimeout : IdleTimeout);
        var scope = new RequestLifetime(ct, transfer ? null : controlTimeout ?? (longPoll ? LongPollTimeout : ControlTimeout), idle);
        HttpResponseMessage? response = null;
        try
        {
            if (request.Content is { } content) request.Content = new RequestContent(content, scope);
            response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, scope.Token);
            scope.Progress();
            response.Content = new ResponseContent(response.Content, scope);
            if (completion == HttpCompletionOption.ResponseContentRead)
                await response.Content.LoadIntoBufferAsync(scope.Token);
            return response;
        }
        catch (Exception error)
        {
            var translated = scope.Translate(error);
            response?.Dispose(); scope.Dispose();
            if (!ReferenceEquals(error, translated)) throw translated;
            throw;
        }
    }

    private sealed class RequestLifetime : IDisposable
    {
        private readonly CancellationToken _caller;
        private readonly CancellationTokenSource _idle = new();
        private readonly CancellationTokenSource _total = new();
        private readonly CancellationTokenSource _linked;
        private readonly TimeSpan _idleDuration;
        private readonly object _gate = new();
        private bool _disposed;
        public RequestLifetime(CancellationToken caller, TimeSpan? total, TimeSpan idle)
        {
            _caller = caller; _idleDuration = idle;
            _linked = CancellationTokenSource.CreateLinkedTokenSource(caller, _idle.Token, _total.Token);
            if (total is { } duration) _total.CancelAfter(duration);
            _idle.CancelAfter(idle);
        }
        public CancellationToken Token => _linked.Token;
        public void Progress() { lock (_gate) if (!_disposed && !_idle.IsCancellationRequested) _idle.CancelAfter(_idleDuration); }
        public Exception Translate(Exception error) => !_caller.IsCancellationRequested && (_idle.IsCancellationRequested || _total.IsCancellationRequested)
            ? new TimeoutException(_total.IsCancellationRequested ? "Network control request deadline exceeded; original operation retained." : "Network transfer made no progress; original operation retained.", error) : error;
        public void Dispose()
        {
            lock (_gate) { if (_disposed) return; _disposed = true; _linked.Dispose(); _idle.Dispose(); _total.Dispose(); }
        }
    }

    private sealed class RequestContent : HttpContent
    {
        private readonly HttpContent _inner; private readonly RequestLifetime _scope;
        public RequestContent(HttpContent inner, RequestLifetime scope)
        { _inner = inner; _scope = scope; foreach (var header in inner.Headers) Headers.TryAddWithoutValidation(header.Key, header.Value); }
        protected override bool TryComputeLength(out long length) { length = _inner.Headers.ContentLength ?? -1; return length >= 0; }
        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) => SerializeToStreamAsync(stream, context, _scope.Token);
        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken ct)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _scope.Token);
            using var progress = new ProgressStream(stream, _scope, leaveOpen: true);
            await _inner.CopyToAsync(progress, context, linked.Token);
        }
        protected override void Dispose(bool disposing) { if (disposing) _inner.Dispose(); base.Dispose(disposing); }
    }

    private sealed class ResponseContent : HttpContent
    {
        private readonly HttpContent _inner; private readonly RequestLifetime _scope;
        public ResponseContent(HttpContent inner, RequestLifetime scope)
        { _inner = inner; _scope = scope; foreach (var header in inner.Headers) Headers.TryAddWithoutValidation(header.Key, header.Value); }
        protected override bool TryComputeLength(out long length) { length = _inner.Headers.ContentLength ?? -1; return length >= 0; }
        protected override Task<Stream> CreateContentReadStreamAsync() => CreateContentReadStreamAsync(_scope.Token);
        protected override async Task<Stream> CreateContentReadStreamAsync(CancellationToken ct)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _scope.Token);
            try { return new ProgressStream(await _inner.ReadAsStreamAsync(linked.Token), _scope, leaveOpen: false); }
            catch (Exception error) { throw _scope.Translate(error); }
        }
        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) => SerializeToStreamAsync(stream, context, _scope.Token);
        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken ct)
        { await using var input = await CreateContentReadStreamAsync(ct); await input.CopyToAsync(stream, ct); }
        protected override void Dispose(bool disposing)
        { if (disposing) { _inner.Dispose(); _scope.Dispose(); } base.Dispose(disposing); }
    }

    private sealed class ProgressStream(Stream inner, RequestLifetime scope, bool leaveOpen) : Stream
    {
        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => inner.CanWrite;
        public override long Length => inner.Length;
        public override long Position { get => inner.Position; set => throw new NotSupportedException(); }
        public override void Flush() => inner.Flush();
        public override Task FlushAsync(CancellationToken ct) => inner.FlushAsync(ct);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override int Read(byte[] buffer, int offset, int count) => ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct) => ReadAsync(buffer.AsMemory(offset, count), ct).AsTask();
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, scope.Token);
            try { var count = await inner.ReadAsync(buffer, linked.Token); if (count > 0) scope.Progress(); return count; }
            catch (Exception error) { throw scope.Translate(error); }
        }
        public override void Write(byte[] buffer, int offset, int count) => WriteAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct) => WriteAsync(buffer.AsMemory(offset, count), ct).AsTask();
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, scope.Token);
            try
            {
                while (!buffer.IsEmpty)
                { var size = Math.Min(buffer.Length, 16 * 1024); await inner.WriteAsync(buffer[..size], linked.Token); scope.Progress(); buffer = buffer[size..]; }
            }
            catch (Exception error) { throw scope.Translate(error); }
        }
        protected override void Dispose(bool disposing) { if (disposing && !leaveOpen) inner.Dispose(); base.Dispose(disposing); }
    }
}
