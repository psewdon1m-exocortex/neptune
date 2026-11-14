using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

// Availability of this adapter does not enable batching automatically. The
// engine must separately qualify the enrolled server and economic policy.
public interface ISmallFileBatchRemote
{
    SyncRootBinding Binding { get; }
    Task<SmallFileBatchCapability?> ReadSmallBatchCapabilitiesAsync(CancellationToken ct);
    Task<SmallFileBatchCapability?> ReadLegacySmallBatchCapabilitiesAsync(CancellationToken ct)
        => Task.FromResult<SmallFileBatchCapability?>(null);
    Task<SmallFileBatchOutcome> PrepareSmallBatchAsync(SmallFileBatchPlan plan, CancellationToken ct);
    Task<SmallFileBatchOutcome> ReadSmallBatchAsync(SmallFileBatchPlan plan, CancellationToken ct);
    Task<SmallFileBatchOutcome> SendSmallBatchAsync(SmallFileBatchPlan plan, Stream sealedAggregate, CancellationToken ct);
    Task<SmallFileBatchOutcome> ExecuteSmallJobAsync(SmallFileBatchPlan plan, Stream sealedAggregate, CancellationToken ct);
}

public sealed partial class SaturnGuardedCycleTransport : ISmallFileBatchRemote
{
    public async Task<SmallFileBatchCapability?> ReadLegacySmallBatchCapabilitiesAsync(CancellationToken ct)
    {
        byte[] raw;
        try { raw = await Send("small-batches/capabilities", HttpMethod.Get, new ByteArrayContent([]), 16 * 1024, ct); }
        catch (GuardedCycleRemoteException error) when (error.Status == HttpStatusCode.NotFound) { return null; }
        var capability = SmallFileBatchCapability.Parse(raw, _configuration);
        if (capability.ExecutionJob) throw new InvalidDataException("Legacy batch probe returned another protocol.");
        return capability;
    }
    public async Task<SmallFileBatchCapability?> ReadSmallBatchCapabilitiesAsync(CancellationToken ct)
    {
        byte[] raw;
        try
        {
            raw = await Send("small-jobs/single-request-capabilities", HttpMethod.Get, new ByteArrayContent([]), 16 * 1024, ct);
            var single = SmallFileBatchCapability.Parse(raw, _configuration);
            if (!single.SingleRequest) throw new InvalidDataException("Single request probe returned another protocol.");
            return single;
        }
        catch (GuardedCycleRemoteException error) when (error.Status == HttpStatusCode.NotFound) { }
        try
        {
            raw = await Send("small-jobs/pipeline-capabilities", HttpMethod.Get, new ByteArrayContent([]), 16 * 1024, ct);
            var pipeline = SmallFileBatchCapability.Parse(raw, _configuration);
            if (!pipeline.ExecutionJob || pipeline.ParallelJobs != 2) throw new InvalidDataException("Pipeline probe returned another protocol.");
            return pipeline;
        }
        catch (GuardedCycleRemoteException error) when (error.Status == HttpStatusCode.NotFound) { }
        try
        {
            raw = await Send("small-jobs/capabilities", HttpMethod.Get, new ByteArrayContent([]), 16 * 1024, ct);
            var job = SmallFileBatchCapability.Parse(raw, _configuration);
            if (!job.ExecutionJob) throw new InvalidDataException("Execution-job probe returned another protocol.");
            return job;
        }
        catch (GuardedCycleRemoteException error) when (error.Status == HttpStatusCode.NotFound) { }
        try { raw = await Send("small-batches/capabilities", HttpMethod.Get, new ByteArrayContent([]), 16 * 1024, ct); }
        catch (GuardedCycleRemoteException error) when (error.Status == HttpStatusCode.NotFound) { return null; }
        return SmallFileBatchCapability.Parse(raw, _configuration);
    }
    public async Task<SmallFileBatchOutcome> PrepareSmallBatchAsync(SmallFileBatchPlan plan, CancellationToken ct)
        => await BatchJson("prepare", plan, plan.EncodePlan(), ct);
    public async Task<SmallFileBatchOutcome> ReadSmallBatchAsync(SmallFileBatchPlan plan, CancellationToken ct)
        => await BatchJson("status", plan, plan.ExecutionJob ? plan.EncodePlan() : plan.EncodeStatusRequest(), ct);
    private async Task<SmallFileBatchOutcome> BatchJson(string command, SmallFileBatchPlan plan, byte[] bytes, CancellationToken ct)
    {
        RequireBatchBinding(plan); using var content = new ByteArrayContent(bytes); content.Headers.ContentType = new("application/json");
        return plan.ReadOutcome(await Send((plan.ExecutionJob ? "small-jobs/" : "small-batches/") + command, HttpMethod.Post, content, SmallFileBatchPlan.MaximumMetadataBytes, ct));
    }
    public Task<SmallFileBatchOutcome> SendSmallBatchAsync(SmallFileBatchPlan plan, Stream sealedAggregate, CancellationToken ct)
        => SendSmallBatch(plan, sealedAggregate, false, ct);
    public Task<SmallFileBatchOutcome> ExecuteSmallJobAsync(SmallFileBatchPlan plan, Stream sealedAggregate, CancellationToken ct)
        => plan.ExecutionJob ? SendSmallBatch(plan, sealedAggregate, true, ct) : throw new InvalidDataException("Single request requires an execution job.");
    private async Task<SmallFileBatchOutcome> SendSmallBatch(SmallFileBatchPlan plan, Stream sealedAggregate, bool singleRequest, CancellationToken ct)
    {
        RequireBatchBinding(plan);
        if (!sealedAggregate.CanRead || !sealedAggregate.CanSeek || sealedAggregate.Position != 0 || sealedAggregate.Length != plan.DataBytes)
            throw new InvalidDataException("Batch transport requires the exact seekable sealed aggregate at offset zero.");
        using var content = new SmallBatchContent(plan, sealedAggregate);
        return plan.ReadOutcome(await Send(singleRequest ? "small-jobs/execute" : plan.ExecutionJob ? "small-jobs/data" : "small-batches/data", HttpMethod.Post, content, SmallFileBatchPlan.MaximumMetadataBytes, ct));
    }
    private void RequireBatchBinding(SmallFileBatchPlan plan)
    { if (plan.Binding != Binding) throw new InvalidDataException("Batch transport cannot rebind the enrolled root."); }

    private sealed class SmallBatchContent : HttpContent
    {
        private readonly SmallFileBatchPlan _plan; private readonly Stream _source;
        private readonly byte[] _manifest, _opening, _closing; private readonly byte[][] _headers;
        private readonly long _length; private int _started;
        internal SmallBatchContent(SmallFileBatchPlan plan, Stream source)
        {
            (_plan, _source) = (plan, source); _manifest = plan.ExecutionJob ? plan.EncodePlan() : plan.EncodeManifest();
            var boundary = "neptune_" + Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(24));
            Headers.ContentType = new MediaTypeHeaderValue("multipart/related"); Headers.ContentType.Parameters.Add(new("boundary", boundary));
            _opening = Encoding.ASCII.GetBytes("--" + boundary + "\r\nContent-Type: application/json\r\nContent-Length: " + _manifest.Length.ToString(CultureInfo.InvariantCulture) + "\r\n\r\n");
            _headers = plan.Entries.Select(e => Encoding.ASCII.GetBytes("\r\n--" + boundary + "\r\nContent-Type: application/octet-stream\r\nContent-ID: <" + e.Original.Intent.IdempotencyKey
                + ">\r\nContent-Length: " + e.Original.Intent.SizeBytes + "\r\n\r\n")).ToArray();
            _closing = Encoding.ASCII.GetBytes("\r\n--" + boundary + "--\r\n");
            _length = checked(_opening.Length + (long)_manifest.Length + _headers.Sum(h => (long)h.Length) + plan.DataBytes + _closing.Length);
        }
        protected override bool TryComputeLength(out long length) { length = _length; return true; }
        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) => Serialize(stream, CancellationToken.None);
        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken ct) => Serialize(stream, ct);
        private async Task Serialize(Stream output, CancellationToken ct)
        {
            if (Interlocked.Exchange(ref _started, 1) != 0 || _source.Position != 0 || _source.Length != _plan.DataBytes)
                throw new InvalidDataException("Implicit transport retries cannot rewind an attempted aggregate.");
            await output.WriteAsync(_opening, ct); await output.WriteAsync(_manifest, ct); var buffer = new byte[64 * 1024];
            for (var n = 0; n < _headers.Length; n++)
            {
                await output.WriteAsync(_headers[n], ct); var original = _plan.Entries[n].Original.Intent;
                using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256); long remaining = LocalCycleWire.Decimal(original.SizeBytes);
                while (remaining > 0)
                {
                    var size = (int)Math.Min(remaining, buffer.Length); await _source.ReadExactlyAsync(buffer.AsMemory(0, size), ct);
                    hash.AppendData(buffer, 0, size); await output.WriteAsync(buffer.AsMemory(0, size), ct); remaining -= size;
                }
                // Check before the following delimiter permits publication of
                // this item. Previous complete items retain their own receipts.
                if (Convert.ToHexStringLower(hash.GetHashAndReset()) != original.Sha256) throw new InvalidDataException("Attempted aggregate span digest changed.");
            }
            if (_source.Position != _source.Length) throw new InvalidDataException("Unexpected aggregate epilogue bytes.");
            await output.WriteAsync(_closing, ct);
        }
        // The caller retains the native ownership handle through the response.
        // Disposing request content must not close the recovery spool stream.
    }
}
