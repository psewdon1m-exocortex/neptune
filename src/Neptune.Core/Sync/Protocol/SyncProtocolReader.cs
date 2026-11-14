using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Neptune.Core.Sync.Protocol;

// Inactive candidate reader. It neither establishes source completeness nor
// grants snapshot/feed authority, current presence or baseline adoption.
public sealed class SyncWireException(string code) : Exception(code) { public string Code { get; } = code; }
public sealed record SyncRootBinding(string Protocol, string DescriptorSchema, string ServerIdentity, string RootId, string ServerEpoch, string PolicyVersion, string BindingVersion);
public sealed record SyncDescriptor(string NodeId, string? ParentId, string Name, string Type, string SizeBytes, string? ContentSha256, string? ContentVersionId, string Version, string Availability);
public sealed record SyncFeedItem(SyncDescriptor Descriptor, string Seq, string Kind, string OperationId, string? BeforeVersion);
public sealed record SyncFeedPage(SyncRootBinding Binding, string FromExclusive, string ToInclusive, IReadOnlyList<SyncFeedItem> Items, string NextCursor, bool HasMore);
public sealed record SyncSnapshotReady(SyncRootBinding Binding, string SnapshotId, string CutSeq, string EntriesCursor, string ChangesCursor, long LeaseSeconds, long AbsoluteLifetimeSeconds);

public static class SyncProtocolReader
{
    public const int MaxPageBytes = 4 * 1024 * 1024;
    public const int MaxPageItems = 500;
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private static SyncWireException Invalid(string code = "invalid_contract") => new(code);
    private static void Scalar(string s)
    {
        for (var i = 0; i < s.Length; i++)
            if (char.IsHighSurrogate(s[i])) { if (++i >= s.Length || !char.IsLowSurrogate(s[i])) throw Invalid(); }
            else if (char.IsLowSurrogate(s[i])) throw Invalid();
    }
    public static JsonDocument Parse(ReadOnlyMemory<byte> raw)
    {
        if (raw.Length > MaxPageBytes) throw Invalid("page_limit");
        if (raw.Span.StartsWith(new byte[] { 0xef, 0xbb, 0xbf })) raw = raw[3..];
        try
        {
            _ = StrictUtf8.GetCharCount(raw.Span);
            var r = new Utf8JsonReader(raw.Span, new JsonReaderOptions { MaxDepth = 33, CommentHandling = JsonCommentHandling.Disallow });
            var scopes = new Stack<HashSet<string>?>();
            while (r.Read())
            {
                if (r.TokenType != JsonTokenType.PropertyName && r.CurrentDepth > 32) throw Invalid("invalid_json");
                if (r.TokenType == JsonTokenType.StartObject) scopes.Push(new(StringComparer.Ordinal));
                if (r.TokenType == JsonTokenType.StartArray) scopes.Push(null);
                if (r.TokenType is JsonTokenType.EndObject or JsonTokenType.EndArray) scopes.Pop();
                if (r.TokenType is JsonTokenType.PropertyName or JsonTokenType.String)
                {
                    var s = r.GetString() ?? throw Invalid("invalid_json"); Scalar(s);
                    if (r.TokenType == JsonTokenType.PropertyName && !(scopes.Peek()?.Add(s) ?? false)) throw Invalid("invalid_json");
                }
            }
            return JsonDocument.Parse(raw, new JsonDocumentOptions { MaxDepth = 33 });
        }
        catch (SyncWireException) { throw; }
        catch (Exception e) when (e is JsonException or DecoderFallbackException or InvalidOperationException or ArgumentException)
        { throw Invalid("invalid_json"); }
    }
    private static JsonElement Field(JsonElement m, string key) => m.ValueKind == JsonValueKind.Object && m.TryGetProperty(key, out var v) ? v : throw Invalid();
    private static string Text(JsonElement v) => v.ValueKind == JsonValueKind.String ? v.GetString() ?? throw Invalid() : throw Invalid();
    private static string Text(JsonElement m, string k) => Text(Field(m, k));
    private static string? NullableText(JsonElement m, string k) { var v = Field(m, k); return v.ValueKind == JsonValueKind.Null ? null : Text(v); }
    private static bool Opaque(string s, int maximum = 128) => s.Length is > 0 && s.Length <= maximum && s.All(c => c is >= '!' and <= '~');
    private static bool Version(string s) => Guid.TryParseExact(s, "D", out _) && s.Length == 36 && s == s.ToLowerInvariant();
    public static long Integer64(JsonElement v) => Integer64(Text(v));
    private static long Integer64(string s)
    {
        if (s.Length is < 1 or > 19 || s.Length > 1 && s[0] == '0' || !s.All(c => c is >= '0' and <= '9')
            || !long.TryParse(s, NumberStyles.None, CultureInfo.InvariantCulture, out var n)) throw Invalid();
        return n;
    }
    public static SyncDescriptor ReadDescriptor(ReadOnlyMemory<byte> raw) { using var d = Parse(raw); return Descriptor(d.RootElement); }
    public static SyncSnapshotReady ReadSnapshot(ReadOnlyMemory<byte> raw) { using var d = Parse(raw); return Snapshot(d.RootElement); }
    public static SyncFeedPage ReadFeed(ReadOnlyMemory<byte> raw, SyncRootBinding previous, string after) { using var d = Parse(raw); return Feed(d.RootElement, previous, after); }
    public static void ReadExpected(ReadOnlyMemory<byte> raw) { using var d = Parse(raw); Expected(d.RootElement); }
    public static SyncRootBinding Binding(JsonElement m)
    {
        var b = new SyncRootBinding(Text(m, "protocol"), Text(m, "descriptor_schema"), Text(m, "server_identity"), Text(m, "root_id"), Text(m, "server_epoch"), Text(m, "policy_version"), Text(m, "binding_version"));
        if (b.Protocol != "neptune-sync/1" || b.DescriptorSchema != "saturn.current-namespace.v1" || !Version(b.BindingVersion)
            || new[] { b.ServerIdentity, b.RootId, b.ServerEpoch, b.PolicyVersion }.Any(s => !Opaque(s))) throw Invalid();
        return b;
    }
    public static SyncDescriptor Descriptor(JsonElement m)
    {
        var d = new SyncDescriptor(Text(m, "node_id"), NullableText(m, "parent_id"), Text(m, "name"), Text(m, "type"), Text(m, "size_bytes"), NullableText(m, "content_sha256"), NullableText(m, "content_version_id"), Text(m, "version"), Text(m, "availability"));
        Scalar(d.Name); _ = Integer64(d.SizeBytes);
        if (!Opaque(d.NodeId) || d.ParentId is not null && (!Opaque(d.ParentId) || d.ParentId == d.NodeId) || !Version(d.Version)
            || d.Name.Length == 0 || StrictUtf8.GetByteCount(d.Name) > 1020 || d.Name.IndexOfAny(['\0', '/', '\\']) >= 0
            || d.Type is not ("file" or "folder") || d.Availability is not ("AVAILABLE" or "UNAVAILABLE" or "UNKNOWN")
            || d.ContentVersionId is not null && !Opaque(d.ContentVersionId)
            || d.ContentSha256 is not null && (d.ContentSha256.Length != 64 || !d.ContentSha256.All(c => c is >= '0' and <= '9' or >= 'a' and <= 'f'))
            || d.Type == "folder" && (d.ContentSha256 is not null || d.ContentVersionId is not null)
            || d.Type == "file" && d.Availability == "AVAILABLE" && (d.ContentSha256 is null || d.ContentVersionId is null)) throw Invalid();
        return d;
    }
    public static void Expected(JsonElement m)
    {
        var kind = Text(m, "kind"); var count = m.EnumerateObject().Count();
        if (kind == "absent" && count == 1) return;
        if (kind != "version" || count != 3 || !Opaque(Text(m, "node_id")) || !Version(Text(m, "version"))) throw Invalid();
    }
    public static SyncFeedPage Feed(JsonElement m, SyncRootBinding previous, string after)
    {
        var binding = Binding(m); if (binding != previous) throw Invalid("binding_mismatch");
        var fromText = Text(m, "from_exclusive"); var throughText = Text(m, "to_inclusive");
        var from = Integer64(fromText); var through = Integer64(throughText);
        if (from != Integer64(after) || through < from) throw Invalid("feed_gap");
        var raw = Field(m, "items"); if (raw.ValueKind != JsonValueKind.Array || raw.GetArrayLength() > MaxPageItems) throw Invalid("page_limit");
        var items = new List<SyncFeedItem>(raw.GetArrayLength()); var seq = from;
        foreach (var entry in raw.EnumerateArray())
        {
            var d = Descriptor(entry); var s = Text(entry, "seq"); var n = Integer64(s);
            if (seq == long.MaxValue || n != seq + 1) throw Invalid("feed_gap"); seq = n;
            var kind = Text(entry, "kind"); var operation = Text(entry, "operation_id");
            if (kind is not ("upsert" or "tombstone") || !Opaque(operation)) throw Invalid();
            var before = kind == "tombstone" ? Text(entry, "before_version") : null;
            if (before is not null && (!Version(before) || before == d.Version)) throw Invalid();
            items.Add(new(d, s, kind, operation, before));
        }
        if (seq != through) throw Invalid("feed_gap");
        var next = Text(m, "next_cursor"); var more = Field(m, "has_more");
        if (!Opaque(next, 4096) || more.ValueKind is not (JsonValueKind.True or JsonValueKind.False)) throw Invalid();
        if (more.GetBoolean() && items.Count == 0) throw Invalid("feed_gap");
        return new(binding, fromText, throughText, items.AsReadOnly(), next, more.GetBoolean());
    }
    public static SyncSnapshotReady Snapshot(JsonElement m)
    {
        var binding = Binding(m); var id = Text(m, "snapshot_id"); var cut = Text(m, "cut_seq");
        _ = Integer64(cut); var entries = Text(m, "entries_cursor"); var changes = Text(m, "changes_cursor");
        if (Text(m, "state") != "READY" || !Opaque(id) || !Opaque(entries, 4096) || !Opaque(changes, 4096)) throw Invalid();
        static long Lease(JsonElement v)
        { if (v.ValueKind != JsonValueKind.Number || !v.TryGetDouble(out var n) || n < 1 || n > 86400 || n != Math.Truncate(n)) throw Invalid(); return (long)n; }
        var lease = Lease(Field(m, "lease_seconds")); var absolute = Lease(Field(m, "absolute_lifetime_seconds"));
        if (lease > absolute) throw Invalid(); return new(binding, id, cut, entries, changes, lease, absolute);
    }
}
