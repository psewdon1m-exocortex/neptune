using System.Text.Json;

namespace Neptune.Core.Sync.Protocol;

public sealed record SyncSnapshotEntriesPage(SyncRootBinding Binding, string SnapshotId, string CutSeq,
    string? AfterNodeId, string? LastNodeId, IReadOnlyList<SyncDescriptor> Items, string NextCursor, bool HasMore);

/// <summary>Inactive bounded page reader. Terminal shape does not establish
/// server coverage, local completeness, current presence or baseline adoption.</summary>
public static class SyncSnapshotEntriesReader
{
    private static SyncWireException Invalid(string code = "invalid_contract") => new(code);
    private static JsonElement Field(JsonElement m, string key) => m.ValueKind == JsonValueKind.Object && m.TryGetProperty(key, out var value) ? value : throw Invalid();
    private static string Text(JsonElement m, string key) { var v = Field(m, key); return v.ValueKind == JsonValueKind.String ? v.GetString()! : throw Invalid(); }
    private static string? Nullable(JsonElement m, string key) { var v = Field(m, key); return v.ValueKind == JsonValueKind.Null ? null : v.ValueKind == JsonValueKind.String ? v.GetString()! : throw Invalid(); }
    private static bool Opaque(string value, int maximum = 128) => value.Length is > 0 && value.Length <= maximum && value.All(c => c is >= '!' and <= '~');

    public static SyncSnapshotEntriesPage Read(ReadOnlyMemory<byte> raw, SyncSnapshotReady snapshot, string? after)
    {
        if (!Opaque(snapshot.EntriesCursor, 4096) || !Opaque(snapshot.ChangesCursor, 4096)
            || snapshot.LeaseSeconds < 1 || snapshot.AbsoluteLifetimeSeconds > 86400 || snapshot.LeaseSeconds > snapshot.AbsoluteLifetimeSeconds) throw Invalid();
        using var document = SyncProtocolReader.Parse(raw);
        var m = document.RootElement;
        var binding = SyncProtocolReader.Binding(m);
        if (binding != snapshot.Binding) throw Invalid("binding_mismatch");
        var id = Text(m, "snapshot_id"); var cut = Text(m, "cut_seq");
        if (!Opaque(id)) throw Invalid();
        _ = SyncProtocolReader.Integer64(Field(m, "cut_seq"));
        if (id != snapshot.SnapshotId || cut != snapshot.CutSeq) throw Invalid("snapshot_mismatch");
        var from = Nullable(m, "after_node_id"); var end = Nullable(m, "last_node_id");
        if (after is not null && !Opaque(after) || from is not null && !Opaque(from) || end is not null && !Opaque(end)) throw Invalid();
        if (from != after) throw Invalid("snapshot_page_gap");
        var items = Field(m, "items");
        if (items.ValueKind != JsonValueKind.Array || items.GetArrayLength() > SyncProtocolReader.MaxPageItems) throw Invalid("page_limit");
        var entries = new List<SyncDescriptor>(items.GetArrayLength());
        var last = after;
        foreach (var item in items.EnumerateArray())
        {
            var entry = SyncProtocolReader.Descriptor(item);
            if (last is not null && string.CompareOrdinal(entry.NodeId, last) <= 0) throw Invalid("snapshot_page_gap");
            entries.Add(entry); last = entry.NodeId;
        }
        if (end != last) throw Invalid("snapshot_page_gap");
        var next = Text(m, "next_cursor"); var more = Field(m, "has_more");
        if (!Opaque(next, 4096) || more.ValueKind is not (JsonValueKind.True or JsonValueKind.False)) throw Invalid();
        if (more.GetBoolean() && entries.Count == 0) throw Invalid("snapshot_page_gap");
        return new(binding, id, cut, from, end, entries.AsReadOnly(), next, more.GetBoolean());
    }
}
