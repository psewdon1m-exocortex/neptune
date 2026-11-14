using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Neptune.Core.Sync.Protocol;

namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private sealed record RemoteBatchValue(string Json,long Sequence,bool Deleted,long Bytes);

    // One qualified pointer/policy and one indexed read for a bounded page.
    // Every event is checked in order before a single atomic set-based write.
    private static async Task<long> UpsertCycleRemoteBatch(SqliteConnection c,SqliteTransaction t,string id,
        IReadOnlyList<(SyncDescriptor Descriptor,long Sequence,bool Deleted,bool Replay)> events,
        bool snapshot,bool pending,CancellationToken ct)
    {
        if(events.Count==0)return 0;
        if(events.Count>500)throw new InvalidDataException("Remote page exceeds bounded admission.");
        var location=await CycleRemoteLocation(c,t,id,pending,ct);
        var policy=await ReadMappingPolicy(c,t,id,ct);
        var previous=new Dictionary<string,RemoteBatchValue>(StringComparer.Ordinal);
        await using(var q=SyncCommand(c,t,$"SELECT node_id,description,seq,deleted,bytes FROM {location.Remote} WHERE scope_id=$scope AND node_id IN(SELECT value FROM json_each($nodes))"))
        {
            CycleId(q,location.Id);q.Parameters.AddWithValue("$nodes",JsonSerializer.Serialize(events.Select(e=>e.Descriptor.NodeId).Distinct()));
            await using var r=await q.ExecuteReaderAsync(ct);
            while(await r.ReadAsync(ct))previous.Add(r.GetString(0),new(r.GetString(1),r.GetInt64(2),r.GetInt32(3)!=0,r.GetInt64(4)));
        }
        var rows=new Dictionary<string,object>(StringComparer.Ordinal);var invalidations=new List<object>();long delta=0;
        foreach(var (d,seq,deleted,replay) in events)
        {
            var json=JsonSerializer.Serialize(d,LocalCycleWire.Json);
            previous.TryGetValue(d.NodeId,out var old);
            if(replay&& (old is null||old.Sequence<seq))throw new InvalidDataException("Committed remote chunk disappeared during replay.");
            if(old?.Sequence>seq)continue;
            if(old?.Sequence==seq)
            {
                if(old.Json!=json||old.Deleted!=deleted)throw new InvalidDataException("An immutable remote prefix changed on replay.");
                continue;
            }
            var bytes=checked(256+Encoding.UTF8.GetByteCount(json));delta=checked(delta+bytes-(old?.Bytes??0));
            previous[d.NodeId]=new(json,seq,deleted,bytes);
            var key=d.ParentId is null?"":policy.Codec=="portable_v1"?SyncNameCodec.Key(d.Name,policy.Codec):WebDavSyncClient.CanonicalPathKey(d.Name);
            rows[d.NodeId]=new{node=d.NodeId,parent=d.ParentId,name=d.Name,key,kind=d.Type=="folder"?1:0,json,seq,deleted=deleted?1:0,bytes};
            if(!snapshot&&!pending)invalidations.Add(new{node=d.NodeId,version=deleted?"deleted":d.Version});
        }
        if(rows.Count>0)
        {
            await using var q=SyncCommand(c,t,$"""
                INSERT INTO {location.Remote}(scope_id,node_id,parent_id,name,name_key,kind,description,seq,deleted,bytes)
                SELECT $scope,json_extract(value,'$.node'),json_extract(value,'$.parent'),json_extract(value,'$.name'),json_extract(value,'$.key'),
                    json_extract(value,'$.kind'),json_extract(value,'$.json'),json_extract(value,'$.seq'),json_extract(value,'$.deleted'),json_extract(value,'$.bytes')
                FROM json_each($rows) WHERE 1
                ON CONFLICT(scope_id,node_id) DO UPDATE SET parent_id=excluded.parent_id,name=excluded.name,name_key=excluded.name_key,
                    kind=excluded.kind,description=excluded.description,seq=excluded.seq,deleted=excluded.deleted,bytes=excluded.bytes
                """);
            CycleId(q,location.Id);q.Parameters.AddWithValue("$rows",JsonSerializer.Serialize(rows.Values));await q.ExecuteNonQueryAsync(ct);
        }
        if(invalidations.Count>0)
        {
            await using var q=SyncCommand(c,t,"""
                UPDATE local_cycle_files AS f SET phase=2 WHERE f.scope_id=$scope AND f.kind=0 AND f.phase<>1 AND f.baseline IS NOT NULL
                AND f.remote_node IN(SELECT json_extract(value,'$.node') FROM json_each($rows))
                AND EXISTS(SELECT 1 FROM json_each($rows) e WHERE json_extract(e.value,'$.node')=f.remote_node
                    AND json_extract(f.baseline,'$.description.version')<>json_extract(e.value,'$.version'))
                """);
            CycleId(q,id);q.Parameters.AddWithValue("$rows",JsonSerializer.Serialize(invalidations));await q.ExecuteNonQueryAsync(ct);
        }
        return delta;
    }
}
