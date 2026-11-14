using System.Text.Json;
using Neptune.Core.Sync.Protocol;
namespace Neptune.Core;

public sealed partial class NeptuneStateStore
{
    private const string StreamPartsSchema="CREATE TABLE client_stream_parts(operation_id TEXT NOT NULL REFERENCES client_stream_operations(operation_id),part_index INTEGER NOT NULL CHECK(part_index>=0),chunk_json TEXT NOT NULL,PRIMARY KEY(operation_id,part_index)) WITHOUT ROWID";
    private async Task EnsureStreamParts(CancellationToken ct)
    {
        async Task<bool> Exists(){await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);await using var q=SyncCommand(c,t,"SELECT 1 FROM sqlite_master WHERE name='client_stream_parts'");return await q.ExecuteScalarAsync(ct)is not null;}
        if(await Exists())return;
        await using var migration=await AcquireSyncMigrationLockAsync(ct);if(await Exists())return;
        if(!await PreflightClientEvolution(ct))throw new InvalidDataException("Client schema must be qualified before adding stream ACKs.");
        var copy=checked(new FileInfo(databasePath).Length+(File.Exists(databasePath+"-wal")?new FileInfo(databasePath+"-wal").Length:0));
        CheckLocalDiskAdmission(new(),checked(copy+NamespaceReserve(32)));
        var backup=databasePath+".pre-stream-v2-"+Guid.NewGuid().ToString("N")+".db";await CreateCoherenceDatabaseCopy(databasePath,backup,ct);
        var sha256=await HashSyncBackupAsync(backup,ct);await File.WriteAllTextAsync(backup+".json",JsonSerializer.Serialize(new{source=Path.GetFullPath(databasePath),backup,sha256,minimumWriter="neptune.client-evolution.v2"}),ct);
        using(var file=new FileStream(backup+".json",FileMode.Open,FileAccess.Write,FileShare.None))file.Flush(true);
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:false);
        await ExecuteSyncAsync(c,t,StreamPartsSchema,ct);await ExecuteSyncAsync(c,t,"UPDATE client_evolution_format SET minimum_writer='neptune.client-evolution.v2' WHERE singleton=1",ct);await t.CommitAsync(ct);
    }
    internal async Task<SyncStreamChunk?> ReadOwnedStreamPart(string root,string owner,string scope,string operation,int index,CancellationToken ct)
    {
        await using var c=await OpenCycleConnection(ct);using var t=c.BeginTransaction(deferred:true);_=await RequireCycle(c,t,root,owner,scope,true,ct);
        await using var q=SyncCommand(c,t,"SELECT p.chunk_json FROM client_stream_parts p JOIN client_stream_operations s ON s.operation_id=p.operation_id WHERE s.scope_id=$scope AND p.operation_id=$op AND p.part_index=$index");
        CycleId(q,scope);q.Parameters.AddWithValue("$op",operation);q.Parameters.AddWithValue("$index",index);var raw=await q.ExecuteScalarAsync(ct)as string;return raw is null?null:JsonSerializer.Deserialize<SyncStreamChunk>(raw,LocalCycleWire.Json);
    }
    private static async Task SaveStreamParts(Microsoft.Data.Sqlite.SqliteConnection c,Microsoft.Data.Sqlite.SqliteTransaction t,SyncStreamInput input,SyncStreamStatus status,CancellationToken ct)
    {
        var rows=JsonSerializer.Serialize(status.Chunks.Select(p=>new{index=p.Index,json=JsonSerializer.Serialize(p,LocalCycleWire.Json)}));
        await using var check=SyncCommand(c,t,"SELECT 1 FROM client_stream_parts p JOIN json_each($rows) n ON p.part_index=json_extract(n.value,'$.index') WHERE p.operation_id=$op AND p.chunk_json<>json_extract(n.value,'$.json') LIMIT 1");
        check.Parameters.AddWithValue("$op",input.OperationId);check.Parameters.AddWithValue("$rows",rows);if(await check.ExecuteScalarAsync(ct)is not null)throw new InvalidDataException("Original indexed stream ACK changed.");
        check.CommandText="INSERT INTO client_stream_parts(operation_id,part_index,chunk_json) SELECT $op,json_extract(value,'$.index'),json_extract(value,'$.json') FROM json_each($rows) WHERE 1 ON CONFLICT(operation_id,part_index) DO NOTHING";
        await check.ExecuteNonQueryAsync(ct);
    }
}
public sealed partial class LocalSourceJournalSession
{
    internal Task<SyncStreamChunk?> ReadStreamPartAsync(string scope,string op,int index,CancellationToken ct)=>SerialAsync(()=>_store.ReadOwnedStreamPart(_root,_owner,scope,op,index,ct),ct);
}
