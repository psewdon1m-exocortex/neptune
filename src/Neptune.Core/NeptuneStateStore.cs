using Microsoft.Data.Sqlite;

namespace Neptune.Core;

public sealed class NeptuneStateStore(string databasePath)
{
    private string ConnectionString => new SqliteConnectionStringBuilder
    {
        DataSource = databasePath,
        Mode = SqliteOpenMode.ReadWriteCreate,
        Cache = SqliteCacheMode.Shared,
        ForeignKeys = true,
        DefaultTimeout = 30
    }.ToString();

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(databasePath)!);
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS backup_runs (
                run_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                client_instance_id TEXT NOT NULL,
                state TEXT NOT NULL,
                spool_path TEXT,
                size INTEGER,
                sha256 TEXT,
                saturn_run_id TEXT,
                uploaded_bytes INTEGER NOT NULL DEFAULT 0,
                attempt INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                error TEXT
            );
            CREATE INDEX IF NOT EXISTS backup_runs_project_state
                ON backup_runs(project_id, state);
            CREATE TABLE IF NOT EXISTS sync_mappings (
                mapping_id TEXT PRIMARY KEY,
                client_instance_id TEXT NOT NULL,
                local_path TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(client_instance_id, local_path)
            );
            CREATE TABLE IF NOT EXISTS sync_files (
                mapping_id TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                local_size INTEGER NOT NULL,
                local_mtime_utc TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                remote_etag TEXT,
                state TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                error TEXT,
                PRIMARY KEY(mapping_id, relative_path),
                FOREIGN KEY(mapping_id) REFERENCES sync_mappings(mapping_id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS remote_commands (
                command_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                payload TEXT NOT NULL,
                state TEXT NOT NULL,
                error TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS remote_commands_capacity BEFORE INSERT ON remote_commands
            WHEN NOT EXISTS (SELECT 1 FROM remote_commands WHERE command_id=NEW.command_id)
            BEGIN
                DELETE FROM remote_commands WHERE state IN ('succeeded','failed') AND updated_at < strftime('%Y-%m-%dT%H:%M:%fZ','now','-30 days');
                SELECT CASE WHEN (SELECT COUNT(*) FROM remote_commands) >= 10000 THEN RAISE(ABORT,'remote command capacity reached; retry later') END;
            END;
            CREATE TRIGGER IF NOT EXISTS backup_runs_capacity BEFORE INSERT ON backup_runs
            BEGIN
                DELETE FROM backup_runs WHERE state='complete'
                  AND run_id NOT IN (SELECT run_id FROM backup_runs WHERE state='complete' ORDER BY updated_at DESC LIMIT 1000)
                  AND run_id NOT IN (SELECT run_id FROM backup_runs WHERE state='complete' GROUP BY client_instance_id,project_id HAVING updated_at=MAX(updated_at))
                  AND (length(run_id) < 64 OR updated_at < strftime('%Y-%m-%dT%H:%M:%fZ','now','-30 days'));
                SELECT CASE WHEN (SELECT COUNT(*) FROM backup_runs) >= 12000 THEN RAISE(ABORT,'backup history capacity reached; retry later') END;
            END;
            """;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task PruneSpoolAsync(string directory, CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(directory)) return;
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT spool_path, state FROM backup_runs WHERE spool_path IS NOT NULL";
        var active = new HashSet<string>(OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
        var complete = new HashSet<string>(active.Comparer);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var filename = Path.GetFullPath(reader.GetString(0));
            (reader.GetString(1) == "complete" ? complete : active).Add(filename);
        }
        foreach (var filename in Directory.EnumerateFiles(directory, "*", new EnumerationOptions { AttributesToSkip = FileAttributes.ReparsePoint, RecurseSubdirectories = false }))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var resolved = Path.GetFullPath(filename);
            if (!active.Contains(resolved) && (complete.Contains(resolved) || File.GetLastWriteTimeUtc(resolved) < DateTime.UtcNow.AddDays(-1))) File.Delete(resolved);
        }
    }

    public async Task<RemoteCommandResult?> GetRemoteCommandAsync(string commandId, CancellationToken cancellationToken = default)
    {
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT command_id, state, error FROM remote_commands WHERE command_id=$id";
        command.Parameters.AddWithValue("$id", commandId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? new RemoteCommandResult(reader.GetString(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2))
            : null;
    }

    public async Task SaveRemoteCommandAsync(string commandId, string projectId, string kind, string payload, string state, string? error, CancellationToken cancellationToken = default)
    {
        if (commandId.Length > 128 || projectId.Length > 128 || kind.Length > 64 || payload.Length > 65536)
            throw new InvalidDataException("Remote command exceeds the journal size limit.");
        if (error?.Length > 1024) error = error[..1024];
        if (state is "succeeded" or "failed") payload = "{}";
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO remote_commands(command_id, project_id, kind, payload, state, error, updated_at)
            VALUES($id,$projectId,$kind,$payload,$state,$error,$updatedAt)
            ON CONFLICT(command_id) DO UPDATE SET state=excluded.state,payload=excluded.payload,error=excluded.error,updated_at=excluded.updated_at;
            """;
        command.Parameters.AddWithValue("$id", commandId);
        command.Parameters.AddWithValue("$projectId", projectId);
        command.Parameters.AddWithValue("$kind", kind);
        command.Parameters.AddWithValue("$payload", payload);
        command.Parameters.AddWithValue("$state", state);
        command.Parameters.AddWithValue("$error", (object?)error ?? DBNull.Value);
        command.Parameters.AddWithValue("$updatedAt", DateTimeOffset.UtcNow.ToString("O"));
        await command.ExecuteNonQueryAsync(cancellationToken);
        await using var prune = connection.CreateCommand();
        prune.CommandText = "DELETE FROM remote_commands WHERE project_id=$projectId AND state IN ('succeeded','failed') AND updated_at < $cutoff";
        prune.Parameters.AddWithValue("$projectId", projectId);
        prune.Parameters.AddWithValue("$cutoff", DateTimeOffset.UtcNow.AddDays(-30).ToString("O"));
        await prune.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<RemoteCommandResult>> ListRemoteCommandResultsAsync(string projectId, CancellationToken cancellationToken = default)
    {
        var result = new List<RemoteCommandResult>();
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT command_id, state, error FROM remote_commands WHERE project_id=$projectId AND state IN ('succeeded','failed') ORDER BY updated_at DESC LIMIT 100";
        command.Parameters.AddWithValue("$projectId", projectId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            result.Add(new RemoteCommandResult(reader.GetString(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
        return result;
    }

    public async Task AddRunAsync(BackupRun run, CancellationToken cancellationToken = default)
    {
        if (run.RunId.Length > 128 || run.ProjectId.Length > 128 || run.ClientInstanceId.Length > 128 || run.SpoolPath?.Length > 4096)
            throw new InvalidDataException("Backup run exceeds journal field limits.");
        if (run.Error?.Length > 1024) run = run with { Error = run.Error[..1024] };
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO backup_runs(
                run_id, project_id, client_instance_id, state, spool_path, size, sha256,
                saturn_run_id, uploaded_bytes, attempt, created_at, updated_at, error)
            VALUES ($runId, $projectId, $clientId, $state, $spoolPath, $size, $sha256,
                $saturnRunId, $uploadedBytes, $attempt, $createdAt, $updatedAt, $error);
            """;
        command.Parameters.AddWithValue("$runId", run.RunId);
        command.Parameters.AddWithValue("$projectId", run.ProjectId);
        command.Parameters.AddWithValue("$clientId", run.ClientInstanceId);
        command.Parameters.AddWithValue("$state", run.State);
        command.Parameters.AddWithValue("$spoolPath", (object?)run.SpoolPath ?? DBNull.Value);
        command.Parameters.AddWithValue("$size", (object?)run.Size ?? DBNull.Value);
        command.Parameters.AddWithValue("$sha256", (object?)run.Sha256 ?? DBNull.Value);
        command.Parameters.AddWithValue("$saturnRunId", (object?)run.SaturnRunId ?? DBNull.Value);
        command.Parameters.AddWithValue("$uploadedBytes", run.UploadedBytes);
        command.Parameters.AddWithValue("$attempt", run.Attempt);
        command.Parameters.AddWithValue("$createdAt", run.CreatedAt.ToString("O"));
        command.Parameters.AddWithValue("$updatedAt", run.UpdatedAt.ToString("O"));
        command.Parameters.AddWithValue("$error", (object?)run.Error ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<BackupRun?> GetRunAsync(string clientInstanceId, string runId, CancellationToken cancellationToken = default)
    {
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT run_id,project_id,client_instance_id,state,spool_path,size,sha256,saturn_run_id,uploaded_bytes,attempt,created_at,updated_at,error FROM backup_runs WHERE client_instance_id=$clientId AND run_id=$runId";
        command.Parameters.AddWithValue("$clientId", clientInstanceId);
        command.Parameters.AddWithValue("$runId", runId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        return new BackupRun(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetInt64(5),
            reader.IsDBNull(6) ? null : reader.GetString(6), reader.IsDBNull(7) ? null : reader.GetString(7),
            reader.GetInt64(8), reader.GetInt32(9), DateTimeOffset.Parse(reader.GetString(10)),
            DateTimeOffset.Parse(reader.GetString(11)), reader.IsDBNull(12) ? null : reader.GetString(12));
    }

    public async Task UpdateRunAsync(BackupRun run, CancellationToken cancellationToken = default)
    {
        if (run.Error?.Length > 1024) run = run with { Error = run.Error[..1024] };
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE backup_runs SET state=$state, spool_path=$spoolPath, size=$size, sha256=$sha256,
                saturn_run_id=$saturnRunId, uploaded_bytes=$uploadedBytes, attempt=$attempt,
                updated_at=$updatedAt, error=$error
            WHERE run_id=$runId AND client_instance_id=$clientId;
            """;
        command.Parameters.AddWithValue("$runId", run.RunId);
        command.Parameters.AddWithValue("$clientId", run.ClientInstanceId);
        command.Parameters.AddWithValue("$state", run.State);
        command.Parameters.AddWithValue("$spoolPath", (object?)run.SpoolPath ?? DBNull.Value);
        command.Parameters.AddWithValue("$size", (object?)run.Size ?? DBNull.Value);
        command.Parameters.AddWithValue("$sha256", (object?)run.Sha256 ?? DBNull.Value);
        command.Parameters.AddWithValue("$saturnRunId", (object?)run.SaturnRunId ?? DBNull.Value);
        command.Parameters.AddWithValue("$uploadedBytes", run.UploadedBytes);
        command.Parameters.AddWithValue("$attempt", run.Attempt);
        command.Parameters.AddWithValue("$updatedAt", run.UpdatedAt.ToString("O"));
        command.Parameters.AddWithValue("$error", (object?)run.Error ?? DBNull.Value);
        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
            throw new InvalidOperationException($"Backup run '{run.RunId}' is missing or belongs to another client.");
    }

    public async Task<IReadOnlyList<BackupRun>> ListRecoverableRunsAsync(string clientInstanceId, CancellationToken cancellationToken = default)
    {
        var result = new List<BackupRun>();
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT run_id, project_id, client_instance_id, state, spool_path, size, sha256,
                saturn_run_id, uploaded_bytes, attempt, created_at, updated_at, error
            FROM backup_runs
            WHERE client_instance_id=$clientId AND state IN ('spooled','uploading','retry-wait')
            ORDER BY created_at;
            """;
        command.Parameters.AddWithValue("$clientId", clientInstanceId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            result.Add(new BackupRun(
                reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetInt64(5),
                reader.IsDBNull(6) ? null : reader.GetString(6), reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.GetInt64(8), reader.GetInt32(9), DateTimeOffset.Parse(reader.GetString(10)),
                DateTimeOffset.Parse(reader.GetString(11)), reader.IsDBNull(12) ? null : reader.GetString(12)));
        return result;
    }

    public async Task<ProjectRunSummary> GetProjectRunSummaryAsync(string clientInstanceId, string projectId, CancellationToken cancellationToken = default)
    {
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        BackupRun? latest = null;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                SELECT run_id, project_id, client_instance_id, state, spool_path, size, sha256,
                    saturn_run_id, uploaded_bytes, attempt, created_at, updated_at, error
                FROM backup_runs WHERE client_instance_id=$clientId AND project_id=$projectId
                ORDER BY created_at DESC LIMIT 1;
                """;
            command.Parameters.AddWithValue("$clientId", clientInstanceId);
            command.Parameters.AddWithValue("$projectId", projectId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
                latest = new BackupRun(
                    reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetInt64(5),
                    reader.IsDBNull(6) ? null : reader.GetString(6), reader.IsDBNull(7) ? null : reader.GetString(7),
                    reader.GetInt64(8), reader.GetInt32(9), DateTimeOffset.Parse(reader.GetString(10)),
                    DateTimeOffset.Parse(reader.GetString(11)), reader.IsDBNull(12) ? null : reader.GetString(12));
        }
        await using var success = connection.CreateCommand();
        success.CommandText = "SELECT updated_at FROM backup_runs WHERE client_instance_id=$clientId AND project_id=$projectId AND state='complete' ORDER BY updated_at DESC LIMIT 1";
        success.Parameters.AddWithValue("$clientId", clientInstanceId);
        success.Parameters.AddWithValue("$projectId", projectId);
        var value = await success.ExecuteScalarAsync(cancellationToken);
        return new ProjectRunSummary(latest, value is string text ? DateTimeOffset.Parse(text) : null);
    }

    public async Task AddMappingAsync(SyncMapping mapping, CancellationToken cancellationToken = default)
    {
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO sync_mappings(mapping_id, client_instance_id, local_path, enabled, created_at)
            VALUES ($mappingId, $clientId, $localPath, $enabled, $createdAt);
            """;
        command.Parameters.AddWithValue("$mappingId", mapping.MappingId);
        command.Parameters.AddWithValue("$clientId", mapping.ClientInstanceId);
        command.Parameters.AddWithValue("$localPath", mapping.LocalPath);
        command.Parameters.AddWithValue("$enabled", mapping.Enabled ? 1 : 0);
        command.Parameters.AddWithValue("$createdAt", mapping.CreatedAt.ToString("O"));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<SyncMapping>> ListMappingsAsync(CancellationToken cancellationToken = default)
    {
        var result = new List<SyncMapping>();
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT mapping_id, client_instance_id, local_path, enabled, created_at FROM sync_mappings ORDER BY created_at";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            result.Add(new SyncMapping(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetBoolean(3), DateTimeOffset.Parse(reader.GetString(4))));
        return result;
    }

    public async Task RemoveMappingAsync(string mappingId, string clientInstanceId, CancellationToken cancellationToken = default)
    {
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "DELETE FROM sync_mappings WHERE mapping_id = $mappingId AND client_instance_id = $clientId";
        command.Parameters.AddWithValue("$mappingId", mappingId);
        command.Parameters.AddWithValue("$clientId", clientInstanceId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<SyncFileRecord?> GetSyncFileAsync(string mappingId, string relativePath, CancellationToken cancellationToken = default)
    {
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT mapping_id, relative_path, local_size, local_mtime_utc, sha256, remote_etag, state, updated_at, error
            FROM sync_files WHERE mapping_id=$mappingId AND relative_path=$relativePath;
            """;
        command.Parameters.AddWithValue("$mappingId", mappingId);
        command.Parameters.AddWithValue("$relativePath", relativePath);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        return new SyncFileRecord(reader.GetString(0), reader.GetString(1), reader.GetInt64(2), DateTimeOffset.Parse(reader.GetString(3)), reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetString(5), reader.GetString(6), DateTimeOffset.Parse(reader.GetString(7)), reader.IsDBNull(8) ? null : reader.GetString(8));
    }

    public async Task UpsertSyncFileAsync(SyncFileRecord record, CancellationToken cancellationToken = default)
    {
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO sync_files(mapping_id, relative_path, local_size, local_mtime_utc, sha256, remote_etag, state, updated_at, error)
            VALUES ($mappingId,$relativePath,$size,$mtime,$sha256,$etag,$state,$updatedAt,$error)
            ON CONFLICT(mapping_id, relative_path) DO UPDATE SET
                local_size=excluded.local_size, local_mtime_utc=excluded.local_mtime_utc,
                sha256=excluded.sha256, remote_etag=excluded.remote_etag, state=excluded.state,
                updated_at=excluded.updated_at, error=excluded.error;
            """;
        command.Parameters.AddWithValue("$mappingId", record.MappingId);
        command.Parameters.AddWithValue("$relativePath", record.RelativePath);
        command.Parameters.AddWithValue("$size", record.LocalSize);
        command.Parameters.AddWithValue("$mtime", record.LocalModifiedAt.ToString("O"));
        command.Parameters.AddWithValue("$sha256", record.Sha256);
        command.Parameters.AddWithValue("$etag", (object?)record.RemoteEtag ?? DBNull.Value);
        command.Parameters.AddWithValue("$state", record.State);
        command.Parameters.AddWithValue("$updatedAt", record.UpdatedAt.ToString("O"));
        command.Parameters.AddWithValue("$error", (object?)record.Error ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
