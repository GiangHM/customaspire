// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Runtime.CompilerServices;
using Aspire.Dashboard.Otlp.Model;
using Google.Protobuf;
using Microsoft.Data.Sqlite;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;

namespace Aspire.Dashboard.Otlp.Storage.Persistence;

/// <summary>
/// SQLite-backed implementation of <see cref="ITelemetryStorage"/> for persistent telemetry storage.
/// Stores OTLP telemetry payloads so that they survive dashboard restarts.
/// </summary>
/// <remarks>
/// <para>
/// Spans are stored in two related tables:
/// <list type="bullet">
/// <item>
/// <term><c>spans</c></term>
/// <description>Stores the full serialized <see cref="ResourceSpans"/> proto payload (one row per write call) for lossless startup replay.</description>
/// </item>
/// <item>
/// <term><c>span_index</c></term>
/// <description>Structured per-span rows with a <c>parent_span_id</c> column that is <see langword="null"/> for root spans, enabling direct parent-child relationship queries.</description>
/// </item>
/// </list>
/// </para>
/// <para>
/// Log and metric data are stored in <c>logs</c> and <c>metrics</c> tables respectively as proto blobs, which are sufficient for startup replay.
/// </para>
/// </remarks>
internal sealed class SqliteTelemetryStorage : ITelemetryStorage
{
    private readonly string _databasePath;
    private readonly ILogger<SqliteTelemetryStorage> _logger;
    private SqliteConnection? _connection;

    /// <summary>
    /// Initializes a new instance of <see cref="SqliteTelemetryStorage"/>.
    /// </summary>
    /// <param name="databasePath">The file-system path to the SQLite database file.</param>
    /// <param name="logger">Logger instance.</param>
    public SqliteTelemetryStorage(string databasePath, ILogger<SqliteTelemetryStorage> logger)
    {
        ArgumentException.ThrowIfNullOrEmpty(databasePath);
        _databasePath = databasePath;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = _databasePath,
            Mode = SqliteOpenMode.ReadWriteCreate
        }.ToString();

        _connection = new SqliteConnection(connectionString);
        await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        await CreateSchemaAsync(_connection, cancellationToken).ConfigureAwait(false);

        _logger.LogDebug("SQLite telemetry storage initialized at '{Path}'.", _databasePath);
    }

    private static async Task CreateSchemaAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        const string createLogsTable = """
            CREATE TABLE IF NOT EXISTS logs (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp    TEXT    NOT NULL,
                resource_key TEXT    NOT NULL,
                payload      BLOB    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_logs_resource_key ON logs (resource_key);
            CREATE INDEX IF NOT EXISTS idx_logs_timestamp     ON logs (timestamp);
            """;

        // 'spans' stores the full ResourceSpans proto blob per WriteSpansAsync call for lossless replay.
        // 'span_index' provides a structured per-span index with parent-child relationship support.
        const string createSpansTables = """
            CREATE TABLE IF NOT EXISTS spans (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                resource_key TEXT    NOT NULL,
                payload      BLOB    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_spans_resource_key ON spans (resource_key);

            CREATE TABLE IF NOT EXISTS span_index (
                span_id              TEXT    NOT NULL PRIMARY KEY,
                trace_id             TEXT    NOT NULL,
                parent_span_id       TEXT,
                spans_id             INTEGER NOT NULL,
                name                 TEXT    NOT NULL,
                kind                 INTEGER NOT NULL,
                start_time_unix_nano INTEGER NOT NULL,
                end_time_unix_nano   INTEGER NOT NULL,
                status_code          INTEGER NOT NULL,
                FOREIGN KEY (spans_id) REFERENCES spans(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_span_index_trace_id       ON span_index (trace_id);
            CREATE INDEX IF NOT EXISTS idx_span_index_parent_span_id ON span_index (parent_span_id);
            """;

        const string createMetricsTable = """
            CREATE TABLE IF NOT EXISTS metrics (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                resource_key TEXT    NOT NULL,
                payload      BLOB    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_metrics_resource_key ON metrics (resource_key);
            """;

        using var cmd = connection.CreateCommand();
        cmd.CommandText = createLogsTable + createSpansTables + createMetricsTable;
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task WriteLogsAsync(ResourceLogs resourceLogs, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        var resourceKey = GetResourceKey(resourceLogs.Resource);
        var timestamp = GetTimestamp(resourceLogs);
        var payload = resourceLogs.ToByteArray();

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "INSERT INTO logs (timestamp, resource_key, payload) VALUES (@timestamp, @resource_key, @payload)";
        cmd.Parameters.AddWithValue("@timestamp", timestamp);
        cmd.Parameters.AddWithValue("@resource_key", resourceKey);
        cmd.Parameters.AddWithValue("@payload", payload);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task WriteSpansAsync(ResourceSpans resourceSpans, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        var resourceKey = GetResourceKey(resourceSpans.Resource);
        var payload = resourceSpans.ToByteArray();

        using var transaction = _connection!.BeginTransaction();
        try
        {
            // Insert the full proto payload blob for startup replay.
            long spansId;
            using (var insertBatch = _connection.CreateCommand())
            {
                insertBatch.Transaction = transaction;
                insertBatch.CommandText = "INSERT INTO spans (resource_key, payload) VALUES (@resource_key, @payload); SELECT last_insert_rowid();";
                insertBatch.Parameters.AddWithValue("@resource_key", resourceKey);
                insertBatch.Parameters.AddWithValue("@payload", payload);
                var result = await insertBatch.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                spansId = (long)result!;
            }

            // Insert structured per-span rows for parent-child relationship queries.
            using var insertSpan = _connection.CreateCommand();
            insertSpan.Transaction = transaction;
            insertSpan.CommandText = """
                INSERT OR IGNORE INTO span_index
                    (span_id, trace_id, parent_span_id, spans_id, name, kind,
                     start_time_unix_nano, end_time_unix_nano, status_code)
                VALUES
                    (@span_id, @trace_id, @parent_span_id, @spans_id, @name, @kind,
                     @start_time_unix_nano, @end_time_unix_nano, @status_code)
                """;

            var pSpanId = insertSpan.Parameters.Add("@span_id", SqliteType.Text);
            var pTraceId = insertSpan.Parameters.Add("@trace_id", SqliteType.Text);
            var pParentSpanId = insertSpan.Parameters.Add("@parent_span_id", SqliteType.Text);
            var pSpansId = insertSpan.Parameters.Add("@spans_id", SqliteType.Integer);
            var pName = insertSpan.Parameters.Add("@name", SqliteType.Text);
            var pKind = insertSpan.Parameters.Add("@kind", SqliteType.Integer);
            var pStartTime = insertSpan.Parameters.Add("@start_time_unix_nano", SqliteType.Integer);
            var pEndTime = insertSpan.Parameters.Add("@end_time_unix_nano", SqliteType.Integer);
            var pStatusCode = insertSpan.Parameters.Add("@status_code", SqliteType.Integer);

            pSpansId.Value = spansId;

            foreach (var scopeSpans in resourceSpans.ScopeSpans)
            {
                foreach (var span in scopeSpans.Spans)
                {
                    pSpanId.Value = span.SpanId.ToHexString();
                    pTraceId.Value = span.TraceId.ToHexString();
                    pParentSpanId.Value = span.ParentSpanId.IsEmpty
                        ? DBNull.Value
                        : (object)span.ParentSpanId.ToHexString();
                    pName.Value = span.Name;
                    pKind.Value = (int)span.Kind;
                    pStartTime.Value = (long)span.StartTimeUnixNano;
                    pEndTime.Value = (long)span.EndTimeUnixNano;
                    pStatusCode.Value = span.Status is null ? 0 : (int)span.Status.Code;

                    await insertSpan.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    /// <inheritdoc />
    public async Task WriteMetricsAsync(ResourceMetrics resourceMetrics, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        var resourceKey = GetResourceKey(resourceMetrics.Resource);
        var payload = resourceMetrics.ToByteArray();

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "INSERT INTO metrics (resource_key, payload) VALUES (@resource_key, @payload)";
        cmd.Parameters.AddWithValue("@resource_key", resourceKey);
        cmd.Parameters.AddWithValue("@payload", payload);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ResourceLogs> ReadLogsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "SELECT payload FROM logs ORDER BY timestamp ASC, id ASC";

        using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var payload = (byte[])reader["payload"];
            yield return ResourceLogs.Parser.ParseFrom(payload);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ResourceSpans> ReadSpansAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "SELECT payload FROM spans ORDER BY id ASC";

        using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var payload = (byte[])reader["payload"];
            yield return ResourceSpans.Parser.ParseFrom(payload);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ResourceMetrics> ReadMetricsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "SELECT payload FROM metrics ORDER BY id ASC";

        using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var payload = (byte[])reader["payload"];
            yield return ResourceMetrics.Parser.ParseFrom(payload);
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
            _connection = null;
        }
    }

    private void EnsureInitialized()
    {
        if (_connection is null)
        {
            throw new InvalidOperationException($"{nameof(SqliteTelemetryStorage)} has not been initialized. Call {nameof(InitializeAsync)} before using this instance.");
        }
    }

    private static string GetResourceKey(Resource? resource)
    {
        if (resource is null)
        {
            return string.Empty;
        }

        var key = resource.GetResourceKey();
        return key.InstanceId is null
            ? key.Name
            : $"{key.Name}/{key.InstanceId}";
    }

    private static string GetTimestamp(ResourceLogs resourceLogs)
    {
        // Use the earliest log record timestamp, falling back to the current UTC time.
        ulong minNanos = ulong.MaxValue;

        foreach (var scopeLogs in resourceLogs.ScopeLogs)
        {
            foreach (var record in scopeLogs.LogRecords)
            {
                var nanos = record.TimeUnixNano > 0 ? record.TimeUnixNano : record.ObservedTimeUnixNano;
                if (nanos > 0 && nanos < minNanos)
                {
                    minNanos = nanos;
                }
            }
        }

        if (minNanos == ulong.MaxValue)
        {
            return DateTime.UtcNow.ToString("o");
        }

        return DateTime.UnixEpoch.AddTicks((long)(minNanos / 100)).ToString("o");
    }
}

