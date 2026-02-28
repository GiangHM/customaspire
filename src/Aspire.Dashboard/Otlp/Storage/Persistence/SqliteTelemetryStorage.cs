// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.Otlp.Model;
using Google.Protobuf;
using Microsoft.Data.Sqlite;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;

namespace Aspire.Dashboard.Otlp.Storage.Persistence;

/// <summary>
/// A SQLite-backed implementation of <see cref="ITelemetryStorage"/> that persists OTLP telemetry
/// (traces/spans, logs, and metrics) to a local SQLite database file.
/// </summary>
/// <remarks>
/// <para>
/// Trace data is stored in two tables: <c>SpanBatches</c> stores the full serialised
/// <see cref="ResourceSpans"/> protobuf payload for lossless replay, while <c>Spans</c>
/// maintains a structured index that includes the <c>parent_span_id</c> column so that
/// parent-child relationships between spans can be queried directly.
/// </para>
/// <para>
/// Log and metric data are stored as serialised protobuf blobs in <c>LogBatches</c> and
/// <c>MetricBatches</c> tables respectively, which are sufficient for startup-replay purposes.
/// </para>
/// <para>
/// Configure the database path via <c>Dashboard:Storage:SqlitePath</c> in the dashboard
/// configuration. If the path is not configured, use <see cref="NullTelemetryStorage"/> instead.
/// </para>
/// </remarks>
internal sealed class SqliteTelemetryStorage : ITelemetryStorage
{
    private readonly string _databasePath;
    private readonly ILogger<SqliteTelemetryStorage> _logger;
    private SqliteConnection? _connection;

    /// <summary>
    /// Initializes a new instance of the <see cref="SqliteTelemetryStorage"/> class.
    /// </summary>
    /// <param name="databasePath">The file system path for the SQLite database.</param>
    /// <param name="logger">A logger for recording storage diagnostics.</param>
    public SqliteTelemetryStorage(string databasePath, ILogger<SqliteTelemetryStorage> logger)
    {
        ArgumentException.ThrowIfNullOrEmpty(databasePath);
        ArgumentNullException.ThrowIfNull(logger);
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

        // Enable WAL mode for improved write throughput.
        await ExecuteNonQueryAsync("PRAGMA journal_mode=WAL;", cancellationToken).ConfigureAwait(false);

        await CreateSchemaAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogDebug("SQLite telemetry storage initialised at '{Path}'.", _databasePath);
    }

    private async Task CreateSchemaAsync(CancellationToken cancellationToken)
    {
        // SpanBatches stores the full proto payload per WriteSpansAsync call for lossless replay.
        // Spans provides a structured index with parent-child relationship columns.
        const string ddl = """
            CREATE TABLE IF NOT EXISTS SpanBatches (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                proto_bytes BLOB    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS Spans (
                trace_id              TEXT    NOT NULL,
                span_id               TEXT    NOT NULL,
                parent_span_id        TEXT,
                batch_id              INTEGER NOT NULL,
                name                  TEXT    NOT NULL,
                kind                  INTEGER NOT NULL,
                start_time_unix_nano  INTEGER NOT NULL,
                end_time_unix_nano    INTEGER NOT NULL,
                status_code           INTEGER NOT NULL,
                PRIMARY KEY (trace_id, span_id),
                FOREIGN KEY (batch_id) REFERENCES SpanBatches(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS LogBatches (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                proto_bytes BLOB    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS MetricBatches (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                proto_bytes BLOB    NOT NULL
            );
            """;

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = ddl;
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task WriteSpansAsync(ResourceSpans resourceSpans, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        var protoBytes = resourceSpans.ToByteArray();

        using var transaction = _connection!.BeginTransaction();
        try
        {
            // Insert the full proto payload for replay.
            long batchId;
            using (var insertBatch = _connection.CreateCommand())
            {
                insertBatch.Transaction = transaction;
                insertBatch.CommandText = "INSERT INTO SpanBatches (proto_bytes) VALUES (@proto_bytes); SELECT last_insert_rowid();";
                insertBatch.Parameters.AddWithValue("@proto_bytes", protoBytes);
                var result = await insertBatch.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                batchId = (long)result!;
            }

            // Insert structured span rows for querying.
            using var insertSpan = _connection.CreateCommand();
            insertSpan.Transaction = transaction;
            insertSpan.CommandText = """
                INSERT OR IGNORE INTO Spans
                    (trace_id, span_id, parent_span_id, batch_id, name, kind,
                     start_time_unix_nano, end_time_unix_nano, status_code)
                VALUES
                    (@trace_id, @span_id, @parent_span_id, @batch_id, @name, @kind,
                     @start_time_unix_nano, @end_time_unix_nano, @status_code)
                """;

            var pTraceId = insertSpan.Parameters.Add("@trace_id", SqliteType.Text);
            var pSpanId = insertSpan.Parameters.Add("@span_id", SqliteType.Text);
            var pParentSpanId = insertSpan.Parameters.Add("@parent_span_id", SqliteType.Text);
            var pBatchId = insertSpan.Parameters.Add("@batch_id", SqliteType.Integer);
            var pName = insertSpan.Parameters.Add("@name", SqliteType.Text);
            var pKind = insertSpan.Parameters.Add("@kind", SqliteType.Integer);
            var pStartTime = insertSpan.Parameters.Add("@start_time_unix_nano", SqliteType.Integer);
            var pEndTime = insertSpan.Parameters.Add("@end_time_unix_nano", SqliteType.Integer);
            var pStatusCode = insertSpan.Parameters.Add("@status_code", SqliteType.Integer);

            pBatchId.Value = batchId;

            foreach (var scopeSpans in resourceSpans.ScopeSpans)
            {
                foreach (var span in scopeSpans.Spans)
                {
                    pTraceId.Value = span.TraceId.ToHexString();
                    pSpanId.Value = span.SpanId.ToHexString();
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
    public async Task WriteLogsAsync(ResourceLogs resourceLogs, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        var protoBytes = resourceLogs.ToByteArray();

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "INSERT INTO LogBatches (proto_bytes) VALUES (@proto_bytes)";
        cmd.Parameters.AddWithValue("@proto_bytes", protoBytes);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task WriteMetricsAsync(ResourceMetrics resourceMetrics, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        var protoBytes = resourceMetrics.ToByteArray();

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "INSERT INTO MetricBatches (proto_bytes) VALUES (@proto_bytes)";
        cmd.Parameters.AddWithValue("@proto_bytes", protoBytes);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ResourceSpans> ReadSpansAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "SELECT proto_bytes FROM SpanBatches ORDER BY id";
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var bytes = (byte[])reader.GetValue(0);
            ResourceSpans resourceSpans;
            try
            {
                resourceSpans = ResourceSpans.Parser.ParseFrom(bytes);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to deserialise a ResourceSpans row; skipping.");
                continue;
            }

            yield return resourceSpans;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ResourceLogs> ReadLogsAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "SELECT proto_bytes FROM LogBatches ORDER BY id";
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var bytes = (byte[])reader.GetValue(0);
            ResourceLogs resourceLogs;
            try
            {
                resourceLogs = ResourceLogs.Parser.ParseFrom(bytes);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to deserialise a ResourceLogs row; skipping.");
                continue;
            }

            yield return resourceLogs;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ResourceMetrics> ReadMetricsAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "SELECT proto_bytes FROM MetricBatches ORDER BY id";
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var bytes = (byte[])reader.GetValue(0);
            ResourceMetrics resourceMetrics;
            try
            {
                resourceMetrics = ResourceMetrics.Parser.ParseFrom(bytes);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to deserialise a ResourceMetrics row; skipping.");
                continue;
            }

            yield return resourceMetrics;
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
            throw new InvalidOperationException($"{nameof(SqliteTelemetryStorage)} has not been initialised. Call {nameof(InitializeAsync)} first.");
        }
    }

    private async Task ExecuteNonQueryAsync(string sql, CancellationToken cancellationToken)
    {
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
