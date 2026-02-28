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

        const string createSpansTable = """
            CREATE TABLE IF NOT EXISTS spans (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                resource_key TEXT    NOT NULL,
                payload      BLOB    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_spans_resource_key ON spans (resource_key);
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
        cmd.CommandText = createLogsTable + createSpansTable + createMetricsTable;
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

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "INSERT INTO spans (resource_key, payload) VALUES (@resource_key, @payload)";
        cmd.Parameters.AddWithValue("@resource_key", resourceKey);
        cmd.Parameters.AddWithValue("@payload", payload);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
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

        var ticks = (long)(minNanos / 100);
        return new DateTime(ticks, DateTimeKind.Utc).ToString("o");
    }
}
