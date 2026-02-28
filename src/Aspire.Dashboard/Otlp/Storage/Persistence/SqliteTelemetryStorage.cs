// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Google.Protobuf;
using Microsoft.Data.Sqlite;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;

namespace Aspire.Dashboard.Otlp.Storage.Persistence;

/// <summary>
/// A SQLite-backed implementation of <see cref="ITelemetryStorage"/> that persists OTLP telemetry
/// data to a local SQLite database so that telemetry survives dashboard restarts.
/// </summary>
internal sealed class SqliteTelemetryStorage : ITelemetryStorage
{
    private const string TelemetryLogsTable = "telemetry_logs";
    private const string TelemetrySpansTable = "telemetry_spans";
    private const string TelemetryMetricsTable = "telemetry_metrics";

    private readonly string _databasePath;
    private SqliteConnection? _connection;

    /// <summary>
    /// Initializes a new instance of <see cref="SqliteTelemetryStorage"/> with the specified database path.
    /// </summary>
    /// <param name="databasePath">The file path to the SQLite database.</param>
    public SqliteTelemetryStorage(string databasePath)
    {
        ArgumentException.ThrowIfNullOrEmpty(databasePath);
        _databasePath = databasePath;
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

        await CreateTablesAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task CreateTablesAsync(CancellationToken cancellationToken)
    {
        const string createTables = """
            CREATE TABLE IF NOT EXISTS telemetry_logs (
                id    INTEGER PRIMARY KEY AUTOINCREMENT,
                data  BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS telemetry_spans (
                id    INTEGER PRIMARY KEY AUTOINCREMENT,
                data  BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS telemetry_metrics (
                id    INTEGER PRIMARY KEY AUTOINCREMENT,
                data  BLOB NOT NULL
            );
            """;

        var command = _connection!.CreateCommand();
        await using (command.ConfigureAwait(false))
        {
            command.CommandText = createTables;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task WriteLogsAsync(ResourceLogs resourceLogs, CancellationToken cancellationToken = default)
    {
        await InsertBytesAsync(TelemetryLogsTable, resourceLogs.ToByteArray(), cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task WriteSpansAsync(ResourceSpans resourceSpans, CancellationToken cancellationToken = default)
    {
        await InsertBytesAsync(TelemetrySpansTable, resourceSpans.ToByteArray(), cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task WriteMetricsAsync(ResourceMetrics resourceMetrics, CancellationToken cancellationToken = default)
    {
        await InsertBytesAsync(TelemetryMetricsTable, resourceMetrics.ToByteArray(), cancellationToken).ConfigureAwait(false);
    }

    private async Task InsertBytesAsync(string tableName, byte[] data, CancellationToken cancellationToken)
    {
        EnsureKnownTableName(tableName);
        EnsureInitialized();
        var command = _connection!.CreateCommand();
        await using (command.ConfigureAwait(false))
        {
            command.CommandText = $"INSERT INTO {tableName} (data) VALUES ($data)";
            command.Parameters.AddWithValue("$data", data);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public IAsyncEnumerable<ResourceLogs> ReadLogsAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        return ReadRowsAsync(TelemetryLogsTable, ResourceLogs.Parser.ParseFrom, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<ResourceSpans> ReadSpansAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        return ReadRowsAsync(TelemetrySpansTable, ResourceSpans.Parser.ParseFrom, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<ResourceMetrics> ReadMetricsAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        return ReadRowsAsync(TelemetryMetricsTable, ResourceMetrics.Parser.ParseFrom, cancellationToken);
    }

    private async IAsyncEnumerable<T> ReadRowsAsync<T>(
        string tableName,
        Func<byte[], T> deserialize,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        EnsureKnownTableName(tableName);
        var command = _connection!.CreateCommand();
        await using (command.ConfigureAwait(false))
        {
            command.CommandText = $"SELECT data FROM {tableName} ORDER BY id ASC";
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var data = (byte[])reader.GetValue(0);
                    yield return deserialize(data);
                }
            }
        }
    }

    private void EnsureInitialized()
    {
        if (_connection is null)
        {
            throw new InvalidOperationException($"{nameof(SqliteTelemetryStorage)} has not been initialized. Call {nameof(InitializeAsync)} first.");
        }
    }

    private static void EnsureKnownTableName(string tableName)
    {
        if (tableName != TelemetryLogsTable && tableName != TelemetrySpansTable && tableName != TelemetryMetricsTable)
        {
            throw new ArgumentException($"Unknown table name: {tableName}", nameof(tableName));
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
}
