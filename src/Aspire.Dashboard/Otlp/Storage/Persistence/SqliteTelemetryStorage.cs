// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Google.Protobuf;
using Microsoft.Data.Sqlite;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;

namespace Aspire.Dashboard.Otlp.Storage.Persistence;

/// <summary>
/// SQLite-backed implementation of <see cref="ITelemetryStorage"/>.
/// Metrics are stored with an instrument-based relational schema
/// (Resource → Meter → Instrument → data points).
/// Logs and spans are stored as serialized protobuf blobs for replay on startup.
/// </summary>
internal sealed class SqliteTelemetryStorage : ITelemetryStorage
{
    private readonly string _databasePath;
    private readonly ILogger<SqliteTelemetryStorage> _logger;
    private SqliteConnection? _connection;

    /// <summary>
    /// Initializes a new instance of <see cref="SqliteTelemetryStorage"/>.
    /// </summary>
    /// <param name="databasePath">The file path for the SQLite database.</param>
    /// <param name="logger">Logger for diagnostic messages.</param>
    public SqliteTelemetryStorage(string databasePath, ILogger<SqliteTelemetryStorage> logger)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);
        _databasePath = databasePath;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        _connection = new SqliteConnection($"Data Source={_databasePath}");
        await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        await ExecuteNonQueryAsync(@"PRAGMA journal_mode=WAL;", cancellationToken).ConfigureAwait(false);
        await ExecuteNonQueryAsync(@"PRAGMA foreign_keys=ON;", cancellationToken).ConfigureAwait(false);

        // Metric tables — instrument-based hierarchy
        await ExecuteNonQueryAsync(@"
            CREATE TABLE IF NOT EXISTS resources (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                service_name TEXT NOT NULL,
                instance_id  TEXT NOT NULL DEFAULT '',
                UNIQUE(service_name, instance_id)
            );", cancellationToken).ConfigureAwait(false);

        await ExecuteNonQueryAsync(@"
            CREATE TABLE IF NOT EXISTS meters (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                resource_id INTEGER NOT NULL REFERENCES resources(id),
                name        TEXT NOT NULL,
                version     TEXT NOT NULL DEFAULT '',
                UNIQUE(resource_id, name, version)
            );", cancellationToken).ConfigureAwait(false);

        await ExecuteNonQueryAsync(@"
            CREATE TABLE IF NOT EXISTS instruments (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                meter_id                INTEGER NOT NULL REFERENCES meters(id),
                name                    TEXT    NOT NULL,
                description             TEXT    NOT NULL DEFAULT '',
                unit                    TEXT    NOT NULL DEFAULT '',
                type                    INTEGER NOT NULL,
                aggregation_temporality INTEGER NOT NULL DEFAULT 0,
                is_monotonic            INTEGER NOT NULL DEFAULT 0,
                UNIQUE(meter_id, name)
            );", cancellationToken).ConfigureAwait(false);

        await ExecuteNonQueryAsync(@"
            CREATE TABLE IF NOT EXISTS number_data_points (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument_id     INTEGER NOT NULL REFERENCES instruments(id),
                attributes_json   TEXT    NOT NULL DEFAULT '[]',
                start_time_unix_ns INTEGER NOT NULL,
                end_time_unix_ns   INTEGER NOT NULL,
                value_type        INTEGER NOT NULL,
                value_long        INTEGER,
                value_double      REAL
            );", cancellationToken).ConfigureAwait(false);

        await ExecuteNonQueryAsync(@"
            CREATE TABLE IF NOT EXISTS histogram_data_points (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                instrument_id        INTEGER NOT NULL REFERENCES instruments(id),
                attributes_json      TEXT    NOT NULL DEFAULT '[]',
                start_time_unix_ns   INTEGER NOT NULL,
                end_time_unix_ns     INTEGER NOT NULL,
                sum                  REAL    NOT NULL,
                count                INTEGER NOT NULL,
                bucket_counts_json   TEXT    NOT NULL DEFAULT '[]',
                explicit_bounds_json TEXT    NOT NULL DEFAULT '[]'
            );", cancellationToken).ConfigureAwait(false);

        // Log and span tables — proto blob storage
        await ExecuteNonQueryAsync(@"
            CREATE TABLE IF NOT EXISTS log_batches (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                data BLOB NOT NULL
            );", cancellationToken).ConfigureAwait(false);

        await ExecuteNonQueryAsync(@"
            CREATE TABLE IF NOT EXISTS span_batches (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                data BLOB NOT NULL
            );", cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task WriteMetricsAsync(ResourceMetrics resourceMetrics, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        var resourceId = await GetOrInsertResourceAsync(resourceMetrics.Resource, cancellationToken).ConfigureAwait(false);

        foreach (var scopeMetrics in resourceMetrics.ScopeMetrics)
        {
            var meterName = scopeMetrics.Scope?.Name ?? string.Empty;
            var meterVersion = scopeMetrics.Scope?.Version ?? string.Empty;
            var meterId = await GetOrInsertMeterAsync(resourceId, meterName, meterVersion, cancellationToken).ConfigureAwait(false);

            foreach (var metric in scopeMetrics.Metrics)
            {
                var instrumentId = await GetOrInsertInstrumentAsync(meterId, metric, cancellationToken).ConfigureAwait(false);

                switch (metric.DataCase)
                {
                    case Metric.DataOneofCase.Gauge:
                        foreach (var dp in metric.Gauge.DataPoints)
                        {
                            await InsertNumberDataPointAsync(instrumentId, dp, cancellationToken).ConfigureAwait(false);
                        }
                        break;

                    case Metric.DataOneofCase.Sum:
                        foreach (var dp in metric.Sum.DataPoints)
                        {
                            await InsertNumberDataPointAsync(instrumentId, dp, cancellationToken).ConfigureAwait(false);
                        }
                        break;

                    case Metric.DataOneofCase.Histogram:
                        foreach (var dp in metric.Histogram.DataPoints)
                        {
                            await InsertHistogramDataPointAsync(instrumentId, dp, cancellationToken).ConfigureAwait(false);
                        }
                        break;
                }
            }
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ResourceMetrics> ReadMetricsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        // Load all resources
        var resources = new List<(long Id, string ServiceName, string InstanceId)>();
        using (var cmd = _connection!.CreateCommand())
        {
            cmd.CommandText = "SELECT id, service_name, instance_id FROM resources ORDER BY id;";
            using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                resources.Add((reader.GetInt64(0), reader.GetString(1), reader.GetString(2)));
            }
        }

        foreach (var (resourceId, serviceName, instanceId) in resources)
        {
            var resourceMetrics = new ResourceMetrics
            {
                Resource = new Resource
                {
                    Attributes =
                    {
                        new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = serviceName } },
                        new KeyValue { Key = "service.instance.id", Value = new AnyValue { StringValue = instanceId } }
                    }
                }
            };

            // Load meters for this resource
            var meters = new List<(long Id, string Name, string Version)>();
            using (var cmd = _connection!.CreateCommand())
            {
                cmd.CommandText = "SELECT id, name, version FROM meters WHERE resource_id = @rid ORDER BY id;";
                cmd.Parameters.AddWithValue("@rid", resourceId);
                using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    meters.Add((reader.GetInt64(0), reader.GetString(1), reader.GetString(2)));
                }
            }

            foreach (var (meterId, meterName, meterVersion) in meters)
            {
                var scopeMetrics = new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = meterName, Version = meterVersion }
                };

                // Load instruments for this meter
                var instruments = new List<(long Id, string Name, string Description, string Unit, int Type, int Temporality, int IsMonotonic)>();
                using (var cmd = _connection!.CreateCommand())
                {
                    cmd.CommandText = "SELECT id, name, description, unit, type, aggregation_temporality, is_monotonic FROM instruments WHERE meter_id = @mid ORDER BY id;";
                    cmd.Parameters.AddWithValue("@mid", meterId);
                    using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        instruments.Add((reader.GetInt64(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetInt32(4), reader.GetInt32(5), reader.GetInt32(6)));
                    }
                }

                foreach (var (instrumentId, instrName, instrDesc, instrUnit, instrType, temporality, isMonotonic) in instruments)
                {
                    var metric = new Metric
                    {
                        Name = instrName,
                        Description = instrDesc,
                        Unit = instrUnit
                    };

                    // instrType: 0=Gauge, 1=Sum, 2=Histogram
                    switch (instrType)
                    {
                        case 0: // Gauge
                            var gauge = new Gauge();
                            await LoadNumberDataPointsAsync(instrumentId, gauge.DataPoints, cancellationToken).ConfigureAwait(false);
                            metric.Gauge = gauge;
                            break;

                        case 1: // Sum
                            var sum = new Sum
                            {
                                AggregationTemporality = (AggregationTemporality)temporality,
                                IsMonotonic = isMonotonic != 0
                            };
                            await LoadNumberDataPointsAsync(instrumentId, sum.DataPoints, cancellationToken).ConfigureAwait(false);
                            metric.Sum = sum;
                            break;

                        case 2: // Histogram
                            var histogram = new Histogram
                            {
                                AggregationTemporality = (AggregationTemporality)temporality
                            };
                            await LoadHistogramDataPointsAsync(instrumentId, histogram.DataPoints, cancellationToken).ConfigureAwait(false);
                            metric.Histogram = histogram;
                            break;
                    }

                    scopeMetrics.Metrics.Add(metric);
                }

                resourceMetrics.ScopeMetrics.Add(scopeMetrics);
            }

            yield return resourceMetrics;
        }
    }

    /// <inheritdoc />
    public async Task WriteLogsAsync(ResourceLogs resourceLogs, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        var data = resourceLogs.ToByteArray();
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "INSERT INTO log_batches (data) VALUES (@data);";
        cmd.Parameters.AddWithValue("@data", data);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ResourceLogs> ReadLogsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "SELECT data FROM log_batches ORDER BY id;";
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var data = (byte[])reader.GetValue(0);
            yield return ResourceLogs.Parser.ParseFrom(data);
        }
    }

    /// <inheritdoc />
    public async Task WriteSpansAsync(ResourceSpans resourceSpans, CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        var data = resourceSpans.ToByteArray();
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "INSERT INTO span_batches (data) VALUES (@data);";
        cmd.Parameters.AddWithValue("@data", data);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ResourceSpans> ReadSpansAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        EnsureInitialized();
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "SELECT data FROM span_batches ORDER BY id;";
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var data = (byte[])reader.GetValue(0);
            yield return ResourceSpans.Parser.ParseFrom(data);
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

    private async Task<long> GetOrInsertResourceAsync(Resource resource, CancellationToken cancellationToken)
    {
        var serviceName = GetAttributeValue(resource.Attributes, "service.name") ?? string.Empty;
        var instanceId = GetAttributeValue(resource.Attributes, "service.instance.id") ?? string.Empty;

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO resources (service_name, instance_id)
            VALUES (@sn, @iid)
            ON CONFLICT(service_name, instance_id) DO NOTHING;
            SELECT id FROM resources WHERE service_name = @sn AND instance_id = @iid;";
        cmd.Parameters.AddWithValue("@sn", serviceName);
        cmd.Parameters.AddWithValue("@iid", instanceId);
        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    private async Task<long> GetOrInsertMeterAsync(long resourceId, string name, string version, CancellationToken cancellationToken)
    {
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO meters (resource_id, name, version)
            VALUES (@rid, @name, @ver)
            ON CONFLICT(resource_id, name, version) DO NOTHING;
            SELECT id FROM meters WHERE resource_id = @rid AND name = @name AND version = @ver;";
        cmd.Parameters.AddWithValue("@rid", resourceId);
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@ver", version);
        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    private async Task<long> GetOrInsertInstrumentAsync(long meterId, Metric metric, CancellationToken cancellationToken)
    {
        var (type, temporality, isMonotonic) = GetInstrumentTypeInfo(metric);

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO instruments (meter_id, name, description, unit, type, aggregation_temporality, is_monotonic)
            VALUES (@mid, @name, @desc, @unit, @type, @temp, @mono)
            ON CONFLICT(meter_id, name) DO NOTHING;
            SELECT id FROM instruments WHERE meter_id = @mid AND name = @name;";
        cmd.Parameters.AddWithValue("@mid", meterId);
        cmd.Parameters.AddWithValue("@name", metric.Name ?? string.Empty);
        cmd.Parameters.AddWithValue("@desc", metric.Description ?? string.Empty);
        cmd.Parameters.AddWithValue("@unit", metric.Unit ?? string.Empty);
        cmd.Parameters.AddWithValue("@type", type);
        cmd.Parameters.AddWithValue("@temp", temporality);
        cmd.Parameters.AddWithValue("@mono", isMonotonic ? 1 : 0);
        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    private async Task InsertNumberDataPointAsync(long instrumentId, NumberDataPoint dp, CancellationToken cancellationToken)
    {
        var attrsJson = SerializeAttributes(dp.Attributes);
        var (valueType, valueLong, valueDouble) = dp.ValueCase == NumberDataPoint.ValueOneofCase.AsInt
            ? (0, (long?)dp.AsInt, (double?)null)
            : (1, (long?)null, (double?)dp.AsDouble);

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO number_data_points
                (instrument_id, attributes_json, start_time_unix_ns, end_time_unix_ns, value_type, value_long, value_double)
            VALUES (@iid, @attrs, @start, @end, @vtype, @vlong, @vdbl);";
        cmd.Parameters.AddWithValue("@iid", instrumentId);
        cmd.Parameters.AddWithValue("@attrs", attrsJson);
        cmd.Parameters.AddWithValue("@start", (long)dp.StartTimeUnixNano);
        cmd.Parameters.AddWithValue("@end", (long)dp.TimeUnixNano);
        cmd.Parameters.AddWithValue("@vtype", valueType);
        cmd.Parameters.AddWithValue("@vlong", (object?)valueLong ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@vdbl", (object?)valueDouble ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task InsertHistogramDataPointAsync(long instrumentId, HistogramDataPoint dp, CancellationToken cancellationToken)
    {
        var attrsJson = SerializeAttributes(dp.Attributes);
        var bucketCountsJson = JsonSerializer.Serialize(dp.BucketCounts);
        var boundsJson = JsonSerializer.Serialize(dp.ExplicitBounds);

        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO histogram_data_points
                (instrument_id, attributes_json, start_time_unix_ns, end_time_unix_ns, sum, count, bucket_counts_json, explicit_bounds_json)
            VALUES (@iid, @attrs, @start, @end, @sum, @count, @buckets, @bounds);";
        cmd.Parameters.AddWithValue("@iid", instrumentId);
        cmd.Parameters.AddWithValue("@attrs", attrsJson);
        cmd.Parameters.AddWithValue("@start", (long)dp.StartTimeUnixNano);
        cmd.Parameters.AddWithValue("@end", (long)dp.TimeUnixNano);
        cmd.Parameters.AddWithValue("@sum", dp.Sum);
        cmd.Parameters.AddWithValue("@count", (long)dp.Count);
        cmd.Parameters.AddWithValue("@buckets", bucketCountsJson);
        cmd.Parameters.AddWithValue("@bounds", boundsJson);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task LoadNumberDataPointsAsync(long instrumentId, Google.Protobuf.Collections.RepeatedField<NumberDataPoint> dataPoints, CancellationToken cancellationToken)
    {
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "SELECT attributes_json, start_time_unix_ns, end_time_unix_ns, value_type, value_long, value_double FROM number_data_points WHERE instrument_id = @iid ORDER BY id;";
        cmd.Parameters.AddWithValue("@iid", instrumentId);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var attrsJson = reader.GetString(0);
            var startNs = (ulong)reader.GetInt64(1);
            var endNs = (ulong)reader.GetInt64(2);
            var valueType = reader.GetInt32(3);

            var dp = new NumberDataPoint
            {
                StartTimeUnixNano = startNs,
                TimeUnixNano = endNs
            };

            DeserializeAttributes(attrsJson, dp.Attributes, instrumentId, _logger);

            if (valueType == 0)
            {
                dp.AsInt = reader.GetInt64(4);
            }
            else
            {
                dp.AsDouble = reader.GetDouble(5);
            }

            dataPoints.Add(dp);
        }
    }

    private async Task LoadHistogramDataPointsAsync(long instrumentId, Google.Protobuf.Collections.RepeatedField<HistogramDataPoint> dataPoints, CancellationToken cancellationToken)
    {
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = "SELECT attributes_json, start_time_unix_ns, end_time_unix_ns, sum, count, bucket_counts_json, explicit_bounds_json FROM histogram_data_points WHERE instrument_id = @iid ORDER BY id;";
        cmd.Parameters.AddWithValue("@iid", instrumentId);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var attrsJson = reader.GetString(0);
            var startNs = (ulong)reader.GetInt64(1);
            var endNs = (ulong)reader.GetInt64(2);
            var sum = reader.GetDouble(3);
            var count = (ulong)reader.GetInt64(4);
            var bucketCountsJson = reader.GetString(5);
            var boundsJson = reader.GetString(6);

            var dp = new HistogramDataPoint
            {
                StartTimeUnixNano = startNs,
                TimeUnixNano = endNs,
                Sum = sum,
                Count = count
            };

            DeserializeAttributes(attrsJson, dp.Attributes, instrumentId, _logger);

            try
            {
                var bucketCounts = JsonSerializer.Deserialize<ulong[]>(bucketCountsJson);
                if (bucketCounts is not null)
                {
                    dp.BucketCounts.AddRange(bucketCounts);
                }
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(ex, "Failed to deserialize bucket_counts_json for histogram data point (instrument_id={InstrumentId}). Skipping bucket counts.", instrumentId);
            }

            try
            {
                var explicitBounds = JsonSerializer.Deserialize<double[]>(boundsJson);
                if (explicitBounds is not null)
                {
                    dp.ExplicitBounds.AddRange(explicitBounds);
                }
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(ex, "Failed to deserialize explicit_bounds_json for histogram data point (instrument_id={InstrumentId}). Skipping explicit bounds.", instrumentId);
            }

            dataPoints.Add(dp);
        }
    }

    private static (int Type, int Temporality, bool IsMonotonic) GetInstrumentTypeInfo(Metric metric)
    {
        return metric.DataCase switch
        {
            Metric.DataOneofCase.Gauge => (0, 0, false),
            Metric.DataOneofCase.Sum => (1, (int)metric.Sum.AggregationTemporality, metric.Sum.IsMonotonic),
            Metric.DataOneofCase.Histogram => (2, (int)metric.Histogram.AggregationTemporality, false),
            _ => (0, 0, false)
        };
    }

    private static string? GetAttributeValue(Google.Protobuf.Collections.RepeatedField<KeyValue> attributes, string key)
    {
        foreach (var kv in attributes)
        {
            if (kv.Key == key)
            {
                return kv.Value?.StringValue;
            }
        }
        return null;
    }

    private static string SerializeAttributes(Google.Protobuf.Collections.RepeatedField<KeyValue> attributes)
    {
        if (attributes.Count == 0)
        {
            return "[]";
        }

        var pairs = new List<string[]>(attributes.Count);
        foreach (var kv in attributes)
        {
            pairs.Add([kv.Key, kv.Value?.StringValue ?? string.Empty]);
        }
        return JsonSerializer.Serialize(pairs);
    }

    private static void DeserializeAttributes(string json, Google.Protobuf.Collections.RepeatedField<KeyValue> attributes, long instrumentId, ILogger logger)
    {
        if (json == "[]")
        {
            return;
        }

        try
        {
            var pairs = JsonSerializer.Deserialize<string[][]>(json);
            if (pairs is not null)
            {
                foreach (var pair in pairs)
                {
                    if (pair.Length >= 2)
                    {
                        attributes.Add(new KeyValue
                        {
                            Key = pair[0],
                            Value = new AnyValue { StringValue = pair[1] }
                        });
                    }
                }
            }
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "Failed to deserialize attributes_json for data point (instrument_id={InstrumentId}). Skipping attributes.", instrumentId);
        }
    }

    private async Task ExecuteNonQueryAsync(string sql, CancellationToken cancellationToken)
    {
        using var cmd = _connection!.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private void EnsureInitialized()
    {
        if (_connection is null)
        {
            throw new InvalidOperationException($"{nameof(SqliteTelemetryStorage)} has not been initialized. Call {nameof(InitializeAsync)} first.");
        }
    }
}
