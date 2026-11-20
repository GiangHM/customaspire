// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.Otlp.Storage.Persistence;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;
using Xunit;
using static Aspire.Tests.Shared.Telemetry.TelemetryTestHelpers;

namespace Aspire.Dashboard.Tests.Persistence;

public sealed class SqliteRetentionTests : IDisposable
{
    private static readonly DateTime s_testTime = new(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"aspire-retention-{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        try
        {
            File.Delete(_dbPath);
        }
        catch
        {
            // Best-effort cleanup.
        }
    }

    [Fact]
    public async Task InitializeAsync_LogsBeyondLimit_KeepsNewestBatches()
    {
        await using (var unbounded = CreateStorage(retention: null))
        {
            await unbounded.InitializeAsync();
            for (var i = 0; i < 10; i++)
            {
                await unbounded.WriteLogsAsync(CreateLogs(i));
            }
        }

        await using var storage = CreateStorage(new SqliteRetentionLimits(MaxLogBatches: 3, MaxSpanBatches: 3, MaxMetricPointsPerInstrument: 3));
        await storage.InitializeAsync();

        var names = new List<string>();
        await foreach (var logs in storage.ReadLogsAsync())
        {
            names.Add(logs.Resource.Attributes.Single(a => a.Key == "service.name").Value.StringValue);
        }

        Assert.Equal(["app7", "app8", "app9"], names);
    }

    [Fact]
    public async Task InitializeAsync_SpansBeyondLimit_KeepsNewestBatchesAndTheirIndexRows()
    {
        await using (var unbounded = CreateStorage(retention: null))
        {
            await unbounded.InitializeAsync();
            for (var i = 0; i < 10; i++)
            {
                await unbounded.WriteSpansAsync(CreateSpans(i));
            }
        }

        await using var storage = CreateStorage(new SqliteRetentionLimits(MaxLogBatches: 3, MaxSpanBatches: 3, MaxMetricPointsPerInstrument: 3));
        await storage.InitializeAsync();

        var batches = 0;
        await foreach (var _ in storage.ReadSpansAsync())
        {
            batches++;
        }

        Assert.Equal(3, batches);
        Assert.Equal(3, Count("span_index"));
    }

    [Fact]
    public async Task InitializeAsync_MetricPointsBeyondLimit_KeepsNewestPointsPerInstrument()
    {
        await using (var unbounded = CreateStorage(retention: null))
        {
            await unbounded.InitializeAsync();
            for (var i = 0; i < 6; i++)
            {
                await unbounded.WriteMetricsAsync(CreateMetrics("metric1", i));
                await unbounded.WriteMetricsAsync(CreateMetrics("metric2", i));
            }
        }

        await using var storage = CreateStorage(new SqliteRetentionLimits(MaxLogBatches: 3, MaxSpanBatches: 3, MaxMetricPointsPerInstrument: 2));
        await storage.InitializeAsync();

        // Two instruments, two points each.
        Assert.Equal(4, Count("number_data_points"));
    }

    [Fact]
    public async Task WriteLogsAsync_PrunesPeriodically()
    {
        await using var storage = CreateStorage(new SqliteRetentionLimits(MaxLogBatches: 5, MaxSpanBatches: 5, MaxMetricPointsPerInstrument: 5));
        await storage.InitializeAsync();

        // The 1000th write triggers a prune down to 5 rows, and one more write follows it.
        for (var i = 0; i < 1001; i++)
        {
            await storage.WriteLogsAsync(CreateLogs(i));
        }

        Assert.Equal(6, Count("logs"));
    }

    [Fact]
    public async Task InitializeAsync_NoRetention_KeepsEverything()
    {
        await using var storage = CreateStorage(retention: null);
        await storage.InitializeAsync();
        for (var i = 0; i < 20; i++)
        {
            await storage.WriteLogsAsync(CreateLogs(i));
        }

        await storage.DisposeAsync();
        await using var reopened = CreateStorage(retention: null);
        await reopened.InitializeAsync();

        Assert.Equal(20, Count("logs"));
    }

    private SqliteTelemetryStorage CreateStorage(SqliteRetentionLimits? retention) =>
        new(_dbPath, NullLogger<SqliteTelemetryStorage>.Instance, retention);

    private long Count(string table)
    {
        using var connection = new SqliteConnection($"Data Source={_dbPath};Pooling=false");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {table}";
        return (long)command.ExecuteScalar()!;
    }

    private static ResourceLogs CreateLogs(int index) => new()
    {
        Resource = CreateResource(name: $"app{index}", instanceId: "instance"),
        ScopeLogs = { new ScopeLogs { Scope = CreateScope(), LogRecords = { CreateLogRecord(time: s_testTime.AddSeconds(index), message: $"log{index}") } } }
    };

    private static ResourceSpans CreateSpans(int index) => new()
    {
        Resource = CreateResource(name: "app", instanceId: "instance"),
        ScopeSpans = { new ScopeSpans { Scope = CreateScope(), Spans = { CreateSpan(traceId: $"trace{index}", spanId: $"span{index}", startTime: s_testTime, endTime: s_testTime.AddMinutes(1)) } } }
    };

    private static ResourceMetrics CreateMetrics(string metricName, int index) => new()
    {
        Resource = CreateResource(name: "app", instanceId: "instance"),
        ScopeMetrics = { new ScopeMetrics { Scope = CreateScope(name: "meter"), Metrics = { CreateSumMetric(metricName, s_testTime.AddSeconds(index), value: index) } } }
    };
}
