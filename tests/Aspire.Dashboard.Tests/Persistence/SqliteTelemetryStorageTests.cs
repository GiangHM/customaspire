// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.Otlp.Storage.Persistence;
using Microsoft.Extensions.Logging.Abstractions;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;
using Xunit;

namespace Aspire.Dashboard.Tests.Persistence;

public class SqliteTelemetryStorageTests : IAsyncDisposable
{
    private readonly string _dbPath;
    private readonly SqliteTelemetryStorage _storage;

    public SqliteTelemetryStorageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"aspire-test-{Guid.NewGuid():N}.db");
        _storage = new SqliteTelemetryStorage(_dbPath, NullLogger<SqliteTelemetryStorage>.Instance);
    }

    public async ValueTask DisposeAsync()
    {
        await _storage.DisposeAsync();

        if (File.Exists(_dbPath))
        {
            File.Delete(_dbPath);
        }
    }

    [Fact]
    public async Task InitializeAsync_CreatesDatabase()
    {
        // Act
        await _storage.InitializeAsync();

        // Assert — database file was created
        Assert.True(File.Exists(_dbPath));
    }

    [Fact]
    public async Task InitializeAsync_IsIdempotent()
    {
        // Multiple initializations should not throw.
        await _storage.InitializeAsync();
        await _storage.InitializeAsync();
    }

    [Fact]
    public async Task WriteLogsAsync_AndReadLogsAsync_RoundTrip()
    {
        // Arrange
        await _storage.InitializeAsync();
        var resourceLogs = CreateResourceLogs("TestService", "instance-1");

        // Act
        await _storage.WriteLogsAsync(resourceLogs);

        // Assert
        var items = new List<ResourceLogs>();
        await foreach (var item in _storage.ReadLogsAsync())
        {
            items.Add(item);
        }

        Assert.Single(items);
        Assert.Equal(resourceLogs.Resource.Attributes.Count, items[0].Resource.Attributes.Count);
        Assert.Equal(resourceLogs.ScopeLogs.Count, items[0].ScopeLogs.Count);
        Assert.Equal(
            resourceLogs.ScopeLogs[0].LogRecords[0].Body.StringValue,
            items[0].ScopeLogs[0].LogRecords[0].Body.StringValue);
    }

    [Fact]
    public async Task WriteLogsAsync_MultipleEntries_PreservesOrder()
    {
        // Arrange
        await _storage.InitializeAsync();

        var baseNanos = (ulong)new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks * 100;
        var rl1 = CreateResourceLogs("Service1", "i1", timeUnixNano: baseNanos + 1000);
        var rl2 = CreateResourceLogs("Service2", "i2", timeUnixNano: baseNanos + 2000);
        var rl3 = CreateResourceLogs("Service3", "i3", timeUnixNano: baseNanos + 3000);

        // Act
        await _storage.WriteLogsAsync(rl1);
        await _storage.WriteLogsAsync(rl2);
        await _storage.WriteLogsAsync(rl3);

        // Assert — should be returned in ascending timestamp order
        var items = new List<ResourceLogs>();
        await foreach (var item in _storage.ReadLogsAsync())
        {
            items.Add(item);
        }

        Assert.Equal(3, items.Count);
        Assert.Equal("Service1", GetServiceName(items[0].Resource));
        Assert.Equal("Service2", GetServiceName(items[1].Resource));
        Assert.Equal("Service3", GetServiceName(items[2].Resource));
    }

    [Fact]
    public async Task ReadLogsAsync_EmptyDatabase_ReturnsEmpty()
    {
        // Arrange
        await _storage.InitializeAsync();

        // Act
        var items = new List<ResourceLogs>();
        await foreach (var item in _storage.ReadLogsAsync())
        {
            items.Add(item);
        }

        // Assert
        Assert.Empty(items);
    }

    [Fact]
    public async Task WriteSpansAsync_AndReadSpansAsync_RoundTrip()
    {
        // Arrange
        await _storage.InitializeAsync();
        var resourceSpans = new ResourceSpans
        {
            Resource = CreateResource("SpanService", "span-i1"),
            ScopeSpans = { new ScopeSpans { Spans = { new Span { Name = "TestSpan" } } } }
        };

        // Act
        await _storage.WriteSpansAsync(resourceSpans);

        // Assert
        var items = new List<ResourceSpans>();
        await foreach (var item in _storage.ReadSpansAsync())
        {
            items.Add(item);
        }

        Assert.Single(items);
        Assert.Equal("TestSpan", items[0].ScopeSpans[0].Spans[0].Name);
    }

    [Fact]
    public async Task WriteMetricsAsync_AndReadMetricsAsync_RoundTrip()
    {
        // Arrange
        await _storage.InitializeAsync();
        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource("MetricService", "metric-i1"),
            ScopeMetrics = { new ScopeMetrics { Metrics = { new Metric { Name = "TestMetric" } } } }
        };

        // Act
        await _storage.WriteMetricsAsync(resourceMetrics);

        // Assert
        var items = new List<ResourceMetrics>();
        await foreach (var item in _storage.ReadMetricsAsync())
        {
            items.Add(item);
        }

        Assert.Single(items);
        Assert.Equal("TestMetric", items[0].ScopeMetrics[0].Metrics[0].Name);
    }

    [Fact]
    public async Task WriteLogsAsync_WithNullResource_DoesNotThrow()
    {
        // Arrange
        await _storage.InitializeAsync();
        var resourceLogs = new ResourceLogs
        {
            ScopeLogs = { new ScopeLogs { LogRecords = { new LogRecord { Body = new AnyValue { StringValue = "no-resource" } } } } }
        };

        // Act & Assert — should not throw even with a null/missing resource
        await _storage.WriteLogsAsync(resourceLogs);
    }

    [Fact]
    public async Task DisposeAsync_DoesNotThrow()
    {
        await _storage.InitializeAsync();
        await _storage.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_BeforeInitialize_DoesNotThrow()
    {
        // Should not throw even if Initialize was never called.
        await _storage.DisposeAsync();
    }

    private static ResourceLogs CreateResourceLogs(string serviceName, string instanceId, ulong timeUnixNano = 0)
    {
        return new ResourceLogs
        {
            Resource = CreateResource(serviceName, instanceId),
            ScopeLogs =
            {
                new ScopeLogs
                {
                    LogRecords =
                    {
                        new LogRecord
                        {
                            TimeUnixNano = timeUnixNano,
                            Body = new AnyValue { StringValue = $"Log from {serviceName}" }
                        }
                    }
                }
            }
        };
    }

    private static Resource CreateResource(string serviceName, string instanceId) => new()
    {
        Attributes =
        {
            new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = serviceName } },
            new KeyValue { Key = "service.instance.id", Value = new AnyValue { StringValue = instanceId } }
        }
    };

    private static string GetServiceName(Resource resource)
    {
        foreach (var attr in resource.Attributes)
        {
            if (attr.Key == "service.name")
            {
                return attr.Value.StringValue;
            }
        }

        return string.Empty;
    }
}
