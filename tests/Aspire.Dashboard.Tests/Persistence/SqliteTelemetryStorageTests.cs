// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.Otlp.Storage.Persistence;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;
using Xunit;

namespace Aspire.Dashboard.Tests.Persistence;

public class SqliteTelemetryStorageTests : IAsyncDisposable
{
    private readonly string _databasePath;
    private readonly SqliteTelemetryStorage _storage;

    public SqliteTelemetryStorageTests()
    {
        _databasePath = Path.Combine(Path.GetTempPath(), $"aspire-test-{Guid.NewGuid():N}.db");
        _storage = new SqliteTelemetryStorage(_databasePath);
    }

    public async ValueTask DisposeAsync()
    {
        await _storage.DisposeAsync();
        if (File.Exists(_databasePath))
        {
            File.Delete(_databasePath);
        }
    }

    [Fact]
    public async Task InitializeAsync_CreatesDatabase()
    {
        // Act
        await _storage.InitializeAsync();

        // Assert
        Assert.True(File.Exists(_databasePath));
    }

    [Fact]
    public async Task InitializeAsync_CanBeCalledMultipleTimes()
    {
        // Act & Assert (idempotent)
        await _storage.InitializeAsync();
        await _storage.InitializeAsync();
    }

    [Fact]
    public async Task WriteLogsAsync_And_ReadLogsAsync_RoundTrips()
    {
        // Arrange
        await _storage.InitializeAsync();
        var resourceLogs = CreateResourceLogs("TestService");

        // Act
        await _storage.WriteLogsAsync(resourceLogs);
        var items = new List<ResourceLogs>();
        await foreach (var item in _storage.ReadLogsAsync())
        {
            items.Add(item);
        }

        // Assert
        Assert.Single(items);
        Assert.Equal("TestService", items[0].Resource.Attributes[0].Value.StringValue);
    }

    [Fact]
    public async Task WriteSpansAsync_And_ReadSpansAsync_RoundTrips()
    {
        // Arrange
        await _storage.InitializeAsync();
        var resourceSpans = CreateResourceSpans("TraceService");

        // Act
        await _storage.WriteSpansAsync(resourceSpans);
        var items = new List<ResourceSpans>();
        await foreach (var item in _storage.ReadSpansAsync())
        {
            items.Add(item);
        }

        // Assert
        Assert.Single(items);
        Assert.Equal("TraceService", items[0].Resource.Attributes[0].Value.StringValue);
    }

    [Fact]
    public async Task WriteMetricsAsync_And_ReadMetricsAsync_RoundTrips()
    {
        // Arrange
        await _storage.InitializeAsync();
        var resourceMetrics = CreateResourceMetrics("MetricsService");

        // Act
        await _storage.WriteMetricsAsync(resourceMetrics);
        var items = new List<ResourceMetrics>();
        await foreach (var item in _storage.ReadMetricsAsync())
        {
            items.Add(item);
        }

        // Assert
        Assert.Single(items);
        Assert.Equal("MetricsService", items[0].Resource.Attributes[0].Value.StringValue);
    }

    [Fact]
    public async Task ReadLogsAsync_ReturnsEmpty_WhenNoLogsWritten()
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
    public async Task ReadSpansAsync_ReturnsEmpty_WhenNoSpansWritten()
    {
        // Arrange
        await _storage.InitializeAsync();

        // Act
        var items = new List<ResourceSpans>();
        await foreach (var item in _storage.ReadSpansAsync())
        {
            items.Add(item);
        }

        // Assert
        Assert.Empty(items);
    }

    [Fact]
    public async Task ReadMetricsAsync_ReturnsEmpty_WhenNoMetricsWritten()
    {
        // Arrange
        await _storage.InitializeAsync();

        // Act
        var items = new List<ResourceMetrics>();
        await foreach (var item in _storage.ReadMetricsAsync())
        {
            items.Add(item);
        }

        // Assert
        Assert.Empty(items);
    }

    [Fact]
    public async Task WriteMultiple_ReadLogs_ReturnsAllInInsertionOrder()
    {
        // Arrange
        await _storage.InitializeAsync();
        var logs1 = CreateResourceLogs("Service1");
        var logs2 = CreateResourceLogs("Service2");
        var logs3 = CreateResourceLogs("Service3");

        // Act
        await _storage.WriteLogsAsync(logs1);
        await _storage.WriteLogsAsync(logs2);
        await _storage.WriteLogsAsync(logs3);

        var items = new List<ResourceLogs>();
        await foreach (var item in _storage.ReadLogsAsync())
        {
            items.Add(item);
        }

        // Assert
        Assert.Equal(3, items.Count);
        Assert.Equal("Service1", items[0].Resource.Attributes[0].Value.StringValue);
        Assert.Equal("Service2", items[1].Resource.Attributes[0].Value.StringValue);
        Assert.Equal("Service3", items[2].Resource.Attributes[0].Value.StringValue);
    }

    [Fact]
    public async Task DisposeAsync_DoesNotThrow()
    {
        // Arrange
        await _storage.InitializeAsync();

        // Act & Assert
        await _storage.DisposeAsync();
    }

    [Fact]
    public void Constructor_ThrowsForNullPath()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new SqliteTelemetryStorage(null!));
    }

    [Fact]
    public void Constructor_ThrowsForEmptyPath()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() => new SqliteTelemetryStorage(""));
    }

    private static ResourceLogs CreateResourceLogs(string serviceName) => new()
    {
        Resource = CreateResource(serviceName),
        ScopeLogs = { new ScopeLogs { LogRecords = { new LogRecord() } } }
    };

    private static ResourceSpans CreateResourceSpans(string serviceName) => new()
    {
        Resource = CreateResource(serviceName),
        ScopeSpans = { new ScopeSpans { Spans = { new Span() } } }
    };

    private static ResourceMetrics CreateResourceMetrics(string serviceName) => new()
    {
        Resource = CreateResource(serviceName),
        ScopeMetrics = { new ScopeMetrics { Metrics = { new Metric() } } }
    };

    private static Resource CreateResource(string serviceName) => new()
    {
        Attributes =
        {
            new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = serviceName } }
        }
    };
}
