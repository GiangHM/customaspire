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

public class NullTelemetryStorageTests
{
    [Fact]
    public async Task InitializeAsync_DoesNotThrow()
    {
        // Arrange
        var storage = NullTelemetryStorage.Instance;

        // Act & Assert
        await storage.InitializeAsync();
    }

    [Fact]
    public async Task WriteLogsAsync_DoesNotThrow()
    {
        // Arrange
        var storage = NullTelemetryStorage.Instance;
        var resourceLogs = new ResourceLogs
        {
            Resource = CreateResource(),
            ScopeLogs = { new ScopeLogs { LogRecords = { new LogRecord() } } }
        };

        // Act & Assert
        await storage.WriteLogsAsync(resourceLogs);
    }

    [Fact]
    public async Task WriteSpansAsync_DoesNotThrow()
    {
        // Arrange
        var storage = NullTelemetryStorage.Instance;
        var resourceSpans = new ResourceSpans
        {
            Resource = CreateResource(),
            ScopeSpans = { new ScopeSpans { Spans = { new Span() } } }
        };

        // Act & Assert
        await storage.WriteSpansAsync(resourceSpans);
    }

    [Fact]
    public async Task WriteMetricsAsync_DoesNotThrow()
    {
        // Arrange
        var storage = NullTelemetryStorage.Instance;
        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource(),
            ScopeMetrics = { new ScopeMetrics { Metrics = { new Metric() } } }
        };

        // Act & Assert
        await storage.WriteMetricsAsync(resourceMetrics);
    }

    [Fact]
    public async Task ReadLogsAsync_ReturnsEmpty()
    {
        // Arrange
        var storage = NullTelemetryStorage.Instance;

        // Act
        var items = new List<ResourceLogs>();
        await foreach (var item in storage.ReadLogsAsync())
        {
            items.Add(item);
        }

        // Assert
        Assert.Empty(items);
    }

    [Fact]
    public async Task ReadSpansAsync_ReturnsEmpty()
    {
        // Arrange
        var storage = NullTelemetryStorage.Instance;

        // Act
        var items = new List<ResourceSpans>();
        await foreach (var item in storage.ReadSpansAsync())
        {
            items.Add(item);
        }

        // Assert
        Assert.Empty(items);
    }

    [Fact]
    public async Task ReadMetricsAsync_ReturnsEmpty()
    {
        // Arrange
        var storage = NullTelemetryStorage.Instance;

        // Act
        var items = new List<ResourceMetrics>();
        await foreach (var item in storage.ReadMetricsAsync())
        {
            items.Add(item);
        }

        // Assert
        Assert.Empty(items);
    }

    [Fact]
    public async Task DisposeAsync_DoesNotThrow()
    {
        // Arrange
        var storage = NullTelemetryStorage.Instance;

        // Act & Assert
        await storage.DisposeAsync();
    }

    private static Resource CreateResource() => new()
    {
        Attributes =
        {
            new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = "TestService" } }
        }
    };
}
