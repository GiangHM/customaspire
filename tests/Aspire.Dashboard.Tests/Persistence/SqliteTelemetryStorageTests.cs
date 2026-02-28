// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.Otlp.Storage.Persistence;
using Google.Protobuf;
using Microsoft.Extensions.Logging.Abstractions;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;
using System.Text;
using Xunit;

namespace Aspire.Dashboard.Tests.Persistence;

public sealed class SqliteTelemetryStorageTests : IAsyncDisposable
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
            try
            {
                File.Delete(_dbPath);
            }
            catch
            {
                // Best-effort cleanup; don't let file-locking issues mask test failures.
            }
        }
    }

    [Fact]
    public async Task InitializeAsync_CreatesDatabase()
    {
        await _storage.InitializeAsync();

        Assert.True(File.Exists(_dbPath));
    }

    [Fact]
    public async Task InitializeAsync_IsIdempotent()
    {
        // Multiple initializations should not throw.
        await _storage.InitializeAsync();
        await _storage.InitializeAsync();
    }

    // ---- Logs tests ----

    [Fact]
    public async Task WriteLogsAsync_AndReadLogsAsync_RoundTrip()
    {
        await _storage.InitializeAsync();
        var resourceLogs = CreateResourceLogs("TestService", "instance-1");

        await _storage.WriteLogsAsync(resourceLogs);

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
        await _storage.InitializeAsync();

        var baseNanos = (ulong)new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds() * 1_000_000;
        var rl1 = CreateResourceLogs("Service1", "i1", timeUnixNano: baseNanos + 1000);
        var rl2 = CreateResourceLogs("Service2", "i2", timeUnixNano: baseNanos + 2000);
        var rl3 = CreateResourceLogs("Service3", "i3", timeUnixNano: baseNanos + 3000);

        await _storage.WriteLogsAsync(rl1);
        await _storage.WriteLogsAsync(rl2);
        await _storage.WriteLogsAsync(rl3);

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
        await _storage.InitializeAsync();

        var items = new List<ResourceLogs>();
        await foreach (var item in _storage.ReadLogsAsync())
        {
            items.Add(item);
        }

        Assert.Empty(items);
    }

    [Fact]
    public async Task WriteLogsAsync_WithNullResource_DoesNotThrow()
    {
        await _storage.InitializeAsync();
        var resourceLogs = new ResourceLogs
        {
            ScopeLogs = { new ScopeLogs { LogRecords = { new LogRecord { Body = new AnyValue { StringValue = "no-resource" } } } } }
        };

        // Should not throw even with a null/missing resource.
        await _storage.WriteLogsAsync(resourceLogs);
    }

    // ---- Spans tests ----

    [Fact]
    public async Task WriteSpansAsync_And_ReadSpansAsync_RoundTrips()
    {
        await _storage.InitializeAsync();

        var parentSpanId = ByteString.CopyFrom(Encoding.UTF8.GetBytes("parent01"));
        var childSpanId = ByteString.CopyFrom(Encoding.UTF8.GetBytes("child001"));
        var traceId = ByteString.CopyFrom(Encoding.UTF8.GetBytes("trace001traceid!"));

        var resourceSpans = new ResourceSpans
        {
            Resource = CreateResource("svc-a", "svc-a-i1"),
            ScopeSpans =
            {
                new ScopeSpans
                {
                    Scope = new InstrumentationScope { Name = "TestLib", Version = "1.0" },
                    Spans =
                    {
                        new Span
                        {
                            TraceId = traceId,
                            SpanId = parentSpanId,
                            ParentSpanId = ByteString.Empty,
                            Name = "parent-span",
                            Kind = Span.Types.SpanKind.Server,
                            StartTimeUnixNano = 1_000_000,
                            EndTimeUnixNano = 2_000_000,
                            Status = new Status { Code = Status.Types.StatusCode.Ok }
                        },
                        new Span
                        {
                            TraceId = traceId,
                            SpanId = childSpanId,
                            ParentSpanId = parentSpanId,
                            Name = "child-span",
                            Kind = Span.Types.SpanKind.Internal,
                            StartTimeUnixNano = 1_100_000,
                            EndTimeUnixNano = 1_900_000,
                            Status = new Status { Code = Status.Types.StatusCode.Unset }
                        }
                    }
                }
            }
        };

        await _storage.WriteSpansAsync(resourceSpans);

        var readBatches = new List<ResourceSpans>();
        await foreach (var batch in _storage.ReadSpansAsync())
        {
            readBatches.Add(batch);
        }

        Assert.Single(readBatches);
        var readBatch = readBatches[0];

        Assert.Single(readBatch.ScopeSpans);
        var scopeSpans = readBatch.ScopeSpans[0];
        Assert.Equal("TestLib", scopeSpans.Scope.Name);
        Assert.Equal(2, scopeSpans.Spans.Count);

        var parentRead = scopeSpans.Spans.First(s => s.SpanId == parentSpanId);
        var childRead = scopeSpans.Spans.First(s => s.SpanId == childSpanId);

        Assert.Equal("parent-span", parentRead.Name);
        Assert.True(parentRead.ParentSpanId.IsEmpty, "Root span should have no parent.");
        Assert.Equal(Span.Types.SpanKind.Server, parentRead.Kind);

        Assert.Equal("child-span", childRead.Name);
        Assert.Equal(parentSpanId, childRead.ParentSpanId);
        Assert.Equal(Span.Types.SpanKind.Internal, childRead.Kind);
    }

    [Fact]
    public async Task WriteSpansAsync_ParentChildRelation_PreservedInIndex()
    {
        await _storage.InitializeAsync();

        var parentSpanId = ByteString.CopyFrom(Encoding.UTF8.GetBytes("parent01"));
        var childSpanId = ByteString.CopyFrom(Encoding.UTF8.GetBytes("child001"));
        var traceId = ByteString.CopyFrom(Encoding.UTF8.GetBytes("trace001traceid!"));

        var resourceSpans = new ResourceSpans
        {
            Resource = CreateResource("svc-b", "svc-b-i1"),
            ScopeSpans =
            {
                new ScopeSpans
                {
                    Scope = new InstrumentationScope { Name = "ScopeB" },
                    Spans =
                    {
                        new Span
                        {
                            TraceId = traceId,
                            SpanId = parentSpanId,
                            ParentSpanId = ByteString.Empty,
                            Name = "root",
                            Kind = Span.Types.SpanKind.Server,
                            StartTimeUnixNano = 1_000,
                            EndTimeUnixNano = 9_000
                        },
                        new Span
                        {
                            TraceId = traceId,
                            SpanId = childSpanId,
                            ParentSpanId = parentSpanId,
                            Name = "child",
                            Kind = Span.Types.SpanKind.Client,
                            StartTimeUnixNano = 2_000,
                            EndTimeUnixNano = 8_000
                        }
                    }
                }
            }
        };

        await _storage.WriteSpansAsync(resourceSpans);

        // Verify that the parent-child link is preserved via round-trip read.
        var batches = new List<ResourceSpans>();
        await foreach (var b in _storage.ReadSpansAsync())
        {
            batches.Add(b);
        }

        Assert.Single(batches);
        var spans = batches[0].ScopeSpans[0].Spans;

        var root = spans.Single(s => s.SpanId == parentSpanId);
        var child = spans.Single(s => s.SpanId == childSpanId);

        Assert.True(root.ParentSpanId.IsEmpty, "Root span must have no parent.");
        Assert.Equal(parentSpanId, child.ParentSpanId);
    }

    [Fact]
    public async Task WriteSpansAsync_MultipleWrites_AllReadBack()
    {
        await _storage.InitializeAsync();

        var traceId = ByteString.CopyFrom(Encoding.UTF8.GetBytes("trace001traceid!"));

        for (var i = 0; i < 3; i++)
        {
            var resourceSpans = new ResourceSpans
            {
                Resource = CreateResource($"svc-{i}", $"svc-{i}-i1"),
                ScopeSpans =
                {
                    new ScopeSpans
                    {
                        Scope = new InstrumentationScope { Name = $"Scope{i}" },
                        Spans =
                        {
                            new Span
                            {
                                TraceId = traceId,
                                SpanId = ByteString.CopyFrom(Encoding.UTF8.GetBytes($"span0{i}01")),
                                ParentSpanId = ByteString.Empty,
                                Name = $"span-{i}",
                                Kind = Span.Types.SpanKind.Internal,
                                StartTimeUnixNano = (ulong)(i * 1_000),
                                EndTimeUnixNano = (ulong)(i * 1_000 + 500)
                            }
                        }
                    }
                }
            };

            await _storage.WriteSpansAsync(resourceSpans);
        }

        var batches = new List<ResourceSpans>();
        await foreach (var batch in _storage.ReadSpansAsync())
        {
            batches.Add(batch);
        }

        Assert.Equal(3, batches.Count);
    }

    [Fact]
    public async Task ReadSpansAsync_EmptyDatabase_ReturnsEmpty()
    {
        await _storage.InitializeAsync();

        var batches = new List<ResourceSpans>();
        await foreach (var batch in _storage.ReadSpansAsync())
        {
            batches.Add(batch);
        }

        Assert.Empty(batches);
    }

    // ---- Metrics tests ----

    [Fact]
    public async Task WriteMetricsAsync_AndReadMetricsAsync_RoundTrip()
    {
        await _storage.InitializeAsync();
        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource("MetricService", "metric-i1"),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Metrics =
                    {
                        new Metric
                        {
                            Name = "requests",
                            Sum = new Sum
                            {
                                DataPoints = { new NumberDataPoint { AsInt = 42, TimeUnixNano = 1_000 } }
                            }
                        }
                    }
                }
            }
        };

        await _storage.WriteMetricsAsync(resourceMetrics);

        var items = new List<ResourceMetrics>();
        await foreach (var item in _storage.ReadMetricsAsync())
        {
            items.Add(item);
        }

        Assert.Single(items);
        Assert.Equal("requests", items[0].ScopeMetrics[0].Metrics[0].Name);
        Assert.Equal(42, items[0].ScopeMetrics[0].Metrics[0].Sum.DataPoints[0].AsInt);
    }

    // ---- Dispose tests ----

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

    [Fact]
    public async Task DisposeAsync_CanBeCalledMultipleTimes()
    {
        await _storage.InitializeAsync();
        await _storage.DisposeAsync();
        await _storage.DisposeAsync();
    }

    // ---- Helpers ----

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
