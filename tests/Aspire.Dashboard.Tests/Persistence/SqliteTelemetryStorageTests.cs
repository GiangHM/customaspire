// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.Otlp.Storage.Persistence;
using Microsoft.Extensions.Logging.Abstractions;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;
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

    // ─── Metrics roundtrip ────────────────────────────────────────────────────

    [Fact]
    public async Task WriteMetricsAsync_AndReadBack_GaugeMetric_RoundTrips()
    {
        // Arrange
        await _storage.InitializeAsync();

        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource("svc-gauge", "inst-1"),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "my.meter", Version = "1.0" },
                    Metrics =
                    {
                        new Metric
                        {
                            Name = "cpu.usage",
                            Description = "CPU usage",
                            Unit = "%",
                            Gauge = new Gauge
                            {
                                DataPoints =
                                {
                                    new NumberDataPoint
                                    {
                                        AsDouble = 42.5,
                                        StartTimeUnixNano = 1_000_000,
                                        TimeUnixNano = 2_000_000
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        // Act
        await _storage.WriteMetricsAsync(resourceMetrics);

        var read = new List<ResourceMetrics>();
        await foreach (var rm in _storage.ReadMetricsAsync())
        {
            read.Add(rm);
        }

        // Assert
        var result = Assert.Single(read);
        Assert.Equal("svc-gauge", GetAttr(result.Resource, "service.name"));
        Assert.Equal("inst-1", GetAttr(result.Resource, "service.instance.id"));

        var scopeMetrics = Assert.Single(result.ScopeMetrics);
        Assert.Equal("my.meter", scopeMetrics.Scope.Name);
        Assert.Equal("1.0", scopeMetrics.Scope.Version);

        var metric = Assert.Single(scopeMetrics.Metrics);
        Assert.Equal("cpu.usage", metric.Name);
        Assert.Equal("CPU usage", metric.Description);
        Assert.Equal("%", metric.Unit);
        Assert.Equal(Metric.DataOneofCase.Gauge, metric.DataCase);

        var dp = Assert.Single(metric.Gauge.DataPoints);
        Assert.Equal(42.5, dp.AsDouble);
        Assert.Equal(1_000_000UL, dp.StartTimeUnixNano);
        Assert.Equal(2_000_000UL, dp.TimeUnixNano);
    }

    [Fact]
    public async Task WriteMetricsAsync_AndReadBack_SumMetricWithLongValue_RoundTrips()
    {
        // Arrange
        await _storage.InitializeAsync();

        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource("svc-sum", "inst-2"),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "requests.meter" },
                    Metrics =
                    {
                        new Metric
                        {
                            Name = "http.requests",
                            Description = "Total HTTP requests",
                            Unit = "requests",
                            Sum = new Sum
                            {
                                AggregationTemporality = AggregationTemporality.Cumulative,
                                IsMonotonic = true,
                                DataPoints =
                                {
                                    new NumberDataPoint
                                    {
                                        AsInt = 100,
                                        StartTimeUnixNano = 1_000_000,
                                        TimeUnixNano = 3_000_000,
                                        Attributes =
                                        {
                                            new KeyValue { Key = "method", Value = new AnyValue { StringValue = "GET" } },
                                            new KeyValue { Key = "status", Value = new AnyValue { StringValue = "200" } }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        // Act
        await _storage.WriteMetricsAsync(resourceMetrics);

        var read = new List<ResourceMetrics>();
        await foreach (var rm in _storage.ReadMetricsAsync())
        {
            read.Add(rm);
        }

        // Assert
        var result = Assert.Single(read);
        var scopeMetrics = Assert.Single(result.ScopeMetrics);
        var metric = Assert.Single(scopeMetrics.Metrics);

        Assert.Equal(Metric.DataOneofCase.Sum, metric.DataCase);
        Assert.Equal(AggregationTemporality.Cumulative, metric.Sum.AggregationTemporality);
        Assert.True(metric.Sum.IsMonotonic);

        var dp = Assert.Single(metric.Sum.DataPoints);
        Assert.Equal(100L, dp.AsInt);
        Assert.Equal(2, dp.Attributes.Count);
        Assert.Equal("method", dp.Attributes[0].Key);
        Assert.Equal("GET", dp.Attributes[0].Value.StringValue);
        Assert.Equal("status", dp.Attributes[1].Key);
        Assert.Equal("200", dp.Attributes[1].Value.StringValue);
    }

    [Fact]
    public async Task WriteMetricsAsync_AndReadBack_HistogramMetric_RoundTrips()
    {
        // Arrange
        await _storage.InitializeAsync();

        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource("svc-hist", "inst-3"),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "latency.meter" },
                    Metrics =
                    {
                        new Metric
                        {
                            Name = "http.latency",
                            Description = "HTTP latency distribution",
                            Unit = "ms",
                            Histogram = new Histogram
                            {
                                AggregationTemporality = AggregationTemporality.Delta,
                                DataPoints =
                                {
                                    new HistogramDataPoint
                                    {
                                        Count = 10,
                                        Sum = 150.5,
                                        StartTimeUnixNano = 1_000_000,
                                        TimeUnixNano = 5_000_000,
                                        ExplicitBounds = { 10.0, 50.0, 100.0 },
                                        BucketCounts = { 2, 3, 4, 1 }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        // Act
        await _storage.WriteMetricsAsync(resourceMetrics);

        var read = new List<ResourceMetrics>();
        await foreach (var rm in _storage.ReadMetricsAsync())
        {
            read.Add(rm);
        }

        // Assert
        var result = Assert.Single(read);
        var scopeMetrics = Assert.Single(result.ScopeMetrics);
        var metric = Assert.Single(scopeMetrics.Metrics);

        Assert.Equal("http.latency", metric.Name);
        Assert.Equal(Metric.DataOneofCase.Histogram, metric.DataCase);
        Assert.Equal(AggregationTemporality.Delta, metric.Histogram.AggregationTemporality);

        var dp = Assert.Single(metric.Histogram.DataPoints);
        Assert.Equal(10UL, dp.Count);
        Assert.Equal(150.5, dp.Sum);
        Assert.Equal(1_000_000UL, dp.StartTimeUnixNano);
        Assert.Equal(5_000_000UL, dp.TimeUnixNano);
        Assert.Equal([10.0, 50.0, 100.0], dp.ExplicitBounds);
        Assert.Equal([2UL, 3UL, 4UL, 1UL], dp.BucketCounts);
    }

    [Fact]
    public async Task WriteMetricsAsync_MultipleResources_EachResourceYieldedSeparately()
    {
        // Arrange
        await _storage.InitializeAsync();

        var rm1 = new ResourceMetrics
        {
            Resource = CreateResource("svc-a", "inst-a"),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "meter-a" },
                    Metrics = { CreateGaugeMetric("metric-a", 1.0) }
                }
            }
        };

        var rm2 = new ResourceMetrics
        {
            Resource = CreateResource("svc-b", "inst-b"),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "meter-b" },
                    Metrics = { CreateGaugeMetric("metric-b", 2.0) }
                }
            }
        };

        // Act
        await _storage.WriteMetricsAsync(rm1);
        await _storage.WriteMetricsAsync(rm2);

        var read = new List<ResourceMetrics>();
        await foreach (var rm in _storage.ReadMetricsAsync())
        {
            read.Add(rm);
        }

        // Assert
        Assert.Equal(2, read.Count);
        Assert.Contains(read, r => GetAttr(r.Resource, "service.name") == "svc-a");
        Assert.Contains(read, r => GetAttr(r.Resource, "service.name") == "svc-b");
    }

    [Fact]
    public async Task WriteMetricsAsync_MultipleScopes_AllScopesPreserved()
    {
        // Arrange
        await _storage.InitializeAsync();

        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource("multi-scope-svc", "inst-1"),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "meter.one" },
                    Metrics = { CreateGaugeMetric("metric-1", 10.0) }
                },
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "meter.two" },
                    Metrics = { CreateGaugeMetric("metric-2", 20.0) }
                }
            }
        };

        // Act
        await _storage.WriteMetricsAsync(resourceMetrics);

        var read = new List<ResourceMetrics>();
        await foreach (var rm in _storage.ReadMetricsAsync())
        {
            read.Add(rm);
        }

        // Assert
        var result = Assert.Single(read);
        Assert.Equal(2, result.ScopeMetrics.Count);

        var scopeNames = result.ScopeMetrics.Select(s => s.Scope.Name).ToList();
        Assert.Contains("meter.one", scopeNames);
        Assert.Contains("meter.two", scopeNames);
    }

    [Fact]
    public async Task WriteMetricsAsync_SameResourceTwice_DataPointsAccumulate()
    {
        // Arrange
        await _storage.InitializeAsync();

        var rm1 = new ResourceMetrics
        {
            Resource = CreateResource("svc-same", "inst-x"),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "my.meter" },
                    Metrics = { CreateSumMetric("counter", 1, AggregationTemporality.Cumulative) }
                }
            }
        };

        var rm2 = new ResourceMetrics
        {
            Resource = CreateResource("svc-same", "inst-x"),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "my.meter" },
                    Metrics = { CreateSumMetric("counter", 2, AggregationTemporality.Cumulative) }
                }
            }
        };

        // Act
        await _storage.WriteMetricsAsync(rm1);
        await _storage.WriteMetricsAsync(rm2);

        var read = new List<ResourceMetrics>();
        await foreach (var rm in _storage.ReadMetricsAsync())
        {
            read.Add(rm);
        }

        // Assert — same resource/meter/instrument, so one ResourceMetrics is returned
        // but the two data points are both persisted.
        var result = Assert.Single(read);
        var scopeMetrics = Assert.Single(result.ScopeMetrics);
        var metric = Assert.Single(scopeMetrics.Metrics);
        Assert.Equal(2, metric.Sum.DataPoints.Count);
    }

    [Fact]
    public async Task ReadMetricsAsync_EmptyDatabase_ReturnsEmpty()
    {
        // Arrange
        await _storage.InitializeAsync();

        // Act
        var read = new List<ResourceMetrics>();
        await foreach (var rm in _storage.ReadMetricsAsync())
        {
            read.Add(rm);
        }

        // Assert
        Assert.Empty(read);
    }

    // ─── Logs roundtrip ───────────────────────────────────────────────────────

    [Fact]
    public async Task WriteLogsAsync_AndReadBack_RoundTrips()
    {
        // Arrange
        await _storage.InitializeAsync();

        var resourceLogs = new OpenTelemetry.Proto.Logs.V1.ResourceLogs
        {
            Resource = CreateResource("log-svc", "log-inst"),
            ScopeLogs =
            {
                new OpenTelemetry.Proto.Logs.V1.ScopeLogs
                {
                    Scope = new InstrumentationScope { Name = "log-scope" },
                    LogRecords =
                    {
                        new OpenTelemetry.Proto.Logs.V1.LogRecord
                        {
                            TimeUnixNano = 1_000_000,
                            Body = new AnyValue { StringValue = "Hello log" }
                        }
                    }
                }
            }
        };

        // Act
        await _storage.WriteLogsAsync(resourceLogs);

        var read = new List<OpenTelemetry.Proto.Logs.V1.ResourceLogs>();
        await foreach (var rl in _storage.ReadLogsAsync())
        {
            read.Add(rl);
        }

        // Assert
        var result = Assert.Single(read);
        Assert.Equal("log-svc", GetAttr(result.Resource, "service.name"));
        var scopeLogs = Assert.Single(result.ScopeLogs);
        var logRecord = Assert.Single(scopeLogs.LogRecords);
        Assert.Equal("Hello log", logRecord.Body.StringValue);
    }

    // ─── Spans roundtrip ──────────────────────────────────────────────────────

    [Fact]
    public async Task WriteSpansAsync_AndReadBack_RoundTrips()
    {
        // Arrange
        await _storage.InitializeAsync();

        var traceId = Google.Protobuf.ByteString.CopyFrom(new byte[16]);
        var spanId = Google.Protobuf.ByteString.CopyFrom(new byte[8]);

        var resourceSpans = new OpenTelemetry.Proto.Trace.V1.ResourceSpans
        {
            Resource = CreateResource("trace-svc", "trace-inst"),
            ScopeSpans =
            {
                new OpenTelemetry.Proto.Trace.V1.ScopeSpans
                {
                    Scope = new InstrumentationScope { Name = "trace-scope" },
                    Spans =
                    {
                        new OpenTelemetry.Proto.Trace.V1.Span
                        {
                            TraceId = traceId,
                            SpanId = spanId,
                            Name = "my-span",
                            StartTimeUnixNano = 1_000_000,
                            EndTimeUnixNano = 2_000_000
                        }
                    }
                }
            }
        };

        // Act
        await _storage.WriteSpansAsync(resourceSpans);

        var read = new List<OpenTelemetry.Proto.Trace.V1.ResourceSpans>();
        await foreach (var rs in _storage.ReadSpansAsync())
        {
            read.Add(rs);
        }

        // Assert
        var result = Assert.Single(read);
        Assert.Equal("trace-svc", GetAttr(result.Resource, "service.name"));
        var scopeSpans = Assert.Single(result.ScopeSpans);
        var span = Assert.Single(scopeSpans.Spans);
        Assert.Equal("my-span", span.Name);
        Assert.Equal(1_000_000UL, span.StartTimeUnixNano);
        Assert.Equal(2_000_000UL, span.EndTimeUnixNano);
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private static Resource CreateResource(string serviceName, string instanceId) => new()
    {
        Attributes =
        {
            new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = serviceName } },
            new KeyValue { Key = "service.instance.id", Value = new AnyValue { StringValue = instanceId } }
        }
    };

    private static Metric CreateGaugeMetric(string name, double value) => new()
    {
        Name = name,
        Description = $"Description of {name}",
        Unit = "units",
        Gauge = new Gauge
        {
            DataPoints =
            {
                new NumberDataPoint
                {
                    AsDouble = value,
                    StartTimeUnixNano = 1_000_000,
                    TimeUnixNano = 2_000_000
                }
            }
        }
    };

    private static Metric CreateSumMetric(string name, int value, AggregationTemporality temporality) => new()
    {
        Name = name,
        Description = $"Description of {name}",
        Unit = "count",
        Sum = new Sum
        {
            AggregationTemporality = temporality,
            IsMonotonic = true,
            DataPoints =
            {
                new NumberDataPoint
                {
                    AsInt = value,
                    StartTimeUnixNano = 1_000_000,
                    TimeUnixNano = 2_000_000
                }
            }
        }
    };

    private static string? GetAttr(Resource resource, string key)
    {
        foreach (var kv in resource.Attributes)
        {
            if (kv.Key == key)
            {
                return kv.Value?.StringValue;
            }
        }
        return null;
    }
}
