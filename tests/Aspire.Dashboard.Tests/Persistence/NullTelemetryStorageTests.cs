// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.Otlp.Storage.Persistence;
using Google.Protobuf;
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
        var storage = NullTelemetryStorage.Instance;

        await storage.InitializeAsync();
    }

    [Fact]
    public async Task WriteLogsAsync_DoesNotThrow()
    {
        var storage = NullTelemetryStorage.Instance;
        var resourceLogs = new ResourceLogs
        {
            Resource = CreateResource(),
            ScopeLogs = { new ScopeLogs { LogRecords = { new LogRecord() } } }
        };

        await storage.WriteLogsAsync(resourceLogs);
    }

    [Fact]
    public async Task WriteLogsAsync_WithAllSeverities_DoesNotThrow()
    {
        var storage = NullTelemetryStorage.Instance;
        var scopeLogs = new ScopeLogs
        {
            Scope = new InstrumentationScope { Name = "TestLogger" },
            LogRecords =
            {
                new LogRecord { SeverityNumber = SeverityNumber.Trace, Body = new AnyValue { StringValue = "trace" } },
                new LogRecord { SeverityNumber = SeverityNumber.Debug, Body = new AnyValue { StringValue = "debug" } },
                new LogRecord { SeverityNumber = SeverityNumber.Info, Body = new AnyValue { StringValue = "info" } },
                new LogRecord { SeverityNumber = SeverityNumber.Warn, Body = new AnyValue { StringValue = "warn" } },
                new LogRecord { SeverityNumber = SeverityNumber.Error, Body = new AnyValue { StringValue = "error" } },
                new LogRecord { SeverityNumber = SeverityNumber.Fatal, Body = new AnyValue { StringValue = "fatal" } },
            }
        };
        var resourceLogs = new ResourceLogs
        {
            Resource = CreateResource(),
            ScopeLogs = { scopeLogs }
        };

        await storage.WriteLogsAsync(resourceLogs);
    }

    [Fact]
    public async Task WriteLogsAsync_WithAttributes_DoesNotThrow()
    {
        var storage = NullTelemetryStorage.Instance;
        var logRecord = new LogRecord
        {
            SeverityNumber = SeverityNumber.Info,
            Body = new AnyValue { StringValue = "Test {Log}" },
            TimeUnixNano = 1000,
            ObservedTimeUnixNano = 1000,
            Attributes =
            {
                new KeyValue { Key = "{OriginalFormat}", Value = new AnyValue { StringValue = "Test {Log}" } },
                new KeyValue { Key = "Log", Value = new AnyValue { StringValue = "Value!" } }
            }
        };
        var resourceLogs = new ResourceLogs
        {
            Resource = CreateResource(),
            ScopeLogs = { new ScopeLogs { LogRecords = { logRecord } } }
        };

        await storage.WriteLogsAsync(resourceLogs);
    }

    [Fact]
    public async Task WriteLogsAsync_WithCancellationToken_DoesNotThrow()
    {
        var storage = NullTelemetryStorage.Instance;
        var resourceLogs = new ResourceLogs
        {
            Resource = CreateResource(),
            ScopeLogs = { new ScopeLogs { LogRecords = { new LogRecord() } } }
        };

        using var cts = new CancellationTokenSource();
        await storage.WriteLogsAsync(resourceLogs, cts.Token);
    }

    [Fact]
    public async Task WriteSpansAsync_DoesNotThrow()
    {
        var storage = NullTelemetryStorage.Instance;
        var resourceSpans = new ResourceSpans
        {
            Resource = CreateResource(),
            ScopeSpans = { new ScopeSpans { Spans = { new Span() } } }
        };

        await storage.WriteSpansAsync(resourceSpans);
    }

    [Fact]
    public async Task WriteSpansAsync_WithFullSpanDetails_DoesNotThrow()
    {
        var storage = NullTelemetryStorage.Instance;
        var span = new Span
        {
            TraceId = ByteString.CopyFromUtf8("TestTraceId1234"),
            SpanId = ByteString.CopyFromUtf8("TestSpan"),
            Name = "Test span",
            Kind = Span.Types.SpanKind.Server,
            StartTimeUnixNano = 1000,
            EndTimeUnixNano = 2000,
            Attributes =
            {
                new KeyValue { Key = "http.method", Value = new AnyValue { StringValue = "GET" } },
                new KeyValue { Key = "http.status_code", Value = new AnyValue { IntValue = 200 } }
            },
            Events =
            {
                new Span.Types.Event
                {
                    Name = "test-event",
                    TimeUnixNano = 1500,
                    Attributes = { new KeyValue { Key = "event.attr", Value = new AnyValue { StringValue = "val" } } }
                }
            },
            Links =
            {
                new Span.Types.Link
                {
                    TraceId = ByteString.CopyFromUtf8("LinkedTraceId12"),
                    SpanId = ByteString.CopyFromUtf8("LinkedSpn"),
                    Attributes = { new KeyValue { Key = "link.attr", Value = new AnyValue { StringValue = "linked" } } }
                }
            },
            Status = new Status { Code = Status.Types.StatusCode.Ok, Message = "OK" }
        };
        var resourceSpans = new ResourceSpans
        {
            Resource = CreateResource(),
            ScopeSpans =
            {
                new ScopeSpans
                {
                    Scope = new InstrumentationScope { Name = "TestLibrary", Version = "1.0.0" },
                    Spans = { span }
                }
            }
        };

        await storage.WriteSpansAsync(resourceSpans);
    }

    [Fact]
    public async Task WriteSpansAsync_WithCancellationToken_DoesNotThrow()
    {
        var storage = NullTelemetryStorage.Instance;
        var resourceSpans = new ResourceSpans
        {
            Resource = CreateResource(),
            ScopeSpans = { new ScopeSpans { Spans = { new Span() } } }
        };

        using var cts = new CancellationTokenSource();
        await storage.WriteSpansAsync(resourceSpans, cts.Token);
    }

    [Fact]
    public async Task WriteMetricsAsync_DoesNotThrow()
    {
        var storage = NullTelemetryStorage.Instance;
        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource(),
            ScopeMetrics = { new ScopeMetrics { Metrics = { new Metric() } } }
        };

        await storage.WriteMetricsAsync(resourceMetrics);
    }

    [Fact]
    public async Task WriteMetricsAsync_WithSumMetric_DoesNotThrow()
    {
        var storage = NullTelemetryStorage.Instance;
        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource(),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "test-meter" },
                    Metrics =
                    {
                        new Metric
                        {
                            Name = "requests",
                            Description = "Total requests",
                            Unit = "count",
                            Sum = new Sum
                            {
                                IsMonotonic = true,
                                AggregationTemporality = AggregationTemporality.Cumulative,
                                DataPoints =
                                {
                                    new NumberDataPoint
                                    {
                                        StartTimeUnixNano = 1000,
                                        TimeUnixNano = 2000,
                                        AsInt = 42,
                                        Attributes = { new KeyValue { Key = "env", Value = new AnyValue { StringValue = "prod" } } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        await storage.WriteMetricsAsync(resourceMetrics);
    }

    [Fact]
    public async Task WriteMetricsAsync_WithGaugeMetric_DoesNotThrow()
    {
        var storage = NullTelemetryStorage.Instance;
        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource(),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "test-meter" },
                    Metrics =
                    {
                        new Metric
                        {
                            Name = "temperature",
                            Description = "Current temperature",
                            Unit = "celsius",
                            Gauge = new Gauge
                            {
                                DataPoints =
                                {
                                    new NumberDataPoint
                                    {
                                        TimeUnixNano = 2000,
                                        AsDouble = 23.5
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        await storage.WriteMetricsAsync(resourceMetrics);
    }

    [Fact]
    public async Task WriteMetricsAsync_WithHistogramMetric_DoesNotThrow()
    {
        var storage = NullTelemetryStorage.Instance;
        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource(),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "test-meter" },
                    Metrics =
                    {
                        new Metric
                        {
                            Name = "request.duration",
                            Description = "Request duration histogram",
                            Unit = "ms",
                            Histogram = new Histogram
                            {
                                AggregationTemporality = AggregationTemporality.Cumulative,
                                DataPoints =
                                {
                                    new HistogramDataPoint
                                    {
                                        Count = 10,
                                        Sum = 500.0,
                                        Min = 10.0,
                                        Max = 200.0,
                                        StartTimeUnixNano = 1000,
                                        TimeUnixNano = 2000,
                                        ExplicitBounds = { 10, 25, 50, 100, 200 },
                                        BucketCounts = { 1, 2, 3, 2, 1, 1 },
                                        Attributes = { new KeyValue { Key = "route", Value = new AnyValue { StringValue = "/api/test" } } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        await storage.WriteMetricsAsync(resourceMetrics);
    }

    [Fact]
    public async Task WriteMetricsAsync_WithExponentialHistogramMetric_DoesNotThrow()
    {
        var storage = NullTelemetryStorage.Instance;
        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource(),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "test-meter" },
                    Metrics =
                    {
                        new Metric
                        {
                            Name = "latency",
                            Description = "Exponential histogram latency",
                            Unit = "ms",
                            ExponentialHistogram = new ExponentialHistogram
                            {
                                AggregationTemporality = AggregationTemporality.Cumulative,
                                DataPoints =
                                {
                                    new ExponentialHistogramDataPoint
                                    {
                                        Count = 5,
                                        Sum = 100.0,
                                        Scale = 1,
                                        ZeroCount = 0,
                                        StartTimeUnixNano = 1000,
                                        TimeUnixNano = 2000,
                                        Positive = new ExponentialHistogramDataPoint.Types.Buckets { Offset = 0, BucketCounts = { 1, 2, 2 } },
                                        Negative = new ExponentialHistogramDataPoint.Types.Buckets { Offset = 0, BucketCounts = { 0 } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        await storage.WriteMetricsAsync(resourceMetrics);
    }

    [Fact]
    public async Task WriteMetricsAsync_WithCancellationToken_DoesNotThrow()
    {
        var storage = NullTelemetryStorage.Instance;
        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource(),
            ScopeMetrics = { new ScopeMetrics { Metrics = { new Metric() } } }
        };

        using var cts = new CancellationTokenSource();
        await storage.WriteMetricsAsync(resourceMetrics, cts.Token);
    }

    [Fact]
    public async Task ReadLogsAsync_ReturnsEmpty()
    {
        var storage = NullTelemetryStorage.Instance;

        var items = new List<ResourceLogs>();
        await foreach (var item in storage.ReadLogsAsync())
        {
            items.Add(item);
        }

        Assert.Empty(items);
    }

    [Fact]
    public async Task ReadLogsAsync_WithCancellationToken_ReturnsEmpty()
    {
        var storage = NullTelemetryStorage.Instance;

        using var cts = new CancellationTokenSource();
        var items = new List<ResourceLogs>();
        await foreach (var item in storage.ReadLogsAsync(cts.Token))
        {
            items.Add(item);
        }

        Assert.Empty(items);
    }

    [Fact]
    public async Task ReadSpansAsync_ReturnsEmpty()
    {
        var storage = NullTelemetryStorage.Instance;

        var items = new List<ResourceSpans>();
        await foreach (var item in storage.ReadSpansAsync())
        {
            items.Add(item);
        }

        Assert.Empty(items);
    }

    [Fact]
    public async Task ReadSpansAsync_WithCancellationToken_ReturnsEmpty()
    {
        var storage = NullTelemetryStorage.Instance;

        using var cts = new CancellationTokenSource();
        var items = new List<ResourceSpans>();
        await foreach (var item in storage.ReadSpansAsync(cts.Token))
        {
            items.Add(item);
        }

        Assert.Empty(items);
    }

    [Fact]
    public async Task ReadMetricsAsync_ReturnsEmpty()
    {
        var storage = NullTelemetryStorage.Instance;

        var items = new List<ResourceMetrics>();
        await foreach (var item in storage.ReadMetricsAsync())
        {
            items.Add(item);
        }

        Assert.Empty(items);
    }

    [Fact]
    public async Task ReadMetricsAsync_WithCancellationToken_ReturnsEmpty()
    {
        var storage = NullTelemetryStorage.Instance;

        using var cts = new CancellationTokenSource();
        var items = new List<ResourceMetrics>();
        await foreach (var item in storage.ReadMetricsAsync(cts.Token))
        {
            items.Add(item);
        }

        Assert.Empty(items);
    }

    [Fact]
    public async Task DisposeAsync_DoesNotThrow()
    {
        var storage = NullTelemetryStorage.Instance;

        await storage.DisposeAsync();
    }

    [Fact]
    public void Instance_IsSingleton()
    {
        var instance1 = NullTelemetryStorage.Instance;
        var instance2 = NullTelemetryStorage.Instance;

        Assert.Same(instance1, instance2);
    }

    private static Resource CreateResource() => new()
    {
        Attributes =
        {
            new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = "TestService" } }
        }
    };
}
