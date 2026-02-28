// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Runtime.CompilerServices;
using System.Text.Json;
using Aspire.Dashboard.Otlp.Model.Serialization;
using Aspire.Dashboard.Otlp.Storage.Persistence;
using Aspire.Otlp.Serialization;
using Google.Protobuf;
using OpenTelemetry.Proto.Common.V1;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Resource.V1;
using OpenTelemetry.Proto.Trace.V1;
using Xunit;

namespace Aspire.Dashboard.Tests.Persistence;

/// <summary>
/// Integration tests for ITelemetryStorage read/write contract and JSON (de-)serialization.
/// Uses an in-memory storage implementation to validate the full write-then-read cycle.
/// </summary>
public class InMemoryTelemetryStorageTests
{
    #region Read/Write integration tests

    [Fact]
    public async Task WriteAndReadLogsAsync_SingleBatch_ReturnsSameBatch()
    {
        await using var storage = new InMemoryTelemetryStorage();
        await storage.InitializeAsync();

        var resource = CreateResource("LogService", "log-01");
        var resourceLogs = new ResourceLogs
        {
            Resource = resource,
            ScopeLogs =
            {
                new ScopeLogs
                {
                    Scope = new InstrumentationScope { Name = "TestLogger" },
                    LogRecords =
                    {
                        CreateLogRecord(SeverityNumber.Info, "Test log message", 1000),
                        CreateLogRecord(SeverityNumber.Error, "Error message", 2000)
                    }
                }
            }
        };

        await storage.WriteLogsAsync(resourceLogs);

        var results = new List<ResourceLogs>();
        await foreach (var item in storage.ReadLogsAsync())
        {
            results.Add(item);
        }

        Assert.Single(results);
        var result = results[0];
        Assert.Equal("LogService", GetServiceName(result.Resource));
        Assert.Single(result.ScopeLogs);
        Assert.Equal("TestLogger", result.ScopeLogs[0].Scope.Name);
        Assert.Equal(2, result.ScopeLogs[0].LogRecords.Count);
    }

    [Fact]
    public async Task WriteAndReadLogsAsync_MultipleBatches_ReturnsAllBatches()
    {
        await using var storage = new InMemoryTelemetryStorage();
        await storage.InitializeAsync();

        var batch1 = new ResourceLogs { Resource = CreateResource("ServiceA", "a-01"), ScopeLogs = { new ScopeLogs { LogRecords = { CreateLogRecord(SeverityNumber.Info, "From A", 1000) } } } };
        var batch2 = new ResourceLogs { Resource = CreateResource("ServiceB", "b-01"), ScopeLogs = { new ScopeLogs { LogRecords = { CreateLogRecord(SeverityNumber.Warn, "From B", 2000) } } } };
        var batch3 = new ResourceLogs { Resource = CreateResource("ServiceC", "c-01"), ScopeLogs = { new ScopeLogs { LogRecords = { CreateLogRecord(SeverityNumber.Error, "From C", 3000) } } } };

        await storage.WriteLogsAsync(batch1);
        await storage.WriteLogsAsync(batch2);
        await storage.WriteLogsAsync(batch3);

        var results = new List<ResourceLogs>();
        await foreach (var item in storage.ReadLogsAsync())
        {
            results.Add(item);
        }

        Assert.Equal(3, results.Count);
        Assert.Equal("ServiceA", GetServiceName(results[0].Resource));
        Assert.Equal("ServiceB", GetServiceName(results[1].Resource));
        Assert.Equal("ServiceC", GetServiceName(results[2].Resource));
    }

    [Fact]
    public async Task WriteAndReadLogsAsync_WithCancellationToken_WorksCorrectly()
    {
        await using var storage = new InMemoryTelemetryStorage();
        await storage.InitializeAsync();

        var resourceLogs = new ResourceLogs
        {
            Resource = CreateResource("CancelService", "c-01"),
            ScopeLogs = { new ScopeLogs { LogRecords = { CreateLogRecord(SeverityNumber.Info, "Message", 1000) } } }
        };

        using var cts = new CancellationTokenSource();
        await storage.WriteLogsAsync(resourceLogs, cts.Token);

        var results = new List<ResourceLogs>();
        await foreach (var item in storage.ReadLogsAsync(cts.Token))
        {
            results.Add(item);
        }

        Assert.Single(results);
    }

    [Fact]
    public async Task WriteAndReadSpansAsync_SingleBatch_ReturnsSameBatch()
    {
        await using var storage = new InMemoryTelemetryStorage();
        await storage.InitializeAsync();

        var resourceSpans = new ResourceSpans
        {
            Resource = CreateResource("TraceService", "t-01"),
            ScopeSpans =
            {
                new ScopeSpans
                {
                    Scope = new InstrumentationScope { Name = "TestLibrary", Version = "1.0.0" },
                    Spans =
                    {
                        CreateSpan("trace001", "span001", null, "GET /api/test", Span.Types.SpanKind.Server),
                        CreateSpan("trace001", "span002", "span001", "db.query", Span.Types.SpanKind.Client)
                    }
                }
            }
        };

        await storage.WriteSpansAsync(resourceSpans);

        var results = new List<ResourceSpans>();
        await foreach (var item in storage.ReadSpansAsync())
        {
            results.Add(item);
        }

        Assert.Single(results);
        var result = results[0];
        Assert.Equal("TraceService", GetServiceName(result.Resource));
        Assert.Single(result.ScopeSpans);
        Assert.Equal("TestLibrary", result.ScopeSpans[0].Scope.Name);
        Assert.Equal(2, result.ScopeSpans[0].Spans.Count);
    }

    [Fact]
    public async Task WriteAndReadSpansAsync_SpanWithEventsAndLinks_PreservesDetails()
    {
        await using var storage = new InMemoryTelemetryStorage();
        await storage.InitializeAsync();

        var span = new Span
        {
            TraceId = ByteString.CopyFromUtf8("TestTraceId1234!"),
            SpanId = ByteString.CopyFromUtf8("TestSpan"),
            Name = "complex-span",
            Kind = Span.Types.SpanKind.Internal,
            StartTimeUnixNano = 1000,
            EndTimeUnixNano = 5000,
            Attributes =
            {
                new KeyValue { Key = "custom.key", Value = new AnyValue { StringValue = "custom.value" } }
            },
            Events =
            {
                new Span.Types.Event
                {
                    Name = "exception",
                    TimeUnixNano = 2000,
                    Attributes = { new KeyValue { Key = "exception.message", Value = new AnyValue { StringValue = "NullRef" } } }
                }
            },
            Links =
            {
                new Span.Types.Link
                {
                    TraceId = ByteString.CopyFromUtf8("LinkedTraceId123"),
                    SpanId = ByteString.CopyFromUtf8("LinkedSp")
                }
            },
            Status = new Status { Code = Status.Types.StatusCode.Error, Message = "error occurred" }
        };

        var resourceSpans = new ResourceSpans
        {
            Resource = CreateResource("TestService", "t-01"),
            ScopeSpans = { new ScopeSpans { Spans = { span } } }
        };

        await storage.WriteSpansAsync(resourceSpans);

        var results = new List<ResourceSpans>();
        await foreach (var item in storage.ReadSpansAsync())
        {
            results.Add(item);
        }

        Assert.Single(results);
        var resultSpan = results[0].ScopeSpans[0].Spans[0];
        Assert.Equal("complex-span", resultSpan.Name);
        Assert.Single(resultSpan.Events);
        Assert.Equal("exception", resultSpan.Events[0].Name);
        Assert.Single(resultSpan.Links);
        Assert.Equal(Status.Types.StatusCode.Error, resultSpan.Status.Code);
    }

    [Fact]
    public async Task WriteAndReadSpansAsync_MultipleBatches_ReturnsAllBatches()
    {
        await using var storage = new InMemoryTelemetryStorage();
        await storage.InitializeAsync();

        await storage.WriteSpansAsync(new ResourceSpans
        {
            Resource = CreateResource("Service1", "s1"),
            ScopeSpans = { new ScopeSpans { Spans = { CreateSpan("t1", "s1", null, "span1", Span.Types.SpanKind.Server) } } }
        });
        await storage.WriteSpansAsync(new ResourceSpans
        {
            Resource = CreateResource("Service2", "s2"),
            ScopeSpans = { new ScopeSpans { Spans = { CreateSpan("t2", "s2", null, "span2", Span.Types.SpanKind.Client) } } }
        });

        var results = new List<ResourceSpans>();
        await foreach (var item in storage.ReadSpansAsync())
        {
            results.Add(item);
        }

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task WriteAndReadMetricsAsync_WithSumMetric_PreservesValues()
    {
        await using var storage = new InMemoryTelemetryStorage();
        await storage.InitializeAsync();

        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource("MetricService", "m-01"),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "test-meter" },
                    Metrics =
                    {
                        new Metric
                        {
                            Name = "requests.total",
                            Description = "Total HTTP requests",
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
                                        AsInt = 100,
                                        Attributes = { new KeyValue { Key = "method", Value = new AnyValue { StringValue = "GET" } } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        await storage.WriteMetricsAsync(resourceMetrics);

        var results = new List<ResourceMetrics>();
        await foreach (var item in storage.ReadMetricsAsync())
        {
            results.Add(item);
        }

        Assert.Single(results);
        var result = results[0];
        Assert.Equal("MetricService", GetServiceName(result.Resource));
        Assert.Single(result.ScopeMetrics);
        Assert.Equal("test-meter", result.ScopeMetrics[0].Scope.Name);

        var metric = result.ScopeMetrics[0].Metrics[0];
        Assert.Equal("requests.total", metric.Name);
        Assert.NotNull(metric.Sum);
        Assert.True(metric.Sum.IsMonotonic);
        Assert.Single(metric.Sum.DataPoints);
        Assert.Equal(100L, metric.Sum.DataPoints[0].AsInt);
    }

    [Fact]
    public async Task WriteAndReadMetricsAsync_WithGaugeMetric_PreservesValues()
    {
        await using var storage = new InMemoryTelemetryStorage();
        await storage.InitializeAsync();

        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource("MetricService", "m-01"),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "test-meter" },
                    Metrics =
                    {
                        new Metric
                        {
                            Name = "cpu.usage",
                            Description = "CPU usage percentage",
                            Unit = "%",
                            Gauge = new Gauge
                            {
                                DataPoints =
                                {
                                    new NumberDataPoint
                                    {
                                        TimeUnixNano = 2000,
                                        AsDouble = 72.5
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        await storage.WriteMetricsAsync(resourceMetrics);

        var results = new List<ResourceMetrics>();
        await foreach (var item in storage.ReadMetricsAsync())
        {
            results.Add(item);
        }

        Assert.Single(results);
        var metric = results[0].ScopeMetrics[0].Metrics[0];
        Assert.Equal("cpu.usage", metric.Name);
        Assert.NotNull(metric.Gauge);
        Assert.Single(metric.Gauge.DataPoints);
        Assert.Equal(72.5, metric.Gauge.DataPoints[0].AsDouble);
    }

    [Fact]
    public async Task WriteAndReadMetricsAsync_WithHistogramMetric_PreservesAllBuckets()
    {
        await using var storage = new InMemoryTelemetryStorage();
        await storage.InitializeAsync();

        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource("MetricService", "m-01"),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "test-meter" },
                    Metrics =
                    {
                        new Metric
                        {
                            Name = "http.server.request.duration",
                            Description = "Request duration histogram",
                            Unit = "s",
                            Histogram = new Histogram
                            {
                                AggregationTemporality = AggregationTemporality.Cumulative,
                                DataPoints =
                                {
                                    new HistogramDataPoint
                                    {
                                        Count = 10,
                                        Sum = 5.0,
                                        Min = 0.001,
                                        Max = 2.0,
                                        StartTimeUnixNano = 1000,
                                        TimeUnixNano = 2000,
                                        ExplicitBounds = { 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0 },
                                        BucketCounts = { 1, 1, 1, 1, 2, 1, 1, 1, 0, 1, 0 },
                                        Attributes = { new KeyValue { Key = "http.method", Value = new AnyValue { StringValue = "GET" } } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        await storage.WriteMetricsAsync(resourceMetrics);

        var results = new List<ResourceMetrics>();
        await foreach (var item in storage.ReadMetricsAsync())
        {
            results.Add(item);
        }

        Assert.Single(results);
        var metric = results[0].ScopeMetrics[0].Metrics[0];
        Assert.Equal("http.server.request.duration", metric.Name);
        Assert.NotNull(metric.Histogram);
        Assert.Equal(AggregationTemporality.Cumulative, metric.Histogram.AggregationTemporality);

        var dp = metric.Histogram.DataPoints[0];
        Assert.Equal(10UL, dp.Count);
        Assert.Equal(5.0, dp.Sum);
        Assert.Equal(0.001, dp.Min);
        Assert.Equal(2.0, dp.Max);
        Assert.Equal(10, dp.ExplicitBounds.Count);
        Assert.Equal(11, dp.BucketCounts.Count);
    }

    [Fact]
    public async Task WriteAndReadMetricsAsync_MultipleBatches_ReturnsAllBatches()
    {
        await using var storage = new InMemoryTelemetryStorage();
        await storage.InitializeAsync();

        await storage.WriteMetricsAsync(new ResourceMetrics
        {
            Resource = CreateResource("Service1", "s1"),
            ScopeMetrics = { new ScopeMetrics { Metrics = { new Metric { Name = "m1", Gauge = new Gauge() } } } }
        });
        await storage.WriteMetricsAsync(new ResourceMetrics
        {
            Resource = CreateResource("Service2", "s2"),
            ScopeMetrics = { new ScopeMetrics { Metrics = { new Metric { Name = "m2", Sum = new Sum() } } } }
        });

        var results = new List<ResourceMetrics>();
        await foreach (var item in storage.ReadMetricsAsync())
        {
            results.Add(item);
        }

        Assert.Equal(2, results.Count);
        Assert.Equal("Service1", GetServiceName(results[0].Resource));
        Assert.Equal("Service2", GetServiceName(results[1].Resource));
    }

    [Fact]
    public async Task ReadLogsAsync_BeforeAnyWrites_ReturnsEmpty()
    {
        await using var storage = new InMemoryTelemetryStorage();
        await storage.InitializeAsync();

        var results = new List<ResourceLogs>();
        await foreach (var item in storage.ReadLogsAsync())
        {
            results.Add(item);
        }

        Assert.Empty(results);
    }

    [Fact]
    public async Task ReadSpansAsync_BeforeAnyWrites_ReturnsEmpty()
    {
        await using var storage = new InMemoryTelemetryStorage();
        await storage.InitializeAsync();

        var results = new List<ResourceSpans>();
        await foreach (var item in storage.ReadSpansAsync())
        {
            results.Add(item);
        }

        Assert.Empty(results);
    }

    [Fact]
    public async Task ReadMetricsAsync_BeforeAnyWrites_ReturnsEmpty()
    {
        await using var storage = new InMemoryTelemetryStorage();
        await storage.InitializeAsync();

        var results = new List<ResourceMetrics>();
        await foreach (var item in storage.ReadMetricsAsync())
        {
            results.Add(item);
        }

        Assert.Empty(results);
    }

    #endregion

    #region JSON serialization/deserialization tests

    [Fact]
    public void JsonSerialization_LogRecord_RoundTrip()
    {
        var original = new OtlpExportLogsServiceRequestJson
        {
            ResourceLogs =
            [
                new OtlpResourceLogsJson
                {
                    Resource = new OtlpResourceJson
                    {
                        Attributes =
                        [
                            new OtlpKeyValueJson { Key = "service.name", Value = new OtlpAnyValueJson { StringValue = "TestService" } }
                        ]
                    },
                    ScopeLogs =
                    [
                        new OtlpScopeLogsJson
                        {
                            Scope = new OtlpInstrumentationScopeJson { Name = "TestLogger" },
                            LogRecords =
                            [
                                new OtlpLogRecordJson
                                {
                                    TimeUnixNano = 1000,
                                    ObservedTimeUnixNano = 1000,
                                    SeverityNumber = (int)SeverityNumber.Info,
                                    SeverityText = "Information",
                                    Body = new OtlpAnyValueJson { StringValue = "Test log message" },
                                    TraceId = "0102030405060708090a0b0c0d0e0f10",
                                    SpanId = "0102030405060708",
                                    Attributes =
                                    [
                                        new OtlpKeyValueJson { Key = "attr.key", Value = new OtlpAnyValueJson { StringValue = "attr.value" } }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        };

        var json = JsonSerializer.Serialize(original, OtlpJsonSerializerContext.DefaultOptions);
        var deserialized = JsonSerializer.Deserialize<OtlpExportLogsServiceRequestJson>(json, OtlpJsonSerializerContext.DefaultOptions);

        Assert.NotNull(deserialized);
        Assert.NotNull(deserialized.ResourceLogs);
        Assert.Single(deserialized.ResourceLogs);

        var resourceLogs = deserialized.ResourceLogs[0];
        Assert.NotNull(resourceLogs.Resource?.Attributes);
        Assert.Contains(resourceLogs.Resource.Attributes, a => a.Key == "service.name" && a.Value?.StringValue == "TestService");

        Assert.Single(resourceLogs.ScopeLogs!);
        var scopeLogs = resourceLogs.ScopeLogs![0];
        Assert.Equal("TestLogger", scopeLogs.Scope?.Name);

        Assert.Single(scopeLogs.LogRecords!);
        var logRecord = scopeLogs.LogRecords![0];
        Assert.Equal(1000UL, logRecord.TimeUnixNano);
        Assert.Equal((int)SeverityNumber.Info, logRecord.SeverityNumber);
        Assert.Equal("Test log message", logRecord.Body?.StringValue);
        Assert.Equal("0102030405060708090a0b0c0d0e0f10", logRecord.TraceId);
        Assert.Equal("0102030405060708", logRecord.SpanId);
    }

    [Fact]
    public void JsonSerialization_LogRecord_AllSeverities_RoundTrip()
    {
        var severities = new[]
        {
            SeverityNumber.Trace,
            SeverityNumber.Debug,
            SeverityNumber.Info,
            SeverityNumber.Warn,
            SeverityNumber.Error,
            SeverityNumber.Fatal
        };

        foreach (var severity in severities)
        {
            var original = new OtlpExportLogsServiceRequestJson
            {
                ResourceLogs =
                [
                    new OtlpResourceLogsJson
                    {
                        ScopeLogs =
                        [
                            new OtlpScopeLogsJson
                            {
                                LogRecords = [ new OtlpLogRecordJson { SeverityNumber = (int)severity } ]
                            }
                        ]
                    }
                ]
            };

            var json = JsonSerializer.Serialize(original, OtlpJsonSerializerContext.DefaultOptions);
            var deserialized = JsonSerializer.Deserialize<OtlpExportLogsServiceRequestJson>(json, OtlpJsonSerializerContext.DefaultOptions);

            Assert.NotNull(deserialized?.ResourceLogs?[0].ScopeLogs?[0].LogRecords?[0]);
            Assert.Equal((int)severity, deserialized.ResourceLogs![0].ScopeLogs![0].LogRecords![0].SeverityNumber);
        }
    }

    [Fact]
    public void JsonSerialization_Trace_RoundTrip()
    {
        var original = new OtlpExportTraceServiceRequestJson
        {
            ResourceSpans =
            [
                new OtlpResourceSpansJson
                {
                    Resource = new OtlpResourceJson
                    {
                        Attributes = [ new OtlpKeyValueJson { Key = "service.name", Value = new OtlpAnyValueJson { StringValue = "TestService" } } ]
                    },
                    ScopeSpans =
                    [
                        new OtlpScopeSpansJson
                        {
                            Scope = new OtlpInstrumentationScopeJson { Name = "TestLibrary", Version = "1.0.0" },
                            Spans =
                            [
                                new OtlpSpanJson
                                {
                                    TraceId = "5b8efff798038103d269b633813fc60c",
                                    SpanId = "eee19b7ec3c1b174",
                                    ParentSpanId = "eee19b7ec3c1b173",
                                    Name = "GET /api/users",
                                    Kind = (int)Span.Types.SpanKind.Server,
                                    StartTimeUnixNano = 1000,
                                    EndTimeUnixNano = 2000,
                                    Attributes =
                                    [
                                        new OtlpKeyValueJson { Key = "http.method", Value = new OtlpAnyValueJson { StringValue = "GET" } },
                                        new OtlpKeyValueJson { Key = "http.status_code", Value = new OtlpAnyValueJson { IntValue = 200 } }
                                    ],
                                    Status = new OtlpSpanStatusJson { Code = (int)Status.Types.StatusCode.Ok, Message = "OK" }
                                }
                            ]
                        }
                    ]
                }
            ]
        };

        var json = JsonSerializer.Serialize(original, OtlpJsonSerializerContext.DefaultOptions);
        var deserialized = JsonSerializer.Deserialize<OtlpExportTraceServiceRequestJson>(json, OtlpJsonSerializerContext.DefaultOptions);

        Assert.NotNull(deserialized);
        Assert.Single(deserialized.ResourceSpans!);
        var scopeSpans = deserialized.ResourceSpans![0].ScopeSpans;
        Assert.Single(scopeSpans!);
        var span = scopeSpans![0].Spans![0];
        Assert.Equal("5b8efff798038103d269b633813fc60c", span.TraceId);
        Assert.Equal("eee19b7ec3c1b174", span.SpanId);
        Assert.Equal("eee19b7ec3c1b173", span.ParentSpanId);
        Assert.Equal("GET /api/users", span.Name);
        Assert.Equal((int)Span.Types.SpanKind.Server, span.Kind);
        Assert.Equal(1000UL, span.StartTimeUnixNano);
        Assert.Equal(2000UL, span.EndTimeUnixNano);
        Assert.Equal(2, span.Attributes!.Length);
        Assert.Equal((int)Status.Types.StatusCode.Ok, span.Status?.Code);
    }

    [Fact]
    public void JsonSerialization_Metrics_SumRoundTrip()
    {
        var original = new OtlpExportMetricsServiceRequestJson
        {
            ResourceMetrics =
            [
                new OtlpResourceMetricsJson
                {
                    Resource = new OtlpResourceJson
                    {
                        Attributes = [ new OtlpKeyValueJson { Key = "service.name", Value = new OtlpAnyValueJson { StringValue = "MetricService" } } ]
                    },
                    ScopeMetrics =
                    [
                        new OtlpScopeMetricsJson
                        {
                            Scope = new OtlpInstrumentationScopeJson { Name = "test-meter" },
                            Metrics =
                            [
                                new OtlpMetricJson
                                {
                                    Name = "requests",
                                    Description = "Total requests",
                                    Unit = "count",
                                    Sum = new OtlpSumJson
                                    {
                                        IsMonotonic = true,
                                        AggregationTemporality = (int)AggregationTemporality.Cumulative,
                                        DataPoints =
                                        [
                                            new OtlpNumberDataPointJson
                                            {
                                                StartTimeUnixNano = 1000,
                                                TimeUnixNano = 2000,
                                                AsInt = 42
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        };

        var json = JsonSerializer.Serialize(original, OtlpJsonSerializerContext.DefaultOptions);
        var deserialized = JsonSerializer.Deserialize<OtlpExportMetricsServiceRequestJson>(json, OtlpJsonSerializerContext.DefaultOptions);

        Assert.NotNull(deserialized);
        Assert.Single(deserialized.ResourceMetrics!);
        var metric = deserialized.ResourceMetrics![0].ScopeMetrics![0].Metrics![0];
        Assert.Equal("requests", metric.Name);
        Assert.NotNull(metric.Sum);
        Assert.True(metric.Sum.IsMonotonic);
        Assert.Equal((int)AggregationTemporality.Cumulative, metric.Sum.AggregationTemporality);
        Assert.Single(metric.Sum.DataPoints!);
        Assert.Equal(42L, metric.Sum.DataPoints![0].AsInt);
    }

    [Fact]
    public void JsonSerialization_Metrics_GaugeRoundTrip()
    {
        var original = new OtlpExportMetricsServiceRequestJson
        {
            ResourceMetrics =
            [
                new OtlpResourceMetricsJson
                {
                    ScopeMetrics =
                    [
                        new OtlpScopeMetricsJson
                        {
                            Metrics =
                            [
                                new OtlpMetricJson
                                {
                                    Name = "cpu.usage",
                                    Gauge = new OtlpGaugeJson
                                    {
                                        DataPoints = [ new OtlpNumberDataPointJson { TimeUnixNano = 5000, AsDouble = 63.7 } ]
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        };

        var json = JsonSerializer.Serialize(original, OtlpJsonSerializerContext.DefaultOptions);
        var deserialized = JsonSerializer.Deserialize<OtlpExportMetricsServiceRequestJson>(json, OtlpJsonSerializerContext.DefaultOptions);

        Assert.NotNull(deserialized);
        var metric = deserialized.ResourceMetrics![0].ScopeMetrics![0].Metrics![0];
        Assert.Equal("cpu.usage", metric.Name);
        Assert.NotNull(metric.Gauge);
        Assert.Single(metric.Gauge.DataPoints!);
        Assert.Equal(63.7, metric.Gauge.DataPoints![0].AsDouble);
    }

    [Fact]
    public void JsonSerialization_Metrics_HistogramRoundTrip()
    {
        var original = new OtlpExportMetricsServiceRequestJson
        {
            ResourceMetrics =
            [
                new OtlpResourceMetricsJson
                {
                    ScopeMetrics =
                    [
                        new OtlpScopeMetricsJson
                        {
                            Metrics =
                            [
                                new OtlpMetricJson
                                {
                                    Name = "request.duration",
                                    Description = "Request duration",
                                    Unit = "ms",
                                    Histogram = new OtlpHistogramJson
                                    {
                                        AggregationTemporality = (int)AggregationTemporality.Cumulative,
                                        DataPoints =
                                        [
                                            new OtlpHistogramDataPointJson
                                            {
                                                StartTimeUnixNano = 1000,
                                                TimeUnixNano = 2000,
                                                Count = 10,
                                                Sum = 500.0,
                                                Min = 10.0,
                                                Max = 200.0,
                                                ExplicitBounds = [10.0, 50.0, 100.0, 200.0],
                                                BucketCounts = ["2", "3", "3", "1", "1"],
                                                Attributes =
                                                [
                                                    new OtlpKeyValueJson { Key = "route", Value = new OtlpAnyValueJson { StringValue = "/api/test" } }
                                                ]
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        };

        var json = JsonSerializer.Serialize(original, OtlpJsonSerializerContext.DefaultOptions);
        var deserialized = JsonSerializer.Deserialize<OtlpExportMetricsServiceRequestJson>(json, OtlpJsonSerializerContext.DefaultOptions);

        Assert.NotNull(deserialized);
        var metric = deserialized.ResourceMetrics![0].ScopeMetrics![0].Metrics![0];
        Assert.Equal("request.duration", metric.Name);
        Assert.NotNull(metric.Histogram);
        Assert.Equal((int)AggregationTemporality.Cumulative, metric.Histogram.AggregationTemporality);

        var dp = metric.Histogram.DataPoints![0];
        Assert.Equal(10UL, dp.Count);
        Assert.Equal(500.0, dp.Sum);
        Assert.Equal(10.0, dp.Min);
        Assert.Equal(200.0, dp.Max);
        Assert.Equal(4, dp.ExplicitBounds!.Length);
        Assert.Equal(5, dp.BucketCounts!.Length);
        Assert.Single(dp.Attributes!);
        Assert.Equal("route", dp.Attributes![0].Key);
    }

    [Fact]
    public void JsonSerialization_Metrics_ExponentialHistogramRoundTrip()
    {
        var original = new OtlpExportMetricsServiceRequestJson
        {
            ResourceMetrics =
            [
                new OtlpResourceMetricsJson
                {
                    ScopeMetrics =
                    [
                        new OtlpScopeMetricsJson
                        {
                            Metrics =
                            [
                                new OtlpMetricJson
                                {
                                    Name = "latency",
                                    ExponentialHistogram = new OtlpExponentialHistogramJson
                                    {
                                        AggregationTemporality = (int)AggregationTemporality.Delta,
                                        DataPoints =
                                        [
                                            new OtlpExponentialHistogramDataPointJson
                                            {
                                                StartTimeUnixNano = 1000,
                                                TimeUnixNano = 2000,
                                                Count = 5,
                                                Sum = 100.0,
                                                Scale = 2,
                                                ZeroCount = 0,
                                                Positive = new OtlpExponentialHistogramBucketsJson { Offset = 1, BucketCounts = ["1", "2", "2"] },
                                                Negative = new OtlpExponentialHistogramBucketsJson { Offset = 0, BucketCounts = ["0"] }
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        };

        var json = JsonSerializer.Serialize(original, OtlpJsonSerializerContext.DefaultOptions);
        var deserialized = JsonSerializer.Deserialize<OtlpExportMetricsServiceRequestJson>(json, OtlpJsonSerializerContext.DefaultOptions);

        Assert.NotNull(deserialized);
        var metric = deserialized.ResourceMetrics![0].ScopeMetrics![0].Metrics![0];
        Assert.NotNull(metric.ExponentialHistogram);

        var dp = metric.ExponentialHistogram.DataPoints![0];
        Assert.Equal(5UL, dp.Count);
        Assert.Equal(100.0, dp.Sum);
        Assert.Equal(2, dp.Scale);
        Assert.NotNull(dp.Positive);
        Assert.Equal(1, dp.Positive.Offset);
        Assert.Equal(3, dp.Positive.BucketCounts!.Length);
    }

    [Fact]
    public void JsonSerialization_AnyValue_AllTypesRoundTrip()
    {
        var original = new OtlpKeyValueJson[]
        {
            new() { Key = "string_val", Value = new OtlpAnyValueJson { StringValue = "hello" } },
            new() { Key = "bool_val", Value = new OtlpAnyValueJson { BoolValue = true } },
            new() { Key = "int_val", Value = new OtlpAnyValueJson { IntValue = 42 } },
            new() { Key = "double_val", Value = new OtlpAnyValueJson { DoubleValue = 3.14 } },
            new() {
                Key = "array_val",
                Value = new OtlpAnyValueJson
                {
                    ArrayValue = new OtlpArrayValueJson
                    {
                        Values = [ new OtlpAnyValueJson { StringValue = "item1" }, new OtlpAnyValueJson { IntValue = 2 } ]
                    }
                }
            }
        };

        var json = JsonSerializer.Serialize(original, OtlpJsonSerializerContext.DefaultOptions);
        var deserialized = JsonSerializer.Deserialize<OtlpKeyValueJson[]>(json, OtlpJsonSerializerContext.DefaultOptions);

        Assert.NotNull(deserialized);
        Assert.Equal(5, deserialized.Length);
        Assert.Equal("hello", deserialized[0].Value?.StringValue);
        Assert.Equal(true, deserialized[1].Value?.BoolValue);
        Assert.Equal(42L, deserialized[2].Value?.IntValue);
        Assert.Equal(3.14, deserialized[3].Value?.DoubleValue);
        Assert.Equal(2, deserialized[4].Value?.ArrayValue?.Values?.Length);
    }

    [Fact]
    public void JsonSerialization_ProtoToJsonToProtobuf_LogsRoundTrip()
    {
        var jsonType = new OtlpExportLogsServiceRequestJson
        {
            ResourceLogs =
            [
                new OtlpResourceLogsJson
                {
                    Resource = new OtlpResourceJson
                    {
                        Attributes =
                        [
                            new OtlpKeyValueJson { Key = "service.name", Value = new OtlpAnyValueJson { StringValue = "TestService" } },
                            new OtlpKeyValueJson { Key = "service.instance.id", Value = new OtlpAnyValueJson { StringValue = "t-01" } }
                        ]
                    },
                    ScopeLogs =
                    [
                        new OtlpScopeLogsJson
                        {
                            Scope = new OtlpInstrumentationScopeJson { Name = "TestScope" },
                            LogRecords =
                            [
                                new OtlpLogRecordJson
                                {
                                    TimeUnixNano = 1000,
                                    SeverityNumber = (int)SeverityNumber.Info,
                                    Body = new OtlpAnyValueJson { StringValue = "Test message" },
                                    Attributes = [ new OtlpKeyValueJson { Key = "key", Value = new OtlpAnyValueJson { StringValue = "value" } } ]
                                }
                            ]
                        }
                    ]
                }
            ]
        };

        // Serialize to JSON string and deserialize back
        var jsonString = JsonSerializer.Serialize(jsonType, OtlpJsonSerializerContext.DefaultOptions);
        var deserializedJson = JsonSerializer.Deserialize<OtlpExportLogsServiceRequestJson>(jsonString, OtlpJsonSerializerContext.DefaultOptions);

        // Convert JSON type → protobuf
        Assert.NotNull(deserializedJson);
        var protoResult = OtlpJsonToProtobufConverter.ToProtobuf(deserializedJson);

        Assert.Single(protoResult.ResourceLogs);
        var resultLogs = protoResult.ResourceLogs[0];
        Assert.Equal(2, resultLogs.Resource.Attributes.Count);
        Assert.Single(resultLogs.ScopeLogs);
        var scopeLogs = resultLogs.ScopeLogs[0];
        Assert.Equal("TestScope", scopeLogs.Scope.Name);
        Assert.Single(scopeLogs.LogRecords);
        var logRecord = scopeLogs.LogRecords[0];
        Assert.Equal(1000UL, logRecord.TimeUnixNano);
        Assert.Equal(SeverityNumber.Info, logRecord.SeverityNumber);
        Assert.Equal("Test message", logRecord.Body.StringValue);
    }

    [Fact]
    public void JsonSerialization_ProtoToJsonToProtobuf_TracesRoundTrip()
    {
        var jsonType = new OtlpExportTraceServiceRequestJson
        {
            ResourceSpans =
            [
                new OtlpResourceSpansJson
                {
                    Resource = new OtlpResourceJson
                    {
                        Attributes = [ new OtlpKeyValueJson { Key = "service.name", Value = new OtlpAnyValueJson { StringValue = "TraceService" } } ]
                    },
                    ScopeSpans =
                    [
                        new OtlpScopeSpansJson
                        {
                            Scope = new OtlpInstrumentationScopeJson { Name = "TestLib" },
                            Spans =
                            [
                                new OtlpSpanJson
                                {
                                    TraceId = "0102030405060708090a0b0c0d0e0f10",
                                    SpanId = "0102030405060708",
                                    Name = "test-span",
                                    Kind = (int)Span.Types.SpanKind.Internal,
                                    StartTimeUnixNano = 1000,
                                    EndTimeUnixNano = 2000,
                                    Attributes =
                                    [
                                        new OtlpKeyValueJson { Key = "span.attr", Value = new OtlpAnyValueJson { StringValue = "val" } }
                                    ],
                                    Events =
                                    [
                                        new OtlpSpanEventJson
                                        {
                                            Name = "event1",
                                            TimeUnixNano = 1500,
                                            Attributes = [ new OtlpKeyValueJson { Key = "evt.key", Value = new OtlpAnyValueJson { StringValue = "evt.val" } } ]
                                        }
                                    ],
                                    Status = new OtlpSpanStatusJson { Code = (int)Status.Types.StatusCode.Ok }
                                }
                            ]
                        }
                    ]
                }
            ]
        };

        var jsonString = JsonSerializer.Serialize(jsonType, OtlpJsonSerializerContext.DefaultOptions);
        var deserializedJson = JsonSerializer.Deserialize<OtlpExportTraceServiceRequestJson>(jsonString, OtlpJsonSerializerContext.DefaultOptions);

        Assert.NotNull(deserializedJson);
        var protoResult = OtlpJsonToProtobufConverter.ToProtobuf(deserializedJson);

        Assert.Single(protoResult.ResourceSpans);
        var scopeSpans = protoResult.ResourceSpans[0].ScopeSpans;
        Assert.Single(scopeSpans);
        var span = scopeSpans[0].Spans[0];
        Assert.Equal("test-span", span.Name);
        Assert.Equal(Span.Types.SpanKind.Internal, span.Kind);
        Assert.Single(span.Attributes);
        Assert.Single(span.Events);
        Assert.Equal("event1", span.Events[0].Name);
        Assert.Equal(Status.Types.StatusCode.Ok, span.Status.Code);
    }

    [Fact]
    public void JsonSerialization_ProtoToJsonToProtobuf_MetricsHistogramRoundTrip()
    {
        var jsonType = new OtlpExportMetricsServiceRequestJson
        {
            ResourceMetrics =
            [
                new OtlpResourceMetricsJson
                {
                    Resource = new OtlpResourceJson
                    {
                        Attributes = [ new OtlpKeyValueJson { Key = "service.name", Value = new OtlpAnyValueJson { StringValue = "MetricSvc" } } ]
                    },
                    ScopeMetrics =
                    [
                        new OtlpScopeMetricsJson
                        {
                            Scope = new OtlpInstrumentationScopeJson { Name = "test-meter" },
                            Metrics =
                            [
                                new OtlpMetricJson
                                {
                                    Name = "http.request.duration",
                                    Description = "HTTP request duration",
                                    Unit = "s",
                                    Histogram = new OtlpHistogramJson
                                    {
                                        AggregationTemporality = (int)AggregationTemporality.Cumulative,
                                        DataPoints =
                                        [
                                            new OtlpHistogramDataPointJson
                                            {
                                                StartTimeUnixNano = 0,
                                                TimeUnixNano = 1000,
                                                Count = 5,
                                                Sum = 2.5,
                                                ExplicitBounds = [0.1, 0.5, 1.0, 5.0],
                                                BucketCounts = ["1", "2", "1", "1", "0"]
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        };

        var jsonString = JsonSerializer.Serialize(jsonType, OtlpJsonSerializerContext.DefaultOptions);
        var deserializedJson = JsonSerializer.Deserialize<OtlpExportMetricsServiceRequestJson>(jsonString, OtlpJsonSerializerContext.DefaultOptions);

        Assert.NotNull(deserializedJson);
        var protoResult = OtlpJsonToProtobufConverter.ToProtobuf(deserializedJson);

        Assert.Single(protoResult.ResourceMetrics);
        var metric = protoResult.ResourceMetrics[0].ScopeMetrics[0].Metrics[0];
        Assert.Equal("http.request.duration", metric.Name);
        Assert.NotNull(metric.Histogram);
        Assert.Equal(AggregationTemporality.Cumulative, metric.Histogram.AggregationTemporality);

        var dp = metric.Histogram.DataPoints[0];
        Assert.Equal(5UL, dp.Count);
        Assert.Equal(2.5, dp.Sum);
        Assert.Equal(4, dp.ExplicitBounds.Count);
        Assert.Equal(5, dp.BucketCounts.Count);
    }

    #endregion

    #region Helpers

    private static Resource CreateResource(string serviceName, string instanceId) => new()
    {
        Attributes =
        {
            new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = serviceName } },
            new KeyValue { Key = "service.instance.id", Value = new AnyValue { StringValue = instanceId } }
        }
    };

    private static string GetServiceName(Resource resource)
        => resource.Attributes.FirstOrDefault(a => a.Key == "service.name")?.Value?.StringValue ?? string.Empty;

    private static LogRecord CreateLogRecord(SeverityNumber severity, string message, ulong timeNano) => new()
    {
        SeverityNumber = severity,
        Body = new AnyValue { StringValue = message },
        TimeUnixNano = timeNano,
        ObservedTimeUnixNano = timeNano
    };

    private static Span CreateSpan(string traceId, string spanId, string? parentSpanId, string name, Span.Types.SpanKind kind) => new()
    {
        TraceId = ByteString.CopyFromUtf8(traceId.PadRight(16, '_')[..16]),
        SpanId = ByteString.CopyFromUtf8(spanId.PadRight(8, '_')[..8]),
        ParentSpanId = parentSpanId is null ? ByteString.Empty : ByteString.CopyFromUtf8(parentSpanId.PadRight(8, '_')[..8]),
        Name = name,
        Kind = kind,
        StartTimeUnixNano = 1000,
        EndTimeUnixNano = 2000
    };

    #endregion

    /// <summary>
    /// An in-memory ITelemetryStorage implementation used for integration testing.
    /// </summary>
    private sealed class InMemoryTelemetryStorage : ITelemetryStorage
    {
        private readonly List<ResourceLogs> _logs = [];
        private readonly List<ResourceSpans> _spans = [];
        private readonly List<ResourceMetrics> _metrics = [];

        public Task InitializeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task WriteLogsAsync(ResourceLogs resourceLogs, CancellationToken cancellationToken = default)
        {
            _logs.Add(resourceLogs);
            return Task.CompletedTask;
        }

        public Task WriteSpansAsync(ResourceSpans resourceSpans, CancellationToken cancellationToken = default)
        {
            _spans.Add(resourceSpans);
            return Task.CompletedTask;
        }

        public Task WriteMetricsAsync(ResourceMetrics resourceMetrics, CancellationToken cancellationToken = default)
        {
            _metrics.Add(resourceMetrics);
            return Task.CompletedTask;
        }

        public IAsyncEnumerable<ResourceLogs> ReadLogsAsync(CancellationToken cancellationToken = default)
            => ReadItems(_logs, cancellationToken);

        public IAsyncEnumerable<ResourceSpans> ReadSpansAsync(CancellationToken cancellationToken = default)
            => ReadItems(_spans, cancellationToken);

        public IAsyncEnumerable<ResourceMetrics> ReadMetricsAsync(CancellationToken cancellationToken = default)
            => ReadItems(_metrics, cancellationToken);

        private static async IAsyncEnumerable<T> ReadItems<T>(List<T> items, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            foreach (var item in items)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return item;
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
