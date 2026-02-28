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
            File.Delete(_dbPath);
        }

        // Remove WAL files if present.
        foreach (var ext in new[] { "-wal", "-shm" })
        {
            var extra = _dbPath + ext;
            if (File.Exists(extra))
            {
                File.Delete(extra);
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
    public async Task WriteSpansAsync_And_ReadSpansAsync_RoundTrips()
    {
        await _storage.InitializeAsync();

        var resource = CreateResource("svc-a");
        var parentSpanId = ByteString.CopyFrom(Encoding.UTF8.GetBytes("parent01"));
        var childSpanId = ByteString.CopyFrom(Encoding.UTF8.GetBytes("child001"));
        var traceId = ByteString.CopyFrom(Encoding.UTF8.GetBytes("trace001traceid!"));

        var resourceSpans = new ResourceSpans
        {
            Resource = resource,
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
    public async Task WriteSpansAsync_ParentChildRelation_StoredInSpansTable()
    {
        await _storage.InitializeAsync();

        var parentSpanId = ByteString.CopyFrom(Encoding.UTF8.GetBytes("parent01"));
        var childSpanId = ByteString.CopyFrom(Encoding.UTF8.GetBytes("child001"));
        var traceId = ByteString.CopyFrom(Encoding.UTF8.GetBytes("trace001traceid!"));

        var resourceSpans = new ResourceSpans
        {
            Resource = CreateResource("svc-b"),
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

        // Verify via ReadSpansAsync that the parent-child link is preserved.
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
                Resource = CreateResource($"svc-{i}"),
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
    public async Task WriteLogsAsync_And_ReadLogsAsync_RoundTrips()
    {
        await _storage.InitializeAsync();

        var resourceLogs = new ResourceLogs
        {
            Resource = CreateResource("log-svc"),
            ScopeLogs =
            {
                new ScopeLogs
                {
                    Scope = new InstrumentationScope { Name = "LogScope" },
                    LogRecords =
                    {
                        new LogRecord
                        {
                            TimeUnixNano = 1_000,
                            SeverityNumber = SeverityNumber.Info,
                            Body = new AnyValue { StringValue = "Hello log" }
                        }
                    }
                }
            }
        };

        await _storage.WriteLogsAsync(resourceLogs);

        var logs = new List<ResourceLogs>();
        await foreach (var l in _storage.ReadLogsAsync())
        {
            logs.Add(l);
        }

        Assert.Single(logs);
        Assert.Equal("Hello log", logs[0].ScopeLogs[0].LogRecords[0].Body.StringValue);
    }

    [Fact]
    public async Task WriteMetricsAsync_And_ReadMetricsAsync_RoundTrips()
    {
        await _storage.InitializeAsync();

        var resourceMetrics = new ResourceMetrics
        {
            Resource = CreateResource("metric-svc"),
            ScopeMetrics =
            {
                new ScopeMetrics
                {
                    Scope = new InstrumentationScope { Name = "MetricScope" },
                    Metrics =
                    {
                        new Metric
                        {
                            Name = "requests",
                            Sum = new Sum
                            {
                                DataPoints =
                                {
                                    new NumberDataPoint { AsInt = 42, TimeUnixNano = 1_000 }
                                }
                            }
                        }
                    }
                }
            }
        };

        await _storage.WriteMetricsAsync(resourceMetrics);

        var metrics = new List<ResourceMetrics>();
        await foreach (var m in _storage.ReadMetricsAsync())
        {
            metrics.Add(m);
        }

        Assert.Single(metrics);
        Assert.Equal("requests", metrics[0].ScopeMetrics[0].Metrics[0].Name);
        Assert.Equal(42, metrics[0].ScopeMetrics[0].Metrics[0].Sum.DataPoints[0].AsInt);
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

    [Fact]
    public async Task DisposeAsync_CanBeCalledMultipleTimes()
    {
        await _storage.InitializeAsync();
        await _storage.DisposeAsync();
        await _storage.DisposeAsync();
    }

    private static Resource CreateResource(string? name = null) => new()
    {
        Attributes =
        {
            new KeyValue { Key = "service.name", Value = new AnyValue { StringValue = name ?? "TestService" } }
        }
    };
}
