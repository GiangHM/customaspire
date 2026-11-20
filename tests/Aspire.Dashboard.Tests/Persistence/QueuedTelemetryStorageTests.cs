// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.Otlp.Storage.Persistence;
using Microsoft.Extensions.Logging.Abstractions;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;
using Xunit;
using static Aspire.Tests.Shared.Telemetry.TelemetryTestHelpers;

namespace Aspire.Dashboard.Tests.Persistence;

public sealed class QueuedTelemetryStorageTests
{
    [Fact]
    public async Task WriteLogsAsync_WritesReachInnerStorageInOrder()
    {
        var inner = new RecordingStorage();
        await using var storage = new QueuedTelemetryStorage(inner, NullLogger<QueuedTelemetryStorage>.Instance);
        await storage.InitializeAsync();

        for (var i = 0; i < 100; i++)
        {
            await storage.WriteLogsAsync(CreateLogs(i));
        }

        await WaitForAsync(() => inner.Written.Count == 100);
        Assert.Equal(Enumerable.Range(0, 100).Select(i => $"app{i}"), inner.Written.Select(GetServiceName));
    }

    [Fact]
    public async Task WriteLogsAsync_BeforeInitialize_AreWrittenOnceInitialized()
    {
        var inner = new RecordingStorage();
        await using var storage = new QueuedTelemetryStorage(inner, NullLogger<QueuedTelemetryStorage>.Instance);

        await storage.WriteLogsAsync(CreateLogs(1));
        Assert.Empty(inner.Written);

        await storage.InitializeAsync();

        await WaitForAsync(() => inner.Written.Count == 1);
    }

    [Fact]
    public async Task DisposeAsync_DrainsQueuedWrites()
    {
        var inner = new RecordingStorage { WriteDelay = TimeSpan.FromMilliseconds(2) };
        var storage = new QueuedTelemetryStorage(inner, NullLogger<QueuedTelemetryStorage>.Instance);
        await storage.InitializeAsync();

        for (var i = 0; i < 50; i++)
        {
            await storage.WriteLogsAsync(CreateLogs(i));
        }

        await storage.DisposeAsync();

        Assert.Equal(50, inner.Written.Count);
        Assert.True(inner.Disposed);
    }

    [Fact]
    public async Task WriteLogsAsync_QueueFull_DropsWritesAndCountsThem()
    {
        var inner = new RecordingStorage { BlockWrites = true };
        await using var storage = new QueuedTelemetryStorage(inner, NullLogger<QueuedTelemetryStorage>.Instance, capacity: 2);
        await storage.InitializeAsync();

        // The writer takes the first item and blocks on it, leaving the queue empty.
        await storage.WriteLogsAsync(CreateLogs(0));
        await WaitForAsync(() => inner.WriteStarted);

        // Two more fit in the queue; the rest are dropped.
        for (var i = 1; i <= 10; i++)
        {
            await storage.WriteLogsAsync(CreateLogs(i));
        }

        Assert.Equal(8, storage.DroppedCount);

        inner.Release();
        await WaitForAsync(() => inner.Written.Count == 3);
    }

    [Fact]
    public async Task WriteLogsAsync_InnerWriteFails_WriterKeepsRunning()
    {
        var inner = new RecordingStorage { FailFirstWrite = true };
        await using var storage = new QueuedTelemetryStorage(inner, NullLogger<QueuedTelemetryStorage>.Instance);
        await storage.InitializeAsync();

        await storage.WriteLogsAsync(CreateLogs(0));
        await storage.WriteLogsAsync(CreateLogs(1));

        await WaitForAsync(() => inner.Written.Count == 1);
        Assert.Equal("app1", GetServiceName(inner.Written.Single()));
    }

    [Fact]
    public async Task DisposeAsync_DrainTimesOut_AbandonsRemainingWritesAndCompletes()
    {
        var inner = new RecordingStorage { BlockWrites = true };
        var storage = new QueuedTelemetryStorage(inner, NullLogger<QueuedTelemetryStorage>.Instance, drainTimeout: TimeSpan.FromMilliseconds(100));
        await storage.InitializeAsync();
        await storage.WriteLogsAsync(CreateLogs(0));
        await WaitForAsync(() => inner.WriteStarted);

        await storage.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.True(inner.Disposed);
        Assert.Empty(inner.Written);
    }

    private static ResourceLogs CreateLogs(int index) => new()
    {
        Resource = CreateResource(name: $"app{index}", instanceId: "instance"),
        ScopeLogs = { new ScopeLogs { Scope = CreateScope(), LogRecords = { CreateLogRecord(message: $"log{index}") } } }
    };

    private static string GetServiceName(ResourceLogs resourceLogs) =>
        resourceLogs.Resource.Attributes.Single(a => a.Key == "service.name").Value.StringValue;

    private static async Task WaitForAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Timed out waiting for condition.");
            await Task.Delay(10);
        }
    }

    private sealed class RecordingStorage : ITelemetryStorage
    {
        private readonly object _lock = new();
        private readonly List<ResourceLogs> _written = [];
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _writeStarted;
        private int _writes;

        public TimeSpan WriteDelay { get; init; }
        public bool BlockWrites { get; init; }
        public bool FailFirstWrite { get; init; }
        public bool Disposed { get; private set; }
        public bool WriteStarted => Volatile.Read(ref _writeStarted) == 1;

        public List<ResourceLogs> Written
        {
            get
            {
                lock (_lock)
                {
                    return [.. _written];
                }
            }
        }

        public void Release() => _release.TrySetResult();

        public Task InitializeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public async Task WriteLogsAsync(ResourceLogs resourceLogs, CancellationToken cancellationToken = default)
        {
            Volatile.Write(ref _writeStarted, 1);

            if (BlockWrites)
            {
                await _release.Task.WaitAsync(cancellationToken);
            }

            if (WriteDelay > TimeSpan.Zero)
            {
                await Task.Delay(WriteDelay, cancellationToken);
            }

            if (FailFirstWrite && Interlocked.Increment(ref _writes) == 1)
            {
                throw new InvalidOperationException("Simulated write failure.");
            }

            lock (_lock)
            {
                _written.Add(resourceLogs);
            }
        }

        public Task WriteSpansAsync(ResourceSpans resourceSpans, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task WriteMetricsAsync(ResourceMetrics resourceMetrics, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public IAsyncEnumerable<ResourceLogs> ReadLogsAsync(CancellationToken cancellationToken = default) => NullTelemetryStorage.Instance.ReadLogsAsync(cancellationToken);

        public IAsyncEnumerable<ResourceSpans> ReadSpansAsync(CancellationToken cancellationToken = default) => NullTelemetryStorage.Instance.ReadSpansAsync(cancellationToken);

        public IAsyncEnumerable<ResourceMetrics> ReadMetricsAsync(CancellationToken cancellationToken = default) => NullTelemetryStorage.Instance.ReadMetricsAsync(cancellationToken);

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }
}
