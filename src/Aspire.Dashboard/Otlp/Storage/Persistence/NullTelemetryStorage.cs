// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;

namespace Aspire.Dashboard.Otlp.Storage.Persistence;

/// <summary>
/// A no-op implementation of <see cref="ITelemetryStorage"/> used when persistence is disabled.
/// All write operations are silently ignored and all read operations return empty sequences.
/// </summary>
internal sealed class NullTelemetryStorage : ITelemetryStorage
{
    /// <summary>
    /// Gets the singleton instance of <see cref="NullTelemetryStorage"/>.
    /// </summary>
    public static readonly NullTelemetryStorage Instance = new();

    private NullTelemetryStorage()
    {
    }

    /// <inheritdoc />
    public Task InitializeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <inheritdoc />
    public Task WriteLogsAsync(ResourceLogs resourceLogs, CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <inheritdoc />
    public Task WriteSpansAsync(ResourceSpans resourceSpans, CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <inheritdoc />
    public Task WriteMetricsAsync(ResourceMetrics resourceMetrics, CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <inheritdoc />
    public IAsyncEnumerable<ResourceLogs> ReadLogsAsync(CancellationToken cancellationToken = default)
        => EmptyAsyncEnumerable<ResourceLogs>.Instance;

    /// <inheritdoc />
    public IAsyncEnumerable<ResourceSpans> ReadSpansAsync(CancellationToken cancellationToken = default)
        => EmptyAsyncEnumerable<ResourceSpans>.Instance;

    /// <inheritdoc />
    public IAsyncEnumerable<ResourceMetrics> ReadMetricsAsync(CancellationToken cancellationToken = default)
        => EmptyAsyncEnumerable<ResourceMetrics>.Instance;

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private sealed class EmptyAsyncEnumerable<T> : IAsyncEnumerable<T>
    {
        public static readonly EmptyAsyncEnumerable<T> Instance = new();

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            => EmptyAsyncEnumerator.Instance;

        private sealed class EmptyAsyncEnumerator : IAsyncEnumerator<T>
        {
            public static readonly EmptyAsyncEnumerator Instance = new();

            public T Current => default!;
            public ValueTask<bool> MoveNextAsync() => new(false);
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
