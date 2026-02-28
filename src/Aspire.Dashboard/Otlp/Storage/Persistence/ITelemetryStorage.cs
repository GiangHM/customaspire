// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;

namespace Aspire.Dashboard.Otlp.Storage.Persistence;

/// <summary>
/// Defines the contract for persistent storage of OTLP telemetry data.
/// Implementations provide write-through storage so that telemetry survives dashboard restarts.
/// </summary>
/// <remarks>
/// The storage interface operates on raw OTLP proto messages
/// (<see cref="ResourceLogs"/>, <see cref="ResourceSpans"/>, <see cref="ResourceMetrics"/>),
/// which match the inputs accepted by <see cref="TelemetryRepository"/>.
/// On startup, stored payloads can be replayed through the repository to restore in-memory state.
/// </remarks>
public interface ITelemetryStorage : IAsyncDisposable
{
    /// <summary>
    /// Initializes the storage backend, performing any required setup such as creating database schema.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Persists a batch of log records for a single resource.
    /// </summary>
    /// <param name="resourceLogs">The OTLP resource logs payload to store.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task WriteLogsAsync(ResourceLogs resourceLogs, CancellationToken cancellationToken = default);

    /// <summary>
    /// Persists a batch of trace spans for a single resource.
    /// </summary>
    /// <param name="resourceSpans">The OTLP resource spans payload to store.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task WriteSpansAsync(ResourceSpans resourceSpans, CancellationToken cancellationToken = default);

    /// <summary>
    /// Persists a batch of metric data points for a single resource.
    /// </summary>
    /// <param name="resourceMetrics">The OTLP resource metrics payload to store.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task WriteMetricsAsync(ResourceMetrics resourceMetrics, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads all persisted log records, yielding one <see cref="ResourceLogs"/> per stored resource batch.
    /// Intended for use during dashboard startup to restore in-memory state.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An async sequence of <see cref="ResourceLogs"/> payloads.</returns>
    IAsyncEnumerable<ResourceLogs> ReadLogsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads all persisted trace spans, yielding one <see cref="ResourceSpans"/> per stored resource batch.
    /// Intended for use during dashboard startup to restore in-memory state.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An async sequence of <see cref="ResourceSpans"/> payloads.</returns>
    IAsyncEnumerable<ResourceSpans> ReadSpansAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads all persisted metric data points, yielding one <see cref="ResourceMetrics"/> per stored resource batch.
    /// Intended for use during dashboard startup to restore in-memory state.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>An async sequence of <see cref="ResourceMetrics"/> payloads.</returns>
    IAsyncEnumerable<ResourceMetrics> ReadMetricsAsync(CancellationToken cancellationToken = default);
}
