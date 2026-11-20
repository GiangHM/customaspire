// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Threading.Channels;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;

namespace Aspire.Dashboard.Otlp.Storage.Persistence;

/// <summary>
/// Decorates an <see cref="ITelemetryStorage"/> so that writes are queued and executed in order by a single
/// background writer, instead of running on (or racing with) the OTLP ingestion path.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>The queue is bounded. When the storage cannot keep up, new writes are dropped (and counted) rather than
/// growing memory without limit or blocking ingestion. Telemetry is still held in the repository's memory.</item>
/// <item>Disposal stops accepting writes and drains the queue (up to a timeout) before disposing the inner storage,
/// so telemetry received just before shutdown is not lost.</item>
/// <item>Reads pass straight through. They are intended for startup replay, before writes begin.</item>
/// </list>
/// </remarks>
internal sealed class QueuedTelemetryStorage : ITelemetryStorage
{
    internal const int DefaultCapacity = 10_000;
    internal static readonly TimeSpan s_defaultDrainTimeout = TimeSpan.FromSeconds(10);

    private readonly ITelemetryStorage _inner;
    private readonly ILogger<QueuedTelemetryStorage> _logger;
    private readonly TimeSpan _drainTimeout;
    private readonly Channel<Func<CancellationToken, Task>> _queue;
    private readonly CancellationTokenSource _writerCts = new();
    private Task? _writerTask;
    private long _droppedCount;
    private int _disposed;

    public QueuedTelemetryStorage(ITelemetryStorage inner, ILogger<QueuedTelemetryStorage> logger, int capacity = DefaultCapacity, TimeSpan? drainTimeout = null)
    {
        _inner = inner;
        _logger = logger;
        _drainTimeout = drainTimeout ?? s_defaultDrainTimeout;
        _queue = Channel.CreateBounded<Func<CancellationToken, Task>>(new BoundedChannelOptions(capacity)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    /// <summary>
    /// Gets the number of writes dropped because the queue was full.
    /// </summary>
    internal long DroppedCount => Interlocked.Read(ref _droppedCount);

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await _inner.InitializeAsync(cancellationToken).ConfigureAwait(false);

        // Writes queued before initialization wait in the channel until the writer starts.
        _writerTask ??= Task.Run(WriteLoopAsync, CancellationToken.None);
    }

    public Task WriteLogsAsync(ResourceLogs resourceLogs, CancellationToken cancellationToken = default)
    {
        Enqueue(ct => _inner.WriteLogsAsync(resourceLogs, ct));
        return Task.CompletedTask;
    }

    public Task WriteSpansAsync(ResourceSpans resourceSpans, CancellationToken cancellationToken = default)
    {
        Enqueue(ct => _inner.WriteSpansAsync(resourceSpans, ct));
        return Task.CompletedTask;
    }

    public Task WriteMetricsAsync(ResourceMetrics resourceMetrics, CancellationToken cancellationToken = default)
    {
        Enqueue(ct => _inner.WriteMetricsAsync(resourceMetrics, ct));
        return Task.CompletedTask;
    }

    public IAsyncEnumerable<ResourceLogs> ReadLogsAsync(CancellationToken cancellationToken = default) => _inner.ReadLogsAsync(cancellationToken);

    public IAsyncEnumerable<ResourceSpans> ReadSpansAsync(CancellationToken cancellationToken = default) => _inner.ReadSpansAsync(cancellationToken);

    public IAsyncEnumerable<ResourceMetrics> ReadMetricsAsync(CancellationToken cancellationToken = default) => _inner.ReadMetricsAsync(cancellationToken);

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        // Stop accepting writes, then let the writer drain what is already queued.
        _queue.Writer.TryComplete();

        if (_writerTask is not null)
        {
            try
            {
                await _writerTask.WaitAsync(_drainTimeout).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                _logger.LogWarning("Timed out after {Timeout} draining persisted telemetry on shutdown. {Remaining} write(s) were not saved.", _drainTimeout, _queue.Reader.Count);
                await _writerCts.CancelAsync().ConfigureAwait(false);
                await _writerTask.ConfigureAwait(false);
            }
        }

        _writerCts.Dispose();
        await _inner.DisposeAsync().ConfigureAwait(false);
    }

    private void Enqueue(Func<CancellationToken, Task> write)
    {
        if (_queue.Writer.TryWrite(write) || Volatile.Read(ref _disposed) == 1)
        {
            return;
        }

        // Log the first drop and then every 1000th, so a sustained overload does not flood the log.
        var dropped = Interlocked.Increment(ref _droppedCount);
        if (dropped == 1 || dropped % 1000 == 0)
        {
            _logger.LogWarning("The telemetry persistence queue is full; {Dropped} write(s) dropped so far. Telemetry stays available in memory but will not be restored after a restart.", dropped);
        }
    }

    private async Task WriteLoopAsync()
    {
        var reader = _queue.Reader;
        var cancellationToken = _writerCts.Token;

        try
        {
            while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (reader.TryRead(out var write))
                {
                    try
                    {
                        await write(cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogWarning(ex, "Failed to persist telemetry to storage.");
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Drain timed out; remaining writes are abandoned.
        }
    }
}
