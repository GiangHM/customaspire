// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.Configuration;

namespace Aspire.Dashboard.Otlp.Storage.Persistence;

/// <summary>
/// Limits that bound the size of the SQLite database. Older rows beyond these limits are deleted,
/// mirroring the in-memory limits applied by <see cref="TelemetryRepository"/>.
/// </summary>
/// <param name="MaxLogBatches">Maximum number of stored log batches (each batch holds one or more log records).</param>
/// <param name="MaxSpanBatches">Maximum number of stored span batches (each batch holds one or more spans).</param>
/// <param name="MaxMetricPointsPerInstrument">Maximum number of stored data points for each metric instrument.</param>
internal sealed record SqliteRetentionLimits(int MaxLogBatches, int MaxSpanBatches, int MaxMetricPointsPerInstrument)
{
    /// <summary>
    /// Creates retention limits from the dashboard's in-memory telemetry limits.
    /// Every stored batch holds at least one record, so keeping as many batches as the in-memory record limit
    /// always retains at least everything the in-memory buffers can hold.
    /// </summary>
    public static SqliteRetentionLimits FromTelemetryLimits(TelemetryLimitOptions limits) =>
        new(limits.MaxLogCount, limits.MaxTraceCount, limits.MaxMetricsCount);
}
