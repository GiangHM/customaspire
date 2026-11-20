// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

namespace Aspire.Dashboard.Otlp.Storage.Persistence;

/// <summary>
/// Initializes persistent telemetry storage and replays stored telemetry into <see cref="TelemetryRepository"/>
/// when the dashboard starts.
/// </summary>
/// <remarks>
/// Hosted services start before the web server begins listening, so replay completes before
/// the dashboard accepts new OTLP data.
/// </remarks>
internal sealed class TelemetryStorageInitializer : IHostedService
{
    private readonly ITelemetryStorage _storage;
    private readonly TelemetryRepository _repository;
    private readonly ILogger<TelemetryStorageInitializer> _logger;

    public TelemetryStorageInitializer(ITelemetryStorage storage, TelemetryRepository repository, ILogger<TelemetryStorageInitializer> logger)
    {
        _storage = storage;
        _repository = repository;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // A failure here (for example, an unwritable path) is a configuration error and should stop startup
        // rather than silently running without the persistence the user asked for.
        await _storage.InitializeAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            await _repository.ReplayFromStorageAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Unreadable stored data must not prevent the dashboard from starting.
            _logger.LogError(ex, "Failed to replay persisted telemetry. The dashboard will start with the data restored so far.");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
