// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Dashboard.Configuration;
using Aspire.Dashboard.Model;
using Aspire.Dashboard.Otlp.Model;
using Aspire.Dashboard.Otlp.Storage;
using Aspire.Dashboard.Otlp.Storage.Persistence;
using Google.Protobuf.Collections;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using OpenTelemetry.Proto.Logs.V1;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;
using Xunit;
using static Aspire.Tests.Shared.Telemetry.TelemetryTestHelpers;

namespace Aspire.Dashboard.Tests.Persistence;

/// <summary>
/// Verifies that telemetry written through <see cref="TelemetryRepository"/> survives a restart,
/// i.e. a new repository over the same SQLite file is restored by <c>ReplayFromStorageAsync</c>.
/// </summary>
public sealed class TelemetryStorageReplayTests : IDisposable
{
    private static readonly DateTime s_testTime = new(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"aspire-replay-{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        try
        {
            File.Delete(_dbPath);
        }
        catch
        {
            // Best-effort cleanup.
        }
    }

    [Fact]
    public async Task ReplayFromStorageAsync_AfterRestart_RestoresLogsAndTraces()
    {
        // First "process": write telemetry through the repository.
        await using (var storage = await CreateStorageAsync())
        {
            var repository = CreateRepositoryWithStorage(storage);

            var addContext = new AddContext();
            repository.AddLogs(addContext, new RepeatedField<ResourceLogs>
            {
                new ResourceLogs
                {
                    Resource = CreateResource(name: "app1", instanceId: "instance1"),
                    ScopeLogs = { new ScopeLogs { Scope = CreateScope(), LogRecords = { CreateLogRecord(time: s_testTime, message: "persisted-log") } } }
                }
            });
            repository.AddTraces(addContext, new RepeatedField<ResourceSpans>
            {
                new ResourceSpans
                {
                    Resource = CreateResource(name: "app1", instanceId: "instance1"),
                    ScopeSpans = { new ScopeSpans { Scope = CreateScope(), Spans = { CreateSpan(traceId: "trace1", spanId: "span1", startTime: s_testTime, endTime: s_testTime.AddMinutes(1)) } } }
                }
            });
            Assert.Equal(0, addContext.FailureCount);

            // Writes are fire-and-forget, so wait until they have reached the database.
            await WaitForAsync(async () => await CountAsync(storage.ReadLogsAsync()) == 1 && await CountAsync(storage.ReadSpansAsync()) == 1);
        }

        // Second "process": a fresh repository over the same file starts empty until replayed.
        await using (var storage = await CreateStorageAsync())
        {
            var repository = CreateRepositoryWithStorage(storage);
            Assert.Empty(repository.GetResources());

            await repository.ReplayFromStorageAsync();

            var resource = Assert.Single(repository.GetResources());
            Assert.Equal("app1", resource.ResourceName);

            var logs = repository.GetLogs(new GetLogsContext { ResourceKey = null, StartIndex = 0, Count = 10, Filters = [] });
            Assert.Equal("persisted-log", Assert.Single(logs.Items).Message);

            var traces = repository.GetTraces(new GetTracesRequest { ResourceKey = null, FilterText = string.Empty, StartIndex = 0, Count = 10, Filters = [] });
            Assert.Single(traces.PagedResult.Items);
        }
    }

    [Fact]
    public async Task ReplayFromStorageAsync_AfterRestart_RestoresMetrics()
    {
        await using (var storage = await CreateStorageAsync())
        {
            var repository = CreateRepositoryWithStorage(storage);
            repository.AddMetrics(new AddContext(), new RepeatedField<ResourceMetrics>
            {
                new ResourceMetrics
                {
                    Resource = CreateResource(name: "app1", instanceId: "instance1"),
                    ScopeMetrics = { new ScopeMetrics { Scope = CreateScope(name: "meter1"), Metrics = { CreateSumMetric("persisted-metric", s_testTime, value: 5) } } }
                }
            });

            await WaitForAsync(async () => await CountAsync(storage.ReadMetricsAsync()) == 1);
        }

        await using (var storage = await CreateStorageAsync())
        {
            var repository = CreateRepositoryWithStorage(storage);

            await repository.ReplayFromStorageAsync();

            var resource = Assert.Single(repository.GetResources());
            var instrument = Assert.Single(repository.GetInstrumentsSummaries(resource.ResourceKey));
            Assert.Equal("persisted-metric", instrument.Name);
        }
    }

    [Fact]
    public async Task InitializeAsync_CalledTwice_DoesNotLeakConnection()
    {
        var storage = new SqliteTelemetryStorage(_dbPath, NullLogger<SqliteTelemetryStorage>.Instance);

        await storage.InitializeAsync();
        await storage.InitializeAsync();
        await storage.DisposeAsync();

        // A leaked connection would keep the file locked on Windows.
        File.Delete(_dbPath);
        Assert.False(File.Exists(_dbPath));
    }

    [Fact]
    public async Task ReplayFromStorageAsync_DoesNotWriteReplayedDataBackToStorage()
    {
        await using var storage = await CreateStorageAsync();
        await storage.WriteLogsAsync(new ResourceLogs
        {
            Resource = CreateResource(name: "app1", instanceId: "instance1"),
            ScopeLogs = { new ScopeLogs { Scope = CreateScope(), LogRecords = { CreateLogRecord(time: s_testTime, message: "log1") } } }
        });

        var repository = CreateRepositoryWithStorage(storage);
        await repository.ReplayFromStorageAsync();

        // Give any (unexpected) fire-and-forget write a chance to land before asserting.
        await Task.Delay(200);

        Assert.Equal(1, await CountAsync(storage.ReadLogsAsync()));
        Assert.Single(repository.GetLogs(new GetLogsContext { ResourceKey = null, StartIndex = 0, Count = 10, Filters = [] }).Items);
    }

    [Fact]
    public async Task ReplayFromStorageAsync_PausedLogs_StillRestoresPersistedData()
    {
        await using var storage = await CreateStorageAsync();
        await storage.WriteLogsAsync(new ResourceLogs
        {
            Resource = CreateResource(name: "app1", instanceId: "instance1"),
            ScopeLogs = { new ScopeLogs { Scope = CreateScope(), LogRecords = { CreateLogRecord(time: s_testTime, message: "log1") } } }
        });

        var pauseManager = new PauseManager();
        pauseManager.SetStructuredLogsPaused(true);
        var repository = CreateRepositoryWithStorage(storage, pauseManager);

        await repository.ReplayFromStorageAsync();

        Assert.Single(repository.GetLogs(new GetLogsContext { ResourceKey = null, StartIndex = 0, Count = 10, Filters = [] }).Items);
    }

    [Fact]
    public async Task ReplayFromStorageAsync_NullStorage_IsNoOp()
    {
        var repository = CreateRepositoryWithStorage(NullTelemetryStorage.Instance);

        await repository.ReplayFromStorageAsync();

        Assert.Empty(repository.GetResources());
    }

    [Fact]
    public async Task InitializeAsync_CreatesMissingParentDirectory()
    {
        var directory = Path.Combine(Path.GetTempPath(), $"aspire-replay-dir-{Guid.NewGuid():N}", "nested");
        var path = Path.Combine(directory, "telemetry.db");
        try
        {
            await using var storage = new SqliteTelemetryStorage(path, NullLogger<SqliteTelemetryStorage>.Instance);

            await storage.InitializeAsync();

            Assert.True(File.Exists(path));
        }
        finally
        {
            Directory.Delete(Path.GetDirectoryName(directory)!, recursive: true);
        }
    }

    [Fact]
    public async Task WriteAsync_ConcurrentWrites_AllPersisted()
    {
        await using var storage = await CreateStorageAsync();

        var tasks = Enumerable.Range(0, 50).Select(i => storage.WriteLogsAsync(new ResourceLogs
        {
            Resource = CreateResource(name: $"app{i}", instanceId: $"instance{i}"),
            ScopeLogs = { new ScopeLogs { Scope = CreateScope(), LogRecords = { CreateLogRecord(time: s_testTime.AddSeconds(i), message: $"log{i}") } } }
        }));
        await Task.WhenAll(tasks);

        Assert.Equal(50, await CountAsync(storage.ReadLogsAsync()));
    }

    private async Task<SqliteTelemetryStorage> CreateStorageAsync()
    {
        var storage = new SqliteTelemetryStorage(_dbPath, NullLogger<SqliteTelemetryStorage>.Instance);
        await storage.InitializeAsync();
        return storage;
    }

    private static TelemetryRepository CreateRepositoryWithStorage(ITelemetryStorage storage, PauseManager? pauseManager = null)
    {
        return new TelemetryRepository(
            NullLoggerFactory.Instance,
            Options.Create(new DashboardOptions()),
            pauseManager ?? new PauseManager(),
            [],
            storage);
    }

    private static async Task<int> CountAsync<T>(IAsyncEnumerable<T> source)
    {
        var count = 0;
        await foreach (var _ in source)
        {
            count++;
        }

        return count;
    }

    private static async Task WaitForAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!await condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Timed out waiting for telemetry to be persisted.");
            await Task.Delay(25);
        }
    }
}
