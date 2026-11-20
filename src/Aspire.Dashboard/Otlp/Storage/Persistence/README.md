# Persistence Layer

This folder contains the persistent telemetry storage implementations for the Aspire Dashboard.

## Overview

The default Aspire Dashboard stores all telemetry (logs, traces, metrics) in memory using circular buffers.
The persistence layer adds optional write-through storage to SQLite, so telemetry data survives dashboard restarts.

## Configuration

Set `Dashboard:Storage:SqlitePath` (environment variable `DASHBOARD__STORAGE__SQLITEPATH`) to the path of the
database file. When it is empty, nothing is persisted. The Dockerfile sets it to `/data/telemetry.db`; mount a
volume at `/data` to keep the file across container restarts.

## How it works

- `ITelemetryStorage` — abstraction over a persistence backend.
- `NullTelemetryStorage` — used when persistence is disabled.
- `SqliteTelemetryStorage` — SQLite implementation. Safe for concurrent callers (access is serialized).
- `QueuedTelemetryStorage` — wraps the SQLite storage. `TelemetryRepository` writes are queued to a single background
  writer, which keeps ingestion fast and preserves order. The queue is bounded: if SQLite cannot keep up, writes are
  dropped and counted (telemetry stays in memory). Disposal drains the queue before closing the database.
- `TelemetryStorageInitializer` — hosted service that initializes the storage and replays it into `TelemetryRepository`
  at startup, before the dashboard accepts telemetry.

## Retention

The database is bounded by the dashboard's telemetry limits (`Dashboard:TelemetryLimits`): the newest
`MaxLogCount` log batches, `MaxTraceCount` span batches and `MaxMetricsCount` data points per metric instrument are
kept. Older rows are deleted when the storage starts and then after every 1000 writes. SQLite reuses the freed pages,
so the file stops growing but does not shrink.

## Security

Persisted telemetry (log messages, attributes, and so on) is stored unencrypted. Protect the database file and its
volume like any other store of application data.
