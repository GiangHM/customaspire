# Persistence Layer

This folder contains the persistent telemetry storage implementations for the Aspire Dashboard.

## Overview

The default Aspire Dashboard stores all telemetry (logs, traces, metrics) in memory using circular buffers.
The persistence layer adds optional write-through storage to SQLite, so telemetry data survives dashboard restarts.

## Key Files (planned)

- `ITelemetryStorage.cs` — Abstraction interface for persistence backends
- `SqliteTelemetryStorage.cs` — SQLite implementation
- Configuration is via `Dashboard:Storage:SqlitePath` in dashboard config
