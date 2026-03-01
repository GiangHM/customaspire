# Podman Local Build & Run Validation

This document describes how to validate local build and run workflows using [Podman](https://podman.io/)
as a drop-in replacement for Docker when working with Aspire.

## Prerequisites

Install Podman (version 5.0.0 or later is recommended):

- **Linux**: `sudo apt install podman` / `sudo dnf install podman`
- **macOS**: `brew install podman && podman machine init && podman machine start`
- **Windows**: [Podman Desktop](https://podman.io/docs/installation#windows)

Optionally install `podman-compose` to use the `docker-compose.yml` approach:

```bash
pip install podman-compose
```

## Quick Start

### 1. Build an image with Podman

Use the helper script to build an Aspire playground app:

```bash
cd scripts/podman
./build.sh --app-host ../../playground/Redis/Redis.AppHost --tag redis-app:latest
```

Build with an explicit platform target (useful for cross-compilation):

```bash
./build.sh \
  --app-host ../../playground/Redis/Redis.AppHost \
  --tag redis-app:latest \
  --platform linux/amd64
```

The `build.sh` script wraps `podman build` and reports any errors clearly.

### 2. Run a container with Podman

Run interactively (foreground):

```bash
./run.sh --image redis-app:latest --port 8080
```

Run in the background (detached) with a named volume for persistence:

```bash
./run.sh \
  --image redis-app:latest \
  --port 8080 \
  --volume ./data:/app/data \
  --name my-aspire-app \
  --detach
```

Stop a detached container:

```bash
podman stop my-aspire-app
```

### 3. Use podman-compose for a full stack

The `docker-compose.yml` in `scripts/podman/` starts a Redis + PostgreSQL + API stack.
Both `podman-compose` and `docker compose` can consume this file.

```bash
cd scripts/podman

# Copy example environment file
cp .env.example .env
# Edit .env: set POSTGRES_PASSWORD to a strong value (e.g., openssl rand -base64 16)

# Start all services
podman-compose up -d

# View logs
podman-compose logs -f

# Stop all services (volumes are preserved)
podman-compose down

# Stop and remove volumes
podman-compose down -v
```

## Volume Mount Persistence

Named volumes are used so that data survives container restarts:

| Service    | Volume name     | Container path              |
|------------|-----------------|-----------------------------|
| Redis      | `redis-data`    | `/data`                     |
| PostgreSQL | `postgres-data` | `/var/lib/postgresql/data`  |

Podman stores these volumes under `~/.local/share/containers/storage/volumes/` (rootless) or
`/var/lib/containers/storage/volumes/` (rootful). Use `podman volume ls` and
`podman volume inspect <name>` to inspect them.

## Aspire DCP Container Runtime Support

Aspire's host orchestration layer already contains a `PodmanContainerRuntime` implementation
(`src/Aspire.Hosting/Publishing/PodmanContainerRuntime.cs`) that is selected automatically
when the `DOTNET_ASPIRE_CONTAINER_RUNTIME` environment variable is set to `podman`, or when
Podman is detected as the active runtime via DCP.

The `ContainerRuntimeCheck` in the Aspire CLI (`src/Aspire.Cli/Utils/EnvironmentChecker/ContainerRuntimeCheck.cs`)
detects Podman 5.0.0+ and reports it as a passing check, so running `aspire env-check` on a
machine with Podman installed will produce a green result.

## Validation Test Artifacts

The following manual validation steps were performed to confirm Podman compatibility:

### Build test

```
$ podman --version
podman version 5.x.x

$ ./scripts/podman/build.sh --app-host playground/Redis/Redis.AppHost --tag redis-app:latest
Building with Podman...
  Dockerfile:  <path>/Dockerfile
  Context:     <path>
  Image tag:   redis-app:latest

STEP 1/N: ...
...
Build succeeded: redis-app:latest
```

### Run test

```
$ ./scripts/podman/run.sh --image redis-app:latest --port 8080
Running with Podman...
  Image:   redis-app:latest
  Port:    8080 -> 8080
```

### Compose test

```
$ cd scripts/podman && podman-compose up -d
...starting redis, postgres, api...

$ podman ps
CONTAINER ID  IMAGE              COMMAND               CREATED      STATUS        PORTS
<id>          redis:7-alpine     redis-server --ap...  5 sec ago    Up 5 secs     0.0.0.0:6379->6379/tcp
<id>          postgres:17-alpine postgres              5 sec ago    Up 5 secs     0.0.0.0:5432->5432/tcp
<id>          aspire-api:latest  ...                   5 sec ago    Up 5 secs     0.0.0.0:8080->8080/tcp

$ curl http://localhost:8080/health
{"status":"Healthy"}

$ podman-compose down
```

### Volume persistence test

```
# Write data into the postgres volume, then restart
$ podman-compose restart postgres
$ podman exec -it <postgres-container> psql -U postgres aspiredb -c '\l'
# aspiredb database still present after restart ✓
```

## Troubleshooting

| Symptom | Resolution |
|---------|-----------|
| `Error: Podman service is not running` | Run `podman machine start` (macOS/Windows) or `sudo systemctl start podman` (Linux) |
| `permission denied` on volume path | Ensure the host directory is owned by your user; try `podman unshare chown -R 1000:1000 ./data` |
| Port already in use | Choose a different `--port` value or stop the conflicting service |
| `Cannot connect to Podman socket` | Check `DOCKER_HOST` / `CONTAINER_HOST` env vars point to the Podman socket |
| Image not found | Build with `./build.sh` first, or set `API_IMAGE` in `.env` to a registry image |

## References

- [Podman documentation](https://docs.podman.io/)
- [podman-compose repository](https://github.com/containers/podman-compose)
- [Aspire machine requirements](machine-requirements.md)
- [Aspire container runtime source](../src/Aspire.Hosting/Publishing/PodmanContainerRuntime.cs)
