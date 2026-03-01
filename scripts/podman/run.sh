#!/usr/bin/env bash
# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.

# Podman run script for Aspire playground applications.
# Usage: ./run.sh [--image <image>] [--port <host-port>] [--volume <host-path:container-path>]
# Example: ./run.sh --image aspire-app:latest --port 8080

set -euo pipefail

IMAGE="aspire-app:latest"
HOST_PORT="8080"
CONTAINER_PORT="8080"
VOLUME_MOUNT=""
DETACH=false
CONTAINER_NAME=""
EXTRA_ARGS=""

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --image <image>             Container image to run (default: aspire-app:latest)"
    echo "  --port <host-port>          Host port to bind (default: 8080)"
    echo "  --container-port <port>     Container port to expose (default: 8080)"
    echo "  --volume <src:dest>         Volume mount for persistence (e.g., ./data:/app/data)"
    echo "  --name <container-name>     Name for the container"
    echo "  --detach                    Run container in background"
    echo "  --help                      Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --image aspire-app:latest --port 8080"
    echo "  $0 --image aspire-app:latest --volume ./data:/app/data --detach"
    echo "  $0 --image aspire-app:latest --name my-aspire-app --port 9090"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --image)
            IMAGE="$2"
            shift 2
            ;;
        --port)
            HOST_PORT="$2"
            shift 2
            ;;
        --container-port)
            CONTAINER_PORT="$2"
            shift 2
            ;;
        --volume)
            VOLUME_MOUNT="$2"
            shift 2
            ;;
        --name)
            CONTAINER_NAME="$2"
            shift 2
            ;;
        --detach)
            DETACH=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Check if Podman is available
if ! command -v podman &>/dev/null; then
    echo "Error: Podman is not installed or not in PATH."
    echo "Install Podman from: https://podman.io/getting-started/installation"
    exit 1
fi

# Verify Podman is running
if ! podman ps &>/dev/null; then
    echo "Error: Podman service is not running."
    echo "Start the Podman service with: sudo systemctl start podman"
    echo "Or on macOS/Windows: podman machine start"
    exit 1
fi

# Check if the image exists locally; pull if not
if ! podman image exists "${IMAGE}"; then
    echo "Image '${IMAGE}' not found locally. Attempting to pull..."
    if ! podman pull "${IMAGE}"; then
        echo "Error: Failed to pull image '${IMAGE}'."
        echo "Build the image first using: ./build.sh --tag ${IMAGE}"
        exit 1
    fi
fi

echo ""
echo "Running with Podman..."
echo "  Image:   ${IMAGE}"
echo "  Port:    ${HOST_PORT} -> ${CONTAINER_PORT}"
[[ -n "${VOLUME_MOUNT}" ]] && echo "  Volume:  ${VOLUME_MOUNT}"
[[ -n "${CONTAINER_NAME}" ]] && echo "  Name:    ${CONTAINER_NAME}"
[[ "${DETACH}" == "true" ]] && echo "  Mode:    detached"
echo ""

RUN_ARGS=("run" "--rm")

if [[ "${DETACH}" == "true" ]]; then
    RUN_ARGS+=("--detach")
fi

RUN_ARGS+=("-p" "${HOST_PORT}:${CONTAINER_PORT}")

if [[ -n "${VOLUME_MOUNT}" ]]; then
    # Create host directory if it doesn't exist
    HOST_DIR="${VOLUME_MOUNT%%:*}"
    if [[ ! -d "${HOST_DIR}" ]]; then
        echo "Creating host volume directory: ${HOST_DIR}"
        mkdir -p "${HOST_DIR}"
    fi
    RUN_ARGS+=("-v" "${VOLUME_MOUNT}")
fi

if [[ -n "${CONTAINER_NAME}" ]]; then
    RUN_ARGS+=("--name" "${CONTAINER_NAME}")
fi

RUN_ARGS+=("${IMAGE}")

# Run the container
podman "${RUN_ARGS[@]}"

if [[ "${DETACH}" == "true" ]]; then
    echo ""
    echo "Container started in background."
    echo "Access the app at: http://localhost:${HOST_PORT}"
    echo "View logs:         podman logs ${CONTAINER_NAME:-<container-id>}"
    echo "Stop container:    podman stop ${CONTAINER_NAME:-<container-id>}"
fi
