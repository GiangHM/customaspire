#!/usr/bin/env bash
# Licensed to the .NET Foundation under one or more agreements.
# The .NET Foundation licenses this file to you under the MIT license.

# Podman build script for Aspire applications.
# Usage: ./build.sh [--app-host <path>] [--tag <image-tag>] [--platform <platform>] [--context <path>]
# Example: ./build.sh --app-host src/Aspire.Dashboard --tag aspire-dashboard:latest

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

APP_HOST_PATH=""
IMAGE_TAG="aspire-app:latest"
PLATFORM=""
BUILD_CONTEXT=""

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --app-host <path>     Path to the Aspire AppHost project directory (required)"
    echo "  --tag <image-tag>     Tag for the built image (default: aspire-app:latest)"
    echo "  --platform <platform> Target platform (e.g., linux/amd64, linux/arm64)"
    echo "  --context <path>      Build context directory (default: repo root)"
    echo "  --help                Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --app-host src/Aspire.Dashboard"
    echo "  $0 --app-host src/Aspire.Dashboard --tag aspire-dashboard:1.0"
    echo "  $0 --app-host src/Aspire.Dashboard --platform linux/amd64"
    echo "  $0 --app-host src/Aspire.Dashboard --context . --tag aspire-dashboard:latest"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --app-host)
            APP_HOST_PATH="$2"
            shift 2
            ;;
        --tag)
            IMAGE_TAG="$2"
            shift 2
            ;;
        --platform)
            PLATFORM="$2"
            shift 2
            ;;
        --context)
            BUILD_CONTEXT="$2"
            shift 2
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

echo "Podman version: $(podman --version)"

# Verify Podman is running
if ! podman ps &>/dev/null; then
    echo "Error: Podman service is not running."
    echo "Start the Podman service with: sudo systemctl start podman"
    echo "Or on macOS/Windows: podman machine start"
    exit 1
fi

if [[ -z "${APP_HOST_PATH}" ]]; then
    echo "Error: --app-host is required."
    echo ""
    usage
    exit 1
fi

# Resolve the app host path
if [[ ! "${APP_HOST_PATH}" = /* ]]; then
    APP_HOST_PATH="${REPO_ROOT}/${APP_HOST_PATH}"
fi

if [[ ! -d "${APP_HOST_PATH}" ]]; then
    echo "Error: AppHost directory does not exist: ${APP_HOST_PATH}"
    exit 1
fi

# Find Dockerfile in the app host directory or parent
DOCKERFILE=""
for search_path in "${APP_HOST_PATH}" "$(dirname "${APP_HOST_PATH}")"; do
    if [[ -f "${search_path}/Dockerfile" ]]; then
        DOCKERFILE="${search_path}/Dockerfile"
        break
    fi
done

if [[ -z "${DOCKERFILE}" ]]; then
    echo "Error: No Dockerfile found in ${APP_HOST_PATH} or its parent directory."
    echo "Ensure a Dockerfile exists before running this script."
    exit 1
fi

# Resolve build context: use --context if provided, otherwise default to repo root.
# The repo root is the correct default for Aspire Dockerfiles that reference paths
# relative to the repository root (e.g., Directory.Build.props, NuGet.config).
# Use --context to override when building Dockerfiles that expect a different build context.
if [[ -n "${BUILD_CONTEXT}" ]]; then
    if [[ "${BUILD_CONTEXT}" != /* ]]; then
        BUILD_CONTEXT="${REPO_ROOT}/${BUILD_CONTEXT}"
    fi
    CONTEXT_PATH="${BUILD_CONTEXT}"
else
    CONTEXT_PATH="${REPO_ROOT}"
fi

echo ""
echo "Building with Podman..."
echo "  Dockerfile:  ${DOCKERFILE}"
echo "  Context:     ${CONTEXT_PATH}"
echo "  Image tag:   ${IMAGE_TAG}"
[[ -n "${PLATFORM}" ]] && echo "  Platform:    ${PLATFORM}"
echo ""

BUILD_ARGS=("build" "--file" "${DOCKERFILE}" "--tag" "${IMAGE_TAG}")

if [[ -n "${PLATFORM}" ]]; then
    BUILD_ARGS+=("--platform" "${PLATFORM}")
fi

BUILD_ARGS+=("${CONTEXT_PATH}")

# Run the Podman build
podman "${BUILD_ARGS[@]}"

echo ""
echo "Build succeeded: ${IMAGE_TAG}"
echo "Run the image with: podman run --rm -p 8080:8080 ${IMAGE_TAG}"
echo "Or use podman-compose: podman-compose -f docker-compose.yml up"
