#!/bin/bash
set -euo pipefail

# ---------------------------------------------------------------------------
# TundraDB Linux Bundle Creator (Docker-based)
#
# Builds a self-contained Linux x86_64 distributable from macOS (or Linux)
# using Docker. Produces a .tar.gz archive with tundra_shell and all its
# shared library dependencies.
#
# Prerequisites:
#   - Docker Desktop running (with linux/amd64 support)
#
# Usage:
#   ./scripts/create_linux_bundle.sh                 # build image + extract
#   ./scripts/create_linux_bundle.sh --no-cache      # rebuild from scratch
#   ./scripts/create_linux_bundle.sh --skip-build    # reuse existing image
# ---------------------------------------------------------------------------

VERSION="1.0.0"
IMAGE_NAME="tundradb-linux-release"
CONTAINER_NAME="tundradb-linux-extract"

NO_CACHE=""
SKIP_BUILD=false

for arg in "$@"; do
    case "$arg" in
        --no-cache)   NO_CACHE="--no-cache" ;;
        --skip-build) SKIP_BUILD=true ;;
    esac
done

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DIST_DIR="${PROJECT_DIR}/dist"

echo "=== TundraDB Linux Bundle Creator ==="
echo "Version    : ${VERSION}"
echo "Image      : ${IMAGE_NAME}"
echo "Project    : ${PROJECT_DIR}"
echo "Output     : ${DIST_DIR}/"
echo ""

# --- 1. Check prerequisites ---
if ! command -v docker &>/dev/null; then
    echo "❌ Docker not found. Please install Docker Desktop."
    exit 1
fi

if ! docker info &>/dev/null 2>&1; then
    echo "❌ Docker daemon is not running. Please start Docker Desktop."
    exit 1
fi

# --- 2. Build the Docker image ---
if [ "$SKIP_BUILD" = false ]; then
    echo ">>> Building Docker image (this will take a few minutes on first run)..."
    echo "    Subsequent builds use Docker layer caching and are much faster."
    echo ""

    docker build \
        --platform linux/amd64 \
        -f "${PROJECT_DIR}/docker/Dockerfile.linux-release" \
        -t "${IMAGE_NAME}" \
        --build-arg TUNDRADB_VERSION="${VERSION}" \
        ${NO_CACHE} \
        "${PROJECT_DIR}"

    echo ""
    echo "✅ Docker image built successfully."
else
    echo ">>> Skipping build (--skip-build). Reusing existing image."
    if ! docker image inspect "${IMAGE_NAME}" &>/dev/null; then
        echo "❌ Image '${IMAGE_NAME}' not found. Run without --skip-build first."
        exit 1
    fi
fi

# --- 3. Extract the tarball ---
echo ""
echo ">>> Extracting Linux tarball from Docker image..."

mkdir -p "${DIST_DIR}"

# Remove any previous extraction container
docker rm -f "${CONTAINER_NAME}" &>/dev/null || true

# Create a container (don't start it) and copy the tarball out
docker create --platform linux/amd64 --name "${CONTAINER_NAME}" "${IMAGE_NAME}" /bin/true
docker cp "${CONTAINER_NAME}:/dist/TundraDB-${VERSION}-Linux-x86_64.tar.gz" "${DIST_DIR}/"
docker rm "${CONTAINER_NAME}"

ARCHIVE="${DIST_DIR}/TundraDB-${VERSION}-Linux-x86_64.tar.gz"
ARCHIVE_SIZE=$(du -sh "${ARCHIVE}" | awk '{print $1}')

echo ""
echo "=== Done ==="
echo "📦 Archive  : dist/TundraDB-${VERSION}-Linux-x86_64.tar.gz  (${ARCHIVE_SIZE})"
echo ""
echo "To use on a Linux machine:"
echo "  scp dist/TundraDB-${VERSION}-Linux-x86_64.tar.gz user@host:~/"
echo "  ssh user@host"
echo "  tar xzf TundraDB-${VERSION}-Linux-x86_64.tar.gz"
echo "  cd TundraDB-${VERSION}-Linux-x86_64"
echo "  ./tundra_shell --db-path ./my-database"

