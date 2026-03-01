#!/bin/bash
set -euo pipefail

# ---------------------------------------------------------------------------
# TundraDB macOS Bundle Creator
#
# Produces a self-contained distributable archive for macOS.
# Uses dylibbundler to collect and rewrite all dynamic library references.
#
# Prerequisites:
#   brew install dylibbundler
#
# Usage:
#   ./scripts/create_macos_bundle.sh              # defaults
#   ./scripts/create_macos_bundle.sh --skip-build  # reuse existing build
# ---------------------------------------------------------------------------

VERSION="1.0.0"
BUNDLE_NAME="TundraDB"
APP_NAME="tundra_shell"

SKIP_BUILD=false
for arg in "$@"; do
    case "$arg" in
        --skip-build) SKIP_BUILD=true ;;
    esac
done

# Paths (all relative to project root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${PROJECT_DIR}/build_release"
DIST_DIR="${PROJECT_DIR}/dist"
BUNDLE_DIR="${DIST_DIR}/${BUNDLE_NAME}-${VERSION}-macOS"
ARCH="$(uname -m)"   # arm64 or x86_64

echo "=== TundraDB macOS Bundle Creator ==="
echo "Version : ${VERSION}"
echo "Arch    : ${ARCH}"
echo "Build   : ${BUILD_DIR}"
echo "Output  : ${BUNDLE_DIR}"
echo ""

# ---------------------------------------------------------------------------
# 1. Check prerequisites
# ---------------------------------------------------------------------------
if ! command -v dylibbundler &>/dev/null; then
    echo "❌ dylibbundler not found. Install it with:"
    echo "   brew install dylibbundler"
    exit 1
fi

if ! command -v cmake &>/dev/null; then
    echo "❌ cmake not found. Install it with:"
    echo "   brew install cmake"
    exit 1
fi

# ---------------------------------------------------------------------------
# 2. Build (Release)
# ---------------------------------------------------------------------------
if [ "$SKIP_BUILD" = false ]; then
    echo ">>> Configuring (Release)..."
    cmake -B "${BUILD_DIR}" -S "${PROJECT_DIR}" \
        -DCMAKE_BUILD_TYPE=Release \
        -DTUNDRADB_BUILD_TESTS=OFF \
        -DTUNDRADB_BUILD_BENCHMARKS=OFF

    echo ""
    echo ">>> Building..."
    cmake --build "${BUILD_DIR}" -j"$(sysctl -n hw.ncpu)"
    echo ""
fi

if [ ! -f "${BUILD_DIR}/${APP_NAME}" ]; then
    echo "❌ Binary not found at ${BUILD_DIR}/${APP_NAME}"
    echo "   Run without --skip-build or build manually first."
    exit 1
fi

# ---------------------------------------------------------------------------
# 3. Prepare bundle directory
# ---------------------------------------------------------------------------
rm -rf "${BUNDLE_DIR}"
mkdir -p "${BUNDLE_DIR}/bin"
mkdir -p "${BUNDLE_DIR}/libs"

# Copy the binary (we need a fresh copy; dylibbundler modifies it in place)
cp "${BUILD_DIR}/${APP_NAME}" "${BUNDLE_DIR}/bin/"
chmod +w "${BUNDLE_DIR}/bin/${APP_NAME}"

echo ">>> Running dylibbundler..."
dylibbundler -od -b \
    -x "${BUNDLE_DIR}/bin/${APP_NAME}" \
    -d "${BUNDLE_DIR}/libs/" \
    -p @executable_path/../libs/ \
    -s /usr/local/lib \
    -s /opt/homebrew/lib \
    -s /opt/homebrew/opt/llvm/lib

echo ""

# ---------------------------------------------------------------------------
# 4. Create launcher script
# ---------------------------------------------------------------------------
cat > "${BUNDLE_DIR}/tundra_shell" << 'EOF'
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/bin/tundra_shell" "$@"
EOF
chmod +x "${BUNDLE_DIR}/tundra_shell"

# ---------------------------------------------------------------------------
# 5. Metadata
# ---------------------------------------------------------------------------
echo "${VERSION}" > "${BUNDLE_DIR}/VERSION"

# ---------------------------------------------------------------------------
# 6. Verify
# ---------------------------------------------------------------------------
echo ">>> Verifying binary..."
UNRESOLVED=$(otool -L "${BUNDLE_DIR}/bin/${APP_NAME}" \
    | tail -n +2 \
    | awk '{print $1}' \
    | grep -vE '^(/usr/lib/|/System/|@executable_path|@rpath)' || true)

if [ -n "$UNRESOLVED" ]; then
    echo "⚠️  Warning — these references are not bundled:"
    echo "$UNRESOLVED"
else
    echo "✅ All non-system libraries are bundled"
fi

# Quick smoke test
if "${BUNDLE_DIR}/tundra_shell" --help &>/dev/null; then
    echo "✅ Smoke test passed (--help)"
else
    echo "⚠️  Smoke test failed — the binary may not run correctly"
fi

# ---------------------------------------------------------------------------
# 7. Create archive
# ---------------------------------------------------------------------------
ARCHIVE_NAME="${BUNDLE_NAME}-${VERSION}-macOS-${ARCH}.tar.gz"
cd "${DIST_DIR}"
tar -czf "${ARCHIVE_NAME}" "$(basename "${BUNDLE_DIR}")"

ARCHIVE_SIZE=$(du -sh "${ARCHIVE_NAME}" | awk '{print $1}')

echo ""
echo "=== Done ==="
echo "📦 Archive  : dist/${ARCHIVE_NAME}  (${ARCHIVE_SIZE})"
echo "📁 Directory: dist/$(basename "${BUNDLE_DIR}")/"
echo ""
echo "To test:"
echo "  cd dist/$(basename "${BUNDLE_DIR}")"
echo "  ./tundra_shell --help"
