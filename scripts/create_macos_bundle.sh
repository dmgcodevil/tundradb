#!/bin/bash

# TundraDB macOS Bundle Creator
# Uses environment variables for library loading (no binary modification)
set -e

# Configuration
BUNDLE_NAME="TundraDB"
VERSION="1.0.0"
BUILD_DIR="build"
BUNDLE_DIR="dist/${BUNDLE_NAME}-${VERSION}-macOS"
APP_NAME="tundra_shell"

echo "Creating TundraDB macOS bundle..."

# Clean and create bundle directory
rm -rf dist
mkdir -p "${BUNDLE_DIR}/bin"
mkdir -p "${BUNDLE_DIR}/lib"

# Build the project if not already built
if [ ! -f "${BUILD_DIR}/${APP_NAME}" ]; then
    echo "Building TundraDB..."
    mkdir -p ${BUILD_DIR}
    cd ${BUILD_DIR}
    cmake .. -DCMAKE_BUILD_TYPE=Release
    make -j$(sysctl -n hw.ncpu)
    cd ..
fi

# Copy the main executable without modification
cp "${BUILD_DIR}/${APP_NAME}" "${BUNDLE_DIR}/bin/"

# Copy all required libraries without modifying them
echo "Copying dependencies..."

# Function to copy libraries recursively
copy_dependencies() {
    local binary="$1"
    local lib_dir="$2"
    
    # Get list of dynamic libraries
    otool -L "$binary" | grep -E "(homebrew|local|@rpath)" | awk '{print $1}' | while read lib; do
        # Handle @rpath libraries
        if [[ "$lib" == @rpath/* ]]; then
            lib_name=$(basename "$lib")
            # Try to find the actual library in common locations
            actual_lib=""
            for search_path in /opt/homebrew/lib /usr/local/lib; do
                if [ -f "${search_path}/${lib_name}" ]; then
                    actual_lib="${search_path}/${lib_name}"
                    break
                fi
            done
            
            if [ -n "$actual_lib" ] && [ -f "$actual_lib" ]; then
                if [ ! -f "${lib_dir}/${lib_name}" ]; then
                    echo "  Copying $lib_name (from @rpath)"
                    cp "$actual_lib" "${lib_dir}/"
                    chmod +w "${lib_dir}/${lib_name}"
                    
                    # Copy dependencies of this library recursively
                    copy_dependencies "${lib_dir}/${lib_name}" "$lib_dir"
                fi
            fi
        elif [ -f "$lib" ]; then
            # Handle regular library paths
            lib_name=$(basename "$lib")
            if [ ! -f "${lib_dir}/${lib_name}" ]; then
                echo "  Copying $lib_name"
                cp "$lib" "${lib_dir}/"
                chmod +w "${lib_dir}/${lib_name}"
                
                # Copy dependencies of this library recursively
                copy_dependencies "${lib_dir}/${lib_name}" "$lib_dir"
            fi
        fi
    done
}

# Copy dependencies for the main executable
copy_dependencies "${BUNDLE_DIR}/bin/${APP_NAME}" "${BUNDLE_DIR}/lib"

# Create a launcher script that sets up the environment
cat > "${BUNDLE_DIR}/tundra_shell" << 'EOF'
#!/bin/bash
# TundraDB Launcher Script

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set up library paths for both Homebrew locations and our bundled libs
export DYLD_LIBRARY_PATH="${SCRIPT_DIR}/lib:/opt/homebrew/lib:/usr/local/lib:${DYLD_LIBRARY_PATH}"
export DYLD_FALLBACK_LIBRARY_PATH="${SCRIPT_DIR}/lib:/opt/homebrew/lib:/usr/local/lib"

# Run the application
exec "${SCRIPT_DIR}/bin/tundra_shell" "$@"
EOF

chmod +x "${BUNDLE_DIR}/tundra_shell"

# Create README
cat > "${BUNDLE_DIR}/README.md" << EOF
# TundraDB v${VERSION} for macOS

## Installation
1. Extract this archive to any location
2. Run \`./tundra_shell\` from the extracted directory

## Usage
\`\`\`bash
./tundra_shell --help
./tundra_shell --db-path /path/to/your/database
\`\`\`

## Requirements
- macOS 10.15 or later
- This bundle includes most dependencies, but may require Homebrew libraries to be installed

## Support
For issues and documentation, visit: https://github.com/yourusername/tundradb
EOF

# Create version info
echo "${VERSION}" > "${BUNDLE_DIR}/VERSION"

# Create the final archive
cd dist
tar -czf "${BUNDLE_NAME}-${VERSION}-macOS.tar.gz" "${BUNDLE_NAME}-${VERSION}-macOS"
cd ..

echo "✅ Bundle created successfully!"
echo "📦 Location: dist/${BUNDLE_NAME}-${VERSION}-macOS.tar.gz"
echo "📁 Directory: ${BUNDLE_DIR}"
echo ""
echo "To test the bundle:"
echo "  cd ${BUNDLE_DIR}"
echo "  ./tundra_shell --help" 