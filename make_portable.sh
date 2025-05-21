#!/bin/bash
set -e

# Build TundraDB from source
echo "Building TundraDB from source..."
mkdir -p build
cd build
cmake ..
make -j$(sysctl -n hw.ncpu)
cd ..

# Create distribution directory
DIST_DIR="tundradb_portable"
LIB_DIR="$DIST_DIR/lib"
BIN_DIR="$DIST_DIR/bin"

# Clean up previous build if exists
rm -rf "$DIST_DIR"

mkdir -p "$LIB_DIR"
mkdir -p "$BIN_DIR"

# Copy the binary
echo "Copying tundra_shell binary..."
cp build/tundra_shell "$BIN_DIR/"

# Find and copy dependencies
echo "Analyzing dependencies..."
DEPS=$(otool -L "$BIN_DIR/tundra_shell" | grep -v "$(basename $BIN_DIR/tundra_shell)" | grep -v /System | grep -v /usr/lib | awk '{print $1}')

for dep in $DEPS; do
    echo "Processing dependency: $dep"
    # Skip system libraries
    if [[ "$dep" == /usr/lib/* || "$dep" == /System/* ]]; then
        echo "Skipping system library: $dep"
        continue
    fi
    
    # Copy the library to the lib directory
    lib_name=$(basename "$dep")
    echo "Copying $dep to $LIB_DIR/$lib_name"
    cp "$dep" "$LIB_DIR/$lib_name"
    
    # Update the binary to use the bundled library
    echo "Updating binary to use bundled $lib_name"
    install_name_tool -change "$dep" "@executable_path/../lib/$lib_name" "$BIN_DIR/tundra_shell"
    
    # Process the library's dependencies
    lib_deps=$(otool -L "$LIB_DIR/$lib_name" | grep -v "$lib_name" | grep -v /System | grep -v /usr/lib | awk '{print $1}')
    for lib_dep in $lib_deps; do
        lib_dep_name=$(basename "$lib_dep")
        if [[ ! -f "$LIB_DIR/$lib_dep_name" && "$lib_dep" != /usr/lib/* && "$lib_dep" != /System/* ]]; then
            echo "Copying dependency $lib_dep to $LIB_DIR/$lib_dep_name"
            cp "$lib_dep" "$LIB_DIR/$lib_dep_name"
        fi
        
        # Update the library to use the bundled dependency
        echo "Updating $lib_name to use bundled $lib_dep_name"
        install_name_tool -change "$lib_dep" "@executable_path/../lib/$lib_dep_name" "$LIB_DIR/$lib_name"
    done
done

# Second pass to fix nested dependencies
echo "Fixing nested dependencies..."
for lib in "$LIB_DIR"/*; do
    lib_name=$(basename "$lib")
    lib_deps=$(otool -L "$lib" | grep -v "$lib_name" | grep -v /System | grep -v /usr/lib | awk '{print $1}')
    
    for lib_dep in $lib_deps; do
        lib_dep_name=$(basename "$lib_dep")
        if [[ -f "$LIB_DIR/$lib_dep_name" ]]; then
            echo "Updating $lib_name to use bundled $lib_dep_name"
            install_name_tool -change "$lib_dep" "@executable_path/../lib/$lib_dep_name" "$lib"
        fi
    done
done

# Create a launcher script
echo "Creating launcher script..."
cat > "$DIST_DIR/tundra_shell_launcher.sh" << 'EOF'
#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export DYLD_LIBRARY_PATH="$DIR/lib:$DYLD_LIBRARY_PATH"
"$DIR/bin/tundra_shell" "$@"
EOF

chmod +x "$DIST_DIR/tundra_shell_launcher.sh"

# Create README
cat > "$DIST_DIR/README.txt" << 'EOF'
TundraDB Portable Shell

To run:
1. Open Terminal
2. Navigate to this directory
3. Execute: ./tundra_shell_launcher.sh

This bundle contains all necessary dependencies to run TundraDB Shell on macOS.
EOF

echo "Packaging as zip file..."
zip -r tundradb_portable.zip "$DIST_DIR"

echo "Done! Portable binary created in tundradb_portable.zip"
echo "Unzip and run ./tundra_shell_launcher.sh on the target machine" 