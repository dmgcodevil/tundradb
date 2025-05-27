#!/bin/bash

# TundraDB macOS App Bundle Creator
set -e

# Configuration
APP_NAME="TundraDB"
VERSION="1.0.0"
BUILD_DIR="build"
APP_BUNDLE="dist/${APP_NAME}.app"
EXECUTABLE_NAME="tundra_shell"

echo "Creating TundraDB macOS App Bundle..."

# Clean and create app bundle structure
rm -rf dist
mkdir -p "${APP_BUNDLE}/Contents/MacOS"
mkdir -p "${APP_BUNDLE}/Contents/Resources"
mkdir -p "${APP_BUNDLE}/Contents/Frameworks"

# Build the project if not already built
if [ ! -f "${BUILD_DIR}/${EXECUTABLE_NAME}" ]; then
    echo "Building TundraDB..."
    mkdir -p ${BUILD_DIR}
    cd ${BUILD_DIR}
    cmake .. -DCMAKE_BUILD_TYPE=Release
    make -j$(sysctl -n hw.ncpu)
    cd ..
fi

# Copy the main executable
cp "${BUILD_DIR}/${EXECUTABLE_NAME}" "${APP_BUNDLE}/Contents/MacOS/"

# Create Info.plist
cat > "${APP_BUNDLE}/Contents/Info.plist" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleExecutable</key>
    <string>${EXECUTABLE_NAME}</string>
    <key>CFBundleIdentifier</key>
    <string>com.tundradb.shell</string>
    <key>CFBundleName</key>
    <string>${APP_NAME}</string>
    <key>CFBundleVersion</key>
    <string>${VERSION}</string>
    <key>CFBundleShortVersionString</key>
    <string>${VERSION}</string>
    <key>CFBundleInfoDictionaryVersion</key>
    <string>6.0</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleSignature</key>
    <string>TNDR</string>
    <key>LSMinimumSystemVersion</key>
    <string>10.15</string>
    <key>LSApplicationCategoryType</key>
    <string>public.app-category.developer-tools</string>
    <key>NSHighResolutionCapable</key>
    <true/>
</dict>
</plist>
EOF

# Function to copy and fix library dependencies
fix_dependencies() {
    local binary="$1"
    local frameworks_dir="$2"
    
    # Get list of dynamic libraries
    otool -L "$binary" | grep -E "(homebrew|local)" | awk '{print $1}' | while read lib; do
        if [ -f "$lib" ]; then
            lib_name=$(basename "$lib")
            if [ ! -f "${frameworks_dir}/${lib_name}" ]; then
                echo "  Copying $lib_name"
                cp "$lib" "${frameworks_dir}/"
                chmod +w "${frameworks_dir}/${lib_name}"
                
                # Fix the library's own dependencies recursively
                fix_dependencies "${frameworks_dir}/${lib_name}" "$frameworks_dir"
                
                # Update the library's install name
                install_name_tool -id "@loader_path/../Frameworks/${lib_name}" "${frameworks_dir}/${lib_name}"
            fi
            
            # Update the binary to use the bundled library
            install_name_tool -change "$lib" "@loader_path/../Frameworks/${lib_name}" "$binary"
        fi
    done
}

# Bundle dependencies
echo "Bundling dependencies..."
fix_dependencies "${APP_BUNDLE}/Contents/MacOS/${EXECUTABLE_NAME}" "${APP_BUNDLE}/Contents/Frameworks"

# Create a simple icon (you can replace this with a proper .icns file)
# For now, we'll skip the icon creation

# Create a launcher script for Terminal usage
cat > "dist/tundra_shell" << 'EOF'
#!/bin/bash
# TundraDB Terminal Launcher

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Run the app bundle executable
exec "${SCRIPT_DIR}/TundraDB.app/Contents/MacOS/tundra_shell" "$@"
EOF

chmod +x "dist/tundra_shell"

# Create README
cat > "dist/README.md" << EOF
# TundraDB v${VERSION} for macOS

## Installation Options

### Option 1: App Bundle
1. Copy \`${APP_NAME}.app\` to your Applications folder
2. Run from Terminal: \`/Applications/${APP_NAME}.app/Contents/MacOS/${EXECUTABLE_NAME}\`

### Option 2: Terminal Script
1. Use the provided \`tundra_shell\` script
2. Run: \`./tundra_shell --help\`

## Usage
\`\`\`bash
# Using the app bundle directly
/Applications/${APP_NAME}.app/Contents/MacOS/${EXECUTABLE_NAME} --db-path /path/to/db

# Using the launcher script
./tundra_shell --db-path /path/to/db
\`\`\`

## Requirements
- macOS 10.15 or later
- No additional dependencies required (all bundled)

## Support
For issues and documentation, visit: https://github.com/yourusername/tundradb
EOF

# Create DMG (optional - requires create-dmg or hdiutil)
if command -v create-dmg >/dev/null 2>&1; then
    echo "Creating DMG..."
    create-dmg \
        --volname "${APP_NAME} ${VERSION}" \
        --window-pos 200 120 \
        --window-size 600 300 \
        --icon-size 100 \
        --icon "${APP_NAME}.app" 175 120 \
        --hide-extension "${APP_NAME}.app" \
        --app-drop-link 425 120 \
        "dist/${APP_NAME}-${VERSION}.dmg" \
        "dist/"
else
    echo "create-dmg not found, skipping DMG creation"
    echo "You can install it with: brew install create-dmg"
fi

echo "✅ App bundle created successfully!"
echo "📱 App Bundle: dist/${APP_NAME}.app"
echo "🖥️  Terminal Script: dist/tundra_shell"
echo ""
echo "To test:"
echo "  ./dist/tundra_shell --help"
echo "  open dist/${APP_NAME}.app"
EOF 