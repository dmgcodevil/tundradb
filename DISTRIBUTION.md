# TundraDB macOS Distribution Guide

This guide explains how to create portable distribution packages for TundraDB on macOS.

## Quick Start

### Option 1: Simple Portable Bundle (Recommended)
```bash
# Create a portable bundle with all dependencies
./scripts/create_macos_bundle.sh

# This creates:
# - dist/TundraDB-1.0.0-macOS.tar.gz (compressed archive)
# - dist/TundraDB-1.0.0-macOS/ (directory with all files)
```

### Option 2: macOS App Bundle
```bash
# Create a native macOS .app bundle
./scripts/create_macos_app.sh

# This creates:
# - dist/TundraDB.app (native macOS application)
# - dist/tundra_shell (terminal launcher script)
# - dist/TundraDB-1.0.0.dmg (if create-dmg is installed)
```

## Distribution Options

### 1. Portable Bundle (.tar.gz)
**Best for:** Developers, command-line users, CI/CD

**Advantages:**
- Self-contained (no external dependencies)
- Works on any macOS system
- Easy to distribute via download
- Can be extracted anywhere

**Usage:**
```bash
# Extract and run
tar -xzf TundraDB-1.0.0-macOS.tar.gz
cd TundraDB-1.0.0-macOS
./tundra_shell --help
```

### 2. macOS App Bundle (.app)
**Best for:** End users, GUI integration

**Advantages:**
- Native macOS experience
- Can be installed in Applications folder
- Proper macOS metadata and versioning
- Can be signed and notarized

**Usage:**
```bash
# Copy to Applications and run
cp -r TundraDB.app /Applications/
/Applications/TundraDB.app/Contents/MacOS/tundra_shell --help
```

### 3. Disk Image (.dmg)
**Best for:** Professional distribution

**Prerequisites:**
```bash
brew install create-dmg
```

**Advantages:**
- Professional installer experience
- Can include license agreements
- Drag-and-drop installation
- Compressed and signed

### 4. Homebrew Formula
**Best for:** Developer community

**Setup:**
1. Create a GitHub repository for your Homebrew tap
2. Copy `Formula/tundradb.rb` to your tap repository
3. Update the URL and SHA256 in the formula

**Usage:**
```bash
# Users can install with:
brew tap yourusername/tundradb
brew install tundradb
```

## Customization

### Updating Version
Edit the version in the scripts:
```bash
# In scripts/create_macos_bundle.sh and scripts/create_macos_app.sh
VERSION="1.0.0"  # Change this
```

### Adding Custom Resources
```bash
# Add files to the bundle
mkdir -p "${BUNDLE_DIR}/share/examples"
cp examples/* "${BUNDLE_DIR}/share/examples/"
```

### Code Signing (Optional)
For distribution outside the App Store:
```bash
# Sign the executable
codesign --force --sign "Developer ID Application: Your Name" \
    "${APP_BUNDLE}/Contents/MacOS/tundra_shell"

# Sign the app bundle
codesign --force --sign "Developer ID Application: Your Name" \
    "${APP_BUNDLE}"
```

### Notarization (Optional)
For Gatekeeper compatibility:
```bash
# Create a zip for notarization
ditto -c -k --keepParent "${APP_BUNDLE}" "${APP_NAME}.zip"

# Submit for notarization
xcrun notarytool submit "${APP_NAME}.zip" \
    --keychain-profile "AC_PASSWORD" \
    --wait

# Staple the notarization
xcrun stapler staple "${APP_BUNDLE}"
```

## Testing Your Distribution

### Test the Portable Bundle
```bash
# Extract to a temporary location
cd /tmp
tar -xzf /path/to/TundraDB-1.0.0-macOS.tar.gz
cd TundraDB-1.0.0-macOS

# Test basic functionality
./tundra_shell --help
./tundra_shell --db-path ./test-db

# Check dependencies are bundled
otool -L bin/tundra_shell
```

### Test the App Bundle
```bash
# Test the app bundle
open dist/TundraDB.app

# Test from command line
dist/TundraDB.app/Contents/MacOS/tundra_shell --help

# Check bundle structure
ls -la dist/TundraDB.app/Contents/
```

## Troubleshooting

### Common Issues

1. **Missing Dependencies**
   ```bash
   # Check what libraries are missing
   otool -L build/tundra_shell
   
   # The script should bundle all non-system libraries
   ```

2. **Permission Denied**
   ```bash
   # Make sure scripts are executable
   chmod +x scripts/*.sh
   ```

3. **Library Path Issues**
   ```bash
   # Check library paths are correctly updated
   otool -L dist/TundraDB-1.0.0-macOS/bin/tundra_shell
   
   # Should show @loader_path/../lib/... for bundled libraries
   ```

4. **Gatekeeper Issues**
   ```bash
   # Remove quarantine attribute
   xattr -rd com.apple.quarantine TundraDB.app
   ```

### Debugging
```bash
# Enable verbose output in scripts
set -x  # Add this to the top of scripts

# Check library loading
DYLD_PRINT_LIBRARIES=1 ./tundra_shell --help
```

## Best Practices

1. **Always test on a clean system** without development dependencies
2. **Use Release builds** for distribution (`-DCMAKE_BUILD_TYPE=Release`)
3. **Strip debug symbols** to reduce size:
   ```bash
   strip "${BUNDLE_DIR}/bin/tundra_shell"
   ```
4. **Include comprehensive documentation** and examples
5. **Version your releases** consistently
6. **Test on multiple macOS versions** if possible

## Automation

### GitHub Actions
You can automate distribution building with GitHub Actions:

```yaml
# .github/workflows/release.yml
name: Build macOS Distribution
on:
  release:
    types: [created]
jobs:
  build-macos:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install dependencies
        run: |
          brew install antlr4-cpp-runtime apache-arrow boost tbb libcds
      - name: Build distribution
        run: ./scripts/create_macos_bundle.sh
      - name: Upload artifacts
        uses: actions/upload-artifact@v3
        with:
          name: tundradb-macos
          path: dist/
```

This comprehensive approach gives you multiple distribution options depending on your target audience and use case. 