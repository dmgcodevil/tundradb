# TundraDB — macOS Release Process

## Overview

This document describes how to build and package TundraDB for macOS distribution.
The process produces a self-contained `.tar.gz` archive containing `tundra_shell`
and all its dynamic library dependencies — no Homebrew or other installs required
on the target machine.

### Output structure

```
TundraDB-1.0.0-macOS-arm64.tar.gz
└── TundraDB-1.0.0-macOS/
    ├── tundra_shell        ← launcher script (run this)
    ├── bin/
    │   └── tundra_shell    ← binary (library paths rewritten)
    ├── libs/
    │   └── *.dylib         ← all bundled dependencies
    └── VERSION
```

---

## Prerequisites

Install these on the build machine (the machine where you create the release):

```bash
# Build tools
brew install cmake

# Library bundler — copies dylibs and rewrites load paths
brew install dylibbundler
```

All TundraDB dependencies (Arrow, Parquet, TBB, ANTLR, LLVM, etc.) must already
be installed. If you can build TundraDB from source, you're good.

---

## Quick Release (one command)

```bash
./scripts/create_macos_bundle.sh
```

This will:

1. Configure a **Release** build in `build_release/`
2. Build `tundra_shell`
3. Copy the binary to `dist/TundraDB-<version>-macOS/bin/`
4. Run `dylibbundler` to collect all `.dylib` dependencies into `libs/`
   and rewrite the binary's load paths to `@executable_path/../libs/`
5. Create a launcher script
6. Verify all references are resolved
7. Create `dist/TundraDB-<version>-macOS-<arch>.tar.gz`

### Options

| Flag            | Effect                                      |
|-----------------|---------------------------------------------|
| `--skip-build`  | Skip cmake configure/build, reuse existing  |

### Examples

```bash
# Full build + package
./scripts/create_macos_bundle.sh

# Just re-package (binary already built in build_release/)
./scripts/create_macos_bundle.sh --skip-build
```

---

## Step-by-Step (manual)

If you prefer to run the steps individually:

### 1. Configure Release build

```bash
cmake -B build_release -S . \
    -DCMAKE_BUILD_TYPE=Release \
    -DTUNDRADB_BUILD_TESTS=OFF \
    -DTUNDRADB_BUILD_BENCHMARKS=OFF
```

### 2. Build

```bash
cmake --build build_release -j$(sysctl -n hw.ncpu)
```

### 3. Prepare bundle directory

```bash
mkdir -p dist/TundraDB-1.0.0-macOS/bin
mkdir -p dist/TundraDB-1.0.0-macOS/libs
cp build_release/tundra_shell dist/TundraDB-1.0.0-macOS/bin/
```

### 4. Bundle dynamic libraries

```bash
dylibbundler -od -b \
    -x dist/TundraDB-1.0.0-macOS/bin/tundra_shell \
    -d dist/TundraDB-1.0.0-macOS/libs/ \
    -p @executable_path/../libs/ \
    -s /usr/local/lib \
    -s /opt/homebrew/lib \
    -s /opt/homebrew/opt/llvm/lib
```

**What this does:**
- `-od` — overwrite existing files in the destination
- `-b` — bundle (copy) libraries
- `-x` — the executable to process
- `-d` — where to copy the `.dylib` files
- `-p` — prefix to rewrite into the binary (`@executable_path/../libs/`)
- `-s` — extra directories to search for `@rpath` libraries

If `dylibbundler` prompts for a library path, it means an `@rpath` reference
couldn't be found in the search paths. Add the missing directory with another
`-s /path/to/dir` flag.

### 5. Verify

```bash
# Should show only /usr/lib, /System, and @executable_path references
otool -L dist/TundraDB-1.0.0-macOS/bin/tundra_shell

# Smoke test
dist/TundraDB-1.0.0-macOS/bin/tundra_shell --help
```

### 6. Create archive

```bash
cd dist
tar -czf TundraDB-1.0.0-macOS-arm64.tar.gz TundraDB-1.0.0-macOS
```

---

## How It Works

### The problem

macOS binaries reference their dynamic libraries by path. A development build
points to absolute Homebrew paths like `/opt/homebrew/lib/libarrow.dylib`.
This won't work on a machine without Homebrew.

### The solution

`dylibbundler` does two things:

1. **Copies** every non-system `.dylib` (and their transitive dependencies)
   into the bundle's `libs/` directory.
2. **Rewrites** every library reference in the binary (using `install_name_tool`)
   from absolute paths to `@executable_path/../libs/<libname>`.

This makes the binary fully self-contained. At runtime, macOS resolves
`@executable_path` to the directory containing the binary (`bin/`), then
looks for libs in `../libs/` relative to that.

### Directory layout requirement

The binary expects libs at `../libs/` relative to itself, so the structure
**must** be:

```
<anything>/
├── bin/
│   └── tundra_shell          ← binary lives here
└── libs/
    └── *.dylib               ← @executable_path/../libs/ resolves here
```

---

## Folder Structure

| Directory        | Purpose                        | In `.gitignore` |
|------------------|--------------------------------|-----------------|
| `build_release/` | CMake build artifacts          | ✅ Yes          |
| `dist/`          | Final distributable packages   | ✅ Yes          |
| `build/`         | Development (Debug) build      | ✅ Yes          |

`build_release/` contains CMake cache, object files, and intermediate build
artifacts — these are **not** distributable. The clean, distributable output
goes into `dist/`.

---

## Troubleshooting

### `dylibbundler` prompts for a library path

```
/!\ WARNING : can't get path for '@rpath/libfoo.dylib'
Please specify the directory where this library is located:
```

Find the library and add its directory as a search path:

```bash
find /opt/homebrew /usr/local -name "libfoo*" -type f 2>/dev/null
# Then re-run with: -s /path/to/directory
```

### Binary crashes with "Library not loaded"

Check that the directory layout is correct — `libs/` must be a sibling of
`bin/`:

```bash
otool -L bin/tundra_shell | head -5
# Should show @executable_path/../libs/...
```

### Archive too large

The bundle includes ~100 dylibs (~20 MB compressed). To reduce size:

```bash
# Strip debug symbols from bundled libs
strip -x dist/TundraDB-1.0.0-macOS/libs/*.dylib
```

---

## Platform Notes

- The archive is **architecture-specific**: an `arm64` build won't run on
  Intel Macs (and vice versa). The script names the archive accordingly.
- To support both architectures, build on each platform separately or
  create a Universal Binary with `CMAKE_OSX_ARCHITECTURES="arm64;x86_64"`
  (requires all dependencies to also be universal).
- This archive is **macOS only**. For Linux, see the Linux release process.

