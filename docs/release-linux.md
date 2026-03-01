# TundraDB — Linux Release Process

## Overview

This document describes how to build a self-contained Linux (x86_64)
distribution of TundraDB from **any** machine with Docker — including macOS.

The process uses Docker to spin up an Ubuntu 24.04 build environment, compile
`tundra_shell` in Release mode, bundle all shared library dependencies, and
produce a `.tar.gz` archive that runs on any modern Linux x86_64 system without
needing to install anything.

### Output structure

```
TundraDB-1.0.0-Linux-x86_64.tar.gz
└── TundraDB-1.0.0-Linux-x86_64/
    ├── tundra_shell        ← launcher script (run this)
    ├── bin/
    │   └── tundra_shell    ← ELF binary (RPATH set to $ORIGIN/../libs)
    ├── libs/
    │   └── *.so*           ← all bundled shared libraries
    └── VERSION
```

---

## Prerequisites

On the **build machine** (macOS or Linux):

```bash
# Docker Desktop (macOS) or Docker Engine (Linux)
# https://docs.docker.com/get-docker/
docker --version   # verify it's installed and running
```

That's it — all C++ dependencies are installed **inside** the Docker image.

---

## Quick Release (one command)

```bash
./scripts/create_linux_bundle.sh
```

This will:

1. Build a Docker image (`tundradb-linux-release`) with all build dependencies
2. Compile `tundra_shell` in Release mode inside the container
3. Use `ldd` to collect all non-system `.so` dependencies
4. Use `patchelf` to set `RPATH` to `$ORIGIN/../libs`
5. Strip debug symbols for smaller size
6. Create the tarball inside the image
7. Extract it to `dist/TundraDB-1.0.0-Linux-x86_64.tar.gz`

### Options

| Flag            | Effect                                                  |
|-----------------|---------------------------------------------------------|
| `--no-cache`    | Force full Docker rebuild (ignore layer cache)          |
| `--skip-build`  | Skip Docker build, reuse existing image                 |

### Timing

| Scenario                          | Approximate time   |
|-----------------------------------|--------------------|
| First build (no Docker cache)     | 10–20 minutes      |
| Rebuild (only source changed)     | 1–3 minutes        |
| `--skip-build` (extract only)     | ~5 seconds         |

> Docker caches each layer. Only the `COPY . .` and subsequent layers are
> rebuilt when source code changes. Dependency installation is cached.

---

## How it works

### Docker image layers

```
┌─────────────────────────────────┐
│  Ubuntu 24.04 + GCC 13          │  ← cached after first build
│  CMake 3.30                     │
│  Arrow, Parquet, TBB, CDS       │
│  spdlog, fmt, ANTLR4, LLVM      │
│  patchelf                        │
├─────────────────────────────────┤
│  COPY . . (source code)         │  ← rebuilt when source changes
│  cmake -B build_release ...     │
│  cmake --build build_release    │
├─────────────────────────────────┤
│  Bundle phase:                   │
│    ldd → collect .so files       │
│    patchelf → set RPATH          │
│    strip → remove debug symbols  │
│    tar → create archive          │
└─────────────────────────────────┘
```

### Library bundling

On Linux, shared libraries are located via `RPATH` (embedded in the ELF binary)
or `LD_LIBRARY_PATH` (environment variable). We use `patchelf` to set:

- **Binary**: `RPATH = $ORIGIN/../libs`
- **Bundled libs**: `RPATH = $ORIGIN`

This makes the binary fully self-contained — it finds its libraries relative to
its own location, with no need to install them system-wide.

### What's bundled vs. not bundled

| Bundled (copied to libs/)          | NOT bundled (expected on system) |
|------------------------------------|----------------------------------|
| libarrow, libparquet               | libc, libm (glibc)              |
| libtbb                             | libpthread, libdl, librt        |
| libspdlog, libfmt                  | ld-linux-x86-64.so (loader)     |
| libantlr4-runtime                  | linux-vdso.so (kernel)          |
| libcds                             |                                  |
| libLLVM*, libstdc++                |                                  |
| all transitive .so dependencies    |                                  |

> The only system requirement is a glibc-based Linux (Ubuntu 20.04+, Debian 11+,
> RHEL 8+, Fedora 33+, etc.). Musl-based distros (Alpine) are not supported.

---

## Step-by-step (manual)

If you prefer running Docker commands directly:

### 1. Build the image

```bash
cd /path/to/tundradb

docker build --platform linux/amd64 \
    -f docker/Dockerfile.linux-release \
    -t tundradb-linux-release \
    --build-arg TUNDRADB_VERSION=1.0.0 \
    .
```

### 2. Extract the tarball

```bash
mkdir -p dist

docker create --platform linux/amd64 \
    --name tundradb-extract \
    tundradb-linux-release /bin/true

docker cp tundradb-extract:/dist/TundraDB-1.0.0-Linux-x86_64.tar.gz dist/
docker rm tundradb-extract
```

### 3. (Optional) Inspect the bundle

```bash
# List contents
tar tzf dist/TundraDB-1.0.0-Linux-x86_64.tar.gz

# Inspect RPATH
docker run --rm --platform linux/amd64 tundradb-linux-release \
    patchelf --print-rpath /dist/TundraDB-1.0.0-Linux-x86_64/bin/tundra_shell

# List linked libraries
docker run --rm --platform linux/amd64 tundradb-linux-release \
    ldd /dist/TundraDB-1.0.0-Linux-x86_64/bin/tundra_shell
```

---

## Using the package (for end-users)

On the target Linux machine:

```bash
# Download / copy the archive
tar xzf TundraDB-1.0.0-Linux-x86_64.tar.gz
cd TundraDB-1.0.0-Linux-x86_64

# Run TundraDB shell
./tundra_shell --db-path ./my-database
```

No installation, no root, no package manager needed.

---

## Troubleshooting

### Docker build fails at Arrow installation

The Arrow apt repository URL may change. Check the
[Apache Arrow docs](https://arrow.apache.org/install/) for the latest
Ubuntu/Debian installation instructions.

### `qemu: uncaught target signal 11` during build

This can happen when building `linux/amd64` on Apple Silicon via QEMU emulation.
Solutions:

1. Increase Docker Desktop memory (Settings → Resources → 8 GB+)
2. Use `--no-cache` to avoid stale layers
3. If persistent, use a remote Linux builder (`docker buildx create --driver remote`)

### Binary reports "GLIBC not found" on target

The bundle is built against Ubuntu 24.04 (glibc 2.39). If the target system has
an older glibc, you'll see errors like:

```
/lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.38' not found
```

**Fix**: Build with an older base image. Change `ubuntu:24.04` to `ubuntu:22.04`
in `docker/Dockerfile.linux-release` (and adjust package names if needed).

### Archive is too large

```bash
# Check what's taking space
tar tzf dist/TundraDB-1.0.0-Linux-x86_64.tar.gz | head -30

# The script already strips symbols, but you can also compress harder:
cd dist
tar xzf TundraDB-1.0.0-Linux-x86_64.tar.gz
tar -cJf TundraDB-1.0.0-Linux-x86_64.tar.xz TundraDB-1.0.0-Linux-x86_64
# .tar.xz is ~30% smaller than .tar.gz
```

---

## Platform Notes

- This produces an **x86_64** binary. It will run on Intel/AMD 64-bit Linux.
  ARM64 Linux support would require building with `--platform linux/arm64`.
- For macOS distribution, see `docs/release-macos.md`.
- The Docker image is ~3–4 GB. The final tarball is typically 20–50 MB.

