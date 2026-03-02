# TundraDB

<p align="center">
  <b>A high-performance graph database with bitemporal versioning, per-schema IDs, and Apache Arrow storage.</b>
</p>

TundraDB is an embeddable graph database written in C++23. It features a custom query language (TundraQL) with Cypher-inspired pattern matching, SQL-style joins (INNER/LEFT/RIGHT/FULL), bitemporal time-travel queries, arena-based memory management, and columnar persistence via Apache Parquet.

For the full TundraQL language reference, see [`docs/tundraql.html`](docs/tundraql.html). For architecture details, see [`docs/architecture.html`](docs/architecture.html).

## Quick Start

```sql
CREATE SCHEMA User (name: STRING, age: INT64);
CREATE SCHEMA Company (name: STRING, size: INT64);

CREATE NODE User (name = "Alice", age = 30) RETURN id;    -- → id: 0
CREATE NODE User (name = "Bob",   age = 25) RETURN id;    -- → id: 1
CREATE NODE Company (name = "Google", size = 3000);        -- → id: 0

-- IDs are per-schema: User has 0,1 — Company has 0
CREATE EDGE friend   FROM User(0) TO User(1);
CREATE EDGE works_at FROM User(1) TO Company(0);

MATCH (u:User)-[:friend INNER]->(f:User)-[:works_at INNER]->(c:Company)
    WHERE u.name = "Alice"
    SELECT u.name, f.name, c.name;

COMMIT;
```

## Installation

### Build from source (macOS)

**Prerequisites:**

```bash
brew install cmake apache-arrow spdlog tbb libcds antlr4-cpp-runtime openjdk@21
```

**Build:**

```bash
git clone https://github.com/your-repo/tundradb.git
cd tundradb
cmake -B build_release -DCMAKE_BUILD_TYPE=Release \
      -DTUNDRADB_BUILD_TESTS=OFF -DTUNDRADB_BUILD_BENCHMARKS=OFF
cmake --build build_release -j$(sysctl -n hw.ncpu)

# Run
./build_release/tundra_shell --db-path ./mydb
```

### Build from source (Linux)

See the [Dockerfile](Dockerfile) for the full list of Ubuntu 24.04 packages. The key dependencies are:

```bash
apt install -y gcc-13 g++-13 cmake uuid-dev libtbb-dev libcds-dev openjdk-11-jdk llvm
# + Arrow/Parquet from Apache APT repo
# + spdlog, ANTLR4 runtime, fmt from source
```

### Pre-built packages

Self-contained distributable archives (no dependencies required on the target machine):

```bash
# macOS (requires dylibbundler on build machine)
./scripts/create_macos_bundle.sh
# → dist/TundraDB-1.0.0-macOS-arm64.tar.gz

# Linux x86_64 via Docker (from macOS or Linux)
./scripts/create_linux_bundle.sh
# → dist/TundraDB-1.0.0-Linux-x86_64.tar.gz
```

See [`docs/release-macos.md`](docs/release-macos.md) and [`docs/release-linux.md`](docs/release-linux.md) for details.

## Running in Docker

```bash
# Build the image (first run takes ~15 min, cached afterwards)
docker build --platform linux/amd64 -t tundradb .

# Interactive shell with persistent data
docker run -it --rm --platform linux/amd64 \
    -v ~/tundradb-data:/data \
    tundradb /bin/bash

# Inside the container:
./build/tundra_shell --db-path /data/mydb
```

## CLI Options

```bash
tundra_shell [options]

Options:
  --db-path <path>       Database directory (default: ./test-db)
  --script <file>        Execute a TundraQL script file
  --detach               Exit after script execution (batch mode)
  --unique-db            Use a timestamped database directory
  --output <file>        Write output to file
  --help                 Show help
```

**Examples:**

```bash
# Interactive shell
./tundra_shell --db-path ./mydb

# Batch execution
./tundra_shell --script setup.sql --db-path ./mydb --detach

# Script with output capture
./tundra_shell --script queries.sql --output results.txt --unique-db
```

## TundraQL at a Glance

| Statement | Example |
|-----------|---------|
| **CREATE SCHEMA** | `CREATE SCHEMA User (name: STRING, age: INT64);` |
| **CREATE NODE** | `CREATE NODE User (name = "Alice", age = 30) RETURN id;` |
| **CREATE EDGE** | `CREATE EDGE friend FROM User(0) TO User(1);` |
| **CREATE EDGE** (by property) | `CREATE UNIQUE EDGE works_at FROM (User{name="Alice"}) TO (Company{name="Google"});` |
| **MATCH** | `MATCH (u:User)-[:friend INNER]->(f:User) WHERE u.name = "Alice" SELECT u.name, f.name;` |
| **JOIN types** | `INNER`, `LEFT`, `RIGHT`, `FULL` |
| **DELETE** | `DELETE (u:User) WHERE u.age < 18;` |
| **DELETE EDGE** | `DELETE EDGE friend FROM User(0) TO User(1);` |
| **SHOW** | `SHOW EDGE TYPES;` / `SHOW EDGES friend;` |
| **COMMIT** | `COMMIT;` |

**Key concepts:**
- **Per-schema IDs** — each schema has its own auto-incrementing counter starting from 0
- **Bitemporal versioning** — `VALIDTIME` and `TXNTIME` for time-travel queries
- **Data types** — `STRING`, `INT64`, `DOUBLE`, `BOOL`

For the complete language reference, see [`docs/tundraql.html`](docs/tundraql.html).

## Key Features

- **Graph model** with typed nodes, directed edges, and Cypher-style pattern matching
- **SQL-style joins** (INNER, LEFT, RIGHT, FULL) on graph traversals
- **Bitemporal versioning** with VALIDTIME/TXNTIME for point-in-time queries
- **Apache Arrow** columnar in-memory format with Parquet persistence
- **Arena-based memory** — NodeArena, StringArena (tiered: inline/SSO/arena/heap), FreeListArena
- **Per-schema sharding** with configurable chunk sizes
- **ANTLR4-based parser** for TundraQL
- **Embeddable** — use as a C++ library via `find_package(TundraDB)`

## Development

```bash
# Debug build (with all targets)
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j$(sysctl -n hw.ncpu)

# Run tests
cd build && ctest --output-on-failure

# Run benchmarks
./build/tundra_bench_runner
```

## Project Structure

```
tundradb/
├── include/          C++ headers (public API)
├── src/              Implementation
├── antlr/            TundraQL grammar (TundraQL.g4)
├── tests/            Google Test suites
├── bench/            Google Benchmark suites
├── docs/             Documentation site + release guides
├── scripts/          Build/packaging scripts
├── docker/           Dockerfiles for Linux builds
└── libs/             Vendored dependencies (linenoise, json.hpp)
```

## License

[MIT](LICENSE)
