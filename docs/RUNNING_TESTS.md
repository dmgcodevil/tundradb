# Running Tests

This guide explains how to run TundraDB tests: all tests, a single test executable, or an individual test case.

## Prerequisites

- Project built with tests enabled (default: `TUNDRADB_BUILD_TESTS=ON`).
- From the project root, use a build directory such as `build/` or `build_tsan/`.

---

## 1. Run all tests (CTest)

From your **build directory**:

```bash
cd build   # or build_tsan, build_debug, etc.
ctest
```

Verbose output (print each test name as it runs):

```bash
ctest --output-on-failure -V
```

---

## 2. Run a single test executable

Each test file is built as its own binary. Run one by name from the **build directory**:

```bash
cd build
./tests/node_test
./tests/database_test
./tests/temporal_query_test
```

Or from the project root:

```bash
./build/tests/node_test
```

### Test executables (from `tests/CMakeLists.txt`)

| Binary                 | Source file              |
|------------------------|--------------------------|
| `node_test`            | `tests/node_test.cpp`    |
| `node_arena_test`      | `tests/node_arena_test.cpp` |
| `node_version_test`    | `tests/node_version_test.cpp` |
| `node_view_test`       | `tests/node_view_test.cpp` |
| `database_test`        | `tests/database_test.cpp` |
| `temporal_query_test`  | `tests/temporal_query_test.cpp` |
| `sharding_test`        | `tests/sharding_test.cpp` |
| `snapshot_test`        | `tests/snapshot_test.cpp` |
| `edge_store_test`      | `tests/edge_store_test.cpp` |
| `schema_utils_test`     | `tests/schema_utils_test.cpp` |
| `join_test`            | `tests/join_test.cpp`    |
| `where_expression_test`| `tests/where_expression_test.cpp` |
| `update_query_test`    | `tests/update_query_test.cpp` |
| `update_query_join_test`| `tests/update_query_join_test.cpp` |
| `memory_arena_test`    | `tests/memory_arena_test.cpp` |
| `free_list_arena_test`  | `tests/free_list_arena_test.cpp` |
| `string_ref_concurrent_test` | `tests/string_ref_concurrent_test.cpp` |
| `concurrency_test`     | `tests/concurrency_test.cpp` |
| `concurrent_set_stress_test` | `tests/concurrent_set_stress_test.cpp` |
| `table_info_test`      | `tests/table_info_test.cpp` |
| `array_query_test`     | `tests/array_query_test.cpp` |
| `arena_leak_test`      | `tests/arena_leak_test.cpp` |

---

## 3. Run an individual test case (Google Test filter)

Tests use **Google Test**. You can run a single test or a subset with `--gtest_filter`.

### Run one test by name

```bash
./tests/node_test --gtest_filter=NodeTest.NodeManagerCreateNode
./tests/node_test --gtest_filter=NodeTest.NodeGetValueArray
```

### Run all tests in a test suite

```bash
./tests/node_test --gtest_filter=NodeTest.*
```

### Run tests matching a pattern

Pattern is `TestSuiteName.TestName`; `*` is a wildcard:

```bash
# All tests whose name contains "Array"
./tests/node_test --gtest_filter=*Array*

# All tests in NodeTest that contain "GetValue"
./tests/node_test --gtest_filter=NodeTest.*GetValue*
```

### Exclude tests

Use a negative filter with `-`:

```bash
# Run all NodeTest tests except those with "DISABLED_" in the name
./tests/node_test --gtest_filter=NodeTest.-*DISABLED*
```

### Combine with CTest

To run a single test **executable** via CTest (by test name as registered in CMake):

```bash
ctest -R NodeTest
ctest -R DatabaseTest
```

This runs the whole `node_test` or `database_test` binary; it does **not** run a single test case inside that binary. To run one test case, use the binary + `--gtest_filter` as above.

---

## 4. Useful GTest options

| Option | Description |
|--------|-------------|
| `--gtest_filter=PATTERN` | Run only tests matching `PATTERN` (e.g. `SuiteName.TestName` or `*Substring*`) |
| `--gtest_list_tests`    | List all tests (no execution) |
| `--gtest_repeat=N`      | Run the selected tests N times |
| `--gtest_break_on_failure` | Break into the debugger on first failure |
| `--gtest_print_time=0`  | Don’t print elapsed time per test |

Examples:

```bash
# List all tests in node_test
./tests/node_test --gtest_list_tests

# Run one test 10 times (e.g. for flakiness)
./tests/node_test --gtest_filter=NodeTest.NodeGetValueArray --gtest_repeat=10
```

---

## 5. Build only one test target

To build just one test without building everything:

```bash
cd build
make node_test
# or
cmake --build . --target node_test
```

Then run it (and optionally with a filter) as in sections 2 and 3.

---

## 6. Debug build with all safety checks

The project includes multiple safety layers for catching memory bugs. To enable **all of them** in a single build:

```bash
cd build
cmake .. \
  -DCMAKE_BUILD_TYPE=Debug \
  -DENABLE_SANITIZERS=ON \
  -DSANITIZER_TYPE=address \
  -DCMAKE_EXE_LINKER_FLAGS="-L/opt/homebrew/opt/llvm/lib/c++ -Wl,-rpath,/opt/homebrew/opt/llvm/lib/c++"
cmake --build .
ctest --output-on-failure
```

> **Note:** The `CMAKE_EXE_LINKER_FLAGS` line is a workaround for Homebrew LLVM 21+ on macOS. If you're using Apple Clang or a different toolchain, you can omit it.

### What each flag enables

| Flag | Layer | What it catches |
|------|-------|-----------------|
| `CMAKE_BUILD_TYPE=Debug` | `assert()` in `StringRef::release()` / `ArrayRef::release()` | Double-release, release with ref_count already 0 |
| `ENABLE_SANITIZERS=ON` | Runtime sanitizer instrumentation | Depends on `SANITIZER_TYPE` (see below) |
| `SANITIZER_TYPE=address` | AddressSanitizer (ASan) | Use-after-free, buffer overflow, memory leaks |

### Always-on safety (any build type)

These are active regardless of build configuration:

- **`static_assert`** — `StringRef` and `ArrayRef` must not be trivially copyable. Prevents accidental `memcpy` that would skip ref-count updates. Checked at compile time.
- **`active_allocs` counters** — `StringPool` and `ArrayArena` track live allocations. The `ArenaLeakTest` suite verifies counts drop to zero after cleanup.

### Available sanitizer types

Only one sanitizer can be active at a time. Switch by changing `SANITIZER_TYPE`:

```bash
# AddressSanitizer — memory errors (recommended first pass)
cmake .. -DENABLE_SANITIZERS=ON -DSANITIZER_TYPE=address

# UndefinedBehaviorSanitizer — signed overflow, null deref, alignment
cmake .. -DENABLE_SANITIZERS=ON -DSANITIZER_TYPE=undefined

# ThreadSanitizer — data races (uses build_tsan/ by convention)
cmake .. -DENABLE_SANITIZERS=ON -DSANITIZER_TYPE=thread

# MemorySanitizer — uninitialized reads (Clang-only, not on macOS)
cmake .. -DENABLE_SANITIZERS=ON -DSANITIZER_TYPE=memory
```

### Switching back to a fast build

```bash
cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_SANITIZERS=OFF
cmake --build .
```

---

## Quick reference

| Goal | Command |
|------|--------|
| All tests | `ctest` (from build dir) |
| One executable | `./tests/node_test` |
| One test case | `./tests/node_test --gtest_filter=NodeTest.NodeGetValueArray` |
| List tests | `./tests/node_test --gtest_list_tests` |
| One test via CTest | `ctest -R NodeTest` (runs full `node_test` binary) |
| All checks + ASan | See [Section 6](#6-debug-build-with-all-safety-checks) |
| Fast build (no sanitizers) | `cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DENABLE_SANITIZERS=OFF` |
