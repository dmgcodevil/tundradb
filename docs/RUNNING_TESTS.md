# Running Tests

## Prerequisites

- CMake >= 3.30
- Homebrew LLVM (for coverage): `brew install llvm`
- Google Test (installed via Homebrew or vcpkg)

## Standard Build & Run

```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build . -j$(nproc)
ctest --output-on-failure
```

To run a single test:

```bash
./tests/node_test
./tests/node_test --gtest_filter='NodeMapFieldTest.*'
```

## Build with Coverage

Coverage requires Homebrew LLVM (not Xcode's Clang) so that the compiler
and `llvm-profdata`/`llvm-cov` versions match.

### 1. Configure

```bash
mkdir -p build_cov && cd build_cov
cmake .. \
  -DCMAKE_BUILD_TYPE=Debug \
  -DENABLE_COVERAGE=ON \
  -DCMAKE_C_COMPILER=/opt/homebrew/opt/llvm/bin/clang \
  -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm/bin/clang++ \
  -DCMAKE_EXE_LINKER_FLAGS="-L/opt/homebrew/opt/llvm/lib/c++ -Wl,-rpath,/opt/homebrew/opt/llvm/lib/c++"
```

### 2. Build

```bash
cmake --build . -j$(nproc)
```

### 3. Run Tests & Collect Profiles

Each test binary writes a `.profraw` file. Run them with
`LLVM_PROFILE_FILE` to control output location:

```bash
cd build_cov

# Run all tests, each producing its own .profraw
for t in tests/*_test; do
  [ -x "$t" ] && LLVM_PROFILE_FILE="$(basename $t).profraw" "$t" --gtest_print_time=0 || true
done
```

Or run a single test:

```bash
LLVM_PROFILE_FILE=node_test.profraw ./tests/node_test
```

### 4. Merge Profiles

```bash
/opt/homebrew/opt/llvm/bin/llvm-profdata merge \
  -sparse *.profraw -o coverage.profdata
```

### 5. Generate Report

**Terminal summary (per-file line coverage):**

```bash
# Build the -object flags for all test binaries
OBJS=""
for t in tests/*_test; do
  [ -x "$t" ] && OBJS="$OBJS -object=$t"
done

/opt/homebrew/opt/llvm/bin/llvm-cov report \
  -instr-profile=coverage.profdata \
  $OBJS \
  -ignore-filename-regex='(tests/|build|googletest|/opt/homebrew)'
```

**Coverage for a specific file:**

```bash
/opt/homebrew/opt/llvm/bin/llvm-cov show \
  -instr-profile=coverage.profdata \
  $OBJS \
  -sources ../include/node_arena.hpp \
  -format=text
```

**HTML report:**

```bash
/opt/homebrew/opt/llvm/bin/llvm-cov show \
  -instr-profile=coverage.profdata \
  $OBJS \
  -format=html \
  -output-dir=coverage_html \
  -ignore-filename-regex='(tests/|build|googletest|/opt/homebrew)'

open coverage_html/index.html
```

## Build with Sanitizers

Sanitizers instrument the binary at compile time to detect memory errors,
undefined behavior, and data races. They are controlled by two CMake options:

| Option | Values | Default |
|---|---|---|
| `ENABLE_SANITIZERS` | `ON` / `OFF` | `OFF` |
| `SANITIZER_TYPE` | `address`, `undefined`, `thread`, `memory` | `address` |

### AddressSanitizer (ASan) — recommended default

ASan detects use-after-free, buffer overflows (heap, stack, global), and
memory leaks. It catches bugs that are invisible on macOS without
instrumentation (see `docs/INCIDENT_MAP_ENTRY_BUFFER_OVERFLOW.md`).

```bash
cmake -B build_asan -DCMAKE_BUILD_TYPE=Debug \
  -DENABLE_SANITIZERS=ON -DSANITIZER_TYPE=address
cmake --build build_asan -j$(sysctl -n hw.ncpu)
ctest --test-dir build_asan --output-on-failure
```

Run a single test under ASan:

```bash
./build_asan/tests/map_arena_test
./build_asan/tests/snapshot_test --gtest_filter='*PreservesMapValues*'
```

ASan slows execution by ~2x and increases memory usage ~2x. This is
acceptable for test runs but not for benchmarks or profiling.

### UndefinedBehaviorSanitizer (UBSan)

Detects signed integer overflow, null pointer dereference, misaligned
access, and other undefined behavior.

```bash
cmake -B build_ubsan -DCMAKE_BUILD_TYPE=Debug \
  -DENABLE_SANITIZERS=ON -DSANITIZER_TYPE=undefined
cmake --build build_ubsan -j$(sysctl -n hw.ncpu)
ctest --test-dir build_ubsan --output-on-failure
```

### ThreadSanitizer (TSan)

Detects data races. Cannot be combined with ASan in the same build.

```bash
cmake -B build_tsan -DCMAKE_BUILD_TYPE=Debug \
  -DENABLE_SANITIZERS=ON -DSANITIZER_TYPE=thread
cmake --build build_tsan -j$(sysctl -n hw.ncpu)
ctest --test-dir build_tsan --output-on-failure
```

TSan suppression rules are in `tests/tsan_suppressions.txt`.

### Switching between sanitizer and normal builds

Each sanitizer should use its own build directory (`build_asan`,
`build_ubsan`, `build_tsan`) to avoid mixing instrumented and
non-instrumented object files. Keep a separate `build` directory for
normal development:

```
build/            # normal Debug or Release, no sanitizers
build_asan/       # Debug + ASan
build_ubsan/      # Debug + UBSan
build_tsan/       # Debug + TSan
build_cov/        # Debug + coverage
```

### Reading ASan output

When ASan detects an error it prints a report like:

```
==PID==ERROR: AddressSanitizer: stack-buffer-overflow on address 0x...
READ of size 16 at 0x...
    #0 SomeFunction (file.cpp:123)
    #1 CallerFunction (file.cpp:456)
```

Key things to look for:
- **Error type**: `stack-buffer-overflow`, `heap-use-after-free`,
  `heap-buffer-overflow`, `stack-use-after-return`, etc.
- **READ vs WRITE**: tells you whether the bug is an over-read (reading
  past the object) or an over-write (corrupting adjacent memory).
- **Stack trace**: the `#0` frame is where the bad access happens;
  frames below it show who called it.
- **Variable annotation**: ASan marks which variable was overflowed,
  e.g., `[48, 52) 'i32' <== Memory access at offset 48 partially
  overflows this variable`.

### CI sanitizer matrix

GitHub Actions runs three configurations on every push/PR to `main`:

| Name | Build type | Sanitizer |
|---|---|---|
| Release | Release | none |
| Debug + ASan | Debug | address |
| Debug + UBSan | Debug | undefined |

See `.github/workflows/cmake-single-platform.yml`.

## Quick Reference

| What | Command |
|---|---|
| Build (normal) | `cmake -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build -j$(sysctl -n hw.ncpu)` |
| Build (ASan) | `cmake -B build_asan -DCMAKE_BUILD_TYPE=Debug -DENABLE_SANITIZERS=ON && cmake --build build_asan -j$(sysctl -n hw.ncpu)` |
| Run all tests | `ctest --test-dir build --output-on-failure` |
| Run one test | `./build/tests/<name>_test` |
| Filter tests | `./build/tests/<name>_test --gtest_filter='Suite.Case'` |
| List tests | `./build/tests/<name>_test --gtest_list_tests` |
| Coverage report | See coverage steps above |
