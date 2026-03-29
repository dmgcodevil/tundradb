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

```bash
mkdir -p build_asan && cd build_asan
cmake .. \
  -DCMAKE_BUILD_TYPE=Debug \
  -DENABLE_SANITIZERS=ON \
  -DSANITIZER_TYPE=address
cmake --build . -j$(nproc)
ctest --output-on-failure
```

Supported sanitizer types: `address`, `thread`, `undefined`, `memory`.

## Quick Reference

| What | Command |
|---|---|
| Build all tests | `cmake --build . -j$(nproc)` |
| Run all tests | `ctest --output-on-failure` |
| Run one test | `./tests/<name>_test` |
| Filter tests | `./tests/<name>_test --gtest_filter='Suite.Case'` |
| List tests | `./tests/<name>_test --gtest_list_tests` |
| Coverage report | See steps 1-5 above |
