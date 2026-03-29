#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Run all tests with llvm-cov coverage and produce a report.
#
# Usage:
#   ./scripts/run_tests_coverage.sh              # terminal summary
#   ./scripts/run_tests_coverage.sh --html       # also generate HTML report
#   ./scripts/run_tests_coverage.sh --file <path> # show coverage for one file
# ---------------------------------------------------------------------------

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build_cov"
LLVM_PREFIX="/opt/homebrew/opt/llvm"
PROFDATA="${LLVM_PREFIX}/bin/llvm-profdata"
LLVM_COV="${LLVM_PREFIX}/bin/llvm-cov"
CC="${LLVM_PREFIX}/bin/clang"
CXX="${LLVM_PREFIX}/bin/clang++"

HTML=false
SINGLE_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --html)  HTML=true; shift ;;
    --file)  SINGLE_FILE="$2"; shift 2 ;;
    *)       echo "Unknown option: $1"; exit 1 ;;
  esac
done

# --- 1. Configure (only if needed) ----------------------------------------
if [[ ! -f "${BUILD_DIR}/Makefile" ]]; then
  echo "==> Configuring coverage build in ${BUILD_DIR}"
  cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE=Debug \
    -DENABLE_COVERAGE=ON \
    -DCMAKE_C_COMPILER="${CC}" \
    -DCMAKE_CXX_COMPILER="${CXX}" \
    -DCMAKE_EXE_LINKER_FLAGS="-L${LLVM_PREFIX}/lib/c++ -Wl,-rpath,${LLVM_PREFIX}/lib/c++"
fi

# --- 2. Build -------------------------------------------------------------
echo "==> Building"
cmake --build "${BUILD_DIR}" -j"$(sysctl -n hw.logicalcpu)"

# --- 3. Run tests & collect profiles --------------------------------------
echo "==> Running tests"
cd "${BUILD_DIR}"
rm -f ./*.profraw

FAILED=0
for t in tests/*_test; do
  [[ -x "$t" ]] || continue
  bname="$(basename "$t")"
  if LLVM_PROFILE_FILE="${bname}.profraw" "./$t" --gtest_print_time=0 >/dev/null 2>&1; then
    printf "  %-40s PASS\n" "$bname"
  else
    printf "  %-40s FAIL\n" "$bname"
    FAILED=$((FAILED + 1))
  fi
done

if [[ $FAILED -gt 0 ]]; then
  echo "WARNING: $FAILED test(s) failed"
fi

# --- 4. Merge profiles ----------------------------------------------------
echo "==> Merging profiles"
"${PROFDATA}" merge -sparse ./*.profraw -o coverage.profdata

# --- 5. Build -object flags -----------------------------------------------
OBJS=()
for t in tests/*_test; do
  [[ -x "$t" ]] && OBJS+=("-object=$t")
done

IGNORE='-ignore-filename-regex=(tests/|build|googletest|/opt/homebrew)'

# --- 6. Report ------------------------------------------------------------
if [[ -n "${SINGLE_FILE}" ]]; then
  echo "==> Coverage for ${SINGLE_FILE}"
  "${LLVM_COV}" show \
    -instr-profile=coverage.profdata \
    "${OBJS[@]}" \
    -sources "${SINGLE_FILE}" \
    -format=text
else
  echo ""
  echo "==> Coverage summary"
  "${LLVM_COV}" report \
    -instr-profile=coverage.profdata \
    "${OBJS[@]}" \
    "${IGNORE}"

  if $HTML; then
    echo ""
    echo "==> Generating HTML report"
    "${LLVM_COV}" show \
      -instr-profile=coverage.profdata \
      "${OBJS[@]}" \
      -format=html \
      -output-dir=coverage_html \
      "${IGNORE}"
    echo "    Open: ${BUILD_DIR}/coverage_html/index.html"
  fi
fi
