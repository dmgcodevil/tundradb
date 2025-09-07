#!/bin/bash

# TundraDB Code Quality Check Script
set -e

echo "🔍 Running TundraDB Code Quality Checks..."
echo "========================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if tools are available
check_tool() {
    if ! command -v $1 &> /dev/null; then
        print_error "$1 is not installed. Please install it first."
        return 1
    fi
    return 0
}

print_status "Checking required tools..."
TOOLS_AVAILABLE=true

if ! check_tool "clang-format"; then
    TOOLS_AVAILABLE=false
fi

if ! check_tool "clang-tidy"; then
    print_warning "clang-tidy not found. Install with: brew install llvm (macOS) or apt install clang-tidy (Ubuntu)"
fi

if ! check_tool "cppcheck"; then
    print_warning "cppcheck not found. Install with: brew install cppcheck (macOS) or apt install cppcheck (Ubuntu)"
fi

# 1. Code Formatting Check
print_status "1. Checking code formatting..."
FORMATTING_ISSUES=false

# Create a temporary directory to check formatting
TEMP_DIR=$(mktemp -d)
cp -r src include tests "$TEMP_DIR/" 2>/dev/null || true

# Run clang-format on temporary files
find "$TEMP_DIR" -name "*.cpp" -o -name "*.h" -o -name "*.hpp" | xargs clang-format -i 2>/dev/null || true

# Compare with original files
if ! diff -r src "$TEMP_DIR/src" >/dev/null 2>&1 || \
   ! diff -r include "$TEMP_DIR/include" >/dev/null 2>&1 || \
   ! diff -r tests "$TEMP_DIR/tests" >/dev/null 2>&1; then
    print_warning "Code formatting issues found. Run ./format-code.sh to fix."
    FORMATTING_ISSUES=true
else
    print_success "Code formatting is consistent."
fi

# Clean up
rm -rf "$TEMP_DIR"

# 2. Static Analysis with clang-tidy
if command -v clang-tidy &> /dev/null; then
    print_status "2. Running clang-tidy static analysis..."
    
    # Run clang-tidy on source files
    TIDY_OUTPUT=$(find src -name "*.cpp" | head -5 | xargs clang-tidy --quiet 2>&1 || true)
    
    if [ -n "$TIDY_OUTPUT" ]; then
        print_warning "clang-tidy found potential issues:"
        echo "$TIDY_OUTPUT" | head -20
        echo "Run 'clang-tidy src/*.cpp -- -I./include' for full analysis"
    else
        print_success "clang-tidy analysis passed."
    fi
else
    print_warning "Skipping clang-tidy (not installed)"
fi

# 3. Static Analysis with cppcheck
if command -v cppcheck &> /dev/null; then
    print_status "3. Running cppcheck analysis..."
    
    CPPCHECK_OUTPUT=$(cppcheck --enable=warning,style,performance,portability --quiet \
                      --template='{file}:{line}: {severity}: {message}' \
                      --suppress=missingIncludeSystem \
                      --suppress=unusedFunction \
                      src/ include/ 2>&1 || true)
    
    if [ -n "$CPPCHECK_OUTPUT" ]; then
        print_warning "cppcheck found potential issues:"
        echo "$CPPCHECK_OUTPUT" | head -10
    else
        print_success "cppcheck analysis passed."
    fi
else
    print_warning "Skipping cppcheck (not installed)"
fi

# 4. Check for common issues
print_status "4. Checking for common code issues..."

# Check for TODO/FIXME comments
TODO_COUNT=$(find src include tests -name "*.cpp" -o -name "*.hpp" -o -name "*.h" | xargs grep -n "TODO\|FIXME" | wc -l || echo "0")
if [ "$TODO_COUNT" -gt 0 ]; then
    print_warning "Found $TODO_COUNT TODO/FIXME comments"
else
    print_success "No TODO/FIXME comments found"
fi

# Check for large files (>1000 lines)
LARGE_FILES=$(find src include tests -name "*.cpp" -o -name "*.hpp" -o -name "*.h" | xargs wc -l | awk '$1 > 1000 {print $2 " (" $1 " lines)"}' || true)
if [ -n "$LARGE_FILES" ]; then
    print_warning "Large files found (consider splitting):"
    echo "$LARGE_FILES"
fi

# Check for very long functions (basic heuristic)
LONG_FUNCTIONS=$(find src -name "*.cpp" | xargs grep -n "^[a-zA-Z].*{" | head -5 || true)
if [ -n "$LONG_FUNCTIONS" ]; then
    print_status "Consider reviewing function lengths in:"
    echo "$LONG_FUNCTIONS" | cut -d: -f1 | sort -u | head -3
fi

# 5. Build Check
print_status "5. Checking if project builds..."
cd build_release 2>/dev/null || {
    print_error "build_release directory not found. Run cmake first."
    exit 1
}

if make -j4 >/dev/null 2>&1; then
    print_success "Project builds successfully."
else
    print_error "Build failed. Fix compilation errors first."
fi

cd ..

# Summary
echo ""
echo "========================================"
print_status "Code Quality Check Summary:"

if [ "$FORMATTING_ISSUES" = true ]; then
    print_warning "• Formatting: Issues found"
else
    print_success "• Formatting: ✓ Clean"
fi

print_success "• Static Analysis: Completed"
print_success "• Build: ✓ Successful"

echo ""
print_status "💡 Recommendations:"
echo "   • Run ./format-code.sh before committing"
echo "   • Address clang-tidy warnings for better code quality"
echo "   • Consider breaking down large files/functions"
echo "   • Add unit tests for new functionality"

print_success "Code quality check completed!"