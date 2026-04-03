#!/bin/bash

# Profile with macOS Instruments
# Usage: ./profile_with_instruments.sh <executable_name>

if [ $# -eq 0 ]; then
    echo "Usage: $0 <executable_name>"
    echo "Example: $0 simple_perf_test"
    exit 1
fi

EXECUTABLE=$1
BUILD_DIR="../build_release"

echo "🔥 Profiling $EXECUTABLE with Instruments..."

# Check if executable exists
if [ ! -f "$BUILD_DIR/tests/$EXECUTABLE" ]; then
    echo "❌ Executable not found: $BUILD_DIR/tests/$EXECUTABLE"
    echo "Available executables:"
    ls -la "$BUILD_DIR/tests/" | grep -E "(simple_perf|micro_benchmark|hypothesis)_test"
    exit 1
fi

# Create output directory
mkdir -p ../profiling_results

# Profile with Time Profiler
echo "📊 Running Time Profiler..."
xcrun xctrace record --template 'Time Profiler' \
    --output "../profiling_results/${EXECUTABLE}_time_profile.trace" \
    --launch "$BUILD_DIR/tests/$EXECUTABLE"

echo "✅ Profiling complete!"
echo "📁 Results saved to: ../profiling_results/${EXECUTABLE}_time_profile.trace"
echo ""
echo "To view results:"
echo "  open ../profiling_results/${EXECUTABLE}_time_profile.trace"
echo ""
echo "Or use command line analysis:"
echo "  xcrun xctrace export --input ../profiling_results/${EXECUTABLE}_time_profile.trace --xpath '/trace-toc/run[@number=\"1\"]/data/table[@schema=\"time-profile\"]'"