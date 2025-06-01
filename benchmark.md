# TundraDB Benchmarking & Profiling Guide 🚀

This guide covers performance benchmarking and flamegraph profiling for TundraDB.

## Overview

TundraDB includes comprehensive benchmark tests for measuring performance across different graph operations:
- **Node Creation**: Bulk node insertion performance
- **Simple Joins**: User→Company relationship queries  
- **Complex Joins**: 3-way User→Friend→Company queries
- **Full Scans**: Table scan performance
- **Filtered Queries**: WHERE clause performance with indexes

## Prerequisites

- macOS with Xcode Command Line Tools
- CMake and Make
- Python 3
- FlameGraph tools (automatically downloaded)

## Quick Start

### 1. Build Benchmarks
```bash
cd /path/to/tundradb
mkdir -p build && cd build
cmake ..
make benchmark_test
```

### 2. Run Benchmarks
```bash
# Run all benchmarks
./tests/benchmark_test

# Run specific benchmark
./tests/benchmark_test --filter=BM_SimpleJoin

# Run with custom timing
./tests/benchmark_test --filter=BM_SimpleJoin --benchmark_min_time=10s
```

## Performance Profiling with Flamegraphs 🔥

### One-Time Setup
```bash
# Download FlameGraph tools (only needed once)
cd /tmp && git clone https://github.com/brendangregg/FlameGraph.git
```

### Complete Profiling Workflow

#### Step 1: Profile Your Benchmark
```bash
cd build

# Profile a specific benchmark (5 second sample)
./tests/benchmark_test --filter=BM_SimpleJoin --benchmark_repetitions=1 --benchmark_min_time=5s & 
PID=$!; sleep 1; sample $PID 5 -f profile_sample.txt; wait
```

#### Step 2: Generate Flamegraph
```bash
cd ..
python3 parse_sample.py build/profile_sample.txt | /tmp/FlameGraph/flamegraph.pl > build/flamegraph.svg
```

#### Step 3: View Results
```bash
open build/flamegraph.svg
```

## Available Benchmark Filters

| Filter | Description | What It Measures |
|--------|-------------|------------------|
| `BM_NodeCreation` | Node insertion performance | Node creation throughput |
| `BM_FullScan` | Table scan performance | Sequential scan speed |
| `BM_SimpleJoin` | User→Company joins | Basic relationship queries |
| `BM_ComplexJoin` | 3-way joins | Complex graph traversals |
| `BM_FilteredQuery` | WHERE clause queries | Index performance |

## Dataset Sizes

Benchmarks run on three dataset sizes:

- **Small**: 100 users, 20 companies, 50 products
- **Medium**: 5,000 users, 500 companies, 1,000 products  
- **Large**: 50,000 users, 5,000 companies, 10,000 products

## Performance Metrics (Typical Results)

| Operation | Throughput | Notes |
|-----------|------------|-------|
| Node Creation | ~90K nodes/sec | Bulk insertion |
| Full Scan | ~25K nodes/sec | Sequential access |
| Simple Join | ~15K queries/sec | 2-table joins |
| Complex Join | ~7K queries/sec | 3-way joins |
| Filtered Query | ~50K queries/sec | Indexed lookups |

## Profiling Different Operations

### Profile Node Creation
```bash
./tests/benchmark_test --filter=BM_NodeCreation --benchmark_min_time=5s & 
PID=$!; sample $PID 5 -f node_creation_profile.txt; wait
python3 parse_sample.py build/node_creation_profile.txt | /tmp/FlameGraph/flamegraph.pl > build/node_creation_flamegraph.svg
open build/node_creation_flamegraph.svg
```

### Profile Complex Joins
```bash
./tests/benchmark_test --filter=BM_ComplexJoin --benchmark_min_time=10s & 
PID=$!; sample $PID 8 -f complex_join_profile.txt; wait
python3 parse_sample.py build/complex_join_profile.txt | /tmp/FlameGraph/flamegraph.pl > build/complex_join_flamegraph.svg
open build/complex_join_flamegraph.svg
```

## Understanding Flamegraphs

### Reading the Visualization
- **Width = Time**: Wider blocks = more CPU time spent
- **Height = Call Stack**: Bottom = entry points, Top = leaf functions
- **Click to Zoom**: Focus on specific function call paths
- **Hover for Details**: See exact function names and sample counts

### Key Areas to Analyze
1. **Wide blocks at the bottom**: Core database operations
2. **Tall stacks**: Deep function call chains (potential optimization targets)
3. **Fragmented areas**: Many small functions (potential consolidation opportunities)
4. **Color coding**: Different colors help distinguish call paths

## Troubleshooting

### "No such file or directory" Error
```bash
# Make sure you're in the build directory
cd build
ls -la tests/benchmark_test  # Should exist and be executable
```

### "No stack counts found" Error
```bash
# Check if profile data was captured
ls -la profile_sample.txt
head -20 profile_sample.txt  # Should contain call graph data
```

### Empty Flamegraph
```bash
# Increase profiling duration
sample $PID 10 -f profile_sample.txt  # 10 seconds instead of 5
```

## Files Generated

| File | Description |
|------|-------------|
| `profile_sample.txt` | Raw macOS sample profiler output |
| `flamegraph.svg` | Interactive flamegraph visualization |
| `parse_sample.py` | Custom macOS sample format parser |

## Automation Script

Create `profile.sh` for easy profiling:

```bash
#!/bin/bash
BENCHMARK=${1:-BM_SimpleJoin}
DURATION=${2:-5}

echo "Profiling $BENCHMARK for ${DURATION}s..."
cd build
.tests/benchmark_test --filter=$BENCHMARK --benchmark_min_time=${DURATION}s & 
PID=$!; sleep 1; sample $PID $DURATION -f profile_${BENCHMARK}.txt; wait

echo "Generating flamegraph..."
cd ..
python3 parse_sample.py build/profile_${BENCHMARK}.txt | /tmp/FlameGraph/flamegraph.pl > build/flamegraph_${BENCHMARK}.svg

echo "Opening flamegraph..."
open build/flamegraph_${BENCHMARK}.svg

echo "Profiling complete! Flamegraph: build/flamegraph_${BENCHMARK}.svg"
```

Usage:
```bash
chmod +x profile.sh
./profile.sh BM_ComplexJoin 10  # Profile complex joins for 10 seconds
```

## Performance Optimization Tips

1. **Identify Hotspots**: Look for wide blocks in flamegraphs
2. **Reduce Call Depth**: Tall stacks indicate potential inlining opportunities
3. **Memory Access Patterns**: Sequential access shows up as smooth blocks
4. **Lock Contention**: Scattered patterns may indicate synchronization issues
5. **I/O Operations**: Look for system call patterns in the graph

## Google Benchmark Integration

The benchmark tests use Google Benchmark framework with these key features:

- **Automatic timing**: Runs until statistically significant
- **Multiple iterations**: Averages results across runs  
- **Custom counters**: Reports operations per second
- **Memory usage**: Tracks allocations (when enabled)
- **JSON output**: Machine-readable results for CI/CD

For detailed Google Benchmark options:
```bash
./tests/benchmark_test --help
``` 