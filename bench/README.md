# TundraDB Benchmark Suite

Benchmark TundraDB against Kuzu and Neo4j on graph traversal queries.

## 🚀 Quick Start

### 1. Generate Dataset (Once)

```bash
cd bench
python3 generate_dataset.py --users 1000000 --companies 10000 --avg-degree 5
```

**This creates deterministic CSV files in `data/` directory:**
- `users.csv` - User nodes (id, name, age, country)
- `companies.csv` - Company nodes (id, name, industry)
- `friend.csv` - FRIEND edges (src, dst)
- `works_at.csv` - WORKS_AT edges (src, dst)
- `friend_kuzu.csv` - FRIEND edges in Kuzu format (FROM, TO)
- `works_at_kuzu.csv` - WORKS_AT edges in Kuzu format (FROM, TO)

### 2. Build TundraDB Benchmark Runner

```bash
cd ..
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make tundra_bench_runner -j$(sysctl -n hw.ncpu)
```

### 3. Run Benchmark

```bash
cd ../bench
python3 run_bench.py --repetitions 5
```

**Output:**
```json
{
  "kuzu": {
    "Q2_friend_join": {
      "median_ms": 85.3,
      "p90_ms": 92.1,
      "p99_ms": 98.7
    }
  },
  "tundradb": {
    "Q2_friend_join": {
      "median_ms": 142.0,
      "p90_ms": 155.3,
      "p99_ms": 162.8
    }
  }
}
```

---

## 📊 Benchmark Query

**Q2: Friend Join with Filters**

```cypher
# Kuzu/Neo4j:
MATCH (u:User) WHERE u.age > 30 AND u.country = 'US'
MATCH (u)-[:FRIEND]->(f:User) WHERE f.age > 25
RETURN count(*)

# TundraDB:
MATCH (u:User) WHERE u.age > 30 AND u.country = 'US'
TRAVERSE (u)-[:FRIEND]->(f:User) WHERE f.age > 25
```

**What it tests:**
- Node filtering (age, country)
- Graph traversal (FRIEND edges)
- Join performance
- Secondary filtering on neighbors

---

## ⚙️ Configuration

### Dataset Generation Options:

```bash
python3 generate_dataset.py \
    --users 1000000 \          # Number of users
    --companies 10000 \         # Number of companies
    --avg-degree 5 \            # Average friends per user
    --output-dir data \         # Output directory
    --seed 42                   # Random seed (for reproducibility)
```

### Benchmark Options:

```bash
python3 run_bench.py \
    --data-dir data \                    # Where CSV files are
    --repetitions 5 \                    # Run each query N times
    --output results.json \              # Output file
    --kuzu-db kuzudb \                   # Kuzu database path
    --tundra-runner ../build/tundra_bench_runner
```

---

## 🔧 Setup Requirements

### Python Dependencies:

```bash
pip3 install pandas numpy kuzu
```

### Optional (for Neo4j comparison):

```bash
pip3 install neo4j
```

---

## 📈 Workflow

### Option 1: Use Existing Data (Fast)

```bash
# Data already exists in data/ folder
python3 run_bench.py
```

### Option 2: Generate New Data

```bash
# Generate specific size dataset
python3 generate_dataset.py --users 500000

# Run benchmark
python3 run_bench.py
```

### Option 3: Generate During Benchmark

```bash
# Will generate if data doesn't exist
python3 run_bench.py --generate --scale-users 1000000
```

---

## 🎯 Best Practices

1. **Generate data once** with fixed seed (reproducible results)
2. **Run benchmarks multiple times** (--repetitions 5+)
3. **Use Release builds** for TundraDB
4. **Clean Kuzu DB** between runs: `rm -rf kuzudb/`
5. **Compare median times** (more stable than average)

---

## 📁 Directory Structure

```
bench/
├── README.md (this file)
├── generate_dataset.py      # Generate CSV data
├── run_bench.py              # Run benchmarks
├── tundra_runner.cpp         # TundraDB benchmark executable
├── requirements.txt          # Python dependencies
└── data/                     # Generated CSV files
    ├── users.csv
    ├── companies.csv
    ├── friend.csv
    ├── friend_kuzu.csv
    ├── works_at.csv
    └── works_at_kuzu.csv
```

---

## 🐛 Troubleshooting

**Error: "kuzu not installed"**
```bash
pip3 install kuzu
```

**Error: "tundra_bench_runner not found"**
```bash
cd ../build && make tundra_bench_runner
```

**Error: "Data files not found"**
```bash
python3 generate_dataset.py
```

---

## 🎯 Example: Full Benchmark Run

```bash
# 1. Generate 1M user dataset (once)
python3 generate_dataset.py --users 1000000 --companies 10000

# 2. Run benchmark 10 times
python3 run_bench.py --repetitions 10

# 3. View results
cat bench_results.json
```

**Results are deterministic** - same data, same queries, same results! ✅

