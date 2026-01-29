#!/usr/bin/env python3
import argparse
import json
import os
import random
import string
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

# Optional imports (guarded)
try:
    from neo4j import GraphDatabase
except Exception:
    GraphDatabase = None

try:
    import kuzu
except Exception:
    kuzu = None


@dataclass
class BenchConfig:
    scale_users: int
    avg_degree: int
    companies: int
    repetitions: int
    limit: int
    output: Path
    neo4j_uri: str
    neo4j_user: str
    neo4j_pass: str
    kuzu_db_path: Path
    tundradb_root: Path
    tundra_runner: Path


def generate_dataset(cfg: BenchConfig, out_dir: Path) -> dict:
    """Import and call generate_dataset.py to create new data."""
    import subprocess
    import sys
    
    print("Generating new dataset...")
    cmd = [
        sys.executable, 'generate_dataset.py',
        '--users', str(cfg.scale_users),
        '--avg-degree', str(cfg.avg_degree),
        '--companies', str(cfg.companies),
        '--output-dir', str(out_dir),
        '--seed', '42'
    ]
    
    result = subprocess.run(cmd, cwd=out_dir.parent)
    if result.returncode != 0:
        raise RuntimeError(f"Dataset generation failed with code {result.returncode}")
    
    return {
        'users': out_dir / 'users.csv',
        'companies': out_dir / 'companies.csv',
        'friend': out_dir / 'friend.csv',
        'works_at': out_dir / 'works_at.csv',
    }


def timeit(fn, reps: int):
    times = []
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000.0)
    times.sort()
    return {
        'median_ms': float(np.median(times)),
        'p90_ms': float(np.percentile(times, 90)),
        'p99_ms': float(np.percentile(times, 99)),
        'all_ms': times,
    }


# Neo4j loader and runners -----------------------------------------------------

def neo4j_connect(cfg: BenchConfig):
    if GraphDatabase is None:
        raise RuntimeError('Neo4j driver not installed')
    driver = GraphDatabase.driver(cfg.neo4j_uri, auth=(cfg.neo4j_user, cfg.neo4j_pass))
    return driver


def neo4j_load(driver, files: dict):
    with driver.session() as sess:
        sess.run("MATCH (n) DETACH DELETE n")
        sess.run("CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE")
        sess.run("CREATE CONSTRAINT company_id IF NOT EXISTS FOR (c:Company) REQUIRE c.id IS UNIQUE")
        # Users
        with open(files['users'], 'r') as f:
            next(f)
            batch = []
            for line in f:
                id_, name, age, country = line.strip().split(',')
                batch.append({'id': int(id_), 'name': name, 'age': int(age), 'country': country})
                if len(batch) >= 10000:
                    sess.run("UNWIND $rows AS r CREATE (:User {id:r.id,name:r.name,age:r.age,country:r.country})", rows=batch)
                    batch.clear()
            if batch:
                sess.run("UNWIND $rows AS r CREATE (:User {id:r.id,name:r.name,age:r.age,country:r.country})", rows=batch)
        # Companies
        with open(files['companies'], 'r') as f:
            next(f)
            batch = []
            for line in f:
                id_, name, industry = line.strip().split(',')
                batch.append({'id': int(id_), 'name': name, 'industry': industry})
                if len(batch) >= 10000:
                    sess.run("UNWIND $rows AS r CREATE (:Company {id:r.id,name:r.name,industry:r.industry})", rows=batch)
                    batch.clear()
            if batch:
                sess.run("UNWIND $rows AS r CREATE (:Company {id:r.id,name:r.name,industry:r.industry})", rows=batch)
        # FRIEND
        with open(files['friend'], 'r') as f:
            next(f)
            batch = []
            for line in f:
                s, d = line.strip().split(',')
                batch.append({'s': int(s), 'd': int(d)})
                if len(batch) >= 20000:
                    sess.run("""
                        UNWIND $rows AS r
                        MATCH (a:User {id:r.s}),(b:User {id:r.d})
                        CREATE (a)-[:FRIEND]->(b)
                    """, rows=batch)
                    batch.clear()
            if batch:
                sess.run("""
                    UNWIND $rows AS r
                    MATCH (a:User {id:r.s}),(b:User {id:r.d})
                    CREATE (a)-[:FRIEND]->(b)
                """, rows=batch)
        # WORKS_AT
        with open(files['works_at'], 'r') as f:
            next(f)
            batch = []
            for line in f:
                s, d = line.strip().split(',')
                batch.append({'s': int(s), 'd': int(d)})
                if len(batch) >= 20000:
                    sess.run("""
                        UNWIND $rows AS r
                        MATCH (a:User {id:r.s}),(c:Company {id:r.d})
                        CREATE (a)-[:WORKS_AT]->(c)
                    """, rows=batch)
                    batch.clear()
            if batch:
                sess.run("""
                    UNWIND $rows AS r
                    MATCH (a:User {id:r.s}),(c:Company {id:r.d})
                    CREATE (a)-[:WORKS_AT]->(c)
                """, rows=batch)


def neo4j_query_runners(driver, cfg: BenchConfig):
    q2 = (
        "MATCH (u:User) WHERE u.age > $age AND u.country = $country "
        "MATCH (u)-[:FRIEND]->(f:User) WHERE f.age > $age2 "
        "RETURN count(*) LIMIT $L"
    )
    def run_q2():
        with driver.session() as sess:
            sess.run(q2, age=30, country='US', age2=25, L=cfg.limit).consume()
    return {'Q2_friend_join': run_q2}


# Kùzu loader and runners ------------------------------------------------------

def kuzu_connect(cfg: BenchConfig):
    if kuzu is None:
        raise RuntimeError('kuzu not installed')
    db = kuzu.Database(str(cfg.kuzu_db_path))
    conn = kuzu.Connection(db)
    return conn


def kuzu_load(conn, files: dict):
    conn.execute("CREATE NODE TABLE User(id INT64, name STRING, age INT64, country STRING, PRIMARY KEY(id))")
    conn.execute("CREATE NODE TABLE Company(id INT64, name STRING, industry STRING, PRIMARY KEY(id))")
    conn.execute("CREATE REL TABLE FRIEND(FROM User TO User)")
    conn.execute("CREATE REL TABLE WORKS_AT(FROM User TO Company)")
    conn.execute(f"COPY User FROM '{files['users']}' (HEADER=true)")
    conn.execute(f"COPY Company FROM '{files['companies']}' (HEADER=true)")
    # Rewrite edge CSVs to have FROM,TO headers for Kùzu COPY
    friend_k = files['friend'].with_name('friend_kuzu.csv')
    works_k = files['works_at'].with_name('works_at_kuzu.csv')
    pd.read_csv(files['friend']).rename(columns={'src': 'FROM', 'dst': 'TO'}).to_csv(friend_k, index=False)
    pd.read_csv(files['works_at']).rename(columns={'src': 'FROM', 'dst': 'TO'}).to_csv(works_k, index=False)
    conn.execute(f"COPY FRIEND FROM '{friend_k}' (HEADER=true)")
    conn.execute(f"COPY WORKS_AT FROM '{works_k}' (HEADER=true)")


def kuzu_query_runners(conn, cfg: BenchConfig):
    print("kuzu_query_runners")
    q2 = (
        "MATCH (u:User) WHERE u.age > 30 AND u.country = 'US' "
        "MATCH (u)-[:FRIEND]->(f:User) WHERE f.age > 25 "
        "RETURN u.id AS uid, f.id AS fid"
    )
    # def run_q2():
    #     res = conn.execute(q2.replace("$L", str(cfg.limit)))
    #     try:
    #         df = res.to_df()
    #         print(f"kuzu_rows={len(df)}", flush=True)
    #     except Exception:
    #         pass
    def run_q2():
        res = conn.execute(q2)
        cnt = 0
        while res.has_next():
            _ = res.get_next()
            cnt += 1
        print(f"kuzu_rows={cnt}", flush=True)
    return {'Q2_friend_join': run_q2}


# TundraDB runners -------------------------------------------------------------

def tundra_run_benchmark(cfg: BenchConfig, files: dict) -> dict:
    """
    Run TundraDB benchmark.
    Calls tundra_runner once with repetitions parameter.
    The runner loads data once, runs query N times internally.
    """
    bin_path = cfg.tundra_runner
    cmd = [
        str(bin_path), 
        str(files['users']), 
        str(files['companies']), 
        str(files['friend']), 
        str(files['works_at']),
        str(cfg.repetitions)  # Pass repetitions to C++ runner
    ]
    
    print(f"Running TundraDB benchmark ({cfg.repetitions} repetitions)...")
    cp = subprocess.run(cmd, capture_output=True, text=True)
    
    if cp.returncode != 0:
        raise RuntimeError(f"tundra_bench_runner failed: {cp.returncode}\n{cp.stderr}")
    
    # Parse output: one query time per line (in ms)
    times = []
    for line in cp.stdout.strip().split('\n'):
        try:
            times.append(float(line))
        except ValueError:
            continue
    
    if len(times) != cfg.repetitions:
        print(f"⚠️  Warning: Expected {cfg.repetitions} times, got {len(times)}")
    
    # Print debug info
    if cp.stderr:
        print(cp.stderr.strip())
    
    return {
        'median_ms': float(np.median(times)) if times else 0.0,
        'p90_ms': float(np.percentile(times, 90)) if times else 0.0,
        'p99_ms': float(np.percentile(times, 99)) if times else 0.0,
        'all_ms': times,
    }


def tundra_query_runners(cfg: BenchConfig, files: dict):
    """Compatibility wrapper - not used in new flow."""
    # Old flow used timeit(), new flow calls runner directly
    pass


def main():
    ap = argparse.ArgumentParser(
        description='Run graph database benchmark (TundraDB vs Kuzu vs Neo4j)'
    )
    # env defaults for Neo4j (prefer 127.0.0.1 for local)
    env_uri = os.getenv('NEO4J_URI', 'bolt://127.0.0.1:7687')
    env_user = os.getenv('NEO4J_USER', 'neo4j')
    env_pass = os.getenv('NEO4J_PASS', '12345678')

    ap.add_argument('--data-dir', type=Path, default=Path('data'),
                    help='Directory containing CSV files (default: data/)')
    ap.add_argument('--generate', action='store_true',
                    help='Generate new dataset (otherwise use existing)')
    ap.add_argument('--scale-users', type=int, default=1000000,
                    help='Number of users (only if --generate)')
    ap.add_argument('--avg-degree', type=int, default=5,
                    help='Average friends per user (only if --generate)')
    ap.add_argument('--companies', type=int, default=10000,
                    help='Number of companies (only if --generate)')
    ap.add_argument('--repetitions', type=int, default=5,
                    help='Number of times to run each query')
    ap.add_argument('--limit', type=int, default=1000000)
    ap.add_argument('--output', type=Path, default=Path('bench_results.json'))
    ap.add_argument('--neo4j-uri', type=str, default=env_uri)
    ap.add_argument('--neo4j-user', type=str, default=env_user)
    ap.add_argument('--neo4j-pass', type=str, default=env_pass)
    ap.add_argument('--kuzu-db', type=Path, default=Path('kuzudb'))
    ap.add_argument('--tundradb-root', type=Path, default=Path('.'))
    ap.add_argument('--tundra-runner', type=Path, default=Path('../build/tundra_bench_runner'))
    args = ap.parse_args()

    cfg = BenchConfig(
        scale_users=args.scale_users,
        avg_degree=args.avg_degree,
        companies=args.companies,
        repetitions=args.repetitions,
        limit=args.limit,
        output=args.output,
        neo4j_uri=args.neo4j_uri,
        neo4j_user=args.neo4j_user,
        neo4j_pass=args.neo4j_pass,
        kuzu_db_path=args.kuzu_db,
        tundradb_root=args.tundradb_root,
        tundra_runner=args.tundra_runner,
    )

    # Check if data files exist
    data_dir = args.data_dir
    required_files = ['users.csv', 'companies.csv', 'friend.csv', 'works_at.csv']
    files = {
        'users': data_dir / 'users.csv',
        'companies': data_dir / 'companies.csv',
        'friend': data_dir / 'friend.csv',
        'works_at': data_dir / 'works_at.csv',
    }
    
    # Check if all required files exist
    all_exist = all(f.exists() for f in files.values())
    
    if args.generate or not all_exist:
        if not all_exist:
            print(f"⚠️  Data files not found in {data_dir}")
            print("Generating new dataset...")
        files = generate_dataset(cfg, data_dir)
    else:
        print(f"✅ Using existing dataset from {data_dir}")
        for name, path in files.items():
            rows = sum(1 for _ in open(path)) - 1  # -1 for header
            print(f"   - {name}: {rows:,} rows")
        print()

    results = {}

    # Neo4j
    try:
        driver = neo4j_connect(cfg)
        neo4j_load(driver, files)
        neo_runners = neo4j_query_runners(driver, cfg)
        results['neo4j'] = {
            name: timeit(fn, cfg.repetitions) for name, fn in neo_runners.items()
        }
    except Exception as e:
        results['neo4j'] = {'error': str(e)}

    # Kùzu
    try:
        if cfg.kuzu_db_path.exists():
            os.remove(cfg.kuzu_db_path)
        # cfg.kuzu_db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = kuzu_connect(cfg)
        kuzu_load(conn, files)
        kuzu_runners = kuzu_query_runners(conn, cfg)
        results['kuzu'] = {
            name: timeit(fn, cfg.repetitions) for name, fn in kuzu_runners.items()
        }
    except Exception as e:
        results['kuzu'] = {'error': str(e)}

    # TundraDB
    try:
        tundra_result = tundra_run_benchmark(cfg, files)
        results['tundradb'] = {
            'Q2_friend_join': tundra_result
        }
    except Exception as e:
        results['tundradb'] = {'error': str(e)}

    with open(cfg.output, 'w') as f:
        json.dump(results, f, indent=2)
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()