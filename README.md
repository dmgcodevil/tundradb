# TundraDB - Graph Database Query Language Guide

TundraDB is a high-performance graph database with a custom query language called TundraQL. This guide demonstrates the full feature set with practical examples.

## Building TundraDB from Source

If you need to build TundraDB shell from source:

```bash
# Clone the repository (if you haven't already)
git clone https://github.com/your-repo/tundradb.git
cd tundradb

# Build in release mode
mkdir -p build_release
cd build_release
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(sysctl -n hw.ncpu)

# The binary will be in build_release/tundra_shell
```

Required dependencies:
- ANTLR4 C++ runtime: `brew install antlr4-cpp-runtime`
- Apache Arrow: `brew install apache-arrow`
- spdlog: `brew install spdlog`
- libcds: `brew install libcds`
- gtest: `brew install googletest`
- Google Benchmark:
  ```
    git clone https://github.com/google/benchmark.git && \
    cd benchmark && \
    mkdir build && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DBENCHMARK_ENABLE_GTEST_TESTS=OFF .. && \
    make -j && \
    make install && \
  ```
- TBB: `brew install tbb`
- Java: `brew install openjdk@21` then `export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"`

## Getting Started

1. Launch the TundraDB shell:
   ```bash
   ./build_release/tundra_shell
   ```
   or if using the portable version:
   ```bash
   ./tundra_shell_launcher.sh
   ```

2. **Script Execution**: Run TundraQL script files:
   ```bash
   # Execute a script and keep shell open
   ./tundra_shell --script my_script.sql
   
   # Execute script and exit (batch mode)
   ./tundra_shell --script my_script.sql --detach
   
   # Execute with unique database and output file
   ./tundra_shell --script my_script.sql --unique-db --output results.txt
   ```

3. Create your schemas and data as shown below, then start querying!

## Data Model

### Schema Definition

Define the structure of your node types using supported data types:

```sql
-- User schema with STRING and INT64 types
CREATE SCHEMA User (name: STRING, age: INT64, department: STRING);

-- Company schema 
CREATE SCHEMA Company (name: STRING, industry: STRING, size: INT64);

-- Project schema
CREATE SCHEMA Project (name: STRING, budget: INT64, status: STRING);
```

**Supported Data Types:**
- `STRING`: Text values (e.g., "Alice", "TechCorp")
- `INT64`: Integer values (e.g., 25, 1000)

**Note**: An `id` field of type `INT64` is automatically added to every schema.

### Node Creation

Create nodes with the defined schemas:

```sql
-- Create User nodes
CREATE NODE User (name="Alice", age=25, department="Engineering") RETURN id;  -- ID = 0
CREATE NODE User (name="Bob", age=30, department="Engineering") RETURN id;    -- ID = 1
CREATE NODE User (name="Charlie", age=35, department="Marketing") RETURN id;  -- ID = 2
CREATE NODE User (name="David", age=40, department="Engineering") RETURN id;  -- ID = 3

-- Create Company nodes
CREATE NODE Company (name="TechCorp", industry="Technology", size=500) RETURN id;  -- ID = 4
CREATE NODE Company (name="FinanceInc", industry="Finance", size=300) RETURN id;   -- ID = 5

-- Create Project nodes
CREATE NODE Project (name="WebApp", budget=100000, status="Active") RETURN id;     -- ID = 6
CREATE NODE Project (name="MobileApp", budget=150000, status="Active") RETURN id;  -- ID = 7
```

**Important**: Node IDs are unique across all node types and start at 0.

### Edge Creation

TundraDB offers multiple ways to create edges between nodes:

#### 1. Legacy ID-Based Syntax

```sql
-- Create edges using node IDs
CREATE EDGE WORKS_AT FROM User(0) TO Company(4);  -- Alice works at TechCorp
CREATE EDGE WORKS_AT FROM User(1) TO Company(4);  -- Bob works at TechCorp
CREATE EDGE WORKS_AT FROM User(2) TO Company(5);  -- Charlie works at FinanceInc

-- Create friendship relationships
CREATE EDGE FRIEND FROM User(0) TO User(1);  -- Alice is friends with Bob
```

#### 2. 🆕 Property-Based Node Selection

Select nodes by their properties instead of IDs:

```sql
-- Single edge with UNIQUE constraint (fails if multiple matches)
CREATE UNIQUE EDGE WORKS_AT FROM (User{name="Alice"}) TO (Company{name="TechCorp"});

-- Single edge with UNIQUE constraint using multiple properties
CREATE UNIQUE EDGE ASSIGNED_TO FROM (User{name="Bob", department="Engineering"}) 
                                  TO (Project{name="WebApp"});
```

#### 3. 🆕 Batch Edge Creation

Create multiple edges when properties match multiple nodes:

```sql
-- Create edges from ALL Engineering users to TechCorp
CREATE EDGE WORKS_AT FROM (User{department="Engineering"}) TO (Company{name="TechCorp"});
-- Result: Creates 3 edges (Alice, Bob, David → TechCorp)

-- Create edges from ALL Active projects to specific users
CREATE EDGE ASSIGNED_TO FROM (User{name="Alice"}) TO (Project{status="Active"});
-- Result: Creates 2 edges (Alice → WebApp, Alice → MobileApp)
```

**Edge Creation Summary:**
- **Legacy syntax**: `User(0)` - Select by ID
- **UNIQUE edges**: Fail if property selector matches multiple nodes
- **Batch edges**: Create edges to ALL matching nodes
- **Property matching**: Use `{property=value}` syntax for selection

### Committing Changes

Store data persistently:

```sql
COMMIT;
```

## Query Language

### Basic Node Queries

```sql
-- Query all users
MATCH (u:User);

-- Query with specific fields
MATCH (u:User) SELECT u.name, u.age;

-- Query all companies
MATCH (c:Company);
```

### WHERE Clauses

Filter data using various operators:

```sql
-- Equality operators
MATCH (u:User) WHERE u.name = "Alice";
MATCH (u:User) WHERE u.age != 30;

-- Comparison operators  
MATCH (u:User) WHERE u.age > 30;
MATCH (u:User) WHERE u.age < 35;

-- String and numeric filtering
MATCH (c:Company) WHERE c.industry = "Technology";
MATCH (p:Project) WHERE p.budget > 100000;
```

### Relationship Queries

#### Basic Traversals

```sql
-- Find users who work at companies
MATCH (u:User)-[:WORKS_AT]->(c:Company);

-- Find users assigned to projects
MATCH (u:User)-[:ASSIGNED_TO]->(p:Project);

-- Find friendships
MATCH (u:User)-[:FRIEND]->(f:User);
```

#### Combined Queries

```sql
-- Find users at specific companies
MATCH (u:User)-[:WORKS_AT]->(c:Company) 
WHERE c.name = "TechCorp";

-- Find users over 30 who work at tech companies
MATCH (u:User)-[:WORKS_AT]->(c:Company) 
WHERE u.age > 30 AND c.industry = "Technology";

-- Find friends who work at the same company
MATCH (u1:User)-[:FRIEND]->(u2:User), 
      (u1)-[:WORKS_AT]->(c:Company), 
      (u2)-[:WORKS_AT]->(c);
```

### Join Types

TundraQL supports SQL-style joins for relationship queries:

```sql
-- INNER join (default) - only matched relationships
MATCH (u:User)-[:WORKS_AT INNER]->(c:Company);

-- LEFT join - all users, even those without companies
MATCH (u:User)-[:WORKS_AT LEFT]->(c:Company);

-- RIGHT join - all companies, even those without users  
MATCH (u:User)-[:WORKS_AT RIGHT]->(c:Company);

-- FULL join - all users and companies
MATCH (u:User)-[:WORKS_AT FULL]->(c:Company);
```

#### Join Examples

Given this data setup:
```sql
-- Users: Alice(0), Bob(1), Charlie(2), David(3)
-- Companies: TechCorp(4), FinanceInc(5) 
-- Edges: Alice→TechCorp, Bob→TechCorp
```

**INNER JOIN** returns only matched relationships:
```
| u.id | u.name  | c.id | c.name     |
|------|---------|------|------------|
|  0   | "Alice" |  4   | "TechCorp" |
|  1   | "Bob"   |  4   | "TechCorp" |
```

**LEFT JOIN** includes all users:
```
| u.id | u.name    | c.id | c.name     |
|------|-----------|------|------------|
|  0   | "Alice"   |  4   | "TechCorp" |
|  1   | "Bob"     |  4   | "TechCorp" |
|  2   | "Charlie" | null | null       |
|  3   | "David"   | null | null       |
```

**FULL JOIN** includes all users and companies:
```
| u.id | u.name    | c.id | c.name      |
|------|-----------|------|-------------|
|  0   | "Alice"   |  4   | "TechCorp"  |
|  1   | "Bob"     |  4   | "TechCorp"  |
|  2   | "Charlie" | null | null        |
|  3   | "David"   | null | null        |
| null | null      |  5   | "FinanceInc"|
```

## 🆕 DELETE Operations

TundraDB supports flexible DELETE operations for both nodes and relationships:

### Delete by ID

```sql
-- Delete specific nodes by ID
DELETE User(0);     -- Delete Alice
DELETE Company(4);  -- Delete TechCorp

-- Delete relationships by ID  
DELETE EDGE WORKS_AT FROM User(1) TO Company(4);
```

### Delete by Pattern

```sql
-- Delete all nodes of a type
DELETE (u:User);
DELETE (c:Company);

-- Delete specific nodes by pattern
DELETE (u:User{name="Alice"});
DELETE (c:Company{industry="Technology"});
```

### Delete with WHERE Clauses

```sql
-- Delete users over a certain age
DELETE (u:User) WHERE u.age > 35;

-- Delete companies with small size
DELETE (c:Company) WHERE c.size < 100;

-- Delete projects with specific status
DELETE (p:Project) WHERE p.status = "Cancelled";
```

### Delete Relationships

```sql
-- Delete all edges of a specific type
DELETE EDGE WORKS_AT;

-- Delete edges with pattern matching
DELETE EDGE WORKS_AT FROM (User{department="Marketing"}) TO (Company{});

-- Delete edges with WHERE clauses
DELETE EDGE ASSIGNED_TO FROM (User{}) TO (Project{}) WHERE Project.status = "Complete";
```

## Complete Example Session

Here's a comprehensive example showing TundraDB's full capabilities:

```sql
-- 1. Create schemas
CREATE SCHEMA User (name: STRING, age: INT64, department: STRING);
CREATE SCHEMA Company (name: STRING, industry: STRING, size: INT64);
CREATE SCHEMA Project (name: STRING, budget: INT64, status: STRING);

-- 2. Create nodes
CREATE NODE User (name="Alice", age=25, department="Engineering") RETURN id;
CREATE NODE User (name="Bob", age=30, department="Engineering") RETURN id;
CREATE NODE User (name="Charlie", age=35, department="Marketing") RETURN id;

CREATE NODE Company (name="TechCorp", industry="Technology", size=500) RETURN id;
CREATE NODE Company (name="FinanceInc", industry="Finance", size=300) RETURN id;

CREATE NODE Project (name="WebApp", budget=100000, status="Active") RETURN id;
CREATE NODE Project (name="MobileApp", budget=150000, status="Complete") RETURN id;

-- 3. Create relationships using multiple syntaxes
-- Legacy ID-based
CREATE EDGE WORKS_AT FROM User(0) TO Company(3);

-- Property-based with UNIQUE constraint
CREATE UNIQUE EDGE WORKS_AT FROM (User{name="Bob"}) TO (Company{name="TechCorp"});

-- Batch edge creation  
CREATE EDGE ASSIGNED_TO FROM (User{department="Engineering"}) TO (Project{status="Active"});

-- 4. Query examples
-- Basic traversal
MATCH (u:User)-[:WORKS_AT]->(c:Company);

-- Complex filtering
MATCH (u:User)-[:WORKS_AT]->(c:Company) 
WHERE u.age > 25 AND c.industry = "Technology"
SELECT u.name, u.age, c.name;

-- JOIN examples
MATCH (u:User)-[:WORKS_AT LEFT]->(c:Company);

-- 5. Cleanup operations
-- Delete completed projects
DELETE (p:Project) WHERE p.status = "Complete";

-- Delete marketing employees
DELETE (u:User) WHERE u.department = "Marketing";

-- Commit changes
COMMIT;
```

## Features Summary

✅ **Schema Management**: Define typed node structures  
✅ **Node Operations**: Create, query, and delete nodes  
✅ **Advanced Edge Creation**: ID-based, property-based, and batch creation  
✅ **Flexible Queries**: Pattern matching, WHERE clauses, complex traversals  
✅ **JOIN Support**: INNER, LEFT, RIGHT, and FULL joins  
✅ **DELETE Operations**: By ID, pattern, or WHERE conditions  
✅ **Script Execution**: Batch processing and automation  
✅ **Data Persistence**: COMMIT changes to disk  

## Command Line Options

```bash
# Basic usage
./tundra_shell

# Script execution
./tundra_shell --script file.sql                    # Execute script, keep shell open
./tundra_shell -s file.sql --detach                 # Execute and exit
./tundra_shell -s file.sql --unique-db              # Use timestamped database
./tundra_shell -s file.sql --output results.txt     # Save output to file

# Combined options
./tundra_shell -s file.sql -u -o results.txt --detach
```

TundraDB provides a powerful and intuitive graph database experience with modern query capabilities and flexible data manipulation features.
