# TundraDB - Graph Database Query Language Guide

TundraDB is a high-performance graph database with a custom query language called TundraQL. This guide demonstrates the basic usage patterns with examples.

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
   ```
   ./build_release/tundra_shell
   ```
   or if using the portable version:
   ```
   ./tundra_shell_launcher.sh
   ```

2. Create your schemas and data as shown below, then start querying!

## Data Model Examples

### Defining Schemas

First, define the structure of your node types:

```sql
-- Define a User schema with name and age
CREATE SCHEMA User (name: STRING, age: INT64);

-- Define a Company schema with name and size
CREATE SCHEMA Company (name: STRING, size: INT64);
```

### Creating Nodes

Once schemas are defined, you can create nodes:

```sql
-- Create User nodes
CREATE NODE User (name="Alex", age=25);  -- ID = 0
CREATE NODE User (name="Bob", age=31);   -- ID = 1
CREATE NODE User (name="Jeff", age=33);  -- ID = 2
CREATE NODE User (name="Sam", age=21);   -- ID = 3
CREATE NODE User (name="Matt", age=40);  -- ID = 4

-- Create Company nodes
CREATE NODE Company (name="Google", size=3000);  -- ID = 5
CREATE NODE Company (name="IBM", size=1000);     -- ID = 6
CREATE NODE Company (name="AWS", size=2000);     -- ID = 7
```

**Important Note**: Node IDs are unique across all node types and start at 0. The IDs shown above assume you're creating nodes in exactly this order.

### Creating Relationships

Create relationships between nodes using their IDs:

```sql
-- Create WORKS_AT relationships (using the correct IDs)
CREATE EDGE WORKS_AT FROM User(0) TO Company(5);  -- Alex works at Google
CREATE EDGE WORKS_AT FROM User(1) TO Company(6);  -- Bob works at IBM
CREATE EDGE WORKS_AT FROM User(2) TO Company(7);  -- Jeff works at AWS
CREATE EDGE WORKS_AT FROM User(3) TO Company(7);  -- Sam works at AWS
CREATE EDGE WORKS_AT FROM User(4) TO Company(5);  -- Matt works at Google

-- Create FRIEND relationships between users
CREATE EDGE FRIEND FROM User(0) TO User(1);  -- Alex is friends with Bob
CREATE EDGE FRIEND FROM User(0) TO User(2);  -- Alex is friends with Jeff
CREATE EDGE FRIEND FROM User(1) TO User(4);  -- Bob is friends with Matt
```

## Query Examples

### Basic Node Queries

Query all users:

```sql
MATCH (u:User);
```

Query all companies:

```sql
MATCH (c:Company);
```

Query with specific fields:

```sql
MATCH (u:User) SELECT u.name, u.age;
```

### Relationship Queries

Find users who work at companies:

```sql
MATCH (u:User)-[:WORKS_AT]->(c:Company);
```

Find users who work at Google:

```sql
MATCH (u:User)-[:WORKS_AT]->(c:Company) 
WHERE c.name = "Google";
```

Find users over 30 who work at AWS:

```sql
MATCH (u:User)-[:WORKS_AT]->(c:Company) 
WHERE u.age > 30 AND c.name = "AWS";
```

### Advanced Queries

Find friends who work at the same company:

```sql
MATCH (u1:User)-[:FRIEND]->(u2:User), 
      (u1)-[:WORKS_AT]->(c:Company), 
      (u2)-[:WORKS_AT]->(c);
```

Find the average age of users at each company:

```sql
MATCH (u:User)-[:WORKS_AT]->(c:Company)
SELECT c.name, AVG(u.age);
```

### Join Types

TundraQL supports different join types:

```sql
-- INNER join (default) - only returns users who work at companies
MATCH (u:User)-[:WORKS_AT INNER]->(c:Company);

-- LEFT join - returns all users, even those without companies
MATCH (u:User)-[:WORKS_AT LEFT]->(c:Company);

-- RIGHT join - returns all companies, even those without users
MATCH (u:User)-[:WORKS_AT RIGHT]->(c:Company);

-- FULL join - returns all users and companies
MATCH (u:User)-[:WORKS_AT FULL]->(c:Company);
```

## Complete Example Session

Here's a complete example session:

```sql
-- Create schemas
CREATE SCHEMA User (name: STRING, age: INT64);
CREATE SCHEMA Company (name: STRING, size: INT64);

-- Create users
CREATE NODE User (name="Alex", age=25);  -- ID = 0
CREATE NODE User (name="Bob", age=31);   -- ID = 1
CREATE NODE User (name="Jeff", age=33);  -- ID = 2

-- Create companies
CREATE NODE Company (name="Google", size=3000);  -- ID = 3
CREATE NODE Company (name="IBM", size=1000);     -- ID = 4

-- Create relationships
CREATE EDGE WORKS_AT FROM User(0) TO Company(3);
CREATE EDGE WORKS_AT FROM User(1) TO Company(4);
CREATE EDGE WORKS_AT FROM User(2) TO Company(3);

-- Query users at Google
MATCH (u:User)-[:WORKS_AT]->(c:Company) 
WHERE c.name = "Google"
SELECT u.name, u.age, c.name;
```

This query would return Alex and Jeff, who both work at Google. 
