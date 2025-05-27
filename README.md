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

### Commiting changes

To store data on disk:

```sql
COMMIT;
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
MATCH (u1:User)-[:FRIEND]->(u2:User),  (u1)-[:WORKS_AT]->(c:Company), (u2)-[:WORKS_AT]->(c);
```

Find friends and their companies they work for

```sql
MATCH (u:User)-[:FRIEND INNER]->(f:User), (f)-[:WORKS_AT INNER]->(c:Company);
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

### Testing

Find users with friends (Inner,Left/Right/Join):

Input:

```sql
CREATE NODE User (name="Alex", age=25);  -- ID = 0
CREATE NODE User (name="Bob", age=31);   -- ID = 1
CREATE NODE User (name="Jeff", age=33);  -- ID = 2
CREATE NODE User (name="Sam", age=21);   -- ID = 3

CREATE EDGE FRIEND FROM User(0) TO User(1);  -- Alex is friends with Bob
CREATE EDGE FRIEND FROM User(0) TO User(2);  -- Alex is friends with Jeff

```

#### Test-1

Query:

```sql
MATCH (u:User)-[:FRIEND INNER]->(f:User);
```

Result (✅ Correct):

```
+======+========+=======+======+========+=======+
| u.id | u.name | u.age | f.id | f.name | f.age |
+======+========+=======+======+========+=======+
|  0   | "Alex" |  25   |  1   | "Bob"  |  31   |
+------+--------+-------+------+--------+-------+
|  0   | "Alex" |  25   |  2   | "Jeff" |  33   |
+------+--------+-------+------+--------+-------+
```

✓ Correct - Shows only Alex's relationships with Bob and Jeff
✓ Matches only exist where there's a FRIEND relationship

#### Test-2

Query:

```sql
MATCH (u:User)-[:FRIEND LEFT]->(f:User);
```

Result (✅  Correct):

```
+======+========+=======+======+========+=======+
| u.id | u.name | u.age | f.id | f.name | f.age |
+======+========+=======+======+========+=======+
|  0   | "Alex" |  25   |  1   | "Bob"  |  31   |
+------+--------+-------+------+--------+-------+
|  0   | "Alex" |  25   |  2   | "Jeff" |  33   |
+------+--------+-------+------+--------+-------+
|  1   | "Bob"  |  31   | null |  null  | null  |
+------+--------+-------+------+--------+-------+
|  2   | "Jeff" |  33   | null |  null  | null  |
+------+--------+-------+------+--------+-------+
|  3   | "Sam"  |  21   | null |  null  | null  |
+------+--------+-------+------+--------+-------+
```

✓ Correct - Shows all users on the left side
✓ Bob, Jeff, and Sam have NULL values on the right because they don't have outgoing FRIEND relationships

#### Test-3
Query:

```sql
MATCH (u:User)-[:FRIEND RIGHT]->(f:User);
```

Result (✅ Correct):

```
+======+========+=======+======+========+=======+
| u.id | u.name | u.age | f.id | f.name | f.age |
+======+========+=======+======+========+=======+
|  0   | "Alex" |  25   |  1   | "Bob"  |  31   |
+------+--------+-------+------+--------+-------+
|  0   | "Alex" |  25   |  2   | "Jeff" |  33   |
+------+--------+-------+------+--------+-------+
| null |  null  | null  |  3   | "Sam"  |  21   |
+------+--------+-------+------+--------+-------+
```

✓ Correct - Shows all users on the right side
✓ Sam has NULL values on the left because no one has a FRIEND relationship with Sam

#### Test-4

Query:

```sql
MATCH (u:User)-[:FRIEND FULL]->(f:User);
```

Result (✅ Fixed), [ticket](https://github.com/dmgcodevil/tundradb/issues/1)

```
+======+========+=======+======+========+=======+
| u.id | u.name | u.age | f.id | f.name | f.age |
+======+========+=======+======+========+=======+
|  0   | "Alex" |  25   |  1   | "Bob"  |  31   |
+------+--------+-------+------+--------+-------+
|  0   | "Alex" |  25   |  2   | "Jeff" |  33   |
+------+--------+-------+------+--------+-------+
|  1   | "Bob"  |  31   | null |  null  | null  |
+------+--------+-------+------+--------+-------+
|  2   | "Jeff" |  33   | null |  null  | null  |
+------+--------+-------+------+--------+-------+
|  3   | "Sam"  |  21   | null |  null  | null  |
+------+--------+-------+------+--------+-------+
```

The Result-4 for the FULL JOIN is not entirely correct.
A FULL JOIN (FULL OUTER JOIN) should include:
All matching rows (like INNER JOIN)
All non-matching rows from the left table (like LEFT JOIN)
All non-matching rows from the right table (like RIGHT JOIN)
The current Result-4 shows:
Alex's relationships with Bob and Jeff (matching rows)
Bob, Jeff, and Sam with NULL values on the right (left-only rows)
But it's missing: a row with Sam on the right side and NULL values on the left
The complete correct result should include this additional row:

```
| null |  null  | null  |  3   | "Sam"  |  21   |
```

For a proper FULL JOIN, you need to combine all distinct records from both sides of the relationship, so Sam should appear both:
As a left-side node with no matching right-side nodes
As a right-side node with no matching left-side nodes

#### Test-5:

Add a new edge Bob -> Alex

```sql
CREATE EDGE FRIEND FROM User(1) TO User(0);
```

Query:

```sql
MATCH (u:User)-[:FRIEND INNER]->(f:User);
```

Result (✅ Correct):

```sql
+======+========+=======+======+========+=======+
| u.id | u.name | u.age | f.id | f.name | f.age |
+======+========+=======+======+========+=======+
|  0   | "Alex" |  25   |  1   | "Bob"  |  31   |
+------+--------+-------+------+--------+-------+
|  0   | "Alex" |  25   |  2   | "Jeff" |  33   |
+------+--------+-------+------+--------+-------+
|  1   | "Bob"  |  31   |  0   | "Alex" |  25   |
+------+--------+-------+------+--------+-------+
```

#### Test-6:

Query:

```sql
MATCH (u:User)-[:FRIEND INNER]->(f:User) WHERE u.age > 30;
```

Result (✅ Fixed) [ticket](https://github.com/dmgcodevil/tundradb/issues/2)

```
+======+========+=======+======+========+=======+
| u.id | u.name | u.age | f.id | f.name | f.age |
+======+========+=======+======+========+=======+
|  1   | "Bob"  |  31   |  0   | "Alex" |  25   |
+------+--------+-------+------+--------+-------+

```

OR filter `f`

```sql
MATCH (u:User)-[:FRIEND INNER]->(f:User) WHERE f.age > 30;
```

Result:

```sql
+======+========+=======+======+========+=======+
| u.id | u.name | u.age | f.id | f.name | f.age |
+======+========+=======+======+========+=======+
|  0   | "Alex" |  25   |  1   | "Bob"  |  31   |
+------+--------+-------+------+--------+-------+
```


### Test suite User, Company

Given:

```
+======+========+=======+======+========+=======+
| u.id | u.name | u.age | f.id | f.name | f.age |
+======+========+=======+======+========+=======+
|  0   | "Alex" |  25   |  1   | "Bob"  |  31   |
+------+--------+-------+------+--------+-------+
|  0   | "Alex" |  25   |  2   | "Jeff" |  33   |
+------+--------+-------+------+--------+-------+
|  1   | "Bob"  |  31   | null |  null  | null  |
+------+--------+-------+------+--------+-------+
|  2   | "Jeff" |  33   | null |  null  | null  |
+------+--------+-------+------+--------+-------+
|  3   | "Sam"  |  21   | null |  null  | null  |
+------+--------+-------+------+--------+-------+
```

Create Company schema:

```sql
CREATE SCHEMA Company (name: STRING, size: INT64);
```

Create companies:

```sql
CREATE NODE Company (name="Google", size=3000);  -- ID = 4
CREATE NODE Company (name="IBM", size=1000);     -- ID = 5
CREATE NODE Company (name="AWS", size=2000);     -- ID = 6
```


Create edges:

```sql
CREATE EDGE WORKS_AT FROM User(0) TO Company(4);  -- Alex works at Google
CREATE EDGE WORKS_AT FROM User(1) TO Company(5);  -- Bob works at IBM
```


#### Test-1


Query:

```sql
MATCH (u:User)-[:FRIEND INNER]->(f:User), (f)-[:WORKS_AT INNER]->(c:Company);
```

Result (✅ Fixed): [ticket](https://github.com/dmgcodevil/tundradb/issues/3)

```
+======+========+=======+======+========+=======+======+==========+========+
| u.id | u.name | u.age | f.id | f.name | f.age | c.id |  c.name  | c.size |
+======+========+=======+======+========+=======+======+==========+========+
|  0   | "Alex" |  25   |  1   | "Bob"  |  31   |  3   |  "IBM"   |  1000  |
+------+--------+-------+------+--------+-------+------+----------+--------+
|  0   | "Alex" |  25   |  1   | "Bob"  |  31   |  4   | "Google" |  3000  |
+------+--------+-------+------+--------+-------+------+----------+--------+
|  1   | "Bob"  |  31   |  0   | "Alex" |  25   |  5   |  "IBM"   |  1000  |
+------+--------+-------+------+--------+-------+------+----------+--------+
```

#### Test-2


Giving:

```
+======+========+=======+======+========+=======+
| u.id | u.name | u.age | f.id | f.name | f.age |
+======+========+=======+======+========+=======+
|  0   | "Alex" |  25   |  1   | "Bob"  |  31   |
+------+--------+-------+------+--------+-------+
|  0   | "Alex" |  25   |  2   | "Jeff" |  33   |
+------+--------+-------+------+--------+-------+
```

```
+======+========+=======+======+==========+========+
| u.id | u.name | u.age | c.id |  c.name  | c.size |
+======+========+=======+======+==========+========+
|  0   | "Alex" |  25   |  3   | "Google" |  3000  |
+------+--------+-------+------+----------+--------+
|  1   | "Bob"  |  31   |  4   |  "IBM"   |  1000  |
+------+--------+-------+------+----------+--------+
|  2   | "Jeff" |  33   |  5   |  "AWS"   |  2000  |
+------+--------+-------+------+----------+--------+
```


Query:

```sql
MATCH (u:User)-[:FRIEND INNER]->(f:User), (f)-[:WORKS_AT INNER]->(c:Company);
```

Result (✅ Correct):

```
+======+========+=======+======+========+=======+======+========+========+
| u.id | u.name | u.age | f.id | f.name | f.age | c.id | c.name | c.size |
+======+========+=======+======+========+=======+======+========+========+
|  0   | "Alex" |  25   |  1   | "Bob"  |  31   |  4   | "IBM"  |  1000  |
+------+--------+-------+------+--------+-------+------+--------+--------+
|  0   | "Alex" |  25   |  2   | "Jeff" |  33   |  5   | "AWS"  |  2000  |
+------+--------+-------+------+--------+-------+------+--------+--------+
```

#### Test-2

For this test case we need to add a new user who works for the Google (as Alex)
and hi also fried with alex


```sql
CREATE NODE User (name="Sam", age=21);  - id 6
CREATE EDGE FRIEND FROM User(0) TO User(6);
CREATE EDGE WORKS_AT FROM User(6) TO Company(3);
```

Query:

```sql
MATCH (u1:User)-[:FRIEND]->(u2:User),  (u1)-[:WORKS_AT]->(c:Company), (u2)-[:WORKS_AT]->(c);
```

Result (✅ Fixed): [ticket](https://github.com/dmgcodevil/tundradb/issues/4)

```
+======+========+=======+======+========+=======+======+==========+========+
| u.id | u.name | u.age | f.id | f.name | f.age | c.id |  c.name  | c.size |
+======+========+=======+======+========+=======+======+==========+========+
|  0   | "Alex" |  25   |  2   | "Jeff" |  33   |  4   | "Google" |  3000  |
+------+--------+-------+------+--------+-------+------+----------+--------+
```

