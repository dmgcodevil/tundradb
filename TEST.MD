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

