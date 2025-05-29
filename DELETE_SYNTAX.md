# TundraDB DELETE Syntax

TundraDB now supports `DELETE` statements to remove nodes and relationships from the graph database. The DELETE functionality follows a pattern similar to other graph query languages while maintaining TundraDB's unique syntax.

## Syntax Overview

```sql
DELETE <target> [WHERE <condition>];
```

## Delete Target Types

### 1. Delete by Node ID
Delete a specific node by its ID and schema type:

```sql
DELETE User(123);
DELETE Company(456);
```

**Example:**
```sql
-- Delete user with ID 5
DELETE User(5);
```

### 2. Delete by Pattern
Delete nodes matching a pattern, optionally with WHERE conditions:

```sql
DELETE (u:User);
DELETE (c:Company) WHERE c.industry = "Technology";
DELETE (u:User) WHERE u.age > 65;
```

**Examples:**
```sql
-- Delete all users
DELETE (u:User);

-- Delete users older than 30
DELETE (u:User) WHERE u.age > 30;

-- Delete companies in specific industry
DELETE (c:Company) WHERE c.industry = "Finance";
```

### 3. Delete Relationships (Future)
Delete relationships matching a path pattern:

```sql
DELETE (u:User)-[:WORKS_AT]->(c:Company);
DELETE (u:User)-[:FRIEND]->(f:User) WHERE u.age < 25;
```

*Note: Relationship deletion is planned but not yet implemented.*

## WHERE Clause Support

The DELETE statement supports WHERE clauses with the following operators:

- `=` (equals)
- `!=` (not equals) 
- `>` (greater than)
- `<` (less than)
- `>=` (greater than or equal)
- `<=` (less than or equal)

**Supported value types:**
- Integers: `WHERE u.age > 30`
- Strings: `WHERE u.name = "Alice"`
- Floats: `WHERE u.salary > 50000.0`

## Implementation Details

### Node Deletion Process

1. **By ID**: Directly removes the specified node from the schema table
2. **By Pattern**: 
   - Executes a MATCH query to find nodes matching the pattern and WHERE conditions
   - Extracts node IDs from the result set
   - Removes each matching node from the database

### Return Value

DELETE statements return the number of nodes/relationships deleted:

```sql
tundra> DELETE (u:User) WHERE u.age > 30;
Deleted 2 User nodes
```

### Error Handling

- If a node ID doesn't exist, the operation fails gracefully with an error message
- If no nodes match the pattern/conditions, returns 0 deleted count
- Invalid syntax results in a parse error

## Auto-completion Support

The TundraDB shell provides auto-completion for DELETE statements:

- Type `DELETE ` to see completion options
- Supports completion for common patterns like `DELETE (` and `DELETE User(`

## Examples

### Complete Example Session

```sql
-- Create schema and data (id column is added automatically)
CREATE SCHEMA User (name: STRING, age: INT64);
CREATE NODE User (name="Alice", age=25) RETURN id;
CREATE NODE User (name="Bob", age=30) RETURN id;
CREATE NODE User (name="Charlie", age=35) RETURN id;

-- Show all users
MATCH (u:User);

-- Delete specific user by ID
DELETE User(2);

-- Delete users by condition
DELETE (u:User) WHERE u.age > 30;

-- Show remaining users
MATCH (u:User);
```

## Future Enhancements

1. **Relationship Deletion**: Support for deleting edges/relationships
2. **Cascade Deletion**: Option to delete related nodes when deleting relationships
3. **Batch Operations**: Optimized deletion for large datasets
4. **Transaction Support**: Rollback capability for failed deletions
5. **Complex WHERE Clauses**: Support for AND/OR operations

## Grammar Definition

The DELETE statement is defined in the TundraQL grammar as:

```antlr
deleteStatement: K_DELETE deleteTarget (K_WHERE whereClause)? SEMI;

deleteTarget: 
    nodeLocator                    // DELETE User(123);
    | pathPattern                  // DELETE (u:User)-[:FRIEND]->(f:User);
    | nodePattern;                 // DELETE (u:User);
```

This provides a flexible foundation for various deletion patterns while maintaining consistency with TundraDB's overall query language design. 