# TundraDB Test Scripts

This folder contains various test scripts to demonstrate and test TundraDB functionality.

## Usage

You can execute these scripts using the TundraDB shell with the `--script` option:

```bash
# Execute a script with a unique database
./build/tundra_shell --script test_scripts/simple_test.sql --unique-db

# Execute a script with a specific database path
./build/tundra_shell --script test_scripts/delete_test.sql --db-path ./my-test-db

# Execute a script and then continue with interactive shell
./build/tundra_shell --script test_scripts/join_test.sql --unique-db
```

## Available Test Scripts

### `simple_test.sql`
- **Purpose**: Basic functionality demonstration
- **Features**: Schema creation, node creation, basic queries, relationships
- **Good for**: First-time users, basic functionality verification

### `delete_test.sql`
- **Purpose**: DELETE functionality testing
- **Features**: DELETE by ID, DELETE by pattern, DELETE with WHERE clauses
- **Good for**: Testing deletion operations, data cleanup scenarios

### `join_test.sql`
- **Purpose**: JOIN operations testing
- **Features**: INNER, LEFT, RIGHT, and FULL joins
- **Good for**: Understanding relationship traversal and join behavior

### `where_test.sql`
- **Purpose**: WHERE clause functionality testing
- **Features**: Various comparison operators (=, !=, >, <, >=, <=) with different data types
- **Good for**: Testing filtering and conditional queries

### `where_and_or_test.sql`
- **Purpose**: Complex WHERE clause testing with AND/OR operators
- **Features**: Compound WHERE clauses using AND/OR logical operators
- **Good for**: Testing complex filtering conditions and logical combinations

### `where_and_or_simple_test.sql`
- **Purpose**: Basic AND/OR functionality demonstration
- **Features**: Simple examples of AND/OR usage in WHERE clauses
- **Good for**: Learning basic logical operators in queries

### `enhanced_edge_test.sql`

## Command Line Options

### `--script` / `-s`
Execute a script file and then keep the shell open for interactive use.

### `--unique-db` / `--temp-db` / `-u`
Append a timestamp to the database path to create a unique database folder. Useful for:
- Testing without affecting existing databases
- Running multiple tests in parallel
- Creating isolated test environments

### `--db-path` / `-d`
Specify a custom database path instead of the default `./test-db`.

## Example Workflows

### Quick Test with Cleanup
```bash
# Run a test with a temporary database that won't interfere with your main data
./build/tundra_shell --script test_scripts/simple_test.sql --unique-db
```

### Development Testing
```bash
# Test specific functionality with a dedicated test database
./build/tundra_shell --script test_scripts/join_test.sql --db-path ./join-test-db
```

### Interactive Development
```bash
# Run a setup script then continue working interactively
./build/tundra_shell --script test_scripts/simple_test.sql --unique-db
# Shell remains open for additional commands
tundra> MATCH (u:User) WHERE u.age > 25;
tundra> exit
```

## Schema Notes

- **ID Column**: All schemas automatically include an `id: INT64` column as the primary key
- **Data Types**: Supported types are `STRING`, `INT64`, and `FLOAT64`
- **Relationships**: Created using `CREATE EDGE` statements between existing nodes

## WHERE Clause Syntax

TundraDB supports complex WHERE clauses with logical operators:

### Basic Comparisons
```sql
-- Equality and inequality
MATCH (u:User) WHERE u.name = "Alice";
MATCH (u:User) WHERE u.age != 25;

-- Numeric comparisons
MATCH (u:User) WHERE u.age > 30;
MATCH (u:User) WHERE u.salary <= 100000;
```

### Logical Operators
```sql
-- AND operator (all conditions must be true)
MATCH (u:User) WHERE u.age > 25 AND u.city = "NYC";

-- OR operator (at least one condition must be true)
MATCH (u:User) WHERE u.city = "NYC" OR u.city = "SF";

-- Multiple conditions
MATCH (u:User) WHERE u.name = "Alice" AND u.age = 25 AND u.city = "NYC";
MATCH (u:User) WHERE u.age = 25 OR u.age = 30 OR u.age = 35;
```

### Current Limitations
- **Operator Precedence**: Currently uses left-to-right evaluation with simple heuristics
- **Parentheses**: Not yet supported for explicit precedence control
- **Complex Expressions**: Mixed AND/OR in the same expression may not follow standard precedence rules

### Supported Operators
- `=` (equals)
- `!=` (not equals)  
- `>` (greater than)
- `<` (less than)
- `>=` (greater than or equal)
- `<=` (less than or equal)

## Tips

1. Use `--unique-db` for testing to avoid database conflicts
2. Scripts support comments using `--` at the beginning of lines
3. Each statement must end with a semicolon (`;`)
4. The shell will show execution progress and any errors
5. After script execution, you can continue with interactive commands 