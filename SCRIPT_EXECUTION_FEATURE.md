# TundraDB Script Execution and Unique Database Features

## Overview

TundraDB shell now supports two powerful new features for development and testing:

1. **Script Execution**: Execute TundraQL script files and then continue with interactive shell
2. **Unique Database**: Create timestamped database folders for isolated testing

## New Command Line Options

### `--script` / `-s FILE`
Execute a script file containing TundraQL statements, then keep the shell open for interactive use.

```bash
./build/tundra_shell --script test_scripts/simple_test.sql
```

**Features:**
- Executes all statements in the file sequentially
- Shows progress and execution status for each statement
- Handles comments (lines starting with `--`)
- Continues to interactive shell after script completion
- Reports execution statistics (statements executed/failed)

### `--unique-db` / `--temp-db` / `-u`
Append a timestamp to the database path to create a unique database folder.

```bash
./build/tundra_shell --unique-db
# Creates database at: ./test-db_20241128_143052_123
```

**Benefits:**
- Avoid conflicts with existing databases
- Enable parallel testing
- Create isolated test environments
- Automatic cleanup through unique naming

## Combined Usage

The most powerful combination is using both features together:

```bash
./build/tundra_shell --script test_scripts/join_test.sql --unique-db
```

This will:
1. Create a unique timestamped database
2. Execute the script against the new database
3. Keep the shell open for additional interactive commands
4. Leave you with a clean, isolated test environment

## Implementation Details

### Script Execution Engine
- **Parser Integration**: Uses the same ANTLR parser as interactive mode
- **Error Handling**: Continues execution even if individual statements fail
- **Progress Reporting**: Shows each statement being executed
- **Statistics**: Reports total statements executed and failed

### Unique Database Generation
- **Timestamp Format**: `YYYYMMDD_HHMMSS_mmm` (includes milliseconds)
- **Path Construction**: Appends timestamp to provided or default database path
- **Example**: `./test-db` becomes `./test-db_20241128_143052_123`

### Code Architecture
- **Modular Design**: `executeStatement()` function used by both script and interactive modes
- **File Processing**: `executeScriptFile()` handles file reading and statement parsing
- **Consistent Error Handling**: Same error reporting across all execution modes

## Use Cases

### Development Testing
```bash
# Test new features without affecting main database
./build/tundra_shell --script test_scripts/new_feature_test.sql --unique-db
```

### Automated Testing
```bash
# Run test suite with isolated databases
for script in test_scripts/*.sql; do
    ./build/tundra_shell --script "$script" --unique-db
done
```

### Interactive Development
```bash
# Set up test data then work interactively
./build/tundra_shell --script test_scripts/setup_data.sql --unique-db
# Continue with interactive commands...
tundra> MATCH (u:User) WHERE u.age > 25;
```

### Demo and Training
```bash
# Prepare demo environment
./build/tundra_shell --script demo_setup.sql --db-path ./demo-db
```

## Test Scripts Organization

All test scripts are organized in the `test_scripts/` folder:

- `simple_test.sql` - Basic functionality demonstration
- `delete_test.sql` - DELETE operations testing
- `join_test.sql` - JOIN types testing (INNER, LEFT, RIGHT, FULL)
- `where_test.sql` - WHERE clause operators testing
- `README.md` - Comprehensive documentation

## Error Handling

### Script Execution Errors
- Individual statement failures don't stop script execution
- Clear error messages with line numbers
- Final summary shows success/failure counts

### File Access Errors
- Clear error messages for missing files
- Graceful fallback to interactive mode
- Helpful usage suggestions

## Future Enhancements

1. **Batch Mode**: `--batch` flag to exit after script execution
2. **Output Redirection**: Save query results to files
3. **Variable Substitution**: Support for parameterized scripts
4. **Script Includes**: Support for including other script files
5. **Conditional Execution**: IF/ELSE logic in scripts
6. **Transaction Support**: Rollback on script failure

## Examples

### Quick Feature Test
```bash
# Test DELETE functionality in isolation
./build/tundra_shell --script test_scripts/delete_test.sql --unique-db
```

### Development Workflow
```bash
# Set up development environment
./build/tundra_shell --script test_scripts/dev_setup.sql --db-path ./dev-db

# Test specific feature
./build/tundra_shell --script test_scripts/feature_test.sql --unique-db

# Run comprehensive test suite
./build/tundra_shell --script test_scripts/full_test.sql --unique-db
```

This feature significantly improves the development and testing experience by providing automated script execution with database isolation capabilities. 