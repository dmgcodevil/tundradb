#!/bin/bash

# TundraDB Test Runner
# Runs all test scripts in detach mode with unique databases and output files

set -e  # Exit on any error

SCRIPT_DIR="."  # Current directory (test_scripts)
BINARY="../build/tundra_shell"  # Binary is in parent/build
BASE_DB_PATH="../build/test-db/test-db"  # Store test databases in build/test-db

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== TundraDB Test Runner ===${NC}"
echo "Running all test scripts with unique databases and output files"
echo ""

# Check if binary exists
if [ ! -f "$BINARY" ]; then
    echo -e "${RED}Error: Binary not found at $BINARY${NC}"
    echo "Please build the project first with 'make' or 'cmake --build build'"
    exit 1
fi

# Create build/test-db directory if it doesn't exist
mkdir -p "../build/test-db"

# Check if test scripts directory exists
if [ ! -d "$SCRIPT_DIR" ]; then
    echo -e "${RED}Error: Test scripts directory not found at $SCRIPT_DIR${NC}"
    exit 1
fi

# Clean up previous test databases
echo -e "${YELLOW}Cleaning up previous test databases...${NC}"
rm -rf "../build/test-db"
echo ""

# Find all .sql files in test_scripts directory
sql_files=($(find "$SCRIPT_DIR" -name "*.sql" -type f | sort))

if [ ${#sql_files[@]} -eq 0 ]; then
    echo -e "${YELLOW}Warning: No .sql files found in $SCRIPT_DIR${NC}"
    exit 0
fi

echo -e "${BLUE}Found ${#sql_files[@]} test scripts:${NC}"
for file in "${sql_files[@]}"; do
    echo "  - $(basename "$file")"
done
echo ""

# Initialize counters
total_tests=${#sql_files[@]}
passed_tests=0
failed_tests=0

# Run each test script
for sql_file in "${sql_files[@]}"; do
    script_name=$(basename "$sql_file" .sql)
    echo -e "${YELLOW}Running test: $script_name${NC}"
    
    # Generate timestamp for unique database
    timestamp=$(date +"%Y%m%d_%H%M%S_%3N")
    db_path="${BASE_DB_PATH}_${script_name}_${timestamp}"
    output_file="${db_path}/test_output.txt"
    
    # Create database directory to ensure output file can be created
    mkdir -p "$db_path"
    
    echo "  Database: $db_path"
    echo "  Output:   $output_file"
    
    # Run the test
    if "$BINARY" --script "$sql_file" --db-path "$db_path" --output "$output_file" --detach; then
        echo -e "  ${GREEN}✓ PASSED${NC}"
        ((passed_tests++))
        
        # Show output file size
        if [ -f "$output_file" ]; then
            file_size=$(wc -l < "$output_file")
            echo "  Output file: $file_size lines"
        fi
    else
        echo -e "  ${RED}✗ FAILED${NC}"
        ((failed_tests++))
        
        # Show last few lines of output if available
        if [ -f "$output_file" ]; then
            echo "  Last 5 lines of output:"
            tail -5 "$output_file" | sed 's/^/    /'
        fi
    fi
    
    echo ""
done

# Summary
echo -e "${BLUE}=== Test Summary ===${NC}"
echo "Total tests: $total_tests"
echo -e "Passed: ${GREEN}$passed_tests${NC}"
echo -e "Failed: ${RED}$failed_tests${NC}"

if [ $failed_tests -eq 0 ]; then
    echo -e "${GREEN}All tests passed! 🎉${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed. Check the output files for details.${NC}"
    exit 1
fi 