#!/bin/bash
set -e

# Create a test directory
TEST_DIR=$(mktemp -d)
TABLE_PATH="$TEST_DIR/column_mapping_test"

echo "Testing column mapping feature with table at $TABLE_PATH"

# Ensure we clean up on exit
function cleanup {
  echo "Cleaning up $TEST_DIR"
  rm -rf "$TEST_DIR"
}
trap cleanup EXIT

# Build the Delta Rust CLI tool
cd "$(dirname "$0")"
echo "Building Delta CLI tool..."
cargo build --bin delta-inspect

CLI_PATH="./target/debug/delta-inspect"

# Create a Delta table with column mapping enabled
echo -e "\nCreating table with column mapping..."
cat > "$TEST_DIR/data.json" << EOF
{"id": 1, "name": "Alice", "age": 30, "city": "New York"}
{"id": 2, "name": "Bob", "age": 25, "city": "San Francisco"}
{"id": 3, "name": "Charlie", "age": 35, "city": "Seattle"}
EOF

$CLI_PATH create \
  --min-reader-version 2 \
  --min-writer-version 5 \
  --column-mapping name \
  --table-location "$TABLE_PATH" \
  "$TEST_DIR/data.json"

# Verify the table was created correctly
echo -e "\nInitial table schema:"
$CLI_PATH schema "$TABLE_PATH"

# Check the table properties to verify column mapping
echo -e "\nTable properties:"
$CLI_PATH info "$TABLE_PATH" | grep -E "delta\.column|min.*Version"

# Rename a column
echo -e "\nRenaming column 'name' to 'full_name'..."
$CLI_PATH column-mapping \
  --rename "name:full_name" \
  "$TABLE_PATH"

# Verify schema after rename
echo -e "\nSchema after renaming 'name' to 'full_name':"
$CLI_PATH schema "$TABLE_PATH"

# Drop a column
echo -e "\nDropping column 'age'..."
$CLI_PATH column-mapping \
  --drop "age" \
  "$TABLE_PATH"

# Verify schema after drop
echo -e "\nSchema after dropping 'age':"
$CLI_PATH schema "$TABLE_PATH"

# Create new data with updated schema
echo -e "\nWriting new data with updated schema..."
cat > "$TEST_DIR/new_data.json" << EOF
{"id": 4, "full_name": "Dave", "city": "Chicago"}
{"id": 5, "full_name": "Eve", "city": "Boston"}
EOF

# Append the new data
$CLI_PATH write \
  --mode append \
  --table-location "$TABLE_PATH" \
  "$TEST_DIR/new_data.json"

# Verify all data
echo -e "\nFinal table data:"
$CLI_PATH table "$TABLE_PATH"

# Show history
echo -e "\nTable history:"
$CLI_PATH history "$TABLE_PATH"

echo -e "\nColumn mapping test completed successfully!" 