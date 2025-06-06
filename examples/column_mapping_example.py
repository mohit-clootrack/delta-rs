#!/usr/bin/env python3
"""
Example showing how to use column mapping features in delta-rs.

Column mapping allows for schema evolution operations like renaming or dropping columns
without breaking downstream readers.
"""

import os
import shutil
import tempfile
from deltalake import DeltaTable, write_deltalake, ColumnMappingMode

# Create a temporary directory for the example
tmp_path = tempfile.mkdtemp()
table_path = os.path.join(tmp_path, "example_table")

try:
    # Create a simple table
    data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35}
    ]
    
    # Write initial data
    write_deltalake(table_path, data)
    
    # Open the table
    dt = DeltaTable(table_path)
    
    print("Initial schema:")
    print(dt.schema().to_pyarrow())
    
    # Enable column mapping with Name mode
    dt.column_mapping(mode=ColumnMappingMode.Name)
    print("\nColumn mapping enabled")
    
    # Read the updated table to see the column mapping metadata
    dt = DeltaTable(table_path)
    print("\nSchema after enabling column mapping:")
    print(dt.schema().to_pyarrow())
    
    # Rename a column
    dt.column_mapping(rename_columns=[("name", "full_name")])
    print("\nRenamed 'name' to 'full_name'")
    
    # Read the updated table
    dt = DeltaTable(table_path)
    print("\nSchema after renaming:")
    print(dt.schema().to_pyarrow())
    
    # Drop a column
    dt.column_mapping(drop_columns=["age"])
    print("\nDropped 'age' column")
    
    # Read the updated table
    dt = DeltaTable(table_path)
    print("\nSchema after dropping column:")
    print(dt.schema().to_pyarrow())
    
    # Write to the table with the new schema
    new_data = [
        {"id": 4, "full_name": "Dave"},
        {"id": 5, "full_name": "Eve"}
    ]
    
    write_deltalake(table_path, new_data, mode="append")
    print("\nAppended new data with new schema")
    
    # Read all data from the table
    df = dt.to_pandas()
    print("\nFinal table contents:")
    print(df)

finally:
    # Clean up
    shutil.rmtree(tmp_path) 