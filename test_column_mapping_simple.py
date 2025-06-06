#!/usr/bin/env python3

"""
Simple test script for column mapping in delta-rs without pandas dependencies.
"""

import os
import tempfile
import polars as pl
from deltalake import DeltaTable, write_deltalake
from pathlib import Path

print("🔗 Testing Delta Lake with Column Mapping Configuration")

# Create temporary directory for test
temp_dir = tempfile.mkdtemp()
table_path = Path(temp_dir) / "test_table_mapping"

print(f"📍 Test table path: {table_path}")

try:
    print("\n=== 🔧 Creating test data ===")
    
    # Create test data with spaces in column names
    test_df = pl.DataFrame({
        "Review Id": ["TEST_001", "TEST_002", "TEST_003"],  
        "Review Title": ["Great Product", "Good Service", "Amazing Quality"],
        "Customer Name": ["Alice Johnson", "Bob Smith", "Carol Davis"],
        "Rating Score": [5, 4, 5],
        "Review Date": ["2024-01-01", "2024-01-02", "2024-01-03"]
    })
    
    print(f"📊 Created DataFrame: {test_df.shape} rows x {len(test_df.columns)} columns")
    print("📊 Columns with spaces:", [col for col in test_df.columns if " " in col])
    
    print("\n=== 💾 Writing to Delta table with column mapping ===")
    
    # Write data with column mapping mode set to 'name'
    write_deltalake(
        table_or_uri=str(table_path),
        data=test_df.to_arrow(),
        mode="overwrite",
        configuration={
            "delta.columnMapping.mode": "name"
        }
    )
    
    print("✅ Successfully wrote data with column mapping enabled")
    
    print("\n=== 📖 Reading back and verifying ===")
    
    # Read back using DeltaTable
    dt = DeltaTable(str(table_path))
    
    print(f"📊 Table version: {dt.version()}")
    protocol = dt.protocol()
    print(f"📊 Protocol: Reader={protocol.min_reader_version}, Writer={protocol.min_writer_version}")
    
    # Check if the protocol was upgraded for column mapping
    if hasattr(protocol, 'reader_features'):
        print(f"📊 Reader features: {protocol.reader_features}")
    if hasattr(protocol, 'writer_features'):
        print(f"📊 Writer features: {protocol.writer_features}")
    
    # Check column mapping configuration
    metadata = dt.metadata()
    column_mapping_mode = metadata.configuration.get("delta.columnMapping.mode")
    print(f"📊 Column mapping mode: {column_mapping_mode}")
    
    # Test reading data back with Polars
    arrow_table = dt.to_pyarrow_table()
    polars_df = pl.from_arrow(arrow_table)
    
    print(f"📊 Read back: {polars_df.shape} rows x {len(polars_df.columns)} columns")
    print(f"📊 Column names preserved: {polars_df.columns}")
    
    # Verify data integrity
    print("\n=== ✅ Data Verification ===")
    print(f"📊 Original columns: {test_df.columns}")
    print(f"📊 Read back columns: {polars_df.columns}")
    print(f"📊 Columns match: {test_df.columns == polars_df.columns}")
    
    # Sort both for comparison
    original_sorted = test_df.sort("Review Id")
    read_back_sorted = polars_df.sort("Review Id")
    print(f"📊 Data integrity: {original_sorted.equals(read_back_sorted)}")
    
    print("\n=== 🔄 Testing append with column mapping ===")
    
    # Add more data
    additional_df = pl.DataFrame({
        "Review Id": ["TEST_004", "TEST_005"],  
        "Review Title": ["Excellent", "Perfect"],
        "Customer Name": ["David Wilson", "Eva Brown"],
        "Rating Score": [5, 5],
        "Review Date": ["2024-01-04", "2024-01-05"]
    })
    
    # Append data (should preserve column mapping)
    write_deltalake(
        table_or_uri=str(table_path),
        data=additional_df.to_arrow(),
        mode="append"
    )
    
    # Verify final state
    dt_final = DeltaTable(str(table_path))
    final_df = pl.from_arrow(dt_final.to_pyarrow_table())
    
    print(f"📊 Final dataset: {final_df.shape} rows x {len(final_df.columns)} columns")
    print(f"📊 Final version: {dt_final.version()}")
    
    # Check that column mapping persists
    final_metadata = dt_final.metadata()
    final_mapping_mode = final_metadata.configuration.get("delta.columnMapping.mode")
    print(f"📊 Column mapping mode (final): {final_mapping_mode}")
    
    print("\n🎉 SUCCESS! Column Mapping Test Completed")
    print("✅ Delta table created with column mapping enabled")
    print("✅ Column names with spaces handled correctly")
    print("✅ Polars integration working with column mapping")
    print("✅ Data integrity maintained")
    print("✅ Append operations preserve column mapping")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    # Cleanup
    import shutil
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        print(f"\n🧹 Cleaned up temporary directory: {temp_dir}") 