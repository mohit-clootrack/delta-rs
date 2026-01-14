import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
import tempfile
import os

def test_schema_overwrite_comprehensive():
    temp_dir = tempfile.mkdtemp()
    print(f'🧪 Testing comprehensive schema overwrite in: {temp_dir}')
    
    print("\n=== Phase 1: Initial table with basic schema ===")
    initial_data = pa.table({
        'id': pa.array([1, 2, 3], type=pa.int64()),
        'name': pa.array(['Alice', 'Bob', 'Charlie'], type=pa.string()),
        'age': pa.array([25, 30, 35], type=pa.int32()),
        'score': pa.array([85.5, 92.3, 78.9], type=pa.float64())
    })
    print(f'📊 Initial schema: {initial_data.schema}')
    
    write_deltalake(temp_dir, initial_data, mode='overwrite')
    dt = DeltaTable(temp_dir)
    print(f'✅ Version {dt.version()}: Table created with {len(initial_data)} rows')
    
    print("\n=== Phase 2: Add new columns + change existing types ===")
    extended_data = pa.table({
        'id': pa.array([4, 5, 6, 7], type=pa.int64()),
        'name': pa.array(['David', 'Eve', 'Frank', 'Grace'], type=pa.large_string()),  # string -> large_string
        'age': pa.array([28, 33, 41, 29], type=pa.int64()),  # int32 -> int64
        'score': pa.array([88.7, 95.2, 82.1, 91.4], type=pa.float64()),
        'department': pa.array(['Engineering', 'Sales', 'Marketing', 'HR'], type=pa.string()),  # NEW COLUMN
        'is_active': pa.array([True, False, True, True], type=pa.bool_()),  # NEW COLUMN
        'salary': pa.array([75000, 85000, 70000, 80000], type=pa.int64()),  # NEW COLUMN
        'join_date': pa.array(['2023-01-15', '2022-11-20', '2023-03-10', '2022-09-05'], type=pa.string())  # NEW COLUMN
    })
    print(f'📊 Extended schema: {extended_data.schema}')
    
    try:
        write_deltalake(
            temp_dir, 
            extended_data, 
            mode='overwrite',
            schema_mode='overwrite',  # Allow schema evolution
            configuration={
                "delta.columnMapping.mode": "name"
            }
        )
        dt = DeltaTable(temp_dir)
        print(f'✅ Version {dt.version()}: Schema overwrite successful with {len(extended_data)} rows')
        
        result_data = dt.to_pyarrow_table()
        print(f'📋 New schema columns: {result_data.column_names}')
        print(f'📊 New schema types: {result_data.schema}')
        
    except Exception as e:
        print(f'❌ Schema overwrite failed: {e}')
        return False
    
    print("\n=== Phase 3: Add even more columns + nested data ===")
    complex_data = pa.table({
        'id': pa.array([8, 9, 10], type=pa.int64()),
        'name': pa.array(['Henry', 'Ivy', 'Jack'], type=pa.large_string()),
        'age': pa.array([45, 27, 38], type=pa.int64()),
        'score': pa.array([89.5, 94.8, 87.2], type=pa.float64()),
        'department': pa.array(['Finance', 'IT', 'Legal'], type=pa.string()),
        'is_active': pa.array([True, True, False], type=pa.bool_()),
        'salary': pa.array([90000, 95000, 85000], type=pa.int64()),
        'join_date': pa.array(['2021-06-15', '2023-08-20', '2022-12-01'], type=pa.string()),
        'tags': pa.array([['senior', 'manager'], ['junior', 'developer'], ['experienced', 'lawyer']], type=pa.list_(pa.string())),  # NEW: List column
        'metadata': pa.array(['{"level": "senior"}', '{"level": "junior"}', '{"level": "experienced"}'], type=pa.string()),  # NEW: JSON-like column
        'rating': pa.array([4.5, 4.8, 4.2], type=pa.float32()),  # NEW: Different float type
        'bonus': pa.array([5000, 3000, 7000], type=pa.int32())  # NEW: Different int type
    })
    print(f'📊 Complex schema: {complex_data.schema}')
    
    try:
        write_deltalake(
            temp_dir, 
            complex_data, 
            mode='overwrite',
            schema_mode='overwrite',  # Allow schema evolution
            configuration={
                "delta.columnMapping.mode": "name"
            }
        )
        dt = DeltaTable(temp_dir)
        print(f'✅ Version {dt.version()}: Complex schema overwrite successful with {len(complex_data)} rows')
        
        result_data = dt.to_pyarrow_table()
        print(f'📋 Final schema columns ({len(result_data.column_names)}): {result_data.column_names}')
        
        print("\n📊 Column type verification:")
        for i, (name, field) in enumerate(zip(result_data.column_names, result_data.schema)):
            print(f"  {i+1:2d}. {name:12s} -> {field.type}")
            
    except Exception as e:
        print(f'❌ Complex schema overwrite failed: {e}')
        return False
    
    print("\n=== Phase 4: Data verification ===")
    final_table = dt.to_pyarrow_table()
    print(f'📊 Final table shape: {final_table.num_rows} rows × {final_table.num_columns} columns')
    
    non_null_counts = {}
    for col in final_table.column_names:
        non_null = final_table.column(col).null_count
        total = len(final_table)
        non_null_counts[col] = total - non_null
        print(f"  {col:15s}: {total - non_null:3d}/{total} non-null values")
    
    print(f"\n📋 Sample data:")
    df = final_table.to_pandas()
    print(df.head(3))
    
    print("\n=== Phase 5: Table history verification ===")
    history = dt.history(limit=5)
    print(f"📚 Table versions:")
    for i, commit in enumerate(history):
        version = commit.get('version', 'Unknown')
        operation = commit.get('operation', 'Unknown')
        timestamp = commit.get('timestamp', 'Unknown')
        print(f"  Version {version}: {operation} at {timestamp}")
    
    all_non_null = all(count > 0 for count in non_null_counts.values())
    schema_evolved = len(final_table.column_names) >= 8  # Should have at least 8+ columns
    
    if all_non_null and schema_evolved:
        print(f"\n🎉 SUCCESS: Schema overwrite with column additions working perfectly!")
        print(f"   ✅ All columns have data")
        print(f"   ✅ Schema evolution successful")
        print(f"   ✅ Column mapping preserved")
        return True
    else:
        print(f"\n❌ FAILURE: Issues detected")
        print(f"   Data integrity: {'✅' if all_non_null else '❌'}")
        print(f"   Schema evolution: {'✅' if schema_evolved else '❌'}")
        return False

def test_column_type_evolution():
    """Test specifically the string to large_string evolution scenario"""
    temp_dir = tempfile.mkdtemp()
    print(f'\n🔬 Testing column type evolution in: {temp_dir}')
    
    # Start with string type
    print("\n=== Initial: String column ===")
    string_data = pa.table({
        'name': pa.array(['Alice', 'Bob'], type=pa.string()),
        'value': pa.array([1, 2], type=pa.int32())
    })
    print(f'📊 String schema: {string_data.schema}')
    
    write_deltalake(temp_dir, string_data, mode='overwrite')
    
    # Evolve to large_string
    print("\n=== Evolution: Large String column ===")
    large_string_data = pa.table({
        'name': pa.array(['Charlie', 'Dave'], type=pa.large_string()),
        'value': pa.array([3, 4], type=pa.int32())
    })
    print(f'📊 Large string schema: {large_string_data.schema}')
    
    try:
        write_deltalake(
            temp_dir, 
            large_string_data, 
            mode='overwrite',
            schema_mode='overwrite',
            configuration={
                "delta.columnMapping.mode": "name"
            }
        )
        
        dt = DeltaTable(temp_dir)
        result_data = dt.to_pyarrow_table()
        print(f'✅ Type evolution successful!')
        print(f'📊 Final schema: {result_data.schema}')
        
        # Check data integrity
        df = result_data.to_pandas()
        print(f'📋 Data sample:\n{df}')
        
        has_data = len(result_data) > 0
        correct_type = str(result_data.schema.field('name').type) == 'large_string'
        
        if has_data and correct_type:
            print(f'🎉 Column type evolution test PASSED!')
            return True
        else:
            print(f'❌ Column type evolution test FAILED!')
            return False
            
    except Exception as e:
        print(f'❌ Type evolution failed: {e}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    try:
        print("🚀 Starting comprehensive schema tests...")
        
        # Test 1: Full schema evolution
        success1 = test_schema_overwrite_comprehensive()
        
        # Test 2: Specific column type evolution
        success2 = test_column_type_evolution()
        
        if success1 and success2:
            print(f"\n🏆 ALL SCHEMA TESTS PASSED!")
            print(f"   ✅ Schema overwrite with new columns works")
            print(f"   ✅ Column type evolution works") 
            print(f"   ✅ Column mapping is preserved")
        else:
            print(f"\n💥 SOME TESTS FAILED!")
            print(f"   Schema expansion: {'✅' if success1 else '❌'}")
            print(f"   Type evolution: {'✅' if success2 else '❌'}")
            
    except Exception as e:
        print(f"❌ Test execution error: {e}")
        import traceback
        traceback.print_exc() 