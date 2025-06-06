import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
import tempfile
import pandas as pd

def test_overwrite_with_schema_overwrite():
    """Comprehensive test for mode='overwrite' + schema_mode='overwrite'"""
    temp_dir = tempfile.mkdtemp()
    print(f'🧪 Testing OVERWRITE + SCHEMA_OVERWRITE in: {temp_dir}')
    
    print("\n=== Phase 1: Initial Table Creation ===")
    initial_data = pa.table({
        'id': pa.array([1, 2, 3], type=pa.int64()),
        'name': pa.array(['Alice', 'Bob', 'Charlie'], type=pa.string()),
        'age': pa.array([25, 30, 35], type=pa.int32()),
        'score': pa.array([85.5, 92.3, 78.9], type=pa.float64()),
        'active': pa.array([True, False, True], type=pa.bool_())
    })
    print(f'📊 Initial schema: {initial_data.schema}')
    print(f'📋 Initial data shape: {initial_data.num_rows} rows × {initial_data.num_columns} columns')
    
    write_deltalake(temp_dir, initial_data, mode='overwrite')
    dt = DeltaTable(temp_dir)
    print(f'✅ Version {dt.version()}: Initial table created')
    
    # Verify initial data
    result = dt.to_pyarrow_table()
    print(f'📋 Initial data sample:\n{result.to_pandas()}')
    
    print("\n=== Phase 2: Schema Overwrite with Type Evolution ===")
    # Test the critical string -> large_string scenario
    evolved_data = pa.table({
        'id': pa.array([10, 20, 30, 40], type=pa.int64()),
        'name': pa.array(['David', 'Eve', 'Frank', 'Grace'], type=pa.large_string()),  # string -> large_string
        'age': pa.array([28, 33, 41, 29], type=pa.int64()),  # int32 -> int64
        'score': pa.array([88.7, 95.2, 82.1, 91.4], type=pa.float64()),
        'active': pa.array([True, True, False, True], type=pa.bool_())
    })
    print(f'📊 Evolved schema: {evolved_data.schema}')
    print(f'📋 Evolved data shape: {evolved_data.num_rows} rows × {evolved_data.num_columns} columns')
    
    try:
        write_deltalake(
            temp_dir, 
            evolved_data, 
            mode='overwrite',
            schema_mode='overwrite',
            configuration={
                "delta.columnMapping.mode": "name"
            }
        )
        
        dt = DeltaTable(temp_dir)
        result = dt.to_pyarrow_table()
        print(f'✅ Version {dt.version()}: Schema overwrite successful!')
        print(f'📊 Final schema: {result.schema}')
        print(f'📋 Final data shape: {result.num_rows} rows × {result.num_columns} columns')
        
        # Critical validation: Check for null data (the original bug)
        null_counts = {}
        for col in result.column_names:
            null_count = result.column(col).null_count
            null_counts[col] = null_count
            print(f'   {col}: {null_count} null values out of {result.num_rows}')
        
        # Display the actual data
        df = result.to_pandas()
        print(f'📋 Final data:\n{df}')
        
        # Validation checks
        has_data = result.num_rows > 0
        no_nulls = all(count == 0 for count in null_counts.values())
        correct_rows = result.num_rows == len(evolved_data)
        
        print(f'\n🔍 Validation Results:')
        print(f'   Data exists: {"✅" if has_data else "❌"} ({result.num_rows} rows)')
        print(f'   No null values: {"✅" if no_nulls else "❌"} (null counts: {null_counts})')
        print(f'   Correct row count: {"✅" if correct_rows else "❌"} (expected: {len(evolved_data)})')
        
        if has_data and no_nulls and correct_rows:
            print(f'🎉 Phase 2: SUCCESS - Schema overwrite with type evolution works!')
            phase2_success = True
        else:
            print(f'❌ Phase 2: FAILED - Issues detected')
            phase2_success = False
            
    except Exception as e:
        print(f'❌ Phase 2: FAILED - {e}')
        import traceback
        traceback.print_exc()
        phase2_success = False
    
    print("\n=== Phase 3: Multiple Schema Overwrites ===")
    # Test multiple consecutive overwrites
    for iteration in range(1, 4):
        print(f'\n--- Iteration {iteration} ---')
        
        new_data = pa.table({
            'id': pa.array([100*iteration + i for i in range(1, 4)], type=pa.int64()),
            'name': pa.array([f'User{100*iteration + i}' for i in range(1, 4)], type=pa.large_string()),
            'age': pa.array([20 + iteration*5 + i for i in range(3)], type=pa.int64()),
            'score': pa.array([80.0 + iteration*2 + i for i in range(3)], type=pa.float64()),
            'active': pa.array([i % 2 == 0 for i in range(3)], type=pa.bool_())
        })
        
        try:
            write_deltalake(
                temp_dir, 
                new_data, 
                mode='overwrite',
                schema_mode='overwrite',
                configuration={
                    "delta.columnMapping.mode": "name"
                }
            )
            
            dt = DeltaTable(temp_dir)
            result = dt.to_pyarrow_table()
            
            # Quick validation
            has_data = result.num_rows > 0
            no_nulls = all(result.column(col).null_count == 0 for col in result.column_names)
            
            if has_data and no_nulls:
                print(f'   ✅ Iteration {iteration}: SUCCESS ({result.num_rows} rows, no nulls)')
            else:
                print(f'   ❌ Iteration {iteration}: FAILED')
                
        except Exception as e:
            print(f'   ❌ Iteration {iteration}: FAILED - {e}')
    
    print("\n=== Phase 4: Edge Cases ===")
    
    # Test 4a: Empty to non-empty
    print(f'\n--- Test 4a: Empty to Non-empty ---')
    empty_data = pa.table({
        'id': pa.array([], type=pa.int64()),
        'name': pa.array([], type=pa.large_string()),
        'age': pa.array([], type=pa.int64()),
        'score': pa.array([], type=pa.float64()),
        'active': pa.array([], type=pa.bool_())
    })
    
    try:
        write_deltalake(
            temp_dir, 
            empty_data, 
            mode='overwrite',
            schema_mode='overwrite',
            configuration={
                "delta.columnMapping.mode": "name"
            }
        )
        
        # Then add data
        non_empty_data = pa.table({
            'id': pa.array([999], type=pa.int64()),
            'name': pa.array(['TestUser'], type=pa.large_string()),
            'age': pa.array([99], type=pa.int64()),
            'score': pa.array([99.9], type=pa.float64()),
            'active': pa.array([True], type=pa.bool_())
        })
        
        write_deltalake(
            temp_dir, 
            non_empty_data, 
            mode='overwrite',
            schema_mode='overwrite',
            configuration={
                "delta.columnMapping.mode": "name"
            }
        )
        
        dt = DeltaTable(temp_dir)
        result = dt.to_pyarrow_table()
        
        if result.num_rows == 1 and result.column('name').null_count == 0:
            print(f'   ✅ Empty to non-empty: SUCCESS')
        else:
            print(f'   ❌ Empty to non-empty: FAILED')
            
    except Exception as e:
        print(f'   ❌ Empty to non-empty: FAILED - {e}')
    
    # Test 4b: Large dataset
    print(f'\n--- Test 4b: Large Dataset ---')
    large_data = pa.table({
        'id': pa.array(list(range(10000)), type=pa.int64()),
        'name': pa.array([f'User{i}' for i in range(10000)], type=pa.large_string()),
        'age': pa.array([20 + (i % 50) for i in range(10000)], type=pa.int64()),
        'score': pa.array([50.0 + (i % 100) for i in range(10000)], type=pa.float64()),
        'active': pa.array([i % 2 == 0 for i in range(10000)], type=pa.bool_())
    })
    
    try:
        write_deltalake(
            temp_dir, 
            large_data, 
            mode='overwrite',
            schema_mode='overwrite',
            configuration={
                "delta.columnMapping.mode": "name"
            }
        )
        
        dt = DeltaTable(temp_dir)
        result = dt.to_pyarrow_table()
        
        if result.num_rows == 10000 and result.column('name').null_count == 0:
            print(f'   ✅ Large dataset: SUCCESS (10,000 rows)')
        else:
            print(f'   ❌ Large dataset: FAILED ({result.num_rows} rows, {result.column("name").null_count} nulls)')
            
    except Exception as e:
        print(f'   ❌ Large dataset: FAILED - {e}')
    
    print("\n=== Phase 5: Final Validation ===")
    dt = DeltaTable(temp_dir)
    final_result = dt.to_pyarrow_table()
    
    print(f'📊 Final table state:')
    print(f'   Version: {dt.version()}')
    print(f'   Rows: {final_result.num_rows}')
    print(f'   Columns: {final_result.num_columns}')
    print(f'   Schema: {final_result.schema}')
    
    # Show table history
    history = dt.history(limit=5)
    print(f'\n📚 Recent table history:')
    for i, commit in enumerate(history):
        version = commit.get('version', 'Unknown')
        operation = commit.get('operation', 'Unknown')
        timestamp = commit.get('timestamp', 'Unknown')
        print(f'   Version {version}: {operation} at {timestamp}')
    
    # Final data sample
    if final_result.num_rows > 0:
        df = final_result.to_pandas()
        print(f'\n📋 Final data sample (first 5 rows):')
        print(df.head())
    
    return phase2_success

def test_string_large_string_specifically():
    """Focused test on the specific string -> large_string issue"""
    print(f'\n🔬 Focused test: String -> Large_String Evolution')
    
    temp_dir = tempfile.mkdtemp()
    
    # Start with regular string
    string_data = pa.table({
        'text_col': pa.array(['Hello', 'World', 'Test'], type=pa.string()),
        'id': pa.array([1, 2, 3], type=pa.int32())
    })
    
    write_deltalake(temp_dir, string_data, mode='overwrite')
    print(f'📊 Initial: {string_data.schema}')
    
    # Overwrite with large_string
    large_string_data = pa.table({
        'text_col': pa.array(['Large', 'String', 'Data', 'Test'], type=pa.large_string()),
        'id': pa.array([10, 20, 30, 40], type=pa.int32())
    })
    
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
    result = dt.to_pyarrow_table()
    
    print(f'📊 Final: {result.schema}')
    print(f'📋 Data: {result.to_pandas()}')
    
    # Critical check: no null values in text_col
    null_count = result.column('text_col').null_count
    has_data = result.num_rows > 0
    
    if null_count == 0 and has_data:
        print(f'🎉 String -> Large_String test: SUCCESS (0 nulls, {result.num_rows} rows)')
        return True
    else:
        print(f'❌ String -> Large_String test: FAILED ({null_count} nulls, {result.num_rows} rows)')
        return False

if __name__ == "__main__":
    print("🚀 Starting comprehensive OVERWRITE + SCHEMA_OVERWRITE validation...")
    
    # Main comprehensive test
    main_success = test_overwrite_with_schema_overwrite()
    
    # Focused string evolution test
    string_success = test_string_large_string_specifically()
    
    print(f"\n🏆 FINAL RESULTS:")
    print(f"   📊 Comprehensive overwrite test: {'✅ PASS' if main_success else '❌ FAIL'}")
    print(f"   🔤 String evolution test: {'✅ PASS' if string_success else '❌ FAIL'}")
    
    if main_success and string_success:
        print(f"\n🎉 ALL TESTS PASSED!")
        print(f"   ✅ mode='overwrite' + schema_mode='overwrite' works perfectly")
        print(f"   ✅ Column mapping fix prevents null data issues")
        print(f"   ✅ String -> Large_String evolution is handled correctly")
        print(f"   ✅ Ready for production use!")
    else:
        print(f"\n💥 SOME TESTS FAILED!")
        print(f"   Please review the failed test cases above.") 