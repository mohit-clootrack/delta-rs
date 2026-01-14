import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
import tempfile
import shutil

def test_schema_mode_overwrite():
    """Test schema_mode='overwrite' functionality"""
    temp_dir = tempfile.mkdtemp()
    print(f'Testing schema_mode="overwrite" in: {temp_dir}')
    
    try:
        # Initial table with some columns
        initial_data = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        print('Initial schema:', initial_data.schema)
        
        write_deltalake(temp_dir, initial_data, mode='overwrite')
        print('Initial table written successfully')
        
        dt = DeltaTable(temp_dir)
        print('Schema after initial write:')
        schema_fields = list(dt.schema().to_arrow())
        for field in schema_fields:
            print(f'  {field.name}: {field.type}')
        
        # New data with completely different schema
        new_data = pa.table({
            'user_id': [10, 20, 30],
            'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com'],
            'active': [True, False, True]
        })
        print('\nNew schema for overwrite (completely different):', new_data.schema)
        
        print('\n--- Testing OVERWRITE with schema_mode="overwrite" ---')
        write_deltalake(temp_dir, new_data, mode='overwrite', schema_mode='overwrite')
        print('Schema overwrite completed successfully!')
        
        dt = DeltaTable(temp_dir)
        print('Final table schema after schema overwrite:')
        schema_fields = list(dt.schema().to_arrow())
        for field in schema_fields:
            print(f'  {field.name}: {field.type}')
        
        final_data = dt.to_pandas()
        print(f'\nTotal rows: {len(final_data)}')
        print('Final data:')
        print(final_data)
        
        # Verify the schema was completely replaced
        expected_columns = {'user_id', 'email', 'active'}
        actual_columns = set(final_data.columns)
        if actual_columns == expected_columns:
            print('✅ Schema overwrite successful - old schema completely replaced!')
        else:
            print(f'❌ Schema overwrite failed. Expected: {expected_columns}, Got: {actual_columns}')
            
    except Exception as e:
        print(f'Error during schema overwrite test: {e}')
        import traceback
        traceback.print_exc()
    finally:
        shutil.rmtree(temp_dir)

def test_regular_overwrite():
    """Test regular overwrite functionality (same schema)"""
    temp_dir = tempfile.mkdtemp()
    print(f'\nTesting regular overwrite in: {temp_dir}')
    
    try:
        # Initial table
        initial_data = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300]
        })
        print('Initial schema:', initial_data.schema)
        
        write_deltalake(temp_dir, initial_data, mode='overwrite')
        print('Initial table written successfully')
        
        dt = DeltaTable(temp_dir)
        initial_version = dt.version()
        print(f'Initial version: {initial_version}')
        print('Initial data:')
        print(dt.to_pandas())
        
        # New data with same schema but different values
        new_data = pa.table({
            'id': [4, 5, 6, 7],
            'name': ['David', 'Eve', 'Frank', 'Grace'],
            'value': [400, 500, 600, 700]
        })
        print('\nNew data (same schema, different values):', new_data.schema)
        
        print('\n--- Testing regular OVERWRITE (same schema) ---')
        write_deltalake(temp_dir, new_data, mode='overwrite')
        print('Regular overwrite completed successfully!')
        
        dt = DeltaTable(temp_dir)
        final_version = dt.version()
        print(f'Final version: {final_version}')
        
        final_data = dt.to_pandas()
        print(f'Total rows: {len(final_data)}')
        print('Final data:')
        print(final_data)
        
        # Verify the data was completely replaced
        if len(final_data) == 4 and final_data['id'].tolist() == [4, 5, 6, 7]:
            print('✅ Regular overwrite successful - old data completely replaced!')
        else:
            print('❌ Regular overwrite failed - old data not properly replaced')
            
    except Exception as e:
        print(f'Error during regular overwrite test: {e}')
        import traceback
        traceback.print_exc()
    finally:
        shutil.rmtree(temp_dir)

def test_overwrite_with_schema_evolution():
    """Test overwrite with schema evolution (adding columns)"""
    temp_dir = tempfile.mkdtemp()
    print(f'\nTesting overwrite with schema evolution in: {temp_dir}')
    
    try:
        # Initial table
        initial_data = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        print('Initial schema:', initial_data.schema)
        
        write_deltalake(temp_dir, initial_data, mode='overwrite')
        print('Initial table written successfully')
        
        # New data with additional columns
        new_data = pa.table({
            'id': [4, 5, 6],
            'name': ['David', 'Eve', 'Frank'],
            'age': [25, 30, 35],
            'city': ['New York', 'London', 'Tokyo']
        })
        print('\nNew schema (with additional columns):', new_data.schema)
        
        print('\n--- Testing OVERWRITE with schema_mode="merge" (adding columns) ---')
        write_deltalake(temp_dir, new_data, mode='overwrite', schema_mode='merge')
        print('Overwrite with schema merge completed successfully!')
        
        dt = DeltaTable(temp_dir)
        print('Final table schema:')
        schema_fields = list(dt.schema().to_arrow())
        for field in schema_fields:
            print(f'  {field.name}: {field.type}')
        
        final_data = dt.to_pandas()
        print(f'Total rows: {len(final_data)}')
        print('Final data:')
        print(final_data)
        
        # Verify schema was expanded and data replaced
        expected_columns = {'id', 'name', 'age', 'city'}
        actual_columns = set(final_data.columns)
        if actual_columns == expected_columns and len(final_data) == 3:
            print('✅ Overwrite with schema merge successful!')
        else:
            print(f'❌ Overwrite with schema merge failed. Expected columns: {expected_columns}, Got: {actual_columns}')
            
    except Exception as e:
        print(f'Error during overwrite with schema evolution test: {e}')
        import traceback
        traceback.print_exc()
    finally:
        shutil.rmtree(temp_dir)

if __name__ == "__main__":
    print("🧪 Testing Delta Lake Schema and Overwrite Modes")
    print("=" * 50)
    
    test_schema_mode_overwrite()
    test_regular_overwrite()
    test_overwrite_with_schema_evolution()
    
    print("\n🎉 All tests completed!") 