import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
import tempfile
import shutil

temp_dir = tempfile.mkdtemp()
print(f'Using temp directory: {temp_dir}')

try:
    initial_data = pa.table({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie']
    })
    print('Initial schema:', initial_data.schema)
    
    write_deltalake(temp_dir, initial_data, mode='overwrite')
    print('Initial table written successfully')
    
    dt = DeltaTable(temp_dir)
    print('Schema after initial write:')
    schema_fields = list(dt.schema().to_arrow())
    for field in schema_fields:
        print(f'  {field.name}: {field.type}')
    
    new_data = pa.table({
        'id': [4, 5, 6],
        'name': ['David', 'Eve', 'Frank'],
        'age': [25, 30, 35],
        'city': ['New York', 'London', 'Tokyo']
    })
    print('\nNew schema for overwrite:', new_data.schema)
    
    try:
        print('\n--- Testing OVERWRITE with schema_mode="merge" ---')
        write_deltalake(temp_dir, new_data, mode='overwrite', schema_mode='merge')
        print('Schema merge with overwrite completed successfully!')
        
        dt = DeltaTable(temp_dir)
        print('Final table schema after overwrite:')
        schema_fields = list(dt.schema().to_arrow())
        for field in schema_fields:
            print(f'  {field.name}: {field.type}')
        
        final_data = dt.to_pandas()
        print(f'\nTotal rows: {len(final_data)}')
        print('Final data:')
        print(final_data)
        
    except Exception as e:
        print(f'Error during schema merge overwrite: {e}')
        import traceback
        traceback.print_exc()

finally:
    shutil.rmtree(temp_dir) 