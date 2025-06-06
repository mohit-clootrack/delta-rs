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
    
    new_data = pa.table({
        'id': [4, 5, 6],
        'name': ['David', 'Eve', 'Frank'],
        'age': [25, 30, 35],
        'city': ['New York', 'London', 'Tokyo']
    })
    print('New schema:', new_data.schema)
    
    try:
        write_deltalake(temp_dir, new_data, mode='append', schema_mode='merge')
        print('Schema merge completed successfully!')
        
        dt = DeltaTable(temp_dir)
        print('Final table schema:')
        for field in dt.schema().to_arrow():
            print(f'  {field.name}: {field.type}')
        
        final_data = dt.to_pandas()
        print(f'Total rows: {len(final_data)}')
        print('Sample data:')
        print(final_data.head())
        
    except Exception as e:
        print(f'Error during schema merge: {e}')
        import traceback
        traceback.print_exc()

finally:
    shutil.rmtree(temp_dir) 