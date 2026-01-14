import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
import tempfile

temp_dir = tempfile.mkdtemp()
print(f'Testing column mapping fix in: {temp_dir}')

# Step 1: Create table with string types
initial_data = pa.table({
    'name': pa.array(['Alice', 'Bob'], type=pa.string()),  # regular string
    'value': pa.array([1, 2], type=pa.int32())
})
print(f'Initial data schema: {initial_data.schema}')

write_deltalake(temp_dir, initial_data, mode='overwrite')
print('✅ Initial table created')

# Step 2: Try to write large_string data with column mapping enabled
new_data = pa.table({
    'name': pa.array(['Charlie', 'Dave'], type=pa.large_string()),  # large_string  
    'value': pa.array([3, 4], type=pa.int32())
})
print(f'New data schema: {new_data.schema}')

# This should trigger our fix
try:
    write_deltalake(
        temp_dir, 
        new_data, 
        mode='overwrite',
        configuration={
            "delta.columnMapping.mode": "name"
        }
    )
    print('✅ Write with column mapping succeeded')
    
    # Check result
    dt = DeltaTable(temp_dir)
    result_data = dt.to_pyarrow_table()
    print(f'Result data: {result_data.to_pandas()}')
    print(f'Result schema: {result_data.schema}')
    
    # Check for non-null values
    non_null_count = result_data.column('name').null_count
    total_count = len(result_data)
    print(f'Non-null rows: {total_count - non_null_count} out of {total_count}')
    
    if non_null_count == 0:
        print('🎉 SUCCESS: Column mapping fix is working!')
    else:
        print('❌ FAILURE: Data is still null')
        
except Exception as e:
    print(f'❌ Error: {e}')
    import traceback
    traceback.print_exc() 