import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
import tempfile

temp_dir = tempfile.mkdtemp()
print(f'Debugging detailed column mapping in: {temp_dir}')

# Create table with column mapping enabled
new_data = pa.table({
    'name': pa.array(['Charlie', 'Dave'], type=pa.large_string()),
    'value': pa.array([3, 4], type=pa.int32())
})

write_deltalake(
    temp_dir, 
    new_data, 
    mode='overwrite',
    configuration={
        "delta.columnMapping.mode": "name"
    }
)
print('✅ Write with column mapping succeeded')

# Debug the table reading process
dt = DeltaTable(temp_dir)

print("\n=== COLUMN MAPPING DEBUG ===")

# Check the logical to physical mapping
delta_schema = dt.schema()
metadata = dt.metadata()
column_mapping_mode = metadata.configuration.get("delta.columnMapping.mode")

print(f"Column mapping mode: {column_mapping_mode}")

# Manually build the mapping like the code does
logical_to_physical = {}
physical_to_logical = {}

schema = dt.to_pyarrow_table().schema  # This gets logical schema
print(f"Schema names: {schema.names}")

for field_name in schema.names:
    print(f"\nProcessing field: {field_name}")
    
    # Get the corresponding Delta field for this logical name
    try:
        delta_field = next(f for f in delta_schema.fields if f.name == field_name)
        print(f"  Found delta field: {delta_field.name}")
        print(f"  Delta field metadata: {delta_field.metadata}")
        
        # Check if the Delta field has physical name metadata
        if hasattr(delta_field, 'metadata') and delta_field.metadata:
            physical_name_key = 'delta.columnMapping.physicalName'
            if physical_name_key in delta_field.metadata:
                physical_name = delta_field.metadata[physical_name_key]
                logical_to_physical[field_name] = physical_name
                physical_to_logical[physical_name] = field_name
                print(f"  ✅ Mapped: {field_name} -> {physical_name}")
            else:
                logical_to_physical[field_name] = field_name
                physical_to_logical[field_name] = field_name
                print(f"  ⚠️  No physical name, using logical: {field_name}")
        else:
            logical_to_physical[field_name] = field_name
            physical_to_logical[field_name] = field_name
            print(f"  ⚠️  No metadata, using logical: {field_name}")
            
    except StopIteration:
        logical_to_physical[field_name] = field_name
        physical_to_logical[field_name] = field_name
        print(f"  ❌ Field not found in Delta schema, using logical: {field_name}")

print(f"\nFinal mappings:")
print(f"  Logical -> Physical: {logical_to_physical}")
print(f"  Physical -> Logical: {physical_to_logical}")

# Now test reading using the dataset
dataset = dt.to_pyarrow_dataset()
table = dataset.to_table()

print(f"\nActual data read:")
print(f"  Schema: {table.schema}")
print(f"  Column names: {table.schema.names}")
print(f"  Data:")
for i, col_name in enumerate(table.schema.names):
    column_data = table.column(i).to_pylist()
    print(f"    {col_name}: {column_data}")

# Test reading raw parquet files to see what's actually stored
import pyarrow.parquet as pq
files = dt.file_uris()
print(f"\nParquet files: {files}")

if files:
    raw_table = pq.read_table(files[0])
    print(f"\nRaw parquet data:")
    print(f"  Schema: {raw_table.schema}")
    print(f"  Column names: {raw_table.schema.names}")
    print(f"  Data:")
    for i, col_name in enumerate(raw_table.schema.names):
        column_data = raw_table.column(i).to_pylist()
        print(f"    {col_name}: {column_data}") 