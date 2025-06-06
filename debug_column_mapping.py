import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
import tempfile

temp_dir = tempfile.mkdtemp()
print(f'Debugging column mapping in: {temp_dir}')

# Step 1: Create table with string types
initial_data = pa.table({
    'name': pa.array(['Alice', 'Bob'], type=pa.string()),
    'value': pa.array([1, 2], type=pa.int32())
})

write_deltalake(temp_dir, initial_data, mode='overwrite')
print('✅ Initial table created')

# Step 2: Write with column mapping enabled
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

# Step 3: Debug the schema and metadata
dt = DeltaTable(temp_dir)

print("\n=== DEBUGGING COLUMN MAPPING METADATA ===")

# Check Delta schema
delta_schema = dt.schema()
print(f"Delta schema type: {type(delta_schema)}")
print(f"Delta schema fields: {delta_schema.fields}")

for i, field in enumerate(delta_schema.fields):
    print(f"\nField {i}: {field.name}")
    print(f"  Type: {type(field)}")
    print(f"  Has metadata attr: {hasattr(field, 'metadata')}")
    if hasattr(field, 'metadata'):
        print(f"  Metadata: {field.metadata}")
        print(f"  Metadata type: {type(field.metadata)}")
        if field.metadata:
            for key, value in field.metadata.items():
                print(f"    {key}: {value} (type: {type(value)})")

# Check PyArrow schema
arrow_schema = dt.to_pyarrow_table().schema
print(f"\nPyArrow schema type: {type(arrow_schema)}")
print(f"PyArrow schema fields: {arrow_schema}")

for i, field in enumerate(arrow_schema):
    print(f"\nPyArrow Field {i}: {field.name}")
    print(f"  Type: {type(field)}")
    print(f"  Has metadata attr: {hasattr(field, 'metadata')}")
    print(f"  Metadata: {field.metadata}")
    print(f"  Metadata type: {type(field.metadata)}")

# Check configuration
metadata = dt.metadata()
print(f"\nTable configuration:")
for key, value in metadata.configuration.items():
    print(f"  {key}: {value}")

print(f"\nColumn mapping mode: {metadata.configuration.get('delta.columnMapping.mode')}") 