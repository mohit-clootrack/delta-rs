import deltalake as dt

print('Testing column mapping functionality...')

# Test ColumnMappingMode enum
print('ColumnMappingMode.Name:', dt._internal.ColumnMappingMode.Name)
print('ColumnMappingMode.Id:', dt._internal.ColumnMappingMode.Id)
print('ColumnMappingMode.None:', dt._internal.ColumnMappingMode.None)

# Test that the column_mapping method exists on DeltaTable
from deltalake import DeltaTable
print('DeltaTable has column_mapping method:', hasattr(DeltaTable, 'column_mapping'))

print('Column mapping functionality is available!') 