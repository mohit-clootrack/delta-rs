# Column Mapping Mode Support for delta-rs

## Overview

Implement full support for Delta Lake column mapping mode (`name` and `id`) in delta-rs, enabling reading tables where logical column names differ from physical Parquet column names.

## Background

**Column Mapping Mode** decouples logical column names (user-visible) from physical column names (in Parquet files):
- **`name` mode**: Uses `delta.columnMapping.physicalName` field metadata
- **`id` mode**: Uses Parquet `field_id` via `delta.columnMapping.id` field metadata

**Root Cause of Current Issues** (Issues #930, #2914, #3348):
- `partitioned_file_from_action()` looks up partition values using **logical names**
- Transaction log stores partition values with **physical names** (e.g., `"col-abc123": "value"`)
- Schema adapter matches columns by name without physical/logical translation

## Implementation Checklist

### Phase 0: Setup
- [x] Add upstream remote: `git remote add upstream https://github.com/delta-io/delta-rs.git`
- [x] Fetch latest from upstream: `git fetch upstream`
- [x] Merge upstream main into working branch
- [x] Resolve any merge conflicts
- [x] Verify build works: `cargo build -p deltalake-core`

### Phase 1: Core Infrastructure
- [ ] Create `crates/core/src/kernel/column_mapping.rs` with `ColumnMappingResolver` struct
  - Bidirectional mapping: physical <-> logical names
  - Support for both `name` and `id` modes
  - Helper methods: `to_physical()`, `to_logical()`, `is_identity()`
- [ ] Add `column_mapping_resolver()` method to `Snapshot` and `EagerSnapshot`
- [ ] Export module in `crates/core/src/kernel/mod.rs`

### Phase 2: Fix Partition Value Handling (Critical)
- [ ] Update `partitioned_file_from_action()` in `crates/core/src/delta_datafusion/mod.rs`:
  - Add `ColumnMappingResolver` parameter
  - Use physical name for `action.partition_values` HashMap lookup
  - Keep logical name for schema field lookup
- [ ] Update `DeltaScanBuilder::build()` to pass column mapping resolver

### Phase 3: Fix Schema Adapter for Parquet Reading
- [ ] Update `DeltaSchemaAdapterFactory` in `crates/core/src/delta_datafusion/schema_adapter.rs`:
  - Add `column_mapping` field
  - Constructor accepts `Option<ColumnMappingResolver>`
- [ ] Update `DeltaSchemaAdapter::map_column_index()`:
  - For `name` mode: lookup by physical name in Parquet schema
  - For `id` mode: lookup by field_id in Parquet schema
- [ ] Update `SchemaMapping::map_batch()`:
  - Rename columns from physical to logical names after reading

### Phase 4: Fix Statistics Handling
- [ ] Update `resolve_column_value_stat()` in `crates/core/src/table/state_arrow.rs`:
  - Accept column mapping resolver
  - Translate logical path segments to physical for stats lookup
- [ ] Update `resolve_column_count_stat()` similarly
- [ ] Update `stats_as_batch()` to use column mapping

### Phase 5: Write Support (Future Work)
> **Note:** Write support requires significant changes and is currently disabled.
> The `ColumnMapping` feature is commented out in `protocol.rs` ProtocolChecker.

- [ ] Enable `TableFeature::ColumnMapping` in `kernel/transaction/protocol.rs`
- [ ] Update `WriterConfig` to accept column mapping mode
- [ ] Transform Arrow schema fields to physical names before writing Parquet:
  - Add `file_schema_physical()` method using `make_physical()` from delta_kernel
- [ ] Transform partition values to physical names in `Add` actions:
  - Modify `create_add()` in `writer/stats.rs`
- [ ] Verify statistics are correct (should already work since stats come from Parquet)

### Phase 6: Python Bindings
- [ ] Add `column_mapping_mode()` method to `RawDeltaTable` in `python/src/lib.rs`
- [ ] Add `physical_name(logical_name)` method
- [ ] Update Python type stubs in `python/deltalake/_internal.pyi`

### Phase 7: Testing
- [ ] Unit tests for `ColumnMappingResolver`
- [ ] Integration test: read table with column mapping (use existing test data)
- [ ] Integration test: DataFusion query with column mapping
- [ ] Integration test: partition pruning with column mapping
- [ ] Integration test: write to table with column mapping
- [ ] Integration test: roundtrip (write then read) with column mapping
- [ ] Python test: read table with column mapping
- [ ] Python test: write to table with column mapping
- [ ] Test all data types with column mapping (primitives, nested structs, arrays, maps)

### Phase 8: Verification
- [ ] Remove `column_mapping` from DAT skipped tests if applicable
- [ ] Run full test suite
- [ ] Test with sample tables created by Spark/Databricks

---

## Key Files to Modify

| File | Purpose |
|------|---------|
| `crates/core/src/kernel/column_mapping.rs` | NEW - Core column mapping utilities |
| `crates/core/src/delta_datafusion/mod.rs` | Fix `partitioned_file_from_action()` |
| `crates/core/src/delta_datafusion/schema_adapter.rs` | Physical/logical name mapping for Parquet |
| `crates/core/src/table/state_arrow.rs` | Statistics with column mapping |
| `crates/core/src/kernel/snapshot/mod.rs` | Add resolver access methods |
| `crates/core/src/operations/write/writer.rs` | Write support with physical names |
| `crates/core/src/protocol/mod.rs` | Protocol version handling for column mapping |
| `python/src/lib.rs` | Python bindings |

---

## Test Data

Existing test table: `/home/user/delta-rs/crates/test/tests/data/table_with_column_mapping/`

Schema fields use metadata like:
```json
{
  "name": "Company Very Short",
  "metadata": {
    "delta.columnMapping.id": 1,
    "delta.columnMapping.physicalName": "col-173b4db9-b5ad-427f-9e75-516aae37fbbb"
  }
}
```

Partition values in log use physical names:
```json
{"partitionValues": {"col-173b4db9-b5ad-427f-9e75-516aae37fbbb": "BMS"}}
```

---

## Edge Cases

1. **Nested structs**: Apply mapping recursively
2. **Missing metadata**: Validate with `validate_schema_column_mapping()`, return clear error
3. **Mixed stats names**: Try physical first, fall back to logical
4. **`id` mode**: Match by Parquet field_id, not by name

---

## Backward Compatibility

- `ColumnMappingMode::None` preserves existing behavior exactly
- No public API signature changes
- All internal parameters added are optional or backward-compatible

---

## Verification Plan

1. Run existing tests: `cargo test -p deltalake-core`
2. Run Python tests: `cd python && pytest tests/`
3. Manual verification with test table:
   ```python
   import deltalake
   dt = deltalake.DeltaTable("crates/test/tests/data/table_with_column_mapping")
   print(dt.to_pandas())  # Should show data with logical column names
   ```
4. DataFusion query verification:
   ```python
   from datafusion import SessionContext
   ctx = SessionContext()
   ctx.register_table("test", dt.to_pyarrow_dataset())
   ctx.sql('SELECT "Company Very Short" FROM test').collect()
   ```
