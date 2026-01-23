"""Tests for merge operations with column mapping enabled."""

import tempfile
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.query import QueryBuilder


class TestMergeWithColumnMapping:
    """Test merge operations with column mapping enabled."""

    def test_merge_update_all_with_column_mapping(self, tmp_path):
        """Test that when_matched_update_all works correctly with column mapping."""
        # Create a table with column mapping enabled
        schema = pa.schema([
            pa.field("id", pa.string()),
            pa.field("value", pa.int64()),
        ])

        initial_data = pa.table({
            "id": ["A", "B", "C"],
            "value": [100, 200, 300],
        })

        write_deltalake(
            tmp_path,
            initial_data,
            mode="overwrite",
            schema=schema,
            configuration={
                "delta.columnMapping.mode": "name",
            },
        )

        dt = DeltaTable(tmp_path)

        # Verify column mapping is enabled
        config = dt.metadata().configuration
        assert config.get("delta.columnMapping.mode") == "name", "Column mapping should be enabled"

        # Verify schema has column mapping metadata
        delta_schema = dt.schema()
        for field in delta_schema.fields:
            metadata = field.metadata
            assert "delta.columnMapping.physicalName" in metadata, f"Field {field.name} should have physical name mapping"
            assert "delta.columnMapping.id" in metadata, f"Field {field.name} should have column ID"

        # Create source data for merge
        source_data = pa.table({
            "id": ["A", "B"],  # Update A and B
            "value": [150, 250],
        })

        # Perform merge with update_all
        dt.merge(
            source=source_data,
            predicate="target.id = source.id",
            source_alias="source",
            target_alias="target",
        ).when_matched_update_all().execute()

        # Reload and read back data
        dt = DeltaTable(tmp_path)
        result = dt.to_pyarrow_table()

        # Sort for comparison
        result = result.sort_by("id")

        # Verify data is correct
        expected = pa.table({
            "id": ["A", "B", "C"],
            "value": [150, 250, 300],  # A and B should be updated
        })
        expected = expected.sort_by("id")

        assert result.column("id").to_pylist() == expected.column("id").to_pylist(), \
            f"IDs don't match. Got: {result.column('id').to_pylist()}"
        assert result.column("value").to_pylist() == expected.column("value").to_pylist(), \
            f"Values don't match. Got: {result.column('value').to_pylist()}, expected: {expected.column('value').to_pylist()}"

    def test_merge_insert_all_with_column_mapping(self, tmp_path):
        """Test that when_not_matched_insert_all works correctly with column mapping."""
        # Create a table with column mapping enabled
        initial_data = pa.table({
            "id": ["A", "B"],
            "value": [100, 200],
        })

        write_deltalake(
            tmp_path,
            initial_data,
            mode="overwrite",
            configuration={
                "delta.columnMapping.mode": "name",
            },
        )

        dt = DeltaTable(tmp_path)

        # Create source data for merge with new rows
        source_data = pa.table({
            "id": ["C", "D"],  # These are new rows
            "value": [300, 400],
        })

        # Perform merge with insert_all
        dt.merge(
            source=source_data,
            predicate="target.id = source.id",
            source_alias="source",
            target_alias="target",
        ).when_not_matched_insert_all().execute()

        # Reload and read back data
        dt = DeltaTable(tmp_path)
        result = dt.to_pyarrow_table()

        # Sort for comparison
        result = result.sort_by("id")

        # Verify data is correct
        expected = pa.table({
            "id": ["A", "B", "C", "D"],
            "value": [100, 200, 300, 400],
        })
        expected = expected.sort_by("id")

        assert result.column("id").to_pylist() == expected.column("id").to_pylist(), \
            f"IDs don't match. Got: {result.column('id').to_pylist()}"
        assert result.column("value").to_pylist() == expected.column("value").to_pylist(), \
            f"Values don't match. Got: {result.column('value').to_pylist()}, expected: {expected.column('value').to_pylist()}"

    def test_merge_update_and_insert_all_with_column_mapping(self, tmp_path):
        """Test combined update_all and insert_all with column mapping."""
        # Create a table with column mapping enabled
        initial_data = pa.table({
            "id": ["A", "B"],
            "value": [100, 200],
        })

        write_deltalake(
            tmp_path,
            initial_data,
            mode="overwrite",
            configuration={
                "delta.columnMapping.mode": "name",
            },
        )

        dt = DeltaTable(tmp_path)

        # Create source data with both existing and new rows
        source_data = pa.table({
            "id": ["A", "C"],  # A exists, C is new
            "value": [150, 300],
        })

        # Perform merge with both update_all and insert_all
        dt.merge(
            source=source_data,
            predicate="target.id = source.id",
            source_alias="source",
            target_alias="target",
        ).when_matched_update_all().when_not_matched_insert_all().execute()

        # Reload and read back data
        dt = DeltaTable(tmp_path)
        result = dt.to_pyarrow_table()

        # Sort for comparison
        result = result.sort_by("id")

        # Verify data is correct
        expected = pa.table({
            "id": ["A", "B", "C"],
            "value": [150, 200, 300],  # A updated, B unchanged, C inserted
        })
        expected = expected.sort_by("id")

        assert result.column("id").to_pylist() == expected.column("id").to_pylist(), \
            f"IDs don't match. Got: {result.column('id').to_pylist()}"
        assert result.column("value").to_pylist() == expected.column("value").to_pylist(), \
            f"Values don't match. Got: {result.column('value').to_pylist()}, expected: {expected.column('value').to_pylist()}"

    def test_merge_with_special_column_names_and_column_mapping(self, tmp_path):
        """Test merge with columns that have special characters and column mapping."""
        # Create a table with column mapping enabled and special column names
        initial_data = pa.table({
            "user id": ["A", "B"],
            "total value": [100, 200],
        })

        write_deltalake(
            tmp_path,
            initial_data,
            mode="overwrite",
            configuration={
                "delta.columnMapping.mode": "name",
            },
        )

        dt = DeltaTable(tmp_path)

        # Verify column mapping metadata is present
        delta_schema = dt.schema()
        for field in delta_schema.fields:
            metadata = field.metadata
            assert "delta.columnMapping.physicalName" in metadata, f"Field {field.name} should have physical name mapping"
            # Physical name should be different from logical name (which has spaces)
            physical_name = metadata["delta.columnMapping.physicalName"]
            assert physical_name.startswith("col-"), f"Physical name should start with 'col-', got: {physical_name}"

        # Create source data for merge
        source_data = pa.table({
            "user id": ["A", "C"],
            "total value": [150, 300],
        })

        # Perform merge with update_all and insert_all
        dt.merge(
            source=source_data,
            predicate='target.`user id` = source.`user id`',
            source_alias="source",
            target_alias="target",
        ).when_matched_update_all().when_not_matched_insert_all().execute()

        # Reload and read back data
        dt = DeltaTable(tmp_path)
        result = dt.to_pyarrow_table()

        # Verify column names are still logical names
        assert "user id" in result.column_names, f"Column 'user id' should exist with logical name"
        assert "total value" in result.column_names, f"Column 'total value' should exist with logical name"

        # Sort for comparison
        result = result.sort_by("user id")

        # Verify data is correct
        assert result.column("user id").to_pylist() == ["A", "B", "C"], \
            f"user id values don't match. Got: {result.column('user id').to_pylist()}"
        assert result.column("total value").to_pylist() == [150, 200, 300], \
            f"total value values don't match. Got: {result.column('total value').to_pylist()}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
