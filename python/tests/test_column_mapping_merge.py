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
        initial_data = pa.table({
            "id": ["A", "B", "C"],
            "value": [100, 200, 300],
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

    def test_autoschema_merge_with_column_mapping(self, tmp_path):
        """Test merge with merge_schema=True (autoschema) and column mapping.

        This tests the scenario where source has new columns that need to be
        added to the target table during merge (schema evolution).
        """
        # Create a table with column mapping enabled and 2 columns
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

        # Verify initial schema has 2 columns with proper column mapping
        delta_schema = dt.schema()
        assert len(delta_schema.fields) == 2, "Initial table should have 2 columns"

        initial_max_id = int(dt.metadata().configuration.get("delta.columnMapping.maxColumnId", "0"))
        assert initial_max_id == 2, f"Initial maxColumnId should be 2, got {initial_max_id}"

        # Create source data with a NEW column (new_col)
        source_data = pa.table({
            "id": ["C", "D"],  # New rows
            "value": [300, 400],
            "new_col": [999, 888],  # NEW column not in target
        })

        # Perform merge with schema evolution enabled (autoschema)
        # This should add the new_col to the target schema
        dt.merge(
            source=source_data,
            predicate="target.id = source.id",
            source_alias="source",
            target_alias="target",
            merge_schema=True,  # Enable schema evolution
        ).when_not_matched_insert_all().execute()

        # Reload and check the schema
        dt = DeltaTable(tmp_path)
        delta_schema = dt.schema()

        # Verify the new column was added
        field_names = [f.name for f in delta_schema.fields]
        assert "new_col" in field_names, f"new_col should be added to schema. Got: {field_names}"

        # Verify new column has proper column mapping metadata
        new_col_field = next(f for f in delta_schema.fields if f.name == "new_col")
        assert "delta.columnMapping.physicalName" in new_col_field.metadata, \
            "new_col should have physical name mapping"
        assert "delta.columnMapping.id" in new_col_field.metadata, \
            "new_col should have column ID"

        # Verify maxColumnId was incremented
        new_max_id = int(dt.metadata().configuration.get("delta.columnMapping.maxColumnId", "0"))
        assert new_max_id == 3, f"maxColumnId should be 3 after adding new column, got {new_max_id}"

        # Verify the new column ID is correct
        new_col_id = int(new_col_field.metadata["delta.columnMapping.id"])
        assert new_col_id == 3, f"new_col should have ID 3, got {new_col_id}"

        # Read back the data
        result = dt.to_pyarrow_table()
        result = result.sort_by("id")

        # Verify data is correct
        # A and B should have NULL for new_col (not matched)
        # C and D should have values for all columns
        assert result.column("id").to_pylist() == ["A", "B", "C", "D"], \
            f"IDs don't match. Got: {result.column('id').to_pylist()}"
        assert result.column("value").to_pylist() == [100, 200, 300, 400], \
            f"Values don't match. Got: {result.column('value').to_pylist()}"

        # new_col should be NULL for A and B, and have values for C and D
        new_col_values = result.column("new_col").to_pylist()
        assert new_col_values[0] is None, f"A should have NULL for new_col, got {new_col_values[0]}"
        assert new_col_values[1] is None, f"B should have NULL for new_col, got {new_col_values[1]}"
        assert new_col_values[2] == 999, f"C should have 999 for new_col, got {new_col_values[2]}"
        assert new_col_values[3] == 888, f"D should have 888 for new_col, got {new_col_values[3]}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
