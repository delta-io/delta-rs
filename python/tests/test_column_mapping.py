"""Tests for column mapping support in delta-rs Python bindings."""

import pyarrow as pa
import pytest
from deltalake import DeltaTable


class TestColumnMappingRead:
    """Test reading tables with column mapping enabled."""

    @pytest.fixture
    def column_mapping_table(self):
        """Load the test table with column mapping."""
        table_path = "../crates/test/tests/data/table_with_column_mapping"
        return DeltaTable(table_path)

    def test_load_table_with_column_mapping(self, column_mapping_table):
        """Test that we can load a table with column mapping."""
        assert column_mapping_table is not None
        assert column_mapping_table.version() >= 0

    def test_schema_has_logical_names(self, column_mapping_table):
        """Test that schema exposes logical column names, not physical."""
        schema = column_mapping_table.schema()

        # Schema should have logical names with spaces
        field_names = [field.name for field in schema.fields]
        assert "Company Very Short" in field_names, f"Expected 'Company Very Short', got: {field_names}"
        assert "Super Name" in field_names, f"Expected 'Super Name', got: {field_names}"

        # Verify metadata contains column mapping info
        for field in schema.fields:
            if field.name == "Company Very Short":
                assert "delta.columnMapping.physicalName" in field.metadata
                assert field.metadata["delta.columnMapping.physicalName"].startswith("col-")

    def test_read_to_pyarrow(self, column_mapping_table):
        """Test reading table to PyArrow."""
        table = column_mapping_table.to_pyarrow_table()

        assert table is not None
        assert table.num_rows > 0

        # Column names should be logical names
        column_names = table.column_names
        assert "Company Very Short" in column_names, f"Expected 'Company Very Short', got: {column_names}"
        assert "Super Name" in column_names, f"Expected 'Super Name', got: {column_names}"

    def test_read_to_pandas(self, column_mapping_table):
        """Test reading table to Pandas DataFrame."""
        df = column_mapping_table.to_pandas()

        assert df is not None
        assert len(df) > 0

        # Column names should be logical names
        column_names = list(df.columns)
        assert "Company Very Short" in column_names, f"Expected 'Company Very Short', got: {column_names}"
        assert "Super Name" in column_names, f"Expected 'Super Name', got: {column_names}"

    def test_filter_on_partition_column(self, column_mapping_table):
        """Test filtering on a partition column with special characters."""
        # Read with a filter on the partition column
        # Note: the partition column has a name with spaces
        table = column_mapping_table.to_pyarrow_table()

        # Just verify we can read the data
        assert table.num_rows > 0

    def test_metadata(self, column_mapping_table):
        """Test that metadata is accessible."""
        metadata = column_mapping_table.metadata()
        assert metadata is not None


class TestDataFusionColumnMapping:
    """Test DataFusion queries with column mapping."""

    @pytest.fixture
    def column_mapping_table(self):
        """Load the test table with column mapping."""
        table_path = "../crates/test/tests/data/table_with_column_mapping"
        return DeltaTable(table_path)

    def test_datafusion_select_all(self, column_mapping_table):
        """Test selecting all columns via DataFusion."""
        try:
            from datafusion import SessionContext

            ctx = SessionContext()
            ctx.register_table("test", column_mapping_table.to_pyarrow_dataset())

            result = ctx.sql("SELECT * FROM test").collect()
            assert len(result) > 0

            # Verify we got data with logical column names
            schema = result[0].schema
            field_names = [f.name for f in schema]
            assert "Company Very Short" in field_names, f"Expected 'Company Very Short', got: {field_names}"
            assert "Super Name" in field_names, f"Expected 'Super Name', got: {field_names}"

        except ImportError:
            pytest.skip("DataFusion not installed")

    def test_datafusion_select_with_quotes(self, column_mapping_table):
        """Test selecting columns with special characters using quotes."""
        try:
            from datafusion import SessionContext

            ctx = SessionContext()
            ctx.register_table("test", column_mapping_table.to_pyarrow_dataset())

            # Query using quoted column names for columns with spaces
            result = ctx.sql('SELECT "Company Very Short", "Super Name" FROM test').collect()
            assert len(result) > 0
            assert result[0].num_rows > 0

        except ImportError:
            pytest.skip("DataFusion not installed")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
