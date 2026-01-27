"""Comprehensive column mapping tests - ALL operations"""

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
import tempfile
import shutil

def run_test(name, test_func):
    tmp_dir = tempfile.mkdtemp()
    try:
        test_func(tmp_dir)
        print(f"✅ {name}")
        return True
    except Exception as e:
        print(f"❌ {name}: {e}")
        return False
    finally:
        shutil.rmtree(tmp_dir)

# ============================================================
# WRITE OPERATIONS
# ============================================================

def test_write_overwrite(tmp_dir):
    """Basic write with overwrite mode"""
    df = pa.table({"col a": ["A", "B"], "col b": [1, 2]})
    write_deltalake(tmp_dir, df, mode="overwrite", 
                    configuration={"delta.columnMapping.mode": "name"})
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table()
    assert result.num_rows == 2
    assert "col a" in result.column_names

def test_write_append_same_schema(tmp_dir):
    """Append with same schema"""
    df1 = pa.table({"col a": ["A"], "col b": [1]})
    write_deltalake(tmp_dir, df1, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    df2 = pa.table({"col a": ["B"], "col b": [2]})
    write_deltalake(tmp_dir, df2, mode="append")
    
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table()
    assert result.num_rows == 2

def test_write_append_schema_evolution(tmp_dir):
    """Append with new column (schema evolution)"""
    df1 = pa.table({"col a": ["A"], "col b": [1]})
    write_deltalake(tmp_dir, df1, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    df2 = pa.table({"col a": ["B"], "col b": [2], "new col": ["X"]})
    write_deltalake(tmp_dir, df2, mode="append", schema_mode="merge")
    
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table()
    assert result.num_rows == 2
    assert "new col" in result.column_names
    # Verify new col has actual data
    new_col_vals = result.column("new col").to_pylist()
    assert "X" in new_col_vals, f"Expected 'X' in new col, got {new_col_vals}"

def test_write_overwrite_schema_evolution(tmp_dir):
    """Overwrite with schema evolution"""
    df1 = pa.table({"col a": ["A"], "col b": [1]})
    write_deltalake(tmp_dir, df1, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    df2 = pa.table({"col a": ["B"], "col b": [2], "new col": ["X"]})
    write_deltalake(tmp_dir, df2, mode="overwrite", schema_mode="overwrite")
    
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table()
    assert "new col" in result.column_names
    assert result.column("new col").to_pylist() == ["X"]

# ============================================================
# READ OPERATIONS
# ============================================================

def test_read_to_pyarrow_table(tmp_dir):
    """Read via to_pyarrow_table"""
    df = pa.table({"col a": ["A", "B"], "col b": [1, 2]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table()
    assert result.column_names == ["col a", "col b"]

def test_read_to_pyarrow_dataset(tmp_dir):
    """Read via to_pyarrow_dataset"""
    df = pa.table({"col a": ["A", "B"], "col b": [1, 2]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    ds = dt.to_pyarrow_dataset()
    result = ds.to_table()
    assert "col a" in result.column_names

def test_read_with_column_selection(tmp_dir):
    """Read with specific columns"""
    df = pa.table({"col a": ["A"], "col b": [1], "col c": [1.5]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table(columns=["col a", "col c"])
    assert result.column_names == ["col a", "col c"]

def test_read_with_filter(tmp_dir):
    """Read with filter"""
    df = pa.table({"col a": ["A", "B", "C"], "value": [1, 2, 3]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table(filters=[("value", ">", 1)])
    assert result.num_rows == 2

def test_read_to_pandas(tmp_dir):
    """Read to pandas"""
    df = pa.table({"col a": ["A", "B"], "col b": [1, 2]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    pdf = dt.to_pandas()
    assert "col a" in pdf.columns

# ============================================================
# DML OPERATIONS
# ============================================================

def test_update(tmp_dir):
    """UPDATE operation"""
    df = pa.table({"id": ["A", "B"], "value": [1, 2]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    dt.update(predicate="id = 'A'", updates={"value": "100"})
    
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table()
    values = dict(zip(result.column("id").to_pylist(), result.column("value").to_pylist()))
    assert values["A"] == 100

def test_delete(tmp_dir):
    """DELETE operation"""
    df = pa.table({"id": ["A", "B", "C"], "value": [1, 2, 3]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    dt.delete(predicate="id = 'B'")
    
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table()
    assert result.num_rows == 2
    assert "B" not in result.column("id").to_pylist()

def test_merge_update(tmp_dir):
    """MERGE with update"""
    df = pa.table({"id": ["A", "B"], "value": [1, 2]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    source = pa.table({"id": ["A"], "value": [100]})
    dt.merge(source, predicate="target.id = source.id",
             source_alias="source", target_alias="target"
    ).when_matched_update_all().execute()
    
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table()
    values = dict(zip(result.column("id").to_pylist(), result.column("value").to_pylist()))
    assert values["A"] == 100

def test_merge_insert(tmp_dir):
    """MERGE with insert"""
    df = pa.table({"id": ["A", "B"], "value": [1, 2]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    source = pa.table({"id": ["C"], "value": [3]})
    dt.merge(source, predicate="target.id = source.id",
             source_alias="source", target_alias="target"
    ).when_not_matched_insert_all().execute()
    
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table()
    assert result.num_rows == 3

def test_merge_delete(tmp_dir):
    """MERGE with delete"""
    df = pa.table({"id": ["A", "B", "C"], "value": [1, 2, 3]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    source = pa.table({"id": ["B"]})
    dt.merge(source, predicate="target.id = source.id",
             source_alias="source", target_alias="target"
    ).when_matched_delete().execute()
    
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table()
    assert result.num_rows == 2
    assert "B" not in result.column("id").to_pylist()

def test_merge_schema_evolution(tmp_dir):
    """MERGE with schema evolution"""
    df = pa.table({"id": ["A", "B"], "value": [1, 2]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    source = pa.table({"id": ["C"], "value": [3], "new col": ["X"]})
    dt.merge(source, predicate="target.id = source.id",
             source_alias="source", target_alias="target",
             merge_schema=True
    ).when_not_matched_insert_all().execute()
    
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table()
    assert "new col" in result.column_names
    new_col_vals = result.column("new col").to_pylist()
    assert "X" in new_col_vals

# ============================================================
# SPECIAL CASES
# ============================================================

def test_special_column_names_spaces(tmp_dir):
    """Column names with spaces"""
    df = pa.table({"my column": ["A"], "another col": [1]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table()
    assert "my column" in result.column_names

def test_partitioned_table(tmp_dir):
    """Partitioned table with column mapping"""
    df = pa.table({"part col": ["A", "A", "B"], "value": [1, 2, 3]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    partition_by=["part col"],
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    result = dt.to_pyarrow_table()
    assert result.num_rows == 3

def test_get_column_mapping_api(tmp_dir):
    """get_column_mapping() API"""
    df = pa.table({"col a": ["A"], "col b": [1]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    mapping = dt.get_column_mapping()
    assert len(mapping) == 2
    assert "col a" in mapping.values()
    assert "col b" in mapping.values()

def test_get_column_mapping_no_mapping(tmp_dir):
    """get_column_mapping() returns empty dict when not enabled"""
    df = pa.table({"col_a": ["A"], "col_b": [1]})
    write_deltalake(tmp_dir, df, mode="overwrite")
    
    dt = DeltaTable(tmp_dir)
    mapping = dt.get_column_mapping()
    assert mapping == {}

def test_polars_fast_read(tmp_dir):
    """Fast Polars read with get_column_mapping"""
    df = pa.table({"user name": ["A", "B"], "value": [1, 2]})
    write_deltalake(tmp_dir, df, mode="overwrite",
                    configuration={"delta.columnMapping.mode": "name"})
    
    dt = DeltaTable(tmp_dir)
    result = pl.read_parquet(dt.file_uris()).rename(dt.get_column_mapping())
    assert "user name" in result.columns
    assert result.shape[0] == 2

# ============================================================
# RUN ALL TESTS
# ============================================================

if __name__ == "__main__":
    tests = [
        # Write operations
        ("WRITE: overwrite", test_write_overwrite),
        ("WRITE: append same schema", test_write_append_same_schema),
        ("WRITE: append schema evolution", test_write_append_schema_evolution),
        ("WRITE: overwrite schema evolution", test_write_overwrite_schema_evolution),
        
        # Read operations
        ("READ: to_pyarrow_table", test_read_to_pyarrow_table),
        ("READ: to_pyarrow_dataset", test_read_to_pyarrow_dataset),
        ("READ: column selection", test_read_with_column_selection),
        ("READ: with filter", test_read_with_filter),
        ("READ: to_pandas", test_read_to_pandas),
        
        # DML operations
        ("DML: UPDATE", test_update),
        ("DML: DELETE", test_delete),
        ("DML: MERGE update", test_merge_update),
        ("DML: MERGE insert", test_merge_insert),
        ("DML: MERGE delete", test_merge_delete),
        ("DML: MERGE schema evolution", test_merge_schema_evolution),
        
        # Special cases
        ("SPECIAL: column names with spaces", test_special_column_names_spaces),
        ("SPECIAL: partitioned table", test_partitioned_table),
        ("API: get_column_mapping()", test_get_column_mapping_api),
        ("API: get_column_mapping() no mapping", test_get_column_mapping_no_mapping),
        ("POLARS: fast read", test_polars_fast_read),
    ]
    
    print("="*60)
    print("COMPREHENSIVE COLUMN MAPPING TESTS")
    print("="*60)
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        if run_test(name, test_func):
            passed += 1
        else:
            failed += 1
    
    print("="*60)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("="*60)
    
    if failed > 0:
        exit(1)
