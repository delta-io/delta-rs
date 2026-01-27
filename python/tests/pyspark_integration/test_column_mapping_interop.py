"""
Comprehensive PySpark interoperability tests for column mapping.

Tests cross-compatibility between delta-rs (deltalake) and PySpark (delta-spark)
for tables with column mapping enabled.
"""

import pathlib

import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake

from .utils import get_spark


def assert_data_equal(expected_pa: pa.Table, actual_pa: pa.Table, sort_by: str = "id"):
    """Compare two PyArrow tables for equality after sorting."""
    expected_df = expected_pa.to_pandas().sort_values(sort_by, ignore_index=True)
    actual_df = actual_pa.to_pandas().sort_values(sort_by, ignore_index=True)

    from pandas.testing import assert_frame_equal
    assert_frame_equal(expected_df, actual_df)


# =============================================================================
# Part 1: PySpark writes with column mapping -> deltalake reads
# =============================================================================


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_pyspark_write_column_mapping_deltalake_read(tmp_path: pathlib.Path):
    """PySpark creates a table with column mapping, deltalake reads it."""
    import pyspark
    spark = get_spark()

    # Create data with PySpark
    data = [
        (1, "Alice", 30, 50000.0),
        (2, "Bob", 25, 60000.0),
        (3, "Charlie", 35, 70000.0),
    ]
    columns = ["id", "name", "age", "salary"]

    df = spark.createDataFrame(data, columns)

    # Write with column mapping mode enabled
    df.write.format("delta") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(str(tmp_path))

    # Read with deltalake
    dt = DeltaTable(str(tmp_path))

    # Verify schema has logical names
    schema = dt.schema()
    field_names = [f.name for f in schema.fields]
    assert "id" in field_names
    assert "name" in field_names

    # Verify data can be read correctly
    table = dt.to_pyarrow_table()
    assert table.num_rows == 3
    assert set(table["id"].to_pylist()) == {1, 2, 3}


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_pyspark_write_column_mapping_special_names_deltalake_read(tmp_path: pathlib.Path):
    """PySpark creates a table with column mapping and special column names."""
    import pyspark
    spark = get_spark()

    # Column names with spaces and special characters
    data = [
        (1, "Alice", 30),
        (2, "Bob", 25),
    ]
    columns = ["user id", "full name", "user age"]

    df = spark.createDataFrame(data, columns)

    # Write with column mapping
    df.write.format("delta") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(str(tmp_path))

    # Read with deltalake
    dt = DeltaTable(str(tmp_path))

    # Verify logical names are preserved
    schema = dt.schema()
    field_names = [f.name for f in schema.fields]
    assert "user id" in field_names
    assert "full name" in field_names
    assert "user age" in field_names

    # Verify data
    table = dt.to_pyarrow_table()
    assert table.num_rows == 2
    assert "user id" in table.column_names


# =============================================================================
# Part 2: deltalake writes with column mapping -> PySpark reads
# =============================================================================


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_deltalake_write_column_mapping_pyspark_read(tmp_path: pathlib.Path):
    """deltalake creates a table with column mapping, PySpark reads it."""
    spark = get_spark()

    # Create data with PyArrow
    data = pa.table({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 25, 35],
    })

    # Write with column mapping
    write_deltalake(
        str(tmp_path),
        data,
        mode="overwrite",
        configuration={
            "delta.columnMapping.mode": "name",
        },
    )

    # Read with PySpark
    spark_df = spark.read.format("delta").load(str(tmp_path))

    # Verify schema
    spark_columns = spark_df.columns
    assert "id" in spark_columns
    assert "name" in spark_columns
    assert "age" in spark_columns

    # Verify data
    result = spark_df.collect()
    assert len(result) == 3
    ids = {row["id"] for row in result}
    assert ids == {1, 2, 3}


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_deltalake_write_append_column_mapping_pyspark_read(tmp_path: pathlib.Path):
    """deltalake appends to a column mapping table, PySpark reads it."""
    spark = get_spark()

    # Initial data
    data1 = pa.table({
        "id": [1, 2],
        "value": [10, 20],
    })

    # Create table with column mapping
    write_deltalake(
        str(tmp_path),
        data1,
        mode="overwrite",
        configuration={
            "delta.columnMapping.mode": "name",
        },
    )

    # Append more data
    data2 = pa.table({
        "id": [3, 4],
        "value": [30, 40],
    })
    write_deltalake(str(tmp_path), data2, mode="append")

    # Read with PySpark and verify all rows
    spark_df = spark.read.format("delta").load(str(tmp_path))
    result = spark_df.collect()
    assert len(result) == 4
    ids = {row["id"] for row in result}
    assert ids == {1, 2, 3, 4}


# =============================================================================
# Part 3: DELETE operations with column mapping
# =============================================================================


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_deltalake_delete_on_pyspark_column_mapping_table(tmp_path: pathlib.Path):
    """deltalake performs DELETE on a PySpark-created column mapping table."""
    import pyspark
    spark = get_spark()

    # Create table with PySpark
    data = [(1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E")]
    df = spark.createDataFrame(data, ["id", "name"])

    df.write.format("delta") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(str(tmp_path))

    # Delete with deltalake
    dt = DeltaTable(str(tmp_path))
    dt.delete("id > 3")

    # Verify with both deltalake and PySpark
    result_dl = dt.to_pyarrow_table()
    assert set(result_dl["id"].to_pylist()) == {1, 2, 3}

    spark_df = spark.read.format("delta").load(str(tmp_path))
    result_spark = {row["id"] for row in spark_df.collect()}
    assert result_spark == {1, 2, 3}


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_pyspark_delete_on_deltalake_column_mapping_table(tmp_path: pathlib.Path):
    """PySpark performs DELETE on a deltalake-created column mapping table."""
    spark = get_spark()

    # Create table with deltalake
    data = pa.table({
        "id": [1, 2, 3, 4, 5],
        "name": ["A", "B", "C", "D", "E"],
    })

    write_deltalake(
        str(tmp_path),
        data,
        mode="overwrite",
        configuration={"delta.columnMapping.mode": "name"},
    )

    # Delete with PySpark
    from delta.tables import DeltaTable as SparkDeltaTable
    spark_dt = SparkDeltaTable.forPath(spark, str(tmp_path))
    spark_dt.delete("id <= 2")

    # Verify with both
    spark_df = spark.read.format("delta").load(str(tmp_path))
    result_spark = {row["id"] for row in spark_df.collect()}
    assert result_spark == {3, 4, 5}

    dt = DeltaTable(str(tmp_path))
    result_dl = set(dt.to_pyarrow_table()["id"].to_pylist())
    assert result_dl == {3, 4, 5}


# =============================================================================
# Part 4: UPDATE operations with column mapping
# =============================================================================


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_deltalake_update_on_pyspark_column_mapping_table(tmp_path: pathlib.Path):
    """deltalake performs UPDATE on a PySpark-created column mapping table."""
    import pyspark
    spark = get_spark()

    # Create table with PySpark
    data = [(1, "A", 10), (2, "B", 20), (3, "C", 30)]
    df = spark.createDataFrame(data, ["id", "name", "value"])

    df.write.format("delta") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(str(tmp_path))

    # Update with deltalake
    dt = DeltaTable(str(tmp_path))
    dt.update(
        predicate="id = 2",
        updates={"value": "200", "name": "'BB'"}
    )

    # Verify
    result = dt.to_pyarrow_table()
    row2 = [r for r in result.to_pydict()["id"] if result.to_pydict()["id"].index(r) == result.to_pydict()["id"].index(2)]

    # Check the updated row
    df_pandas = result.to_pandas()
    updated_row = df_pandas[df_pandas["id"] == 2].iloc[0]
    assert updated_row["value"] == 200
    assert updated_row["name"] == "BB"


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_pyspark_update_on_deltalake_column_mapping_table(tmp_path: pathlib.Path):
    """PySpark performs UPDATE on a deltalake-created column mapping table."""
    spark = get_spark()

    # Create table with deltalake
    data = pa.table({
        "id": [1, 2, 3],
        "name": ["A", "B", "C"],
        "value": [10, 20, 30],
    })

    write_deltalake(
        str(tmp_path),
        data,
        mode="overwrite",
        configuration={"delta.columnMapping.mode": "name"},
    )

    # Update with PySpark
    from delta.tables import DeltaTable as SparkDeltaTable
    from pyspark.sql.functions import lit

    spark_dt = SparkDeltaTable.forPath(spark, str(tmp_path))
    spark_dt.update(
        condition="id = 1",
        set={"value": lit(100), "name": lit("AA")}
    )

    # Verify with deltalake
    dt = DeltaTable(str(tmp_path))
    df = dt.to_pyarrow_table().to_pandas()
    updated_row = df[df["id"] == 1].iloc[0]
    assert updated_row["value"] == 100
    assert updated_row["name"] == "AA"


# =============================================================================
# Part 5: MERGE operations with column mapping
# =============================================================================


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_deltalake_merge_on_pyspark_column_mapping_table(tmp_path: pathlib.Path):
    """deltalake performs MERGE on a PySpark-created column mapping table."""
    import pyspark
    spark = get_spark()

    # Create target table with PySpark
    data = [(1, "A", 10), (2, "B", 20), (3, "C", 30)]
    df = spark.createDataFrame(data, ["id", "name", "value"])

    df.write.format("delta") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(str(tmp_path))

    # Merge with deltalake
    source = pa.table({
        "id": [2, 3, 4],  # 2 & 3 exist (update), 4 is new (insert)
        "name": ["BB", "CC", "D"],
        "value": [200, 300, 40],
    })

    dt = DeltaTable(str(tmp_path))
    dt.merge(
        source,
        predicate="s.id = t.id",
        source_alias="s",
        target_alias="t"
    ).when_matched_update_all().when_not_matched_insert_all().execute()

    # Verify
    result = dt.to_pyarrow_table().to_pandas().sort_values("id")
    assert len(result) == 4
    assert list(result["id"]) == [1, 2, 3, 4]
    assert list(result["name"]) == ["A", "BB", "CC", "D"]
    assert list(result["value"]) == [10, 200, 300, 40]


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_pyspark_merge_on_deltalake_column_mapping_table(tmp_path: pathlib.Path):
    """PySpark performs MERGE on a deltalake-created column mapping table."""
    spark = get_spark()

    # Create target table with deltalake
    data = pa.table({
        "id": [1, 2, 3],
        "name": ["A", "B", "C"],
        "value": [10, 20, 30],
    })

    write_deltalake(
        str(tmp_path),
        data,
        mode="overwrite",
        configuration={"delta.columnMapping.mode": "name"},
    )

    # Create source DataFrame for merge
    source_data = [(2, "BB", 200), (4, "D", 40)]
    source_df = spark.createDataFrame(source_data, ["id", "name", "value"])

    # Merge with PySpark
    from delta.tables import DeltaTable as SparkDeltaTable

    spark_dt = SparkDeltaTable.forPath(spark, str(tmp_path))
    spark_dt.alias("t").merge(
        source_df.alias("s"),
        "t.id = s.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    # Verify with deltalake
    dt = DeltaTable(str(tmp_path))
    result = dt.to_pyarrow_table().to_pandas().sort_values("id")

    assert len(result) == 4
    assert list(result["id"]) == [1, 2, 3, 4]
    # id=2 should be updated, id=4 should be inserted
    assert result[result["id"] == 2]["name"].iloc[0] == "BB"
    assert result[result["id"] == 4]["name"].iloc[0] == "D"


# =============================================================================
# Part 6: Filter/predicate pushdown with column mapping
# =============================================================================


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_filter_pushdown_pyspark_column_mapping_table(tmp_path: pathlib.Path):
    """Test filter pushdown on PySpark-created column mapping table."""
    import pyspark
    spark = get_spark()

    # Create large-ish table with PySpark
    data = [(i, f"name_{i}", i * 10) for i in range(100)]
    df = spark.createDataFrame(data, ["id", "name", "value"])

    df.write.format("delta") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(str(tmp_path))

    # Read with filter using deltalake
    dt = DeltaTable(str(tmp_path))

    # Test with to_pyarrow_dataset filter
    import pyarrow.dataset as ds
    dataset = dt.to_pyarrow_dataset()
    result = dataset.to_table(filter=ds.field("id") > 95)

    assert result.num_rows == 4  # 96, 97, 98, 99
    assert all(id > 95 for id in result["id"].to_pylist())


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_filter_complex_predicates_column_mapping(tmp_path: pathlib.Path):
    """Test complex filter predicates on column mapping table."""
    spark = get_spark()

    # Create table with deltalake
    data = pa.table({
        "id": list(range(1, 21)),
        "category": ["A", "B", "C", "D"] * 5,
        "value": [i * 10 for i in range(1, 21)],
    })

    write_deltalake(
        str(tmp_path),
        data,
        mode="overwrite",
        configuration={"delta.columnMapping.mode": "name"},
    )

    dt = DeltaTable(str(tmp_path))
    import pyarrow.dataset as ds

    dataset = dt.to_pyarrow_dataset()

    # Test compound filter: (id > 10 AND category == 'A') OR value >= 190
    result = dataset.to_table(
        filter=(
            ((ds.field("id") > 10) & (ds.field("category") == "A")) |
            (ds.field("value") >= 190)
        )
    )

    # Verify results manually
    df = result.to_pandas()
    for _, row in df.iterrows():
        condition1 = row["id"] > 10 and row["category"] == "A"
        condition2 = row["value"] >= 190
        assert condition1 or condition2, f"Row doesn't match filter: {row.to_dict()}"


# =============================================================================
# Part 7: Schema evolution with column mapping
# =============================================================================


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_deltalake_add_column_pyspark_reads(tmp_path: pathlib.Path):
    """deltalake adds column to column mapping table, PySpark reads it."""
    spark = get_spark()

    # Create initial table
    data1 = pa.table({
        "id": [1, 2, 3],
        "name": ["A", "B", "C"],
    })

    write_deltalake(
        str(tmp_path),
        data1,
        mode="overwrite",
        configuration={"delta.columnMapping.mode": "name"},
    )

    # Add new column with new data
    data2 = pa.table({
        "id": [4, 5],
        "name": ["D", "E"],
        "new_col": [100, 200],
    })

    write_deltalake(str(tmp_path), data2, mode="append", schema_mode="merge")

    # Read with PySpark
    spark_df = spark.read.format("delta").load(str(tmp_path))

    # Verify schema has new column
    assert "new_col" in spark_df.columns

    # Verify data (old rows should have null for new_col)
    result = spark_df.collect()
    assert len(result) == 5


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_pyspark_add_column_deltalake_reads(tmp_path: pathlib.Path):
    """PySpark adds column to column mapping table, deltalake reads it."""
    import pyspark
    from pyspark.sql.types import IntegerType
    spark = get_spark()

    # Create initial table with PySpark
    data = [(1, "A"), (2, "B")]
    df = spark.createDataFrame(data, ["id", "name"])

    df.write.format("delta") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(str(tmp_path))

    # Add column and data with PySpark
    from delta.tables import DeltaTable as SparkDeltaTable

    spark.sql(f"ALTER TABLE delta.`{tmp_path}` ADD COLUMNS (new_col INT)")

    new_data = [(3, "C", 100)]
    new_df = spark.createDataFrame(new_data, ["id", "name", "new_col"])
    new_df.write.format("delta").mode("append").save(str(tmp_path))

    # Read with deltalake
    dt = DeltaTable(str(tmp_path))
    result = dt.to_pyarrow_table()

    # Verify schema has new column
    assert "new_col" in result.column_names
    assert result.num_rows == 3


# =============================================================================
# Part 8: Roundtrip verification
# =============================================================================


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_full_roundtrip_operations(tmp_path: pathlib.Path):
    """Test full roundtrip: create -> append -> update -> delete -> merge."""
    spark = get_spark()

    # Step 1: PySpark creates table
    data = [(1, "A", 10), (2, "B", 20), (3, "C", 30)]
    df = spark.createDataFrame(data, ["id", "name", "value"])

    df.write.format("delta") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(str(tmp_path))

    # Step 2: deltalake appends
    dt = DeltaTable(str(tmp_path))
    append_data = pa.table({"id": [4, 5], "name": ["D", "E"], "value": [40, 50]})
    write_deltalake(str(tmp_path), append_data, mode="append")

    # Step 3: PySpark updates
    from delta.tables import DeltaTable as SparkDeltaTable
    from pyspark.sql.functions import lit

    spark_dt = SparkDeltaTable.forPath(spark, str(tmp_path))
    spark_dt.update(condition="id = 1", set={"value": lit(100)})

    # Step 4: deltalake deletes
    dt = DeltaTable(str(tmp_path))
    dt.delete("id = 5")

    # Step 5: PySpark merges
    merge_data = [(2, "BB", 200), (6, "F", 60)]
    merge_df = spark.createDataFrame(merge_data, ["id", "name", "value"])

    spark_dt = SparkDeltaTable.forPath(spark, str(tmp_path))
    spark_dt.alias("t").merge(
        merge_df.alias("s"), "t.id = s.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    # Final verification
    dt = DeltaTable(str(tmp_path))
    result = dt.to_pyarrow_table().to_pandas().sort_values("id").reset_index(drop=True)

    expected = pa.table({
        "id": [1, 2, 3, 4, 6],
        "name": ["A", "BB", "C", "D", "F"],
        "value": [100, 200, 30, 40, 60],
    }).to_pandas().sort_values("id").reset_index(drop=True)

    from pandas.testing import assert_frame_equal
    assert_frame_equal(result, expected)

    # Also verify with PySpark
    spark_result = spark.read.format("delta").load(str(tmp_path))
    spark_df = spark_result.toPandas().sort_values("id").reset_index(drop=True)
    assert_frame_equal(spark_df, expected)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
