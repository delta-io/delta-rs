"""Tests that deltalake(delta-rs) can write to tables written by PySpark."""

import os
import pathlib

import pytest
from arro3.core import Array, DataType, Table
from arro3.core import Field as ArrowField

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaProtocolError

from .utils import assert_spark_read_equal, get_spark, run_stream_with_checkpoint


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_write_basic(tmp_path: pathlib.Path):
    # Write table in Spark
    import pyarrow as pa
    import pyspark

    spark = get_spark()
    schema = pyspark.sql.types.StructType(
        [
            pyspark.sql.types.StructField(
                "c1",
                dataType=pyspark.sql.types.IntegerType(),
                nullable=True,
            )
        ]
    )
    spark.createDataFrame([(4,)], schema=schema).write.save(
        str(tmp_path),
        mode="append",
        format="delta",
    )
    # Overwrite table in deltalake
    data = pa.table({"c1": pa.array([5, 6], type=pa.int32())})

    write_deltalake(str(tmp_path), data, mode="overwrite")

    # Read table in Spark
    assert_spark_read_equal(data, str(tmp_path), sort_by="c1")


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_write_invariant(tmp_path: pathlib.Path):
    # Write table in Spark with invariant
    #
    spark = get_spark()
    import delta
    import delta.pip_utils
    import delta.tables
    import pyarrow as pa
    import pyspark

    schema = pyspark.sql.types.StructType(
        [
            pyspark.sql.types.StructField(
                "c1",
                dataType=pyspark.sql.types.IntegerType(),
                nullable=True,
                metadata={
                    "delta.invariants": '{"expression": { "expression": "c1 > 3"} }'
                },
            )
        ]
    )

    delta.tables.DeltaTable.create(spark).location(str(tmp_path)).addColumns(
        schema
    ).execute()

    spark.createDataFrame([(4,)], schema=schema).write.save(
        str(tmp_path),
        mode="append",
        format="delta",
    )

    # Cannot write invalid data to the table
    invalid_data = Table(
        {
            "c1": Array(
                [6, 2],
                ArrowField("c1", type=DataType.int32(), nullable=True),
            ),
        }
    )

    with pytest.raises(
        DeltaProtocolError, match=r"Invariant \(c1 > 3\) violated by value .+2"
    ):
        # raise DeltaProtocolError("test")
        write_deltalake(str(tmp_path), invalid_data, mode="overwrite")

    # Can write valid data to the table
    valid_data = pa.table({"c1": pa.array([5, 6], type=pa.int32())})
    write_deltalake(str(tmp_path), valid_data, mode="append")

    expected = pa.table({"c1": pa.array([4, 5, 6], type=pa.int32())})
    assert_spark_read_equal(expected, str(tmp_path), sort_by="c1")


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_spark_read_optimize_history(tmp_path: pathlib.Path):
    import pyarrow as pa

    ids = ["1"] * 10
    values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

    id_array = pa.array(ids, type=pa.string())
    value_array = pa.array(values, type=pa.int32())

    pa_table = pa.Table.from_arrays([id_array, value_array], names=["id", "value"])

    # Two writes on purpose for an optimize to occur
    write_deltalake(tmp_path, pa_table, mode="append", partition_by=["id"])
    write_deltalake(tmp_path, pa_table, mode="append", partition_by=["id"])

    dt = DeltaTable(tmp_path)
    dt.optimize.compact(partition_filters=[("id", "=", "1")])

    spark = get_spark()
    history_df = spark.sql(f"DESCRIBE HISTORY '{tmp_path}'")

    latest_operation_metrics = (
        history_df.orderBy(history_df.version.desc()).select("operationMetrics").first()
    )

    assert latest_operation_metrics["operationMetrics"] is not None


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_spark_read_z_ordered_history(tmp_path: pathlib.Path):
    import pyarrow as pa

    ids = ["1"] * 10
    values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

    id_array = pa.array(ids, type=pa.string())
    value_array = pa.array(values, type=pa.int32())

    pa_table = pa.Table.from_arrays([id_array, value_array], names=["id", "value"])

    # Two writes on purpose for an optimize to occur
    write_deltalake(tmp_path, pa_table, mode="append", partition_by=["id"])
    write_deltalake(tmp_path, pa_table, mode="append", partition_by=["id"])

    dt = DeltaTable(tmp_path)
    dt.optimize.z_order(columns=["value"], partition_filters=[("id", "=", "1")])

    spark = get_spark()
    history_df = spark.sql(f"DESCRIBE HISTORY '{tmp_path}'")

    latest_operation_metrics = (
        history_df.orderBy(history_df.version.desc()).select("operationMetrics").first()
    )

    assert latest_operation_metrics["operationMetrics"] is not None


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_spark_read_repair_run(tmp_path):
    import pyarrow as pa

    ids = ["1"] * 10
    values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

    id_array = pa.array(ids, type=pa.string())
    value_array = pa.array(values, type=pa.int32())

    pa_table = pa.Table.from_arrays([id_array, value_array], names=["id", "value"])

    write_deltalake(tmp_path, pa_table, mode="append")
    write_deltalake(tmp_path, pa_table, mode="append")
    dt = DeltaTable(tmp_path)
    os.remove(dt.file_uris()[0])

    dt.repair(dry_run=False)
    spark = get_spark()

    history_df = spark.sql(f"DESCRIBE HISTORY '{tmp_path}'")

    latest_operation_metrics = (
        history_df.orderBy(history_df.version.desc()).select("operationMetrics").first()
    )

    assert latest_operation_metrics["operationMetrics"] is not None


@pytest.mark.pyspark
@pytest.mark.pyarrow
@pytest.mark.integration
def test_spark_stream_schema_evolution(tmp_path: pathlib.Path):
    """https://github.com/delta-io/delta-rs/issues/3274"""
    """
    This test ensures that Spark can still read from tables following
    a schema evolution write, since old behavior was to generate a new table ID
    between schema evolution runs, which caused Spark to error thinking the table had changed.
    """
    import pyarrow as pa

    data_first_write = pa.array(
        [
            {"name": "Alice", "age": 30, "details": {"email": "alice@example.com"}},
            {"name": "Bob", "age": 25, "details": {"email": "bob@example.com"}},
        ]
    )

    data_second_write = pa.array(
        [
            {
                "name": "Charlie",
                "age": 35,
                "details": {"address": "123 Main St", "email": "charlie@example.com"},
            },
            {
                "name": "Diana",
                "age": 28,
                "details": {"address": "456 Elm St", "email": "diana@example.com"},
            },
        ]
    )

    schema_first_write = pa.schema(
        [
            ("name", pa.string()),
            ("age", pa.int64()),
            ("details", pa.struct([("email", pa.string())])),
        ]
    )

    schema_second_write = pa.schema(
        [
            ("name", pa.string()),
            ("age", pa.int64()),
            (
                "details",
                pa.struct(
                    [
                        ("address", pa.string()),
                        ("email", pa.string()),
                    ]
                ),
            ),
        ]
    )
    table_first_write = pa.Table.from_pylist(
        data_first_write, schema=schema_first_write
    )
    table_second_write = pa.Table.from_pylist(
        data_second_write, schema=schema_second_write
    )

    write_deltalake(
        tmp_path,
        table_first_write,
        mode="append",
    )

    run_stream_with_checkpoint(tmp_path.as_posix())

    write_deltalake(tmp_path, table_second_write, mode="append", schema_mode="merge")

    run_stream_with_checkpoint(tmp_path.as_posix())

    # For this test we don't care about the data, we'll just let any
    # exceptions trickle up to report as a failure
