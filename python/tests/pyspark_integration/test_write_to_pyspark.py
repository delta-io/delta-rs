"""Tests that deltalake(delta-rs) can write to tables written by PySpark."""

import pathlib

import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaProtocolError

from .utils import assert_spark_read_equal, get_spark

try:
    import delta
    import delta.pip_utils
    import delta.tables
    import pyspark

    spark = get_spark()
except ModuleNotFoundError:
    pass


@pytest.mark.pyspark
@pytest.mark.integration
def test_write_basic(tmp_path: pathlib.Path):
    # Write table in Spark
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
@pytest.mark.integration
def test_write_invariant(tmp_path: pathlib.Path):
    # Write table in Spark with invariant
    spark = get_spark()

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
    invalid_data = pa.table({"c1": pa.array([6, 2], type=pa.int32())})
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
@pytest.mark.integration
def test_checks_min_writer_version(tmp_path: pathlib.Path):
    # Write table in Spark with constraint
    spark = get_spark()

    spark.createDataFrame([(4,)], schema=["c1"]).write.save(
        str(tmp_path),
        mode="append",
        format="delta",
    )

    # Add a constraint upgrades the minWriterProtocol
    spark.sql(f"ALTER TABLE delta.`{tmp_path!s}` ADD CONSTRAINT x CHECK (c1 > 2)")

    with pytest.raises(
        DeltaProtocolError, match="This table's min_writer_version is 3, but"
    ):
        valid_data = pa.table({"c1": pa.array([5, 6])})
        write_deltalake(str(tmp_path), valid_data, mode="append", engine="pyarrow")


@pytest.mark.pyspark
@pytest.mark.integration
def test_spark_read_optimize_history(tmp_path: pathlib.Path):
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
@pytest.mark.integration
def test_spark_read_z_ordered_history(tmp_path: pathlib.Path):
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
