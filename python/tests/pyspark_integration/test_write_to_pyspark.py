"""Tests that deltalake(delta-rs) can write to tables written by PySpark"""
import pathlib

import pyarrow as pa
import pytest

from deltalake import write_deltalake
from deltalake.deltalake import PyDeltaTableError

from .utils import assert_spark_read_equal, get_spark

try:
    from pandas.testing import assert_frame_equal
except ModuleNotFoundError:
    _has_pandas = False
else:
    _has_pandas = True


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
def test_write_basic(tmp_path: pathlib.Path, sample_data: pa.Table):
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
    assert_spark_read_equal(data, str(tmp_path))


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
        PyDeltaTableError, match="Invariant (c1 > 3) violated by value .+2"
    ):
        write_deltalake(str(tmp_path), invalid_data, mode="overwrite")

    # Can write valid data to the table
    valid_data = pa.table({"c1": pa.array([5, 6], type=pa.int32())})
    write_deltalake(str(tmp_path), valid_data, mode="append")

    expected = pa.table({"c1": pa.array([4, 5, 6], type=pa.int32())})
    assert_spark_read_equal(expected, str(tmp_path))


@pytest.mark.pyspark
@pytest.mark.integration
def test_checks_min_writer_version():
    # Write table in Spark with constraint
    # assert we fail to write any data to it
    pass
