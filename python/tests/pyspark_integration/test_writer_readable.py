"""Test that pyspark can read tables written by deltalake(delta-rs)"""
import pathlib
from typing import List

import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake

try:
    from pandas.testing import assert_frame_equal
except ModuleNotFoundError:
    _has_pandas = False
else:
    _has_pandas = True


def get_spark():
    builder = (
        pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return delta.pip_utils.configure_spark_with_delta_pip(builder).getOrCreate()


try:
    import delta
    import delta.pip_utils
    import delta.tables
    import pyspark

    spark = get_spark()
except ModuleNotFoundError:
    pass


def assert_spark_read_equal(
    expected: pa.Table, uri: str, sort_by: List[str] = ["int32"]
):
    df = spark.read.format("delta").load(uri)

    # Spark and pyarrow don't convert these types to the same Pandas values
    incompatible_types = ["timestamp", "struct"]

    assert_frame_equal(
        df.toPandas()
        .sort_values(sort_by, ignore_index=True)
        .drop(incompatible_types, axis="columns"),
        expected.to_pandas()
        .sort_values(sort_by, ignore_index=True)
        .drop(incompatible_types, axis="columns"),
    )


@pytest.mark.pyspark
@pytest.mark.integration
def test_basic_read(sample_data: pa.Table, existing_table: DeltaTable):
    uri = existing_table._table.table_uri() + "/"

    assert_spark_read_equal(sample_data, uri)

    dt = delta.tables.DeltaTable.forPath(spark, uri)
    history = dt.history().collect()
    assert len(history) == 1
    assert history[0].version == 0


@pytest.mark.pyspark
@pytest.mark.integration
def test_partitioned(tmp_path: pathlib.Path, sample_data: pa.Table):
    partition_cols = ["date32", "utf8"]

    # Add null values to sample data to verify we can read null partitions
    sample_data_with_null = sample_data
    for col in partition_cols:
        i = sample_data.schema.get_field_index(col)
        field = sample_data.schema.field(i)
        nulls = pa.array([None] * sample_data.num_rows, type=field.type)
        sample_data_with_null = sample_data_with_null.set_column(i, field, nulls)
    data = pa.concat_tables([sample_data, sample_data_with_null])

    write_deltalake(str(tmp_path), data, partition_by=partition_cols)

    assert_spark_read_equal(data, str(tmp_path), sort_by=["utf8", "int32"])


@pytest.mark.pyspark
@pytest.mark.integration
def test_overwrite(
    tmp_path: pathlib.Path, sample_data: pa.Table, existing_table: DeltaTable
):
    path = str(tmp_path)

    write_deltalake(path, sample_data, mode="append")
    expected = pa.concat_tables([sample_data, sample_data])
    assert_spark_read_equal(expected, path)

    write_deltalake(path, sample_data, mode="overwrite")
    assert_spark_read_equal(sample_data, path)
