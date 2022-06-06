"""Test that pyspark can read tables written by deltalake(delta-rs)"""
import delta
import pyarrow as pa
import pyspark
import pytest
from delta.tables import DeltaTable as SparkDeltaTable

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
    return delta.configure_spark_with_delta_pip(builder).getOrCreate()

try:
    import pyspark
    import delta
    from delta.tables import DeltaTable as SparkDeltaTable
    spark = get_spark()
except ModuleNotFoundError:
    pass


@pytest.mark.pyspark
@pytest.mark.integration
def test_basic_read(sample_data: pa.Table, existing_table: DeltaTable):
    uri = existing_table._table.table_uri()

    df = spark.read.format("delta").load(uri)
    # breakpoint()
    assert_frame_equal(df.toPandas(), existing_table.to_pandas()) 

    dt = SparkDeltaTable(spark, uri)
    history = dt.history().collect()
    assert len(history) == 1
    assert history[0].version == 0
