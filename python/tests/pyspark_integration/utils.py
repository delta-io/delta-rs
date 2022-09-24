from typing import List

import pyarrow as pa

try:
    import delta
    import delta.pip_utils
    import delta.tables
    import pyspark
except ModuleNotFoundError:
    pass

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


def assert_spark_read_equal(
    expected: pa.Table, uri: str, sort_by: List[str] = ["int32"]
):
    spark = get_spark()
    df = spark.read.format("delta").load(uri)

    # Spark and pyarrow don't convert these types to the same Pandas values
    incompatible_types = ["timestamp", "struct"]

    assert_frame_equal(
        df.toPandas()
        .sort_values(sort_by, ignore_index=True)
        .drop(incompatible_types, axis="columns", errors="ignore"),
        expected.to_pandas()
        .sort_values(sort_by, ignore_index=True)
        .drop(incompatible_types, axis="columns", errors="ignore"),
    )
