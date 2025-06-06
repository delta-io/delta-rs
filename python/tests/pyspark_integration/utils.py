from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa


def get_spark():
    import delta
    import delta.pip_utils
    import delta.tables
    import pyspark

    builder = (
        pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.ansi.enabled", "false")
    )
    return delta.pip_utils.configure_spark_with_delta_pip(builder).getOrCreate()


def assert_spark_read_equal(
    expected: "pa.Table", uri: str, sort_by: list[str] = ["int32"]
):
    from pandas.testing import assert_frame_equal

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


def run_stream_with_checkpoint(source_table: str):
    spark = get_spark()

    stream_path = source_table + "/stream"
    checkpoint_path = stream_path + "streaming_checkpoints/"

    streaming_df = spark.readStream.format("delta").load(source_table)
    query = (
        streaming_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .start(stream_path)
    )
    query.processAllAvailable()
    query.stop()
