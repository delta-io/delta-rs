from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa


def get_spark():
    import os

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

    # Add hadoop-aws dependency for S3A filesystem support
    builder = builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.6"
    )
    builder = builder.config(
        "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    # Configure S3 access using environment variables for MinIO/LocalStack integration
    aws_endpoint = os.environ.get(
        "AWS_ENDPOINT_URL", "http://localhost:4566"
    )  # Default to MinIO
    aws_access_key = os.environ.get(
        "AWS_ACCESS_KEY_ID", "deltalake"
    )  # Default MinIO credentials
    aws_secret_key = os.environ.get(
        "AWS_SECRET_ACCESS_KEY", "weloverust"
    )  # Default MinIO secret

    # Configure both s3 and s3a schemes for compatibility
    builder = builder.config("spark.hadoop.fs.s3a.access.key", aws_access_key)
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", aws_endpoint)
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config(
        "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    # Additional S3A configuration for proper connectivity
    builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    builder = builder.config("spark.hadoop.fs.s3a.connection.timeout", "30000")
    builder = builder.config("spark.hadoop.fs.s3a.socket.timeout", "30000")

    # Legacy s3 scheme mapping to s3a for compatibility with older Delta Lake versions
    builder = builder.config(
        "spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    builder = builder.config("spark.hadoop.fs.s3.awsAccessKeyId", aws_access_key)
    builder = builder.config("spark.hadoop.fs.s3.awsSecretAccessKey", aws_secret_key)
    builder = builder.config("spark.hadoop.fs.s3.endpoint", aws_endpoint)

    return delta.pip_utils.configure_spark_with_delta_pip(
        builder,
        extra_packages=[
            "org.apache.spark:spark-hadoop-cloud_2.13:4.0.1",
        ],
    ).getOrCreate()


def assert_spark_read_equal(
    expected: "pa.Table", uri: str, sort_by: list[str] = ["int32"]
):
    from pandas.testing import assert_frame_equal

    spark = get_spark()
    df = spark.read.format("delta").load(uri)

    # Spark and pyarrow don't convert these types to the same Pandas values
    incompatible_types = ["timestamp", "timestamp_ntz", "struct"]

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
