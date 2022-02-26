"""Generate test data."""
# TODO: Once we have a writer, replace this script.
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

builder = (
    SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.range(5).repartition(1).withColumn("date", F.lit("2021-01-01").cast("date"))
df.write.partitionBy("date").format("delta").save("date_partitioned_df")

df = (
    spark.range(5)
    .repartition(1)
    .withColumn("date", F.lit("2021-01-01").cast("timestamp"))
)
df.write.partitionBy("date").format("delta").save("timestamp_partitioned_df")
