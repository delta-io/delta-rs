# Using Delta Lake with Sail

[Sail](https://github.com/lakehq/sail) is an open-source multimodal distributed compute framework, built in Rust, unifying batch, streaming, and AI workloads. For seamless adoption, Sail offers a drop-in replacement for the Spark SQL and DataFrame APIs in both single-host and distributed settings. You can use Sail for both ad-hoc queries on your laptop and large-scale data processing jobs in a cluster.

Delta Lake is a natural fit for Sail. Delta Lake provides a reliable storage layer with strong data management guarantees, while Sail focuses on flexible, unified multimodal compute. By combining the two, you can start with a familiar PySpark client, while benefiting from Sail’s native support for Delta Lake tables and ensuring interoperability with existing Delta datasets.

## Getting Started with Sail

### Installing Sail

The easiest way to get started is by installing PySail and the PySpark client.

```bash
pip install "pysail"
pip install "pyspark-client"
```

### Using the Sail Library

You can use the Sail library to start a Spark Connect server and connect to it using PySpark.

```python
from pysail.spark import SparkConnectServer
from pyspark.sql import SparkSession

server = SparkConnectServer()
server.start()
_, port = server.listening_address

spark = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
```

<br>
In all the examples below, `spark` refers to the Spark session connected to the Sail server.

## Basic Delta Lake Usage in Sail

### DataFrame API

```python
path = "file:///tmp/sail/users"
df = spark.createDataFrame(
    [(1, "Alice"), (2, "Bob")],
    schema="id INT, name STRING",
)

# This creates a new table or overwrites an existing one.
df.write.format("delta").mode("overwrite").save(path)
# This appends data to an existing Delta table.
df.write.format("delta").mode("append").save(path)

df = spark.read.format("delta").load(path)
df.show()
```

### SQL

```python
spark.sql("""
CREATE TABLE users (id INT, name STRING)
USING delta
LOCATION 'file:///tmp/sail/users'
""")

spark.sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
spark.sql("SELECT * FROM users").show()
```

## Data Partitioning

Partitioned Delta tables organize data into directories based on the values of one or more columns. This improves query performance by skipping data files that do not match the filter conditions.

### DataFrame API

```python
path = "file:///tmp/sail/metrics"
df = spark.createDataFrame(
    [(2024, 1.0), (2025, 2.0)],
    schema="year INT, value FLOAT",
)

df.write.format("delta").mode("overwrite").partitionBy("year").save(path)

df = spark.read.format("delta").load(path).filter("year > 2024")
df.show()
```

### SQL

```python
spark.sql("""
CREATE TABLE metrics (year INT, value FLOAT)
USING delta
LOCATION 'file:///tmp/sail/metrics'
PARTITIONED BY (year)
""")

spark.sql("INSERT INTO metrics VALUES (2024, 1.0), (2025, 2.0)")
spark.sql("SELECT * FROM metrics WHERE year > 2024").show()
```

## Schema Evolution

Delta Lake handles schema evolution gracefully. By default, if you try to write data with a different schema than the one of the existing Delta table, an error will occur. You can enable schema evolution by setting the `mergeSchema` option to `true` when writing  data. In this case, if you change the data type of an existing column to a compatible type, or add a new column, Delta Lake will automatically update the schema of the table.

```python
df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
```

You can also use the `overwriteSchema` option to overwrite the schema of an existing Delta table. But this works only if you set the write mode to `overwrite`.

```python
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
```

## Time Travel

You can use the time travel feature of Delta Lake to query historical versions of a Delta table.

```python
df = spark.read.format("delta").option("versionAsOf", "0").load(path)
```

## Contribute to Sail

You can refer to the [Sail documentation](https://docs.lakesail.com/sail/latest/) for more information, such as reading or writing Delta tables stored in various object storages supported by Sail.

Excited about Sail and want to contribute? Join them on [GitHub](https://github.com/lakehq/sail)! 🚀