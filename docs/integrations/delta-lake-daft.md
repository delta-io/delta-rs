# Using Delta Lake with Daft

[Daft](https://www.getdaft.io) is a framework for ETL, analytics, and ML/AI at scale with a familiar Python dataframe API, implemented in Rust.

Daft and Delta Lake work really well together. Daft provides unified compute for Delta Lake’s unified storage. Together, Delta Lake and Daft give you high-performance query optimization and distributed compute on massive datasets.

Delta Lake is a great storage format for Daft workloads. Delta gives Daft users:

- **Query optimization** via file-skipping and column pruning
- **Versioning** for easy time travel functionality
- **Faster reads** via Z-ordering
- **ACID transactions** and **schema enforcement** for more reliable reads and writes

For Delta Lake users, Daft is a great data processing tool because it gives you the following features:

- **Multimodal Dataframes**: read, write and transform multimodal data incl. images, JSON, PDF, audio, etc.
- **Parallel + Distributed Reads**: Daft parallelizes Delta Lake table reads over all cores of your machine, if using the default multithreading runner, or all cores + machines of your Ray cluster, if using the distributed Ray runner.
- **Skipping Filtered Data**: Daft implements automatic partition pruning and stats-based file pruning for filter predicates, skipping data that doesn’t need to be read.

Let's look at how to use Delta Lake with Daft.

## Installing Daft for Delta Lake

The easiest way to use the Delta Lake table format with Daft DataFrames is to install Daft with the `[deltalake]` extras using `pip`:

```python
!pip install -U "getdaft[deltalake]"
```

This adds the `deltalake` Python package to your install. This package is used to fetch metadata about the Delta Lake table, such as paths to the underlying Parquet files and table statistics. You can of course also install the `deltalake` manually.

## Read Delta Lake into a Daft DataFrame

You can easily read Delta Lake tables into a Daft DataFrame using the `read_delta_lake` method. Let's use it to read some data stored in a Delta Lake on disk. You can access the data stored as a Delta Lake [on Github](https://github.com/delta-io/delta-examples/tree/master/data/people_countries_delta_dask)

```python
import daft

# read delta table into Daft DataFrame
df = daft.read_delta_lake("path/to/delta_table")
```

You can also read in Delta Lake data from remote sources like S3:

```python
# table_uri = (
#     "s3://daft-public-datasets/red-pajamas/"
#     "stackexchange-sample-north-germanic-deltalake"
# )
# df = daft.read_delta_lake(table_uri)
```

```python
df
```

<div>
<table class="dataframe">
<thead><tr><th style="text-wrap: nowrap; max-width:192px; overflow:auto; text-align:left">first_name<br />Utf8</th><th style="text-wrap: nowrap; max-width:192px; overflow:auto; text-align:left">last_name<br />Utf8</th><th style="text-wrap: nowrap; max-width:192px; overflow:auto; text-align:left">country<br />Utf8</th><th style="text-wrap: nowrap; max-width:192px; overflow:auto; text-align:left">continent<br />Utf8</th></tr></thead>
</table>
<small>(No data to display: Dataframe not materialized)</small>
</div>

Daft DataFrames are lazy by default. This means that the contents will not be computed ("materialized") unless you explicitly tell Daft to do so. This is best practice for working with larger-than-memory datasets and parallel/distributed architectures.

The Delta table we have just loaded only has 5 rows. You can materialize it in memory using the `.collect` method:

```python
> df.collect()

|    | first_name   | last_name   | country   | continent   |
|---:|:-------------|:------------|:----------|:------------|
|  0 | Ernesto      | Guevara     | Argentina | NaN         |
|  1 | Bruce        | Lee         | China     | Asia        |
|  2 | Jack         | Ma          | China     | Asia        |
|  3 | Wolfgang     | Manche      | Germany   | NaN         |
|  4 | Soraya       | Jala        | Germany   | NaN         |
```

## Write to Delta Lake

You can use `write_deltalake` to write a Daft DataFrame to a Delta table:

```
df.write_deltalake("tmp/daft-table", mode="overwrite")
```

Daft supports multiple write modes. See the [Daft documentation](https://www.getdaft.io/projects/docs/en/latest/api_docs/doc_gen/dataframe_methods/daft.DataFrame.write_deltalake.html#daft.DataFrame.write_deltalake) for more information.

## What can I do with a Daft DataFrame?

Daft gives you [full-featured DataFrame functionality](https://www.getdaft.io/projects/docs/en/latest/user_guide/basic_concepts.html), similar to what you might be used to from pandas, Dask or PySpark.

On top of this, Daft also gives you:

- **Multimodal data type support** to work with Images, URLs, Tensors and more
- **Expressions API** for easy column transformations
- **UDFs** for multi-column transformation, incl. ML applications

Check out the [Daft User Guide](https://www.getdaft.io/projects/docs/en/latest/user_guide/index.html) for a complete list of DataFrame operations.

## Data Skipping Optimizations

Delta Lake and Daft work together to give you highly-optimized query performance.

Delta Lake stores your data in Parquet files. Parquet is a columnar row format that natively supports column pruning. If your query only needs to read data from a specific column or set of columns, you don't need to read in the entire file. This can save you lots of time and compute.

Delta Lake goes beyond the basic Parquet features by also giving you:

- partitioned reads
- file skipping via z-ordering.

This is great for Daft users who want to run efficient queries on large-scale data.

Let's look at how this works.

### Partitioned Reads

You may have noticed the Delta Lake warning at the top when we first called `collect()` on our DataFrame:

> `WARNING: has partitioning keys = [PartitionField(country#Utf8)], but no partition filter was specified. This will result in a full table scan.`

Delta Lake is informing us that the data is partitioned on the `country` column.

Daft does some nice magic here to help you out. The Daft query optimizer has access to all of the Delta Lake metadata. This means it can optimize your query by skipping the partitions that are not relevant for this query. Instead of having to read all 3 partitions, we can read only 1 and get the same result, just faster!

```python
# Filter on partition columns will result in efficient partition pruning; non-matching partitions will be skipped.
> df.where(df["country"] == "Germany").show()

|    | first_name   | last_name   | country   |   continent |
|---:|:-------------|:------------|:----------|------------:|
|  0 | Wolfgang     | Manche      | Germany   |         nan |
|  1 | Soraya       | Jala        | Germany   |         nan |
```

You can use the `explain()` method to see how Daft is optimizing your query.

> Since we've already called `collect` on our DataFrame, it is already in memory. So below we copy the output of `explain(show_all=True)` **before** calling `collect`:

Running `df.where(df["continent"] == "Asia").explain(True)` returns:

```
(...)

== Optimized Logical Plan ==

* PythonScanOperator: DeltaLakeScanOperator(None)
|   File schema = first_name#Utf8, last_name#Utf8, country#Utf8, continent#Utf8
|   Partitioning keys = [PartitionField(country#Utf8)]
|   Filter pushdown = col(continent) == lit("Asia")
|   Output schema = first_name#Utf8, last_name#Utf8, country#Utf8, continent#Utf8


== Physical Plan ==

* TabularScan:
|   Num Scan Tasks = 3
|   Estimated Scan Bytes = 3045
|   Clustering spec = { Num partitions = 3 }
```

Whereas running `df.where(df["country"] == "Germany").explain(True)` returns:

```
(...)

== Optimized Logical Plan ==

* PythonScanOperator: DeltaLakeScanOperator(None)
|   File schema = first_name#Utf8, last_name#Utf8, country#Utf8, continent#Utf8
|   Partitioning keys = [PartitionField(country#Utf8)]
|   Partition Filter = col(country) == lit("Germany")
|   Output schema = first_name#Utf8, last_name#Utf8, country#Utf8, continent#Utf8


== Physical Plan ==

* TabularScan:
|   Num Scan Tasks = 1
|   Estimated Scan Bytes = 1025
|   Clustering spec = { Num partitions = 1 }
```

Running a query on a non-partitioned column like `continent` will require reading in all partitions, totalling 3045 bytes in the case of this toy example.

Instead, running a query on a partitioned column (`country` in this case) means Daft only has to read only the relevant partition, saving us a ~60% of the compute. This has huge impacts when you're working at scale.

### Z-Ordering for enhanced file skipping

[Z-ordering](https://delta.io/blog/2023-06-03-delta-lake-z-order/) stores similar data close together to optimize query performance. This is especially useful when you're querying on one or multiple columns.

Using Z-Ordered Delta tables instead of regular Parquet can give Daft users significant speed-ups.

Read [High-Performance Querying on Massive Delta Lake Tables with Daft](https://delta.io/blog/daft-delta-lake-integration/) for an in-depth benchmarking of query optimization with Delta Lake and Daft using partitioning and Z-ordering.

## Daft gives you Multimodal Data Type Support

Daft has a rich multimodal type-system with support for Python objects, Images, URLs, Tensors and more.

The [Expressions API](https://www.getdaft.io/projects/docs/en/latest/api_docs/expressions.html) provides useful tools to work with these data types. By combining multimodal data support with the [User-Defined Functions API](https://www.getdaft.io/projects/docs/en/latest/api_docs/udf.html) you can [run ML workloads](https://www.getdaft.io/projects/docs/en/latest/user_guide/tutorials.html#mnist-digit-classification) right within your DataFrame.

Take a look at the notebook in the [`delta-examples` Github repository](https://github.com/delta-io/delta-examples) for a closer look at how Daft handles URLs, images and ML applications.

## Contribute to `daft`

Excited about Daft and want to contribute? Join them on [Github](https://github.com/Eventual-Inc/Daft) 🚀

Like many technologies, Daft collects some non-identifiable telemetry to improve the product. This is stricly non-identifiable metadata. You can disable telemetry by setting the following environment variable: `DAFT_ANALYTICS_ENABLED=0`. Read more in the [Daft documentation](https://www.getdaft.io/projects/docs/en/latest/faq/telemetry.html).
