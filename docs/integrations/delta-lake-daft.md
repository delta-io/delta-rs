# Using Delta Lake with Daft

[Daft](https://www.getdaft.io) is a framework for ETL, analytics, and ML/AI at scale with a familiar Python dataframe API, implemented in Rust.

For Delta Lake users, Daft is a great data processing tool because it gives you the following features:

- **Skipping Filtered Data**: Daft implements automatic partition pruning and stats-based file pruning for filter predicates, skipping data that doesn’t need to be read.
- **Multimodal Dataframes**: read, write and transform multimodal data incl. images, JSON, PDF, audio, etc.
- **Parallel + Distributed Reads**: Daft parallelizes Delta Lake table reads over all cores of your machine, if using the default multithreading runner, or all cores + machines of your Ray cluster, if using the distributed Ray runner.
- **Multi-cloud Support**: Daft supports reading Delta Lake tables from AWS S3, Azure Blob Store, and GCS, as well as local files.

Daft and Delta Lake work really well together. Daft provides unified compute for Delta Lake’s unified storage. Together, Delta Lake and Daft give you high-performance query optimization and distributed compute on massive datasets.

## Installing Daft for Delta Lake

The easiest way to use Delta Lake format with Daft DataFrames is to install Daft with the `[deltalake]` extras using `pip`:

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

## What can I do with a Daft DataFrame?

Daft gives you [full-featured DataFrame functionality](https://www.getdaft.io/projects/docs/en/latest/user_guide/basic_concepts.html), similar to what you might be used to from pandas, Dask or PySpark.

On top of this, Daft also gives you:

- **Multimodal data type support** to work with Images, URLs, Tensors and more
- **Expressions API** for easy column transformations
- **UDFs** for multi-column transformation, incl. ML applications

Let's take a quick look at some of Daft's basic DataFrame operations.

You can **select columns** from your DataFrame using the `select` method. We'll use the `show` method to show the first `n` rows (defaults to 10):

```python
> df.select("first_name", "country").show()

|    | first_name   | country   |
|---:|:-------------|:----------|
|  0 | Ernesto      | Argentina |
|  1 | Bruce        | China     |
|  2 | Jack         | China     |
|  3 | Wolfgang     | Germany   |
|  4 | Soraya       | Germany   |
```

You can **sort** your Daft DataFrame using the `sort` method:

```python
> df.sort(df["country"], desc=True).show()

|    | first_name   | last_name   | country   | continent   |
|---:|:-------------|:------------|:----------|:------------|
|  0 | Wolfgang     | Manche      | Germany   | NaN         |
|  1 | Soraya       | Jala        | Germany   | NaN         |
|  2 | Bruce        | Lee         | China     | Asia        |
|  3 | Jack         | Ma          | China     | Asia        |
|  4 | Ernesto      | Guevara     | Argentina | NaN         |
```

You can **filter** your DataFrame using the `where` method:

```python
> df.where(df["continent"] == "Asia").show()

|    | first_name   | last_name   | country   | continent   |
|---:|:-------------|:------------|:----------|:------------|
|  0 | Bruce        | Lee         | China     | Asia        |
|  1 | Jack         | Ma          | China     | Asia        |
```

You can group your DataFrame by a specific columns using the `groupby` method. You can then specify the aggregation method, in this case using the `count` aggregator:

```python
> df.select("first_name", "country").groupby(df["country"]).count("first_name").show()

|    | country   |   first_name |
|---:|:----------|-------------:|
|  0 | Germany   |            2 |
|  1 | China     |            2 |
|  2 | Argentina |            1 |
```

Check out the [Daft User Guide](https://www.getdaft.io/projects/docs/en/latest/user_guide/index.html) for a complete list of DataFrame operations.

## Data Skipping Optimizations

You may have noticed the Delta Lake warning at the top when we first called `collect` on our DataFrame:

<div class="alert alert-block alert-danger">WARNING: has partitioning keys = [PartitionField(country#Utf8)], but no partition filter was specified. This will result in a full table scan.</div>

Delta Lake is informing us that the data is partitioned on the `country` column.

Daft's native query optimizer has access to all of the Delta Lake metadata.

This means it can optimize your query by skipping the partitions that are not relevant for this query. Instead of having to read all 3 partitions, we can read only 1 and get the same result, just faster!

```python
# Filter on partition columns will result in efficient partition pruning; non-matching partitions will be skipped.
> df.where(df["country"] == "Germany").show()

|    | first_name   | last_name   | country   |   continent |
|---:|:-------------|:------------|:----------|------------:|
|  0 | Wolfgang     | Manche      | Germany   |         nan |
|  1 | Soraya       | Jala        | Germany   |         nan |
```

You can use the `explain` method to see how Daft is optimizing your query. Since we've already called `collect` on our DataFrame, it is already in memory. So below we copy the output of `explain(show_all=True)` **before** calling `collect`:

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

Running a query on a non-partitioned column like `continent` will require reading in all partitions, totalling 3045 bytes in this case.

Instead, running a query on a partitioned column (`country` in this case) means Daft only has to read only the relevant partition, saving us a whopping 2000+ bytes in this toy example :)

You can read [High-Performance Querying on Massive Delta Lake Tables with Daft](https://delta.io/blog/daft-delta-lake-integration/) for an in-depth benchmarking of query optimization with Delta Lake and Daft.

## Transform columns with Expressions

Daft provides a flexible [Expressions](https://www.getdaft.io/projects/docs/en/latest/api_docs/expressions.html) API for defining computation that needs to happen over your columns.

For example, we can use `daft.col()` expressions together with the `with_column` method to create a new column `full_name`, joining the contents of the `last_name` column to the `first_name` column:

```python
> df_full = df.with_column("full_name", daft.col('first_name') + ' ' + daft.col('last_name'))
> df_full.show()

|    | first_name   | last_name   | country   | continent   | full_name       |
|---:|:-------------|:------------|:----------|:------------|:----------------|
|  0 | Ernesto      | Guevara     | Argentina | NaN         | Ernesto Guevara |
|  1 | Bruce        | Lee         | China     | Asia        | Bruce Lee       |
|  2 | Jack         | Ma          | China     | Asia        | Jack Ma         |
|  3 | Wolfgang     | Manche      | Germany   | NaN         | Wolfgang Manche |
|  4 | Soraya       | Jala        | Germany   | NaN         | Soraya Jala     |
```

## Multimodal Data Type Support

Daft has a rich multimodal type-system with support for Python objects, Images, URLs, Tensors and more.

Daft columns can contain any Python objects. For example, let's add a column containing a Python class `Dog` for some of the people in our dataset:

```python
> import numpy as np

> class Dog:
>     def __init__(self, name):
>         self.name = name

>     def bark(self):
>         return f"{self.name}!"

> df_dogs = daft.from_pydict({
>     'full_name': ['Ernesto Guevara','Bruce Lee','Jack Ma','Wolfgang Manche','Soraya Jala'],
>     "dogs": [Dog("ruffles"), Dog("shnoodles"), Dog("waffles"), Dog("doofus"), Dog("Fluffles")],
> })

> df_dogs.show()

|    | full_name       | dogs                                 |
|---:|:----------------|:-------------------------------------|
|  0 | Ernesto Guevara | <__main__.Dog object at 0x1603d1c10> |
|  1 | Bruce Lee       | <__main__.Dog object at 0x126ab9b90> |
|  2 | Jack Ma         | <__main__.Dog object at 0x1603d27d0> |
|  3 | Wolfgang Manche | <__main__.Dog object at 0x1603d1cd0> |
|  4 | Soraya Jala     | <__main__.Dog object at 0x1603d3f50> |

```

You can join this new `dogs` column to your existing DataFrame using the `join` method:

```python
> df_family = df_full.join(df_dogs, on=["full_name"])
> df_family.show()

|    | full_name       | first_name   | last_name   | country   | continent   | dogs                                 |
|---:|:----------------|:-------------|:------------|:----------|:------------|:-------------------------------------|
|  0 | Ernesto Guevara | Ernesto      | Guevara     | Argentina | NaN         | <__main__.Dog object at 0x1603d1c10> |
|  1 | Bruce Lee       | Bruce        | Lee         | China     | Asia        | <__main__.Dog object at 0x126ab9b90> |
|  2 | Jack Ma         | Jack         | Ma          | China     | Asia        | <__main__.Dog object at 0x1603d27d0> |
|  3 | Wolfgang Manche | Wolfgang     | Manche      | Germany   | NaN         | <__main__.Dog object at 0x1603d1cd0> |
|  4 | Soraya Jala     | Soraya       | Jala        | Germany   | NaN         | <__main__.Dog object at 0x1603d3f50> |
```

We can then use the `apply` method to apply a function to each instance of the Dog class:

```python
> from daft import DataType

> df_family = df_family.with_column(
>     "dogs_bark_name",
>     df_family["dogs"].apply(lambda dog: dog.bark(), return_dtype=DataType.string()),
> )

> df_family.show()

|    | first_name   | last_name   | country   | continent   | full_name       | dogs                                 | dogs_bark_name   |
|---:|:-------------|:------------|:----------|:------------|:----------------|:-------------------------------------|:-----------------|
|  0 | Ernesto      | Guevara     | Argentina | NaN         | Ernesto Guevara | <__main__.Dog object at 0x1603d1c10> | ruffles!         |
|  1 | Bruce        | Lee         | China     | Asia        | Bruce Lee       | <__main__.Dog object at 0x126ab9b90> | shnoodles!       |
|  2 | Jack         | Ma          | China     | Asia        | Jack Ma         | <__main__.Dog object at 0x1603d27d0> | waffles!         |
|  3 | Wolfgang     | Manche      | Germany   | NaN         | Wolfgang Manche | <__main__.Dog object at 0x1603d1cd0> | doofus!          |
|  4 | Soraya       | Jala        | Germany   | NaN         | Soraya Jala     | <__main__.Dog object at 0x1603d3f50> | Fluffles!        |
```

Daft DataFrames can also contain [many other data types](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html), like tensors, JSON, URLs and images. The Expressions API provides useful tools to work with these data types.

Take a look at the notebook in the `delta-examples` Github repository for a closer look at how Daft handles URLs, images and ML applications.

## Transform multiple columns with UDFs

You can use User-Defined Functions (UDFs) to run functions over multiple rows or columns:

```python
> from daft import udf

> @udf(return_dtype=DataType.string())
> def custom_bark(dog_series, owner_series):
>     return [
>         f"{dog.name} loves {owner_name}!"
>         for dog, owner_name
>         in zip(dog_series.to_pylist(), owner_series.to_pylist())
>     ]

> df_family = df_family.with_column("custom_bark", custom_bark(df_family["dogs"], df_family["first_name"]))
> df_family.select("full_name", "dogs_bark_name", "custom_bark").show()

|    | full_name       | dogs_bark_name   | custom_bark            |
|---:|:----------------|:-----------------|:-----------------------|
|  0 | Ernesto Guevara | ruffles!         | ruffles loves Ernesto! |
|  1 | Bruce Lee       | shnoodles!       | shnoodles loves Bruce! |
|  2 | Jack Ma         | waffles!         | waffles loves Jack!    |
|  3 | Wolfgang Manche | doofus!          | doofus loves Wolfgang! |
|  4 | Soraya Jala     | Fluffles!        | Fluffles loves Soraya! |
```

Daft supports workloads with [many more data types](https://www.getdaft.io/projects/docs/en/latest/api_docs/datatype.html) than traditional DataFrame APIs.

By combining multimodal data support with the UDF functionality you can [run ML workloads](https://www.getdaft.io/projects/docs/en/latest/user_guide/tutorials.html#mnist-digit-classification) right within your DataFrame.

## When should I use Daft DataFrames?

Daft DataFrames are designed for multimodal, distributed workloads.

You may want to consider using Daft if you're working with:

1. **Large datasets** that don't fit into memory or would benefit from parallelization
2. **Multimodal data types**, such as images, JSON, vector embeddings, and tensors
3. **ML workloads** that would benefit from interactive computation within DataFrame (via UDFs)

Take a look at the [Daft tutorials](https://www.getdaft.io/projects/docs/en/latest/user_guide/tutorials.html) for in-depth examples of each use case.

## Contribute to `daft`

Excited about Daft and want to contribute? Join them on [Github](https://github.com/Eventual-Inc/Daft) 🚀

Like many technologies, Daft collects some non-identifiable telemetry to improve the product. This is stricly non-identifiable metadata. You can disable telemetry by setting the following environment variable: `DAFT_ANALYTICS_ENABLED=0`. Read more in the [Daft documentation](https://www.getdaft.io/projects/docs/en/latest/faq/telemetry.html).
