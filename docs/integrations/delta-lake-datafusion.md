# Using Delta Lake with DataFusion

This page explains how to use Delta Lake with DataFusion.

Delta Lake offers DataFusion users better performance and more features compared to other formats like CSV or Parquet.

Delta Lake works well with the DataFusion Rust API and the DataFusion Python API.  It's a great option for all DataFusion users.

Delta Lake also depends on DataFusion to implement SQL-related functionality under the hood.  We will also discuss this dependency at the end of this guide in case you're interested in learning more about the symbiotic relationship between the two libraries.

## Delta Lake performance benefits for DataFusion users

Let's run some DataFusion queries on a Parquet file and a Delta table with the same data to learn more about the performance benefits of Delta Lake.

Suppose you have the following dataset with 1 billion rows and 9 columns.  Here are the first three rows of data:

```
+-------+-------+--------------+-------+-------+--------+------+------+---------+
| id1   | id2   | id3          |   id4 |   id5 |    id6 |   v1 |   v2 |      v3 |
|-------+-------+--------------+-------+-------+--------+------+------+---------|
| id016 | id046 | id0000109363 |    88 |    13 | 146094 |    4 |    6 | 18.8377 |
| id039 | id087 | id0000466766 |    14 |    30 | 111330 |    4 |   14 | 46.7973 |
| id047 | id098 | id0000307804 |    85 |    23 | 187639 |    3 |    5 | 47.5773 |
+-------+-------+--------------+-------+-------+--------+------+------+---------+
```

Here's how to register a Delta Lake table with DataFusion:

```python
from datafusion import SessionContext, col, functions as f
from deltalake import DeltaTable

ctx = SessionContext()
table = DeltaTable("G1_1e9_1e2_0_0")
ctx.register_table_provider("my_delta_table", table)
```

Now query the table:

```python
ctx.sql("select id1, sum(v1) as v1 from my_delta_table where id1='id096' group by id1").show()
```

That query takes 2.8 seconds to execute.

Let's register the same dataset as a Parquet table, run the same query, and compare the runtime difference.

Register the Parquet table and run the query:

```python
path = "G1_1e9_1e2_0_0.parquet"
ctx.register_parquet("my_parquet_table", path)
ctx.sql("select id1, sum(v1) as v1 from my_parquet_table where id1='id096' group by id1")
```

This query takes 5.3 seconds to run.

Parquet stores data in row groups and DataFusion can intelligently skip row groups that don't contain relevant data, so the query is faster than a file format like CSV which doesn't support row group skipping.

Delta Lake stores file-level metadata information in the transaction log, so it can skip entire files when queries are executed.  Delta Lake can skip entire files and then skip row groups within the individual files.  This makes Delta Lake even faster than Parquet files, especially for larger datasets spread across many files.

## Dataframe syntax

You can also use DataFusion's dataframe syntax to run the query above. Please note: unlike Polars or Pandas, all dataframes in DataFusion are lazy.

```python
ctx.table("my_delta_table").filter(col("id1") == "id096").aggregate(
    col("id1"), f.sum(col("v1")).alias("v1")
).show()
```

## Delta Lake features for DataFusion users

Delta Lake also provides other features that are useful for DataFusion users like ACID transactions, concurrency protection, time travel, versioned data, and more.

## Why Delta Lake depends on DataFusion

Delta Lake depends on DataFusion to provide some end-user features.

DataFusion is useful in providing SQL-related Delta Lake features. Some examples:

* Update and merge are written in terms of SQL expressions.
* Invariants and constraints are written in terms of SQL expressions.

Anytime we have to evaluate SQL, we need some sort of SQL engine.  We use DataFusion for that.

## Conclusion

Delta Lake is a great file format for DataFusion users.

Delta Lake also uses DataFusion to provide some end-user features.

DataFusion and Delta Lake have a wonderful symbiotic relationship and play very nicely with each other.

See [this guide for more information on Delta Lake and PyArrow](https://delta-io.github.io/delta-rs/integrations/delta-lake-arrow/) and why PyArrow Datasets are often a better option than PyArrow tables.
