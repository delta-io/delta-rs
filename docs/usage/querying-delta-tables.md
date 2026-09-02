# Querying Delta Tables

Delta tables can be queried in several ways. By loading as Arrow data or
an Arrow dataset, they can be used by compatible engines such as Pandas
and DuckDB. By passing on the list of files, they can be loaded into
other engines such as Dask.

Delta tables are often larger than can fit into memory on a single
computer, so this module provides ways to read only the parts of the
data you need. Partition filters allow you to skip reading files that
are part of irrelevant partitions. Only loading the columns required
also saves memory. Finally, some methods allow reading tables
batch-by-batch, allowing you to process the whole table while only
having a portion loaded at any given time.

To load into Pandas or a PyArrow table use the `DeltaTable.to_pandas` and `DeltaTable.to_pyarrow_table` methods, respectively. Both of these support filtering partitions and selecting particular columns.

``` python
>>> from deltalake import DeltaTable
>>> dt = DeltaTable("../rust/tests/data/delta-0.8.0-partitioned")
>>> dt.schema().to_arrow()
value: string
year: string
month: string
day: string
>>> dt.to_pandas(filters=[("year", "=", "2021")], columns=["value"])
      value
0     6
1     7
2     5
3     4
>>> dt.to_pyarrow_table(filters=[("year", "=", "2021")], columns=["value"])
pyarrow.Table
value: string
```

## Choosing an engine

`to_pyarrow_table` and `to_pandas` take an `engine` argument: `"pyarrow"`
(the deprecated default) scans through a PyArrow dataset, `"datafusion"`
reads with the built-in DataFusion engine. The datafusion engine also reads
tables the pyarrow engine cannot (column mapping, deletion vectors), prunes
files automatically from `filters`, and streams internally instead of
materializing dataset fragments.

``` python
>>> dt.to_pandas(engine="datafusion", filters=[("year", "=", "2021")], columns=["value"])
>>> dt.to_pandas(engine="datafusion", filters="year = '2021'", columns=["value"])
```

Under the datafusion engine, `filters` also accepts a SQL predicate string,
passed to the engine as written; tuple filters are sugar compiled to the same
SQL semantics.

The engines agree on tuple filters in the common cases but differ where
PyArrow and SQL semantics genuinely part ways. Migrating code should account
for:

- `("col", "not in", [...])` follows SQL three-valued logic: rows where
  `col` is NULL do not match. The pyarrow engine keeps them.
- `("col", "=", None)` matches NULL rows, consistent with the tuple-filter
  tradition of the listing APIs. The pyarrow engine matches nothing.
- Duplicate names in `columns=` are an error; the pyarrow engine returns the
  column twice.
- Row order follows query execution and is not deterministic across calls.
- `filesystem=`, `pyarrow.dataset.Expression` filters, and the deprecated
  `partitions` argument are pyarrow-engine concepts and are rejected;
  the datafusion engine prunes files from `filters` on its own.

These divergences are pinned by tests in `python/tests/test_engines.py`. The
pyarrow engine is deprecated wholesale: any call that resolves to it warns,
and it will be removed in a future release.

## Selecting files with a pruning predicate

`file_pruning_predicate` selects which files a table's log lists, before any
data is read. It takes either a SQL string or tuple filters. A flat list of
tuples is a conjunction (AND); a list of such lists is an OR across the inner
AND groups:

``` python
>>> # SQL string
>>> dt.file_uris(file_pruning_predicate="(year = 2020 AND month = 2) OR (year = 2021 AND month = 12)")
>>> # flat list of tuples: a single conjunction, year = 2020 AND month = 2
>>> dt.file_uris(file_pruning_predicate=[("year", "=", "2020"), ("month", "=", "2")])
>>> # list of lists: OR across the inner AND groups
>>> dt.file_uris(
...     file_pruning_predicate=[
...         [("year", "=", "2020"), ("month", "=", "2")],
...         [("year", "=", "2021"), ("month", "=", "12")],
...     ],
... )
```

The parameter is accepted on `file_uris`, `partitions`, and
`to_pyarrow_dataset`. The older `partitions` and `partition_filters`
parameters on these methods still work but are deprecated in its favor. The
predicate may reference any column, not just partition columns:

- **Partition columns** prune exactly: every returned file matches the
  predicate, and only matching files are returned.
- **Other columns** prune conservatively using the per-file min/max statistics
  in the transaction log: files that provably contain no matching row are
  dropped, every file that *may* contain one is kept, and files without
  statistics for a referenced column are always kept. The result is a
  complete superset of the matching files. How much gets pruned depends on the
  data layout: statistics only rule out files when similar values are
  colocated, for example by partitioning or z-ordering on the column. See
  [Delta Lake File Skipping](../how-delta-lake-works/delta-lake-file-skipping.md)
  for how to lay out tables so predicates prune well.

As the name says, the predicate prunes files, not rows. On `to_pandas` and
`to_pyarrow_table` there is no pruning parameter: `filters` prunes files just
as effectively through the per-fragment statistics and then filters the
surviving rows exactly. Both methods are thin wrappers over
`to_pyarrow_dataset`, so to prune during log replay instead, before any
per-file fragment setup (which can matter on tables with very large file
counts), unroll the chain:

``` python
>>> dt.to_pandas(filters=[("value", ">=", "5")])
>>> # unrolled: prune files first, then read; whole surviving files come
>>> # back unless you also pass a row filter to to_table
>>> dt.to_pyarrow_dataset(file_pruning_predicate="value >= '5'").to_table().to_pandas()
```

The full syntax for both forms is documented on
[`DeltaTable.file_uris`](../api/delta_table/index.md).

## Lazy reads with the built-in engine

`DeltaTable.scan` reads the table with the built-in DataFusion engine and
returns an Arrow `RecordBatchReader`, so batches stream as they are produced
and can be handed to any Arrow-native engine (PyArrow, DuckDB, Polars).
Unlike `file_pruning_predicate`, its `predicate` filters rows: the result
contains exactly the matching rows. It also reads tables that
`to_pyarrow_dataset` cannot, such as those using column mapping or deletion
vectors.

``` python
>>> reader = dt.scan(columns=["value"], predicate="year = '2021'")
>>> reader.read_all()
```

The predicate follows DataFusion SQL semantics: three-valued NULL logic, SQL
type coercion. These differ from PyArrow dataset filter expressions in edge
cases, so take care when swapping one read path for the other.

Converting to a PyArrow Dataset allows you to filter on columns other
than partition columns and load the result as a stream of batches rather
than a single table. Convert to a dataset using
`DeltaTable.to_pyarrow_dataset`. Filters
applied to datasets will use the partition values and file statistics
from the Delta transaction log and push down any other filters to the
scanning operation.

``` python
>>> import pyarrow.dataset as ds
>>> dataset = dt.to_pyarrow_dataset()
>>> condition = (ds.field("year") == "2021") & (ds.field("value") > "4")
>>> dataset.to_table(filter=condition, columns=["value"]).to_pandas()
  value
0     6
1     7
2     5
>>> batch_iter = dataset.to_batches(filter=condition, columns=["value"], batch_size=2)
>>> for batch in batch_iter: print(batch.to_pandas())
  value
0     6
1     7
  value
0     5
```

PyArrow datasets may also be passed to compatible query engines, such as
[DuckDB](https://duckdb.org/docs/api/python/overview.html)

``` python
>>> import duckdb
>>> ex_data = duckdb.arrow(dataset)
>>> ex_data.filter("year = 2021 and value > 4").project("value")
---------------------
-- Expression Tree --
---------------------
Projection [value]
  Filter [year=2021 AND value>4]
    arrow_scan(140409099470144, 4828104688, 1000000)

---------------------
-- Result Columns  --
---------------------
- value (VARCHAR)

---------------------
-- Result Preview  --
---------------------
value
VARCHAR
[ Rows: 3]
6
7
5
```

Finally, you can always pass the list of file paths to an engine. For
example, you can pass them to `dask.dataframe.read_parquet`. `file_uris`
accepts the same predicates, so the file list can be pruned before it ever
reaches the other engine:

``` python
>>> import dask.dataframe as dd
>>> df = dd.read_parquet(dt.file_uris(file_pruning_predicate="year = 2021 OR month = 2"))
>>> df = dd.read_parquet(dt.file_uris())
>>> df
Dask DataFrame Structure:
                value             year            month              day
npartitions=6
               object  category[known]  category[known]  category[known]
                  ...              ...              ...              ...
...               ...              ...              ...              ...
                  ...              ...              ...              ...
                  ...              ...              ...              ...
Dask Name: read-parquet, 6 tasks
>>> df.compute()
  value  year month day
0     1  2020     1   1
0     2  2020     2   3
0     3  2020     2   5
0     4  2021     4   5
0     5  2021    12   4
0     6  2021    12  20
1     7  2021    12  20
```

When working with the Rust API, Apache Datafusion can be used to query data from a delta table.

```rust
let delta_path = Url::from_directory_path("/rust/tests/data/delta-0.8.0-partitioned").unwrap();
let table = deltalake::open_table(delta_path).await?;
let ctx = SessionContext::new();
ctx.register_table("simple_table", table.table_provider().await?)?;
let df = ctx.sql("SELECT value FROM simple_table WHERE year = 2021").await?;
df.show().await?;
```

Apache Datafusion also supports a Dataframe interface that can be used instead of the SQL interface:
```rust
let delta_path = Url::from_directory_path("/rust/tests/data/delta-0.8.0-partitioned").unwrap();
let table = deltalake::open_table(delta_path).await?;
let ctx = SessionContext::new();
let dataframe = ctx.read_table(table.table_provider().await?)?;
let df = dataframe.filter(col("year").eq(lit(2021)))?.select(vec![col("value")])?;
df.show().await?;
```
