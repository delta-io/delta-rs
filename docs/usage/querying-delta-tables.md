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
>>> dt.schema().to_pyarrow()
value: string
year: string
month: string
day: string
>>> dt.to_pandas(partitions=[("year", "=", "2021")], columns=["value"])
      value
0     6
1     7
2     5
3     4
>>> dt.to_pyarrow_table(partitions=[("year", "=", "2021")], columns=["value"])
pyarrow.Table
value: string
```

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
example, you can pass them to `dask.dataframe.read_parquet`:

``` python
>>> import dask.dataframe as dd
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