`deltalake` is an open source library that makes working with tabular datasets easier, more robust and more performant. With deltalake you can add, remove or update rows in a dataset as new data arrives. You can time travel back to earlier versions of a dataset. You can optimize dataset storage from small files to large files. 

`deltalake` can be used to manage data stored on a local file system or in the cloud. `deltalake` integrates with data manipulation libraries such as Pandas, Polars, DuckDB and DataFusion.

`deltalake` uses a lakehouse framework for managing datasets. With this lakehouse approach you manage your datasets with a `DeltaTable` object and then `deltalake` takes care of the underlying files. Within a `DeltaTable` your data is stored in high performance Parquet files while metadata is stored in a set of JSON files called a transaction log.

`deltalake` is a Rust-based re-implementation of the DeltaLake protocol originally developed at DataBricks. The `deltalake` library has APIs in Rust and Python. The `deltalake` implementation has no dependencies on Java, Spark or DataBricks.


## Important terminology

* `deltalake` refers to the Rust or Python API of delta-rs
* "Delta Spark" refers to the Scala implementation of the Delta Lake transaction log protocol.  This depends on Spark and Java.

## Why implement the Delta Lake transaction log protocol in Rust?

Delta Spark depends on Java and Spark, which is fine for many use cases, but not all Delta Lake users want to depend on these libraries.  `deltalake` allows you to manage your dataset using a Delta Lake approach without any Java or Spark dependencies.

A `DeltaTable` on disk is simply a directory that stores metadata in JSON files and data in Parquet files.  

## Quick start

You can install `deltalake` in Python with `pip`
```bash
pip install deltalake
```
We create a Pandas `DataFrame` and write it to a `DeltaTable`:
```python
import pandas as pd
from deltalake import DeltaTable,write_deltalake

df = pd.DataFrame(
    {
        "id": [1, 2, 3],
        "name": ["Aadhya", "Bob", "Chen"],
    }
)

(
    write_deltalake(
        table_or_uri="delta_table_dir",
        data=df,
    )
)
```
We create a `DeltaTable` object that holds the metadata for the Delta table:
```python
dt = DeltaTable("delta_table_dir")
```
We load the `DeltaTable` into a Pandas `DataFrame` with `to_pandas` on a `DeltaTable`:
```python
new_df = dt.to_pandas()
```

Or we can load the data into a Polars `DataFrame` with `pl.read_delta`:
```python
import polars as pl
new_df = pl.read_delta("delta_table_dir")
```

Or we can load the data with DuckDB:
```python
import duckdb
duckdb.query("SELECT * FROM delta_scan('./delta_table_dir')")
```

Or we can load the data with DataFusion:
```python
from datafusion import SessionContext

ctx = SessionContext()
ctx.register_dataset("my_delta_table", dt.to_pyarrow_dataset())
ctx.sql("select * from my_delta_table")
```


## Contributing

The Delta Lake community welcomes contributors from all developers, regardless of your experience or programming background.

You can write Rust code, Python code, documentation, submit bugs, or give talks to the community.  We welcome all of these contributions.

Feel free to [join our Slack](https://go.delta.io/slack) and message us in the #delta-rs channel any time!

We value kind communication and building a productive, friendly environment for maximum collaboration and fun.

## Project history

Check out this video by Denny Lee & QP Hou to learn about the genesis of the delta-rs project:

<iframe width="560" height="315" src="https://www.youtube.com/embed/ZQdEdifcBh8?si=ytGW7FB-kwl6VqsV" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
