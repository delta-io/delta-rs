# The deltalake package

`deltalake` is an open source library that makes it easier to manage tabular datasets. With `deltalake` you can:
- add, delete or overwrite rows in a dataset as new data arrives
- compact small files into larger files to improve query performance
- time travel back to previous versions of your dataset
- store data in partitioned directories for faster queries
- make faster queries on your dataset by using metadata to skip reading unnecessary files

`deltalake` can be used to manage data stored on a local file system or in the cloud. `deltalake` integrates with data manipulation libraries such as Pandas, Polars, DuckDB and DataFusion for fast queries on your dataset.

deltalake is a lakehouse framework for managing data storage. With this lakehouse approach you manage your datasets with a `DeltaTable` object and then deltalake takes care of the underlying files. Within a `DeltaTable` your data is stored in Parquet files while deltalake stores metadata about the DeltaTable in a set of JSON files called a transaction log.

deltalake is a Rust-based re-implementation of the DeltaLake protocol originally developed  DataBricks. The deltalake library has APIs in Rust and Python. The deltalake library implementation has no dependencies on Java, Spark or DataBricks.

This module provides the capability to read, write, and manage [Delta Lake](https://delta.io/) tables with Python or Rust without Spark or Java. It uses [Apache Arrow](https://arrow.apache.org/) under the hood, so is compatible with other Arrow-native or integrated libraries such as [pandas](https://pandas.pydata.org/), [DuckDB](https://duckdb.org/), and [Polars](https://www.pola.rs/).

## Important terminology

* "Rust deltalake" refers to the Rust API of delta-rs (no Spark dependency)
* "Python deltalake" refers to the Python API of delta-rs (no Spark dependency)
* "Delta Spark" refers to the Scala implementation of the Delta Lake transaction log protocol.  This depends on Spark and Java.

## Why implement the Delta Lake transaction log protocol in Rust and Scala?

Delta Spark depends on Java and Spark, which is fine for many use cases, but not all Delta Lake users want to depend on these libraries.  delta-rs allows using Delta Lake in Rust or other native projects when using a JVM is often not an option.

Python deltalake lets you query Delta tables without depending on Java/Scala.

Suppose you want to query a Delta table with pandas on your local machine.  Python deltalake makes it easy to query the table with a simple `pip install` command - no need to install Java.

## Contributing

The Delta Lake community welcomes contributors from all developers, regardless of your experience or programming background.

You can write Rust code, Python code, documentation, submit bugs, or give talks to the community.  We welcome all of these contributions.

Feel free to [join our Slack](https://go.delta.io/slack) and message us in the #delta-rs channel any time!

We value kind communication and building a productive, friendly environment for maximum collaboration and fun.

## Project history

Check out this video by Denny Lee & QP Hou to learn about the genesis of the delta-rs project:

<iframe width="560" height="315" src="https://www.youtube.com/embed/ZQdEdifcBh8?si=ytGW7FB-kwl6VqsV" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
