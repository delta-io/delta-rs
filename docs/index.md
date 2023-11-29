# The deltalake package

This is the documentation for the native Rust/Python implementation of Delta Lake. It is based on the delta-rs Rust library and requires no Spark or JVM dependencies. For the PySpark implementation, see [delta-spark](https://docs.delta.io/latest/api/python/index.html) instead.

This module provides the capability to read, write, and manage [Delta Lake](https://delta.io/) tables with Python or Rust without Spark or Java. It uses [Apache Arrow](https://arrow.apache.org/) under the hood, so is compatible with other Arrow-native or integrated libraries such as [pandas](https://pandas.pydata.org/), [DuckDB](https://duckdb.org/), and [Polars](https://www.pola.rs/).

## Important terminology

* "Rust deltalake" refers to the Rust API of delta-rs (no Spark dependency)
* "Python deltalake" refers to the Python API of delta-rs (no Spark dependency)
* "Delta Spark" refers to the Scala impementation of the Delta Lake transaction log protocol.  This depends on Spark and Java.

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
