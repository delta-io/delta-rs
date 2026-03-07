`delta-rs` is a Rust-based re-implementation of the [Delta Lake](https://delta.io) protocol originally developed at Databricks. The `deltalake` library has APIs in Rust and Python, and implementation has no dependencies on Java, Spark or Databricks.

## Contributing

The Delta Lake community welcomes contributors from all developers, regardless of your experience or programming background.

You can write Rust code, Python code, documentation, submit bugs, or give talks to the community.  We welcome all of these contributions.

Feel free to [join our Slack](https://go.delta.io/slack) and message us in the #delta-rs channel any time!

We value kind communication and building a productive, friendly environment for maximum collaboration and fun.


## Important terminology

* `deltalake` refers to the Rust or Python API of delta-rs
* "Delta Spark" refers to the Scala implementation of the Delta Lake transaction log protocol.  This depends on Spark and Java.

## Why implement the Delta Lake transaction log protocol in Rust?

Delta Spark depends on Java and Spark, which is fine for many use cases, but not all Delta Lake users want to depend on these libraries.  `deltalake` allows you to manage your dataset using a Delta Lake approach without any Java or Spark dependencies.

A `DeltaTable` on disk is simply a directory that stores metadata in JSON files and data in [Apache Parquet](https://parquet.apache.org) files.
