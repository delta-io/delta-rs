# Python deltalake package

This is the documentation for the native Python implementation of Delta Lake. It is based on the delta-rs Rust library and requires no Spark or JVM dependencies. For the PySpark implementation, see [delta-spark](https://docs.delta.io/latest/api/python/index.html) instead.

This module provides the capability to read, write, and manage [Delta Lake](https://delta.io/) tables from Python without Spark or Java. It uses [Apache Arrow](https://arrow.apache.org/) under the hood, so is compatible with other Arrow-native or integrated libraries such as [Pandas](https://pandas.pydata.org/), [DuckDB](https://duckdb.org/), and [Polars](https://www.pola.rs/).

Note: This module is under active development and some features are experimental. It is not yet as feature-complete as the PySpark implementation of Delta Lake. If you encounter a bug, please let us know in our [GitHub repo](https://github.com/delta-io/delta-rs/issues).