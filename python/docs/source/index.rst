Python deltalake package
=========================================================

This is the documentation for the native Python implementation of deltalake. It
is based on the delta-rs Rust library and requires no Spark or JVM dependencies.
For the PySpark implementation, see `delta-spark`_ instead.

This module provides the capability to read, write, and manage `Delta Lake`_
tables from Python without Spark or Java. It uses `Apache Arrow`_ under the hood,
so is compatible with other Arrow-native or integrated libraries such as
Pandas_, DuckDB_, and Polars_.

.. note::

   This module is under active development and some features are experimental.
   It is not yet as feature-complete as the PySpark implementation of Delta
   Lake. If you encounter a bug, please let us know in our `GitHub repo`_.

.. _delta-spark: https://docs.delta.io/latest/api/python/spark/index.html
.. _Delta Lake: https://delta.io/
.. _Apache Arrow: https://arrow.apache.org/
.. _Pandas: https://pandas.pydata.org/
.. _DuckDB: https://duckdb.org/
.. _Polars: https://www.pola.rs/
.. _GitHub repo: https://github.com/delta-io/delta-rs/issues

.. toctree::
   :maxdepth: 2

   installation
   usage
   api_reference


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
