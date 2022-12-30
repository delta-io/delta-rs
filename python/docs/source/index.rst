Python bindings documentation of delta-rs
=========================================================

This is the documentation of the Python bindings of delta-rs, ``deltalake``.
This module provides the capability to read, write, and manage `Delta Lake`_
tables from Python without Spark or Java. It uses `Apache Arrow`_ under the hood,
so is compatible with other Arrow-native or integrated libraries such as
Pandas_, DuckDB_, and Polars_.

.. note::

   This module is under active development and some features are experimental.
   It is not yet as feature-complete as the PySpark implementation of Delta
   Lake. If you encounter a bug, please let us know in our `GitHub repo`_.


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
