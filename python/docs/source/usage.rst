Usage
====================================

.. py:currentmodule:: deltalake.table

A :class:`DeltaTable` represents the state of a delta table at a particular
version. This includes which files are currently part of the table, the schema
of the table, and other metadata such as creation time.

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/delta-0.2.0")
    >>> dt.version()
    3
    >>> dt.files()
    ['part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet', 
     'part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet', 
     'part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet']


Loading a Delta Table
---------------------

To load the current version, use the constructor:

.. code-block:: python

    >>> dt = DeltaTable("../rust/tests/data/delta-0.2.0")

Depending on your storage backend, you could use the ``storage_options`` parameter to provide some configuration.
Currently only AWS S3 is supported.

.. code-block:: python

    >>> storage_options = {"AWS_ACCESS_KEY_ID": "THE_AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY":"THE_AWS_SECRET_ACCESS_KEY"}
    >>> dt = DeltaTable("../rust/tests/data/delta-0.2.0", storage_options=storage_options)

Alternatively, if you have a data catalog you can load it by reference to a 
database and table name. Currently only AWS Glue is supported.

For AWS Glue catalog, use AWS environment variables to authenticate.

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> from deltalake import DataCatalog
    >>> database_name = "simple_database"
    >>> table_name = "simple_table"
    >>> data_catalog = DataCatalog.AWS
    >>> dt = DeltaTable.from_data_catalog(data_catalog=data_catalog, database_name=database_name, table_name=table_name)
    >>> dt.to_pyarrow_table().to_pydict()
    {'id': [5, 7, 9, 5, 6, 7, 8, 9]}

Besides local filesystems, the following backends are supported:

* AWS S3, detected by the prefix ``s3://``. AWS credentials can be specified using
  environment variables in the same way as the CLI.
* Azure Data Lake Storage Gen 2, detected by the prefix ``adls2://``. Note that
  `specific instructions`_ must be followed to setup the Azure Storage Account.
* Google Cloud Storage, detected by the prefix ``gs://``.

.. _`specific instructions`: https://github.com/delta-io/delta-rs/blob/main/docs/ADLSGen2-HOWTO.md


Time Travel
~~~~~~~~~~~

To load previous table states, you can provide the version number you wish to
load:

.. code-block:: python

    >>> dt = DeltaTable("../rust/tests/data/simple_table", version=2)

Once you've loaded a table, you can also change versions using either a version
number or datetime string:

.. code-block:: python

    >>> dt.load_version(1)
    >>> dt.load_with_timestamp("2021-11-04 00:05:23.283+00:00")

.. warning::

    Previous table versions may not exist if they have been vacuumed, in which
    case an exception will be thrown. See `Vacuuming tables`_ for more information.

Examining a Table
-----------------

Metadata
~~~~~~~~

The delta log maintains basic metadata about a table, including:

* A unique ``id``
* A ``name``, if provided
* A ``description``, if provided
* The list of ``partitionColumns``.
* The ``created_time`` of the table
* A map of table ``configuration``. This includes fields such as ``delta.appendOnly``,
  which if ``true`` indicates the table is not meant to have data deleted from it.

Get metadata from a table with the :meth:`DeltaTable.metadata` method:

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/simple_table")
    >>> dt.metadata()
    Metadata(id: 5fba94ed-9794-4965-ba6e-6ee3c0d22af9, name: None, description: None, partitionColumns: [], created_time: 1587968585495, configuration={})

Schema
~~~~~~

The schema for the table is also saved in the transaction log. It can either be
retrieved in the Delta Lake form as :class:`deltalake.schema.Schema` or as a PyArrow 
schema. The first allows you to introspect any column-level metadata stored in 
the schema, while the latter represents the schema the table will be loaded into.

Use :meth:`DeltaTable.schema` to retrieve the delta lake schema:

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/simple_table")
    >>> dt.schema()
    Schema(Field(id: DataType(long) nullable(True) metadata({})))

These schemas have a JSON representation that can be retrieved. To reconstruct
from json, use :meth:`deltalake.schema.Schema.from_json`.

.. code-block:: python

    >>> dt.schema().json()
    {'type': 'struct', 'fields': [{'name': 'id', 'type': 'long', 'nullable': True, 'metadata': {}}]}

Use :meth:`DeltaTable.pyarrow_schema` to retrieve the PyArrow schema:

.. code-block:: python

    >>> dt.pyarrow_schema()
    id: int64


History
~~~~~~~

Depending on what system wrote the table, the delta table may have provenance
information describing what operations were performed on the table, when, and 
by whom. This information is retained for 30 days by default, unless otherwise
specified by the table configuration ``delta.logRetentionDuration``.

.. note::

    This information is not written by all writers and different writers may use 
    different schemas to encode the actions. For Spark's format, see: 
    https://docs.delta.io/latest/delta-utility.html#history-schema

To view the available history, use :meth:`DeltaTable.history`:

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/simple_table")
    >>> dt.history()
    [{'timestamp': 1587968626537, 'operation': 'DELETE', 'operationParameters': {'predicate': '["((`id` % CAST(2 AS BIGINT)) = CAST(0 AS BIGINT))"]'}, 'readVersion': 3, 'isBlindAppend': False},
     {'timestamp': 1587968614187, 'operation': 'UPDATE', 'operationParameters': {'predicate': '((id#697L % cast(2 as bigint)) = cast(0 as bigint))'}, 'readVersion': 2, 'isBlindAppend': False},
     {'timestamp': 1587968604143, 'operation': 'WRITE', 'operationParameters': {'mode': 'Overwrite', 'partitionBy': '[]'}, 'readVersion': 1, 'isBlindAppend': False},
     {'timestamp': 1587968596254, 'operation': 'MERGE', 'operationParameters': {'predicate': '(oldData.`id` = newData.`id`)'}, 'readVersion': 0, 'isBlindAppend': False},
     {'timestamp': 1587968586154, 'operation': 'WRITE', 'operationParameters': {'mode': 'ErrorIfExists', 'partitionBy': '[]'}, 'isBlindAppend': True}]


Querying Delta Tables
---------------------

Delta tables can be queried in several ways. By loading as Arrow data or an Arrow
dataset, they can be used by compatible engines such as Pandas and DuckDB. By 
passing on the list of files, they can be loaded into other engines such as Dask.

Delta tables are often larger than can fit into memory on a single computer, so
this module provides ways to read only the parts of the data you need. Partition 
filters allow you to skip reading files that are part of irrelevant partitions.
Only loading the columns required also saves memory. Finally, some methods allow
reading tables batch-by-batch, allowing you to process the whole table while only
having a portion loaded at any given time.

To load into Pandas or a PyArrow table use the :meth:`DeltaTable.to_pandas` and
:meth:`DeltaTable.to_pyarrow_table` methods, respectively. Both of these 
support filtering partitions and selecting particular columns.

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/delta-0.8.0-partitioned")
    >>> dt.dt.pyarrow_schema()
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

Converting to a PyArrow Dataset allows you to filter on columns other than 
partition columns and load the result as a stream of batches rather than a single
table. Convert to a dataset using :meth:`DeltaTable.to_pyarrow_dataset`. Filters 
applied to datasets will use the partition values and file statistics from the 
Delta transaction log and push down any other filters to the scanning operation.

.. code-block:: python

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

PyArrow datasets may also be passed to compatible query engines, such as DuckDB_.

.. _DuckDB: https://duckdb.org/docs/api/python

.. code-block:: python

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

Finally, you can always pass the list of file paths to an engine. For example,
you can pass them to ``dask.dataframe.read_parquet``:

.. code-block:: python

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


Managing Delta Tables
---------------------

Vacuuming tables
~~~~~~~~~~~~~~~~

Vacuuming a table will delete any files that have been marked for deletion. This
may make some past versions of a table invalid, so this can break time travel. 
However, it will save storage space. Vacuum will retain files in a certain window,
by default one week, so time travel will still work in shorter ranges.

Delta tables usually don't delete old files automatically, so vacuuming regularly
is considered good practice, unless the table is only appended to.

Use :meth:`DeltaTable.vacuum` to perform the vacuum operation. Note that to
prevent accidental deletion, the function performs a dry-run by default: it will
only list the files to be deleted. Pass ``dry_run=False`` to actually delete files.

.. code-block:: python

    >>> dt = DeltaTable("../rust/tests/data/simple_table")
    >>> dt.vacuum()
    ['../rust/tests/data/simple_table/part-00006-46f2ff20-eb5d-4dda-8498-7bfb2940713b-c000.snappy.parquet', 
     '../rust/tests/data/simple_table/part-00190-8ac0ae67-fb1d-461d-a3d3-8dc112766ff5-c000.snappy.parquet', 
     '../rust/tests/data/simple_table/part-00164-bf40481c-4afd-4c02-befa-90f056c2d77a-c000.snappy.parquet',
     ...]
    >>> dt.vacuum(dry_run=False) # Don't run this unless you are sure!

Optimizing tables
~~~~~~~~~~~~~~~~~

Optimizing tables is not currently supported.

Writing Delta Tables
--------------------

.. py:currentmodule:: deltalake

.. warning::
    The writer is currently *experimental*. Please use on test data first, not
    on production data. Report any issues at https://github.com/delta-io/delta-rs/issues.

For overwrites and appends, use :py:func:`write_deltalake`. If the table does not
already exist, it will be created. The ``data`` parameter will accept a Pandas
DataFrame, a PyArrow Table, or an iterator of PyArrow Record Batches.

.. code-block:: python

    >>> from deltalake.writer import write_deltalake
    >>> df = pd.DataFrame({'x': [1, 2, 3]})
    >>> write_deltalake('path/to/table', df)

.. note::
    :py:func:`write_deltalake` accepts a Pandas DataFrame, but will convert it to 
    a Arrow table before writing. See caveats in :doc:`pyarrow:python/pandas`. 

By default, writes create a new table and error if it already exists. This is 
controlled by the ``mode`` parameter, which mirrors the behavior of Spark's 
:py:meth:`pyspark.sql.DataFrameWriter.saveAsTable` DataFrame method. To overwrite pass in ``mode='overwrite'`` and
to append pass in ``mode='append'``:

.. code-block:: python

    >>> write_deltalake('path/to/table', df, mode='overwrite')
    >>> write_deltalake('path/to/table', df, mode='append')
