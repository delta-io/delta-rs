Usage
====================================

DeltaTable
----------
Resolve partitions for current version of the DeltaTable

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/delta-0.2.0")
    >>> dt.version()
    3
    >>> dt.files()
    ['part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet', 'part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet', 'part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet']


Apply filtering on partitions for current version of the partitioned DeltaTable

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/delta-0.8.0-partitioned")
    >>> dt.version()
    0
    >>> dt.files()
    ['year=2020/month=1/day=1/part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet', 'year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet', 'year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet', 'year=2021/month=12/day=20/part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet', 'year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet', 'year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet']
    >>> partition_filters = [("day", "=", "5")]
    >>> dt.files_by_partitions(partition_filters)
    ['year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet', 'year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet']

Convert DeltaTable into PyArrow Table and Pandas Dataframe

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/simple_table")
    >>> df = dt.to_pandas()
    >>> df
       id
    0   5
    1   7
    2   9
    >>> df[df['id'] > 5]
       id
    1   7
    2   9

Time travel

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/simple_table")
    >>> dt.load_version(2)
    >>> dt.to_pyarrow_table().to_pydict()
    {'id': [5, 7, 9, 5, 6, 7, 8, 9]}

History

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/simple_table")
    >>> dt.history()
    [{'timestamp': 1587968626537, 'operation': 'DELETE', 'operationParameters': {'predicate': '["((`id` % CAST(2 AS BIGINT)) = CAST(0 AS BIGINT))"]'}, 'readVersion': 3, 'isBlindAppend': False}, {'timestamp': 1587968614187, 'operation': 'UPDATE', 'operationParameters': {'predicate': '((id#697L % cast(2 as bigint)) = cast(0 as bigint))'}, 'readVersion': 2, 'isBlindAppend': False}, {'timestamp': 1587968604143, 'operation': 'WRITE', 'operationParameters': {'mode': 'Overwrite', 'partitionBy': '[]'}, 'readVersion': 1, 'isBlindAppend': False}, {'timestamp': 1587968596254, 'operation': 'MERGE', 'operationParameters': {'predicate': '(oldData.`id` = newData.`id`)'}, 'readVersion': 0, 'isBlindAppend': False}, {'timestamp': 1587968586154, 'operation': 'WRITE', 'operationParameters': {'mode': 'ErrorIfExists', 'partitionBy': '[]'}, 'isBlindAppend': True}]

Create a DeltaTable using a Data Catalog

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> from deltalake import DataCatalog
    >>> database_name = "simple_database"
    >>> table_name = "simple_table"
    >>> data_catalog = DataCatalog.AWS
    >>> dt = DeltaTable.from_data_catalog(data_catalog=data_catalog, database_name=database_name, table_name=table_name)
    >>> dt.to_pyarrow_table().to_pydict()
    {'id': [5, 7, 9, 5, 6, 7, 8, 9]}

DeltaSchema
-----------

Delta format

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/simple_table")
    >>> dt.schema()
    Schema(Field(id: DataType(long) nullable(True) metadata({})))

PyArrow format

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/simple_table")
    >>> dt.pyarrow_schema()
    id: int64

Metadata
-----------

.. code-block:: python

    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/simple_table")
    >>> dt.metadata()
    Metadata(id: 5fba94ed-9794-4965-ba6e-6ee3c0d22af9, name: None, description: None, partitionColumns: [], created_time: 1587968585495, configuration={})
