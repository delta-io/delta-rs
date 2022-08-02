API Reference
====================================

DeltaTable
----------

.. automodule:: deltalake.table
    :members:

Writing DeltaTables
-------------------

.. autofunction:: deltalake.write_deltalake

Delta Lake Schemas
------------------

Schemas, fields, and data types are provided in the ``deltalake.schema`` submodule.

.. autoclass:: deltalake.schema.Schema
    :members:

.. autoclass:: deltalake.schema.PrimitiveType
    :members:

.. autoclass:: deltalake.schema.ArrayType
    :members:

.. autoclass:: deltalake.schema.MapType
    :members:

.. autoclass:: deltalake.schema.Field
    :members:

.. autoclass:: deltalake.schema.StructType
    :members:

DataCatalog
-----------

.. automodule:: deltalake.data_catalog
    :members:

DeltaStorageHandler
-------------------

.. automodule:: deltalake.fs
    :members: