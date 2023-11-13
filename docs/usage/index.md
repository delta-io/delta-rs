# Usage

A [DeltaTable][deltalake.table.DeltaTable] represents the state of a
delta table at a particular version. This includes which files are
currently part of the table, the schema of the table, and other metadata
such as creation time.

``` python
>>> from deltalake import DeltaTable
>>> dt = DeltaTable("../rust/tests/data/delta-0.2.0")
>>> dt.version()
3
>>> dt.files()
['part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet', 
 'part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet', 
 'part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet']
```