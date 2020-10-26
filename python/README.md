Delta-python
============

Native [Delta Lake](https://delta.io/) binding for Python.

Usage
-----

```
>>> from delta import DeltaTable
>>> dt = DeltaTable("../rust/tests/data/delta-0.2.0")
>>> dt.version()
3
>>> dt.files()
['part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet', 'part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet', 'part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet']
```
