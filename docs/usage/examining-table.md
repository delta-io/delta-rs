# Examining a Table

## Metadata

The delta log maintains basic metadata about a table, including:

-   A unique `id`
-   A `name`, if provided
-   A `description`, if provided
-   The list of `partitionColumns`.
-   The `created_time` of the table
-   A map of table `configuration`. This includes fields such as
    `delta.appendOnly`, which if `true` indicates the table is not meant
    to have data deleted from it.

Get metadata from a table with the
`DeltaTable.metadata` method:

``` python
>>> from deltalake import DeltaTable
>>> dt = DeltaTable("../rust/tests/data/simple_table")
>>> dt.metadata()
Metadata(id: 5fba94ed-9794-4965-ba6e-6ee3c0d22af9, name: None, description: None, partitionColumns: [], created_time: 1587968585495, configuration={})
```

## Schema

The schema for the table is also saved in the transaction log. It can
either be retrieved in the Delta Lake form as
`deltalake.schema.Schema` or as a
PyArrow schema. The first allows you to introspect any column-level
metadata stored in the schema, while the latter represents the schema
the table will be loaded into.

Use `DeltaTable.schema` to retrieve the delta lake schema:

``` python
>>> from deltalake import DeltaTable
>>> dt = DeltaTable("../rust/tests/data/simple_table")
>>> dt.schema()
Schema([Field(id, PrimitiveType("long"), nullable=True)])
```

These schemas have a JSON representation that can be retrieved. To
reconstruct from json, use
`deltalake.schema.Schema.from_json()`.

``` python
>>> dt.schema().json()
'{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}'
```

Use `deltalake.schema.Schema.to_pyarrow()` to retrieve the PyArrow schema:

``` python
>>> dt.schema().to_pyarrow()
id: int64
```

## History

Depending on what system wrote the table, the delta table may have
provenance information describing what operations were performed on the
table, when, and by whom. This information is retained for 30 days by
default, unless otherwise specified by the table configuration
`delta.logRetentionDuration`.

::: note
::: title
Note
:::

This information is not written by all writers and different writers may
use different schemas to encode the actions. For Spark\'s format, see:
<https://docs.delta.io/latest/delta-utility.html#history-schema>
:::

To view the available history, use `DeltaTable.history`:

``` python
>>> from deltalake import DeltaTable
>>> dt = DeltaTable("../rust/tests/data/simple_table")
>>> dt.history()
[{'timestamp': 1587968626537, 'operation': 'DELETE', 'operationParameters': {'predicate': '["((`id` % CAST(2 AS BIGINT)) = CAST(0 AS BIGINT))"]'}, 'readVersion': 3, 'isBlindAppend': False},
 {'timestamp': 1587968614187, 'operation': 'UPDATE', 'operationParameters': {'predicate': '((id#697L % cast(2 as bigint)) = cast(0 as bigint))'}, 'readVersion': 2, 'isBlindAppend': False},
 {'timestamp': 1587968604143, 'operation': 'WRITE', 'operationParameters': {'mode': 'Overwrite', 'partitionBy': '[]'}, 'readVersion': 1, 'isBlindAppend': False},
 {'timestamp': 1587968596254, 'operation': 'MERGE', 'operationParameters': {'predicate': '(oldData.`id` = newData.`id`)'}, 'readVersion': 0, 'isBlindAppend': False},
 {'timestamp': 1587968586154, 'operation': 'WRITE', 'operationParameters': {'mode': 'ErrorIfExists', 'partitionBy': '[]'}, 'isBlindAppend': True}]
```

## Current Add Actions

The active state for a delta table is determined by the Add actions,
which provide the list of files that are part of the table and metadata
about them, such as creation time, size, and statistics. You can get a
data frame of the add actions data using `DeltaTable.get_add_actions`:

``` python
>>> from deltalake import DeltaTable
>>> dt = DeltaTable("../rust/tests/data/delta-0.8.0")
>>> dt.get_add_actions(flatten=True).to_pandas()
                                                    path  size_bytes   modification_time  data_change  num_records  null_count.value  min.value  max.value
0  part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a...         440 2021-03-06 15:16:07         True            2                 0          0          2
1  part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe...         440 2021-03-06 15:16:16         True            2                 0          2          4
```

This works even with past versions of the table:

``` python
>>> dt = DeltaTable("../rust/tests/data/delta-0.8.0", version=0)
>>> dt.get_add_actions(flatten=True).to_pandas()
                                                path  size_bytes   modification_time  data_change  num_records  null_count.value  min.value  max.value
0  part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a...         440 2021-03-06 15:16:07         True            2                 0          0          2
1  part-00001-911a94a2-43f6-4acb-8620-5e68c265498...         445 2021-03-06 15:16:07         True            3                 0          2          4
```