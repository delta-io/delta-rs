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
[DeltaTable.metadata()][deltalake.table.DeltaTable.metadata] method:

=== "Python"
    ``` python
    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/simple_table")
    >>> dt.metadata()
    Metadata(id: 5fba94ed-9794-4965-ba6e-6ee3c0d22af9, name: None, description: None, partitionColumns: [], created_time: 1587968585495, configuration={})
    ```
=== "Rust"
    ```rust
    let delta_path = Url::from_directory_path("/rust/tests/data/simple_table").unwrap();
    let table = deltalake::open_table(delta_path).await?;
    let metadata = table.metadata()?;
    println!("metadata: {:?}", metadata);
    ```


## Schema

The schema for the table is also saved in the transaction log. It can
either be retrieved in the Delta Lake form as
[Schema][deltalake.schema.Schema] or as a
PyArrow schema. The first allows you to introspect any column-level
metadata stored in the schema, while the latter represents the schema
the table will be loaded into.



=== "Python"
    Use [DeltaTable.schema][deltalake.table.DeltaTable.schema] to retrieve the delta lake schema:
    ``` python
    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/simple_table")
    >>> dt.schema()
    Schema([Field(id, PrimitiveType("long"), nullable=True)])
    ```
=== "Rust"
    Use `DeltaTable::get_schema` to retrieve the delta lake schema
    ```rust
    let delta_path = Url::from_directory_path("/tmp/some-table").unwrap();
    let mut table = open_table(delta_path).await?;
    let schema = table.get_schema()?;
    println!("schema: {:?}", schema);
    ```
These schemas have a JSON representation that can be retrieved.

=== "Python"
    To reconstruct from json, use [DeltaTable.schema.to_json()][deltalake.schema.Schema.to_json].
    ``` python
    >>> dt.schema().to_json()
    '{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}'
    ```
=== "Rust"
    Use `serde_json` to get the schema as a json.
    ```rust
    println!("{}", serde_json::to_string_pretty(&schema)?);
    ```
It is also possible to retrieve the Arrow schema:
=== "Python"

    Use [DeltaTable.schema.to_arrow()][deltalake.schema.Schema.to_arrow] to retrieve the Arro3 schema:

    ``` python
    >>> dt.schema().to_arrow()
    id: int64
    ```
=== "Rust"
    ```rust
    let arrow_schema = table.snapshot()?.arrow_schema()?;
    println!("arrow_schema: {:?}", schema);
    ```

## History

Depending on what system wrote the table, the delta table may have
provenance information describing what operations were performed on the
table, when, and by whom. This information is retained for 30 days by
default, unless otherwise specified by the table configuration
`delta.logRetentionDuration`.

!!! note

    This information is not written by all writers and different writers may
    use different schemas to encode the actions. For Spark\'s format, see:
    <https://docs.delta.io/latest/delta-utility.html#history-schema>


To view the available history, use `DeltaTable.history`:
=== "Python"
    ``` python
    from deltalake import DeltaTable

    dt = DeltaTable("../rust/tests/data/simple_table")
    dt.history()
    ```

    ```
    [{'timestamp': 1587968626537, 'operation': 'DELETE', 'operationParameters': {'predicate': '["((`id` % CAST(2 AS BIGINT)) = CAST(0 AS BIGINT))"]'}, 'readVersion': 3, 'isBlindAppend': False},
    {'timestamp': 1587968614187, 'operation': 'UPDATE', 'operationParameters': {'predicate': '((id#697L % cast(2 as bigint)) = cast(0 as bigint))'}, 'readVersion': 2, 'isBlindAppend': False},
    {'timestamp': 1587968604143, 'operation': 'WRITE', 'operationParameters': {'mode': 'Overwrite', 'partitionBy': '[]'}, 'readVersion': 1, 'isBlindAppend': False},
    {'timestamp': 1587968596254, 'operation': 'MERGE', 'operationParameters': {'predicate': '(oldData.`id` = newData.`id`)'}, 'readVersion': 0, 'isBlindAppend': False},
    {'timestamp': 1587968586154, 'operation': 'WRITE', 'operationParameters': {'mode': 'ErrorIfExists', 'partitionBy': '[]'}, 'isBlindAppend': True}]
    ```
=== "Rust"
    ```rust
    let delta_path = Url::from_directory_path("/tmp/some-table").unwrap();
    let table = open_table(delta_path).await?;
    let history = table.history(None).await?;
    println!("Table history: {:#?}", history);
    ```
## Current Add Actions

The active state for a delta table is determined by the Add actions,
which provide the list of files that are part of the table and metadata
about them, such as creation time, size, and statistics. You can get a
data frame of the add actions data using `DeltaTable.get_add_actions`:

<!-- spellchecker:off --!>

=== "Python"
    ``` python
    >>> from deltalake import DeltaTable
    >>> dt = DeltaTable("../rust/tests/data/delta-0.8.0")
    >>> dt.get_add_actions(flatten=True).to_pandas()
                                                        path  size_bytes   modification_time  data_change  num_records  null_count.value  min.value  max.value
    0  part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a...         440 2021-03-06 15:16:07         True            2                 0          0          2
    1  part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe...         440 2021-03-06 15:16:16         True            2                 0          2          4
    ```

!!! note

    `DeltaTable.get_add_actions` returns an `arro3.core.Table`. If legacy code still expects a single PyArrow `RecordBatch`, you can adapt it like this:

    ``` python
    >>> import pyarrow as pa
    >>> arro3_table = dt.get_add_actions(flatten=True)
    >>> pa_table = pa.table(arro3_table).combine_chunks()
    >>> legacy_batches = pa_table.to_batches(max_chunksize=None)
    >>> legacy_batch = legacy_batches[0] if legacy_batches else pa.RecordBatch.from_arrays(
    ...     [pa.array([], type=f.type) for f in pa_table.schema],
    ...     schema=pa_table.schema,
    ... )
    ```

=== "Rust"
    ```rust
    let delta_path = Url::from_directory_path("/tmp/some-table").unwrap();
    let table = open_table(delta_path).await?;
    let actions = table.snapshot()?.add_actions_table(true)?;
    println!("{}", pretty_format_batches(&vec![actions])?);
    ```
This works even with past versions of the table:

=== "Python"
    ``` python
    >>> dt = DeltaTable("../rust/tests/data/delta-0.8.0", version=0)
    >>> dt.get_add_actions(flatten=True).to_pandas()
                                                    path  size_bytes   modification_time  data_change  num_records  null_count.value  min.value  max.value
    0  part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a...         440 2021-03-06 15:16:07         True            2                 0          0          2
    1  part-00001-911a94a2-43f6-4acb-8620-5e68c265498...         445 2021-03-06 15:16:07         True            3                 0          2          4
    ```
=== "Rust"
    ```rust
    let delta_path = Url::from_directory_path("/rust/tests/data/simple_table").unwrap();
    let mut table = deltalake::open_table(delta_path).await?;
    table.load_version(0).await?;
    let actions = table.snapshot()?.add_actions_table(true)?;
    println!("{}", pretty_format_batches(&vec![actions])?);
    ```

<!-- spellchecker:on --!>
