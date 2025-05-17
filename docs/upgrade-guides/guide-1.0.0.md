# DeltaLake 1.0.0 Migration Guide

DeltaLake 1.0.0 introduces significant changes, including the removal of the legacy Python writer engine. All write operations are now delegated to the `Rust` engine, which provides enhanced performance via `streaming execution` and `multipart uploads` for improved concurrency and reduced memory usage.

## Breaking changes

### `write_deltalake` API

#### `engine` parameter removed

The engine parameter has been removed. All writes now default to the Rust engine.

Before:

```python
write_deltalake(
    "mytable",
    data=data,
    mode="append",
    engine="rust",
)
```

After:

```python
write_deltalake(
    "mytable",
    data=data,
    mode="append",
)
```

### PyArrow-specific parameters removed

The following parameters related to the deprecated Python engine are no longer supported:

- `file_options: Optional[ds.ParquetFileWriteOptions]`
- `max_partitions: Optional[int]`
- `max_open_files: int`
- `max_rows_per_file: int`
- `min_rows_per_group: int`
- `max_rows_per_group: int`

#### `partition_filters` removed

The partition_filters parameter has been removed. Use the `predicate` parameter as an alternative for overwriting a subset during writes

#### `large_dtypes` removed

The `large_dtypes` parameter has been removed. DeltaLake now always passes through all Arrow data types without modification for both `write_deltalake` and `DeltaTable.merge`.

### CommitProperties

#### `custom_metadata` replaced with `commit_properties`

The custom_metadata argument has been replaced by the commit_properties parameter on the following APIs:

- convert_to_deltalake
- DeltaTable
  - create
  - vacuum
  - update
  - merge
  - restore
  - repair
  - add_columns
  - add_constraint
  - set_table_properties
  - compact
  - z_order
  - delete

Before:

```python
convert_to_deltalake(
    uri="mytable",
    custom_metadata={"foo":"bar"},
)
```

After:

```python
convert_to_deltalake(
    uri="mytable",
    commit_properties=CommitProperties(custom_metadata={"foo":"bar"}),
)
```

### `from_data_catalog` removed on `DeltaTable`

This method was previously unimplemented and has now been fully removed from the DeltaTable class.

### Internal changes

#### `WriterProperties`, `ColumnProperties` and `BloomFilterProperties` moved to `deltalake.writer.properties`

Can be imported directly with:

```python
from deltalake import WriterProperties, ColumnProperties, BloomFilterProperties
```

#### `AddAction`, `CommitProperties` and `PostCommithookProperties` moved to `deltalake.transaction`

Can be imported directly with:

```python
from deltalake import AddAction, CommitProperties, PostCommithookProperties
```

### New features

#### Public APIs for transaction management

Functionality previously limited to internal PyArrow writer methods is now publicly available:

- `write_new_deltalake` has been renamed to `create_table_with_add_actions` and is now exposed under deltalake.transaction.
- You can now initiate a write transaction on an existing table using: `DeltaTable.create_write_transaction(...)`

### Behavior changes with unsupported delta features ⚠️

Previously, it was possible to work with Delta tables that included unsupported features (e.g., Deletion Vectors) as long as those features were not actively used during an operation/interaction.

As of version 1.0.0, this is no longer allowed. DeltaLake will now raise an error if it detects unsupported features in a table. This change ensures safety and avoids inconsistencies stemming from partial or undefined behavior.
