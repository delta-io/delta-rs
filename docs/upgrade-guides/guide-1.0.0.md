# DeltaLake 1.0.0 Migration Guide

DeltaLake 1.0.0 introduces significant changes, including the removal of the legacy Python writer engine. All write operations are now delegated to the `Rust` engine, which provides enhanced performance via `streaming execution` and `multipart uploads` for improved concurrency and reduced memory usage.

In addition, DeltaLake no longer has a hard dependency on PyArrow, making it more lightweight and suitable for use in minimal or size-constrained environments. Arrow data handling between Python and Rust is now powered by `arro3`, a lightweight Arrow implementation backed by `arrow-rs`.

For users who still require PyArrow-based read functionality, it remains available as an optional dependency. To continue using these features, install DeltaLake with the `pyarrow` extra, `deltalake[pyarrow]`.

## Breaking changes

### `arro3` adoption

DeltaLake 1.0.0 introduces support for Arrow PyCapsule protocol using a lightweight arrow implementation with `arro3`. As a result, the `write_deltalake` and `DeltaTable.merge` functions now only accept inputs that implement one of the following interfaces:

- `ArrowStreamExportable`
- `ArrowArrayExportable`

This means you can directly pass data from compatible libraries, such as:

- Polars DataFrame
- PyArrow Table or RecordBatchReader
- Pandas DataFrame (via its Arrow export support)

DeltaLake will consume these objects efficiently through the Arrow C Data Interface.

#### Schema, Types and Fields

Schema handling has also transitioned from PyArrow to `arro3`. Internally, schema, types, and fields are now defined and interpreted via the Arrow C Data Interface:

Our schema code also no longer converts to pyarrow but converts to `arro3` schema/types/fields directly over Arrow C Data Interface.

- `to_pyarrow` → replaced with `to_arrow`
- `from_pyarrow` -> replaced with `from_arrow` (can accept any object implementing `ArrowSchemaExportable`)

All DeltaLake schema components—such as schema, field, and type objects—now implement the `__arrow_c_schema__` protocol. This means they are directly consumable by any library that supports the Arrow C Data Interface (e.g., Polars, PyArrow, pandas).

#### Arrow Object Outputs

Several DeltaLake APIs now return arro3-based Arrow objects instead of PyArrow objects. This includes:

- `DeltaTable.get_add_actions`
- `DeltaTable.load_cdf`
- `QueryBuilder.execute`

These returned objects are fully compatible with libraries like Polars and PyArrow. For example:

```python
import polars as pl
import pyarrow as pa

dt = DeltaTable("test")
rb = dt.load_df()

# With polars
df = pl.DataFrame(rb)

# With pyarrow
tbl = pa.table(df)
```

#### `create` API

The `create` API now accepts schema definitions as either:

- A `DeltaSchema` object, or
- Any object implementing the `ArrowSchemaExportable` protocol (e.g., a PyArrow schema)

This ensures full interoperability with modern Arrow-compatible libraries while leveraging the performance and flexibility of the Arrow C Data Interface.


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

### QueryBuilder

QueryBuilder no longer implements `show`, and `execute` will directly return a RecordBatchReader. Additionally the experimental flag has been removed.

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
