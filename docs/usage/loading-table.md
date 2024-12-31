# Loading a Delta Table

A [DeltaTable][deltalake.table.DeltaTable] represents the state of a
delta table at a particular version. This includes which files are
currently part of the table, the schema of the table, and other metadata
such as creation time.

{{ code_example('delta_table', 'get_table_info', ['DeltaTable'])}}

Depending on your storage backend, you could use the `storage_options`
parameter to provide some configuration. Configuration is defined for
specific backends - [s3
options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html#variants),
[azure
options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants),
[gcs
options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html#variants).

```python
>>> storage_options = {"AWS_ACCESS_KEY_ID": "THE_AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY":"THE_AWS_SECRET_ACCESS_KEY"}
>>> dt = DeltaTable("../rust/tests/data/delta-0.2.0", storage_options=storage_options)
```

The configuration can also be provided via the environment, and the
basic service provider is derived from the URL being used. We try to
support many of the well-known formats to identify basic service
properties.

**S3**:

> - s3://\<bucket\>/\<path\>
> - s3a://\<bucket\>/\<path\>

Note that `delta-rs` does not read credentials from a local `.aws/config` or `.aws/creds` file. Credentials can be accessed from environment variables, ec2 metadata, profiles or web identity. You can also pass credentials to `storage_options` using `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

**Azure**:

> - az://\<container\>/\<path\>
> - adl://\<container\>/\<path\>
> - abfs://\<container\>/\<path\>

**GCS**:

> - gs://\<bucket\>/\<path\>

Alternatively, if you have a data catalog you can load it by reference
to a database and table name. Currently only AWS Glue is supported.

For AWS Glue catalog, use AWS environment variables to authenticate.

```python
>>> from deltalake import DeltaTable
>>> from deltalake import DataCatalog
>>> database_name = "simple_database"
>>> table_name = "simple_table"
>>> data_catalog = DataCatalog.AWS
>>> dt = DeltaTable.from_data_catalog(data_catalog=data_catalog, database_name=database_name, table_name=table_name)
>>> dt.to_pyarrow_table().to_pydict()
{'id': [5, 7, 9, 5, 6, 7, 8, 9]}
```

## Verify Table Existence

You can check whether or not a Delta table exists at a particular path by using
the `DeltaTable.is_deltatable()` method.

```python
from deltalake import DeltaTable

table_path = "<path/to/valid/table>"
DeltaTable.is_deltatable(table_path)
# True

invalid_table_path = "<path/to/nonexistent/table>"
DeltaTable.is_deltatable(invalid_table_path)
# False

bucket_table_path = "<path/to/valid/table/in/bucket>"
storage_options = {
    "AWS_ACCESS_KEY_ID": "THE_AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY": "THE_AWS_SECRET_ACCESS_KEY",
    ...
}
DeltaTable.is_deltatable(bucket_table_path, storage_options)
# True
```


## Custom Storage Backends

While delta always needs its internal storage backend to work and be
properly configured, in order to manage the delta log, it may sometime
be advantageous - and is common practice in the arrow world - to
customize the storage interface used for reading the bulk data.

`deltalake` will work with any storage compliant with `pyarrow.fs.FileSystem`, however the root of the filesystem has to be adjusted to point at the root of the Delta table. We can achieve this by wrapping the custom filesystem into a `pyarrow.fs.SubTreeFileSystem`.

```python
import pyarrow.fs as fs
from deltalake import DeltaTable

path = "<path/to/table>"
filesystem = fs.SubTreeFileSystem(path, fs.LocalFileSystem())

dt = DeltaTable(path)
ds = dt.to_pyarrow_dataset(filesystem=filesystem)
```

When using the pyarrow factory method for file systems, the normalized
path is provided on creation. In case of S3 this would look something
like:

```python
import pyarrow.fs as fs
from deltalake import DeltaTable

table_uri = "s3://<bucket>/<path>"
raw_fs, normalized_path = fs.FileSystem.from_uri(table_uri)
filesystem = fs.SubTreeFileSystem(normalized_path, raw_fs)

dt = DeltaTable(table_uri)
ds = dt.to_pyarrow_dataset(filesystem=filesystem)
```

## Time Travel

To load previous table states, you can provide the version number you
wish to load:

```python
>>> dt = DeltaTable("../rust/tests/data/simple_table", version=2)
```

Once you've loaded a table, you can also change versions using either a
version number or datetime string:

```python
>>> dt.load_version(1)
>>> dt.load_with_datetime("2021-11-04 00:05:23.283+00:00")
```

!!! warning

    Previous table versions may not exist if they have been vacuumed, in
    which case an exception will be thrown. See [Vacuuming
    tables](managing-tables.md#vacuuming-tables) for more information.
