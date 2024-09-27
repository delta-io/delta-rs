# GCS Storage Backend

`delta-rs` offers native support for using Google Cloud Storage (GCS) as an object storage backend.

You don’t need to install any extra dependencies to read/write Delta tables to GCS with engines that use `delta-rs`. You do need to configure your GCS access credentials correctly.

## Using Application Default Credentials

Application Default Credentials (ADC) is a strategy used by GCS to automatically find credentials based on the application environment.

If you are working from your local machine and have ADC set up then you can read/write Delta tables from GCS directly, without having to pass your credentials explicitly.

## Example: Write Delta tables to GCS with Polars

Using Polars, you can write a Delta table to GCS like this:

```python
# create a toy dataframe
import polars as pl
df = pl.DataFrame({"foo": [1, 2, 3, 4, 5]})

# define path
table_path = "gs://bucket/delta-table"

# write Delta to GCS
df.write_delta(table_path)
```

## Passing GCS Credentials explicitly

Alternatively, you can pass GCS credentials to your query engine explicitly.

For Polars, you would do this using the `storage_options` keyword. This will forward your credentials to the `object store` library that Polars uses under the hood. Read the [Polars documentation](https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_delta.html) and the [`object store` documentation](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html#variants) for more information.

## Delta Lake on GCS: Required permissions

You will need the following permissions in your GCS account:

- `storage.objects.create`
- `storage.objects.delete` (only required for uploads that overwrite an existing object)
- `storage.objects.get` (only required if you plan on using the Google Cloud CLI)
- `storage.objects.list` (only required if you plan on using the Google Cloud CLI)

For more information, see the [GCP documentation](https://cloud.google.com/storage/docs/uploading-objects)
