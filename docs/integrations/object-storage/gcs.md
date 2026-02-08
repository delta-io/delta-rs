# GCS Storage Backend

`delta-rs` offers native support for using Google Cloud Storage (GCS) as an object storage backend.

You don’t need to install any extra dependencies to read/write Delta tables to GCS with engines that use `delta-rs`. You do need to configure your GCS access credentials correctly.

## Using Rust with GCS

When using the Rust `deltalake` crate with the `gcs` feature enabled, GCS support is **automatically registered** at program startup. You don't need to manually call any registration functions.

### Before (manual registration - deprecated)

```rust
// Legacy approach where applications had to register the handler
deltalake::gcp::register_handlers(None);

let ops = DeltaOps::try_from_uri("gs://bucket/table".parse()?).await?;
```

### After (automatic registration)

```rust
// Enable the gcs feature in Cargo.toml:
// deltalake = { version = "0.29", features = ["gcs"] }

use deltalake::DeltaOps;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The gs:// scheme is recognized automatically
    let ops = DeltaOps::try_from_uri("gs://bucket/table".parse()?).await?;
    Ok(())
}
```

The automatic registration happens via constructor hooks in the meta-crate (see the [CHANGELOG](https://github.com/delta-io/delta-rs/blob/main/CHANGELOG.md) for details).

**Note**: If you're using `deltalake-core` and individual storage crates directly (instead of the `deltalake` meta-crate), you'll still need to call `deltalake_gcp::register_handlers(None)` manually.

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

## Configuration Reference

The following table lists all available configuration options that can be passed via the `storage_options` parameter when working with Google Cloud Storage. These options correspond to the `GoogleConfigKey` enum from the `object_store` crate.

| Configuration Key | Environment Variable | Description |
|-------------------|---------------------|-------------|
| `service_account` | `GOOGLE_SERVICE_ACCOUNT` | Path to service account JSON file for authentication |
| `service_account_key` | `GOOGLE_SERVICE_ACCOUNT_KEY` | Serialized service account key JSON string |
| `application_credentials` | `GOOGLE_APPLICATION_CREDENTIALS` | Path to Application Default Credentials (ADC) file |
| `bucket` / `bucket_name` | `GOOGLE_BUCKET` | GCS bucket name (alternative to specifying in URL) |
| `endpoint` | `GOOGLE_ENDPOINT` | Custom GCS endpoint URL (for testing or GCS-compatible services) |

### Supported URL Schemes

Delta Lake on Google Cloud Storage supports the following URL scheme:

- `gs://bucket-name/path/to/table` - Google Cloud Storage URL

### Authentication Methods

GCS authentication can be configured in several ways (in order of precedence):

1. **Service Account Key** - Provide a service account JSON via `service_account_key` or `service_account` (file path)
2. **Application Default Credentials (ADC)** - Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to a credentials file
3. **GCloud CLI Credentials** - If authenticated via `gcloud auth application-default login`, credentials will be automatically discovered
4. **Workload Identity** - For applications running on GKE, credentials are automatically provided via workload identity

!!! note
    For the complete and authoritative list of configuration options, refer to the [object_store GoogleConfigKey documentation](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html).
