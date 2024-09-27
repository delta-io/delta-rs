# Azure ADLS Storage Backend

`delta-rs` offers native support for using Microsoft Azure Data Lake Storage (ADSL) as an object storage backend.

You don’t need to install any extra dependencies to read/write Delta tables to S3 with engines that use `delta-rs`. You do need to configure your ADLS access credentials correctly.

## Passing Credentials Explicitly

You can also pass ADLS credentials to your query engine explicitly.

For Polars, you would do this using the `storage_options` keyword as demonstrated above. This will forward your credentials to the `object store` library that Polars uses for cloud storage access under the hood. Read the [`object store` documentation](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants) for more information defining specific credentials.

## Example: Write Delta table to ADLS with Polars

Using Polars, you can write a Delta table to ADLS directly like this:

```python
import polars as pl

df = pl.DataFrame({"foo": [1, 2, 3, 4, 5]})

# define container name
container = <container_name>

# define credentials
storage_options = {
    "ACCOUNT_NAME": <account_name>,
    "ACCESS_KEY": <access_key>,
}

# write Delta to ADLS
df_pl.write_delta(
    f"abfs://{container}/delta_table",
    storage_options = storage_options
)
```

## Example with pandas

For libraries without direct `write_delta` methods (like Pandas), you can use the `write_deltalake` function from the `deltalake` library:

```python
import pandas as pd
from deltalake import write_deltalake

df = pd.DataFrame({"foo": [1, 2, 3, 4, 5]})

write_deltalake(
    f"abfs://{container}/delta_table_pandas",
    df,
    storage_options=storage_options
)
```

## Using Local Authentication

If your local session is authenticated using the Azure CLI then you can write Delta tables directly to ADLS. Read more about this in the [Azure CLI documentation](https://learn.microsoft.com/en-us/cli/azure/).
