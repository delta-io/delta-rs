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

## Using Azure CLI

If your local session is authenticated using the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/) then you can read and write Delta tables directly to ADLS.

```python
from deltalake import write_deltalake

# build path
storage_account_name = "<STORAGE_ACCOUNT_NAME>"
container_name = "<CONTAINER_NAME>"
table_name = "<TABLE_NAME>" 
abfs_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{table_name}/"

# define credentials
storage_options = {
    "azure_tenant_id": "<TENANT_ID>",
    "azure_use_azure_cli": "true",
}

dt = DeltaTable(abfs_path,storage_options=storage_options)
```

## Configuration Reference

The following table lists all available configuration options that can be passed via the `storage_options` parameter when working with Azure Data Lake Storage. These options correspond to the `AzureConfigKey` enum from the `object_store` crate.

| Configuration Key | Environment Variable | Description |
|-------------------|---------------------|-------------|
| `account_name` | `AZURE_STORAGE_ACCOUNT_NAME` | Azure storage account name |
| `access_key` | `AZURE_STORAGE_ACCOUNT_KEY` | Azure storage account access key |
| `client_id` | `AZURE_STORAGE_CLIENT_ID` | Service principal client ID for Azure AD authentication |
| `client_secret` | `AZURE_STORAGE_CLIENT_SECRET` | Service principal client secret |
| `authority_id` / `tenant_id` | `AZURE_STORAGE_TENANT_ID` | Azure Active Directory tenant ID |
| `sas_key` | `AZURE_STORAGE_SAS_KEY` | Shared Access Signature (SAS) token (must be percent-encoded) |
| `bearer_token` | `AZURE_STORAGE_TOKEN` | Bearer token for authentication |
| `use_emulator` | `AZURE_STORAGE_USE_EMULATOR` | Use Azurite storage emulator (set to `true`) |
| `endpoint` | `AZURE_STORAGE_ENDPOINT` | Custom Azure endpoint URL |
| `use_azure_cli` | `AZURE_STORAGE_USE_AZURE_CLI` | Use credentials from Azure CLI (set to `true`) |
| `federated_token_file` | `AZURE_FEDERATED_TOKEN_FILE` | Path to federated token file for workload identity |
| `container_name` | `AZURE_STORAGE_CONTAINER_NAME` | Container name (alternative to specifying in URL) |
| `msi_endpoint` | `IDENTITY_ENDPOINT` or `AZURE_MSI_ENDPOINT` | Managed Service Identity (MSI) endpoint |
| `object_id` | `AZURE_STORAGE_OBJECT_ID` | Object ID for managed identity |
| `msi_resource_id` | `AZURE_STORAGE_MSI_RESOURCE_ID` | MSI resource ID |
| `skip_signature` | `AZURE_STORAGE_SKIP_SIGNATURE` | Skip request signature (set to `true` for anonymous access) |
| `use_fabric_endpoint` | `AZURE_STORAGE_USE_FABRIC_ENDPOINT` | Use Microsoft Fabric endpoint (set to `true`) |
| `disable_tagging` | `AZURE_STORAGE_DISABLE_TAGGING` | Disable blob tagging (set to `true` if not supported) |

### Supported URL Schemes

Delta Lake on Azure ADLS supports the following URL schemes:

- `abfss://container@account.dfs.core.windows.net/path/to/table` - Azure Blob File System Secure (ABFSS)
- `abfs://container@account.dfs.core.windows.net/path/to/table` - Azure Blob File System
- `az://container/path/to/table` - Short form Azure scheme
- `adl://container/path/to/table` - Azure Data Lake scheme

!!! note
    For the complete and authoritative list of configuration options, refer to the [object_store AzureConfigKey documentation](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html).
