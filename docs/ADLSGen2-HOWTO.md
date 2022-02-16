HOW-TO: Azure Data Lake Storage Gen2 (ADLS Gen2)
================================================

*"Azure Data Lake Storage Gen2 is a set of capabilities dedicated to big data analytics, built on Azure Blob Storage."*

Source: https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction

## Usage

Using ADLS Gen2 requires creating and Azure Storage Account with specific settings.

This can be done via an [ARM template](https://docs.microsoft.com/en-us/azure/azure-resource-manager/templates/overview) or via the [Azure Portal](https://docs.microsoft.com/en-us/azure/azure-portal/azure-portal-overview).

### ARM Template

Docs: [Microsoft.Storage/storageAccounts - Bicep & ARM template reference | Microsoft Docs](https://docs.microsoft.com/en-us/azure/templates/microsoft.storage/storageaccounts?tabs=json)

The critical part for creating an ADLS Gen2 Storage Account via ARM are the following properties:

* `kind = StorageV2`
* `isHnsEnabled = true`

### Azure Portal

Docs: [Create a storage account for Azure Data Lake Storage Gen2 | Microsoft Docs](https://docs.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account)

## Resources

* [Multi-protocol access on Azure Data Lake Storage | Microsoft Docs](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-multi-protocol-access)
* [Blob service REST API - Azure Storage | Microsoft Docs](https://docs.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api)
* [Azure Data Lake Storage Gen2 REST API reference - Azure Storage | Microsoft Docs](https://docs.microsoft.com/en-us/rest/api/storageservices/data-lake-storage-gen2)

## Example

When you want to connect to a deltatable stored in ADLS Gen2, you need to set two environment variables. 
* AZURE_STORAGE_ACCOUNT_NAME which holds the name of the storage account 
* AZURE_STORAGE_ACCOUNT_KEY which contains the root key for the storage account. 

The url for the table should follow a specific format adls2://{accountname}/{filesystem}/{path to table}. 

example:
```python
  from deltalake import DeltaTable

  delta = DeltaTable("adls2://<accountname>/<filesystem>/<path to table>")
  dataFrames = delta.to_pyarrow_table().to_pandas()
```
