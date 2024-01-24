# Architecture of a Delta Lake table

A Delta table consists of Parquet files that contain data and a transaction log that stores metadata about the transactions.

![](contents-of-delta-table.png)

Let's create a Delta table, perform some operations, and inspect the files that are created.

## Delta Lake transaction examples

Start by creating a pandas DataFrame and writing it out to a Delta table.

```python
import pandas as pd
from deltalake import DeltaTable, write_deltalake

df = pd.DataFrame({"num": [1, 2, 3], "letter": ["a", "b", "c"]})
write_deltalake("tmp/some-table", df)
```

Now inspect the files created in storage:

```
tmp/some-table
├── 0-62dffa23-bbe1-4496-8fb5-bff6724dc677-0.parquet
└── _delta_log
    └── 00000000000000000000.json
```

The Parquet file stores the data that was written.  The `_delta_log` directory stores metadata about the transactions.  Let's inspect the `_delta_log/00000000000000000000.json` file.

```json
{
  "protocol": {
    "minReaderVersion": 1,
    "minWriterVersion": 1
  }
}
{
  "metaData": {
    "id": "b96ea1a2-1830-4da2-8827-5334cc6104ed",
    "name": null,
    "description": null,
    "format": {
      "provider": "parquet",
      "options": {}
    },
    "schemaString": "{\"type\":\"struct\",\"fields\":[{\"name\":\"num\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"letter\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
    "partitionColumns": [],
    "createdTime": 1701740315599,
    "configuration": {}
  }
}
{
  "add": {
    "path": "0-62dffa23-bbe1-4496-8fb5-bff6724dc677-0.parquet",
    "size": 2208,
    "partitionValues": {},
    "modificationTime": 1701740315597,
    "dataChange": true,
    "stats": "{\"numRecords\": 3, \"minValues\": {\"num\": 1, \"letter\": \"a\"}, \"maxValues\": {\"num\": 3, \"letter\": \"c\"}, \"nullCount\": {\"num\": 0, \"letter\": 0}}"
  }
}
{
  "commitInfo": {
    "timestamp": 1701740315602,
    "operation": "CREATE TABLE",
    "operationParameters": {
      "location": "file:///Users/matthew.powers/Documents/code/delta/delta-examples/notebooks/python-deltalake/tmp/some-table",
      "metadata": "{\"configuration\":{},\"created_time\":1701740315599,\"description\":null,\"format\":{\"options\":{},\"provider\":\"parquet\"},\"id\":\"b96ea1a2-1830-4da2-8827-5334cc6104ed\",\"name\":null,\"partition_columns\":[],\"schema\":{\"fields\":[{\"metadata\":{},\"name\":\"num\",\"nullable\":true,\"type\":\"long\"},{\"metadata\":{},\"name\":\"letter\",\"nullable\":true,\"type\":\"string\"}],\"type\":\"struct\"}}",
      "protocol": "{\"minReaderVersion\":1,\"minWriterVersion\":1}",
      "mode": "ErrorIfExists"
    },
    "clientVersion": "delta-rs.0.17.0"
  }
}
```

The tranasction log file contains the following information:

* the files added to the Delta table
* schema of the files
* column level metadata including the min/max value for each file

Create another pandas DataFrame and append it to the Delta table to see how this transaction is recorded.

```python
df = pd.DataFrame({"num": [8, 9], "letter": ["dd", "ee"]})
write_deltalake(f"{cwd}/tmp/delta-table", df, mode="append")
```

Here are the files in storage:

```
tmp/some-table
├── 0-62dffa23-bbe1-4496-8fb5-bff6724dc677-0.parquet
├── 1-57abb6fb-2249-43ba-a7be-cf09bcc230de-0.parquet
└── _delta_log
    ├── 00000000000000000000.json
    └── 00000000000000000001.json
```

Here are the contents of the `_delta_log/00000000000000000001.json` file:

```json
{
  "add": {
    "path": "1-57abb6fb-2249-43ba-a7be-cf09bcc230de-0.parquet",
    "size": 2204,
    "partitionValues": {},
    "modificationTime": 1701740386169,
    "dataChange": true,
    "stats": "{\"numRecords\": 2, \"minValues\": {\"num\": 8, \"letter\": \"dd\"}, \"maxValues\": {\"num\": 9, \"letter\": \"ee\"}, \"nullCount\": {\"num\": 0, \"letter\": 0}}"
  }
}
{
  "commitInfo": {
    "timestamp": 1701740386169,
    "operation": "WRITE",
    "operationParameters": {
      "partitionBy": "[]",
      "mode": "Append"
    },
    "clientVersion": "delta-rs.0.17.0"
  }
}
```

The transaction log records that the second file has been persisted in the Delta table.

Now create a third pandas DataFrame and overwrite the Delta table with the new data.

```python
df = pd.DataFrame({"num": [11, 22], "letter": ["aa", "bb"]})
write_deltalake(f"{cwd}/tmp/delta-table", df, mode="append")
```

Here are the files in storage:

```
tmp/some-table
├── 0-62dffa23-bbe1-4496-8fb5-bff6724dc677-0.parquet
├── 1-57abb6fb-2249-43ba-a7be-cf09bcc230de-0.parquet
├── 2-95ef2108-480c-4b89-96f0-ff9185dab9ad-0.parquet
└── _delta_log
    ├── 00000000000000000000.json
    ├── 00000000000000000001.json
    └── 00000000000000000002.json
```

Here are the contents of the `_delta_log/0002.json` file:

```json
{
  "add": {
    "path": "2-95ef2108-480c-4b89-96f0-ff9185dab9ad-0.parquet",
    "size": 2204,
    "partitionValues": {},
    "modificationTime": 1701740465102,
    "dataChange": true,
    "stats": "{\"numRecords\": 2, \"minValues\": {\"num\": 11, \"letter\": \"aa\"}, \"maxValues\": {\"num\": 22, \"letter\": \"bb\"}, \"nullCount\": {\"num\": 0, \"letter\": 0}}"
  }
}
{
  "remove": {
    "path": "0-62dffa23-bbe1-4496-8fb5-bff6724dc677-0.parquet",
    "deletionTimestamp": 1701740465102,
    "dataChange": true,
    "extendedFileMetadata": false,
    "partitionValues": {},
    "size": 2208
  }
}
{
  "remove": {
    "path": "1-57abb6fb-2249-43ba-a7be-cf09bcc230de-0.parquet",
    "deletionTimestamp": 1701740465102,
    "dataChange": true,
    "extendedFileMetadata": false,
    "partitionValues": {},
    "size": 2204
  }
}
{
  "commitInfo": {
    "timestamp": 1701740465102,
    "operation": "WRITE",
    "operationParameters": {
      "mode": "Overwrite",
      "partitionBy": "[]"
    },
    "clientVersion": "delta-rs.0.17.0"
  }
}
```

This transaction adds a data file and marks the two exising data files for removal.  Marking a file for removal in the transaction log is known as "tombstoning the file" or a "logical delete".  This is different from a "physical delete" which actually removes the data file from storage.

## How Delta table operations differ from data lakes

Data lakes consist of data files persisted in storage.  They don't have a transaction log that retain metadata about the transactions.

Data lakes perform transactions differently than Delta tables.

When you perform an overwrite tranasction with a Delta table, you logically delete the exiting data without physically removing it.

Data lakes don't support logical deletes, so you have to physically delete the data from storage.

Logical data operations are safer because they can be rolled back if they don't complete successfully.  Physically removing data from storage can be dangerous, especially if it's before a transaction is complete.

We're now ready to look into Delta Lake ACID transactions in more detail.
