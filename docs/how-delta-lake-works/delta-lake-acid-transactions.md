# Delta Lake Transactions

This page teaches you about Delta Lake transactions and why transactions are important in production data settings. Data lakes don’t support transactions and this is a huge downside because they offer a poor user experience, lack functionality, and can easily be corrupted.

Transactions on Delta Lake tables are operations that change the state of table and record descriptive entries (metadata) of those changes to the Delta Lake transaction log. Here are some examples of transactions:

- Deleting rows
- Appending to the table
- Compacting small files
- Upserting
- Overwriting rows

All Delta Lake write operations are transactions in Delta tables. Reads actually aren’t technically transactions because they don’t result in new entries being appended to the transaction log.

## What are transactions?

Transactions are any Delta operation that change the underlying files of a Delta table and result in new entries metadata entries in the transaction log. Some Delta operations rearrange data in the existing table (like Z Ordering the table or compacting the small files) and these are also transactions. Let’s look at a simple example.

Suppose you have a Delta table with the following data:

```
num animal
1   cat
2   dog
3   snake
```

Here’s how to create this Delta table:

```python
import pandas as pd
from deltalake import write_deltalake, DeltaTable

df = pd.DataFrame({"num": [1, 2, 3], "animal": ["cat", "dog", "snake"]})
write_deltalake("tmp/my-delta-table", df)
```

Here are the files created in storage.

```
tmp/my-delta-table
├── 0-fea2de92-861a-423e-9708-a9e91dafb27b-0.parquet
└── _delta_log
    └── 00000000000000000000.json
```

Let’s perform an operation to delete every animal from the Delta table that is a cat.

```python
dt = DeltaTable("tmp/my-delta-table")
dt.delete("animal = 'cat'")
```

Let’s take a look at the contents of the Delta table now that the transaction is complete:

```
tmp/my-delta-table
├── 0-fea2de92-861a-423e-9708-a9e91dafb27b-0.parquet
├── _delta_log
│   ├── 00000000000000000000.json
│   └── 00000000000000000001.json
└── part-00001-90312b96-b487-4a8f-9edc-1b9b3963f136-c000.snappy.parquet
```

Notice the `00000000000000000001.json` file that was added to the transaction log to record this transaction. Let’s inspect the content of the file.

```
{
  "add": {
    "path": "part-00001-90312b96-b487-4a8f-9edc-1b9b3963f136-c000.snappy.parquet",
    "partitionValues": {},
    "size": 858,
    "modificationTime": 1705070631953,
    "dataChange": true,
    "stats": "{\"numRecords\":2,\"minValues\":{\"num\":2,\"animal\":\"dog\"},\"maxValues\":{\"num\":3,\"animal\":\"snake\"},\"nullCount\":{\"num\":0,\"animal\":0}}",
    "tags": null,
    "deletionVector": null,
    "baseRowId": null,
    "defaultRowCommitVersion": null,
    "clusteringProvider": null
  }
}
{
  "remove": {
    "path": "0-fea2de92-861a-423e-9708-a9e91dafb27b-0.parquet",
    "dataChange": true,
    "deletionTimestamp": 1705070631953,
    "extendedFileMetadata": true,
    "partitionValues": {},
    "size": 895
  }
}
{
  "commitInfo": {
    "timestamp": 1705070631953,
    "operation": "DELETE",
    "operationParameters": {
      "predicate": "animal = 'cat'"
    },
    "readVersion": 0,
    "operationMetrics": {
      "execution_time_ms": 8013,
      "num_added_files": 1,
      "num_copied_rows": 2,
      "num_deleted_rows": 1,
      "num_removed_files": 1,
      "rewrite_time_ms": 2,
      "scan_time_ms": 5601
    },
    "clientVersion": "delta-rs.0.17.0"
  }
}
```

We can see that this transaction includes two components:

- Remove file `0-fea2de92-861a-423e-9708-a9e91dafb27b-0.parquet`
- Add file `part-00001-90312b96-b487-4a8f-9edc-1b9b3963f136-c000.snappy.parquet`

Transactions are recorded in the transaction log. The transaction log is also referred to as the table metadata and is the `_delta_log` directory in storage.

Let’s see how Delta Lake implements transactions.

## How Delta Lake implements transactions

Here is how Delta Lake implements transactions:

1. Read the existing metadata
2. Read the existing Parquet data files
3. Write the Parquet files for the current transaction
4. Record the new transaction in the transaction log (if there are no conflicts)

Let’s recall our delete operation from the prior section and see how it fits into this transaction model:

1. We read the existing metadata to find the file paths for the existing Parquet files
2. We read the existing Parquet files and identify the files that contains data that should be removed
3. We write new Parquet files with the deleted data filtered out
4. Once the new Parquet files are written, we check for conflicts and then make an entry in the transaction log. The next section will discuss transaction conflicts in more detail.

Blind append operations can skip a few steps and are executed as follows:

1. Write the Parquet files for the current transaction
2. Record the new transaction in the metadata

Delta implements a non locking MVCC (multi version concurrency control) so writers optimistically write new data and simply abandon the transaction if it conflicts at the end. The alternative would be getting a lock at the start thereby guaranteeing the transaction immediately.

Let’s look at the case when a Delta Lake transaction conflicts.

## How Delta Lake transactions can conflict

Suppose you have a transaction that deletes a row of data that’s stored in FileA (Transaction 1). While this job is running, there is another transaction that deletes some other rows in FileA (Transaction 2). Transaction 1 finishes running first and is recorded in the metadata.

Before Transaction 2 is recorded as a transaction, it will check the metadata, find that Transaction 2 conflicts with a transaction that was already recorded (from Transaction 1), and error without recording a new transaction.

Transactions 2 will write Parquet data files, but will not be recorded as a transaction, so the data files will be ignored. The zombie Parquet files can be easily cleaned up via subsequent vacuum operations.

Transaction 2 must fail otherwise it would cause the data to be incorrect.

Delta Lake transactions prevent users from making changes that would corrupt the table. Transaction conflict behavior can differ based on isolation level, which controls the degree to which a transaction must be isolated from modifications made by other concurrent transactions. More about this in the concurrency section.

## Transactions rely on atomic primitives storage guarantees

Suppose you have two transactions that are finishishing at the same exact time. Both of these transactions look at the existing Delta Lake transaction log, see that the latest transaction was `003.json` and determine that the next entry should be `004.json`.

If both transactions are recorded in the `004.json` file, then one of them will be clobbered, and the transaction log entry for the clobbered metadata entry will be lost.

Delta tables rely on storage systems that provide atomic primitives for safe concurrency. The storage system must allow Delta Lake to write the file, _only if it does not exist already_, and error out otherwise. The storage system must NOT permit concurrent writers to overwrite existing metadata entries.

Some clouds have filesystems that don’t explicitly support these atomic primitives, and therefore must be coupled with other services to provide the necessary guarantees.

## Delta Lake transactions are only for a single table

Delta Lake transactions are only valid for a single table.

Some databases offer transaction support for operations that impact multiple tables. Delta Lake does not support multi-table transactions.

## Data lakes don’t support transactions

Data lakes consist of many files in a storage system (e.g. a cloud storage system) and don’t support transactions.

Data lakes don’t have a metadata layer, conflict resolution, or any way to store information about transactions.

Data lakes are prone to multiple types of errors because they don’t support transactions:

- Easy to corrupt
- Downtime/unstable state while jobs are running
- Operations can conflict

Data lakes have many downsides and it’s almost always better to use a lakehouse storage system like Delta Lake compared to a data lake.

## ACID Transactions

We’ve already explored how Delta Lake supports transactions. This section explains how Delta Lake transactions have the Atomic, Consistent, Isolated and Durable (ACID transaction) properties. Reading this section is optional.

ACID transactions are commonplace in databases but notably absent for data lakes.

Delta Lake’s ACID transaction support is one of the major reasons it is almost always a better option than a data lake.

Let’s explore how Delta Lake allows for ACID transactions.

**Atomic transactions**

An atomic transaction either fully completes or fully fails, with nothing in between.

Delta Lake transactions are atomic, unlike data lake transactions that are not atomic.

Suppose you have a job that’s writing 100 files to a table. Further suppose that the job errors out and the cluster dies after writing 40 files:

- For a Delta table, no additional data will be added to the table. Parquet files were written to the table, but the job errored, so no transaction log entry was added and no data was added to the table.
- For a data lake, the 40 files are added and the transaction “partially succeeds”.

For data tables, it’s almost always preferable to have a transaction that “fully fails” instead of one that “partially succeeds” because partial writes are hard to unwind and debug.

Delta Lake implements atomic transactions by writing data files first before making a new entry in the Delta transaction log.

These guarantees are provided at the protocol level through the "transaction" abstraction. We’ve already discussed what constitutes a transaction for Delta Lake.

If there is an error with the transaction and some files don’t get written, then no metadata entry is made and the partial data write is ignored. The zombie Parquet files can be easily cleaned up via subsequent vacuum operations.

Now let’s look at how Delta Lake also provides consistent transactions.

**Consistent transactions**

Consistency means that transactions won’t violate integrity constraints on the Delta table.

Delta Lake has two types of consistency checks:

- Schema enforcement checks
- Column constraints

Schema enforcement checks verify that new data appended to a Delta table matches the schema of the existing table. You cannot append data with a different schema, unless you enable schema evolution.

Delta Lake column constraints allow users to specify the requirements of data that’s added to a Delta table. For example, if you have an age column with a constraint that requires the value to be positive, then Delta Lake will reject appends of any data that doesn’t meet the constraint.

Data lakes don’t support schema enforcement or column constraints. That’s another reason why data lakes are not ACID-compliant.

**Isolated transactions**

Isolation means that transactions are applied to a Delta table sequentially.

Delta Lake transactions are persisted in monotonically increasing transaction files, as we saw in the previous example. First `00000000000000000000.json`, then `00000000000000000001.json`, then `00000000000000000002.json`, and so on.

Delta Lake uses concurrency control to ensure that transactions are executed sequentially, even when user operations are performed concurrently. The next page of this guide explains concurrency in Delta Lake in detail.

**Durable transactions**

Delta tables are generally persisted in cloud object stores which provide durability guarantees.

Durability means that all transactions that are successfully completed will always remain persisted, even if there are service outages or program crashes.

Suppose you have a Delta table that’s persisted in Azure blob storage. The Delta table transactions that are committed will always remain available, even in these circumstances:

- When there are Azure service outages
- If a computation cluster that’s writing the Delta table crashes for some reason
- Two operations are running concurrently and one of them fails

Successful transactions are always registered in the Delta table and persisted no matter what.

## Conclusion

Delta Lake supports transactions which provide necessary reliability guarantees for production data systems.

Vanilla data lakes don’t provide transactions and this can cause nasty bugs and a bad user experience. Let’s look at a couple of scenarios when the lack of transactions cause a poor user experience:

- While running a compaction operation on a data lake, newly compacted “right sized” files are added before the small files are deleted. If you read the data lake while this operation is running, you will see duplicate data.
- While writing to a data lake, a job might fail, which leaves behind partially written files. These files are corrupt, which means that the data lake cannot be read until the corrupt files are manually removed.
- Users want to run a simple DML operation like deleting a few rows of data which require a few files to be rewritten. This operation renders the data lake unusable until it’s done running.

Transactions are a key advantage of Delta Lake vs. data lakes. There are many other advantages, but proper transactions are necessary in production data environments.

Let's take a look at [File Skipping](../how-delta-lake-works/delta-lake-file-skipping.md) in the next section.
