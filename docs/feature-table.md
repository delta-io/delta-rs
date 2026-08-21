# Delta late feature table

The following section outlines some core features like supported [storage backends](#cloud-integrations)
and [operations](#supported-operations) that can be performed against tables. The state of implementation
of features outlined in the Delta [protocol][protocol] is also [tracked](#protocol-support-level).

### Cloud Integrations

| Storage              | Rust | Python   |
|----------------------|:----:|:--------:|
| Local                |  ✅   |   ✅    |
| S3 - AWS             |  ✅   |   ✅    |
| S3 - MinIO           |  ✅   |   ✅    |
| S3 - R2              |  ✅   |   ✅    |
| Azure Blob           |  ✅   |   ✅    |
| Azure ADLS Gen2      |  ✅   |   ✅    |
| Microsoft OneLake    |  ✅   |   ✅    |
| Google Cloud Storage |  ✅   |   ✅    |
| HDFS                 |  ✅   |   ✅    |
| LakeFS               |  ✅   |   ✅    |

### Supported Operations

| Operation             | Rust | Python | Description                                      |
|-----------------------|:----:|:------:|--------------------------------------------------|
| Create                |  ✅   |   ✅    | Create a new table                             |
| Read                  |  ✅   |   ✅    | Read data from a table                         |
| Vacuum                |  ✅   |   ✅    | Remove unused files and log entries            |
| Delete - predicates   |  ✅   |   ✅    | Delete data based on a predicate               |
| Optimize - compaction |  ✅   |   ✅    | Harmonize the size of data file                |
| Optimize - Z-order    |  ✅   |   ✅    | Place similar data into the same file          |
| Merge                 |  ✅   |   ✅    | Merge a target Delta table with source data    |
| Update                |  ✅   |   ✅    | Update values from a table                     |
| Add Column            |  ✅   |   ✅    | Add new columns or nested fields               |
| Add Feature           |  ✅   |   ✅    | Enable delta table features                    |
| Add Constraints       |  ✅   |   ✅    | Set delta constraints, to verify data on write |
| Drop Constraints      |  ✅   |   ✅    | Removes delta constraints                      |
| Set Table Properties  |  ✅   |   ✅    | Set delta table properties                     |
| Convert to Delta      |  ✅   |   ✅    | Convert parquet table to delta table           |
| FS check              |  ✅   |   ✅    | Remove corrupted files from table              |
| Restore               |  ✅   |   ✅    | Restores table to previous version state       |

### Protocol Support Level

| Writer Version | Requirement                                   | Status |
|----------------|-----------------------------------------------|:------:|
| Version 2      | Append Only Tables                            |   ✅   |
| Version 2      | Column Invariants                             |   ✅   |
| Version 3      | Enforce `delta.checkpoint.writeStatsAsJson`   |   ✅   |
| Version 3      | Enforce `delta.checkpoint.writeStatsAsStruct` |   ✅   |
| Version 3      | CHECK constraints                             |   ✅   |
| Version 4      | Change Data Feed                              |   ✅   |
| Version 4      | Generated Columns                             |   ✅   |
| Version 5      | Column Mapping                                |   ✅   |
| Version 6      | Identity Columns                              |   -    |
| Version 7      | Table Features                                |   ✅   |

| Reader Version | Requirement                         | Status |
|----------------|-------------------------------------|--------|
| Version 2      | Column Mapping                      | ✅     |
| Version 3      | Table Features (requires reader V7) | ✅     |

[protocol]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
