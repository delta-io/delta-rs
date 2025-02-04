# Working with Partitions in Delta Lake

Partitions in Delta Lake let you organize data based on specific columns (for example, date columns or country columns). Partitioning can significantly speed up queries that filter on those columns, because unneeded partitions can be skipped entirely.

Below, we demonstrate how to create, query, and update partitioned Delta tables, covering both Python and Rust examples.


## Creating a Partitioned Table

To create a partitioned Delta table, specify one or more partition columns when writing the data. If you’re using Python, pass `partition_by=[<column>]` to the [write_deltalake()][deltalake.write_deltalake] function. In Rust, you can use `with_partition_columns(...)` on the builder when creating the table.

```python
from deltalake import write_deltalake
import pandas as pd

df = pd.DataFrame({
    "num": [1, 2, 3],
    "letter": ["a", "b", "c"],
    "country": ["US", "US", "CA"]
})

# Create a table partitioned by the "country" column
write_deltalake("tmp/partitioned-table", df, partition_by=["country"])
```
The structure in the “tmp/partitioned-table” folder is showing how Delta Lake organizes data by the partition column. The “_delta_log” folder holds transaction metadata, while each “country=<value>” subfolder contains the Parquet files for rows matching that partition value. This layout allows efficient queries and updates on partitioned data.
```plaintext
tmp/partitioned-table/
├── _delta_log/
│   └── 00000000000000000000.json
├── country=CA/
│   └── part-00000-<uuid>.parquet
├── country=US/
│   └── part-00001-<uuid>.parquet
```

## Querying Partitioned Data

### Filtering by partition columns

Because partition columns are part of the storage path, queries that filter on those columns can skip reading unneeded partitions. You can specify partition filters when reading data with [DeltaTable.to_pandas()][deltalake.table.DeltaTable.to_pandas], [DeltaTable.to_pyarrow_table()][deltalake.table.DeltaTable.to_pyarrow_table], or [DeltaTable.to_pyarrow_dataset()][deltalake.table.DeltaTable.to_pyarrow_dataset].

```python
from deltalake import DeltaTable

dt = DeltaTable("tmp/partitioned-table")

# Only read files from partitions where country = 'US'
pdf = dt.to_pandas(partitions=[("country", "=", "US")])
print(pdf)
```
```plaintext
    num letter country
0    1      a      US
1    2      b      US
```

### Partition Columns in Table Metadata

Partition columns can also be inspected via metadata:

```python
from deltalake import DeltaTable

dt = DeltaTable("tmp/partitioned-table")
print(dt.metadata().partition_columns)
```

```plaintext
['country']
```

## Appending and Overwriting Partitions

### Appending to a Partitioned Table

You can simply write additional data with mode="append" and the partition columns will be used to place data in the correct partition directories.

```python
new_data = pd.DataFrame({
    "num": [10, 20, 30],
    "letter": ["x", "y", "z"],
    "country": ["CA", "DE", "DE"]
})
from deltalake import write_deltalake

write_deltalake("tmp/partitioned-table", new_data, mode="append")
```

### Overwriting an Entire Partition

You can overwrite a specific partition, leaving the other partitions intact. Pass in mode="overwrite" together with partition_filters.
```python
df_overwrite = pd.DataFrame({
    "num": [900, 1000],
    "letter": ["m", "n"],
    "country": ["DE", "DE"]
})

from deltalake import DeltaTable, write_deltalake

dt = DeltaTable("tmp/partitioned-table")
write_deltalake(
    dt,
    df_overwrite,
    partition_filters=[("country", "=", "DE")],
    mode="overwrite",
)
```
This will remove only the `country=DE` partition files and overwrite them with the new data.

### Overwriting Parts of the Table Using a Predicate

If you have a more fine-grained predicate than a partition filter, you can use the [predicate argument][deltalake.write_deltalake] (sometimes called replaceWhere) to overwrite only rows matching a specific condition.

(See the “Overwriting part of the table data using a predicate” section in the Writing Delta Tables docs for more details.)

## Updating Partitioned Tables with Merge

You can perform merge operations on partitioned tables in the same way you do on non-partitioned ones. Simply provide a matching predicate that references partition columns if needed.

You can match on both the partition column (country) and some other condition. This example shows a merge operation that checks both the partition column (“country”) and a numeric column (“num”) when merging:
- The table is partitioned by “country,” so underlying data is physically split by each country value.
- The merge condition (predicate) matches target rows where both “country” and “num” align with the source.
- When a match occurs, it updates “letter”; otherwise, it inserts the new row.
- This approach ensures that only rows in the relevant partition (“US”) are processed, keeping operations efficient.

```python
from deltalake import DeltaTable
import pyarrow as pa

dt = DeltaTable("tmp/partitioned-table")

# New data that references an existing partition "US"
source_data = pa.table({"num": [1, 101], "letter": ["A", "B"], "country": ["US", "US"]})

(
    dt.merge(
        source=source_data,
        predicate="target.country = source.country AND target.num = source.num",
        source_alias="source",
        target_alias="target"
    )
    .when_matched_update(
        updates={"letter": "source.letter"}
    )
    .when_not_matched_insert_all()
    .execute()
)
```

## Deleting Partition Data

You may want to delete all rows from a specific partition. For example:
```python
dt = DeltaTable("tmp/partitioned-table")

# Delete all rows from the 'US' partition:
dt.delete("country = 'US'")
```
This command logically deletes the data by creating a new transaction.

## Maintaining Partitioned Tables

### Optimize & Vacuum

Partitioned tables can accummulate many small files if a partition is frequently appended to. You can compact these into larger files on a specific partition:
```python
dt.optimize(partition_filters=[("country", "=", "US")])
```

Then optionally vacuum the table to remove older, unreferenced files.

### Handling High-Cardinality Columns

Partitioning can be very powerful, but be mindful of using high-cardinality columns (columns with too many unique values). This can create an excessive number of directories and can hurt performance. For example, partitioning by date is typically better than partitioning by user_id if user_id has

