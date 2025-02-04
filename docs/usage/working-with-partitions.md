# Working with Partitions in Delta Lake

Partitions in Delta Lake let you organize data based on specific columns (for example, date columns or country columns). Partitioning can significantly speed up queries that filter on those columns, because unneeded partitions can be skipped entirely.

Below, we demonstrate how to create, query, and update partitioned Delta tables, covering both Python and Rust examples.


## Creating a Partitioned Table

To create a partitioned Delta table, specify one or more partition columns when creating the table. Here we partition by the country column.
```python
from deltalake import write_deltalake,DeltaTable
import pandas as pd

df = pd.DataFrame({
    "num": [1, 2, 3],
    "letter": ["a", "b", "c"],
    "country": ["US", "US", "CA"]
})

# Create a table partitioned by the "country" column
write_deltalake("tmp/partitioned-table", df, partition_by=["country"])
```

The structure in the "tmp/partitioned-table" folder shows how Delta Lake organizes data by the partition column. The "_delta_log" folder holds transaction metadata, while each "country=<value>" subfolder contains the Parquet files for rows matching that partition value. This layout allows efficient queries and updates on partitioned data.
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

Because partition columns are part of the storage path, queries that filter on those columns can skip reading unneeded partitions. You can specify partition filters when reading data with [DeltaTable.to_pandas()](../../delta_table/#deltalake.DeltaTable.to_pandas).


In this example we restrict our query to the `country="US"` partition.
```python
dt = DeltaTable("tmp/partitioned-table")

pdf = dt.to_pandas(partitions=[("country", "=", "US")])
print(pdf)
```
```plaintext
    num letter country
0    1      a      US
1    2      b      US
```

### Partition Columns in Table Metadata

Partition columns can also be inspected via metadata on a `DeltaTable`.

```python
dt = DeltaTable("tmp/partitioned-table")
print(dt.metadata().partition_columns)
```

```plaintext
['country']
```

## Appending and Overwriting Partitions

### Appending to a Partitioned Table

You can write additional data to partitions (or create new partitions) with `mode="append"` and the partition columns will be used to place data in the correct partition directories.

```python
new_data = pd.DataFrame({
    "num": [10, 20, 30],
    "letter": ["x", "y", "z"],
    "country": ["CA", "DE", "DE"]
})

write_deltalake("tmp/partitioned-table", new_data, mode="append")

dt = DeltaTable("tmp/partitioned-table")
pdf = dt.to_pandas()
print(pdf)
```

```plaintext
   num letter country
0   20      y      DE
1   30      z      DE
2   10      x      CA
3    3      c      CA
4    1      a      US
5    2      b      US
```

### Overwriting a Partition

To overwrite a specific partition or partitions set `mode="overwrite"` together with a predicate string that specifies
which partitions are present in the new data. By setting the predicate `deltalake` is able to skip the other partitions.

In this example we overwrite the `DE` partition with new data.

```python
df_overwrite = pd.DataFrame({
    "num": [900, 1000],
    "letter": ["m", "n"],
    "country": ["DE", "DE"]
})

dt = DeltaTable("tmp/partitioned-table")
write_deltalake(
    dt,
    df_overwrite,
    predicate="country = 'DE'",
    mode="overwrite",
)

dt = DeltaTable("tmp/partitioned-table")
pdf = dt.to_pandas()
print(pdf)
```

```plaintext
    num letter country
0   900      m      DE
1  1000      n      DE
2    10      x      CA
3     3      c      CA
4     1      a      US
5     2      b      US
```

## Updating Partitioned Tables with Merge

You can perform merge operations on partitioned tables in the same way you do on non-partitioned ones. If only a subset of existing partitions are present in the source (i.e. new) data then `deltalake` can skip reading the partitions not present in the source data. You can do this by providing a predicate that specifies which partition values are in the source data.

This example shows an upsert merge operation:
- The merge condition (`predicate`) matches rows between source and target based on the partition column and specifies which partitions are present in the source data
- If a match is found between a source row and a target row, the `"letter"` column is updated with the source data
- Otherwise if no match is found for a source row then the row is inserted, creating a new partition if necessary

```python
dt = DeltaTable("tmp/partitioned-table")

source_data = pd.DataFrame({"num": [1, 101], "letter": ["A", "B"], "country": ["US", "CH"]})

(
    dt.merge(
        source=source_data,
        predicate="target.country = source.country AND target.country in ('US','CH')",
        source_alias="source",
        target_alias="target"
    )
    .when_matched_update(
        updates={"letter": "source.letter"}
    )
    .when_not_matched_insert_all()
    .execute()
)

dt = DeltaTable("tmp/partitioned-table")
pdf = dt.to_pandas()
print(pdf)
```

```plaintext
    num letter country
0   101      B      CH
1     1      A      US
2     2      A      US
3   900      m      DE
4  1000      n      DE
5    10      x      CA
6     3      c      CA
```

## Deleting Partition Data

You may want to delete all rows from a specific partition. For example:
```python
dt = DeltaTable("tmp/partitioned-table")

dt.delete("country = 'US'")

dt = DeltaTable("tmp/partitioned-table")
pdf = dt.to_pandas()
print(pdf)
```

```plaintext
    num letter country
0   101      B      CH
1   900      m      DE
2  1000      n      DE
3    10      x      CA
4     3      c      CA
```
This command logically deletes the data by creating a new transaction.

## Maintaining Partitioned Tables

### Optimize & Vacuum

Partitioned tables can accummulate many small files if a partition is frequently appended to. You can compact these into larger files on a specific partition with [`optimize.compact`](../../delta_table/#deltalake.DeltaTable.optimize).

If we want to target compaction at specific partitions we can include partition filters.

```python
 dt.optimize.compact(partition_filters=[("country", "=", "CA")])
 ```

Then optionally [`vacuum`](../../delta_table/#deltalake.DeltaTable.vacuum) the table to remove older, unreferenced files.

### Handling High-Cardinality Columns

Partitioning can be useful for reducing the time it takes to update and query a table, but be mindful of creating partitions against high-cardinality columns (columns with many unique values). Doing so can create an excessive number of partition directories which can hurt performance. For example, partitioning by date is typically better than partitioning by user_id if user_id has millions of unique values.
