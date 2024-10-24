# Merging a Table

Delta Lake `MERGE` operations allow you to merge source data into a target table based on specific conditions. `MERGE` operations are great for making selective changes to your Delta table without having to rewrite the entire table.

Use [`dt.merge()`][deltalake.DeltaTable.merge] with a single or multiple conditional statements to perform CRUD operations (Create, Read, Update, Delete) at scale. You can also use Delta Lake merge for efficient Change Data Capture (CDC), Slowly Changing Dimensions (SDC) operations and to ensure GDPR compliance.

## Basic Structure of `MERGE` Command

Let’s start by understanding the basic structure of a Delta Lake `MERGE` command in delta-rs.

Use the [TableMerger API](https://delta-io.github.io/delta-rs/api/delta_table/delta_table_merger/) to construct a `MERGE` command with one or multiple conditional clauses:

```python
    (
        dt.merge(                                       # target data
            source=source_data,                         # source data
            predicate="target.x = source.x",
            source_alias="source",
            target_alias="target")
        .when_matched_update(                           # conditional statement
            updates={"x": "source.x", "y":"source.y"})
        .execute()
    )
```

In the syntax above:

- `dt` is your target Delta table
- `source_data` is your source data
- `source` and `target` are your respective aliases for your source and target datasets
- `when_matched_update` is one of many possible conditional statements, see the sections below for more
- `execute()` executes the MERGE operation with the specified settings

Note that executing a `MERGE` operation automatically writes the changes to your Delta table in a single transaction.

## Update

Use [`when_matched_update`][deltalake.table.TableMerger.when_matched_update] to perform an UPDATE operation.

You can define the rules for the update using the `updates` keyword. If a `predicate` is passed to `merge()` then only rows which evaluate to true will be updated.

For example, let’s update the value of a column in the target table based on a matching row in the source table.

```python
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

# define target table
> target_data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
> write_deltalake("tmp_table", target_data)
> dt = DeltaTable("tmp_table")
> dt.to_pandas().sort_values("x", ignore_index=True)

   x  y
0  1  4
1  2  5
2  3  6

# define source table
> source_data = pa.table({"x": [2, 3], "y": [5,8]})
> source_data

   x  y
0  2  5
1  3  8

# define merge logic
> (
>     dt.merge(
>         source=source_data,
>         predicate="target.x = source.x",
>         source_alias="source",
>         target_alias="target")
>     .when_matched_update(
>         updates={"x": "source.x", "y":"source.y"})
>     .execute()
> )
```

First, we match rows for which the `x` values are the same using `predicate="target.x = source.x"`. We then update the `x` and `y` values of the matched row with the new (source) values using `updates={"x": "source.x", "y":"source.y"}`.

```python
# inspect result
> dt.to_pandas().sort_values("x", ignore_index=True)

   x  y
0  1  4
1  2  5
2  3  8
```

The value of the `y` column has been correctly updated for the row that matches our predicate.

You can also use [`when_matched_update_all`][deltalake.table.TableMerger.when_matched_update_all] to update all source fields to target fields. In this case, source and target are required to have the same field names.

## Insert

Use [`when_not_matched_insert`][deltalake.table.TableMerger.when_not_matched_insert] to perform an INSERT operation.

For example, let’s say we start with the same target table:

```python
> target_data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
> write_deltalake("tmp_table", target_data)
> dt = DeltaTable("tmp_table")

   x  y
0  1  4
1  2  5
2  3  6
```

And we want to merge only new records from our source data, without duplication:

```python
> source_data = pa.table({"x": [2,3,7], "y": [4,5,8]})

   x  y
0  2  5
1  3  6
2  7  8
```

The `MERGE` syntax would be as follows:

```python
(
    dt.merge(
        source=source_data,
        predicate="target.x = source.x",
        source_alias="source",
        target_alias="target")
    .when_not_matched_insert(
        updates={"x": "source.x", "y":"source.y"})
    .execute()
)

> # inspect result
> print(dt.to_pandas().sort_values("x", ignore_index=True))

   x  y
0  1  4
1  2  5
2  3  6
3  7  8
```

The new row has been successfully added to the target dataset.

You can also use [`when_not_matched_insert_all`][deltalake.table.TableMerger.when_not_matched_insert_all] to insert a new row to the target table, updating all source fields to target fields. In this case, source and target are required to have the same field names.

## Delete

Use [`when_matched_delete`][deltalake.table.TableMerger.when_matched_delete] to perform a DELETE operation.

For example, given the following `target_data` and `source_data`:

```python
target_data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
write_deltalake("tmp_table", target_data)
dt = DeltaTable("tmp_table")
source_data = pa.table({"x": [2, 3], "deleted": [False, True]})
```

You can delete the rows that match a predicate (in this case `"deleted" = True`) using:

```python
(
    dt.merge(
        source=source_data,
        predicate="target.x = source.x",
        source_alias="source",
        target_alias="target")
    .when_matched_delete(
        predicate="source.deleted = true")
    .execute()
)
```

This will result in:

```python
> dt.to_pandas().sort_values("x", ignore_index=True)

   x  y
0  1  4
1  2  5
```

The matched row has been successfully deleted.

## Upsert

You can combine conditional statements to perform more complex operations.

To perform an upsert operation, use `when_matched_update` and `when_not_matched_insert` in a single `merge()` clause.

For example:

```python
target_data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
write_deltalake("tmp_table", target_data)
dt = DeltaTable("tmp_table")
source_data = pa.table({"x": [2, 3, 5], "y": [5, 8, 11]})

(
    dt.merge(
        source=source_data,
        predicate="target.x = source.x",
        source_alias="source",
        target_alias="target")
    .when_matched_update(
        updates={"x": "source.x", "y":"source.y"})
    .when_not_matched_insert(
        updates={"x": "source.x", "y":"source.y"})
    .execute()
)
```

This will give you the following output:

```python
> dt.to_pandas().sort_values("x", ignore_index=True)

   x  y
0  1  4
1  2  5
2  3  8
3  5  11
```

## Upsert with Delete

Use the [`when_matched_delete`][deltalake.table.TableMerger.when_matched_delete] or [`when_not_matched_by_source_delete`][deltalake.table.TableMerger.when_not_matched_by_source_delete] methods to add a DELETE operation to your upsert. This is helpful if you want to delete stale records from the target data.

For example, given the same `target_data` and `source_data` used in the section above:

```python
(
    dt.merge(
        source=source_data,
        predicate="target.x = source.x",
        source_alias="source",
        target_alias="target")
    .when_matched_update(
        updates={"x": "source.x", "y":"source.y"})
    .when_not_matched_insert(
        updates={"x": "source.x", "y":"source.y"})
    .when_not_matched_by_source_delete()
    .execute()
)
```

This will result in:

```python
> dt.to_pandas().sort_values("x", ignore_index=True)

   x  y
0  2  5
1  3  8
2  5  11
```

The row containing old data no longer present in the source dataset has been successfully deleted.

## Multiple Matches

Note that when multiple match conditions are met, the first condition that matches is executed.

For example, given the following `target_data` and `source_data`:

```python
target_data = pa.table({"x": [1, 2, 3], "y": [4, 5, 6]})
write_deltalake("tmp_table", target_data)
dt = DeltaTable("tmp_table")
source_data = pa.table({"x": [2, 3, 5], "y": [5, 8, 11]})
```

Let’s perform a merge with `when_matched_delete` first, followed by `when_matched_update`:

```python
(
    dt.merge(
        source=source_data,
        predicate="target.x = source.x",
        source_alias="source",
        target_alias="target")
    .when_matched_delete(
        predicate="source.x = target.x")
    .when_matched_update(
        updates={"x": "source.x", "y":"source.y"})
    .execute()
)
```

This will result in:

```python
> dt.to_pandas().sort_values("x", ignore_index=True)

   x  y
0  1  4
```

Let’s now perform the merge with the flipped order: `update` first, then `delete`:

```python
(
    dt.merge(
        source=source_data,
        predicate="target.x = source.x",
        source_alias="source",
        target_alias="target")
    .when_matched_update(
        updates={"x": "source.x", "y":"source.y"})
    .when_matched_delete(
        predicate="source.x = target.x")
    .execute()
)
```

This will result in:

```python
> dt.to_pandas().sort_values("x", ignore_index=True)

   x  y
0  1  4
1  2  5
2  3  8
```

## Conditional Updates

You can perform conditional updates for rows that have no match in the source data using [when_not_matched_by_source_update][deltalake.table.TableMerger.when_not_matched_by_source_update].

For example, given the following target_data and source_data:

```python
target_data = pa.table({"1 x": [1, 2, 3], "1y": [4, 5, 6]})
write_deltalake("tmp", target_data)
dt = DeltaTable("tmp")
source_data = pa.table({"x": [2, 3, 4]})
```

Set y = 0 for all rows that have no matches in the new source data, provided that the original y value is greater than 3:

```python
(
   dt.merge(
       source=new_data,
       predicate='target.x = source.x',
       source_alias='source',
       target_alias='target')
   .when_not_matched_by_source_update(
       predicate = "`1y` > 3",
       updates = {"`1y`": "0"})
   .execute()
)
```

This will result in:

```python
> dt.to_pandas().sort_values("x", ignore_index=True)
   x  y
0  1  0
1  2  5
2  3  6
```

## Notes

- Column names with special characters, such as numbers or spaces should be encapsulated in backticks: "target.`123column`" or "target.`my column`"
