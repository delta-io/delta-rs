# Managing Delta Tables

## Vacuuming tables

Vacuuming a table will delete any files that have been marked for
deletion. This may make some past versions of a table invalid, so this
can break time travel. However, it will save storage space. Vacuum will
retain files in a certain window, by default one week, so time travel
will still work in shorter ranges.

Delta tables usually don't delete old files automatically, so vacuuming
regularly is considered good practice, unless the table is only appended
to.

Use `DeltaTable.vacuum` to perform the vacuum operation. Note that to prevent accidental deletion, the function performs a dry-run by default: it will only list the files to be deleted. Pass `dry_run=False` to actually delete files.

=== "Python"
    ``` python
    >>> dt = DeltaTable("../rust/tests/data/simple_table")
    >>> dt.vacuum()
    ['../rust/tests/data/simple_table/part-00006-46f2ff20-eb5d-4dda-8498-7bfb2940713b-c000.snappy.parquet',
    '../rust/tests/data/simple_table/part-00190-8ac0ae67-fb1d-461d-a3d3-8dc112766ff5-c000.snappy.parquet',
    '../rust/tests/data/simple_table/part-00164-bf40481c-4afd-4c02-befa-90f056c2d77a-c000.snappy.parquet',
    ...]
    >>> dt.vacuum(dry_run=False) # Don't run this unless you are sure!
    ```
=== "Rust"
    ```rust
    let delta_path = Url::from_directory_path("/rust/tests/data/simple_table").unwrap();
    let mut table = open_table(delta_path).await?;
    let (table, vacuum_metrics) = DeltaOps(table).vacuum().with_dry_run(true).await?;
    println!("Files deleted: {:?}", vacuum_metrics.files_deleted);

    let (table, vacuum_metrics) = DeltaOps(table).vacuum().with_dry_run(false).await?;
    ```

## Optimizing tables

Optimizing a table compacts small files into larger files to avoid the small file problem. This is especially important for tables that get small amounts of data appended to with high frequency. In addition to compacting small files, you can colocate similar data in the same files with Z Ordering, which allows for better file skipping and faster queries.

A table `dt = DeltaTable(...)` has two methods for optimizing it:

- `dt.optimize.compact()` for compacting small files,
- `dt.optimize.z_order()` to compact and apply Z Ordering.

See the section [Small file compaction](./optimize/small-file-compaction-with-optimize.md) for more information and a detailed example on `compact`, and the section [Z Order](./optimize/delta-lake-z-order.md) for more information on `z_order`.

=== "Python"
    ```python
    dt = DeltaTable(...)
    dt.optimize.compact()
    ```

=== "Rust"
    ```rust
    let delta_path = Url::from_directory_path("./data/simple_table").unwrap();
    let mut table = open_table(delta_path).await?;
    let (table, metrics) = DeltaOps(table).optimize().with_type(OptimizeType::Compact).await?;
    ```
