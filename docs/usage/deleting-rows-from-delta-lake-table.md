# Deleting rows from a Delta Lake table

This section explains how to delete rows from a Delta Lake table.

Suppose you have the following Delta table with four rows:

```
+-------+----------+
|   num | letter   |
|-------+----------|
|     1 | a        |
|     2 | b        |
|     3 | c        |
|     4 | d        |
+-------+----------+
```

Here's how to delete all the rows where the `num` is greater than 2:

=== "Python"

    ```python
    dt = DeltaTable("tmp/my-table")
    dt.delete("num > 2")
    ```

=== "Rust"
    ```rust
    let delta_path = Url::from_directory_path("/tmp/some-table").unwrap();
    let mut table = open_table(delta_path).await?;
    let (table, delete_metrics) = DeltaOps(table)
        .delete()
        .with_predicate(col("num").gt(lit(2)))
        .await?;
    ```
    `with_predicate` expects an argument that can be translated to a Datafusion `Expression`. This can be either using the Dataframe API, or using a `SQL where` clause:
    ```rust
    let table = deltalake::open_table(delta_path).await?;
    let (table, delete_metrics) = DeltaOps(table)
        .delete()
        .with_predicate("num > 2")
        .await?;
    ```
Here are the contents of the Delta table after the delete operation has been performed:

```
+-------+----------+
|   num | letter   |
|-------+----------|
|     1 | a        |
|     2 | b        |
+-------+----------+
```

`dt.delete()` accepts any `SQL where` clause. If no predicate is provided, all rows will be deleted.

Read more in the [API docs](https://delta-io.github.io/delta-rs/api/delta_table/#deltalake.DeltaTable.delete)
