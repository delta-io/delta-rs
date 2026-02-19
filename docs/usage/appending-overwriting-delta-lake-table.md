# Appending to and overwriting a Delta Lake table

This section explains how to append to an existing Delta table and how to overwrite a Delta table.

## Delta Lake append transactions

Suppose you have a Delta table with the following contents:

```
+-------+----------+
|   num | letter   |
|-------+----------|
|     1 | a        |
|     2 | b        |
|     3 | c        |
+-------+----------+
```

Append two additional rows of data to the table:

=== "Python"

    ```python
    from deltalake import write_deltalake, DeltaTable

    df = pd.DataFrame({"num": [8, 9], "letter": ["dd", "ee"]})
    write_deltalake("tmp/some-table", df, mode="append")
    ```

=== "Rust"

    ```rust
    let delta_path = Url::from_directory_path("/tmp/some-table").unwrap();
    let table = open_table(delta_path).await?;
    DeltaOps(table).write(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("num", DataType::Int32, false),
            Field::new("letter", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![8, 9])),
            Arc::new(StringArray::from(vec![
                "dd", "ee"
            ])),
        ])).with_save_mode(SaveMode::Append).await?;
    ```

Here are the updated contents of the Delta table:

```
+-------+----------+
|   num | letter   |
|-------+----------|
|     1 | a        |
|     2 | b        |
|     3 | c        |
|     8 | dd       |
|     9 | ee       |
+-------+----------+
```

Now let's see how to perform an overwrite transaction.

## Delta Lake overwrite transactions

Now let's see how to overwrite the existing Delta table.
=== "Python"

    ```python
    df = pd.DataFrame({"num": [11, 22], "letter": ["aa", "bb"]})
    write_deltalake("tmp/some-table", df, mode="overwrite")
    ```

=== "Rust"

    ```rust
    let delta_path = Url::from_directory_path("/tmp/some-table").unwrap();
    let table = open_table(delta_path).await?;
    DeltaOps(table).write(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("num", DataType::Int32, false),
            Field::new("letter", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                "a", "b", "c",
            ])),
        ])).with_save_mode(SaveMode::Overwrite).await?;
    ```

Here are the contents of the Delta table after the overwrite operation:

```
+-------+----------+
|   num | letter   |
|-------+----------|
|    11 | aa       |
|    22 | bb       |
+-------+----------+
```

Overwriting just performs a logical delete. It doesn't physically remove the previous data from storage. Time travel back to the previous version to confirm that the old version of the table is still accessible.

=== "Python"

    ```python
    dt = DeltaTable("tmp/some-table", version=1)
    ```

=== "Rust"

    ```rust
    let delta_path = Url::from_directory_path("/tmp/some-table").unwrap();
    let mut table = open_table(delta_path).await?;
    table.load_version(1).await?;
    ```

```
+-------+----------+
|   num | letter   |
|-------+----------|
|     1 | a        |
|     2 | b        |
|     3 | c        |
|     8 | dd       |
|     9 | ee       |
+-------+----------+
```
