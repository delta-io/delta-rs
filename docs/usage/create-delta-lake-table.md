# Creating a Delta Lake Table

This section explains how to create a Delta Lake table.

You can easily write a DataFrame to a Delta table.


=== "pandas"

    ```python
    from deltalake import write_deltalake
    import pandas as pd

    df = pd.DataFrame({"num": [1, 2, 3], "letter": ["a", "b", "c"]})
    write_deltalake("tmp/some-table", df)
    ```

=== "Polars"

    ```python
    import polars as pl

    df = pl.DataFrame({"num": [1, 2, 3], "letter": ["a", "b", "c"]})
    df.write_delta("tmp/some-table")
    ```

=== "Rust"

    ```rust
    let delta_ops = DeltaOps::try_from_uri("tmp/some-table", IORuntime::default()).await?;
    let mut table = delta_ops
        .create()
        .with_table_name("some-table")
        .with_save_mode(SaveMode::Overwrite)
        .with_columns(
            StructType::new(vec![
                StructField::new(
                    "num".to_string(),
                    DataType::Primitive(PrimitiveType::Integer),
                    true,
                ),
                StructField::new(
                    "letter".to_string(),
                    DataType::Primitive(PrimitiveType::String),
                    true,
                ),
            ])
            .fields()
            .cloned(),
        )
        .await?;

    let mut record_batch_writer =
        deltalake::writer::RecordBatchWriter::for_table(&mut table)?;
    record_batch_writer
        .write(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("num", DataType::Int32, true),
                    Field::new("letter", Utf8, true),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec![
                        "a", "b", "c",
                    ])),
                ],
            )?,
        )
        .await?;
    record_batch_writer.flush_and_commit(&mut table).await?;
    ```

Here are the contents of the Delta table in storage:

```
+-------+----------+
|   num | letter   |
|-------+----------|
|     1 | a        |
|     2 | b        |
|     3 | c        |
+-------+----------+
```
