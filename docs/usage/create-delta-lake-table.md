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
    // Create and initialize an empty table
    let table = CreateBuilder::new()
        .with_location(path)
        .with_table_name("/tmp/some-table")
        .with_save_mode(SaveMode::Overwrite)
        .with_columns(
            StructType::try_new(vec![
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
            ])?
            .fields()
            .cloned(),
        )
        .await?;

    // Now write into table
    use arrow_schema::{Field, Schema, DataType as ArrowDataType};
    let data = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("num", ArrowDataType::Int32, true),
            Field::new("letter", ArrowDataType::Utf8, true),
        ])),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
            Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
        ],
    )?;
    
    table
        .write(vec![data])
        .with_save_mode(SaveMode::Overwrite)
        .await?;

    
    
    // Alternatively, you can also create:

    // Builds a DeltaTable instance without initializing or creating the table on storage.
    let table = DeltaTableBuilder::from_url(Url::from_directory_path("/tmp/some-table").unwrap())?.build()?;

    // Now initialize and create the table
    table
        .create()
        .with_columns(vec![
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
        .await?;
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
