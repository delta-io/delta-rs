`deltalake` is an open source library that makes working with tabular datasets easier, more robust and more performant. With `deltalake` you can add, remove or update rows in a dataset as new data arrives. You can time travel back to earlier versions of a dataset. You can optimize dataset storage from small files to large files.

With `deltalake` you can manage data stored on a local file system or in the cloud. `deltalake` integrates with data manipulation libraries such as Pandas, Polars, DuckDB and DataFusion.

`deltalake` uses a lakehouse framework where you manage your datasets with a `DeltaTable` object and `deltalake` takes care of the underlying files.

## Quick start

1. Install the Python dependencies with `pip`:

    ```bash
    pip install deltalake pandas pyarrow tabulate
    ```

    - `pyarrow` and `pandas` are needed for the DataFrame import
    - `tabulate` is needed to print the DataFrame in final the example

1. Import the required dependencies:

    ```python
    from deltalake import write_deltalake, DeltaTable
    import pandas as pd
    ```

1. Create a Pandas `DataFrame` and write it to a `DeltaTable`:

    ```python
    df = pd.DataFrame({"num": [8, 9], "letter": ["aa", "bb"]})
    write_deltalake("tmp/some-table", df)
    ```

1. Create a DeltaTable object to track metadata for the Delta table:

    ```python
    dt = DeltaTable("tmp/some-table")
    ```

1. Overwrite the DataFrame with new data:

    ```python
    df = pd.DataFrame({"num": [11, 22], "letter": ["dd", "ee"]})
    write_deltalake("tmp/some-table", df, mode="overwrite")
    ```

1. Easily revert to the original version (version 0) of the table:

    ```python
    df = DeltaTable("tmp/some-table", version=0)
    ```

1. Confirm the reversion by printing the contents of the table using the Pandas[to_markdown()](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_markdown.html) function:

    ```python
    print(df.to_pandas().to_markdown())
    ```

1. Output shows the original data from step 3:

    ```
    |    |   num | letter   |
    |---:|------:|:---------|
    |  0 |     8 | aa       |
    |  1 |     9 | bb       |
    ```

## Next steps

- Learn about [Querying Delta Tables](usage/querying-delta-tables.md)
- Learn about using `deltalake` [with Polars](integrations/delta-lake-polars.md)
- Learn about using `deltalake` [with DataFusion](integrations/delta-lake-datafusion.md)
