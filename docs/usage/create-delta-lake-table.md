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
