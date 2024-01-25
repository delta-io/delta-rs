# Appending to and overwriting a Delta Lake table

This section explains how to append to an exising Delta table and how to overwrite a Delta table.

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

```python
from deltalake import write_deltalake, DeltaTable

df = pd.DataFrame({"num": [8, 9], "letter": ["dd", "ee"]})
write_deltalake("tmp/some-table", df, mode="append")
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

Now let's see how to overwrite the exisitng Delta table.

```python
df = pd.DataFrame({"num": [11, 22], "letter": ["aa", "bb"]})
write_deltalake("tmp/some-table", df, mode="overwrite")
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

Overwriting just performs a logical delete.  It doesn't physically remove the previous data from storage.  Time travel back to the previous version to confirm that the old version of the table is still accessable.

```python
dt = DeltaTable("tmp/some-table", version=1)

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
