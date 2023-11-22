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

```python
dt = DeltaTable("tmp/my-table")
dt.delete("num > 2")
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
