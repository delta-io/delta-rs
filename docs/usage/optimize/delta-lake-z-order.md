# Delta Lake Z Order

This section explains how to Z Order a Delta table.

Z Ordering colocates similar data in the same files, which allows for better file skipping and faster queries.

Suppose you have a table with `first_name`, `age`, and `country` columns.

If you Z Order the data by the `country` column, then individuals from the same country will be stored in the same files.  When you subquently query the data for individuals from a given country, it will execute faster because more data can be skipped.

Here's how to Z Order a Delta table:

```python
dt = DeltaTable("tmp")
dt.optimize.z_order([country])
```
