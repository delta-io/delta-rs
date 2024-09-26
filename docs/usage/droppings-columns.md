# Dropping columns from a Delta Lake table

This section explains how to drop columns from an exising Delta table.

## Delta Lake drop columns

You can drop columns from a Delta Lake table using your query engine and an `overwrite` operation.

Let's look at an example with `pandas`.

Suppose you have a dataset with two columns:

```
> # create a toy dataframe
> data = {
>     'first_name': ['bob', 'li', 'leah'],
>     'age': [47, 23, 51],
> }
> df = pd.DataFrame.from_dict(data)

> df

  first_name  age
0        bob   47
1         li   23
2       leah   51
```

You then store this data as a Delta table:

```
# write to deltalake
write_deltalake("tmp/delta-table", df)
```

Now you want to remove one of the columns.

You can do this by reading the data back in with pandas and using the pandas `.drop()` method to drop the column:

```
# read delta table with pandas
df = DeltaTable("tmp/delta-table").to_pandas()

# drop column
df = df.drop(columns=["age"])
```

Next, perform an `overwrite` operation to drop the column from your Delta table.

You will need to specify `mode="overwrite"` as well as `schema_mode="overwrite"` because you will be changing the table schema:

```
write_deltalake(
    "tmp/delta-table",
    df,
    mode="overwrite",
    schema_mode="overwrite",
)
```

Read the data back in to confirm that the column has been dropped:

```
> DeltaTable("tmp/delta-table").to_pandas()

  first_name
0        bob
1         li
2       leah
```

You can easily time travel back to earlier versions using the `version` option:

```
> DeltaTable("tmp/delta-table", version=0).to_pandas()

  first_name  age
0        bob   47
1         li   23
2       leah   51
```

## Logical vs Physical Operations

Dropping columns is a _logical operation_. You are not physically deleting the columns.

When you drop a column, Delta simply creates an entry in the transaction log to indicate that queries should ignore the dropped column going forward. The data remains on disk so you time travel back to it.

If you need to physically delete the data from storage, you will have to run a [vacuum](https://delta-io.github.io/delta-rs/usage/managing-tables/#vacuuming-tables) operation. Vacuuming may break time travel functionality.
