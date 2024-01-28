# Writing Delta Tables

For overwrites and appends, use `write_deltalake`. If the table does not already exist, it will be created.
The `data` parameter will accept a Pandas DataFrame, a PyArrow Table, or
an iterator of PyArrow Record Batches.

``` python
>>> from deltalake import write_deltalake
>>> df = pd.DataFrame({'x': [1, 2, 3]})
>>> write_deltalake('path/to/table', df)
```

Note: `write_deltalake` accepts a Pandas DataFrame, but will convert it to a Arrow table before writing. See caveats in `pyarrow:python/pandas`.

By default, writes create a new table and error if it already exists.
This is controlled by the `mode` parameter, which mirrors the behavior
of Spark's `pyspark.sql.DataFrameWriter.saveAsTable` DataFrame method. To overwrite pass in `mode='overwrite'` and to append pass in `mode='append'`:

``` python
>>> write_deltalake('path/to/table', df, mode='overwrite')
>>> write_deltalake('path/to/table', df, mode='append')
```

`write_deltalake` will raise `ValueError` if the schema of the data
passed to it differs from the existing table's schema. If you wish to
alter the schema as part of an overwrite pass in `overwrite_schema=True`.

## Overwriting a partition

You can overwrite a specific partition by using `mode="overwrite"`
together with `partition_filters`. This will remove all files within the
matching partition and insert your data as new files. This can only be
done on one partition at a time. All of the input data must belong to
that partition or else the method will raise an error.

``` python
>>> from deltalake import write_deltalake
>>> df = pd.DataFrame({'x': [1, 2, 3], 'y': ['a', 'a', 'b']})
>>> write_deltalake('path/to/table', df, partition_by=['y'])

>>> table = DeltaTable('path/to/table')
>>> df2 = pd.DataFrame({'x': [100], 'y': ['b']})
>>> write_deltalake(table, df2, partition_filters=[('y', '=', 'b')], mode="overwrite")

>>> table.to_pandas()
     x  y
0    1  a
1    2  a
2  100  b
```

This method could also be used to insert a new partition if one doesn't
already exist, making this operation idempotent.