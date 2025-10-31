# Writing Delta Tables

For overwrites and appends, use `write_deltalake`. If the table does not already exist, it will be created.
The `data` parameter will accept a Pandas DataFrame, a PyArrow Table, or
an iterator of PyArrow Record Batches.

``` python
>>> import pandas as pd
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
alter the schema as part of an overwrite pass in `schema_mode="overwrite"` or `schema_mode="merge"`.
`schema_mode="overwrite"` will completely overwrite the schema, even if columns are dropped; merge will append the new columns
and fill missing columns with `null`. `schema_mode="merge"` is also supported on append operations.

## Overwriting part of the table data using a predicate

!!! note

    This predicate is often called a `replaceWhere` predicate

When you don’t specify the `predicate`, the overwrite save mode will replace
the entire table. Instead of replacing the entire table (which is costly!), you
may want to overwrite only the specific parts of the table that should be
changed. In this case, you can use a `predicate` to overwrite only the relevant
records or partitions. If the predicate and source data being written contain 
partitions that do not exist in the target table, they will be added to the 
target table.

!!! note

    Data written must conform to the same predicate, i.e. not contain any records that don't match the `predicate` condition,
    otherwise the operation will fail

{{ code_example('operations', 'replace_where', ['replaceWhere'])}}

## Using Writer Properties

You can customize the Rust Parquet writer by using the
[WriterProperties](../../api/delta_writer.md#deltalake.WriterProperties).
Additionally, you can apply extra configurations through the
[BloomFilterProperties](../../api/delta_writer.md#deltalake.BloomFilterProperties)
and [ColumnProperties](../../api/delta_writer.md#deltalake.ColumnProperties)
data classes.


Here's how you can do it:
``` python
from deltalake import BloomFilterProperties, ColumnProperties, WriterProperties, write_deltalake
import pyarrow as pa

wp = WriterProperties(
        statistics_truncate_length=200,
        default_column_properties=ColumnProperties(
            bloom_filter_properties=BloomFilterProperties(True, 0.2, 30)
        ),
        column_properties={
            "value_non_bloom": ColumnProperties(bloom_filter_properties=None),
        },
    )

table_path = "/tmp/my_table"

data = pa.table(
        {
            "id": pa.array(["1", "1"], pa.string()),
            "value": pa.array([11, 12], pa.int64()),
            "value_non_bloom": pa.array([11, 12], pa.int64()),
        }
    )

write_deltalake(table_path, data, writer_properties=wp)
```
