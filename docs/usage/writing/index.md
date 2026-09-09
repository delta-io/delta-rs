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

When replacing the entire table with `mode="overwrite"` and `schema_mode="overwrite"`,
you may also provide a different `partition_by` value to replace the table's
partition columns. This is only supported for full table overwrites. Overwrites
that use a `predicate` (also known as `replaceWhere`) must keep the existing
partition columns because they replace only a subset of the table.

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

## Bounding memory when the object store is slow

When a data file reaches its target size, the writer uploads it in the background and starts the next file at once. Each pending upload holds that file's bytes until the store accepts them. A slow store would let them pile up, so `deltalake` caps the bytes they may hold.

The cap is a ceiling, not a reservation. A store that keeps up never reaches it. Once uploads do fall behind, the write waits for one to land before it starts another file, and that backpressure reaches the data source.

By default the cap is a quarter of the memory the process may use — the container limit if there is one, otherwise total system memory — held between 4 and 32 times `target_file_size`. Below 4, uploads run one at a time. Above 32 they do not go faster, because how many keep a store busy depends on its request concurrency, and stores saturate well below 32. At the default 100 MiB `target_file_size` the upper bound is 3.2 GiB, so the memory share only lowers the cap under about 13 GiB of RAM. If the lower bound raises the share instead, `deltalake` warns that the target file size is large for the memory available.

Set `DELTARS_MAX_IN_FLIGHT_UPLOAD_BYTES` to a byte count to choose the cap yourself, or to `-1` to remove it. `deltalake` reads it when a write starts, so it can differ between writes. Keep it above a few times `target_file_size`: a larger file takes the whole cap and still uploads, but then uploads run one at a time.

One cap covers one write call. Every writer in it shares the cap, including the change data feed writer and all partition writers, so a partitioned write does not multiply it. Concurrent writes each get their own. Files still open, one per partition value, hold their current row group outside the cap.

``` python
import os

os.environ["DELTARS_MAX_IN_FLIGHT_UPLOAD_BYTES"] = str(256 * 1024 * 1024)  # 256 MiB

from deltalake import write_deltalake

write_deltalake("s3://bucket/my_table", data, mode="append")
```
