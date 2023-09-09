# Managing Delta Tables

## Vacuuming tables

Vacuuming a table will delete any files that have been marked for
deletion. This may make some past versions of a table invalid, so this
can break time travel. However, it will save storage space. Vacuum will
retain files in a certain window, by default one week, so time travel
will still work in shorter ranges.

Delta tables usually don't delete old files automatically, so vacuuming
regularly is considered good practice, unless the table is only appended
to.

Use `DeltaTable.vacuum` to perform the vacuum operation. Note that to prevent accidental deletion, the function performs a dry-run by default: it will only list the files to be deleted. Pass `dry_run=False` to actually delete files.

``` python
>>> dt = DeltaTable("../rust/tests/data/simple_table")
>>> dt.vacuum()
['../rust/tests/data/simple_table/part-00006-46f2ff20-eb5d-4dda-8498-7bfb2940713b-c000.snappy.parquet',
 '../rust/tests/data/simple_table/part-00190-8ac0ae67-fb1d-461d-a3d3-8dc112766ff5-c000.snappy.parquet',
 '../rust/tests/data/simple_table/part-00164-bf40481c-4afd-4c02-befa-90f056c2d77a-c000.snappy.parquet',
 ...]
>>> dt.vacuum(dry_run=False) # Don't run this unless you are sure!
```

## Optimizing tables

Optimizing tables is not currently supported.