# Adding a Constraint to a table

Check constraints are a way to enforce that only data that meets the constraint is allowed to be added to the table.

## Add the Constraint

```python
from deltalake import DeltaTable
dt = DeltaTable("../rust/tests/data/simple_table")

# Check the schema before hand
print(dt.schema())
# Add the constraint to the table.
dt.alter.add_constraint({"id_gt_0": "id > 0"})
```

After you have added the constraint to the table attempting to append data to the table that violates the constraint
will instead throw an error.

## Verify the constraint by trying to add some data

```python
from deltalake import write_deltalake
import pandas as pd

df = pd.DataFrame({'id': [-1]})
write_deltalake(dt, df, mode='append', engine='rust')
# _internal.DeltaProtocolError: Invariant violations: ["Check or Invariant (id > 0) violated by value in row: [-1]"]
```

Note: ensure you use the `engine='rust'` parameter when writing to the table as this feature is not supported in the
default pyarrow writer. 