# Creating a Delta Lake Table

This section explains how to create a Delta Lake table.

You can easily write a DataFrame to a Delta table.

```python
from deltalake import write_deltalake
import pandas as pd

df = pd.DataFrame({"num": [1, 2, 3], "letter": ["a", "b", "c"]})
write_deltalake("tmp/some-table", df)
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
